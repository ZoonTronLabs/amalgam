//! Only a retained representation or a new slot constructs the full envelope.
use super::*;
use crate::entry::{PlainMetadata, PlainReplacement};

impl<V: Clone> Sharded<V> {
    pub(in crate::memory) fn insert_plain(
        &self,
        key: &str,
        value: V,
        metadata: PlainMetadata,
        time: crate::time::local::WriteTime,
        policy: super::super::CapturePolicy,
    ) -> super::super::RetentionCommit<V> {
        let deadlines = self.timing.prepare_boundaries(
            metadata.logical(),
            metadata.physical(),
            time,
            self.expiry,
        );
        let (hash, shard) = self.route(key);
        let mut state = write(shard);
        let generation = self.generation.load(Ordering::Acquire);
        if generation == u64::MAX {
            return super::super::RetentionCommit::rejected(
                CapacityRejection::GenerationExhausted,
                Vec::new(),
            );
        }
        let Shard {
            entries,
            weight,
            origins,
        } = &mut *state;
        origins.advance(key);
        let slot = entries
            .raw_entry_mut()
            .from_hash(hash, |stored| stored.as_ref() == key);
        let (retired, old_weight, replacing) =
            commit_slot(slot, key, value, metadata, generation, deadlines, policy);
        *weight = *weight - old_weight + 1;
        super::super::RetentionCommit {
            admission: if replacing {
                MemoryAdmission::Replaced
            } else {
                MemoryAdmission::Admitted
            },
            retired,
        }
    }
}
#[allow(clippy::too_many_arguments)]
fn commit_slot<V>(
    slot: RawEntryMut<'_, Arc<str>, Stored<V>, RandomState>,
    key: &str,
    value: V,
    metadata: PlainMetadata,
    generation: u64,
    deadlines: super::super::deadlines::Deadlines,
    policy: super::super::CapturePolicy,
) -> (super::super::Retirements<V>, u128, bool) {
    match slot {
        RawEntryMut::Vacant(slot) => {
            slot.insert(
                Arc::from(key),
                Stored {
                    entry: metadata.into_fresh(value, Box::new([])).into_entry(),
                    generation,
                    deadlines,
                    capture: policy.admission,
                },
            );
            (super::super::Retirements::None, 0, false)
        }
        RawEntryMut::Occupied(mut slot) => {
            let previous = slot.get();
            let weight = entry_weight(&previous.entry);
            let replacing = previous.generation == generation;
            let reason = if replacing {
                Reason::Replaced
            } else {
                Reason::Continuity
            };
            let value = if policy.permits_payload_only(previous.capture) {
                match slot.get_mut().entry.replace_plain(value, metadata) {
                    PlainReplacement::Replaced { retired } => {
                        let stored = slot.get_mut();
                        stored.generation = generation;
                        stored.deadlines = deadlines;
                        stored.capture = policy.admission;
                        return (
                            super::super::Retirements::Reused {
                                value: retired,
                                reason,
                            },
                            weight,
                            replacing,
                        );
                    }
                    PlainReplacement::Shared { incoming } => incoming,
                }
            } else {
                value
            };
            let key = Arc::clone(slot.key());
            let old = slot.insert(Stored {
                entry: metadata.into_fresh(value, Box::new([])).into_entry(),
                generation,
                deadlines,
                capture: policy.admission,
            });
            (
                super::super::Retirements::One(old.retire(key, reason)),
                weight,
                replacing,
            )
        }
    }
}
