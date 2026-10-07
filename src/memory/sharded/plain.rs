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
#[cfg(target_arch = "x86_64")]
impl<V: Clone> Sharded<V> {
    pub(in crate::memory) fn insert_admitted_plain<'a>(
        &self,
        key: &str,
        value: V,
        metadata: PlainMetadata,
        time: crate::time::local::WriteTime,
        policy: super::super::CapturePolicy,
        reservation: crate::execution::DeferredInlinePermit<'a>,
    ) -> (
        crate::execution::InlinePermit<'a>,
        crate::Result<super::super::RetentionCommit<V>>,
    ) {
        let deadlines = self.timing.prepare_boundaries(
            metadata.logical(),
            metadata.physical(),
            time,
            self.expiry,
        );
        let (hash, shard) = self.route(key);
        let mut state = write(shard);
        // write() crossed its mandatory SeqCst fence AFTER scope reservation.
        let permit = reservation.after_writer(&state);
        if let Err(error) = permit.admit() {
            drop(state);
            // The unused value is a function input: it is destroyed after the
            // guard, while the returned permit still covers its destruction.
            return (permit, Err(error));
        }
        let generation = self.generation.load(Ordering::Acquire);
        let commit = if generation == u64::MAX {
            super::super::RetentionCommit::rejected(
                CapacityRejection::GenerationExhausted,
                Vec::new(),
            )
        } else {
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
        };
        drop(state);
        (permit, Ok(commit))
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

#[cfg(all(test, target_arch = "x86_64"))]
mod shared_publication_tests {
    use super::*;
    use crate::execution::Scopes;
    use crate::{EntryOptions, Error, FactoryCancellationReason};
    use std::sync::mpsc;
    use std::time::Duration;

    fn facts() -> (PlainMetadata, crate::time::local::WriteTime) {
        let crate::entry::DefaultFreshPlan::Plain(plan) =
            crate::entry::DefaultFreshPlan::for_options(&EntryOptions::new(Duration::from_secs(
                60,
            )))
            .unwrap()
        else {
            panic!("plain lifetime plan expected")
        };
        let now = Timestamp::from_ticks(0);
        (
            plan.metadata(now),
            crate::time::local::WriteTime::Clock(now),
        )
    }
    fn capture() -> super::super::super::CapturePolicy {
        super::super::super::CapturePolicy {
            admission: CaptureAdmission::Unarmed,
            timing: crate::EvictionCapture::AtInsertion,
        }
    }
    #[tokio::test]
    async fn close_while_waiting_for_a_writer_rejects_without_changing_the_value() {
        let store = Arc::new(Sharded::new(MemoryExpiry::ClockDriven));
        let scopes = Scopes::new();
        let (metadata, time) = facts();
        store.insert_plain("key", 7_u64, metadata, time, capture());
        let hold = write(store.route("key").1);
        let (started, entered) = mpsc::channel();
        let writing_store = store.clone();
        let writing_scopes = scopes.clone();
        let writer = std::thread::spawn(move || {
            let reservation = writing_scopes.defer_inline();
            started.send(()).unwrap();
            let (permit, result) = writing_store.insert_admitted_plain(
                "key",
                9,
                metadata,
                time,
                capture(),
                reservation,
            );
            drop(permit);
            result.map(|commit| commit.admission)
        });
        entered.recv_timeout(Duration::from_secs(2)).unwrap();
        scopes.close();
        assert!(
            tokio::time::timeout(Duration::from_millis(25), scopes.drained())
                .await
                .is_err()
        );
        drop(hold);
        assert!(matches!(writer.join().unwrap(), Err(Error::CacheClosed)));
        tokio::time::timeout(Duration::from_secs(2), scopes.drained())
            .await
            .unwrap();
        let value = store.with_ready("key", Timestamp::from_ticks(0), |entry| *entry.value());
        assert_eq!(value, Some(7));
    }
    #[tokio::test]
    async fn completion_after_writer_release_is_still_counted_through_shutdown() {
        let store = Arc::new(Sharded::<u64>::new(MemoryExpiry::ClockDriven));
        let scopes = Scopes::new();
        let (metadata, time) = facts();
        let (committed, entered) = mpsc::channel();
        let (release, released) = mpsc::channel();
        let writing_store = store.clone();
        let writing_scopes = scopes.clone();
        let writer = std::thread::spawn(move || {
            let (permit, result) = writing_store.insert_admitted_plain(
                "key",
                9,
                metadata,
                time,
                capture(),
                writing_scopes.defer_inline(),
            );
            assert!(matches!(
                result.unwrap().admission,
                MemoryAdmission::Admitted
            ));
            committed.send(()).unwrap();
            released.recv_timeout(Duration::from_secs(2)).unwrap();
            permit.status(None)
        });
        entered.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(
            store.with_ready("key", Timestamp::from_ticks(0), |entry| *entry.value()),
            Some(9)
        );
        scopes.close();
        assert!(
            tokio::time::timeout(Duration::from_millis(25), scopes.drained())
                .await
                .is_err()
        );
        release.send(()).unwrap();
        assert!(matches!(
            writer.join().unwrap(),
            Err(Error::OperationCancelled {
                reason: FactoryCancellationReason::CacheShutdown,
            })
        ));
        tokio::time::timeout(Duration::from_secs(2), scopes.drained())
            .await
            .unwrap();
    }
}
