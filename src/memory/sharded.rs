//! Independently locked unbounded storage; extraction precedes user reclamation.
use super::RetirementReason as Reason;
use super::{
    Arc, CapacityRejection, CaptureAdmission, Entry, Expected, MemoryAdmission, MemoryExpiry,
    MemoryRead, MemoryUsage, MemoryWriteEvent, Retirement, Timestamp, entry_weight,
};
use crate::reader_slots::{
    ReadGuard as RwLockReadGuard, ReaderSlots as RwLock, WriteGuard as RwLockWriteGuard,
};
use ahash::RandomState;
use hashbrown::{HashMap, hash_map::RawEntryMut};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

const SHARDS: usize = 64;
pub(super) struct Sharded<V> {
    shards: Box<[RwLock<Shard<V>>]>,
    hash: RandomState,
    generation: AtomicU64,
    maintenance: AtomicUsize,
    expiry: MemoryExpiry,
}
struct Shard<V> {
    entries: HashMap<Arc<str>, Stored<V>, RandomState>,
    weight: u128,
    origins: super::Origins,
}
struct Stored<V> {
    entry: Entry<V>,
    generation: u64,
    expiration: Option<Instant>,
    capture: CaptureAdmission,
}
impl<V> Stored<V> {
    fn retirement_reason(&self, now: Option<Timestamp>, generation: u64) -> Option<Reason> {
        self.retirement_at(now, generation, None)
    }
    fn retirement_at(
        &self,
        now: Option<Timestamp>,
        generation: u64,
        elapsed: Option<Instant>,
    ) -> Option<Reason> {
        if self.generation != generation {
            Some(Reason::Continuity)
        } else if !self.entry.is_read_eligible() {
            Some(Reason::Eligibility)
        } else if now.is_some_and(|at| self.entry.is_physically_expired(at))
            || self
                .expiration
                .is_some_and(|at| elapsed.unwrap_or_else(Instant::now) >= at)
        {
            Some(Reason::Expired)
        } else {
            None
        }
    }
    fn retire(self, key: Arc<str>, reason: Reason) -> Retirement<V> {
        Retirement {
            key,
            entry: self.entry,
            reason,
            capture: self.capture,
        }
    }
}
impl<V> Sharded<V> {
    pub(super) fn new(expiry: MemoryExpiry) -> Self {
        let hash = RandomState::new();
        let shards = (0..SHARDS)
            .map(|_| {
                RwLock::new(Shard {
                    entries: HashMap::with_hasher(hash.clone()),
                    weight: 0,
                    origins: super::Origins::default(),
                })
            })
            .collect::<Box<[_]>>();
        Self {
            shards,
            hash,
            generation: AtomicU64::new(1),
            maintenance: AtomicUsize::new(0),
            expiry,
        }
    }
    fn route(&self, key: &str) -> (u64, &RwLock<Shard<V>>) {
        // Hash once before acquiring any storage guard; raw equality still
        // compares the complete string, so hash collisions cannot mix keys.
        let hash = self.hash.hash_one(key);
        (hash, &self.shards[(hash as usize) & (SHARDS - 1)])
    }
    /// Holds one reader slot through internal checks and ordinary value Clone.
    /// Optional callbacks must use an owned Entry after releasing this guard.
    pub(super) fn with_ready<R>(
        &self,
        key: &str,
        now: Timestamp,
        read_entry: impl FnOnce(&Entry<V>) -> R,
    ) -> Option<R> {
        let (hash, shard) = self.route(key);
        let state = read(shard);
        let (_, stored) = state
            .entries
            .raw_entry()
            .from_hash(hash, |key_in_map| key_in_map.as_ref() == key)?;
        stored
            .retirement_reason(Some(now), self.generation.load(Ordering::Acquire))
            .is_none()
            .then(|| read_entry(&stored.entry))
    }
    /// A private local clock is sampled after admission, under the value slot.
    /// Logical freshness and physical expiry share that one elapsed sample.
    pub(super) fn with_local_ready<R>(
        &self,
        key: &str,
        clock: &crate::time::local::LocalClock,
        read_entry: impl FnOnce(&Entry<V>, Timestamp) -> R,
    ) -> Option<R> {
        let (hash, shard) = self.route(key);
        let state = read(shard);
        let (_, stored) = state
            .entries
            .raw_entry()
            .from_hash(hash, |k| k.as_ref() == key)?;
        let (now, elapsed) = clock.sample();
        stored
            .retirement_at(
                Some(now),
                self.generation.load(Ordering::Acquire),
                Some(elapsed),
            )
            .is_none()
            .then(|| read_entry(&stored.entry, now))
    }
    pub(super) fn get(&self, key: &str, now: Option<Timestamp>) -> MemoryRead<V> {
        let (hash, shard) = self.route(key);
        let candidate = {
            let state = read(shard);
            let Some((_, stored)) = state
                .entries
                .raw_entry()
                .from_hash(hash, |k| k.as_ref() == key)
            else {
                return MemoryRead::Absent;
            };
            let Some(reason) =
                stored.retirement_reason(now, self.generation.load(Ordering::Acquire))
            else {
                return MemoryRead::Present(stored.entry.clone());
            };
            (stored.entry.clone(), reason)
        };
        let removed = Self::remove_same(shard, hash, key, Some(&candidate.0));
        match removed {
            Some((key, stored)) => MemoryRead::Expired {
                key,
                entry: stored.entry,
                reason: candidate.1,
                capture: stored.capture,
            },
            None => MemoryRead::Raced(candidate.0),
        }
    }
    fn remove_same(
        shard: &RwLock<Shard<V>>,
        hash: u64,
        key: &str,
        expected: Option<&Entry<V>>,
    ) -> Option<(Arc<str>, Stored<V>)> {
        let mut state = write(shard);
        let removed = match state
            .entries
            .raw_entry_mut()
            .from_hash(hash, |k| k.as_ref() == key)
        {
            RawEntryMut::Occupied(slot)
                if expected.is_none_or(|entry| slot.get().entry.is_same_instance(entry)) =>
            {
                Some(slot.remove_entry())
            }
            RawEntryMut::Occupied(_) | RawEntryMut::Vacant(_) => None,
        };
        if let Some((_, stored)) = &removed {
            state.weight -= entry_weight(&stored.entry);
        }
        removed
    }
    pub(super) fn remove(&self, key: &str, expected: Option<&Entry<V>>) -> Option<Retirement<V>> {
        let (hash, shard) = self.route(key);
        Self::remove_same(shard, hash, key, expected)
            .map(|(key, stored)| stored.retire(key, Reason::Explicit))
    }
    pub(super) fn origin_generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }
    pub(super) fn capture_origin_from(
        &self,
        key: &Arc<str>,
        revision: Option<Arc<dyn super::RevisionSource>>,
    ) -> crate::Result<(Arc<dyn super::RevisionSource>, u64, u64)> {
        let (_, shard) = self.route(key);
        let mut state = write(shard);
        let generation = self.origin_generation();
        if generation == u64::MAX {
            return Err(crate::RecoveryError::GenerationExhausted.into());
        }
        let (revision, captured) = state.origins.capture_from(key, revision)?;
        Ok((revision, captured, generation))
    }
    pub(super) fn forget_origin(&self, key: &str, revision: &Arc<dyn super::RevisionSource>) {
        let (_, shard) = self.route(key);
        write(shard).origins.forget(key, revision);
    }
    pub(super) fn skip_origin(&self, key: &str, origin: &super::MemoryOrigin<V>) -> bool {
        let (_, shard) = self.route(key);
        let state = write(shard);
        if origin.matches(self.origin_generation()) {
            state.origins.advance(key);
            true
        } else {
            false
        }
    }
    pub(super) fn invalidate_origin(&self, key: &str) {
        let (_, shard) = self.route(key);
        write(shard).origins.advance(key);
    }
    pub(super) fn remove_as_mutation(&self, key: &str) -> Option<Retirement<V>> {
        let (_, shard) = self.route(key);
        let mut state = write(shard);
        state.origins.advance(key);
        let (key, stored) = state.entries.remove_entry(key)?;
        state.weight -= entry_weight(&stored.entry);
        Some(stored.retire(key, Reason::Explicit))
    }
    pub(super) fn insert_prepared(
        &self,
        key: &super::StorageKey<'_>,
        entry: &mut super::PreparedEntry<V>,
        time: crate::time::local::WriteTime,
        expected: Expected<'_, V>,
        capture: CaptureAdmission,
        event: MemoryWriteEvent,
    ) -> super::RetentionCommit<V> {
        let now = time.now();
        let expiration = match self.expiry {
            MemoryExpiry::ClockDriven => None,
            MemoryExpiry::RealTime => time.physical_start().checked_add(
                entry
                    .meta()
                    .physical_expiration()
                    .saturating_duration_since(now),
            ),
        };
        let (hash, shard) = self.route(key);
        let mut state = write(shard);
        let generation = self.generation.load(Ordering::Acquire);
        if generation == u64::MAX {
            return super::RetentionCommit::rejected(
                CapacityRejection::GenerationExhausted,
                Vec::new(),
            );
        }
        let Shard {
            entries,
            weight,
            origins,
        } = &mut *state;
        let slot = entries
            .raw_entry_mut()
            .from_hash(hash, |stored| stored.as_ref() == key.as_ref());
        let current = match &slot {
            RawEntryMut::Occupied(slot) => Some(slot.get()),
            RawEntryMut::Vacant(_) => None,
        };
        let visible = current.filter(|stored| stored.generation == generation);
        let replacing_visible = visible.is_some();
        if !expected.matches_generation(visible.map(|stored| &stored.entry), generation) {
            return super::RetentionCommit::rejected(CapacityRejection::VersionChanged, Vec::new());
        }
        let capture = match (current, event) {
            (Some(old), MemoryWriteEvent::Expire) => old.capture,
            _ => capture,
        };
        let old_weight = current.map_or(0, |stored| entry_weight(&stored.entry));
        let new_weight = entry.meta().size().map_or(1, |size| size.units()) as u128;
        if expected.changes_origin() {
            origins.advance(key);
        }
        let retired = match slot {
            RawEntryMut::Occupied(mut slot) => {
                let key = Arc::clone(slot.key());
                let old_capture = slot.get().capture;
                let reason = match event {
                    MemoryWriteEvent::Set if slot.get().generation == generation => {
                        Reason::Replaced
                    }
                    MemoryWriteEvent::Set => Reason::Continuity,
                    MemoryWriteEvent::Expire => Reason::Metadata,
                };
                if let Some(previous) = entry.try_reuse(&mut slot.get_mut().entry) {
                    let stored = slot.get_mut();
                    stored.generation = generation;
                    stored.expiration = expiration;
                    stored.capture = capture;
                    super::Retirements::Reused {
                        key,
                        value: previous,
                        reason,
                        capture: old_capture,
                    }
                } else {
                    let old = slot.insert(Stored {
                        entry: entry.take_for_storage(),
                        generation,
                        expiration,
                        capture,
                    });
                    super::Retirements::One(old.retire(key, reason))
                }
            }
            RawEntryMut::Vacant(slot) => {
                slot.insert_hashed_nocheck(
                    hash,
                    key.shared(),
                    Stored {
                        entry: entry.take_for_storage(),
                        generation,
                        expiration,
                        capture,
                    },
                );
                super::Retirements::None
            }
        };
        *weight = *weight - old_weight + new_weight;
        let admission = if replacing_visible {
            MemoryAdmission::Replaced
        } else {
            MemoryAdmission::Admitted
        };
        super::RetentionCommit { admission, retired }
    }
    #[cfg(test)]
    fn insert(
        &self,
        key: Arc<str>,
        entry: Entry<V>,
        now: Timestamp,
        expected: Expected<'_, V>,
        capture: CaptureAdmission,
        event: MemoryWriteEvent,
    ) -> super::RetentionCommit<V> {
        let mut candidate = super::PreparedEntry::Candidate(entry);
        self.insert_prepared(
            &super::StorageKey::Shared(key),
            &mut candidate,
            crate::time::local::WriteTime::Clock(now),
            expected,
            capture,
            event,
        )
    }
    pub(super) fn begin_clear(&self) -> u64 {
        // MAX is terminal admission, never a reused continuity identity.
        #[allow(deprecated, reason = "Atomic::try_update is unavailable on Rust 1.88")]
        self.generation
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |at| at.checked_add(1))
            .map_or(u64::MAX, |at| at + 1)
    }
    #[cfg(test)]
    fn drain_before(&self, shard: usize, generation: u64) -> Vec<Retirement<V>> {
        let mut state = write(&self.shards[shard]);
        let old = std::mem::replace(&mut state.entries, HashMap::with_hasher(self.hash.clone()));
        state.weight = 0;
        let mut retired = Vec::with_capacity(old.len());
        for (key, stored) in old {
            if stored.generation < generation {
                retired.push(stored.retire(key, Reason::Explicit));
            } else {
                state.weight += entry_weight(&stored.entry);
                state.entries.insert(key, stored);
            }
        }
        retired
    }
    pub(super) fn expire(&self, shard: usize, now: Option<Timestamp>) -> Vec<Retirement<V>> {
        let mut state = write(&self.shards[shard]);
        self.expire_locked(&mut state, now)
    }
    fn expire_locked(&self, state: &mut Shard<V>, now: Option<Timestamp>) -> Vec<Retirement<V>> {
        let generation = self.generation.load(Ordering::Acquire);
        let dead: Vec<_> = state
            .entries
            .iter()
            .filter_map(|(key, stored)| {
                stored
                    .retirement_reason(now, generation)
                    .map(|reason| (Arc::clone(key), reason))
            })
            .collect();
        let mut retired = Vec::with_capacity(dead.len());
        for (key, reason) in dead {
            if let Some((key, stored)) = state.entries.remove_entry(&key) {
                state.weight -= entry_weight(&stored.entry);
                retired.push(stored.retire(key, reason));
            }
        }
        retired
    }
    pub(super) fn maintain_step(&self, now: Option<Timestamp>) -> Vec<Retirement<V>> {
        let first = self.maintenance.fetch_add(8, Ordering::Relaxed);
        let mut retired = Vec::new();
        for offset in 0..8 {
            let shard = &self.shards[first.wrapping_add(offset) & (SHARDS - 1)];
            if let Some(mut state) = shard.try_write() {
                retired.extend(self.expire_locked(&mut state, now));
            }
        }
        retired
    }
    pub(super) fn shard_count(&self) -> usize {
        self.shards.len()
    }
    pub(super) fn usage(&self) -> MemoryUsage {
        let mut usage = MemoryUsage {
            entries: 0,
            weight: 0,
        };
        for shard in &self.shards {
            let state = read(shard);
            usage.entries = usage.entries.saturating_add(state.entries.len() as u64);
            usage.weight = usage.weight.saturating_add(state.weight);
        }
        usage
    }
}
fn read<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    lock.read()
}
fn write<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    lock.write()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EntryOptions;
    fn entry(value: u64) -> Entry<u64> {
        Entry::fresh(
            value,
            &EntryOptions::default(),
            Timestamp::from_ticks(0),
            Box::new([]),
            None,
            None,
        )
    }
    fn put(store: &Sharded<u64>, key: &str, value: u64) -> super::super::RetentionCommit<u64> {
        store.insert(
            Arc::from(key),
            entry(value),
            Timestamp::from_ticks(0),
            Expected::Any,
            CaptureAdmission::Armed,
            MemoryWriteEvent::Set,
        )
    }
    #[test]
    fn local_sample_is_taken_after_waiting_for_the_value_slot() {
        use crate::time::local::CacheClock;
        let CacheClock::Local(clock) = CacheClock::local() else {
            unreachable!()
        };
        let store = std::sync::Arc::new(Sharded::new(MemoryExpiry::RealTime));
        let before = clock.sample().0;
        store.insert(
            Arc::from("k"),
            Entry::fresh(
                1,
                &EntryOptions::default(),
                before,
                Box::new([]),
                None,
                None,
            ),
            before,
            Expected::Any,
            CaptureAdmission::Armed,
            MemoryWriteEvent::Set,
        );
        let (_, shard) = store.route("k");
        let writer = write(shard);
        let (started, started_rx) = std::sync::mpsc::channel();
        let reading = Arc::clone(&store);
        let sampled = std::thread::spawn(move || {
            started.send(()).unwrap();
            reading.with_local_ready("k", &clock, |_, now| now).unwrap()
        });
        started_rx.recv().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(20));
        drop(writer);
        let sampled = sampled.join().unwrap();
        assert!(sampled.saturating_duration_since(before) >= std::time::Duration::from_millis(20));
    }
    #[test]
    fn overlapping_clear_drains_preserve_all_newer_generations() {
        let store = Sharded::new(MemoryExpiry::ClockDriven);
        put(&store, "old", 1);
        let first = store.begin_clear();
        put(&store, "new", 2);
        let second = store.begin_clear();
        put(&store, "newest", 3);
        let mut retired = Vec::new();
        for shard in 0..store.shard_count() {
            retired.extend(store.drain_before(shard, first));
        }
        assert_eq!(retired.len(), 1);
        assert_eq!(*retired[0].entry.value(), 1);
        assert_eq!(store.usage().entries, 2);
        retired.clear();
        for shard in 0..store.shard_count() {
            retired.extend(store.drain_before(shard, second));
        }
        assert_eq!(retired.len(), 1);
        assert_eq!(*retired[0].entry.value(), 2);
        let MemoryRead::Present(newest) = store.get("newest", Some(Timestamp::from_ticks(0)))
        else {
            panic!("new generation was lost")
        };
        assert_eq!(*newest.value(), 3);
    }
    #[test]
    fn generation_exhaustion_rejects_admission_without_reusing_a_clear_identity() {
        let store = Sharded::new(MemoryExpiry::ClockDriven);
        put(&store, "old", 1);
        store.generation.store(u64::MAX, Ordering::Release);
        assert_eq!(store.begin_clear(), u64::MAX);
        assert_eq!(
            put(&store, "next", 2).admission,
            MemoryAdmission::Rejected(CapacityRejection::GenerationExhausted)
        );
        for shard in 0..store.shard_count() {
            store.drain_before(shard, u64::MAX);
        }
        assert_eq!(store.usage().entries, 0);
    }
    #[test]
    fn keys_sharing_a_section_are_not_aliased_by_the_precomputed_hash() {
        let store = Sharded::new(MemoryExpiry::ClockDriven);
        let mut found = std::collections::HashMap::new();
        let (a, b) = (0..=SHARDS)
            .find_map(|n| {
                let key = format!("key-{n}");
                let (hash, _) = store.route(&key);
                let section = hash as usize & (SHARDS - 1);
                found.insert(section, key.clone()).map(|old| (old, key))
            })
            .unwrap();
        put(&store, &a, 11);
        put(&store, &b, 22);
        for (key, value) in [(a, 11), (b, 22)] {
            let MemoryRead::Present(entry) = store.get(&key, Some(Timestamp::from_ticks(0))) else {
                panic!("key disappeared")
            };
            assert_eq!(*entry.value(), value);
        }
    }
    #[test]
    fn an_invalidated_slot_is_absent_for_admission_and_retires_with_removed_reason() {
        let store = Sharded::new(MemoryExpiry::ClockDriven);
        let old = entry(1);
        store.insert(
            Arc::from("k"),
            old.clone(),
            Timestamp::from_ticks(0),
            Expected::Any,
            CaptureAdmission::Armed,
            MemoryWriteEvent::Set,
        );
        store.begin_clear();
        let rejected = store.insert(
            Arc::from("k"),
            entry(2),
            Timestamp::from_ticks(0),
            Expected::Same(&old),
            CaptureAdmission::Armed,
            MemoryWriteEvent::Set,
        );
        assert_eq!(
            rejected.admission,
            MemoryAdmission::Rejected(CapacityRejection::VersionChanged)
        );
        let committed = store.insert(
            Arc::from("k"),
            entry(3),
            Timestamp::from_ticks(0),
            Expected::Absent,
            CaptureAdmission::Armed,
            MemoryWriteEvent::Set,
        );
        assert_eq!(committed.admission, MemoryAdmission::Admitted);
        assert_eq!(committed.retired.len(), 1);
        assert_eq!(
            committed.retired[0].reason.fact(),
            Some(crate::MemoryEvictionReason::Removed)
        );
        assert_eq!(*committed.retired[0].entry.value(), 1);
    }
}
