//! Active origin revisions live beside the entries they protect. No idle
//! tombstone is retained; explicit mutations advance only an active revision.
use super::{Backend, Expected};
use crate::Result;
use crate::entry::Entry;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};

#[derive(Default)]
pub(super) struct Origins {
    active: HashMap<Arc<str>, ActiveRevision>,
}
struct ActiveRevision {
    revision: Weak<dyn RevisionSource>,
    snapshots: usize,
}
/// Only private metadata implementations participate; methods run under storage
/// coordination and must never invoke user code or retire values.
pub(crate) trait RevisionSource: Send + Sync {
    fn current(&self) -> u64;
    fn advance(&self);
}
pub(super) struct Revision(AtomicU64);
impl RevisionSource for Revision {
    fn current(&self) -> u64 {
        self.0.load(Ordering::Acquire)
    }
    fn advance(&self) {
        // MAX is terminal and never matches a valid origin snapshot.
        self.0
            .store(self.current().saturating_add(1), Ordering::Release);
    }
}
impl Origins {
    #[cfg(test)]
    pub(super) fn capture(&mut self, key: &Arc<str>) -> Result<(Arc<dyn RevisionSource>, u64)> {
        self.capture_from(key, None)
    }
    pub(super) fn capture_from(
        &mut self,
        key: &Arc<str>,
        provided: Option<Arc<dyn RevisionSource>>,
    ) -> Result<(Arc<dyn RevisionSource>, u64)> {
        let revision = match self
            .active
            .get(key.as_ref())
            .and_then(|active| active.revision.upgrade())
        {
            Some(revision) => revision,
            None => provided.unwrap_or_else(|| Arc::new(Revision(AtomicU64::new(1)))),
        };
        let captured = revision.current();
        if captured == u64::MAX {
            return Err(crate::RecoveryError::GenerationExhausted.into());
        }
        match self.active.get_mut(key.as_ref()) {
            Some(active) if active.revision.ptr_eq(&Arc::downgrade(&revision)) => {
                active.snapshots += 1
            }
            Some(active) => {
                *active = ActiveRevision {
                    revision: Arc::downgrade(&revision),
                    snapshots: 1,
                }
            }
            None => {
                self.active.insert(
                    Arc::clone(key),
                    ActiveRevision {
                        revision: Arc::downgrade(&revision),
                        snapshots: 1,
                    },
                );
            }
        }
        Ok((revision, captured))
    }
    pub(super) fn advance(&self, key: &str) {
        if self.active.is_empty() {
            return;
        }
        if let Some(revision) = self
            .active
            .get(key)
            .and_then(|active| active.revision.upgrade())
        {
            revision.advance();
        }
    }
    pub(super) fn forget(&mut self, key: &str, revision: &Arc<dyn RevisionSource>) {
        if let Some(active) = self.active.get_mut(key)
            && active.revision.ptr_eq(&Arc::downgrade(revision))
        {
            active.snapshots -= 1;
            if active.snapshots == 0 {
                self.active.remove(key);
            }
        }
    }
}

pub(crate) struct MemoryOrigin<V> {
    key: Arc<str>,
    backend: Backend<V>,
    revision: Arc<dyn RevisionSource>,
    captured: u64,
    generation: u64,
}
impl<V> MemoryOrigin<V> {
    pub(super) fn new(
        key: Arc<str>,
        backend: Backend<V>,
        revision: Arc<dyn RevisionSource>,
        captured: u64,
        generation: u64,
    ) -> Self {
        Self {
            key,
            backend,
            revision,
            captured,
            generation,
        }
    }
    pub(super) fn matches(&self, generation: u64) -> bool {
        generation != u64::MAX
            && self.generation == generation
            && self.revision.current() == self.captured
    }
    pub(crate) fn is_current(&self) -> bool {
        let generation = match &self.backend {
            Backend::Unbounded(store) => store.origin_generation(),
            Backend::Retained(store) => super::lock(store).generation,
        };
        self.matches(generation)
    }
}
impl<V> Drop for MemoryOrigin<V> {
    fn drop(&mut self) {
        match &self.backend {
            Backend::Unbounded(store) => store.forget_origin(&self.key, &self.revision),
            Backend::Retained(store) => {
                super::lock(store).origins.forget(&self.key, &self.revision)
            }
        }
    }
}

impl<V> Expected<'_, V> {
    pub(super) fn changes_origin(&self) -> bool {
        match self {
            Self::Mutation | Self::MutationOf(_) | Self::Origin(_) => true,
            Self::Any | Self::Absent | Self::Same(_) => false,
        }
    }
    pub(super) fn matches_generation(&self, current: Option<&Entry<V>>, generation: u64) -> bool {
        match self {
            Self::Origin(origin) => origin.matches(generation),
            Self::Mutation => true,
            Self::MutationOf(expected) => {
                current.is_some_and(|entry| entry.is_same_instance(expected))
            }
            Self::Any | Self::Absent | Self::Same(_) => self.matches(current),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EntryOptions, Events, ManualClock, MemoryLimits, MemoryStore, Timestamp};
    use std::time::Duration;

    fn store(bounded: bool) -> MemoryStore<u64> {
        MemoryStore::with_clock(
            MemoryLimits::new(bounded.then_some(2), None),
            Events::default(),
            Arc::new(ManualClock::new(Timestamp::from_ticks(10_000))),
        )
    }
    fn entry(value: u64) -> Entry<u64> {
        Entry::try_fresh_at(
            value,
            &EntryOptions::new(Duration::from_secs(60)),
            Timestamp::from_ticks(10_000),
            Timestamp::from_ticks(10_000),
            Box::new([]),
            None,
            None,
        )
        .unwrap()
    }
    #[test]
    fn mutation_and_factory_version_are_compared_in_the_storage_commit() {
        for bounded in [false, true] {
            let store = store(bounded);
            let key: Arc<str> = Arc::from("same");
            let origin = store.capture_origin(key.clone()).unwrap();
            let explicit =
                store.prepare_insert(key.clone(), entry(2), Timestamp::from_ticks(10_000));
            let committed = store.apply_insert(explicit);
            store.finish_insert(committed);
            let late = store.prepare_insert(key, entry(1), Timestamp::from_ticks(10_000));
            let committed = store.apply_origin(late, &origin);
            assert_eq!(
                store.finish_insert(committed),
                super::super::MemoryAdmission::Rejected(crate::CapacityRejection::VersionChanged)
            );
            assert_eq!(
                *store
                    .ready_at_for_mutation("same", Timestamp::from_ticks(10_000))
                    .unwrap()
                    .value(),
                2
            );
        }
    }
    #[test]
    fn an_expired_old_factory_does_not_invalidate_a_newer_origin() {
        for bounded in [false, true] {
            let store = store(bounded);
            let now = Timestamp::from_ticks(10_000);
            let key: Arc<str> = Arc::from("same");
            let old = store.capture_origin(key.clone()).unwrap();
            let explicit = store.prepare_insert(key.clone(), entry(2), now);
            store.finish_insert(store.apply_insert(explicit));
            let newer = store.capture_origin(key.clone()).unwrap();
            let expired = Entry::try_fresh_at(
                1,
                &EntryOptions::new(Duration::ZERO),
                now,
                now,
                Box::new([]),
                None,
                None,
            )
            .unwrap();
            let write = store.prepare_insert(key.clone(), expired, now);
            assert_eq!(
                store.finish_insert(store.apply_origin(write, &old)),
                super::super::MemoryAdmission::Rejected(
                    crate::CapacityRejection::PhysicallyExpired
                )
            );
            let write = store.prepare_insert(key, entry(3), now);
            assert_eq!(
                store.finish_insert(store.apply_origin(write, &newer)),
                super::super::MemoryAdmission::Replaced
            );
        }
    }
    #[test]
    fn terminal_revision_is_never_reused_and_idle_keys_leave_no_tombstone() {
        let mut origins = Origins::default();
        for id in 0..1000 {
            let key: Arc<str> = Arc::from(format!("key-{id}"));
            let (revision, captured) = origins.capture(&key).unwrap();
            assert_eq!(captured, 1);
            origins.forget(&key, &revision);
            assert!(origins.active.is_empty());
        }
        let key: Arc<str> = Arc::from("terminal");
        let revision = Arc::new(Revision(AtomicU64::new(u64::MAX - 1)));
        origins.capture_from(&key, Some(revision.clone())).unwrap();
        origins.advance(&key);
        origins.advance(&key);
        assert_eq!(revision.current(), u64::MAX);
        assert!(matches!(
            origins.capture(&key),
            Err(crate::Error::Recovery(
                crate::RecoveryError::GenerationExhausted
            ))
        ));
    }
}
