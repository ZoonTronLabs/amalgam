//! Host adapter preserving built-in storage and fallible supplied storage.
use super::{
    MemoryAdmission, MemoryExpiry, MemoryObserver, MemoryStore, MemoryWriteEvent, ReclamationFence,
    ReclamationGuard, Retirement, RetirementReason,
};
use crate::entry::Entry;
use crate::memory_storage::{MemoryStorageError, RecordInner};
use crate::{
    Events, Result, Timestamp, advanced::EvictionCapture, advanced::MemoryEvictionReason,
    advanced::MemoryEvictions, provider::CapacityRejection, provider::Clock,
    provider::MemoryCondition, provider::MemoryLimits, provider::MemoryRecord,
    provider::MemoryRetirement, provider::MemoryStorage, provider::MemoryStorageEpoch,
    provider::MemoryStorageWrite, provider::MemoryUsage,
};
use std::sync::Arc;

// Storage keeps its closed failure family; cache operations adapt it once.
type StorageResult<T> = std::result::Result<T, MemoryStorageError>;

#[derive(Clone)]
pub(crate) enum CacheMemory<V: Clone + Send + Sync + 'static> {
    Builtin(MemoryStore<V>),
    Supplied(Box<Supplied<V>>),
}
#[derive(Clone)]
pub(crate) struct Supplied<V: Clone + Send + Sync + 'static> {
    provider: Arc<dyn MemoryStorage<V>>,
    epoch: MemoryStorageEpoch,
    observer: MemoryObserver<V>,
    clock: Arc<dyn Clock>,
    expiry: MemoryExpiry,
}
impl<V: Clone + Send + Sync + 'static> CacheMemory<V> {
    pub(crate) fn new(
        provider: Option<Arc<dyn MemoryStorage<V>>>,
        limits: MemoryLimits,
        events: Events,
        clock: &crate::time::local::CacheClock,
        expiry: MemoryExpiry,
        capture: EvictionCapture,
        prefix: Option<&str>,
    ) -> Result<Self> {
        match provider {
            None => Ok(Self::Builtin(
                MemoryStore::with_cache_clock(limits, events, clock, expiry)
                    .with_eviction_capture(capture),
            )),
            Some(provider) => Self::new_for_namespace(
                Some(provider),
                limits,
                events,
                clock.shared(),
                expiry,
                capture,
                crate::provider::MemoryNamespace::new(prefix),
            ),
        }
    }
    pub(crate) fn new_for_namespace(
        provider: Option<Arc<dyn MemoryStorage<V>>>,
        limits: MemoryLimits,
        events: Events,
        clock: Arc<dyn Clock>,
        expiry: MemoryExpiry,
        capture: EvictionCapture,
        namespace: crate::provider::MemoryNamespace,
    ) -> Result<Self> {
        match provider {
            None => Ok(Self::Builtin(
                MemoryStore::with_clock_and_expiry(limits, events, clock, expiry)
                    .with_eviction_capture(capture),
            )),
            Some(provider) => {
                if limits != MemoryLimits::default() {
                    return Err(crate::ConfigError::SuppliedMemoryWithBuiltinLimits.into());
                }
                let epoch = provider.epoch(&namespace);
                if !epoch.same(&provider.epoch(&namespace)) {
                    return Err(crate::ConfigError::UnstableMemoryStorageEpoch.into());
                }
                if namespace.purpose() != crate::provider::MemoryNamespacePurpose::Values
                    && epoch.same(&provider.epoch(&crate::provider::MemoryNamespace::new(Some(
                        namespace.key_prefix(),
                    ))))
                {
                    return Err(crate::ConfigError::AliasedMarkerMemoryStorageEpoch.into());
                }
                epoch.current()?;
                Ok(Self::Supplied(Box::new(Supplied {
                    provider,
                    epoch,
                    observer: MemoryObserver::new(events, capture),
                    clock,
                    expiry,
                })))
            }
        }
    }
    #[inline]
    fn observer(&self) -> &MemoryObserver<V> {
        match self {
            Self::Builtin(store) => &store.observer,
            Self::Supplied(store) => &store.observer,
        }
    }
    #[inline]
    pub(crate) fn for_operation(&self) -> Self {
        match self {
            Self::Builtin(store) => Self::Builtin(store.for_operation()),
            Self::Supplied(store) => {
                let mut store = (**store).clone();
                store.observer = store.observer.for_operation();
                Self::Supplied(Box::new(store))
            }
        }
    }
    pub(crate) fn provider(&self) -> Option<&Arc<dyn MemoryStorage<V>>> {
        match self {
            Self::Builtin(_) => None,
            Self::Supplied(store) => Some(&store.provider),
        }
    }
    pub(crate) fn fence(&self) -> Option<Arc<dyn ReclamationFence>> {
        self.observer().fence()
    }
    pub(crate) fn guard<G>(&self, guard: G) -> ReclamationGuard<G> {
        self.observer().guard(guard)
    }
    pub(crate) fn component_read(&self, component: crate::advanced::ComponentRead) {
        self.observer().component_read(component);
    }
    pub(crate) fn emit(&self, event: crate::CacheEvent) {
        self.observer().emit(event);
    }
    pub(crate) fn emit_lazy(&self, make: impl FnOnce() -> crate::CacheEvent) {
        self.observer().emit_lazy(make);
    }
    pub(crate) fn emit_layer_lazy(&self, make: impl FnOnce() -> crate::advanced::LayerEvent) {
        self.observer().emit_layer_lazy(make);
    }
    #[inline]
    pub(crate) async fn get_at(
        &self,
        key: &str,
        now: Timestamp,
    ) -> StorageResult<Option<Entry<V>>> {
        match self {
            Self::Builtin(store) => Ok(store.get_at(key, now).await),
            Self::Supplied(store) => store.get_at(key, now),
        }
    }
    #[inline]
    pub(crate) fn with_ready<R>(
        &self,
        key: &str,
        now: Timestamp,
        read: impl FnOnce(&Entry<V>, crate::entry::Freshness) -> R,
    ) -> StorageResult<Option<R>> {
        match self {
            Self::Builtin(store) => Ok(store.with_ready(key, now, read)),
            Self::Supplied(store) => Ok(store
                .ready_at(key, now)?
                .as_ref()
                .map(|entry| read(entry, entry.freshness(now)))),
        }
    }
    pub(crate) fn ready_at(&self, key: &str, now: Timestamp) -> StorageResult<Option<Entry<V>>> {
        self.with_ready(key, now, |entry, _| entry.clone())
    }
    pub(crate) async fn insert_at(
        &self,
        key: Arc<str>,
        entry: Entry<V>,
        now: Timestamp,
    ) -> StorageResult<MemoryAdmission> {
        match self {
            Self::Builtin(store) => Ok(store.insert_at(key, entry, now).await),
            Self::Supplied(store) => {
                store.insert(key, entry, now, MemoryCondition::Any, MemoryWriteEvent::Set)
            }
        }
    }
    pub(crate) async fn insert_if_unchanged(
        &self,
        key: Arc<str>,
        expected: Option<&Entry<V>>,
        entry: Entry<V>,
        now: Timestamp,
    ) -> StorageResult<MemoryAdmission> {
        match self {
            Self::Builtin(store) => Ok(store.insert_if_unchanged(key, expected, entry, now).await),
            Self::Supplied(store) => store.insert(
                key,
                entry,
                now,
                expected.map_or(MemoryCondition::Absent, MemoryCondition::Same),
                MemoryWriteEvent::Set,
            ),
        }
    }
    pub(crate) async fn expire_if_unchanged(
        &self,
        key: Arc<str>,
        expected: &Entry<V>,
        entry: Entry<V>,
        now: Timestamp,
    ) -> StorageResult<MemoryAdmission> {
        match self {
            Self::Builtin(store) => Ok(store.expire_if_unchanged(key, expected, entry, now).await),
            Self::Supplied(store) => store.insert(
                key,
                entry,
                now,
                MemoryCondition::Same(expected),
                MemoryWriteEvent::Expire,
            ),
        }
    }
    pub(crate) async fn remove(&self, key: &str) -> StorageResult<Option<Entry<V>>> {
        match self {
            Self::Builtin(store) => Ok(store.remove(key).await),
            Self::Supplied(store) => {
                let entry = store.remove(key, None)?;
                self.emit_layer_lazy(|| {
                    crate::advanced::LayerEvent::Memory(crate::advanced::MemoryEvent::Remove {
                        key: Arc::from(key),
                    })
                });
                Ok(entry)
            }
        }
    }
    pub(crate) async fn remove_if_same(
        &self,
        key: &str,
        expected: &Entry<V>,
    ) -> StorageResult<Option<Entry<V>>> {
        match self {
            Self::Builtin(store) => Ok(store.remove_if_same(key, expected).await),
            Self::Supplied(store) => store.remove(key, Some(expected)),
        }
    }
    pub(crate) fn begin_invalidation(&self) -> StorageResult<MemoryInvalidation<'_, V>> {
        match self {
            Self::Builtin(store) => Ok(MemoryInvalidation::Builtin(store)),
            Self::Supplied(store) => Ok(MemoryInvalidation::Supplied {
                store,
                barrier: store.epoch.advance()?,
            }),
        }
    }
    pub(crate) fn invalidate_all(&self) -> StorageResult<()> {
        self.begin_invalidation()?.finish()
    }
    pub(crate) async fn run_pending_tasks(&self) -> StorageResult<()> {
        match self {
            Self::Builtin(store) => {
                store.run_pending_tasks().await;
                Ok(())
            }
            Self::Supplied(store) => {
                for retired in store.provider.maintain(store.clock.now())? {
                    store.retire(retired);
                }
                Ok(())
            }
        }
    }
    pub(crate) async fn maintain_step(&self) -> StorageResult<()> {
        match self {
            Self::Builtin(store) => {
                store.maintain_step();
                Ok(())
            }
            Self::Supplied(_) => self.run_pending_tasks().await,
        }
    }
    pub(crate) fn usage(&self) -> StorageResult<MemoryUsage> {
        match self {
            Self::Builtin(store) => Ok(store.usage()),
            Self::Supplied(store) => Ok(store.provider.usage()?),
        }
    }
    pub(crate) fn evictions(&self) -> &MemoryEvictions<V> {
        &self.observer().evictions
    }
}
pub(crate) enum MemoryInvalidation<'a, V: Clone + Send + Sync + 'static> {
    Builtin(&'a MemoryStore<V>),
    Supplied {
        store: &'a Supplied<V>,
        barrier: crate::provider::MemoryGeneration,
    },
}
impl<V: Clone + Send + Sync + 'static> MemoryInvalidation<'_, V> {
    pub(crate) fn finish(self) -> StorageResult<()> {
        match self {
            Self::Builtin(store) => {
                store.invalidate_all();
                Ok(())
            }
            Self::Supplied { store, barrier } => store.clear_before(barrier),
        }
    }
}
impl<V: Clone + Send + Sync + 'static> Supplied<V> {
    fn validate_record(&self, key: &str, record: &MemoryRecord<V>) -> StorageResult<()> {
        let violation = if record.key() != key {
            Some(crate::provider::MemoryRecordViolation::Key)
        } else if !record.inner.generation.epoch.same(&self.epoch) {
            Some(crate::provider::MemoryRecordViolation::Namespace)
        } else {
            None
        };
        match violation {
            Some(violation) => Err(MemoryStorageError::InvalidRecord { violation }),
            None => Ok(()),
        }
    }
    fn keep_owner(&self, record: &MemoryRecord<V>) {
        self.observer
            .reclamation
            .retain_observer(record.inner.owner.evictions.clone());
    }
    fn retire_as(&self, record: MemoryRecord<V>, reason: RetirementReason) {
        self.keep_owner(&record);
        let mut target = record.inner.owner.clone();
        target.reclamation = self.observer.reclamation.clone();
        target.retire(Retirement {
            key: Arc::clone(&record.inner.key),
            entry: record.entry().clone(),
            reason,
            capture: record.inner.capture,
        });
    }
    fn retire(&self, retirement: MemoryRetirement<V>) {
        let reason = match retirement.reason {
            MemoryEvictionReason::Removed => RetirementReason::Explicit,
            MemoryEvictionReason::Replaced => RetirementReason::Replaced,
            MemoryEvictionReason::Expired => RetirementReason::Expired,
            MemoryEvictionReason::Capacity => RetirementReason::Capacity,
        };
        self.retire_as(retirement.record, reason);
    }
    fn dead_reason(record: &MemoryRecord<V>) -> RetirementReason {
        if !record.inner.generation.is_current() {
            RetirementReason::Continuity
        } else if !record.entry().is_read_eligible() {
            RetirementReason::Eligibility
        } else {
            RetirementReason::Expired
        }
    }
    #[inline(never)]
    fn get_at(&self, key: &str, now: Timestamp) -> StorageResult<Option<Entry<V>>> {
        self.observer
            .component_read(crate::advanced::ComponentRead::Memory);
        self.epoch.current()?;
        let Some(record) = self.provider.get(key)? else {
            return Ok(None);
        };
        self.validate_record(key, &record)?;
        self.keep_owner(&record);
        if record.is_live_at(now) {
            return Ok(Some(record.entry().clone()));
        }
        self.observer.reclamation.retain(record.entry().clone());
        if let Some(removed) = self.provider.remove(key, Some(record.entry()))? {
            self.retire_as(removed, Self::dead_reason(&record));
        }
        Ok(None)
    }
    #[inline(never)]
    fn ready_at(&self, key: &str, now: Timestamp) -> StorageResult<Option<Entry<V>>> {
        self.observer
            .component_read(crate::advanced::ComponentRead::Memory);
        self.epoch.current()?;
        let Some(record) = self.provider.try_get(key)? else {
            return Ok(None);
        };
        self.validate_record(key, &record)?;
        self.keep_owner(&record);
        Ok(record.is_live_at(now).then(|| record.entry().clone()))
    }
    fn remove(&self, key: &str, expected: Option<&Entry<V>>) -> StorageResult<Option<Entry<V>>> {
        let Some(record) = self.provider.remove(key, expected)? else {
            return Ok(None);
        };
        let entry = record.entry().clone();
        self.retire_as(record, RetirementReason::Explicit);
        Ok(Some(entry))
    }
    fn clear_before(&self, barrier: crate::provider::MemoryGeneration) -> StorageResult<()> {
        for record in self.provider.clear_before(&barrier)? {
            self.retire_as(record, RetirementReason::Explicit);
        }
        Ok(())
    }
    fn rejected(&self, key: Arc<str>, reason: CapacityRejection) -> MemoryAdmission {
        self.observer
            .emit(crate::CacheEvent::MemoryAdmissionRejected { key, reason });
        MemoryAdmission::Rejected(reason)
    }
    fn insert(
        &self,
        key: Arc<str>,
        entry: Entry<V>,
        now: Timestamp,
        condition: MemoryCondition<'_, V>,
        event: MemoryWriteEvent,
    ) -> StorageResult<MemoryAdmission> {
        // Caller and adjusted values survive every exit through coordination.
        self.observer.reclamation.retain(entry.clone());
        if entry.is_physically_expired(now) {
            return Ok(self.rejected(key, CapacityRejection::PhysicallyExpired));
        }
        let candidate = self.prepare_record(Arc::clone(&key), entry, now)?;
        let outcome = self.provider.insert(candidate, condition, now)?;
        let admission = self.finish_insert(outcome, event);
        self.observe_insert(key, admission, event);
        Ok(admission)
    }
    fn prepare_record(
        &self,
        key: Arc<str>,
        entry: Entry<V>,
        now: Timestamp,
    ) -> StorageResult<MemoryRecord<V>> {
        let prepared = entry.at_insertion(now);
        self.observer.reclamation.retain(prepared.clone());
        let generation = self.epoch.current()?;
        let expires = self.storage_deadline(&prepared);
        Ok(MemoryRecord {
            inner: Arc::new(RecordInner {
                key,
                entry: prepared,
                generation,
                expires,
                owner: self.observer.for_owner(),
                capture: self.observer.capture_admission(),
            }),
        })
    }
    fn storage_deadline(&self, entry: &Entry<V>) -> Option<std::time::Instant> {
        match self.expiry {
            MemoryExpiry::ClockDriven => None,
            MemoryExpiry::RealTime => std::time::Instant::now().checked_add(entry.backend_ttl()),
        }
    }
    fn replace_record(
        &self,
        previous: MemoryRecord<V>,
        event: MemoryWriteEvent,
    ) -> MemoryAdmission {
        self.retire_as(
            previous,
            match event {
                MemoryWriteEvent::Set => RetirementReason::Replaced,
                MemoryWriteEvent::Expire => RetirementReason::Metadata,
            },
        );
        MemoryAdmission::Replaced
    }
    fn finish_insert(
        &self,
        outcome: MemoryStorageWrite<V>,
        event: MemoryWriteEvent,
    ) -> MemoryAdmission {
        let (admission, retired) = match outcome {
            MemoryStorageWrite::Admitted { retired } => (MemoryAdmission::Admitted, retired),
            MemoryStorageWrite::Replaced { previous, retired } => {
                (self.replace_record(previous, event), retired)
            }
            MemoryStorageWrite::Rejected { reason, retired } => {
                (MemoryAdmission::Rejected(reason), retired)
            }
        };
        for retired in retired {
            self.retire(retired);
        }
        admission
    }
    fn observe_insert(&self, key: Arc<str>, admission: MemoryAdmission, event: MemoryWriteEvent) {
        match admission {
            MemoryAdmission::Rejected(reason) => {
                self.rejected(key, reason);
            }
            MemoryAdmission::Admitted | MemoryAdmission::Replaced => {
                self.observer.emit_layer_lazy(|| {
                    crate::advanced::LayerEvent::Memory(match event {
                        MemoryWriteEvent::Set => crate::advanced::MemoryEvent::Set { key },
                        MemoryWriteEvent::Expire => crate::advanced::MemoryEvent::Expire { key },
                    })
                })
            }
        }
    }
}
