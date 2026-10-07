//! Concurrent L1 storage with absolute expiry and optional capacity limits.
//! Retired entries leave backend guards before observation or reclamation.
mod custom;
mod deadlines;
mod origin;
pub(crate) use origin::RevisionSource;
pub(crate) use origin::{BorrowedMemoryOrigin, MemoryOrigin};
use origin::{Origins, RevisionSnapshot};
mod reclamation;
mod sharded;
use crate::entry::Entry;
use crate::events::{
    CacheEvent, Events, EvictionCapture, LayerEvent, MemoryEvent, MemoryEvictionReason,
    MemoryEvictions,
};
use crate::options::Priority;
use crate::time::{Clock, Timestamp};
pub(crate) use custom::{CacheMemory, MemoryInvalidation};
use reclamation::Reclamation;
pub(crate) use reclamation::{ReclamationFence, ReclamationGuard};
use sharded::Sharded;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;

/// Independent entry-count and application-defined weight budgets.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MemoryLimits {
    entries: Option<u64>,
    weight: Option<u64>,
}

impl MemoryLimits {
    /// Creates independent limits. None is unbounded; zero admits no positive
    /// count/weight under that limit.
    #[must_use]
    pub const fn new(entries: Option<u64>, weight: Option<u64>) -> Self {
        Self { entries, weight }
    }
    /// The optional entry-count limit.
    #[must_use]
    pub const fn entries(self) -> Option<u64> {
        self.entries
    }
    /// The optional weight limit.
    #[must_use]
    pub const fn weight(self) -> Option<u64> {
        self.weight
    }

    fn fits(self, entries: usize, weight: u128) -> bool {
        self.entries
            .is_none_or(|limit| entries as u128 <= u128::from(limit))
            && self.weight.is_none_or(|limit| weight <= u128::from(limit))
    }
}

/// Selects physical storage cleanup independently of logical freshness.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryExpiry {
    /// Only the injected clock determines expiration. Reads and maintenance
    /// remove dead values; real elapsed time cannot remove a frozen-clock value.
    ClockDriven,
    /// Storage also expires after real elapsed TTL. Appropriate for SystemClock
    /// production storage; explicitly opting in permits time-based eviction.
    RealTime,
}

/// The explicit result of attempting an L1 admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryAdmission {
    /// A new entry was admitted.
    Admitted,
    /// An existing entry was atomically replaced.
    Replaced,
    /// Existing valid contents were preserved.
    Rejected(CapacityRejection),
}

/// Why a memory candidate could not be admitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapacityRejection {
    /// The candidate alone exceeds a configured limit.
    Oversized,
    /// Higher-priority or pinned entries prevent admission.
    ProtectedCapacity,
    /// The candidate is already physically dead.
    PhysicallyExpired,
    /// A conditional insert no longer refers to the current representation.
    VersionChanged,
    /// The store exhausted its non-reusable clear generation.
    GenerationExhausted,
}

/// A diagnostic snapshot of retained entry count and weight.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryUsage {
    /// The number of physically retained entries.
    pub entries: u64,
    /// The sum of application-defined weights, without u32 truncation.
    pub weight: u128,
}

#[derive(Clone, Copy)]
enum MemoryWriteEvent {
    Set,
    Expire,
}
#[derive(Clone, Copy)]
pub(crate) enum CaptureAdmission {
    Armed,
    Unarmed,
}
/// Capture at insertion and willingness to observe a later retirement are
/// independent inputs. Only an unarmed insertion policy can discard metadata.
#[derive(Clone, Copy)]
struct CapturePolicy {
    admission: CaptureAdmission,
    timing: EvictionCapture,
}
impl CapturePolicy {
    fn permits_payload_only(self, original: CaptureAdmission) -> bool {
        self.timing == EvictionCapture::AtInsertion && matches!(original, CaptureAdmission::Unarmed)
    }
}
#[derive(Clone, Copy)]
enum RetirementReason {
    Explicit,
    Replaced,
    Expired,
    Capacity,
    Eligibility,
    Continuity,
    Metadata,
}
impl RetirementReason {
    fn fact(self) -> Option<MemoryEvictionReason> {
        match self {
            Self::Explicit | Self::Eligibility | Self::Continuity => {
                Some(MemoryEvictionReason::Removed)
            }
            Self::Replaced => Some(MemoryEvictionReason::Replaced),
            Self::Expired => Some(MemoryEvictionReason::Expired),
            Self::Capacity => Some(MemoryEvictionReason::Capacity),
            Self::Metadata => None,
        }
    }
    fn logical(self) -> bool {
        matches!(self, Self::Expired | Self::Capacity | Self::Eligibility)
    }
}
enum Backend<V> {
    Unbounded(Arc<Sharded<V>>),
    Retained(Arc<Mutex<Retention<V>>>),
}
impl<V> Clone for Backend<V> {
    fn clone(&self) -> Self {
        match self {
            Self::Unbounded(store) => Self::Unbounded(Arc::clone(store)),
            Self::Retained(store) => Self::Retained(Arc::clone(store)),
        }
    }
}

/// The L1 store. Observation and destruction occur after backend guards.
#[derive(Clone)]
pub struct MemoryStore<V: Clone + Send + Sync + 'static> {
    backend: Backend<V>,
    clock: Option<Arc<dyn Clock>>,
    observer: MemoryObserver<V>,
}
impl<V: Clone + Send + Sync + 'static> MemoryStore<V> {
    /// Legacy entry-count configuration with real-time storage expiry.
    #[must_use]
    pub fn new(max_capacity: Option<u64>, events: Events) -> Self {
        Self::create(
            MemoryLimits::new(max_capacity, None),
            events,
            None,
            MemoryExpiry::RealTime,
        )
    }
    /// Uses the injected clock exclusively for physical expiration.
    #[must_use]
    pub fn with_clock(limits: MemoryLimits, events: Events, clock: Arc<dyn Clock>) -> Self {
        Self::with_clock_and_expiry(limits, events, clock, MemoryExpiry::ClockDriven)
    }
    /// Explicitly selects the physical storage cleanup contract.
    #[must_use]
    pub fn with_clock_and_expiry(
        limits: MemoryLimits,
        events: Events,
        clock: Arc<dyn Clock>,
        expiry: MemoryExpiry,
    ) -> Self {
        Self::create(limits, events, Some(clock), expiry)
    }
    pub(crate) fn with_cache_clock(
        limits: MemoryLimits,
        events: Events,
        clock: &crate::time::local::CacheClock,
        expiry: MemoryExpiry,
    ) -> Self {
        if limits == MemoryLimits::default()
            && let crate::time::local::CacheClock::Local(local) = clock
        {
            return Self {
                backend: Backend::Unbounded(Arc::new(Sharded::with_local_clock(Arc::clone(local)))),
                clock: Some(clock.shared()),
                observer: MemoryObserver::new(events, EvictionCapture::AtInsertion),
            };
        }
        Self::with_clock_and_expiry(limits, events, clock.shared(), expiry)
    }
    fn create(
        limits: MemoryLimits,
        events: Events,
        clock: Option<Arc<dyn Clock>>,
        mode: MemoryExpiry,
    ) -> Self {
        let backend = if limits == MemoryLimits::default() {
            Backend::Unbounded(Arc::new(Sharded::new(mode)))
        } else {
            Backend::Retained(Arc::new(Mutex::new(Retention::new(limits, mode))))
        };
        Self {
            backend,
            clock,
            observer: MemoryObserver::new(events, EvictionCapture::AtInsertion),
        }
    }
    /// Selects insertion-time or retirement-time original-value observation.
    #[must_use]
    pub fn with_eviction_capture(mut self, capture: EvictionCapture) -> Self {
        self.observer.capture = capture;
        self
    }
    /// Independent bounded subscriptions retain the actual stored value.
    pub fn evictions(&self) -> &MemoryEvictions<V> {
        &self.observer.evictions
    }
    pub(crate) fn for_operation(&self) -> Self {
        let mut memory = self.clone();
        memory.observer.reclamation = Reclamation::operation();
        memory
    }
    #[cfg(test)]
    pub(crate) fn guard<G>(&self, guard: G) -> ReclamationGuard<G> {
        self.observer.guard(guard)
    }
    pub(crate) fn emit(&self, event: CacheEvent) {
        self.observer.emit(event);
    }
    pub(crate) fn emit_layer_lazy(&self, make: impl FnOnce() -> LayerEvent) {
        self.observer.emit_layer_lazy(make);
    }
    pub(crate) fn component_read(&self, component: crate::events::ComponentRead) {
        self.observer.component_read(component);
    }
    fn capture_admission(&self) -> CaptureAdmission {
        self.observer.capture_admission()
    }
    /// Reads under the selected physical expiration policy.
    pub async fn get(&self, key: &str) -> Option<Entry<V>> {
        let now = self.clock.as_ref().map(|clock| clock.now());
        self.read(key, now)
    }
    /// Reads at the supplied time; real-time expiry may independently retire data.
    pub async fn get_at(&self, key: &str, now: Timestamp) -> Option<Entry<V>> {
        self.read(key, Some(now))
    }
    fn read(&self, key: &str, now: Option<Timestamp>) -> Option<Entry<V>> {
        self.component_read(crate::events::ComponentRead::Memory);
        let read = match &self.backend {
            Backend::Unbounded(store) => store.get(key, now),
            Backend::Retained(store) => lock(store).get(key, now),
        };
        match read {
            MemoryRead::Present(entry) => Some(entry),
            MemoryRead::Absent => None,
            MemoryRead::Raced(entry) => {
                self.observer.reclamation.retain(entry);
                None
            }
            MemoryRead::Expired {
                key,
                entry,
                reason,
                capture,
            } => {
                self.retire(Retirement {
                    key,
                    entry,
                    reason,
                    capture,
                });
                None
            }
        }
    }
    /// The builtin map holds a reader slot through internal checks and ordinary
    /// value Clone. Observers run before locking; optional callbacks run after it.
    pub(crate) fn has_reader_slots(&self) -> bool {
        matches!(self.backend, Backend::Unbounded(_))
    }
    /// Private unbounded local reads share storage and shutdown publication.
    #[cfg(target_arch = "x86_64")]
    pub(crate) fn with_admitted_local_ready<'a, R>(
        &self,
        key: &str,
        reservation: crate::execution::DeferredInlinePermit<'a>,
        token: Option<&crate::FactoryCancellation>,
        read: impl FnOnce(&Entry<V>, crate::entry::Freshness) -> R,
    ) -> (crate::execution::InlinePermit<'a>, crate::Result<Option<R>>) {
        let Backend::Unbounded(store) = &self.backend else {
            unreachable!("the build-selected LocalSlots plan requires reader slots");
        };
        let (permit, result) = store.with_admitted_local_ready(key, reservation, token, read);
        // Optional observers run after the slot is released and within the
        // published operation, including subscriptions attached during Clone.
        if result.is_ok() {
            self.component_read(crate::events::ComponentRead::Memory);
        }
        (permit, result)
    }
    /// Internal primitive copies only: no observer executes before admission.
    pub(crate) fn with_callback_free_local_ready<R>(
        &self,
        key: &str,
        scopes: &crate::execution::Scopes,
        read: impl FnOnce(&Entry<V>, crate::entry::Freshness) -> R,
    ) -> crate::Result<Option<R>> {
        let Backend::Unbounded(store) = &self.backend else {
            unreachable!("callback-free plan requires builtin unbounded L1");
        };
        store.with_callback_free_local_ready(key, scopes, read)
    }
    pub(crate) fn with_local_ready<R>(
        &self,
        key: &str,
        clock: &crate::time::local::LocalClock,
        read: impl FnOnce(&Entry<V>, crate::entry::Freshness) -> R,
    ) -> Option<R> {
        match &self.backend {
            Backend::Unbounded(store) => {
                self.component_read(crate::events::ComponentRead::Memory);
                store.with_elapsed_ready(key, read)
            }
            Backend::Retained(_) => {
                let now = clock.now();
                self.with_ready(key, now, |entry| read(entry, entry.freshness(now)))
            }
        }
    }
    pub(crate) fn with_ready<R>(
        &self,
        key: &str,
        now: Timestamp,
        read: impl FnOnce(&Entry<V>) -> R,
    ) -> Option<R> {
        self.component_read(crate::events::ComponentRead::Memory);
        match &self.backend {
            Backend::Unbounded(store) => store.with_ready(key, now, read),
            Backend::Retained(store) => {
                // Capacity policy is still being migrated; contention must
                // already preserve a real hit rather than invoke a factory.
                let entry = {
                    let mut state = lock(store);
                    let stored = state.entries.get(key)?;
                    if stored.retirement_reason(Some(now)).is_some() {
                        return None;
                    }
                    let access = state.ticket();
                    let stored = state.entries.get_mut(key)?;
                    stored.access = access;
                    stored.entry.clone()
                };
                Some(read(&entry))
            }
        }
    }
    /// Legacy insertion; rejects explicitly without collateral eviction.
    pub async fn insert(&self, key: Arc<str>, entry: Entry<V>) {
        let now = self
            .clock
            .as_ref()
            .map_or(entry.meta().inserted_at(), |clock| clock.now());
        let _ = self.insert_at(key, entry, now).await;
    }
    /// Atomically admits or rejects a candidate.
    pub async fn insert_at(
        &self,
        key: Arc<str>,
        entry: Entry<V>,
        now: Timestamp,
    ) -> MemoryAdmission {
        self.insert_internal(key, entry, now, Expected::Any, MemoryWriteEvent::Set)
    }
    /// Commits only if the selected old representation remains current.
    pub async fn insert_if_unchanged(
        &self,
        key: Arc<str>,
        expected: Option<&Entry<V>>,
        entry: Entry<V>,
        now: Timestamp,
    ) -> MemoryAdmission {
        let expected = match expected {
            Some(entry) => Expected::Same(entry),
            None => Expected::Absent,
        };
        self.insert_internal(key, entry, now, expected, MemoryWriteEvent::Set)
    }
    pub(crate) async fn expire_if_unchanged(
        &self,
        key: Arc<str>,
        expected: &Entry<V>,
        entry: Entry<V>,
        now: Timestamp,
    ) -> MemoryAdmission {
        self.insert_internal(
            key,
            entry,
            now,
            Expected::Same(expected),
            MemoryWriteEvent::Expire,
        )
    }
    pub(crate) fn prepare_insert(
        &self,
        key: Arc<str>,
        entry: Entry<V>,
        now: Timestamp,
    ) -> PreparedWrite<'static, V> {
        self.prepare_write(StorageKey::Shared(key), entry, now, MemoryWriteEvent::Set)
    }
    pub(crate) fn prepare_expire(
        &self,
        key: Arc<str>,
        entry: Entry<V>,
        now: Timestamp,
    ) -> PreparedWrite<'static, V> {
        self.prepare_write(
            StorageKey::Shared(key),
            entry,
            now,
            MemoryWriteEvent::Expire,
        )
    }
    /// Standalone writes need no retained outer-coordinator envelope.
    pub(crate) fn insert_value_borrowed(
        &self,
        key: &str,
        value: crate::entry::FreshValue<V>,
        time: crate::time::local::WriteTime,
    ) -> MemoryAdmission {
        if self.observer.reclamation.is_deferred() {
            let prepared = self.prepare_value_borrowed(key, value, time);
            return self.finish_insert(self.apply_insert(prepared));
        }
        let mut entry = PreparedEntry::Fresh(value);
        let key = StorageKey::Borrowed(key);
        let commit = if !time.now().is_before(entry.meta().physical_expiration()) {
            self.invalidate_origin(&key);
            RetentionCommit::rejected(CapacityRejection::PhysicallyExpired, Vec::new())
        } else {
            let capture = self.capture_admission();
            match &self.backend {
                Backend::Unbounded(store) => store.insert_prepared(
                    &key,
                    &mut entry,
                    time,
                    Expected::Mutation,
                    CapturePolicy {
                        admission: capture,
                        timing: self.observer.capture,
                    },
                    MemoryWriteEvent::Set,
                ),
                Backend::Retained(store) => lock(store).insert(
                    &key,
                    &mut entry,
                    time.now(),
                    Expected::Mutation,
                    capture,
                    MemoryWriteEvent::Set,
                ),
            }
        };
        commit.retired.finish(&self.observer, &key);
        drop(entry);
        self.finish_admission(&key, MemoryWriteEvent::Set, commit.admission)
    }
    /// A default standalone replacement need not create an owned envelope.
    pub(crate) fn insert_plain_borrowed(
        &self,
        key: &str,
        value: V,
        metadata: crate::entry::PlainMetadata,
        time: crate::time::local::WriteTime,
    ) -> MemoryAdmission {
        if self.observer.reclamation.is_deferred() || matches!(self.backend, Backend::Retained(_)) {
            return self.insert_value_borrowed(key, metadata.into_fresh(value, Box::new([])), time);
        }
        let commit = if !time.now().is_before(metadata.physical()) {
            self.invalidate_origin(key);
            drop(value);
            RetentionCommit::rejected(CapacityRejection::PhysicallyExpired, Vec::new())
        } else {
            let Backend::Unbounded(store) = &self.backend else {
                unreachable!("plain insertion selected an unbounded backend");
            };
            store.insert_plain(
                key,
                value,
                metadata,
                time,
                CapturePolicy {
                    admission: self.capture_admission(),
                    timing: self.observer.capture,
                },
            )
        };
        let key = StorageKey::Borrowed(key);
        commit.retired.finish(&self.observer, &key);
        self.finish_admission(&key, MemoryWriteEvent::Set, commit.admission)
    }
    #[cfg(target_arch = "x86_64")]
    pub(crate) fn supports_shared_plain(&self) -> bool {
        matches!(self.backend, Backend::Unbounded(_)) && !self.observer.reclamation.is_deferred()
    }
    #[cfg(target_arch = "x86_64")]
    pub(crate) fn insert_admitted_plain<'a>(
        &self,
        key: &str,
        value: V,
        metadata: crate::entry::PlainMetadata,
        time: crate::time::local::WriteTime,
        reservation: crate::execution::DeferredInlinePermit<'a>,
    ) -> (
        crate::execution::InlinePermit<'a>,
        crate::Result<MemoryAdmission>,
    ) {
        debug_assert!(self.supports_shared_plain());
        debug_assert!(time.now().is_before(metadata.physical()));
        let Backend::Unbounded(store) = &self.backend else {
            unreachable!("shared publication requires unbounded builtin L1");
        };
        let (permit, commit) = store.insert_admitted_plain(
            key,
            value,
            metadata,
            time,
            CapturePolicy {
                admission: self.capture_admission(),
                timing: self.observer.capture,
            },
            reservation,
        );
        let result = commit.map(|commit| {
            let key = StorageKey::Borrowed(key);
            commit.retired.finish(&self.observer, &key);
            self.finish_admission(&key, MemoryWriteEvent::Set, commit.admission)
        });
        (permit, result)
    }
    fn finish_admission(
        &self,
        key: &StorageKey<'_>,
        event: MemoryWriteEvent,
        admission: MemoryAdmission,
    ) -> MemoryAdmission {
        match admission {
            MemoryAdmission::Rejected(reason) => self.rejected(key.shared(), reason),
            MemoryAdmission::Admitted | MemoryAdmission::Replaced => {
                self.emit_layer_lazy(|| {
                    LayerEvent::Memory(match event {
                        MemoryWriteEvent::Set => MemoryEvent::Set { key: key.shared() },
                        MemoryWriteEvent::Expire => MemoryEvent::Expire { key: key.shared() },
                    })
                });
                admission
            }
        }
    }

    pub(crate) fn prepare_value_borrowed<'a>(
        &self,
        key: &'a str,
        value: crate::entry::FreshValue<V>,
        time: crate::time::local::WriteTime,
    ) -> PreparedWrite<'a, V> {
        let now = time.now();
        if self.observer.reclamation.is_deferred() {
            return self.prepare_insert_borrowed(key, value.into_entry(), now);
        }
        PreparedWrite {
            key: StorageKey::Borrowed(key),
            entry: PreparedEntry::Fresh(value),
            original: None,
            retained: None,
            time,
            capture: self.capture_admission(),
            event: MemoryWriteEvent::Set,
        }
    }
    pub(crate) fn prepare_insert_borrowed<'a>(
        &self,
        key: &'a str,
        entry: Entry<V>,
        now: Timestamp,
    ) -> PreparedWrite<'a, V> {
        self.prepare_write(StorageKey::Borrowed(key), entry, now, MemoryWriteEvent::Set)
    }
    fn prepare_write<'a>(
        &self,
        key: StorageKey<'a>,
        entry: Entry<V>,
        now: Timestamp,
        event: MemoryWriteEvent,
    ) -> PreparedWrite<'a, V> {
        let (prepared, original) = if entry.meta().inserted_at() == now {
            (entry, None)
        } else {
            (entry.at_insertion(now), Some(entry))
        };
        let retained = self.observer.reclamation.pin_for_outer_guard(&prepared);
        PreparedWrite {
            key,
            entry: PreparedEntry::Candidate(prepared),
            retained,
            original,
            time: crate::time::local::WriteTime::Clock(now),
            event,
            capture: self.capture_admission(),
        }
    }
    pub(crate) fn capture_origin(&self, key: Arc<str>) -> crate::Result<MemoryOrigin<V>> {
        self.capture_origin_from(key, None)
    }
    pub(crate) fn capture_origin_from(
        &self,
        key: Arc<str>,
        revision: Option<Arc<dyn RevisionSource>>,
    ) -> crate::Result<MemoryOrigin<V>> {
        let snapshot = self.capture_revision(&key, revision)?;
        Ok(MemoryOrigin::new(key, self.backend.clone(), snapshot))
    }
    pub(crate) fn capture_borrowed_origin_from<'a>(
        &'a self,
        key: &'a Arc<str>,
        revision: Option<Arc<dyn RevisionSource>>,
    ) -> crate::Result<BorrowedMemoryOrigin<'a, V>> {
        let snapshot = self.capture_revision(key, revision)?;
        Ok(BorrowedMemoryOrigin::new(key, &self.backend, snapshot))
    }
    fn capture_revision(
        &self,
        key: &Arc<str>,
        revision: Option<Arc<dyn RevisionSource>>,
    ) -> crate::Result<RevisionSnapshot> {
        let (revision, captured, generation) = match &self.backend {
            Backend::Unbounded(store) => store.capture_origin_from(key, revision)?,
            Backend::Retained(store) => {
                let mut state = lock(store);
                if state.generation == u64::MAX {
                    return Err(crate::RecoveryError::GenerationExhausted.into());
                }
                let (revision, captured) = state.origins.capture_from(key, revision)?;
                (revision, captured, state.generation)
            }
        };
        Ok(RevisionSnapshot::new(revision, captured, generation))
    }
    pub(crate) fn skip_origin(&self, key: &str, origin: &MemoryOrigin<V>) -> bool {
        self.skip_revision(key, origin.snapshot())
    }
    pub(crate) fn skip_borrowed_origin(
        &self,
        key: &str,
        origin: &BorrowedMemoryOrigin<'_, V>,
    ) -> bool {
        self.skip_revision(key, origin.snapshot())
    }
    fn skip_revision(&self, key: &str, snapshot: &RevisionSnapshot) -> bool {
        match &self.backend {
            Backend::Unbounded(store) => store.skip_origin(key, snapshot),
            Backend::Retained(store) => {
                let state = lock(store);
                if snapshot.matches(state.generation) {
                    state.origins.advance(key);
                    true
                } else {
                    false
                }
            }
        }
    }
    pub(crate) fn invalidate_origin(&self, key: &str) {
        match &self.backend {
            Backend::Unbounded(store) => store.invalidate_origin(key),
            Backend::Retained(store) => lock(store).origins.advance(key),
        }
    }
    pub(crate) fn apply_insert<'a>(&self, write: PreparedWrite<'a, V>) -> MemoryCommit<'a, V> {
        self.apply_write(write, Expected::Mutation)
    }
    pub(crate) fn apply_origin<'a>(
        &self,
        write: PreparedWrite<'a, V>,
        origin: &MemoryOrigin<V>,
    ) -> MemoryCommit<'a, V> {
        self.apply_write(write, Expected::Origin(origin.snapshot()))
    }
    pub(crate) fn apply_borrowed_origin<'a>(
        &self,
        write: PreparedWrite<'a, V>,
        origin: &BorrowedMemoryOrigin<'_, V>,
    ) -> MemoryCommit<'a, V> {
        self.apply_write(write, Expected::Origin(origin.snapshot()))
    }
    pub(crate) fn apply_expire<'a>(
        &self,
        write: PreparedWrite<'a, V>,
        expected: &Entry<V>,
    ) -> MemoryCommit<'a, V> {
        self.apply_write(write, Expected::MutationOf(expected))
    }
    fn apply_write<'a>(
        &self,
        mut write: PreparedWrite<'a, V>,
        expected: Expected<'_, V>,
    ) -> MemoryCommit<'a, V> {
        let commit = if !write
            .time
            .now()
            .is_before(write.entry.meta().physical_expiration())
        {
            // A rejected old factory must not invalidate a newer origin.
            if matches!(expected, Expected::Mutation) {
                self.invalidate_origin(&write.key);
            }
            RetentionCommit::rejected(CapacityRejection::PhysicallyExpired, Vec::new())
        } else {
            match &self.backend {
                Backend::Unbounded(store) => store.insert_prepared(
                    &write.key,
                    &mut write.entry,
                    write.time,
                    expected,
                    CapturePolicy {
                        admission: write.capture,
                        timing: self.observer.capture,
                    },
                    write.event,
                ),
                Backend::Retained(store) => lock(store).insert(
                    &write.key,
                    &mut write.entry,
                    write.time.now(),
                    expected,
                    write.capture,
                    write.event,
                ),
            }
        };
        MemoryCommit { write, commit }
    }
    pub(crate) fn finish_insert(&self, completed: MemoryCommit<'_, V>) -> MemoryAdmission {
        let MemoryCommit { write, commit } = completed;
        let PreparedWrite {
            key,
            entry,
            original,
            retained,
            event,
            ..
        } = write;
        commit.retired.finish(&self.observer, &key);
        if let Some(original) = original {
            self.observer.reclamation.retain(original);
        }
        match entry {
            PreparedEntry::Candidate(entry) => self.observer.reclamation.retain(entry),
            PreparedEntry::Fresh(value) => drop(value),
            PreparedEntry::Stored => {}
        }
        if let Some(retained) = retained {
            self.observer.reclamation.retain(retained);
        }
        self.finish_admission(&key, event, commit.admission)
    }

    pub(crate) fn ready_at_for_mutation(&self, key: &str, now: Timestamp) -> Option<Entry<V>> {
        self.read(key, Some(now))
    }
    pub(crate) fn detach_remove(&self, key: &Arc<str>) -> DetachedRemoval<V> {
        let retired = match &self.backend {
            Backend::Unbounded(store) => store.remove_as_mutation(key),
            Backend::Retained(store) => {
                let mut state = lock(store);
                state.origins.advance(key);
                state
                    .remove(key)
                    .map(|stored| stored.retire(Arc::clone(key), RetirementReason::Explicit))
            }
        };
        DetachedRemoval {
            key: Arc::clone(key),
            retired,
        }
    }
    pub(crate) fn finish_remove(&self, removed: DetachedRemoval<V>) {
        if let Some(retired) = removed.retired {
            self.retire(retired);
        }
        self.emit_layer_lazy(|| LayerEvent::Memory(MemoryEvent::Remove { key: removed.key }));
    }
    fn insert_internal(
        &self,
        key: Arc<str>,
        entry: Entry<V>,
        now: Timestamp,
        expected: Expected<'_, V>,
        event: MemoryWriteEvent,
    ) -> MemoryAdmission {
        let prepared = self.prepare_write(StorageKey::Shared(key), entry, now, event);
        let committed = self.apply_write(prepared, expected);
        self.finish_insert(committed)
    }

    fn rejected(&self, key: Arc<str>, reason: CapacityRejection) -> MemoryAdmission {
        self.emit(CacheEvent::MemoryAdmissionRejected { key, reason });
        MemoryAdmission::Rejected(reason)
    }
    fn retire(&self, retired: Retirement<V>) {
        self.observer.retire(retired);
    }
    fn removed(&self, key: &str, expected: Option<&Entry<V>>) -> Option<Entry<V>> {
        let retired = match &self.backend {
            Backend::Unbounded(store) => store.remove(key, expected),
            Backend::Retained(store) => {
                let mut state = lock(store);
                if expected.is_some_and(|expected| {
                    !state
                        .entries
                        .get(key)
                        .is_some_and(|stored| stored.entry.is_same_instance(expected))
                }) {
                    return None;
                }
                state
                    .remove(key)
                    .map(|stored| stored.retire(Arc::from(key), RetirementReason::Explicit))
            }
        }?;
        let entry = retired.entry.clone();
        self.retire(retired);
        Some(entry)
    }
    /// Explicitly removes a value, including pinned data.
    pub async fn remove(&self, key: &str) -> Option<Entry<V>> {
        let removed = self.removed(key, None);
        self.emit_layer_lazy(|| {
            LayerEvent::Memory(MemoryEvent::Remove {
                key: Arc::from(key),
            })
        });
        removed
    }
    /// Removes only the selected representation, preserving concurrent writes.
    pub async fn remove_if_same(&self, key: &str, expected: &Entry<V>) -> Option<Entry<V>> {
        self.removed(key, Some(expected))
    }
    /// Invalidates all stored entries, preserving writes admitted after its barrier.
    pub fn invalidate_all(&self) {
        match &self.backend {
            Backend::Unbounded(store) => {
                // The generation is the clear's visibility barrier. Physical
                // extraction stays off the caller's path and occurs on reads
                // or maintenance, as with the preceding unbounded store.
                store.begin_clear();
            }
            Backend::Retained(store) => {
                let retired = {
                    let mut state = lock(store);
                    state.generation = state.generation.saturating_add(1);
                    let entries = std::mem::take(&mut state.entries);
                    state.weight = 0;
                    entries
                };
                for (key, stored) in retired {
                    self.retire(stored.retire(key, RetirementReason::Explicit));
                }
            }
        }
    }
    /// Applies physical expiry using the injected and selected monotonic clocks.
    pub async fn run_pending_tasks(&self) {
        let now = self.clock.as_ref().map(|clock| clock.now());
        match &self.backend {
            Backend::Unbounded(store) => {
                for shard in 0..store.shard_count() {
                    for retired in store.expire(shard, now) {
                        self.retire(retired);
                    }
                }
            }
            Backend::Retained(store) => {
                let retired = { lock(store).remove_expired(now) };
                for retired in retired {
                    self.retire(retired);
                }
            }
        }
    }
    pub(crate) fn maintain_step(&self) {
        let now = self.clock.as_ref().map(|clock| clock.now());
        let retired = match &self.backend {
            Backend::Unbounded(store) => store.maintain_step(now),
            Backend::Retained(store) => {
                let Ok(mut state) = store.try_lock() else {
                    return;
                };
                state.remove_expired(now)
            }
        };
        for retired in retired {
            self.retire(retired);
        }
    }
    /// A per-section diagnostic snapshot under concurrent mutation.
    #[must_use]
    pub fn usage(&self) -> MemoryUsage {
        match &self.backend {
            Backend::Unbounded(store) => store.usage(),
            Backend::Retained(store) => {
                let state = lock(store);
                MemoryUsage {
                    entries: state.entries.len() as u64,
                    weight: state.weight,
                }
            }
        }
    }
}
enum Expected<'a, V> {
    Any,
    Absent,
    Same(&'a Entry<V>),
    Mutation,
    MutationOf(&'a Entry<V>),
    Origin(&'a RevisionSnapshot),
}

impl<V> Expected<'_, V> {
    fn matches(&self, current: Option<&Entry<V>>) -> bool {
        match self {
            Self::Any => true,
            Self::Absent => current.is_none(),
            Self::Same(expected) | Self::MutationOf(expected) => {
                current.is_some_and(|entry| entry.is_same_instance(expected))
            }
            Self::Mutation => true,
            Self::Origin(_) => unreachable!("origin matching also needs the storage generation"),
        }
    }
}

struct Stored<V> {
    entry: Entry<V>,
    access: u128,
    monotonic_expiration: Option<Instant>,
    capture: CaptureAdmission,
}

struct Retirement<V> {
    key: Arc<str>,
    entry: Entry<V>,
    reason: RetirementReason,
    capture: CaptureAdmission,
}
enum MemoryRead<V> {
    Present(Entry<V>),
    Absent,
    Raced(Entry<V>),
    Expired {
        key: Arc<str>,
        entry: Entry<V>,
        reason: RetirementReason,
        capture: CaptureAdmission,
    },
}

// An admitted candidate transfers its sole representation into storage. A
// rejected candidate stays with the commit until outer coordination is gone.
enum PreparedEntry<V> {
    Candidate(Entry<V>),
    Fresh(crate::entry::FreshValue<V>),
    Stored,
}
impl<V> PreparedEntry<V> {
    fn meta(&self) -> &crate::entry::Metadata {
        match self {
            Self::Candidate(entry) => entry.meta(),
            Self::Fresh(value) => value.meta(),
            Self::Stored => unreachable!("admitted entry is already owned by storage"),
        }
    }
    fn try_reuse_payload(&mut self, stored: &mut Entry<V>) -> Option<V> {
        if !matches!(self, Self::Fresh(_)) {
            return None;
        }
        stored.replace_unique_payload(|| {
            let Self::Fresh(value) = std::mem::replace(self, Self::Stored) else {
                unreachable!("fresh payload was checked before replacement")
            };
            value
        })
    }
    fn take_for_storage(&mut self) -> Entry<V> {
        match std::mem::replace(self, Self::Stored) {
            Self::Candidate(entry) => entry,
            Self::Fresh(value) => value.into_entry(),
            Self::Stored => unreachable!("a candidate transfers into storage only once"),
        }
    }
}

enum StorageKey<'a> {
    Borrowed(&'a str),
    Shared(Arc<str>),
}
impl StorageKey<'_> {
    fn shared(&self) -> Arc<str> {
        match self {
            Self::Borrowed(key) => Arc::from(*key),
            Self::Shared(key) => Arc::clone(key),
        }
    }
}
impl AsRef<str> for StorageKey<'_> {
    fn as_ref(&self) -> &str {
        match self {
            Self::Borrowed(key) => key,
            Self::Shared(key) => key,
        }
    }
}
impl std::ops::Deref for StorageKey<'_> {
    type Target = str;
    fn deref(&self) -> &str {
        self.as_ref()
    }
}
pub(crate) struct PreparedWrite<'a, V> {
    key: StorageKey<'a>,
    entry: PreparedEntry<V>,
    original: Option<Entry<V>>,
    // Optional pin for a real outer coordinator; absent on inline L1 commits.
    retained: Option<Entry<V>>,
    time: crate::time::local::WriteTime,
    capture: CaptureAdmission,
    event: MemoryWriteEvent,
}
pub(crate) struct MemoryCommit<'a, V> {
    write: PreparedWrite<'a, V>,
    commit: RetentionCommit<V>,
}
pub(crate) struct DetachedRemoval<V> {
    key: Arc<str>,
    retired: Option<Retirement<V>>,
}

enum Retirements<V> {
    None,
    One(Retirement<V>),
    Many(Vec<Retirement<V>>),
    Reused { value: V, reason: RetirementReason },
}
impl<V> From<Option<Retirement<V>>> for Retirements<V> {
    fn from(value: Option<Retirement<V>>) -> Self {
        match value {
            None => Self::None,
            Some(value) => Self::One(value),
        }
    }
}
impl<V> From<Vec<Retirement<V>>> for Retirements<V> {
    fn from(mut values: Vec<Retirement<V>>) -> Self {
        match values.len() {
            0 => Self::None,
            1 => Self::One(values.pop().expect("one retirement was present")),
            _ => Self::Many(values),
        }
    }
}
impl<V: Clone + Send + Sync + 'static> Retirements<V> {
    fn finish(self, observer: &MemoryObserver<V>, key: &str) {
        match self {
            Self::None => {}
            Self::One(value) => observer.retire(value),
            Self::Many(values) => {
                for value in values {
                    observer.retire(value);
                }
            }
            Self::Reused { value, reason } => observer.retire_payload(key, value, reason),
        }
    }
}
#[cfg(test)]
impl<V> std::ops::Deref for Retirements<V> {
    type Target = [Retirement<V>];
    fn deref(&self) -> &Self::Target {
        match self {
            Self::None => &[],
            Self::One(value) => std::slice::from_ref(value),
            Self::Many(values) => values,
            Self::Reused { .. } => panic!("unique retirement has no pre-existing owned snapshot"),
        }
    }
}
struct RetentionCommit<V> {
    admission: MemoryAdmission,
    retired: Retirements<V>,
}

impl<V> RetentionCommit<V> {
    fn rejected(reason: CapacityRejection, retired: Vec<Retirement<V>>) -> Self {
        Self {
            admission: MemoryAdmission::Rejected(reason),
            retired: retired.into(),
        }
    }
}

struct ProjectedCapacity {
    entries: usize,
    weight: u128,
}

impl ProjectedCapacity {
    fn fits(&self, limits: MemoryLimits) -> bool {
        limits.fits(self.entries, self.weight)
    }
}

struct EvictionCandidate {
    key: Arc<str>,
    priority: Priority,
    access: u128,
    weight: u128,
}

impl<V> Stored<V> {
    fn retirement_reason(&self, now: Option<Timestamp>) -> Option<RetirementReason> {
        if !self.entry.is_read_eligible() {
            Some(RetirementReason::Eligibility)
        } else if now.is_some_and(|now| self.entry.is_physically_expired(now))
            || self
                .monotonic_expiration
                .is_some_and(|deadline| Instant::now() >= deadline)
        {
            Some(RetirementReason::Expired)
        } else {
            None
        }
    }
    fn retire(self, key: Arc<str>, reason: RetirementReason) -> Retirement<V> {
        Retirement {
            key,
            entry: self.entry,
            reason,
            capture: self.capture,
        }
    }
}

struct Retention<V> {
    origins: Origins,
    generation: u64,
    entries: HashMap<Arc<str>, Stored<V>>,
    weight: u128,
    next_access: u128,
    limits: MemoryLimits,
    expiry: MemoryExpiry,
}

impl<V: Clone> Retention<V> {
    fn new(limits: MemoryLimits, expiry: MemoryExpiry) -> Self {
        Self {
            origins: Origins::default(),
            generation: 1,
            entries: HashMap::new(),
            weight: 0,
            next_access: 0,
            limits,
            expiry,
        }
    }

    fn ticket(&mut self) -> u128 {
        self.next_access = self.next_access.saturating_add(1);
        self.next_access
    }

    fn get(&mut self, key: &str, now: Option<Timestamp>) -> MemoryRead<V> {
        if self
            .entries
            .get(key)
            .is_some_and(|stored| stored.retirement_reason(now).is_some())
            && let Some((key, stored)) = self.entries.remove_entry(key)
        {
            self.weight -= entry_weight(&stored.entry);
            let reason = stored
                .retirement_reason(now)
                .unwrap_or(RetirementReason::Expired);
            return MemoryRead::Expired {
                key,
                entry: stored.entry,
                reason,
                capture: stored.capture,
            };
        }
        let ticket = self.ticket();
        let entry = self.entries.get_mut(key).map(|stored| {
            stored.access = ticket;
            stored.entry.clone()
        });
        match entry {
            Some(entry) => MemoryRead::Present(entry),
            None => MemoryRead::Absent,
        }
    }

    fn remove(&mut self, key: &str) -> Option<Stored<V>> {
        let stored = self.entries.remove(key)?;
        self.weight -= entry_weight(&stored.entry);
        Some(stored)
    }

    fn remove_expired(&mut self, now: Option<Timestamp>) -> Vec<Retirement<V>> {
        let expired: Vec<_> = self
            .entries
            .iter()
            .filter(|(_, stored)| stored.retirement_reason(now).is_some())
            .map(|(key, _)| Arc::clone(key))
            .collect();
        let mut retired = Vec::with_capacity(expired.len());
        for key in expired {
            if let Some(stored) = self.remove(&key) {
                let reason = stored
                    .retirement_reason(now)
                    .unwrap_or(RetirementReason::Expired);
                retired.push(stored.retire(key, reason));
            }
        }
        retired
    }

    fn insert(
        &mut self,
        key: &StorageKey<'_>,
        entry: &mut PreparedEntry<V>,
        now: Timestamp,
        expected: Expected<'_, V>,
        capture: CaptureAdmission,
        event: MemoryWriteEvent,
    ) -> RetentionCommit<V> {
        if !expected.matches_generation(
            self.entries.get(key.as_ref()).map(|stored| &stored.entry),
            self.generation,
        ) {
            return RetentionCommit::rejected(CapacityRejection::VersionChanged, Vec::new());
        }
        if expected.changes_origin() {
            self.origins.advance(key);
        }
        let weight = entry.meta().size().map_or(1, |size| size.units()) as u128;
        if !self.limits.fits(1, weight) {
            return RetentionCommit::rejected(CapacityRejection::Oversized, Vec::new());
        }
        let retired = self.expire_under_pressure(key, weight, now);
        match self.plan(key, weight, entry.meta().priority()) {
            Ok(victims) => self.commit(
                key.shared(),
                entry.take_for_storage(),
                victims,
                retired,
                capture,
                event,
            ),
            Err(reason) => RetentionCommit::rejected(reason, retired),
        }
    }

    fn projection(&self, key: &str, weight: u128) -> ProjectedCapacity {
        let current = self.entries.get(key);
        ProjectedCapacity {
            entries: self.entries.len() + usize::from(current.is_none()),
            weight: self.weight - current.map_or(0, |stored| entry_weight(&stored.entry)) + weight,
        }
    }

    fn expire_under_pressure(
        &mut self,
        key: &str,
        weight: u128,
        now: Timestamp,
    ) -> Vec<Retirement<V>> {
        if self.projection(key, weight).fits(self.limits) {
            Vec::new()
        } else {
            self.remove_expired(Some(now))
        }
    }

    fn plan(
        &self,
        key: &str,
        weight: u128,
        priority: Priority,
    ) -> Result<Vec<Arc<str>>, CapacityRejection> {
        let mut projected = self.projection(key, weight);
        let mut victims = Vec::new();
        if projected.fits(self.limits) {
            return Ok(victims);
        }
        for victim in self.eligible_victims(key, priority) {
            projected.entries -= 1;
            projected.weight -= victim.weight;
            victims.push(victim.key);
            if projected.fits(self.limits) {
                return Ok(victims);
            }
        }
        Err(CapacityRejection::ProtectedCapacity)
    }

    fn eligible_victims(&self, key: &str, priority: Priority) -> Vec<EvictionCandidate> {
        let mut candidates: Vec<_> = self
            .entries
            .iter()
            .filter(|(existing_key, stored)| {
                existing_key.as_ref() != key
                    && stored.entry.meta().priority() != Priority::NeverRemove
                    && stored.entry.meta().priority().eviction_rank() <= priority.eviction_rank()
            })
            .map(|(key, stored)| EvictionCandidate {
                key: Arc::clone(key),
                priority: stored.entry.meta().priority(),
                access: stored.access,
                weight: entry_weight(&stored.entry),
            })
            .collect();
        candidates.sort_unstable_by(|a, b| {
            (a.priority.eviction_rank(), a.access, a.key.as_ref()).cmp(&(
                b.priority.eviction_rank(),
                b.access,
                b.key.as_ref(),
            ))
        });
        candidates
    }

    fn commit(
        &mut self,
        key: Arc<str>,
        entry: Entry<V>,
        victims: Vec<Arc<str>>,
        mut retired: Vec<Retirement<V>>,
        capture: CaptureAdmission,
        event: MemoryWriteEvent,
    ) -> RetentionCommit<V> {
        for victim in victims {
            if let Some(stored) = self.remove(&victim) {
                retired.push(stored.retire(victim, RetirementReason::Capacity));
            }
        }
        let replaced = self.remove(key.as_ref());
        let capture = match (&replaced, event) {
            (Some(old), MemoryWriteEvent::Expire) => old.capture,
            _ => capture,
        };
        let admission = match replaced {
            Some(replaced) => {
                let reason = match event {
                    MemoryWriteEvent::Set => RetirementReason::Replaced,
                    MemoryWriteEvent::Expire => RetirementReason::Metadata,
                };
                retired.push(replaced.retire(Arc::clone(&key), reason));
                MemoryAdmission::Replaced
            }
            None => MemoryAdmission::Admitted,
        };
        self.store(key, entry, capture);
        RetentionCommit {
            admission,
            retired: retired.into(),
        }
    }

    fn store(&mut self, key: Arc<str>, entry: Entry<V>, capture: CaptureAdmission) {
        let weight = entry_weight(&entry);
        let access = self.ticket();
        let monotonic_expiration = match self.expiry {
            MemoryExpiry::ClockDriven => None,
            MemoryExpiry::RealTime => Instant::now().checked_add(entry.backend_ttl()),
        };
        self.entries.insert(
            key,
            Stored {
                entry,
                access,
                monotonic_expiration,
                capture,
            },
        );
        self.weight += weight;
    }
}

fn entry_weight<V>(entry: &Entry<V>) -> u128 {
    u128::from(entry.meta().size().map_or(1, |weight| weight.units()))
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // External code never runs under these locks. Mutations preserve invariants.
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[derive(Clone)]
pub(crate) struct MemoryObserver<V> {
    events: Events,
    evictions: MemoryEvictions<V>,
    capture: EvictionCapture,
    reclamation: Reclamation<V>,
}
impl<V: Clone + Send + Sync + 'static> MemoryObserver<V> {
    fn new(events: Events, capture: EvictionCapture) -> Self {
        Self {
            evictions: MemoryEvictions::with_capacity(events.capacity()),
            events,
            capture,
            reclamation: Reclamation::Immediate,
        }
    }
    fn for_operation(&self) -> Self {
        let mut observer = self.clone();
        observer.reclamation = Reclamation::operation();
        observer
    }
    fn for_owner(&self) -> Self {
        let mut observer = self.clone();
        observer.reclamation = Reclamation::Immediate;
        observer
    }
    pub(crate) fn fence(&self) -> Option<Arc<dyn ReclamationFence>> {
        self.reclamation.fence()
    }
    pub(crate) fn guard<G>(&self, guard: G) -> ReclamationGuard<G> {
        ReclamationGuard::new(guard, self.fence())
    }
    pub(crate) fn emit(&self, event: CacheEvent) {
        self.events
            .emit_deferred(event, |pending| self.reclamation.defer(pending));
    }
    pub(crate) fn emit_lazy(&self, make: impl FnOnce() -> CacheEvent) {
        self.events
            .emit_deferred_lazy(make, |pending| self.reclamation.defer(pending));
    }
    pub(crate) fn emit_layer_lazy(&self, make: impl FnOnce() -> LayerEvent) {
        self.events
            .emit_layer_deferred(make, |pending| self.reclamation.defer(pending));
    }
    pub(crate) fn component_read(&self, component: crate::events::ComponentRead) {
        self.events
            .component_read_deferred(component, |pending| self.reclamation.defer(pending));
    }
    fn capture_admission(&self) -> CaptureAdmission {
        if self.evictions.has_receivers() || self.events.has_layer_receivers() {
            CaptureAdmission::Armed
        } else {
            CaptureAdmission::Unarmed
        }
    }
    fn retire_payload(&self, key: &str, value: V, reason: RetirementReason) {
        // Only immediate, unarmed insertion capture reaches this route. A
        // late layer subscriber still receives its reason/key after the guard.
        // Borrow the operation's still-live key. Disabled observations do not
        // clone a shared cache line or materialize an event-owned string.
        if reason.logical() {
            self.emit_lazy(|| CacheEvent::Eviction {
                key: Arc::from(key),
            });
        }
        if let Some(reason) = reason.fact() {
            self.emit_layer_lazy(|| {
                LayerEvent::Memory(MemoryEvent::Eviction {
                    key: Arc::from(key),
                    reason,
                })
            });
        }
        drop(value);
    }
    fn retire(&self, retired: Retirement<V>) {
        let Retirement {
            key,
            entry,
            reason,
            capture,
        } = retired;
        if reason.logical() {
            self.emit(CacheEvent::Eviction {
                key: Arc::clone(&key),
            });
        }
        if let Some(reason) = reason.fact() {
            self.emit_layer_lazy(|| {
                LayerEvent::Memory(MemoryEvent::Eviction {
                    key: Arc::clone(&key),
                    reason,
                })
            });
            if (matches!(capture, CaptureAdmission::Armed)
                || self.capture == EvictionCapture::AtRetirement)
                && let Some(old) = self.evictions.emit(&key, reason, &entry)
            {
                self.reclamation.retain(old);
            }
        }
        self.reclamation.retain(entry);
    }
}
