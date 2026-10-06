//! Concurrent L1 storage with absolute expiry and optional capacity limits.
//! Retired entries leave backend guards before observation or reclamation.
mod reclamation;
mod sharded;
use crate::entry::Entry;
use crate::events::{
    CacheEvent, Events, EvictionCapture, LayerEvent, MemoryEvent, MemoryEvictionReason,
    MemoryEvictions,
};
use crate::options::Priority;
use crate::time::{Clock, Timestamp};
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
enum CaptureAdmission {
    Armed,
    Unarmed,
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
    events: Events,
    clock: Option<Arc<dyn Clock>>,
    evictions: MemoryEvictions<V>,
    capture: EvictionCapture,
    reclamation: Reclamation<V>,
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
        let evictions = MemoryEvictions::with_capacity(events.capacity());
        Self {
            backend,
            events,
            clock,
            evictions,
            capture: EvictionCapture::AtInsertion,
            reclamation: Reclamation::Immediate,
        }
    }
    /// Selects insertion-time or retirement-time original-value observation.
    #[must_use]
    pub fn with_eviction_capture(mut self, capture: EvictionCapture) -> Self {
        self.capture = capture;
        self
    }
    /// Independent bounded subscriptions retain the actual stored value.
    pub fn evictions(&self) -> &MemoryEvictions<V> {
        &self.evictions
    }
    pub(crate) fn for_operation(&self) -> Self {
        let mut memory = self.clone();
        memory.reclamation = Reclamation::operation();
        memory
    }
    pub(crate) fn fence(&self) -> Option<Arc<dyn ReclamationFence>> {
        self.reclamation.fence()
    }
    pub(crate) fn guard<G>(&self, guard: G) -> ReclamationGuard<G> {
        ReclamationGuard::new(guard, self.fence())
    }
    fn capture_admission(&self) -> CaptureAdmission {
        if self.evictions.has_receivers() || self.events.has_layer_receivers() {
            CaptureAdmission::Armed
        } else {
            CaptureAdmission::Unarmed
        }
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
        let read = match &self.backend {
            Backend::Unbounded(store) => store.get(key, now),
            Backend::Retained(store) => lock(store).get(key, now),
        };
        match read {
            MemoryRead::Present(entry) => Some(entry),
            MemoryRead::Absent => None,
            MemoryRead::Raced(entry) => {
                self.reclamation.retain(entry);
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
    /// Immediately available reads never mutate or run a retirement callback.
    pub(crate) fn ready_at(&self, key: &str, now: Timestamp) -> Option<Entry<V>> {
        match &self.backend {
            Backend::Unbounded(store) => store.ready_at(key, now),
            Backend::Retained(store) => {
                let mut state = match store.try_lock() {
                    Ok(state) => state,
                    Err(std::sync::TryLockError::Poisoned(error)) => error.into_inner(),
                    Err(std::sync::TryLockError::WouldBlock) => return None,
                };
                let stored = state.entries.get(key)?;
                if stored.retirement_reason(Some(now)).is_some() {
                    return None;
                }
                let access = state.ticket();
                let stored = state.entries.get_mut(key)?;
                stored.access = access;
                Some(stored.entry.clone())
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
    fn insert_internal(
        &self,
        key: Arc<str>,
        entry: Entry<V>,
        now: Timestamp,
        expected: Expected<'_, V>,
        event: MemoryWriteEvent,
    ) -> MemoryAdmission {
        if entry.is_physically_expired(now) {
            self.reclamation.retain(entry);
            return self.rejected(key, CapacityRejection::PhysicallyExpired);
        }
        let prepared = entry.at_insertion(now);
        let capture = self.capture_admission();
        let commit = match &self.backend {
            Backend::Unbounded(store) => {
                store.insert(key.clone(), prepared.clone(), now, expected, capture, event)
            }
            Backend::Retained(store) => {
                lock(store).insert(key.clone(), prepared.clone(), now, expected, capture, event)
            }
        };
        for retired in commit.retired {
            self.retire(retired);
        }
        match commit.admission {
            MemoryAdmission::Rejected(reason) => {
                self.reclamation.retain(prepared);
                self.reclamation.retain(entry);
                self.rejected(key, reason)
            }
            MemoryAdmission::Admitted | MemoryAdmission::Replaced => {
                self.events.emit_layer_lazy(|| {
                    LayerEvent::Memory(match event {
                        MemoryWriteEvent::Set => MemoryEvent::Set { key },
                        MemoryWriteEvent::Expire => MemoryEvent::Expire { key },
                    })
                });
                // A timestamp adjustment may create another V representation.
                // Retain both through coordination, including concurrent clear.
                if !entry.is_same_instance(&prepared) {
                    self.reclamation.retain(entry);
                }
                self.reclamation.retain(prepared);
                commit.admission
            }
        }
    }
    fn rejected(&self, key: Arc<str>, reason: CapacityRejection) -> MemoryAdmission {
        self.events
            .emit(CacheEvent::MemoryAdmissionRejected { key, reason });
        MemoryAdmission::Rejected(reason)
    }
    fn retire(&self, retired: Retirement<V>) {
        let Retirement {
            key,
            entry,
            reason,
            capture,
        } = retired;
        if reason.logical() {
            self.events.emit(CacheEvent::Eviction {
                key: Arc::clone(&key),
            });
        }
        if let Some(reason) = reason.fact()
            && (matches!(capture, CaptureAdmission::Armed)
                || self.capture == EvictionCapture::AtRetirement)
        {
            self.events.emit_layer_lazy(|| {
                LayerEvent::Memory(MemoryEvent::Eviction {
                    key: Arc::clone(&key),
                    reason,
                })
            });
            if let Some(old) = self.evictions.emit(&key, reason, &entry) {
                self.reclamation.retain(old);
            }
        }
        self.reclamation.retain(entry);
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
        self.events.emit_layer_lazy(|| {
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
}

impl<V> Expected<'_, V> {
    fn matches(&self, current: Option<&Entry<V>>) -> bool {
        match self {
            Self::Any => true,
            Self::Absent => current.is_none(),
            Self::Same(expected) => current.is_some_and(|entry| entry.is_same_instance(expected)),
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

struct RetentionCommit<V> {
    admission: MemoryAdmission,
    retired: Vec<Retirement<V>>,
}

impl<V> RetentionCommit<V> {
    fn rejected(reason: CapacityRejection, retired: Vec<Retirement<V>>) -> Self {
        Self {
            admission: MemoryAdmission::Rejected(reason),
            retired,
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
    entries: HashMap<Arc<str>, Stored<V>>,
    weight: u128,
    next_access: u128,
    limits: MemoryLimits,
    expiry: MemoryExpiry,
}

impl<V: Clone> Retention<V> {
    fn new(limits: MemoryLimits, expiry: MemoryExpiry) -> Self {
        Self {
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
        key: Arc<str>,
        entry: Entry<V>,
        now: Timestamp,
        expected: Expected<'_, V>,
        capture: CaptureAdmission,
        event: MemoryWriteEvent,
    ) -> RetentionCommit<V> {
        if !expected.matches(self.entries.get(key.as_ref()).map(|stored| &stored.entry)) {
            return RetentionCommit::rejected(CapacityRejection::VersionChanged, Vec::new());
        }
        let weight = entry_weight(&entry);
        if !self.limits.fits(1, weight) {
            return RetentionCommit::rejected(CapacityRejection::Oversized, Vec::new());
        }
        let retired = self.expire_under_pressure(&key, weight, now);
        match self.plan(&key, weight, entry.meta().priority()) {
            Ok(victims) => self.commit(key, entry, victims, retired, capture, event),
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
        RetentionCommit { admission, retired }
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
