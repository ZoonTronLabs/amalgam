//! Concurrent L1 storage with absolute expiry and optional capacity limits.
//!
//! Unbounded storage keeps Moka's concurrent hot path. Bounded storage owns an
//! atomic admission plan: count and weight are independent limits, priorities
//! select victims, and pinned entries are never capacity victims.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use moka::Expiry;
use moka::future::Cache as MokaCache;
use moka::notification::RemovalCause;
use moka::ops::compute::{CompResult, Op};

use crate::entry::Entry;
use crate::events::{CacheEvent, Events};
use crate::options::Priority;
use crate::time::{Clock, Timestamp};

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
}

/// A diagnostic snapshot of retained entry count and weight.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryUsage {
    /// The number of physically retained entries.
    pub entries: u64,
    /// The sum of application-defined weights, without u32 truncation.
    pub weight: u128,
}

struct EntryExpiry {
    clock: Option<Arc<dyn Clock>>,
    mode: MemoryExpiry,
}

impl<V: Send + Sync + 'static> Expiry<Arc<str>, Entry<V>> for EntryExpiry {
    fn expire_after_create(
        &self,
        _key: &Arc<str>,
        value: &Entry<V>,
        _created_at: Instant,
    ) -> Option<Duration> {
        match self.mode {
            MemoryExpiry::ClockDriven => None,
            MemoryExpiry::RealTime => Some(self.clock.as_ref().map_or_else(
                || value.backend_ttl(),
                |clock| value.backend_ttl_at(clock.now()),
            )),
        }
    }

    fn expire_after_update(
        &self,
        key: &Arc<str>,
        value: &Entry<V>,
        updated_at: Instant,
        _remaining: Option<Duration>,
    ) -> Option<Duration> {
        self.expire_after_create(key, value, updated_at)
    }
}

enum Backend<V: Clone + Send + Sync + 'static> {
    Unbounded(MokaCache<Arc<str>, Entry<V>>),
    Retained(Arc<Mutex<Retention<V>>>),
}

impl<V: Clone + Send + Sync + 'static> Clone for Backend<V> {
    fn clone(&self) -> Self {
        match self {
            Self::Unbounded(cache) => Self::Unbounded(cache.clone()),
            Self::Retained(store) => Self::Retained(Arc::clone(store)),
        }
    }
}

/// The L1 memory store. No lock is held while notifying plugins or awaiting I/O.
#[derive(Clone)]
pub struct MemoryStore<V: Clone + Send + Sync + 'static> {
    backend: Backend<V>,
    events: Events,
    clock: Option<Arc<dyn Clock>>,
}

impl<V: Clone + Send + Sync + 'static> MemoryStore<V> {
    /// Legacy entry-count configuration using real-time storage expiry.
    #[must_use]
    pub fn new(max_capacity: Option<u64>, events: Events) -> Self {
        Self::create(
            MemoryLimits::new(max_capacity, None),
            events,
            None,
            MemoryExpiry::RealTime,
        )
    }

    /// Uses the injected clock exclusively. Idle cleanup runs through
    /// run_pending_tasks; a frozen clock survives real elapsed TTL.
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
            let notifications = events.clone();
            let listener = move |key: Arc<Arc<str>>, _value: Entry<V>, cause: RemovalCause| {
                if matches!(cause, RemovalCause::Expired | RemovalCause::Size) {
                    notifications.emit(CacheEvent::Eviction {
                        key: key.as_ref().clone(),
                    });
                }
            };
            Backend::Unbounded(
                MokaCache::builder()
                    .expire_after(EntryExpiry {
                        clock: clock.clone(),
                        mode,
                    })
                    .eviction_listener(listener)
                    .build(),
            )
        } else {
            Backend::Retained(Arc::new(Mutex::new(Retention::new(limits, mode))))
        };
        Self {
            backend,
            events,
            clock,
        }
    }

    /// Reads under the configured expiration contract.
    pub async fn get(&self, key: &str) -> Option<Entry<V>> {
        if let Some(clock) = &self.clock {
            return self.get_at(key, clock.now()).await;
        }
        match &self.backend {
            Backend::Unbounded(cache) => {
                let entry = cache.get(key).await?;
                if entry.is_read_eligible() {
                    return Some(entry);
                }
                if self.remove_if_same(key, &entry).await.is_some() {
                    self.events.emit(CacheEvent::Eviction {
                        key: Arc::from(key),
                    });
                }
                None
            }
            Backend::Retained(store) => {
                let read = { lock(store).get(key, None) };
                self.resolve_read(read)
            }
        }
    }

    /// Rejects values physically expired at the supplied instant. ClockDriven
    /// storage has no earlier real-time expiry; RealTime mode may evict by TTL.
    pub async fn get_at(&self, key: &str, now: Timestamp) -> Option<Entry<V>> {
        match &self.backend {
            Backend::Unbounded(cache) => {
                let entry = cache.get(key).await?;
                if entry.is_read_eligible() && !entry.is_physically_expired(now) {
                    return Some(entry);
                }
                if self.remove_if_same(key, &entry).await.is_some() {
                    self.events.emit(CacheEvent::Eviction {
                        key: Arc::from(key),
                    });
                }
                None
            }
            Backend::Retained(store) => {
                let read = { lock(store).get(key, Some(now)) };
                self.resolve_read(read)
            }
        }
    }
    /// Samples only an immediately available internal L1 read. Pending storage
    /// maintenance and physical-expiry removal use the ordinary owned pipeline.
    /// The caller counts this synchronous section through any eviction callback.
    pub(crate) fn ready_at(&self, key: &str, now: Timestamp) -> Option<Entry<V>> {
        let entry = match &self.backend {
            Backend::Unbounded(cache) => {
                let mut read = std::pin::pin!(cache.get(key));
                let mut context = Context::from_waker(Waker::noop());
                match read.as_mut().poll(&mut context) {
                    Poll::Ready(entry) => entry,
                    Poll::Pending => None,
                }
            }
            Backend::Retained(store) => {
                let read = { lock(store).get(key, Some(now)) };
                self.resolve_read(read)
            }
        }?;
        (entry.is_read_eligible() && !entry.is_physically_expired(now)).then_some(entry)
    }

    /// Legacy best-effort insertion. Admission rejection emits an explicit event.
    pub async fn insert(&self, key: Arc<str>, entry: Entry<V>) {
        let now = self
            .clock
            .as_ref()
            .map_or(entry.meta().inserted_at(), |clock| clock.now());
        let _ = self.insert_at(key, entry, now).await;
    }

    /// Atomically admits a candidate without collateral eviction on rejection.
    pub async fn insert_at(
        &self,
        key: Arc<str>,
        entry: Entry<V>,
        now: Timestamp,
    ) -> MemoryAdmission {
        self.insert_internal(key, entry, now, Expected::Any).await
    }

    /// Commits only while the current representation is unchanged. None expects
    /// absence, protecting passive hydration from a concurrent newer write.
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
        self.insert_internal(key, entry, now, expected).await
    }

    async fn insert_internal(
        &self,
        key: Arc<str>,
        entry: Entry<V>,
        now: Timestamp,
        expected: Expected<'_, V>,
    ) -> MemoryAdmission {
        if entry.is_physically_expired(now) {
            return self.rejected(key, CapacityRejection::PhysicallyExpired);
        }
        // Cloning V may execute caller code. Prepare outside storage locks and
        // keep this handle alive so a rejected candidate cannot be dropped there.
        let prepared = entry.at_insertion(now);
        match &self.backend {
            Backend::Unbounded(cache) => {
                let result = cache
                    .entry(key.clone())
                    .and_compute_with(|current| {
                        let matches = expected.matches(current.as_ref().map(|entry| entry.value()));
                        std::future::ready(if matches {
                            Op::Put(prepared.clone())
                        } else {
                            Op::Nop
                        })
                    })
                    .await;
                match result {
                    CompResult::Inserted(_) => MemoryAdmission::Admitted,
                    CompResult::ReplacedWith(_) => MemoryAdmission::Replaced,
                    CompResult::Unchanged(_) | CompResult::StillNone(_) => {
                        self.rejected(key, CapacityRejection::VersionChanged)
                    }
                    CompResult::Removed(_) => unreachable!("insertion only uses Put or Nop"),
                }
            }
            Backend::Retained(store) => {
                let commit = lock(store).insert(key.clone(), prepared.clone(), now, expected);
                for retired in commit.retired {
                    self.retire(retired);
                }
                match commit.admission {
                    MemoryAdmission::Rejected(reason) => self.rejected(key, reason),
                    MemoryAdmission::Admitted | MemoryAdmission::Replaced => commit.admission,
                }
            }
        }
    }

    fn rejected(&self, key: Arc<str>, reason: CapacityRejection) -> MemoryAdmission {
        self.events
            .emit(CacheEvent::MemoryAdmissionRejected { key, reason });
        MemoryAdmission::Rejected(reason)
    }

    fn retire(&self, retired: Retirement<V>) {
        match retired {
            Retirement::Evicted { key, entry } => {
                self.events.emit(CacheEvent::Eviction { key });
                drop(entry);
            }
            Retirement::Replaced(entry) => drop(entry),
        }
    }

    fn resolve_read(&self, read: MemoryRead<V>) -> Option<Entry<V>> {
        match read {
            MemoryRead::Present(entry) => Some(entry),
            MemoryRead::Absent => None,
            MemoryRead::Expired { key, entry } => {
                self.retire(Retirement::Evicted { key, entry });
                None
            }
        }
    }

    /// Removes a value explicitly, including a pinned value.
    pub async fn remove(&self, key: &str) -> Option<Entry<V>> {
        match &self.backend {
            Backend::Unbounded(cache) => cache.remove(key).await,
            Backend::Retained(store) => lock(store).remove(key).map(|stored| stored.entry),
        }
    }

    /// Removes only the expected representation, preserving concurrent writes.
    pub async fn remove_if_same(&self, key: &str, expected: &Entry<V>) -> Option<Entry<V>> {
        match &self.backend {
            Backend::Unbounded(cache) => {
                let result = cache
                    .entry(Arc::from(key))
                    .and_compute_with(|current| {
                        std::future::ready(
                            if current
                                .as_ref()
                                .is_some_and(|entry| entry.value().is_same_instance(expected))
                            {
                                Op::Remove
                            } else {
                                Op::Nop
                            },
                        )
                    })
                    .await;
                match result {
                    CompResult::Removed(entry) => Some(entry.into_value()),
                    CompResult::Unchanged(_) | CompResult::StillNone(_) => None,
                    CompResult::Inserted(_) | CompResult::ReplacedWith(_) => {
                        unreachable!("conditional removal only uses Remove or Nop")
                    }
                }
            }
            Backend::Retained(store) => {
                let mut state = lock(store);
                if !state
                    .entries
                    .get(key)
                    .is_some_and(|stored| stored.entry.is_same_instance(expected))
                {
                    return None;
                }
                state.remove(key).map(|stored| stored.entry)
            }
        }
    }

    /// Invalidates all entries, including pinned entries.
    pub fn invalidate_all(&self) {
        match &self.backend {
            Backend::Unbounded(cache) => cache.invalidate_all(),
            Backend::Retained(store) => {
                let retired = {
                    let mut state = lock(store);
                    let retired = std::mem::take(&mut state.entries);
                    state.weight = 0;
                    retired
                };
                drop(retired);
            }
        }
    }

    /// Performs pending expiry maintenance. ClockDriven maintenance consults
    /// only the injected clock, with no real-time expiration race.
    pub async fn run_pending_tasks(&self) {
        match &self.backend {
            Backend::Unbounded(cache) => {
                if let Some(clock) = &self.clock {
                    let now = clock.now();
                    let expired: Vec<_> = cache
                        .iter()
                        .filter(|(_, entry)| {
                            !entry.is_read_eligible() || entry.is_physically_expired(now)
                        })
                        .collect();
                    for (key, entry) in expired {
                        if self.remove_if_same(key.as_ref(), &entry).await.is_some() {
                            self.events.emit(CacheEvent::Eviction {
                                key: key.as_ref().clone(),
                            });
                        }
                    }
                }
                cache.run_pending_tasks().await;
            }
            Backend::Retained(store) => {
                // A supplied clock can reenter storage. Sample it before the guard.
                let now = self.clock.as_ref().map(|clock| clock.now());
                let expired = { lock(store).remove_expired(now) };
                for retired in expired {
                    self.retire(retired);
                }
            }
        }
    }

    /// A diagnostic usage snapshot; Moka counts are approximate before maintenance.
    #[must_use]
    pub fn usage(&self) -> MemoryUsage {
        match &self.backend {
            Backend::Unbounded(cache) => MemoryUsage {
                entries: cache.entry_count(),
                weight: cache.iter().map(|(_, entry)| entry_weight(&entry)).sum(),
            },
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
}

enum Retirement<V> {
    Evicted { key: Arc<str>, entry: Entry<V> },
    Replaced(Entry<V>),
}

enum MemoryRead<V> {
    Present(Entry<V>),
    Absent,
    Expired { key: Arc<str>, entry: Entry<V> },
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
    fn expired(&self, now: Option<Timestamp>) -> bool {
        !self.entry.is_read_eligible()
            || now.is_some_and(|now| self.entry.is_physically_expired(now))
            || self
                .monotonic_expiration
                .is_some_and(|deadline| Instant::now() >= deadline)
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
            .is_some_and(|stored| stored.expired(now))
            && let Some((key, stored)) = self.entries.remove_entry(key)
        {
            self.weight -= entry_weight(&stored.entry);
            return MemoryRead::Expired {
                key,
                entry: stored.entry,
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
            .filter(|(_, stored)| stored.expired(now))
            .map(|(key, _)| Arc::clone(key))
            .collect();
        let mut retired = Vec::with_capacity(expired.len());
        for key in expired {
            if let Some(stored) = self.remove(&key) {
                retired.push(Retirement::Evicted {
                    key,
                    entry: stored.entry,
                });
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
            Ok(victims) => self.commit(key, entry, victims, retired),
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
    ) -> RetentionCommit<V> {
        for victim in victims {
            if let Some(stored) = self.remove(&victim) {
                retired.push(Retirement::Evicted {
                    key: victim,
                    entry: stored.entry,
                });
            }
        }
        let replaced = self.remove(key.as_ref()).map(|stored| stored.entry);
        let admission = match replaced {
            Some(replaced) => {
                retired.push(Retirement::Replaced(replaced));
                MemoryAdmission::Replaced
            }
            None => MemoryAdmission::Admitted,
        };
        self.store(key, entry);
        RetentionCommit { admission, retired }
    }

    fn store(&mut self, key: Arc<str>, entry: Entry<V>) {
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
