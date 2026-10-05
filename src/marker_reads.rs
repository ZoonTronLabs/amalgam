//! Independent control-read policy and local observation retention.
#![doc = include_str!("../docs/MARKER_READS.md")]

use crate::entry::{ContinuityStamp, Entry};
use crate::error::Result;
use crate::events::Events;
use crate::execution::lock;
use crate::locking::KeyedLock;
use crate::marker_snapshots::{MarkerLifecyclePolicy, MarkerSnapshotCache};
use crate::memory::{CapacityRejection, MemoryAdmission, MemoryExpiry, MemoryLimits, MemoryStore};
use crate::options::{EntryOptions, JitterSample};
use crate::tags::{MarkerKind, MarkerVersion};
use crate::time::{Clock, Timestamp};
use std::sync::{Arc, Mutex};

/// Selects the control-read contract independently of ordinary value options.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum MarkerReadPolicy {
    /// Preserve existing durable hydration and backplane-continuity boundaries.
    /// Value retrieval, decoding and durable validation share the value deadline.
    #[default]
    DurableRequired,
    /// Apply tag defaults to secondary checks, including L1 hits. Each marker
    /// has its own read deadline after the value read has completed. Explicit
    /// skips or suppressed faults may use known facts without durable validation.
    OptionsControlled,
}

/// A finite cause for a deliberately degraded control read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarkerReadFailure {
    /// A contended marker lock used an existing stale observation.
    LockTimeout,
    /// A retained marker made the soft deadline eligible.
    SoftTimeout,
    /// The unconditional marker deadline elapsed.
    HardTimeout,
    /// The immediate shared marker factory was excluded by its soft budget.
    FactorySoftTimeout,
    /// The immediate shared marker factory was excluded by its hard budget.
    FactoryHardTimeout,
    /// Durable control storage failed.
    Backend,
    /// A control response violated its protocol.
    Protocol,
}

/// Describes control authority separately from whether a value was served.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarkerReadOutcome {
    /// A fresh local observation or initialized backplane clear scalar was used.
    Cached,
    /// Durable storage confirmed presence or absence.
    Observed,
    /// A confirmed local maximum was retained after a lower or absent response.
    /// This does not claim that storage still contains that maximum.
    KnownMaximum,
    /// The selected policy skipped durable storage; no absence was fabricated.
    Skipped,
    /// Fail-safe reused a retained confirmed observation after this fault.
    StaleFallback(MarkerReadFailure),
    /// Storage was unavailable and no confirmed fallback could be used.
    Unavailable(MarkerReadFailure),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MarkerPresence {
    Present(MarkerVersion),
    Absent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MarkerObservation {
    Confirmed(MarkerPresence),
    KnownMaximum(MarkerVersion),
    Retained {
        presence: MarkerPresence,
        failure: MarkerReadFailure,
    },
}

impl MarkerObservation {
    pub(crate) fn presence(self) -> MarkerPresence {
        match self {
            Self::Confirmed(presence) | Self::Retained { presence, .. } => presence,
            Self::KnownMaximum(version) => MarkerPresence::Present(version),
        }
    }

    pub(crate) fn outcome(self) -> MarkerReadOutcome {
        match self {
            Self::Confirmed(_) => MarkerReadOutcome::Cached,
            Self::KnownMaximum(_) => MarkerReadOutcome::KnownMaximum,
            Self::Retained { failure, .. } => MarkerReadOutcome::StaleFallback(failure),
        }
    }

    pub(crate) fn observed_outcome(self) -> MarkerReadOutcome {
        match self {
            Self::Confirmed(_) => MarkerReadOutcome::Observed,
            Self::KnownMaximum(_) => MarkerReadOutcome::KnownMaximum,
            Self::Retained { failure, .. } => MarkerReadOutcome::StaleFallback(failure),
        }
    }

    pub(crate) fn reconcile_maximum(self, maximum: Option<MarkerVersion>) -> Self {
        match (self.presence(), maximum) {
            (MarkerPresence::Absent, Some(maximum)) => Self::KnownMaximum(maximum),
            (MarkerPresence::Present(version), Some(maximum)) if maximum > version => {
                Self::KnownMaximum(maximum)
            }
            (MarkerPresence::Absent | MarkerPresence::Present(_), _) => self,
        }
    }
}

enum ClearObservation {
    Unknown,
    Known {
        epoch: u64,
        outcome: MarkerReadOutcome,
    },
}

struct ClearObservations {
    remove: ClearObservation,
    expire: ClearObservation,
}

pub(crate) struct MarkerObservations {
    pub(crate) memory: MemoryStore<MarkerObservation>,
    pub(crate) locks: KeyedLock,
    pub(crate) lifecycle: MarkerLifecycleAccess,
    clears: Mutex<ClearObservations>,
}

pub(crate) enum MarkerLifecycleAccess {
    DurableOnly,
    CachedSnapshots(Arc<dyn MarkerSnapshotCache>),
}

pub(crate) enum MarkerReads {
    DurableRequired,
    OptionsControlled(Box<MarkerObservations>),
}

impl MarkerReads {
    pub(crate) fn lifecycle_policy(&self) -> MarkerLifecyclePolicy {
        match self {
            Self::DurableRequired => MarkerLifecyclePolicy::DurableOnly,
            Self::OptionsControlled(observations) => match &observations.lifecycle {
                MarkerLifecycleAccess::DurableOnly => MarkerLifecyclePolicy::DurableOnly,
                MarkerLifecycleAccess::CachedSnapshots(_) => MarkerLifecyclePolicy::CachedSnapshots,
            },
        }
    }
    pub(crate) fn policy(&self) -> MarkerReadPolicy {
        match self {
            Self::DurableRequired => MarkerReadPolicy::DurableRequired,
            Self::OptionsControlled(_) => MarkerReadPolicy::OptionsControlled,
        }
    }

    pub(crate) fn invalidate(&self) {
        match self {
            Self::DurableRequired => {}
            Self::OptionsControlled(observations) => observations.invalidate(),
        }
    }
}

impl MarkerObservations {
    pub(crate) fn new(
        limits: MemoryLimits,
        clock: Arc<dyn Clock>,
        expiry: MemoryExpiry,
        lifecycle: MarkerLifecycleAccess,
    ) -> Self {
        Self {
            lifecycle,
            memory: MemoryStore::with_clock_and_expiry(
                limits,
                Events::with_capacity(16),
                clock,
                expiry,
            ),
            locks: KeyedLock::new(64),
            clears: Mutex::new(ClearObservations {
                remove: ClearObservation::Unknown,
                expire: ClearObservation::Unknown,
            }),
        }
    }

    pub(crate) fn key(kind: &MarkerKind) -> Arc<str> {
        match kind {
            MarkerKind::ClearRemove => Arc::from("clear:remove"),
            MarkerKind::ClearExpire => Arc::from("clear:expire"),
            MarkerKind::Tag(tag) => Arc::from(format!("tag:{}", tag.as_str())),
        }
    }

    pub(crate) fn clear_status(&self, kind: &MarkerKind, epoch: u64) -> Option<MarkerReadOutcome> {
        let clears = lock(&self.clears);
        let observed = match kind {
            MarkerKind::ClearRemove => &clears.remove,
            MarkerKind::ClearExpire => &clears.expire,
            MarkerKind::Tag(_) => return None,
        };
        match observed {
            ClearObservation::Unknown => None,
            ClearObservation::Known {
                epoch: captured,
                outcome,
            } => (*captured == epoch).then_some(*outcome),
        }
    }

    pub(crate) fn initialize_clear(
        &self,
        kind: &MarkerKind,
        epoch: u64,
        outcome: MarkerReadOutcome,
    ) {
        let mut clears = lock(&self.clears);
        let target = match kind {
            MarkerKind::ClearRemove => &mut clears.remove,
            MarkerKind::ClearExpire => &mut clears.expire,
            MarkerKind::Tag(_) => return,
        };
        let outcome = match outcome {
            MarkerReadOutcome::Observed => MarkerReadOutcome::Cached,
            outcome => outcome,
        };
        *target = ClearObservation::Known { epoch, outcome };
    }

    pub(crate) fn ready(
        &self,
        kind: &MarkerKind,
        options: &EntryOptions,
        now: Timestamp,
        epoch: u64,
        clear_shortcut: bool,
    ) -> bool {
        self.ready_outcome(kind, options, now, epoch, clear_shortcut)
            .is_some()
    }

    pub(crate) fn ready_outcome(
        &self,
        kind: &MarkerKind,
        options: &EntryOptions,
        now: Timestamp,
        epoch: u64,
        clear_shortcut: bool,
    ) -> Option<MarkerReadOutcome> {
        if clear_shortcut && let Some(outcome) = self.clear_status(kind, epoch) {
            return Some(outcome);
        }
        let cached = if options.skip_memory_read() {
            None
        } else {
            self.memory.ready_at(&Self::key(kind), now)
        };
        if let Some(entry) = &cached
            && entry.freshness(now).is_fresh()
        {
            if matches!(self.lifecycle, MarkerLifecycleAccess::CachedSnapshots(_))
                && entry.should_eager_refresh(now)
            {
                return None;
            }
            return Some(entry.value().outcome());
        }
        if options.skip_distributed_read()
            || cached.is_some() && options.skip_distributed_read_when_stale()
        {
            match self.lifecycle {
                MarkerLifecycleAccess::DurableOnly => Some(MarkerReadOutcome::Skipped),
                // Skipping the read does not also skip the shared factory or
                // an independently enabled snapshot write/locker.
                MarkerLifecycleAccess::CachedSnapshots(_) => None,
            }
        } else {
            None
        }
    }

    pub(crate) async fn store(
        &self,
        kind: &MarkerKind,
        observation: MarkerObservation,
        options: &EntryOptions,
        now: Timestamp,
        jitter: JitterSample,
        stamp: ContinuityStamp,
    ) -> Result<MarkerObservation> {
        if options.skip_memory_write() || !stamp.is_current() {
            return Ok(observation);
        }
        let entry = Entry::try_fresh_with_jitter(
            observation,
            options,
            now,
            now,
            jitter,
            Box::from([]),
            None,
            None,
        )?;
        let entry = entry.with_hydrated_value(observation, stamp.clone());
        Ok(self
            .admit_observation(Self::key(kind), entry, now, stamp)
            .await)
    }

    pub(crate) async fn store_snapshot(
        &self,
        kind: &MarkerKind,
        observation: MarkerObservation,
        snapshot: crate::MarkerSnapshot,
        options: &EntryOptions,
        now: Timestamp,
        stamp: ContinuityStamp,
    ) -> Result<MarkerObservation> {
        if options.skip_memory_write() || !stamp.is_current() {
            return Ok(observation);
        }
        let source = Entry::try_rehydrate(
            observation,
            snapshot.created(),
            snapshot.logical_expiration(),
            snapshot.physical_expiration(),
            false,
            None,
            None,
            Box::from([]),
            now,
        )?;
        let Some(local) = source.for_memory_hydration(options, now)? else {
            return Ok(observation);
        };
        let local = local.with_hydrated_value(observation, stamp.clone());
        Ok(self
            .admit_observation(Self::key(kind), local, now, stamp)
            .await)
    }

    async fn admit_observation(
        &self,
        key: Arc<str>,
        entry: Entry<MarkerObservation>,
        now: Timestamp,
        stamp: ContinuityStamp,
    ) -> MarkerObservation {
        while stamp.is_current() {
            let current = self.memory.get_at(&key, now).await;
            let maximum = current
                .as_ref()
                .and_then(|entry| match entry.value().presence() {
                    MarkerPresence::Present(version) => Some(version),
                    MarkerPresence::Absent => None,
                });
            let merged = entry.with_value(entry.value().reconcile_maximum(maximum));
            let observation = *merged.value();
            let admission = self
                .memory
                .insert_if_unchanged(Arc::clone(&key), current.as_ref(), merged, now)
                .await;
            tracing::trace!(?admission, "marker observation admission");
            if admission != MemoryAdmission::Rejected(CapacityRejection::VersionChanged) {
                return observation;
            }
            tokio::task::yield_now().await;
        }
        *entry.value()
    }

    pub(crate) async fn retain_fallback(
        &self,
        kind: &MarkerKind,
        source: &Entry<MarkerObservation>,
        options: &EntryOptions,
        now: Timestamp,
        failure: MarkerReadFailure,
    ) -> Result<()> {
        if !options.skip_memory_write()
            && let Some(entry) = Entry::try_throttled(source, options, now)?
        {
            let entry = entry.with_value(MarkerObservation::Retained {
                presence: source.value().presence(),
                failure,
            });
            let admission = self
                .memory
                .insert_if_unchanged(Self::key(kind), Some(source), entry, now)
                .await;
            tracing::trace!(?admission, "marker fallback observation admission");
        }
        Ok(())
    }

    pub(crate) async fn merge_maximum(
        &self,
        kind: &MarkerKind,
        maximum: MarkerVersion,
        now: Timestamp,
        stamp: ContinuityStamp,
    ) {
        let key = Self::key(kind);
        while stamp.is_current() {
            let Some(current) = self.memory.get_at(&key, now).await else {
                return;
            };
            let observation = current.value().reconcile_maximum(Some(maximum));
            if observation == *current.value() {
                return;
            }
            let merged = current.with_value(observation);
            let admission = self
                .memory
                .insert_if_unchanged(Arc::clone(&key), Some(&current), merged, now)
                .await;
            if admission != MemoryAdmission::Rejected(CapacityRejection::VersionChanged) {
                return;
            }
            tokio::task::yield_now().await;
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn retain_snapshot_fallback(
        &self,
        kind: &MarkerKind,
        snapshot: crate::MarkerSnapshot,
        presence: MarkerPresence,
        options: &EntryOptions,
        now: Timestamp,
        failure: MarkerReadFailure,
        stamp: ContinuityStamp,
    ) -> Result<()> {
        if options.skip_memory_write() || !stamp.is_current() {
            return Ok(());
        }
        let observation = MarkerObservation::Retained { presence, failure };
        let source = Entry::try_rehydrate(
            observation,
            snapshot.created(),
            snapshot.logical_expiration(),
            snapshot.physical_expiration(),
            false,
            None,
            None,
            Box::from([]),
            now,
        )?
        .with_retention(options.size()?, options.priority());
        if let Some(throttled) = Entry::try_throttled(&source, options, now)? {
            let entry = throttled.with_hydrated_value(observation, stamp.clone());
            self.admit_observation(Self::key(kind), entry, now, stamp)
                .await;
        }
        Ok(())
    }

    pub(crate) fn invalidate(&self) {
        self.memory.invalidate_all();
        let mut clears = lock(&self.clears);
        clears.remove = ClearObservation::Unknown;
        clears.expire = ClearObservation::Unknown;
    }
}
