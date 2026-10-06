//! Tag and clear observations, snapshot repair and marker mutations.
use super::{
    AcquisitionPolicy, Arc, BackplaneCommand, CacheEvent, CancellationSource, CommitMode,
    CommitReport, ContinuityStamp, Duration, EffectOutcome, EnqueueOutcome, Entry, EntryOptions,
    Error, FactoryCancellation, InvalidationStore, JitterSample, LeaseError, LeasePolicy, LinkMode,
    LocalEffect, MarkerAccess, MarkerAdvanceOutcome, MarkerCommand, MarkerError, MarkerKind,
    MarkerLease, MarkerLeaseKey, MarkerLifecycleAccess, MarkerObservation, MarkerObservations,
    MarkerPresence, MarkerReadFailure, MarkerReadOutcome, MarkerReadPolicy, MarkerReads,
    MarkerReplay, MarkerSnapshot, MarkerSnapshotCache, MarkerSnapshotRead, MarkerSnapshotRenewal,
    MarkerVersion, MutationReceipt, Observed, OperationOutcome, OptionsTarget, Ordering, Reason,
    Result, ShutdownTask, SkipReason, StoredMarker, Tag, Timeout, Timestamp, Worker,
    acquire_owned_supervised, bounded,
};

#[path = "marker_eager.rs"]
mod eager;
#[path = "marker_recovery.rs"]
mod recovery;

use crate::recovery::{MarkerMutationRecovery, MarkerSnapshotParticipation, MarkerSnapshotReplay};

#[derive(Clone, Copy)]
enum MarkerSnapshotWritePolicy {
    Operation,
    Replay,
}

#[derive(Clone, Copy)]
enum MarkerFactoryRead {
    Read(MarkerSnapshotRead),
    Unobserved(MarkerUnobservedRead),
}
#[derive(Clone, Copy)]
enum MarkerUnobservedRead {
    Skipped,
    Unavailable(MarkerReadFailure),
}
#[derive(Clone, Copy)]
enum MarkerFactoryMode {
    Foreground,
    Eager,
}
enum MarkerLeaseAcquisition {
    Acquired(MarkerLease),
    Contended,
}

enum MarkerFetch {
    Observed(MarkerPresence),
    Snapshot(MarkerSnapshotRead),
    Deadline(MarkerReadFailure),
}
#[derive(Clone, Copy)]
struct MarkerRefresh<'a> {
    kind: &'a MarkerKind,
    captured: u64,
    cancellation: &'a FactoryCancellation,
    mode: MarkerFactoryMode,
}
enum MarkerSnapshotFetch {
    Read(MarkerSnapshotRead),
    Deadline(MarkerReadFailure),
}
impl MarkerSnapshotFetch {
    fn into_marker_fetch(self) -> MarkerFetch {
        match self {
            Self::Read(read) => MarkerFetch::Snapshot(read),
            Self::Deadline(failure) => MarkerFetch::Deadline(failure),
        }
    }
}
enum MarkerLookup {
    Ready(MarkerReadOutcome),
    Refresh(Option<Entry<MarkerObservation>>),
}
#[derive(Clone, Copy)]
enum MarkerFactoryStale<'a> {
    Memory(&'a Entry<MarkerObservation>),
    Distributed(MarkerSnapshot),
    Absent,
}
#[derive(Clone, Copy)]
enum MarkerLocalCommit {
    Admitted,
    AwaitingFence {
        observation: MarkerObservation,
        created: Timestamp,
        jitter: JitterSample,
    },
}

struct MarkerSnapshotCommit {
    kind: MarkerKind,
    snapshot: MarkerSnapshot,
    options: EntryOptions,
    captured: u64,
    lease: MarkerLease,
    local: MarkerLocalCommit,
}

enum MarkerProviderFault {
    Backend,
    Protocol,
}
impl MarkerProviderFault {
    fn read_failure(self) -> MarkerReadFailure {
        match self {
            Self::Backend => MarkerReadFailure::Backend,
            Self::Protocol => MarkerReadFailure::Protocol,
        }
    }
    fn write_outcome(self) -> crate::MarkerSnapshotWriteOutcome {
        match self {
            Self::Backend => crate::MarkerSnapshotWriteOutcome::BackendFailure,
            Self::Protocol => crate::MarkerSnapshotWriteOutcome::ProtocolFailure,
        }
    }
}
impl<V: Clone + Send + Sync + 'static> Worker<V> {
    fn marker_clear_shortcut(&self) -> bool {
        self.inner.marker_clear_shortcut()
    }

    pub(super) async fn reconcile_controlled_markers(
        &self,
        entry: &Entry<V>,
        cancellation: &FactoryCancellation,
    ) -> Result<()> {
        if self.inner.disable_tagging
            || self.inner.marker_reads.policy() == MarkerReadPolicy::DurableRequired
        {
            return Ok(());
        }
        let MarkerReads::OptionsControlled(observations) = &self.inner.marker_reads else {
            return Ok(());
        };
        let store = match &self.inner.markers {
            MarkerAccess::Local => return Ok(()),
            MarkerAccess::Unavailable => return Err(MarkerError::Unsupported.into()),
            MarkerAccess::Durable(store) => Arc::clone(store),
        };
        // Keep the optional controller's large future out of ordinary value
        // flights. Only participating controlled reads allocate this branch.
        Box::pin(self.check_observed_markers(entry, cancellation, observations, store)).await
    }

    async fn check_observed_markers(
        &self,
        entry: &Entry<V>,
        cancellation: &FactoryCancellation,
        observations: &MarkerObservations,
        store: Arc<dyn InvalidationStore>,
    ) -> Result<()> {
        // Independent deadlines are per marker, as in the released reference.
        for kind in Self::secondary_marker_kinds(entry.meta().tags()) {
            cancellation.check()?;
            self.read_control_marker(observations, Arc::clone(&store), kind.clone(), cancellation)
                .await?;
            if self.marker_invalidates_snapshot(&kind, entry.meta().created()) {
                break;
            }
        }
        cancellation.check()
    }

    pub(super) fn secondary_marker_kinds(tags: &[Tag]) -> impl Iterator<Item = MarkerKind> + '_ {
        std::iter::once(MarkerKind::ClearRemove)
            .chain(tags.iter().cloned().map(MarkerKind::Tag))
            .chain(std::iter::once(MarkerKind::ClearExpire))
    }

    fn marker_invalidates_snapshot(&self, kind: &MarkerKind, created: Timestamp) -> bool {
        [kind, &MarkerKind::ClearRemove].into_iter().any(|kind| {
            self.inner
                .tags
                .marker_version(kind)
                .is_some_and(|version| created <= version.timestamp())
        })
    }

    async fn marker_cached(
        &self,
        observations: &MarkerObservations,
        kind: &MarkerKind,
    ) -> Option<Entry<MarkerObservation>> {
        if self.inner.tags_default_options.skip_memory_read() {
            None
        } else {
            observations
                .memory
                .get_at(&MarkerObservations::key(kind), self.inner.clock.now())
                .await
        }
    }

    fn marker_event(&self, kind: &MarkerKind, outcome: MarkerReadOutcome) {
        self.memory.emit_lazy(|| CacheEvent::MarkerRead {
            kind: kind.clone(),
            outcome,
        });
    }

    async fn read_control_marker(
        &self,
        observations: &MarkerObservations,
        store: Arc<dyn InvalidationStore>,
        kind: MarkerKind,
        cancellation: &FactoryCancellation,
    ) -> Result<()> {
        self.validate_execution_options(&self.inner.tags_default_options, OptionsTarget::Marker)?;
        let captured = self.inner.epoch.load(Ordering::Acquire);
        let outcome = match self.marker_lookup(observations, &kind, captured).await {
            MarkerLookup::Ready(outcome) => outcome,
            MarkerLookup::Refresh(cached) => {
                self.refresh_control_marker(
                    observations,
                    store,
                    &kind,
                    cached.as_ref(),
                    captured,
                    cancellation,
                )
                .await?
            }
        };
        cancellation.check()?;
        if self.marker_clear_shortcut() && self.inner.epoch.load(Ordering::Acquire) == captured {
            observations.initialize_clear(&kind, captured, outcome);
        }
        self.maybe_eager_control_marker(observations, &kind, captured, cancellation)
            .await?;
        self.marker_event(&kind, outcome);
        Ok(())
    }

    async fn marker_lookup(
        &self,
        observations: &MarkerObservations,
        kind: &MarkerKind,
        captured: u64,
    ) -> MarkerLookup {
        if self.marker_clear_shortcut()
            && let Some(outcome) = observations.clear_status(kind, captured)
        {
            return MarkerLookup::Ready(outcome);
        }
        let cached = self.marker_cached(observations, kind).await;
        if let Some(entry) = &cached
            && entry.freshness(self.inner.clock.now()).is_fresh()
        {
            return MarkerLookup::Ready(entry.value().outcome());
        }
        MarkerLookup::Refresh(cached)
    }

    async fn refresh_control_marker(
        &self,
        observations: &MarkerObservations,
        store: Arc<dyn InvalidationStore>,
        kind: &MarkerKind,
        before_lock: Option<&Entry<MarkerObservation>>,
        captured: u64,
        cancellation: &FactoryCancellation,
    ) -> Result<MarkerReadOutcome> {
        let key = MarkerObservations::key(kind);
        let guard = observations
            .locks
            .acquire(
                &key,
                crate::MemoryLockKind::Marker(kind.clone()),
                self.marker_lock_timeout(before_lock),
                cancellation,
                self.memory_acquire_route(),
            )
            .await?
            .map(|guard| self.memory.guard(guard));
        cancellation.check()?;
        if guard.is_none() && self.marker_fallback_eligible(before_lock) {
            // The current owner will refresh this marker: a contending reader
            // uses its retained fact without extending or replacing its TTL.
            return Ok(MarkerReadOutcome::StaleFallback(
                MarkerReadFailure::LockTimeout,
            ));
        }
        self.resolve_control_read(observations, store, kind, captured, cancellation)
            .await
    }

    async fn resolve_control_read(
        &self,
        observations: &MarkerObservations,
        store: Arc<dyn InvalidationStore>,
        kind: &MarkerKind,
        captured: u64,
        cancellation: &FactoryCancellation,
    ) -> Result<MarkerReadOutcome> {
        let cached = self.marker_cached(observations, kind).await;
        let options = &self.inner.tags_default_options;
        if let Some(entry) = &cached
            && entry.freshness(self.inner.clock.now()).is_fresh()
        {
            return Ok(entry.value().outcome());
        }
        if options.skip_distributed_read()
            || cached.is_some() && options.skip_distributed_read_when_stale()
        {
            return match &observations.lifecycle {
                MarkerLifecycleAccess::DurableOnly => Ok(MarkerReadOutcome::Skipped),
                MarkerLifecycleAccess::CachedSnapshots(_) => {
                    self.resolve_control_snapshot(
                        observations,
                        kind,
                        cached.as_ref(),
                        MarkerFactoryRead::Unobserved(MarkerUnobservedRead::Skipped),
                        captured,
                        cancellation,
                    )
                    .await
                }
            };
        }
        self.marker_remote(
            observations,
            store,
            kind,
            cached.as_ref(),
            captured,
            cancellation,
        )
        .await
    }

    fn marker_fallback_eligible(&self, cached: Option<&Entry<MarkerObservation>>) -> bool {
        self.inner.tags_default_options.is_fail_safe_enabled()
            && cached.is_some_and(|entry| {
                entry.is_read_eligible() && !entry.is_physically_expired(self.inner.clock.now())
            })
    }

    fn marker_lock_timeout(&self, cached: Option<&Entry<MarkerObservation>>) -> Timeout {
        let options = &self.inner.tags_default_options;
        if options.memory_lock_timeout().is_infinite() && self.marker_fallback_eligible(cached) {
            options.factory_soft_timeout()
        } else {
            options.memory_lock_timeout()
        }
    }

    async fn marker_remote(
        &self,
        observations: &MarkerObservations,
        store: Arc<dyn InvalidationStore>,
        kind: &MarkerKind,
        cached: Option<&Entry<MarkerObservation>>,
        captured: u64,
        cancellation: &FactoryCancellation,
    ) -> Result<MarkerReadOutcome> {
        let fetched = match &observations.lifecycle {
            MarkerLifecycleAccess::DurableOnly => {
                self.fetch_control_marker(
                    Arc::clone(&store),
                    kind.clone(),
                    cached.is_some(),
                    cancellation,
                )
                .await
            }
            MarkerLifecycleAccess::CachedSnapshots(cache) => self
                .fetch_control_snapshot(
                    Arc::clone(cache),
                    kind.clone(),
                    cached.is_some(),
                    cancellation,
                )
                .await
                .map(MarkerSnapshotFetch::into_marker_fetch),
        };
        match fetched {
            Ok(MarkerFetch::Snapshot(snapshot)) => {
                self.resolve_control_snapshot(
                    observations,
                    kind,
                    cached,
                    MarkerFactoryRead::Read(snapshot),
                    captured,
                    cancellation,
                )
                .await
            }
            Ok(MarkerFetch::Observed(presence)) => {
                self.record_control_observation(observations, kind, presence, captured)
                    .await
            }
            Ok(MarkerFetch::Deadline(failure)) => {
                self.unavailable_control_marker(
                    observations,
                    kind,
                    cached,
                    captured,
                    cancellation,
                    failure,
                )
                .await
            }
            Err(Error::Marker(error)) => {
                let failure = self.suppressed_marker_fault(kind, error)?.read_failure();
                self.unavailable_control_marker(
                    observations,
                    kind,
                    cached,
                    captured,
                    cancellation,
                    failure,
                )
                .await
            }
            Err(error) => Err(error),
        }
    }

    async fn unavailable_control_marker(
        &self,
        observations: &MarkerObservations,
        kind: &MarkerKind,
        cached: Option<&Entry<MarkerObservation>>,
        captured: u64,
        cancellation: &FactoryCancellation,
        failure: MarkerReadFailure,
    ) -> Result<MarkerReadOutcome> {
        match &observations.lifecycle {
            MarkerLifecycleAccess::DurableOnly => {
                self.marker_degraded(observations, kind, cached, failure)
                    .await
            }
            MarkerLifecycleAccess::CachedSnapshots(_) => {
                self.resolve_control_snapshot(
                    observations,
                    kind,
                    cached,
                    MarkerFactoryRead::Unobserved(MarkerUnobservedRead::Unavailable(failure)),
                    captured,
                    cancellation,
                )
                .await
            }
        }
    }

    async fn fetch_control_snapshot(
        &self,
        cache: Arc<dyn MarkerSnapshotCache>,
        kind: MarkerKind,
        has_fallback: bool,
        cancellation: &FactoryCancellation,
    ) -> Result<MarkerSnapshotFetch> {
        let options = &self.inner.tags_default_options;
        let timeout = options.appropriate_distributed_timeout(has_fallback);
        let soft = options.is_fail_safe_enabled()
            && has_fallback
            && timeout == options.distributed_soft_timeout()
            && timeout != options.distributed_hard_timeout();
        let source = CancellationSource::new();
        let token = source.token();
        let scope = self.inner.scope.clone();
        let clock = Arc::clone(&self.inner.clock);
        let mut execution = self.scopes().execution(
            async move {
                cache
                    .read_snapshot(&scope, &kind, clock.now(), token)
                    .await
                    .map(MarkerSnapshotFetch::Read)
                    .map_err(Error::from)
            },
            source,
        );
        execution.link(cancellation, LinkMode::Explicit);
        match bounded(timeout, &mut execution).await? {
            Some(result) => result,
            None => {
                execution.cancel(if soft {
                    Reason::SoftTimeout
                } else {
                    Reason::HardTimeout
                });
                Ok(MarkerSnapshotFetch::Deadline(if soft {
                    MarkerReadFailure::SoftTimeout
                } else {
                    MarkerReadFailure::HardTimeout
                }))
            }
        }
    }

    async fn resolve_control_snapshot(
        &self,
        observations: &MarkerObservations,
        kind: &MarkerKind,
        cached: Option<&Entry<MarkerObservation>>,
        read: MarkerFactoryRead,
        captured: u64,
        cancellation: &FactoryCancellation,
    ) -> Result<MarkerReadOutcome> {
        if let MarkerFactoryRead::Read(read) = read {
            if let Some(maximum) = read.maximum() {
                self.apply_marker(StoredMarker::new(kind.clone(), maximum));
            }
            if let MarkerSnapshotRead::Snapshot(snapshot) = read
                && snapshot.is_fresh(self.inner.clock.now())
            {
                return self
                    .record_control_snapshot(observations, kind, snapshot, captured)
                    .await;
            }
        }
        let lease = match self
            .acquire_control_marker_lease(kind, cancellation, MarkerFactoryMode::Foreground)
            .await?
        {
            MarkerLeaseAcquisition::Acquired(lease) => lease,
            MarkerLeaseAcquisition::Contended => return Err(LeaseError::AcquisitionTimeout.into()),
        };
        let read = if lease.is_held() {
            self.recheck_control_snapshot(kind, cached.is_some(), cancellation)
                .await?
                .map_or(read, MarkerFactoryRead::Read)
        } else {
            read
        };
        self.resolve_leased_control_snapshot(
            observations,
            cached,
            read,
            MarkerRefresh {
                kind,
                captured,
                cancellation,
                mode: MarkerFactoryMode::Foreground,
            },
            lease,
        )
        .await
    }

    async fn resolve_leased_control_snapshot(
        &self,
        observations: &MarkerObservations,
        cached: Option<&Entry<MarkerObservation>>,
        read: MarkerFactoryRead,
        refresh: MarkerRefresh<'_>,
        lease: MarkerLease,
    ) -> Result<MarkerReadOutcome> {
        let MarkerRefresh {
            kind,
            captured,
            cancellation,
            mode,
        } = refresh;
        lease.proof()?;
        cancellation.check()?;
        let (observation, stale) = match read {
            MarkerFactoryRead::Read(MarkerSnapshotRead::Snapshot(snapshot)) => {
                self.apply_marker(StoredMarker::new(kind.clone(), snapshot.version()));
                if matches!(mode, MarkerFactoryMode::Foreground)
                    && snapshot.is_fresh(self.inner.clock.now())
                {
                    let result = self
                        .record_control_snapshot(observations, kind, snapshot, captured)
                        .await;
                    return self.finish_control_marker_lease(kind, lease, result).await;
                }
                (
                    MarkerObservation::KnownMaximum(snapshot.version()),
                    MarkerFactoryStale::Distributed(snapshot),
                )
            }
            MarkerFactoryRead::Read(MarkerSnapshotRead::Missing { maximum }) => {
                if let Some(version) = maximum {
                    self.apply_marker(StoredMarker::new(kind.clone(), version));
                }
                let presence = maximum.map_or(MarkerPresence::Absent, MarkerPresence::Present);
                (
                    MarkerObservation::Confirmed(presence),
                    cached.map_or(MarkerFactoryStale::Absent, MarkerFactoryStale::Memory),
                )
            }
            MarkerFactoryRead::Unobserved(read) => {
                let Some(observation) = self.known_control_factory_observation(kind, cached) else {
                    return self
                        .complete_unobserved_marker_factory(
                            observations,
                            cached,
                            read,
                            refresh,
                            lease,
                        )
                        .await;
                };
                (
                    observation,
                    cached.map_or(MarkerFactoryStale::Absent, MarkerFactoryStale::Memory),
                )
            }
        };
        let observation = observation.reconcile_maximum(self.inner.tags.marker_version(kind));
        self.complete_control_marker_factory(observations, observation, stale, refresh, lease)
            .await
    }

    fn known_control_factory_observation(
        &self,
        kind: &MarkerKind,
        cached: Option<&Entry<MarkerObservation>>,
    ) -> Option<MarkerObservation> {
        let cached = cached.and_then(|entry| match entry.value().presence() {
            MarkerPresence::Present(version) => Some(version),
            MarkerPresence::Absent => None,
        });
        self.inner
            .tags
            .marker_version(kind)
            .into_iter()
            .chain(cached)
            .max()
            .map(MarkerObservation::KnownMaximum)
    }

    async fn complete_unobserved_marker_factory(
        &self,
        observations: &MarkerObservations,
        cached: Option<&Entry<MarkerObservation>>,
        read: MarkerUnobservedRead,
        refresh: MarkerRefresh<'_>,
        lease: MarkerLease,
    ) -> Result<MarkerReadOutcome> {
        let stale = cached.map_or(MarkerFactoryStale::Absent, MarkerFactoryStale::Memory);
        let excluded = match self.excluded_marker_factory(stale, refresh.mode) {
            Ok(excluded) => excluded,
            Err(error) => {
                return self
                    .finish_control_marker_lease(refresh.kind, lease, Err(error))
                    .await;
            }
        };
        let result = if let Some(failure) = excluded {
            self.retain_excluded_marker_factory(
                observations,
                refresh.kind,
                MarkerPresence::Absent,
                stale,
                refresh.captured,
                failure,
            )
            .await
        } else {
            // A completed zero selection is not a confirmed durable absence.
            // Skips/faults cannot install a fresh negative observation.
            Ok(match read {
                MarkerUnobservedRead::Skipped => MarkerReadOutcome::Skipped,
                MarkerUnobservedRead::Unavailable(failure) => {
                    MarkerReadOutcome::Unavailable(failure)
                }
            })
        };
        self.finish_control_marker_lease(refresh.kind, lease, result)
            .await
    }

    async fn acquire_control_marker_lease(
        &self,
        kind: &MarkerKind,
        cancellation: &FactoryCancellation,
        mode: MarkerFactoryMode,
    ) -> Result<MarkerLeaseAcquisition> {
        cancellation.check()?;
        let options = &self.inner.tags_default_options;
        if options.skip_distributed_locker()
            || options.skip_distributed_read() && options.skip_distributed_write()
        {
            return Ok(MarkerLeaseAcquisition::Acquired(MarkerLease::Unleased));
        }
        let Some(locker) = &self.inner.distributed_locker else {
            return Ok(MarkerLeaseAcquisition::Acquired(MarkerLease::Unleased));
        };
        let key = MarkerLeaseKey::new(&self.inner.scope, kind).into_arc();
        let policy = match self.inner.lease_policy {
            LeasePolicy::Fenced => AcquisitionPolicy::TokenOwned,
            LeasePolicy::CooperativeLegacy => AcquisitionPolicy::LegacyBackendContract,
        };
        let locker = Arc::clone(locker);
        let ttl = self.inner.lease_ttl;
        let timeout = match mode {
            MarkerFactoryMode::Foreground => options.distributed_lock_timeout(),
            MarkerFactoryMode::Eager => Timeout::After(Duration::ZERO),
        };
        let owner = self.lease_owner(&key);
        let work = async move {
            acquire_owned_supervised(locker, key, ttl, timeout, policy, owner)
                .await
                .map_err(Error::from)
        };
        let execution = self.scopes().execution(work, CancellationSource::new());
        execution.link(cancellation, LinkMode::Explicit);
        let acquired = execution.await;
        cancellation.check()?;
        match acquired {
            Ok(Some(lease)) => Ok(MarkerLeaseAcquisition::Acquired(
                match self.inner.lease_policy {
                    LeasePolicy::Fenced => MarkerLease::Fenced(lease),
                    LeasePolicy::CooperativeLegacy => MarkerLease::Cooperative(lease),
                },
            )),
            Ok(None) | Err(Error::Lease(LeaseError::AcquisitionTimeout)) => {
                self.control_marker_contention(mode)
            }
            Err(Error::Lease(error @ LeaseError::Backend { .. }))
                if self.inner.lease_policy == LeasePolicy::CooperativeLegacy
                    && !options.rethrow_distributed_locker_exceptions() =>
            {
                tracing::warn!(%error, "explicit cooperative marker locker degradation");
                match mode {
                    MarkerFactoryMode::Foreground => {
                        Ok(MarkerLeaseAcquisition::Acquired(MarkerLease::Unleased))
                    }
                    MarkerFactoryMode::Eager => Ok(MarkerLeaseAcquisition::Contended),
                }
            }
            Err(error) => Err(error),
        }
    }

    fn control_marker_contention(&self, mode: MarkerFactoryMode) -> Result<MarkerLeaseAcquisition> {
        match (mode, self.inner.lease_policy) {
            (MarkerFactoryMode::Foreground, LeasePolicy::Fenced) => {
                Err(LeaseError::AcquisitionTimeout.into())
            }
            (MarkerFactoryMode::Foreground, LeasePolicy::CooperativeLegacy) => {
                Ok(MarkerLeaseAcquisition::Acquired(MarkerLease::Unleased))
            }
            (MarkerFactoryMode::Eager, LeasePolicy::Fenced | LeasePolicy::CooperativeLegacy) => {
                Ok(MarkerLeaseAcquisition::Contended)
            }
        }
    }

    async fn finish_control_marker_lease<T>(
        &self,
        kind: &MarkerKind,
        lease: MarkerLease,
        result: Result<T>,
    ) -> Result<T> {
        let released = match lease.release().await {
            Err(error @ LeaseError::Backend { .. })
                if self.inner.lease_policy == LeasePolicy::CooperativeLegacy
                    && !self
                        .inner
                        .tags_default_options
                        .rethrow_distributed_locker_exceptions() =>
            {
                tracing::warn!(%error, "explicit cooperative marker release degradation");
                Ok(())
            }
            released => released,
        };
        match (result, released) {
            (Ok(value), Ok(())) => Ok(value),
            (Ok(_), Err(error)) => Err(error.into()),
            (Err(error), Ok(())) => Err(error),
            (Err(error), Err(cleanup)) => {
                self.lease_owner(&MarkerObservations::key(kind))
                    .supervise(Box::pin(async move { Err(cleanup) }));
                Err(error)
            }
        }
    }

    async fn recheck_control_snapshot(
        &self,
        kind: &MarkerKind,
        has_fallback: bool,
        cancellation: &FactoryCancellation,
    ) -> Result<Option<MarkerSnapshotRead>> {
        let MarkerReads::OptionsControlled(observations) = &self.inner.marker_reads else {
            return Ok(None);
        };
        let MarkerLifecycleAccess::CachedSnapshots(cache) = &observations.lifecycle else {
            return Ok(None);
        };
        let options = &self.inner.tags_default_options;
        if options.skip_distributed_read()
            || has_fallback && options.skip_distributed_read_when_stale()
        {
            return Ok(None);
        }
        match self
            .fetch_control_snapshot(Arc::clone(cache), kind.clone(), has_fallback, cancellation)
            .await
        {
            Ok(MarkerSnapshotFetch::Read(read)) => Ok(Some(read)),
            Ok(MarkerSnapshotFetch::Deadline(_)) => Ok(None),
            Err(Error::Marker(error)) => {
                self.suppressed_marker_fault(kind, error)?;
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    async fn record_control_snapshot(
        &self,
        observations: &MarkerObservations,
        kind: &MarkerKind,
        snapshot: MarkerSnapshot,
        captured: u64,
    ) -> Result<MarkerReadOutcome> {
        let observation = MarkerObservation::Confirmed(MarkerPresence::Present(snapshot.version()))
            .reconcile_maximum(self.inner.tags.marker_version(kind));
        let observation = observations
            .store_snapshot(
                kind,
                observation,
                snapshot,
                &self.inner.tags_default_options,
                self.inner.clock.now(),
                ContinuityStamp::new(Arc::clone(&self.inner.epoch), captured),
            )
            .await?;
        Ok(observation.observed_outcome())
    }

    async fn complete_control_marker_factory(
        &self,
        observations: &MarkerObservations,
        observation: MarkerObservation,
        stale: MarkerFactoryStale<'_>,
        refresh: MarkerRefresh<'_>,
        lease: MarkerLease,
    ) -> Result<MarkerReadOutcome> {
        let MarkerRefresh {
            kind,
            captured,
            cancellation,
            mode,
        } = refresh;
        cancellation.check()?;
        lease.proof()?;
        let options = &self.inner.tags_default_options;
        let excluded = match self.excluded_marker_factory(stale, mode) {
            Ok(excluded) => excluded,
            Err(error) => {
                return self
                    .finish_control_marker_lease(kind, lease, Err(error))
                    .await;
            }
        };
        if let Some(failure) = excluded {
            let result = self
                .retain_excluded_marker_factory(
                    observations,
                    kind,
                    observation.presence(),
                    stale,
                    captured,
                    failure,
                )
                .await;
            return self.finish_control_marker_lease(kind, lease, result).await;
        }
        // The shared factory is an immediate selection, not remote I/O. Positive
        // factory budgets cannot time out this total, allocation-free decision.
        let observation = observation.reconcile_maximum(self.inner.tags.marker_version(kind));
        let created = self.inner.clock.now();
        let jitter = self.jitter_sample(options)?;
        let (observation, local) = if matches!(&lease, MarkerLease::Fenced(_))
            && matches!(observation.presence(), MarkerPresence::Present(_))
            && !options.skip_distributed_write()
        {
            // A failed native fence must not leave a fresh local observation.
            (
                observation,
                MarkerLocalCommit::AwaitingFence {
                    observation,
                    created,
                    jitter,
                },
            )
        } else {
            let observation = observations
                .store(
                    kind,
                    observation,
                    options,
                    created,
                    jitter,
                    ContinuityStamp::new(Arc::clone(&self.inner.epoch), captured),
                )
                .await?;
            (observation, MarkerLocalCommit::Admitted)
        };
        if let MarkerPresence::Present(version) = observation.presence()
            && !options.skip_distributed_write()
        {
            self.write_control_snapshot(
                MarkerSnapshotCommit {
                    kind: kind.clone(),
                    snapshot: MarkerSnapshot::fresh(version, options, created),
                    options: options.clone(),
                    captured,
                    lease,
                    local,
                },
                cancellation,
            )
            .await?;
        } else {
            self.finish_control_marker_lease(kind, lease, Ok(()))
                .await?;
        }
        cancellation.check()?;
        Ok(observation
            .reconcile_maximum(self.inner.tags.marker_version(kind))
            .observed_outcome())
    }

    fn marker_factory_fallback_eligible(&self, stale: MarkerFactoryStale<'_>) -> bool {
        self.inner.tags_default_options.is_fail_safe_enabled()
            && match stale {
                MarkerFactoryStale::Memory(cached) => self.marker_fallback_eligible(Some(cached)),
                MarkerFactoryStale::Distributed(snapshot) => {
                    !snapshot.is_physically_expired(self.inner.clock.now())
                }
                MarkerFactoryStale::Absent => false,
            }
    }

    fn excluded_marker_factory(
        &self,
        stale: MarkerFactoryStale<'_>,
        mode: MarkerFactoryMode,
    ) -> Result<Option<MarkerReadFailure>> {
        if matches!(mode, MarkerFactoryMode::Eager) {
            return Ok(None);
        }
        let options = &self.inner.tags_default_options;
        let has_fallback = self.marker_factory_fallback_eligible(stale);
        let timeout = options.appropriate_factory_timeout(has_fallback);
        if !timeout
            .as_duration()
            .is_some_and(|duration| duration.is_zero())
        {
            return Ok(None);
        }
        if !options.is_fail_safe_enabled() {
            return Err(Error::FactoryTimeout {
                elapsed: Duration::ZERO,
            });
        }
        Ok(Some(
            if has_fallback
                && timeout == options.factory_soft_timeout()
                && timeout != options.factory_hard_timeout()
            {
                MarkerReadFailure::FactorySoftTimeout
            } else {
                MarkerReadFailure::FactoryHardTimeout
            },
        ))
    }

    async fn retain_excluded_marker_factory(
        &self,
        observations: &MarkerObservations,
        kind: &MarkerKind,
        presence: MarkerPresence,
        stale: MarkerFactoryStale<'_>,
        captured: u64,
        failure: MarkerReadFailure,
    ) -> Result<MarkerReadOutcome> {
        match stale {
            MarkerFactoryStale::Distributed(snapshot)
                if self.marker_factory_fallback_eligible(stale) =>
            {
                observations
                    .retain_snapshot_fallback(
                        kind,
                        snapshot,
                        presence,
                        &self.inner.tags_default_options,
                        self.inner.clock.now(),
                        failure,
                        ContinuityStamp::new(Arc::clone(&self.inner.epoch), captured),
                    )
                    .await?;
                Ok(MarkerReadOutcome::StaleFallback(failure))
            }
            MarkerFactoryStale::Memory(cached) => {
                self.marker_degraded(observations, kind, Some(cached), failure)
                    .await
            }
            MarkerFactoryStale::Distributed(_) | MarkerFactoryStale::Absent => {
                Ok(MarkerReadOutcome::Unavailable(failure))
            }
        }
    }

    async fn write_control_snapshot(
        &self,
        commit: MarkerSnapshotCommit,
        cancellation: &FactoryCancellation,
    ) -> Result<()> {
        let MarkerReads::OptionsControlled(observations) = &self.inner.marker_reads else {
            return Ok(());
        };
        let MarkerLifecycleAccess::CachedSnapshots(cache) = &observations.lifecycle else {
            return Ok(());
        };
        let background = commit.options.allow_background_distributed_operations();
        let source = CancellationSource::new();
        let token = source.token();
        let worker = self.clone();
        let cache = Arc::clone(cache);
        let key = MarkerObservations::key(&commit.kind);
        let execution = self.scopes().execution(
            async move {
                let result = worker
                    .commit_control_snapshot(
                        cache,
                        &commit,
                        token,
                        MarkerSnapshotWritePolicy::Operation,
                    )
                    .await
                    .map(|_| ());
                worker
                    .finish_control_marker_lease(&commit.kind, commit.lease, result)
                    .await
            },
            source,
        );
        execution.link(
            cancellation,
            if background {
                LinkMode::CallerScope
            } else {
                LinkMode::Explicit
            },
        );
        if background {
            let _completion = self.inner.tasks.spawn(
                ShutdownTask::Distributed,
                key,
                self.inner.events.clone(),
                execution,
            );
            Ok(())
        } else {
            execution.await
        }
    }

    async fn commit_control_snapshot(
        &self,
        cache: Arc<dyn MarkerSnapshotCache>,
        commit: &MarkerSnapshotCommit,
        cancellation: FactoryCancellation,
        policy: MarkerSnapshotWritePolicy,
    ) -> Result<EffectOutcome> {
        let kind = &commit.kind;
        let captured = commit.captured;
        cancellation.check()?;
        if self.inner.epoch.load(Ordering::Acquire) != captured {
            return Ok(EffectOutcome::Skipped(SkipReason::Superseded));
        }
        // Like the reference Set, this owned write has no distributed-read
        // timeout. Provider I/O bounds and explicit cancellation still apply.
        let result = match commit.lease.proof()? {
            Some(proof) => {
                cache
                    .renew_snapshot_with_lease(
                        &self.inner.scope,
                        kind,
                        commit.snapshot,
                        self.inner.clock.now(),
                        &proof,
                        cancellation.clone(),
                    )
                    .await
            }
            None => {
                cache
                    .renew_snapshot(
                        &self.inner.scope,
                        kind,
                        commit.snapshot,
                        self.inner.clock.now(),
                        cancellation.clone(),
                    )
                    .await
            }
        }
        .map_err(Error::from);
        cancellation.check()?;
        let (outcome, effect) = match result {
            Ok(renewal) => {
                let (snapshot, outcome) = match renewal {
                    MarkerSnapshotRenewal::Stored(snapshot) => {
                        (Some(snapshot), crate::MarkerSnapshotWriteOutcome::Stored)
                    }
                    MarkerSnapshotRenewal::KeptNewer(snapshot) => {
                        (Some(snapshot), crate::MarkerSnapshotWriteOutcome::KeptNewer)
                    }
                    MarkerSnapshotRenewal::Expired => {
                        (None, crate::MarkerSnapshotWriteOutcome::Expired)
                    }
                };
                if let Some(snapshot) = snapshot {
                    self.apply_marker(StoredMarker::new(kind.clone(), snapshot.version()));
                    if !commit.options.skip_memory_write()
                        && let MarkerReads::OptionsControlled(observations) =
                            &self.inner.marker_reads
                    {
                        match commit.local {
                            MarkerLocalCommit::Admitted => {
                                // Renewal must not shorten the independent L1 lifetime.
                                observations
                                    .merge_maximum(
                                        kind,
                                        snapshot.version(),
                                        self.inner.clock.now(),
                                        ContinuityStamp::new(
                                            Arc::clone(&self.inner.epoch),
                                            captured,
                                        ),
                                    )
                                    .await;
                            }
                            MarkerLocalCommit::AwaitingFence {
                                observation,
                                created,
                                jitter,
                            } => {
                                observations
                                    .store(
                                        kind,
                                        observation.reconcile_maximum(Some(snapshot.version())),
                                        &commit.options,
                                        created,
                                        jitter,
                                        ContinuityStamp::new(
                                            Arc::clone(&self.inner.epoch),
                                            captured,
                                        ),
                                    )
                                    .await?;
                            }
                        }
                    }
                }
                let effect = if snapshot.is_some() {
                    EffectOutcome::Applied
                } else {
                    EffectOutcome::Skipped(SkipReason::PhysicallyExpired)
                };
                (outcome, effect)
            }
            Err(Error::Marker(error)) => {
                return self.marker_snapshot_failure(commit, error, policy);
            }
            Err(error) => return Err(error),
        };
        self.memory.emit_lazy(|| CacheEvent::MarkerSnapshotWrite {
            kind: kind.clone(),
            outcome,
        });
        Ok(effect)
    }

    async fn record_control_observation(
        &self,
        observations: &MarkerObservations,
        kind: &MarkerKind,
        presence: MarkerPresence,
        captured: u64,
    ) -> Result<MarkerReadOutcome> {
        if let MarkerPresence::Present(version) = presence {
            self.apply_marker(StoredMarker::new(kind.clone(), version));
        }
        let options = &self.inner.tags_default_options;
        let observation = observations
            .store(
                kind,
                MarkerObservation::Confirmed(presence)
                    .reconcile_maximum(self.inner.tags.marker_version(kind)),
                options,
                self.inner.clock.now(),
                self.jitter_sample(options)?,
                ContinuityStamp::new(Arc::clone(&self.inner.epoch), captured),
            )
            .await?;
        Ok(observation.observed_outcome())
    }

    fn suppressed_marker_fault(
        &self,
        kind: &MarkerKind,
        error: MarkerError,
    ) -> Result<MarkerProviderFault> {
        let options = &self.inner.tags_default_options;
        let failure = match &error {
            MarkerError::Backend { .. } if !options.rethrow_distributed_exceptions() => {
                MarkerProviderFault::Backend
            }
            MarkerError::Protocol { .. } | MarkerError::ProtocolWithSource { .. }
                if !options.rethrow_serialization_exceptions() =>
            {
                MarkerProviderFault::Protocol
            }
            MarkerError::Unsupported
            | MarkerError::BlankWireVersion
            | MarkerError::ZeroCapacity
            | MarkerError::ScopeCapacity { .. }
            | MarkerError::Backend { .. }
            | MarkerError::Protocol { .. }
            | MarkerError::ProtocolWithSource { .. } => return Err(error.into()),
        };
        tracing::warn!(%error, ?kind, "marker provider uses explicitly selected degradation policy");
        Ok(failure)
    }

    async fn marker_degraded(
        &self,
        observations: &MarkerObservations,
        kind: &MarkerKind,
        cached: Option<&Entry<MarkerObservation>>,
        failure: MarkerReadFailure,
    ) -> Result<MarkerReadOutcome> {
        if self.inner.tags_default_options.is_fail_safe_enabled()
            && let Some(source) = cached
            && source.is_read_eligible()
            && !source.is_physically_expired(self.inner.clock.now())
        {
            observations
                .retain_fallback(
                    kind,
                    source,
                    &self.inner.tags_default_options,
                    self.inner.clock.now(),
                    failure,
                )
                .await?;
            Ok(MarkerReadOutcome::StaleFallback(failure))
        } else {
            Ok(MarkerReadOutcome::Unavailable(failure))
        }
    }

    async fn fetch_control_marker(
        &self,
        store: Arc<dyn InvalidationStore>,
        kind: MarkerKind,
        has_fallback: bool,
        cancellation: &FactoryCancellation,
    ) -> Result<MarkerFetch> {
        let options = &self.inner.tags_default_options;
        let timeout = options.appropriate_distributed_timeout(has_fallback);
        let soft = options.is_fail_safe_enabled()
            && has_fallback
            && timeout == options.distributed_soft_timeout()
            && timeout != options.distributed_hard_timeout();
        let source = CancellationSource::new();
        let phase = source.token();
        let scope = self.inner.scope.clone();
        let mut execution = self.scopes().execution(
            async move {
                store
                    .read_with_cancellation(&scope, &kind, phase)
                    .await
                    .map_err(Error::from)
            },
            source,
        );
        execution.link(cancellation, LinkMode::Explicit);
        match bounded(timeout, &mut execution).await? {
            Some(result) => result.map(|version| {
                MarkerFetch::Observed(match version {
                    Some(version) => MarkerPresence::Present(version),
                    None => MarkerPresence::Absent,
                })
            }),
            None => {
                execution.cancel(if soft {
                    Reason::SoftTimeout
                } else {
                    Reason::HardTimeout
                });
                Ok(MarkerFetch::Deadline(if soft {
                    MarkerReadFailure::SoftTimeout
                } else {
                    MarkerReadFailure::HardTimeout
                }))
            }
        }
    }
    pub(super) async fn reconcile_markers(&self, tags: &[Tag]) -> Result<()> {
        if self.inner.disable_tagging {
            return Ok(());
        }
        if let MarkerAccess::Durable(store) = &self.inner.markers {
            let mut kinds = Vec::with_capacity(tags.len() + 2);
            kinds.push(MarkerKind::ClearRemove);
            kinds.push(MarkerKind::ClearExpire);
            kinds.extend(tags.iter().cloned().map(MarkerKind::Tag));
            for marker in store.read_many(&self.inner.scope, &kinds).await? {
                self.apply_marker(marker);
            }
        }
        Ok(())
    }
    pub(super) fn apply_marker(&self, marker: StoredMarker) {
        // A delayed durable/control marker cannot evict a snapshot created after
        // that marker. Reads apply the registry verdict to each exact snapshot.
        self.inner
            .tags
            .advance(marker.kind().clone(), marker.version());
    }

    pub(super) async fn seed_marker(
        &self,
        marker: &StoredMarker,
        options: &EntryOptions,
    ) -> Result<()> {
        let MarkerReads::OptionsControlled(observations) = &self.inner.marker_reads else {
            return Ok(());
        };
        let captured = self.inner.epoch.load(Ordering::Acquire);
        let observation = observations
            .store(
                marker.kind(),
                MarkerObservation::Confirmed(MarkerPresence::Present(marker.version()))
                    .reconcile_maximum(self.inner.tags.marker_version(marker.kind())),
                options,
                self.inner.clock.now(),
                self.jitter_sample(options)?,
                ContinuityStamp::new(Arc::clone(&self.inner.epoch), captured),
            )
            .await?;
        if self.marker_clear_shortcut() && self.inner.epoch.load(Ordering::Acquire) == captured {
            observations.initialize_clear(marker.kind(), captured, observation.outcome());
        }
        Ok(())
    }
    pub(super) async fn mutate_markers(
        &self,
        kinds: Vec<MarkerKind>,
        options: Option<EntryOptions>,
        cancellation: FactoryCancellation,
    ) -> Result<Observed<MutationReceipt>> {
        let opts = options.unwrap_or_else(|| self.inner.tags_default_options.clone());
        self.validate_marker_options(&opts)?;
        if self.inner.disable_tagging || matches!(self.inner.markers, MarkerAccess::Unavailable) {
            return Err(MarkerError::Unsupported.into());
        }
        let now = self.inner.clock.now();
        let mut commands = Vec::with_capacity(kinds.len());
        for kind in kinds {
            let outcome = self
                .inner
                .tags
                .advance(kind.clone(), MarkerVersion::new(now));
            let marker = outcome.marker().clone();
            self.seed_marker(&marker, &opts).await?;
            if matches!(kind, MarkerKind::ClearRemove) {
                self.memory.invalidate_all()?;
            }
            match &kind {
                MarkerKind::Tag(tag) => self.emit(CacheEvent::RemoveByTag {
                    tag: tag.as_str().to_owned(),
                }),
                MarkerKind::ClearExpire | MarkerKind::ClearRemove => self.emit(CacheEvent::Clear),
            }
            commands.push(MarkerCommand::new(
                Arc::clone(&self.inner.instance_id),
                self.inner.scope.clone(),
                marker,
            )?);
            if let MarkerAdvanceOutcome::Compacted { clear_remove, .. } = outcome {
                self.memory.invalidate_all()?;
                self.seed_marker(
                    &StoredMarker::new(MarkerKind::ClearRemove, clear_remove),
                    &opts,
                )
                .await?;
                commands.push(MarkerCommand::new(
                    Arc::clone(&self.inner.instance_id),
                    self.inner.scope.clone(),
                    StoredMarker::new(MarkerKind::ClearRemove, clear_remove),
                )?);
            }
        }
        let worker = self.clone();
        let mode = if opts.allow_background_distributed_operations()
            && !opts.skip_distributed_write()
            && matches!(self.inner.markers, MarkerAccess::Durable(_))
        {
            CommitMode::Background
        } else {
            CommitMode::Foreground
        };
        let work = async move {
            let mut reports = Vec::with_capacity(commands.len());
            for command in commands {
                reports.push(
                    worker
                        .commit_marker(command, opts.clone(), now, &cancellation)
                        .await?,
                );
            }
            if reports
                .iter()
                .any(|receipt| matches!(receipt, MutationReceipt::Scheduled(_)))
            {
                let key: Arc<str> = Arc::from("invalidation");
                worker
                    .commit_receipt(key, CommitMode::Background, async move {
                        let mut completed = Vec::with_capacity(reports.len());
                        for receipt in reports {
                            completed.push(receipt.wait().await?);
                        }
                        Ok(crate::commit::reports(completed))
                    })
                    .await
            } else {
                let mut completed = Vec::with_capacity(reports.len());
                for receipt in reports {
                    completed.push(receipt.wait().await?);
                }
                Ok(MutationReceipt::Completed(crate::commit::reports(
                    completed,
                )))
            }
        };
        let receipt = self
            .pipeline_receipt(Arc::from("invalidation"), mode, work)
            .await?;
        Ok(Observed::new(receipt, OperationOutcome::Invalidated, None))
    }
    async fn commit_marker(
        &self,
        mut command: MarkerCommand,
        opts: EntryOptions,
        created: Timestamp,
        cancellation: &FactoryCancellation,
    ) -> Result<MutationReceipt> {
        let lane_guard = self
            .memory
            .guard(Arc::clone(&self.inner.marker_lane).lock_owned().await);
        let mut notifications = Vec::with_capacity(2);
        let distributed = match &self.inner.markers {
            MarkerAccess::Local => EffectOutcome::NotConfigured,
            MarkerAccess::Unavailable => return Err(MarkerError::Unsupported.into()),
            MarkerAccess::Durable(_) if opts.skip_distributed_write() => {
                EffectOutcome::Skipped(SkipReason::Policy)
            }
            MarkerAccess::Durable(store) => {
                match store
                    .advance(
                        command.scope(),
                        command.marker().kind().clone(),
                        command.marker().version(),
                    )
                    .await
                {
                    Ok(outcome) => {
                        self.apply_marker(outcome.marker().clone());
                        self.seed_marker(outcome.marker(), &opts).await?;
                        command = MarkerCommand::new(
                            Arc::clone(&self.inner.instance_id),
                            self.inner.scope.clone(),
                            outcome.marker().clone(),
                        )?;
                        if let MarkerAdvanceOutcome::Compacted { clear_remove, .. } = outcome {
                            let clear = MarkerCommand::new(
                                Arc::clone(&self.inner.instance_id),
                                self.inner.scope.clone(),
                                StoredMarker::new(MarkerKind::ClearRemove, clear_remove),
                            )?;
                            self.apply_marker(clear.marker().clone());
                            self.seed_marker(clear.marker(), &opts).await?;
                            notifications.push(clear);
                        }
                        EffectOutcome::Applied
                    }
                    Err(error) => {
                        let queued =
                            self.queue_marker_mutation(command.clone(), opts.clone(), created)?;
                        let error = Error::from(error);
                        self.failure(&Arc::from("invalidation"), &error);
                        if opts.rethrow_distributed_exceptions() {
                            return Err(error);
                        }
                        return Ok(MutationReceipt::Completed(CommitReport {
                            local: LocalEffect::Invalidated,
                            distributed: if queued {
                                EffectOutcome::RecoveryQueued { cause: error }
                            } else {
                                EffectOutcome::FailedSuppressed { cause: error }
                            },
                            backplane: EffectOutcome::Skipped(SkipReason::Policy),
                        }));
                    }
                }
            }
        };
        notifications.push(command);
        let distributed = self
            .populate_marker_mutation(&notifications, &opts, created, distributed, cancellation)
            .await?;
        let worker = self.clone();
        let mode = if opts.allow_background_backplane_operations()
            && !opts.skip_backplane_notifications()
            && self.inner.backplane.is_some()
        {
            CommitMode::Background
        } else {
            CommitMode::Foreground
        };
        self.commit_receipt(Arc::from("invalidation"), mode, async move {
            let _lane_guard = lane_guard;
            let backplane = if opts.skip_backplane_notifications() {
                EffectOutcome::Skipped(SkipReason::Policy)
            } else if worker.inner.backplane.is_none() {
                EffectOutcome::NotConfigured
            } else {
                let mut outcomes = Vec::with_capacity(notifications.len());
                for command in notifications {
                    if let Err(error) = worker
                        .publish(BackplaneCommand::Marker(command.clone()))
                        .await
                    {
                        worker.failure(&Arc::from("invalidation"), &error);
                        let queued = worker.queue_marker(command, MarkerReplay::NotifyOnly)?;
                        if opts.rethrow_backplane_exceptions() {
                            return Err(error);
                        }
                        outcomes.push(if queued {
                            EffectOutcome::RecoveryQueued { cause: error }
                        } else {
                            EffectOutcome::FailedSuppressed { cause: error }
                        });
                    } else {
                        outcomes.push(EffectOutcome::Applied);
                    }
                }
                crate::commit::effects(outcomes)
            };
            Ok(CommitReport {
                local: LocalEffect::Invalidated,
                distributed,
                backplane,
            })
        })
        .await
    }
    pub(super) fn queue_marker(&self, command: MarkerCommand, stage: MarkerReplay) -> Result<bool> {
        match &self.inner.recovery {
            Some(recovery) => Ok(matches!(
                recovery.enqueue_marker(command, stage)?,
                EnqueueOutcome::Queued(_) | EnqueueOutcome::Replaced(_)
            )),
            None => Ok(false),
        }
    }
}
