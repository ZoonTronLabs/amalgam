//! Real secondary reads for memory-only caches sharing an external marker L1.
use super::{
    Arc, CacheEvent, CancellationSource, ContinuityStamp, Entry, FactoryCancellation,
    MarkerFactoryMode, MarkerFactoryStale, MarkerKind, MarkerLease, MarkerLookup,
    MarkerObservation, MarkerObservations, MarkerPresence, MarkerReadFailure, MarkerReadOutcome,
    MarkerReads, MarkerRefresh, Ordering, Result, ShutdownTask, Worker,
};
use crate::memory::MemoryAdmission;

impl<V: Clone + Send + Sync + 'static> Worker<V> {
    pub(super) async fn check_local_markers(
        &self,
        entry: &Entry<V>,
        cancellation: &FactoryCancellation,
        observations: &MarkerObservations,
    ) -> Result<()> {
        for kind in Self::secondary_marker_kinds(entry.meta().tags()) {
            cancellation.check()?;
            self.read_local_control_marker(observations, &kind, cancellation)
                .await?;
            if self.marker_invalidates_snapshot(&kind, entry.meta().created()) {
                break;
            }
        }
        cancellation.check()
    }

    async fn read_local_control_marker(
        &self,
        observations: &MarkerObservations,
        kind: &MarkerKind,
        cancellation: &FactoryCancellation,
    ) -> Result<()> {
        self.validate_execution_options(
            &self.inner.tags_default_options,
            super::OptionsTarget::Marker,
        )?;
        let captured = self.inner.epoch.load(Ordering::Acquire);
        let outcome = match self.marker_lookup(observations, kind, captured).await? {
            MarkerLookup::Ready(outcome) => outcome,
            MarkerLookup::Refresh(before) => {
                self.refresh_local_control_marker(
                    observations,
                    kind,
                    before.as_ref(),
                    captured,
                    cancellation,
                )
                .await?
            }
        };
        cancellation.check()?;
        self.maybe_eager_local_marker(observations, kind, captured, cancellation)
            .await?;
        self.marker_event(kind, outcome);
        Ok(())
    }

    async fn refresh_local_control_marker(
        &self,
        observations: &MarkerObservations,
        kind: &MarkerKind,
        before: Option<&Entry<MarkerObservation>>,
        captured: u64,
        cancellation: &FactoryCancellation,
    ) -> Result<MarkerReadOutcome> {
        let key = observations.lock_key(kind);
        let guard = observations
            .locks
            .acquire(
                &key,
                crate::MemoryLockKind::Marker(kind.clone()),
                self.marker_lock_timeout(before),
                cancellation,
                self.memory_acquire_route(),
            )
            .await?
            .map(|guard| self.memory.guard(guard));
        cancellation.check()?;
        if guard.is_none() && self.marker_fallback_eligible(before) {
            return Ok(MarkerReadOutcome::StaleFallback(
                MarkerReadFailure::LockTimeout,
            ));
        }
        match self.marker_lookup(observations, kind, captured).await? {
            MarkerLookup::Ready(outcome) => Ok(outcome),
            MarkerLookup::Refresh(cached) => {
                self.complete_local_marker_factory(
                    observations,
                    kind,
                    cached.as_ref(),
                    captured,
                    cancellation,
                    MarkerFactoryMode::Foreground,
                )
                .await
            }
        }
    }

    async fn complete_local_marker_factory(
        &self,
        observations: &MarkerObservations,
        kind: &MarkerKind,
        cached: Option<&Entry<MarkerObservation>>,
        captured: u64,
        cancellation: &FactoryCancellation,
        mode: MarkerFactoryMode,
    ) -> Result<MarkerReadOutcome> {
        let observation = self
            .known_control_factory_observation(kind, cached)
            .unwrap_or(MarkerObservation::Local(MarkerPresence::Absent));
        self.complete_control_marker_factory(
            observations,
            observation,
            cached.map_or(MarkerFactoryStale::Absent, MarkerFactoryStale::Memory),
            MarkerRefresh {
                kind,
                captured,
                cancellation,
                mode,
            },
            MarkerLease::Unleased,
        )
        .await
    }

    async fn maybe_eager_local_marker(
        &self,
        observations: &MarkerObservations,
        kind: &MarkerKind,
        captured: u64,
        cancellation: &FactoryCancellation,
    ) -> Result<()> {
        if self
            .inner
            .tags_default_options
            .eager_refresh_threshold()
            .is_none()
        {
            return Ok(());
        }
        let Some(current) = self.marker_cached(observations, kind).await? else {
            return Ok(());
        };
        let now = self.inner.clock.now();
        if !current.should_eager_refresh(now) || !current.is_read_eligible() {
            return Ok(());
        }
        let key = observations.memory.key(kind);
        let claim = observations
            .memory
            .insert_if_unchanged(
                Arc::clone(&key),
                Some(&current),
                current.without_eager_refresh(),
                now,
            )
            .await?;
        cancellation.check()?;
        if !matches!(claim, MemoryAdmission::Admitted | MemoryAdmission::Replaced)
            || self.inner.epoch.load(Ordering::Acquire) != captured
        {
            return Ok(());
        }
        let Some(local) = observations.locks.try_acquire(
            &observations.lock_key(kind),
            crate::MemoryLockKind::Marker(kind.clone()),
            cancellation,
        )?
        else {
            return Ok(());
        };
        let local = self.memory.guard(local);
        self.memory
            .emit_lazy(|| CacheEvent::MarkerEagerRefresh { kind: kind.clone() });
        let worker = self.clone();
        let kind = kind.clone();
        let source = CancellationSource::new();
        let token = source.token();
        let execution = self.scopes().execution(
            async move {
                let _local = local;
                token.check()?;
                let stamp = ContinuityStamp::new(Arc::clone(&worker.inner.epoch), captured);
                if !stamp.is_current() {
                    return Ok(());
                }
                let MarkerReads::OptionsControlled(observations) = &worker.inner.marker_reads
                else {
                    return Ok(());
                };
                let cached = worker.marker_cached(observations, &kind).await?;
                worker
                    .complete_local_marker_factory(
                        observations,
                        &kind,
                        cached.as_ref(),
                        captured,
                        &token,
                        MarkerFactoryMode::Eager,
                    )
                    .await?;
                token.check()
            },
            source,
        );
        let _completion = self.inner.tasks.spawn(
            ShutdownTask::Factory,
            key,
            self.inner.events.clone(),
            execution,
        );
        Ok(())
    }
}
