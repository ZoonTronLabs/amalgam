//! Owned eager marker refresh, separate from ordinary value factories.
use super::{
    Arc, CacheEvent, CancellationSource, ContinuityStamp, Entry, FactoryCancellation,
    MarkerFactoryMode, MarkerFactoryRead, MarkerKind, MarkerLeaseAcquisition,
    MarkerLifecycleAccess, MarkerObservation, MarkerObservations, MarkerReads, MarkerRefresh,
    MarkerSnapshotFetch, MarkerSnapshotRead, MarkerUnobservedRead, Ordering, Result, ShutdownTask,
    StoredMarker, Worker,
};
use crate::memory::MemoryAdmission;

impl<V: Clone + Send + Sync + 'static> Worker<V> {
    pub(super) async fn maybe_eager_control_marker(
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
            || !matches!(
                observations.lifecycle,
                MarkerLifecycleAccess::CachedSnapshots(_)
            )
            || self.marker_clear_shortcut() && observations.clear_status(kind, captured).is_some()
        {
            return Ok(());
        }
        let Some(current) = self.marker_cached(observations, kind).await else {
            return Ok(());
        };
        let now = self.inner.clock.now();
        if !current.should_eager_refresh(now) || !current.is_read_eligible() {
            return Ok(());
        }
        let key = MarkerObservations::key(kind);
        let claim = observations
            .memory
            .insert_if_unchanged(
                Arc::clone(&key),
                Some(&current),
                current.without_eager_refresh(),
                now,
            )
            .await;
        cancellation.check()?;
        if !matches!(claim, MemoryAdmission::Admitted | MemoryAdmission::Replaced)
            || self.inner.epoch.load(Ordering::Acquire) != captured
        {
            return Ok(());
        }
        let Some(local) = observations.locks.try_acquire(
            &key,
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
                worker
                    .eager_control_marker(kind, current, captured, &token)
                    .await
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

    async fn eager_control_marker(
        &self,
        kind: MarkerKind,
        current: Entry<MarkerObservation>,
        captured: u64,
        cancellation: &FactoryCancellation,
    ) -> Result<()> {
        cancellation.check()?;
        let stamp = ContinuityStamp::new(Arc::clone(&self.inner.epoch), captured);
        if !stamp.is_current() || !current.is_read_eligible() {
            return Ok(());
        }
        let MarkerReads::OptionsControlled(observations) = &self.inner.marker_reads else {
            return Ok(());
        };
        let MarkerLifecycleAccess::CachedSnapshots(cache) = &observations.lifecycle else {
            return Ok(());
        };
        let options = &self.inner.tags_default_options;
        let read = if options.skip_distributed_read() || options.skip_distributed_read_when_stale()
        {
            MarkerFactoryRead::Unobserved(MarkerUnobservedRead::Skipped)
        } else {
            match self
                .fetch_control_snapshot(Arc::clone(cache), kind.clone(), true, cancellation)
                .await
            {
                Ok(MarkerSnapshotFetch::Read(read)) => MarkerFactoryRead::Read(read),
                Ok(MarkerSnapshotFetch::Deadline(failure)) => {
                    MarkerFactoryRead::Unobserved(MarkerUnobservedRead::Unavailable(failure))
                }
                Err(crate::Error::Marker(error)) => {
                    MarkerFactoryRead::Unobserved(MarkerUnobservedRead::Unavailable(
                        self.suppressed_marker_fault(&kind, error)?.read_failure(),
                    ))
                }
                Err(error) => return Err(error),
            }
        };
        cancellation.check()?;
        if !stamp.is_current() {
            return Ok(());
        }
        if let MarkerFactoryRead::Read(read) = read {
            if let Some(maximum) = read.maximum() {
                self.apply_marker(StoredMarker::new(kind.clone(), maximum));
            }
            if let MarkerSnapshotRead::Snapshot(snapshot) = read
                && (snapshot.created() > current.meta().created()
                    || snapshot.logical_expiration() > current.meta().logical_expiration())
                && snapshot.is_fresh(self.inner.clock.now())
            {
                self.record_control_snapshot(observations, &kind, snapshot, captured)
                    .await?;
                return Ok(());
            }
        }
        let lease = match self
            .acquire_control_marker_lease(&kind, cancellation, MarkerFactoryMode::Eager)
            .await?
        {
            MarkerLeaseAcquisition::Acquired(lease) => lease,
            MarkerLeaseAcquisition::Contended => return Ok(()),
        };
        self.resolve_leased_control_snapshot(
            observations,
            Some(&current),
            read,
            MarkerRefresh {
                kind: &kind,
                captured,
                cancellation,
                mode: MarkerFactoryMode::Eager,
            },
            lease,
        )
        .await?;
        Ok(())
    }
}
