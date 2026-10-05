//! Original-policy observation replay and durable mutation stage transitions.
use super::*;
use crate::recovery::{MarkerMutationStage, RecoveryError, RecoveryStageTransition};

enum CompactionReplay {
    Continued(Box<MarkerMutationRecovery>),
    Stopped(crate::ReplayOutcome),
}

impl<V: Clone + Send + Sync + 'static> Worker<V> {
    fn marker_snapshot_provider(&self) -> Option<Arc<dyn MarkerSnapshotCache>> {
        match &self.inner.marker_reads {
            MarkerReads::DurableRequired => None,
            MarkerReads::OptionsControlled(observations) => match &observations.lifecycle {
                MarkerLifecycleAccess::DurableOnly => None,
                MarkerLifecycleAccess::CachedSnapshots(cache) => Some(Arc::clone(cache)),
            },
        }
    }

    pub(super) fn queue_marker_mutation(
        &self,
        command: MarkerCommand,
        options: EntryOptions,
        created: Timestamp,
    ) -> Result<bool> {
        let Some(recovery) = &self.inner.recovery else {
            return Ok(false);
        };
        Ok(matches!(
            recovery.enqueue_marker_mutation(MarkerMutationRecovery::capture(
                command, options, created
            )?)?,
            EnqueueOutcome::Queued(_) | EnqueueOutcome::Replaced(_)
        ))
    }

    fn queue_marker_snapshot(&self, commit: &MarkerSnapshotCommit) -> Result<bool> {
        let Some(recovery) = &self.inner.recovery else {
            return Ok(false);
        };
        let participation = match &commit.lease {
            MarkerLease::Unleased => MarkerSnapshotParticipation::Unleased,
            MarkerLease::Fenced(_) => MarkerSnapshotParticipation::Fenced,
            MarkerLease::Cooperative(_) => MarkerSnapshotParticipation::Cooperative,
        };
        let work = MarkerSnapshotReplay::capture(
            self.inner.scope.clone(),
            commit.kind.clone(),
            commit.snapshot,
            commit.options.clone(),
            participation,
        )?;
        Ok(matches!(
            recovery.enqueue_marker_snapshot(work)?,
            EnqueueOutcome::Queued(_) | EnqueueOutcome::Replaced(_)
        ))
    }

    pub(super) fn marker_snapshot_failure(
        &self,
        commit: &MarkerSnapshotCommit,
        error: MarkerError,
        policy: MarkerSnapshotWritePolicy,
    ) -> Result<EffectOutcome> {
        let (fault, rethrow) = match &error {
            MarkerError::Backend { .. } => (
                MarkerProviderFault::Backend,
                commit.options.rethrow_distributed_exceptions(),
            ),
            MarkerError::Protocol { .. } | MarkerError::ProtocolWithSource { .. } => (
                MarkerProviderFault::Protocol,
                commit.options.rethrow_serialization_exceptions(),
            ),
            MarkerError::Unsupported
            | MarkerError::BlankWireVersion
            | MarkerError::ZeroCapacity
            | MarkerError::ScopeCapacity { .. } => return Err(error.into()),
        };
        self.inner
            .events
            .emit_lazy(|| CacheEvent::MarkerSnapshotWrite {
                kind: commit.kind.clone(),
                outcome: fault.write_outcome(),
            });
        if matches!(policy, MarkerSnapshotWritePolicy::Replay) {
            return Err(error.into());
        }
        let queued = self.queue_marker_snapshot(commit)?;
        tracing::warn!(%error, kind = ?commit.kind, queued, "marker snapshot write failed");
        if rethrow {
            return Err(error.into());
        }
        let cause = error.into();
        Ok(if queued {
            EffectOutcome::RecoveryQueued { cause }
        } else {
            EffectOutcome::FailedSuppressed { cause }
        })
    }

    async fn populate_marker_snapshot(
        &self,
        command: &MarkerCommand,
        options: &EntryOptions,
        created: Timestamp,
        cancellation: &FactoryCancellation,
        policy: MarkerSnapshotWritePolicy,
    ) -> Result<EffectOutcome> {
        if options.skip_distributed_write() {
            return Ok(EffectOutcome::Skipped(SkipReason::Policy));
        }
        let Some(cache) = self.marker_snapshot_provider() else {
            return Ok(EffectOutcome::NotConfigured);
        };
        let snapshot = MarkerSnapshot::fresh(command.marker().version(), options, created);
        if snapshot.is_physically_expired(self.inner.clock.now()) {
            return Ok(EffectOutcome::Skipped(SkipReason::PhysicallyExpired));
        }
        let commit = MarkerSnapshotCommit {
            kind: command.marker().kind().clone(),
            snapshot,
            options: options.clone(),
            captured: self.inner.epoch.load(Ordering::Acquire),
            // Explicit tag/clear Set is unleased; factory repairs capture their actual lease.
            lease: MarkerLease::Unleased,
            local: MarkerLocalCommit::Admitted,
        };
        self.commit_control_snapshot(cache, &commit, cancellation.clone(), policy)
            .await
    }

    pub(super) async fn populate_marker_mutation(
        &self,
        commands: &[MarkerCommand],
        options: &EntryOptions,
        created: Timestamp,
        durable: EffectOutcome,
        cancellation: &FactoryCancellation,
    ) -> Result<EffectOutcome> {
        if options.skip_distributed_write() || self.marker_snapshot_provider().is_none() {
            return Ok(durable);
        }
        // Preserve the ordinary nonparticipating mutation future and default hot path.
        Box::pin(self.populate_marker_commands(commands, options, created, durable, cancellation))
            .await
    }

    async fn populate_marker_commands(
        &self,
        commands: &[MarkerCommand],
        options: &EntryOptions,
        created: Timestamp,
        durable: EffectOutcome,
        parent: &FactoryCancellation,
    ) -> Result<EffectOutcome> {
        let source = CancellationSource::new();
        let cancellation = source.token();
        let worker = self.clone();
        let commands = commands.to_vec().into_boxed_slice();
        let link = if options.allow_background_distributed_operations() {
            LinkMode::CallerScope
        } else {
            LinkMode::Explicit
        };
        let options = options.clone();
        let execution = self.inner.scopes.execution(
            async move {
                worker
                    .populate_marker_commands_owned(
                        &commands,
                        &options,
                        created,
                        durable,
                        cancellation,
                    )
                    .await
            },
            source,
        );
        execution.link(parent, link);
        execution.await
    }

    async fn populate_marker_commands_owned(
        &self,
        commands: &[MarkerCommand],
        options: &EntryOptions,
        created: Timestamp,
        durable: EffectOutcome,
        cancellation: FactoryCancellation,
    ) -> Result<EffectOutcome> {
        let mut outcomes = Vec::with_capacity(commands.len() + 1);
        outcomes.push(durable);
        let mut failure = None;
        for command in commands {
            match self
                .populate_marker_snapshot(
                    command,
                    options,
                    created,
                    &cancellation,
                    MarkerSnapshotWritePolicy::Operation,
                )
                .await
            {
                Ok(outcome) => outcomes.push(outcome),
                Err(error) => {
                    if failure.is_none() {
                        failure = Some(error);
                    }
                }
            }
        }
        if let Some(error) = failure {
            // The durable fact is already committed. Preserve all publication obligations,
            // even when strict snapshot policy returns the original storage failure.
            if !options.skip_backplane_notifications() && self.inner.backplane.is_some() {
                for command in commands {
                    self.queue_marker(command.clone(), MarkerReplay::NotifyOnly)?;
                }
            }
            return Err(error);
        }
        Ok(crate::commit::effects(outcomes))
    }

    async fn reacquire_marker_snapshot(
        &self,
        work: &MarkerSnapshotReplay,
        cancellation: &FactoryCancellation,
    ) -> Result<MarkerLease> {
        let policy = match work.participation() {
            MarkerSnapshotParticipation::Unleased => return Ok(MarkerLease::Unleased),
            MarkerSnapshotParticipation::Fenced => AcquisitionPolicy::TokenOwned,
            MarkerSnapshotParticipation::Cooperative => AcquisitionPolicy::LegacyBackendContract,
        };
        cancellation.check()?;
        let locker = Arc::clone(
            self.inner
                .distributed_locker
                .as_ref()
                .ok_or(LeaseError::UnsupportedFencing)?,
        );
        let key = MarkerLeaseKey::new(work.scope(), work.kind()).into_arc();
        let owner = self.lease_owner(&key);
        let ttl = self.inner.lease_ttl;
        let timeout = work.options().distributed_lock_timeout();
        let execution = self.inner.scopes.execution(
            async move {
                acquire_owned_supervised(locker, key, ttl, timeout, policy, owner)
                    .await
                    .map_err(Error::from)
            },
            CancellationSource::new(),
        );
        execution.link(cancellation, LinkMode::Explicit);
        let lease = execution.await?.ok_or(LeaseError::AcquisitionTimeout)?;
        cancellation.check()?;
        Ok(match work.participation() {
            MarkerSnapshotParticipation::Unleased => {
                return Err(RecoveryError::InvalidMarkerStage.into());
            }
            MarkerSnapshotParticipation::Fenced => MarkerLease::Fenced(lease),
            MarkerSnapshotParticipation::Cooperative => MarkerLease::Cooperative(lease),
        })
    }

    pub(in crate::cache) async fn replay_marker_snapshot(
        &self,
        ticket: &crate::ReplayTicket,
        work: &MarkerSnapshotReplay,
        cancellation: &FactoryCancellation,
    ) -> Result<crate::ReplayOutcome> {
        if work.scope() != &self.inner.scope {
            return Err(RecoveryError::MarkerIdentityChanged.into());
        }
        let Some(cache) = self.marker_snapshot_provider() else {
            return Err(MarkerError::Unsupported.into());
        };
        let lease = self.reacquire_marker_snapshot(work, cancellation).await?;
        let commit = MarkerSnapshotCommit {
            kind: work.kind().clone(),
            snapshot: work.snapshot(),
            options: work.options().clone(),
            captured: self.inner.epoch.load(Ordering::Acquire),
            lease,
            local: MarkerLocalCommit::Admitted,
        };
        let _lane = Arc::clone(&self.inner.marker_lane).lock_owned().await;
        let result = if !self
            .inner
            .recovery
            .as_ref()
            .is_some_and(|recovery| recovery.is_current(ticket))
        {
            Ok(crate::ReplayOutcome::Superseded)
        } else if !self.replay_admitted() {
            Ok(crate::ReplayOutcome::Paused)
        } else {
            self.commit_control_snapshot(
                cache,
                &commit,
                cancellation.clone(),
                MarkerSnapshotWritePolicy::Replay,
            )
            .await
            .and_then(Self::marker_replay_effect)
        };
        self.finish_control_marker_lease(&commit.kind, commit.lease, result)
            .await
    }

    fn marker_replay_effect(effect: EffectOutcome) -> Result<crate::ReplayOutcome> {
        match effect {
            EffectOutcome::Applied | EffectOutcome::NotConfigured => {
                Ok(crate::ReplayOutcome::Applied)
            }
            EffectOutcome::Skipped(SkipReason::PhysicallyExpired) => {
                Ok(crate::ReplayOutcome::Expired)
            }
            EffectOutcome::Skipped(SkipReason::Superseded) => Ok(crate::ReplayOutcome::Paused),
            EffectOutcome::Skipped(SkipReason::Policy) => Ok(crate::ReplayOutcome::Superseded),
            EffectOutcome::Batch(_)
            | EffectOutcome::RecoveryQueued { .. }
            | EffectOutcome::FailedSuppressed { .. } => {
                Err(RecoveryError::InvalidMarkerStage.into())
            }
        }
    }

    async fn advance_captured_marker(
        &self,
        work: &MarkerMutationRecovery,
    ) -> Result<MarkerMutationRecovery> {
        let command = work.command();
        let store = match &self.inner.markers {
            MarkerAccess::Durable(store) => store,
            MarkerAccess::Local | MarkerAccess::Unavailable => {
                return Err(MarkerError::Unsupported.into());
            }
        };
        let outcome = store
            .advance(
                command.scope(),
                command.marker().kind().clone(),
                command.marker().version(),
            )
            .await?;
        self.apply_marker(outcome.marker().clone());
        let committed = MarkerCommand::new(
            Arc::clone(&self.inner.instance_id),
            self.inner.scope.clone(),
            outcome.marker().clone(),
        )?;
        let additional = match outcome {
            MarkerAdvanceOutcome::Advanced(_) => Box::from([]),
            MarkerAdvanceOutcome::Compacted { clear_remove, .. } => {
                let clear = MarkerCommand::new(
                    Arc::clone(&self.inner.instance_id),
                    self.inner.scope.clone(),
                    StoredMarker::new(MarkerKind::ClearRemove, clear_remove),
                )?;
                self.apply_marker(clear.marker().clone());
                Box::from([clear])
            }
        };
        work.population(committed, additional).map_err(Error::from)
    }

    pub(in crate::cache) async fn replay_marker_mutation(
        &self,
        ticket: &crate::ReplayTicket,
        original: &MarkerMutationRecovery,
        cancellation: &FactoryCancellation,
    ) -> Result<crate::ReplayOutcome> {
        if original.command().scope() != &self.inner.scope {
            return Err(RecoveryError::MarkerIdentityChanged.into());
        }
        let recovery = self.inner.recovery.as_ref().ok_or(RecoveryError::Stopped)?;
        let _lane = Arc::clone(&self.inner.marker_lane).lock_owned().await;
        if !recovery.is_current(ticket) {
            return Ok(crate::ReplayOutcome::Superseded);
        }
        if !self.replay_admitted() {
            return Ok(crate::ReplayOutcome::Paused);
        }
        cancellation.check()?;
        let inherited;
        let original: &MarkerMutationRecovery = if let Some(child) = original.pending_compaction() {
            inherited = match self
                .replay_inherited_compaction(ticket, original, child, cancellation)
                .await?
            {
                CompactionReplay::Continued(work) => work,
                CompactionReplay::Stopped(outcome) => return Ok(outcome),
            };
            &inherited
        } else {
            original
        };
        let advanced;
        let work = match original.stage() {
            MarkerMutationStage::Advance { .. } => {
                advanced = self.advance_captured_marker(original).await?;
                if recovery.marker_population_stage(ticket, advanced.clone())
                    == RecoveryStageTransition::Superseded
                {
                    return Ok(crate::ReplayOutcome::Superseded);
                }
                &advanced
            }
            MarkerMutationStage::Populate { .. } | MarkerMutationStage::Notify { .. } => original,
        };
        self.finish_captured_marker(ticket, work, cancellation)
            .await
    }

    async fn replay_inherited_compaction(
        &self,
        ticket: &crate::ReplayTicket,
        parent: &MarkerMutationRecovery,
        child: &MarkerMutationRecovery,
        cancellation: &FactoryCancellation,
    ) -> Result<CompactionReplay> {
        let recovery = self.inner.recovery.as_ref().ok_or(RecoveryError::Stopped)?;
        let notify;
        let parent = match child.stage() {
            MarkerMutationStage::Advance { .. } => {
                return Err(RecoveryError::InvalidMarkerStage.into());
            }
            MarkerMutationStage::Populate {
                options, snapshot, ..
            } => {
                let effect = self
                    .populate_marker_snapshot(
                        child.command(),
                        options,
                        snapshot.created(),
                        cancellation,
                        MarkerSnapshotWritePolicy::Replay,
                    )
                    .await?;
                if matches!(effect, EffectOutcome::Skipped(SkipReason::Superseded)) {
                    return Ok(CompactionReplay::Stopped(crate::ReplayOutcome::Paused));
                }
                if options.skip_backplane_notifications() {
                    return self.complete_inherited_compaction(ticket, parent);
                }
                notify = parent.with_compaction(Some(child.notification()?))?;
                if recovery.marker_population_stage(ticket, notify.clone())
                    == RecoveryStageTransition::Superseded
                {
                    return Ok(CompactionReplay::Stopped(crate::ReplayOutcome::Superseded));
                }
                &notify
            }
            MarkerMutationStage::Notify { .. } => parent,
        };
        cancellation.check()?;
        if !recovery.is_current(ticket) {
            return Ok(CompactionReplay::Stopped(crate::ReplayOutcome::Superseded));
        }
        if !self.replay_admitted() {
            return Ok(CompactionReplay::Stopped(crate::ReplayOutcome::Paused));
        }
        if let Some(backplane) = &self.inner.backplane {
            backplane
                .publish_command(BackplaneCommand::Marker(child.command().clone()))
                .await?;
            self.close_circuit(crate::CircuitComponent::Backplane);
        }
        self.complete_inherited_compaction(ticket, parent)
    }

    fn complete_inherited_compaction(
        &self,
        ticket: &crate::ReplayTicket,
        parent: &MarkerMutationRecovery,
    ) -> Result<CompactionReplay> {
        let recovery = self.inner.recovery.as_ref().ok_or(RecoveryError::Stopped)?;
        let next = parent.with_compaction(None)?;
        if recovery.marker_population_stage(ticket, next.clone())
            == RecoveryStageTransition::Superseded
        {
            return Ok(CompactionReplay::Stopped(crate::ReplayOutcome::Superseded));
        }
        Ok(CompactionReplay::Continued(Box::new(next)))
    }

    async fn finish_captured_marker(
        &self,
        ticket: &crate::ReplayTicket,
        work: &MarkerMutationRecovery,
        cancellation: &FactoryCancellation,
    ) -> Result<crate::ReplayOutcome> {
        let recovery = self.inner.recovery.as_ref().ok_or(RecoveryError::Stopped)?;
        let additional = match work.stage() {
            MarkerMutationStage::Advance { .. } => {
                return Err(RecoveryError::InvalidMarkerStage.into());
            }
            MarkerMutationStage::Notify { additional } => additional,
            MarkerMutationStage::Populate {
                options,
                snapshot,
                additional,
            } => {
                for command in std::iter::once(work.command()).chain(additional.iter()) {
                    cancellation.check()?;
                    if !recovery.is_current(ticket) {
                        return Ok(crate::ReplayOutcome::Superseded);
                    }
                    if !self.replay_admitted() {
                        return Ok(crate::ReplayOutcome::Paused);
                    }
                    let effect = self
                        .populate_marker_snapshot(
                            command,
                            options,
                            snapshot.created(),
                            cancellation,
                            MarkerSnapshotWritePolicy::Replay,
                        )
                        .await?;
                    if matches!(effect, EffectOutcome::Skipped(SkipReason::Superseded)) {
                        return Ok(crate::ReplayOutcome::Paused);
                    }
                }
                if options.skip_backplane_notifications() {
                    return Ok(crate::ReplayOutcome::Applied);
                }
                if recovery.notification_stage(ticket) == RecoveryStageTransition::Superseded {
                    return Ok(crate::ReplayOutcome::Superseded);
                }
                additional
            }
        };
        for command in std::iter::once(work.command()).chain(additional.iter()) {
            cancellation.check()?;
            if !recovery.is_current(ticket) {
                return Ok(crate::ReplayOutcome::Superseded);
            }
            if !self.replay_admitted() {
                return Ok(crate::ReplayOutcome::Paused);
            }
            if let Some(backplane) = &self.inner.backplane {
                backplane
                    .publish_command(BackplaneCommand::Marker(command.clone()))
                    .await?;
                self.close_circuit(crate::CircuitComponent::Backplane);
            }
        }
        Ok(crate::ReplayOutcome::Applied)
    }
}
