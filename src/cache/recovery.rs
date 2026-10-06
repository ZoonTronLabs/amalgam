//! Stage-aware replay of captured mutations.
use super::{
    AcquisitionPolicy, Arc, BackplaneCommand, BackplaneState, CacheInner, CancellationSource,
    CircuitComponent, DataMutation, DistributedSnapshot, Error, FactoryCancellation, FlightGuard,
    LeaseError, LeasePolicy, LocalParticipation, MarkerAccess, MarkerAdvanceOutcome, MarkerCommand,
    MarkerError, MarkerKind, MarkerReplay, PendingMutation, RecoveryAction, RecoveryExecutor,
    RecoveryItem, RecoveryWork, ReplayOutcome, ReplayTicket, Result, Storage, StoredMarker,
    TagVerdict, Timestamp, Worker, acquire_owned_supervised, async_trait,
};

impl<V: Clone + Send + Sync + 'static> Worker<V> {
    pub(super) fn replay_admitted(&self) -> Result<bool> {
        self.ensure_health()?;
        Ok(self
            .inner
            .backplane
            .as_ref()
            .and_then(|backplane| backplane.connection_state())
            .is_none_or(|state| matches!(*state.borrow(), BackplaneState::Connected { .. })))
    }
    async fn replay_legacy(
        &self,
        item: &RecoveryItem,
        cancellation: &FactoryCancellation,
    ) -> Result<()> {
        // Deliberately explicit old executor adapter. Canonical enqueue captures
        // bytes/stages and routes through replay_ticket instead.
        let Storage::Hybrid {
            backend,
            serializer,
        } = &self.inner.storage
        else {
            return Ok(());
        };
        match item.action {
            RecoveryAction::Set => {
                if let Some(entry) = self
                    .memory
                    .get_at(&item.key, self.inner.clock.now())
                    .await?
                {
                    let snapshot = DistributedSnapshot::from_entry_with_options(
                        &entry,
                        &self.inner.default_options,
                        entry.meta().inserted_at(),
                    )?;
                    backend
                        .set(
                            &self.inner.l2_key(&item.key),
                            serializer
                                .encode(&snapshot, self.inner.serialization_mode, cancellation)
                                .await?,
                            Some(snapshot.backend_ttl_at(self.inner.clock.now())),
                        )
                        .await?;
                }
            }
            RecoveryAction::Remove => backend.remove(&self.inner.l2_key(&item.key)).await?,
            RecoveryAction::Expire => {}
        }
        Ok(())
    }
    async fn reconcile_replay(
        &self,
        item: &RecoveryItem,
        mutation: &PendingMutation,
        cancellation: &FactoryCancellation,
    ) -> Result<ReplayOutcome> {
        // Recovery bypasses an open circuit, but never treats unavailable current
        // state as safe absence after a missed notification or restart.
        let Storage::Hybrid {
            backend,
            serializer,
        } = &self.inner.storage
        else {
            return Ok(ReplayOutcome::Applied);
        };
        if let Some(current) = backend.get(&self.inner.l2_key(&item.key)).await? {
            let snapshot = serializer
                .decode(&current, self.inner.serialization_mode, cancellation)
                .await?;
            if snapshot.entry().created_ticks > item.timestamp.ticks() {
                return Ok(ReplayOutcome::Superseded);
            }
        }
        let source = match mutation {
            PendingMutation::Commit { mutation, .. }
            | PendingMutation::FencedCommit { mutation, .. } => match mutation {
                DataMutation::Set { bytes, .. } | DataMutation::Expire { bytes, .. } => Some(
                    serializer
                        .decode(bytes, self.inner.serialization_mode, cancellation)
                        .await?
                        .try_into_entry(self.inner.clock.now())?,
                ),
                DataMutation::Remove => None,
            },
            PendingMutation::Legacy
            | PendingMutation::Notify(_)
            | PendingMutation::ColdExpire { .. } => None,
        };
        self.reconcile_markers(source.as_ref().map_or(&[], |entry| entry.meta().tags()))
            .await?;
        if source
            .as_ref()
            .is_some_and(|source| self.tags(source) != TagVerdict::Valid)
        {
            return Ok(ReplayOutcome::Superseded);
        }
        Ok(ReplayOutcome::Applied)
    }
    async fn expire_current(
        &self,
        key: &str,
        logical_expiration: Timestamp,
        cancellation: &FactoryCancellation,
    ) -> Result<ReplayOutcome> {
        let Storage::Hybrid {
            backend,
            serializer,
        } = &self.inner.storage
        else {
            return Ok(ReplayOutcome::Applied);
        };
        let Some(bytes) = backend.get(&self.inner.l2_key(key)).await? else {
            return Ok(ReplayOutcome::Applied);
        };
        let snapshot = serializer
            .decode(&bytes, self.inner.serialization_mode, cancellation)
            .await?;
        // A remote newer write can arrive between reconciliation and this read.
        if snapshot.entry().created_ticks > logical_expiration.ticks() {
            return Ok(ReplayOutcome::Superseded);
        }
        let mut entry = snapshot.entry().clone();
        entry.logical_expiration_ticks = entry
            .logical_expiration_ticks
            .min(logical_expiration.ticks());
        let expired =
            DistributedSnapshot::new(entry, snapshot.inserted_at(), snapshot.retention())?;
        let data = DataMutation::Expire {
            bytes: serializer
                .encode(&expired, self.inner.serialization_mode, cancellation)
                .await?
                .into(),
            physical_expiration: Timestamp::from_ticks(expired.entry().physical_expiration_ticks),
        };
        self.write_data(key, &data, None).await?;
        Ok(ReplayOutcome::Applied)
    }
    async fn replay_owned(
        &self,
        ticket: ReplayTicket,
        cancellation: &FactoryCancellation,
    ) -> Result<ReplayOutcome> {
        let Some(recovery) = &self.inner.recovery else {
            return Ok(ReplayOutcome::Superseded);
        };
        if !self.replay_admitted()? {
            return Ok(ReplayOutcome::Paused);
        }
        match ticket.work() {
            RecoveryWork::Data { item, mutation } => {
                // Acquire cluster participation before the commit lane: an origin
                // holding that lease must remain free to finish its own commit.
                let flight = if matches!(mutation, PendingMutation::FencedCommit { .. }) {
                    let Some(locker) = &self.inner.distributed_locker else {
                        return Err(LeaseError::UnsupportedFencing.into());
                    };
                    let lease = acquire_owned_supervised(
                        Arc::clone(locker),
                        Arc::from(format!("amalgam:lock:{}", self.inner.l2_key(&item.key))),
                        self.inner.lease_ttl,
                        self.inner.default_options.distributed_lock_timeout(),
                        AcquisitionPolicy::TokenOwned,
                        self.lease_owner(&item.key),
                    )
                    .await?
                    .ok_or(LeaseError::AcquisitionTimeout)?;
                    Some(FlightGuard {
                        local: LocalParticipation::ReplayOnly,
                        _reclamation: self.memory.fence(),
                        lease: Some(lease),
                        tasks: Arc::clone(&self.inner.tasks),
                        events: self.inner.events.clone(),
                        key: Arc::clone(&item.key),
                        policy: LeasePolicy::Fenced,
                    })
                } else {
                    None
                };
                let lane = self.inner.lanes.get(&item.key);
                let _lane = self.memory.guard(Arc::clone(&lane.lock).lock_owned().await);
                if !recovery.is_current(&ticket) {
                    return Ok(ReplayOutcome::Superseded);
                }
                if !self.replay_admitted()? {
                    return Ok(ReplayOutcome::Paused);
                }
                let reconciled = self.reconcile_replay(item, mutation, cancellation).await?;
                if reconciled != ReplayOutcome::Applied {
                    return Ok(reconciled);
                }
                if !recovery.is_current(&ticket) {
                    return Ok(ReplayOutcome::Superseded);
                }
                if !self.replay_admitted()? {
                    return Ok(ReplayOutcome::Paused);
                }
                let command = match mutation {
                    PendingMutation::Legacy => {
                        self.replay_legacy(item, cancellation).await?;
                        None
                    }
                    PendingMutation::Notify(command) => Some(command),
                    PendingMutation::ColdExpire {
                        logical_expiration,
                        notification,
                    } => {
                        let outcome = self
                            .expire_current(&item.key, *logical_expiration, cancellation)
                            .await?;
                        if outcome != ReplayOutcome::Applied {
                            return Ok(outcome);
                        }
                        if notification.is_some() {
                            recovery.notification_stage(&ticket);
                        }
                        notification.as_ref()
                    }
                    PendingMutation::Commit {
                        mutation,
                        notification,
                    }
                    | PendingMutation::FencedCommit {
                        mutation,
                        notification,
                    } => {
                        if mutation
                            .remaining_ttl(self.inner.clock.now())
                            .is_some_and(|ttl| ttl.is_zero())
                        {
                            return Ok(ReplayOutcome::Expired);
                        }
                        self.write_data(&item.key, mutation, flight.as_ref())
                            .await?;
                        if notification.is_some() {
                            recovery.notification_stage(&ticket);
                        }
                        notification.as_ref()
                    }
                };
                if !recovery.is_current(&ticket) {
                    return Ok(ReplayOutcome::Superseded);
                }
                if !self.replay_admitted()? {
                    return Ok(ReplayOutcome::Paused);
                }
                if let Some(command) = command
                    && let Some(backplane) = &self.inner.backplane
                {
                    backplane.publish_command(command.clone()).await?;
                    self.close_circuit(CircuitComponent::Backplane);
                }
                Ok(ReplayOutcome::Applied)
            }
            RecoveryWork::MarkerMutation(work) => {
                self.replay_marker_mutation(&ticket, work, cancellation)
                    .await
            }
            RecoveryWork::MarkerSnapshot(work) => {
                self.replay_marker_snapshot(&ticket, work, cancellation)
                    .await
            }
            RecoveryWork::Marker { command, stage } => {
                let _lane_guard = self
                    .memory
                    .guard(Arc::clone(&self.inner.marker_lane).lock_owned().await);
                if !recovery.is_current(&ticket) {
                    return Ok(ReplayOutcome::Superseded);
                }
                if !self.replay_admitted()? {
                    return Ok(ReplayOutcome::Paused);
                }
                if matches!(
                    stage,
                    MarkerReplay::AdvanceAndNotify | MarkerReplay::AdvanceOnly
                ) {
                    match &self.inner.markers {
                        MarkerAccess::Durable(store) => {
                            let outcome = store
                                .advance(
                                    command.scope(),
                                    command.marker().kind().clone(),
                                    command.marker().version(),
                                )
                                .await?;
                            self.apply_marker(outcome.marker().clone());
                            if *stage == MarkerReplay::AdvanceAndNotify {
                                recovery.notification_stage(&ticket);
                            }
                            if let MarkerAdvanceOutcome::Compacted { clear_remove, .. } = outcome {
                                let clear = MarkerCommand::new(
                                    Arc::clone(&self.inner.instance_id),
                                    self.inner.scope.clone(),
                                    StoredMarker::new(MarkerKind::ClearRemove, clear_remove),
                                )?;
                                self.apply_marker(clear.marker().clone());
                                if *stage != MarkerReplay::AdvanceOnly {
                                    self.queue_marker(clear, MarkerReplay::NotifyOnly)?;
                                }
                            }
                        }
                        MarkerAccess::Local => {}
                        MarkerAccess::Unavailable => return Err(MarkerError::Unsupported.into()),
                    }
                }
                if !recovery.is_current(&ticket) {
                    return Ok(ReplayOutcome::Superseded);
                }
                if !self.replay_admitted()? {
                    return Ok(ReplayOutcome::Paused);
                }
                if *stage != MarkerReplay::AdvanceOnly
                    && let Some(backplane) = &self.inner.backplane
                {
                    backplane
                        .publish_command(BackplaneCommand::Marker(command.clone()))
                        .await?;
                    self.close_circuit(CircuitComponent::Backplane);
                }
                Ok(ReplayOutcome::Applied)
            }
        }
    }
}
#[async_trait]
impl<V: Clone + Send + Sync + 'static> RecoveryExecutor for CacheInner<V> {
    async fn replay(&self, item: &RecoveryItem) -> Result<()> {
        if self.scopes.is_closed() {
            return Err(Error::CacheClosed);
        }
        let worker = Worker::ordinary(self.owner.upgrade().ok_or(Error::CacheClosed)?);
        let item = item.clone();
        let source = CancellationSource::new();
        let cancellation = source.token();
        self.scopes
            .execution(
                async move { worker.replay_legacy(&item, &cancellation).await },
                source,
            )
            .await
    }
    async fn replay_ticket(&self, ticket: &ReplayTicket) -> Result<ReplayOutcome> {
        if self.scopes.is_closed() {
            return Err(Error::CacheClosed);
        }
        let worker = Worker::ordinary(self.owner.upgrade().ok_or(Error::CacheClosed)?);
        let ticket = ticket.clone();
        let source = CancellationSource::new();
        let cancellation = source.token();
        self.scopes
            .execution(
                async move { worker.replay_owned(ticket, &cancellation).await },
                source,
            )
            .await
    }
}
