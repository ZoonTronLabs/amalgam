//! Value mutations and owned local/distributed commit pipelines.
use super::{
    Arc, BackplaneAction, BackplaneCommand, BackplaneEvent, BackplaneMessage, CacheEvent,
    CacheLevel, CacheValue, CancellationSource, CircuitComponent, CommitCompletion, CommitMode,
    CommitReceipt, CommitReport, DataCommit, DataMutation, DistributedEvent,
    DistributedExpirePolicy, DistributedSnapshot, Duration, EffectOutcome, EnqueueOutcome, Entry,
    EntryOptions, Error, FactoryCancellation, FactoryProduct, Fence, FlightGuard, Future,
    Instrument, KeyMutation, LayerEvent, LeaseError, LeasePolicy, LeasedMutation,
    LeasedWriteOutcome, LocalCommit, LocalEffect, MutationReceipt, Observed, OperationOutcome,
    PendingMutation, PreparedData, ProductOrigin, RecoveryAction, RecoveryItem, RecoveryWork,
    Result, ShutdownTask, SkipReason, Storage, Tag, Timestamp, Worker, lock, recovery_action,
};
use super::{RecoveryFence, component_span};

impl<V: Clone + Send + Sync + 'static> Worker<V> {
    pub(super) async fn store_product(
        &self,
        key: Arc<str>,
        product: FactoryProduct<V>,
        started: Timestamp,
        guard: FlightGuard,
        cancellation: &FactoryCancellation,
    ) -> Result<CacheValue<V>> {
        let product = product.into_payload()?;
        self.validate_options(&product.options)?;
        guard.proof()?;
        let value = self.copy(&product.value, &product.options)?;
        let stored = self.copy(&product.value, &product.options)?;
        let entry = self.fresh_entry(
            stored,
            &product.options,
            started,
            product.tags,
            product.etag,
            product.last_modified,
        )?;
        let receipt = self
            .write_entry(
                Arc::clone(&key),
                entry,
                product.options,
                Some(guard),
                cancellation,
            )
            .await?;
        match product.origin {
            ProductOrigin::Modified => self.emit(CacheEvent::FactorySuccess {
                key: Arc::clone(&key),
            }),
            ProductOrigin::NotModified | ProductOrigin::Constant => {}
        }
        self.emit(CacheEvent::Set { key });
        Ok(CacheValue {
            value,
            commit: CommitReceipt::Mutation(receipt),
        })
    }
    pub(super) async fn set(
        &self,
        raw: Arc<str>,
        value: V,
        options: Option<EntryOptions>,
        tags: Box<[Tag]>,
        cancellation: &FactoryCancellation,
    ) -> Result<Observed<MutationReceipt>> {
        let opts = self.resolve_options(&raw, options)?;
        let key = self.full_key(&raw);
        let now = self.inner.clock.now();
        let entry = self.fresh_entry(self.copy(&value, &opts)?, &opts, now, tags, None, None)?;
        let receipt = self
            .write_entry(Arc::clone(&key), entry, opts, None, cancellation)
            .await?;
        self.emit(CacheEvent::Set { key });
        Ok(Observed::new(receipt, OperationOutcome::Stored, None))
    }
    async fn write_entry(
        &self,
        key: Arc<str>,
        entry: Entry<V>,
        opts: EntryOptions,
        flight: Option<FlightGuard>,
        cancellation: &FactoryCancellation,
    ) -> Result<MutationReceipt> {
        let data = match &self.inner.storage {
            Storage::MemoryOnly => PreparedData::Absent,
            Storage::Hybrid { serializer, .. } if !opts.skip_distributed_write() => {
                let snapshot = DistributedSnapshot::from_entry_with_options(
                    &entry,
                    &opts,
                    entry.meta().inserted_at(),
                )?;
                match serializer
                    .encode(&snapshot, self.inner.serialization_mode, cancellation)
                    .await
                {
                    Ok(bytes) => PreparedData::Ready(DataMutation::Set {
                        bytes: bytes.into(),
                        physical_expiration: Timestamp::from_ticks(
                            snapshot.entry().physical_expiration_ticks,
                        ),
                    }),
                    Err(error) => {
                        self.failure(&key, &error);
                        if matches!(
                            error,
                            Error::OperationCancelled { .. } | Error::FactoryCancelled { .. }
                        ) || opts.rethrow_serialization_exceptions()
                        {
                            return Err(error);
                        }
                        PreparedData::Failed(error)
                    }
                }
            }
            Storage::Hybrid { .. } => PreparedData::Skipped,
        };
        let lane = self.inner.lanes.get(&key);
        let lane_guard = Arc::clone(&lane.lock).lock_owned().await;
        if let Some(flight) = &flight {
            flight.proof()?;
        }
        let fence = lane.advance(entry.meta().created(), &self.inner.epoch)?;
        let local = if opts.skip_memory_write() {
            LocalCommit::Applied(LocalEffect::Skipped)
        } else if flight
            .as_ref()
            .map(FlightGuard::proof)
            .transpose()?
            .flatten()
            .is_some()
            && matches!(&data, PreparedData::Ready(_))
        {
            // Atomic fenced backend admission must succeed before the value is
            // visible in L1; a proof observed before an await is insufficient.
            LocalCommit::Store(entry.clone())
        } else {
            LocalCommit::Applied(LocalEffect::Stored(
                self.inner
                    .memory
                    .insert_at(Arc::clone(&key), entry.clone(), self.inner.clock.now())
                    .await,
            ))
        };
        let command = self.data_command(BackplaneAction::Set, &key, entry.meta().created(), &opts);
        let worker = self.clone();
        let task_key = Arc::clone(&key);
        let mode = if opts.allow_background_distributed_operations()
            && matches!(self.inner.storage, Storage::Hybrid { .. })
        {
            CommitMode::Background
        } else {
            CommitMode::Foreground
        };
        let work = async move {
            worker
                .commit_data(DataCommit {
                    key: task_key,
                    data,
                    command,
                    opts,
                    fence,
                    flight,
                    local,
                    lane_guard,
                })
                .await
        };
        self.pipeline_receipt(key, mode, work).await
    }
    fn data_command(
        &self,
        action: BackplaneAction,
        key: &Arc<str>,
        timestamp: Timestamp,
        opts: &EntryOptions,
    ) -> Option<BackplaneCommand> {
        if opts.skip_backplane_notifications() || self.inner.backplane.is_none() {
            None
        } else {
            Some(BackplaneCommand::Data(BackplaneMessage {
                source_id: Arc::clone(&self.inner.instance_id),
                timestamp,
                action,
                key: Arc::from(self.inner.l2_key(key)),
            }))
        }
    }
    pub(super) async fn commit_receipt(
        &self,
        key: Arc<str>,
        mode: CommitMode,
        work: impl Future<Output = Result<CommitReport>> + Send + 'static,
    ) -> Result<MutationReceipt> {
        match mode {
            CommitMode::Background => Ok(self.schedule_receipt(key, work)),
            CommitMode::Foreground => Ok(MutationReceipt::Completed(work.await?)),
        }
    }
    fn schedule_receipt(
        &self,
        key: Arc<str>,
        work: impl Future<Output = Result<CommitReport>> + Send + 'static,
    ) -> MutationReceipt {
        let execution = self.scopes().execution(work, CancellationSource::new());
        let receiver = self.inner.tasks.spawn(
            ShutdownTask::Distributed,
            key,
            self.inner.events.clone(),
            execution,
        );
        MutationReceipt::Scheduled(CommitCompletion { receiver })
    }
    pub(super) async fn write_data(
        &self,
        key: &str,
        data: &DataMutation,
        flight: Option<&FlightGuard>,
    ) -> Result<EffectOutcome> {
        let Storage::Hybrid { backend, .. } = &self.inner.storage else {
            return Ok(EffectOutcome::NotConfigured);
        };
        let ttl = data.remaining_ttl(self.inner.clock.now());
        if ttl.is_some_and(|ttl| ttl.is_zero()) {
            return Ok(EffectOutcome::Skipped(SkipReason::PhysicallyExpired));
        }
        if let Some(proof) = flight.map(FlightGuard::proof).transpose()?.flatten() {
            let mutation = match data {
                DataMutation::Set { bytes, .. } | DataMutation::Expire { bytes, .. } => {
                    LeasedMutation::Set {
                        bytes: bytes.to_vec(),
                        ttl,
                    }
                }
                DataMutation::Remove => LeasedMutation::Remove,
            };
            match backend
                .write_with_lease(&self.inner.l2_key(key), mutation, &proof)
                .await?
            {
                LeasedWriteOutcome::Committed => {}
                LeasedWriteOutcome::LeaseLost => return Err(LeaseError::Lost.into()),
            }
        } else {
            match data {
                DataMutation::Set { bytes, .. } | DataMutation::Expire { bytes, .. } => {
                    backend
                        .set(&self.inner.l2_key(key), bytes.to_vec(), ttl)
                        .await?
                }
                DataMutation::Remove => backend.remove(&self.inner.l2_key(key)).await?,
            }
        }
        self.close_circuit(CircuitComponent::Distributed);
        self.inner.events.emit_layer_lazy(|| {
            LayerEvent::Distributed(match data {
                DataMutation::Set { .. } | DataMutation::Expire { .. } => DistributedEvent::Set {
                    key: Arc::from(key),
                },
                DataMutation::Remove => DistributedEvent::Remove {
                    key: Arc::from(key),
                },
            })
        });
        Ok(EffectOutcome::Applied)
    }
    fn enqueue_data(
        &self,
        key: &Arc<str>,
        data: PendingMutation,
        action: RecoveryAction,
        at: Timestamp,
        fence: Arc<Fence>,
    ) -> Result<bool> {
        let Some(recovery) = &self.inner.recovery else {
            return Ok(false);
        };
        let expiry = match &data {
            PendingMutation::Commit {
                mutation:
                    DataMutation::Set {
                        physical_expiration,
                        ..
                    }
                    | DataMutation::Expire {
                        physical_expiration,
                        ..
                    },
                ..
            }
            | PendingMutation::FencedCommit {
                mutation:
                    DataMutation::Set {
                        physical_expiration,
                        ..
                    }
                    | DataMutation::Expire {
                        physical_expiration,
                        ..
                    },
                ..
            } => *physical_expiration,
            _ => at.saturating_add(Duration::from_secs(600)),
        };
        let outcome = recovery.enqueue_versioned(
            RecoveryWork::Data {
                item: RecoveryItem {
                    key: Arc::clone(key),
                    action,
                    timestamp: at,
                    expires_at: expiry,
                    remaining_retries: None,
                },
                mutation: data,
            },
            fence,
        )?;
        Ok(matches!(
            outcome,
            EnqueueOutcome::Queued(_) | EnqueueOutcome::Replaced(_)
        ))
    }
    async fn commit_data(&self, work: DataCommit<V>) -> Result<MutationReceipt> {
        let DataCommit {
            key,
            data,
            command,
            opts,
            fence,
            flight,
            local,
            lane_guard,
        } = work;
        let at = lock(&fence.lane.timestamp).unwrap_or(self.inner.clock.now());
        let action = command
            .as_ref()
            .and_then(|command| match command {
                BackplaneCommand::Data(message) => Some(recovery_action(message.action)),
                BackplaneCommand::Marker(_) => None,
            })
            .unwrap_or(RecoveryAction::Set);
        let distributed = match data {
            PreparedData::Absent => EffectOutcome::NotConfigured,
            PreparedData::Skipped => EffectOutcome::Skipped(SkipReason::Policy),
            PreparedData::Failed(error) => {
                return Ok(MutationReceipt::Completed(CommitReport {
                    local: self.commit_local(&key, local, flight.as_ref()).await?,
                    distributed: EffectOutcome::FailedSuppressed { cause: error },
                    backplane: EffectOutcome::Skipped(SkipReason::Policy),
                }));
            }
            PreparedData::ColdExpire {
                cause,
                logical_expiration,
            } => {
                let queued = self.enqueue_data(
                    &key,
                    PendingMutation::ColdExpire {
                        logical_expiration,
                        notification: command,
                    },
                    RecoveryAction::Expire,
                    at,
                    Arc::clone(&fence),
                )?;
                if opts.rethrow_distributed_exceptions() {
                    return Err(cause);
                }
                return Ok(MutationReceipt::Completed(CommitReport {
                    local: self.commit_local(&key, local, flight.as_ref()).await?,
                    distributed: if queued {
                        EffectOutcome::RecoveryQueued { cause }
                    } else {
                        EffectOutcome::FailedSuppressed { cause }
                    },
                    backplane: EffectOutcome::Skipped(SkipReason::Policy),
                }));
            }
            PreparedData::Ready(data) => {
                let strict = flight.as_ref().is_some_and(|flight| {
                    flight.policy == LeasePolicy::Fenced && flight.lease.is_some()
                });
                let result = if self.circuit(CircuitComponent::Distributed) {
                    self.write_data(&key, &data, flight.as_ref())
                        .instrument(component_span(
                            &self.inner.name,
                            CacheLevel::Distributed,
                            "commit",
                            Some(&key),
                        ))
                        .await
                } else {
                    Err(Error::CircuitOpen {
                        component: CircuitComponent::Distributed,
                    })
                };
                match result {
                    Ok(outcome) => outcome,
                    Err(error) => {
                        self.failure(&key, &error);
                        if matches!(&error,Error::Lease(error) if !matches!(error,LeaseError::Backend {..}))
                        {
                            return Err(error);
                        }
                        let pending = if strict {
                            PendingMutation::FencedCommit {
                                mutation: data,
                                notification: command.clone(),
                            }
                        } else {
                            PendingMutation::Commit {
                                mutation: data,
                                notification: command.clone(),
                            }
                        };
                        let queued =
                            self.enqueue_data(&key, pending, action, at, Arc::clone(&fence))?;
                        if opts.rethrow_distributed_exceptions() {
                            return Err(error);
                        }
                        return Ok(MutationReceipt::Completed(CommitReport {
                            local: self.commit_local(&key, local, flight.as_ref()).await?,
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
        let local = self.commit_local(&key, local, flight.as_ref()).await?;
        if let Some(recovery) = &self.inner.recovery {
            recovery.supersede_through(&key, fence.generation());
        }
        let mode = if opts.allow_background_backplane_operations() && command.is_some() {
            CommitMode::Background
        } else {
            CommitMode::Foreground
        };
        let worker = self.clone();
        let task_key = Arc::clone(&key);
        let work = async move {
            let _lane_guard = lane_guard;
            let backplane = if let Some(command) = command {
                match worker.publish(command.clone()).await {
                    Ok(()) => EffectOutcome::Applied,
                    Err(error) => {
                        worker.failure(&task_key, &error);
                        let queued = worker.enqueue_data(
                            &task_key,
                            PendingMutation::Notify(command),
                            action,
                            at,
                            fence,
                        )?;
                        if opts.rethrow_backplane_exceptions() {
                            return Err(error);
                        }
                        if queued {
                            EffectOutcome::RecoveryQueued { cause: error }
                        } else {
                            EffectOutcome::FailedSuppressed { cause: error }
                        }
                    }
                }
            } else if worker.inner.backplane.is_none() {
                EffectOutcome::NotConfigured
            } else {
                EffectOutcome::Skipped(SkipReason::Policy)
            };
            drop(flight);
            Ok(CommitReport {
                local,
                distributed,
                backplane,
            })
        };
        self.commit_receipt(key, mode, work).await
    }
    async fn commit_local(
        &self,
        key: &Arc<str>,
        local: LocalCommit<V>,
        flight: Option<&FlightGuard>,
    ) -> Result<LocalEffect> {
        match local {
            LocalCommit::Applied(outcome) => Ok(outcome),
            LocalCommit::Store(entry) => {
                if let Some(flight) = flight {
                    flight.proof()?;
                }
                Ok(LocalEffect::Stored(
                    self.inner
                        .memory
                        .insert_at(Arc::clone(key), entry, self.inner.clock.now())
                        .await,
                ))
            }
        }
    }
    pub(super) async fn pipeline_receipt(
        &self,
        key: Arc<str>,
        mode: CommitMode,
        work: impl Future<Output = Result<MutationReceipt>> + Send + 'static,
    ) -> Result<MutationReceipt> {
        match mode {
            // Starting an owned background scope does not suspend. Keep the
            // unused foreground receipt future out of this operation's state.
            CommitMode::Background => {
                Ok(self.schedule_receipt(key, async move { work.await?.wait().await }))
            }
            CommitMode::Foreground => work.await,
        }
    }
    pub(super) async fn publish(&self, command: BackplaneCommand) -> Result<()> {
        let Some(backplane) = &self.inner.backplane else {
            return Ok(());
        };
        if !self.circuit(CircuitComponent::Backplane) {
            return Err(Error::CircuitOpen {
                component: CircuitComponent::Backplane,
            });
        }
        let key = match &command {
            BackplaneCommand::Data(message) => Arc::clone(&message.key),
            BackplaneCommand::Marker(command) => {
                Arc::from(format!("marker:{:?}", command.marker().kind()))
            }
        };
        let published = command.clone();
        backplane
            .publish_command(command)
            .instrument(component_span(
                &self.inner.name,
                CacheLevel::Backplane,
                "publish",
                Some(&key),
            ))
            .await?;
        self.close_circuit(CircuitComponent::Backplane);
        self.inner.events.emit_layer_lazy(|| {
            LayerEvent::Backplane(BackplaneEvent::MessagePublished { command: published })
        });
        self.emit(CacheEvent::MessagePublished { key });
        Ok(())
    }
    pub(super) async fn key_mutation(
        &self,
        raw: Arc<str>,
        options: Option<EntryOptions>,
        mutation: KeyMutation,
        cancellation: &FactoryCancellation,
    ) -> Result<Observed<MutationReceipt>> {
        let opts = self.resolve_options(&raw, options)?;
        let key = self.full_key(&raw);
        let lane = self.inner.lanes.get(&key);
        let lane_guard = Arc::clone(&lane.lock).lock_owned().await;
        let now = self.inner.clock.now();
        let fence = lane.advance(now, &self.inner.epoch)?;
        let local = if opts.skip_memory_write() {
            LocalEffect::Skipped
        } else {
            match mutation {
                KeyMutation::Remove => {
                    self.inner.memory.remove(&key).await;
                    LocalEffect::Removed
                }
                KeyMutation::Expire(_) => {
                    if let Some(entry) = self.inner.memory.get_at(&key, now).await {
                        let expired = entry.with_logical_expiration(now);
                        self.inner
                            .memory
                            .expire_if_unchanged(Arc::clone(&key), &entry, expired, now)
                            .await;
                    }
                    LocalEffect::Expired
                }
            }
        };
        let data = if opts.skip_distributed_write() {
            PreparedData::Skipped
        } else {
            match (&self.inner.storage, mutation) {
                (Storage::MemoryOnly, _) => PreparedData::Absent,
                (
                    Storage::Hybrid { .. },
                    KeyMutation::Remove | KeyMutation::Expire(DistributedExpirePolicy::Remove),
                ) => PreparedData::Ready(DataMutation::Remove),
                (
                    Storage::Hybrid {
                        backend,
                        serializer,
                    },
                    KeyMutation::Expire(DistributedExpirePolicy::RetainStale),
                ) => match backend.get(&self.inner.l2_key(&key)).await {
                    Ok(Some(bytes)) => match serializer
                        .decode(&bytes, self.inner.serialization_mode, cancellation)
                        .await
                    {
                        Ok(snapshot) => {
                            let mut payload = snapshot.entry().clone();
                            payload.logical_expiration_ticks =
                                payload.logical_expiration_ticks.min(now.ticks());
                            let expired = DistributedSnapshot::new(
                                payload,
                                snapshot.inserted_at(),
                                snapshot.retention(),
                            )?;
                            match serializer
                                .encode(&expired, self.inner.serialization_mode, cancellation)
                                .await
                            {
                                Ok(bytes) => PreparedData::Ready(DataMutation::Expire {
                                    bytes: bytes.into(),
                                    physical_expiration: Timestamp::from_ticks(
                                        expired.entry().physical_expiration_ticks,
                                    ),
                                }),
                                Err(error) => {
                                    self.failure(&key, &error);
                                    if matches!(
                                        error,
                                        Error::OperationCancelled { .. }
                                            | Error::FactoryCancelled { .. }
                                    ) || opts.rethrow_serialization_exceptions()
                                    {
                                        return Err(error);
                                    }
                                    PreparedData::Failed(error)
                                }
                            }
                        }
                        Err(error) => {
                            self.failure(&key, &error);
                            if matches!(
                                error,
                                Error::OperationCancelled { .. } | Error::FactoryCancelled { .. }
                            ) || opts.rethrow_serialization_exceptions()
                            {
                                return Err(error);
                            }
                            PreparedData::Failed(error)
                        }
                    },
                    Ok(None) => PreparedData::Ready(DataMutation::Remove),
                    Err(error) => {
                        self.failure(&key, &error);
                        PreparedData::ColdExpire {
                            cause: error,
                            logical_expiration: now,
                        }
                    }
                },
            }
        };
        let action = match mutation {
            KeyMutation::Remove => BackplaneAction::Remove,
            KeyMutation::Expire(_) => BackplaneAction::Expire,
        };
        let command = self.data_command(action, &key, now, &opts);
        let worker = self.clone();
        let task_key = Arc::clone(&key);
        let mode = if opts.allow_background_distributed_operations()
            && matches!(self.inner.storage, Storage::Hybrid { .. })
        {
            CommitMode::Background
        } else {
            CommitMode::Foreground
        };
        let work = async move {
            worker
                .commit_data(DataCommit {
                    key: task_key,
                    data,
                    command,
                    opts,
                    fence,
                    flight: None,
                    local: LocalCommit::Applied(local),
                    lane_guard,
                })
                .await
        };
        let receipt = self.pipeline_receipt(Arc::clone(&key), mode, work).await?;
        self.emit(match mutation {
            KeyMutation::Remove => CacheEvent::Remove { key },
            KeyMutation::Expire(_) => CacheEvent::Expire { key },
        });
        Ok(Observed::new(
            receipt,
            match mutation {
                KeyMutation::Remove => OperationOutcome::Removed,
                KeyMutation::Expire(_) => OperationOutcome::Expired,
            },
            None,
        ))
    }
}
