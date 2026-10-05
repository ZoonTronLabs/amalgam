//! Backplane continuity, maintenance and deterministic shutdown.
use super::{
    Arc, BackplaneAction, BackplaneCommand, BackplaneMessage, BackplaneReadiness, BackplaneState,
    CacheEvent, CacheInner, CancellationSource, CircuitComponent, CloseOutcome, Duration, Entry,
    Error, FallbackAvailability, Fence, Instant, L2ReadPolicy, Lifecycle, MarkerReads, Ordering,
    ReconciliationPolicy, Result, ShutdownError, ShutdownFailure, ShutdownReport, ShutdownTask,
    TagVerdict, Worker, broadcast, health_changed, lock,
};

impl<V: Clone + Send + Sync + 'static> Worker<V> {
    pub(super) async fn await_readiness(&self) -> Result<BackplaneReadiness> {
        let Some(backplane) = &self.inner.backplane else {
            return Ok(BackplaneReadiness::NotConfigured);
        };
        let Some(mut state) = backplane.connection_state() else {
            return Ok(BackplaneReadiness::BestEffort);
        };
        loop {
            let current = *state.borrow_and_update();
            match current {
                BackplaneState::Connected { epoch } => {
                    self.ensure_health();
                    return Ok(BackplaneReadiness::Acknowledged(epoch));
                }
                BackplaneState::Disconnected { .. } => {
                    self.ensure_health();
                }
                BackplaneState::Stopped => {
                    return Err(Error::Backplane("backplane provider stopped".into()));
                }
            }
            if state.changed().await.is_err() {
                return Err(Error::Backplane("backplane health stream closed".into()));
            }
        }
    }
    fn continuity_gap(&self) {
        if self.inner.reconciliation.invalidates_on_gap() {
            #[allow(
                deprecated,
                reason = "Atomic::try_update is unavailable on the supported Rust 1.88"
            )]
            if self
                .inner
                .epoch
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |epoch| {
                    epoch.checked_add(1)
                })
                .is_err()
            {
                tracing::error!("cache continuity generation exhausted");
                self.inner.close();
            }
            self.inner.memory.invalidate_all();
            self.inner.marker_reads.invalidate();
        }
        if let Some(recovery) = &self.inner.recovery {
            recovery.suspend();
        }
    }
    pub(super) fn ensure_health(&self) {
        let Some(health) = self
            .inner
            .backplane
            .as_ref()
            .and_then(|backplane| backplane.connection_state())
        else {
            return;
        };
        let current = *health.borrow();
        let mut seen = lock(&self.inner.health_seen);
        if seen.as_ref() == Some(&current) {
            return;
        }
        let previous = seen.replace(current);
        // Serialize barrier transitions with the observed state. Otherwise a
        // delayed disconnected caller can suspend replay after a newer ACK.
        let gap = !matches!(current, BackplaneState::Connected { .. }) || previous.is_some();
        let invalidate = gap && self.inner.reconciliation.invalidates_on_gap();
        #[allow(
            deprecated,
            reason = "Atomic::try_update is unavailable on the supported Rust 1.88"
        )]
        let exhausted = invalidate
            && self
                .inner
                .epoch
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |epoch| {
                    epoch.checked_add(1)
                })
                .is_err();
        match current {
            BackplaneState::Disconnected { .. } | BackplaneState::Stopped => {
                if let Some(recovery) = &self.inner.recovery {
                    recovery.suspend();
                }
            }
            BackplaneState::Connected { .. } => {
                self.inner
                    .subscription_admitted
                    .store(true, Ordering::Release);
                if let Some(recovery) = &self.inner.recovery
                    && let Err(error) = recovery.pause_after_reconnect()
                {
                    tracing::warn!(%error,"recovery reconnect delay rejected");
                }
            }
        }
        drop(seen);
        if invalidate {
            self.inner.memory.invalidate_all();
            self.inner.marker_reads.invalidate();
        }
        if exhausted {
            tracing::error!("cache continuity generation exhausted");
            self.inner.close();
        }
    }
    pub(super) fn start_maintenance(&self) {
        if tokio::runtime::Handle::try_current().is_err() {
            return;
        }
        if self.inner.maintenance.swap(true, Ordering::AcqRel) {
            return;
        }
        let weak = Arc::downgrade(&self.inner);
        let source = CancellationSource::new();
        let interval = match self.inner.reconciliation {
            ReconciliationPolicy::Periodic(interval) => interval.min(Duration::from_secs(1)),
            ReconciliationPolicy::LocalOnly
            | ReconciliationPolicy::BackplaneContinuity
            | ReconciliationPolicy::BackplaneBestEffort => Duration::from_secs(1),
        };
        let execution = self.inner.scopes.execution(
            async move {
                loop {
                    tokio::time::sleep(interval).await;
                    let Some(inner) = weak.upgrade() else {
                        return Ok(());
                    };
                    inner.memory.run_pending_tasks().await;
                    if let MarkerReads::OptionsControlled(observations) = &inner.marker_reads {
                        observations.memory.run_pending_tasks().await;
                        observations.locks.clean_idle(64);
                    }
                    inner.locks.clean_idle(256);
                    inner.lanes.clean(256);
                    if matches!(inner.reconciliation, ReconciliationPolicy::Periodic(_)) {
                        let interval = match inner.reconciliation {
                            ReconciliationPolicy::Periodic(interval) => interval,
                            ReconciliationPolicy::LocalOnly
                            | ReconciliationPolicy::BackplaneContinuity
                            | ReconciliationPolicy::BackplaneBestEffort => Duration::ZERO,
                        };
                        let reconcile = {
                            let mut last = lock(&inner.last_reconcile);
                            if last.elapsed() >= interval {
                                *last = Instant::now();
                                true
                            } else {
                                false
                            }
                        };
                        if reconcile {
                            inner.memory.invalidate_all();
                        }
                    }
                }
            },
            source,
        );
        let _receiver = self.inner.tasks.spawn(
            ShutdownTask::Maintenance,
            Arc::from("maintenance"),
            self.inner.events.clone(),
            execution,
        );
    }
    pub(super) fn start_listener(&self) {
        let Some(backplane) = &self.inner.backplane else {
            return;
        };
        let mut messages = backplane.subscribe();
        let mut health = backplane.connection_state();
        let weak = Arc::downgrade(&self.inner);
        let source = CancellationSource::new();
        let execution=self.inner.scopes.execution(async move {
            loop {
                tokio::select! {
                    result=messages.recv()=>{
                        let Some(inner)=weak.upgrade()else{return Ok(());};let worker=Worker {inner};
                        match result {
                            Ok(message)=>worker.apply_backplane(message).await?,
                            Err(broadcast::error::RecvError::Lagged(_))=>{worker.continuity_gap();worker.ensure_health();if let Some(recovery)=&worker.inner.recovery {recovery.pause_after_reconnect()?;}},
                            Err(broadcast::error::RecvError::Closed)=>{worker.continuity_gap();return Ok(());}
                        }
                    },
                    ()=health_changed(&mut health)=>{
                        if let Some(inner)=weak.upgrade(){Worker {inner}.ensure_health();}else{return Ok(());}
                    }
                }
            }
        },source);
        let _receiver = self.inner.tasks.spawn(
            ShutdownTask::Backplane,
            Arc::from("listener"),
            self.inner.events.clone(),
            execution,
        );
        self.ensure_health();
    }
    async fn apply_backplane(&self, message: BackplaneMessage) -> Result<()> {
        if self.inner.ignore_incoming_backplane {
            return Ok(());
        }
        let command = match BackplaneCommand::from_message(message) {
            Ok(command) => command,
            Err(error) => {
                self.continuity_gap();
                // A rejected inner frame loses history without disconnecting
                // the transport. Re-enter reconciliation even at the same ACK.
                if self.replay_admitted()
                    && let Some(recovery) = &self.inner.recovery
                {
                    recovery.pause_after_reconnect()?;
                }
                tracing::warn!(%error,"invalid backplane frame triggered reconciliation");
                return Ok(());
            }
        };
        if command.source_id() == &*self.inner.instance_id {
            return Ok(());
        }
        self.close_circuit(CircuitComponent::Backplane);
        match command {
            BackplaneCommand::Marker(command) => {
                if command.scope() == &self.inner.scope {
                    self.apply_marker(command.marker().clone());
                    self.seed_marker(command.marker(), &self.inner.tags_default_options)
                        .await?;
                    self.emit(CacheEvent::MarkerReceived { command });
                }
                Ok(())
            }
            BackplaneCommand::Data(message) => {
                let Some(key) = self.inner.logical_key(&message.key) else {
                    return Ok(());
                };
                self.emit(CacheEvent::MessageReceived {
                    key: Arc::clone(&key),
                });
                let lane = self.inner.lanes.get(&key);
                let lane_guard = Arc::clone(&lane.lock).lock_owned().await;
                if lock(&lane.timestamp).is_some_and(|at| at > message.timestamp) {
                    return Ok(());
                }
                let existing = self.inner.memory.get_at(&key, self.inner.clock.now()).await;
                if existing
                    .as_ref()
                    .is_some_and(|entry| entry.meta().created() > message.timestamp)
                {
                    return Ok(());
                }
                let fence = lane.advance(message.timestamp, &self.inner.epoch)?;
                if let Some(recovery) = &self.inner.recovery {
                    recovery.cancel_through(&key, message.timestamp);
                }
                match message.action {
                    BackplaneAction::Remove => {
                        self.inner.memory.remove(&key).await;
                    }
                    BackplaneAction::Expire => {
                        if let Some(entry) = existing {
                            let expired = entry.with_logical_expiration(message.timestamp);
                            self.inner
                                .memory
                                .insert_if_unchanged(
                                    Arc::clone(&key),
                                    Some(&entry),
                                    expired,
                                    self.inner.clock.now(),
                                )
                                .await;
                        }
                    }
                    BackplaneAction::Set => {
                        if let Some(existing) = existing {
                            drop(lane_guard);
                            self.passive(key, existing, fence);
                            return Ok(());
                        }
                    }
                }
                Ok(())
            }
        }
    }
    fn passive(&self, key: Arc<str>, expected: Entry<V>, fence: Arc<Fence>) {
        let worker = self.clone();
        let task_key = Arc::clone(&key);
        let source = CancellationSource::new();
        let cancellation = source.token();
        let execution = self.inner.scopes.execution(
            async move {
                let raw = worker
                    .inner
                    .key_prefix
                    .as_ref()
                    .and_then(|prefix| key.strip_prefix(&**prefix))
                    .unwrap_or(&key);
                let opts = worker.resolve_options(raw, None)?;
                let read = worker
                    .read_l2(
                        &key,
                        &opts,
                        FallbackAvailability::Unavailable,
                        L2ReadPolicy::FactoryFallback,
                        &cancellation,
                    )
                    .await;
                let _lane = Arc::clone(&fence.lane.lock).lock_owned().await;
                if !fence.passive_is_current() {
                    return Ok(());
                }
                match read {
                    Ok(Some(entry)) if worker.tags(&entry.entry) != TagVerdict::Remove => {
                        if !opts.skip_memory_write()
                            && let Some(local) = entry
                                .entry
                                .for_memory_hydration(&opts, worker.inner.clock.now())?
                        {
                            let local = local.with_hydrated_value(
                                worker.copy(local.value(), &opts)?,
                                fence.continuity_stamp(),
                            );
                            if !fence.passive_is_current() {
                                return Ok(());
                            }
                            worker
                                .inner
                                .memory
                                .insert_if_unchanged(
                                    key,
                                    Some(&expected),
                                    local,
                                    worker.inner.clock.now(),
                                )
                                .await;
                        }
                    }
                    Err(
                        error @ (Error::OperationCancelled { .. } | Error::FactoryCancelled { .. }),
                    ) => {
                        return Err(error);
                    }
                    Ok(Some(_)) | Ok(None) | Err(_) => {
                        worker.inner.memory.remove_if_same(&key, &expected).await;
                    }
                }
                Ok(())
            },
            source,
        );
        let _receiver = self.inner.tasks.spawn(
            ShutdownTask::Distributed,
            task_key,
            self.inner.events.clone(),
            execution,
        );
    }
}
impl<V: Clone + Send + Sync + 'static> CacheInner<V> {
    pub(super) fn close(&self) -> CloseOutcome {
        let outcome = {
            let mut lifecycle = lock(&self.lifecycle);
            match &*lifecycle {
                Lifecycle::Running => {
                    *lifecycle = Lifecycle::Closing;
                    CloseOutcome::Started
                }
                Lifecycle::Closing => CloseOutcome::AlreadyClosing,
                Lifecycle::Closed(_) => CloseOutcome::AlreadyClosed,
            }
        };
        // Another close can be preempted after publishing Closing but before
        // its scope barrier. Every closer establishes that barrier before an
        // awaitable shutdown may report drainage.
        self.scopes.close();
        if outcome == CloseOutcome::Started {
            if let Some(recovery) = &self.recovery {
                recovery.stop();
            }
            for error in self.plugins.stop_all() {
                tracing::warn!(%error,"plugin close failed");
            }
        }
        outcome
    }
    pub(super) async fn shutdown(&self) -> Result<ShutdownReport> {
        let _shutdown = self.shutdown_gate.lock().await;
        if let Lifecycle::Closed(result) = &*lock(&self.lifecycle) {
            return result.clone().map_err(Error::from);
        }
        self.close();
        self.scopes.drained().await;
        self.tasks.drain().await;
        let mut failures = self.tasks.take_failures();
        if let Some(recovery) = &self.recovery
            && let Err(error) = recovery.shutdown().await
        {
            failures.push(ShutdownFailure::Work(error.into()));
        }
        failures.extend(
            self.plugins
                .shutdown()
                .await
                .into_iter()
                .map(ShutdownFailure::Plugin),
        );
        let result = if failures.is_empty() {
            Ok(ShutdownReport)
        } else {
            let mut failures = failures.into_iter();
            match failures.next() {
                Some(first) => Err(ShutdownError::new(first, failures)),
                None => unreachable!("nonempty failure collection"),
            }
        };
        *lock(&self.lifecycle) = Lifecycle::Closed(result.clone());
        result.map_err(Error::from)
    }
}
