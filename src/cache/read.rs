//! Value reads, same-key origin work, fail-safe and eager refresh.
use super::{
    AcquisitionPolicy, Arc, CacheEvent, CacheLevel, CacheValue, CancellationSource,
    CircuitComponent, CommitReceipt, DistributedLease, DistributedLookup, Duration, Entry,
    EntryOptions, Error, Execution, FactoryCancellation, FactoryContext, FactoryError,
    FactoryProduct, FallbackAvailability, FlightGuard, Future, HitKind, HydrationFence,
    HydrationOutcome, Instrument, L1Read, L2ReadPolicy, LeaseError, LeasePolicy, LinkMode,
    LocalParticipation, LockOutcome, LookupKey, MarkerReadPolicy, MaybeValue, Observed,
    OperationOutcome, Ordering, ReadStale, Reason, Result, ShutdownTask, SkipReason, Storage, Tag,
    TagVerdict, Timeout, Worker, acquire_owned_supervised, bounded, component_span, lease_lost,
    newer_of,
};

impl<V: Clone + Send + Sync + 'static> Worker<V> {
    async fn read_l1(&self, key: &str, cancellation: &FactoryCancellation) -> Result<L1Read<V>> {
        let captured = self.inner.epoch.load(Ordering::Acquire);
        let now = self.inner.clock.now();
        let Some(entry) = self.inner.memory.get_at(key, now).await else {
            return Ok(L1Read::Miss);
        };
        self.reconcile_controlled_markers(&entry, cancellation)
            .await?;
        self.ensure_health();
        cancellation.check()?;
        let now = self.inner.clock.now();
        if self.inner.epoch.load(Ordering::Acquire) != captured
            || !entry.is_read_eligible()
            || entry.is_physically_expired(now)
        {
            return Ok(L1Read::Miss);
        }
        Ok(match self.tags(&entry) {
            TagVerdict::Remove => {
                self.inner.memory.remove_if_same(key, &entry).await;
                L1Read::Miss
            }
            TagVerdict::Expire => L1Read::Stale(entry),
            TagVerdict::Valid => {
                if entry.freshness(now).is_fresh() {
                    L1Read::Fresh(entry)
                } else {
                    L1Read::Stale(entry)
                }
            }
        })
    }

    pub(super) async fn read_l2(
        &self,
        key: &Arc<str>,
        opts: &EntryOptions,
        fallback: FallbackAvailability,
        policy: L2ReadPolicy,
        cancellation: &FactoryCancellation,
    ) -> Result<Option<DistributedLookup<V>>> {
        if matches!(self.inner.storage, Storage::MemoryOnly) {
            return Ok(None);
        }
        if opts.skip_distributed_read() {
            return Ok(None);
        }
        if !self.circuit(CircuitComponent::Distributed) {
            let error = Error::CircuitOpen {
                component: CircuitComponent::Distributed,
            };
            return match policy {
                L2ReadPolicy::PreserveFailure => Err(error),
                L2ReadPolicy::FactoryFallback => {
                    tracing::warn!(%error,key=%key,"legacy/origin lookup skipped an open circuit");
                    Ok(None)
                }
            };
        }
        let timeout = opts
            .appropriate_distributed_timeout(matches!(fallback, FallbackAvailability::Available));
        let span = component_span(&self.inner.name, CacheLevel::Distributed, "read", Some(key));
        let source = CancellationSource::new();
        let phase_cancellation = source.token();
        let worker = self.clone();
        let phase_key = Arc::clone(key);
        let mut execution = self.inner.scopes.execution(
            async move { worker.fetch_l2(&phase_key, &phase_cancellation).await }.instrument(span),
            source,
        );
        execution.link(cancellation, LinkMode::Explicit);
        let soft = opts.is_fail_safe_enabled()
            && matches!(fallback, FallbackAvailability::Available)
            && timeout == opts.distributed_soft_timeout()
            && timeout != opts.distributed_hard_timeout();
        // Value retrieval, decoding and the required durable marker lookup are
        // one read. A marker provider cannot escape the caller's chosen budget.
        // Borrow execution through the deadline wrapper so we publish the exact
        // phase reason before dropping its codec, independently of the caller.
        let result = match bounded(timeout, &mut execution).await {
            Ok(Some(result)) => result,
            Ok(None) => {
                execution.cancel(if soft {
                    Reason::SoftTimeout
                } else {
                    Reason::HardTimeout
                });
                if policy == L2ReadPolicy::FactoryFallback
                    && opts.is_fail_safe_enabled()
                    && matches!(fallback, FallbackAvailability::Available)
                    && timeout == opts.distributed_soft_timeout()
                {
                    Ok(None)
                } else {
                    Err(Error::Distributed(
                        "distributed read deadline elapsed".into(),
                    ))
                }
            }
            Err(error) => Err(error),
        };
        match result {
            Ok(value) => {
                if let Some(entry) = &value {
                    self.reconcile_controlled_markers(&entry.entry, cancellation)
                        .await?;
                }
                Ok(value)
            }
            Err(error) => {
                self.failure(key, &error);
                if policy.rethrow(opts, &error) {
                    Err(error)
                } else {
                    tracing::warn!(%error,key=%key,"distributed read degraded to miss");
                    Ok(None)
                }
            }
        }
    }
    async fn fetch_l2(
        &self,
        key: &Arc<str>,
        cancellation: &FactoryCancellation,
    ) -> Result<Option<DistributedLookup<V>>> {
        let Storage::Hybrid {
            backend,
            serializer,
        } = &self.inner.storage
        else {
            return Ok(None);
        };
        cancellation.check()?;
        let hydration = self.hydration_fence(key).await;
        let bytes = backend.get(&self.inner.l2_key(key)).await?;
        self.close_circuit(CircuitComponent::Distributed);
        let Some(bytes) = bytes else { return Ok(None) };
        let snapshot = serializer
            .decode(&bytes, self.inner.serialization_mode, cancellation)
            .await?;
        let source = snapshot.try_into_entry(self.inner.clock.now())?;
        if source.is_physically_expired(self.inner.clock.now()) {
            return Ok(None);
        }
        if self.inner.marker_reads.policy() == MarkerReadPolicy::DurableRequired {
            self.reconcile_markers(source.meta().tags()).await?;
        }
        Ok(Some(DistributedLookup {
            entry: source,
            hydration,
        }))
    }
    async fn hydration_fence(&self, key: &str) -> HydrationFence<V> {
        let lane = self.inner.lanes.get(key);
        let fence = {
            let Ok(_guard) = Arc::clone(&lane.lock).try_lock_owned() else {
                // A read overlapping an already started commit may return its
                // snapshot, but cannot install it after that commit completes.
                return HydrationFence::ConcurrentMutation;
            };
            lane.snapshot(&self.inner.epoch)
        };
        let observed = self.inner.memory.get_at(key, self.inner.clock.now()).await;
        HydrationFence::Stable { fence, observed }
    }
    async fn hydrate(
        &self,
        key: &Arc<str>,
        source: &DistributedLookup<V>,
        opts: &EntryOptions,
    ) -> Result<HydrationOutcome> {
        if opts.skip_memory_write() {
            return Ok(HydrationOutcome::Skipped(SkipReason::Policy));
        }
        let HydrationFence::Stable { fence, observed } = &source.hydration else {
            return Ok(HydrationOutcome::Skipped(SkipReason::Superseded));
        };
        let Ok(_guard) = Arc::clone(&fence.lane.lock).try_lock_owned() else {
            // Hydration is optional. A newer mutation already owning this lane
            // must not delay the read or install an older snapshot afterward.
            return Ok(HydrationOutcome::Skipped(SkipReason::Superseded));
        };
        let verdict = self.tags(&source.entry);
        if !fence.passive_is_current()
            || verdict == TagVerdict::Remove
            || observed.as_ref().is_some_and(|entry| {
                entry.meta().created() > source.entry.meta().created()
                    || (entry.meta().created() == source.entry.meta().created()
                        && (verdict == TagVerdict::Expire
                            || source.entry.is_logically_expired(self.inner.clock.now())))
            })
        {
            return Ok(HydrationOutcome::Skipped(SkipReason::Superseded));
        }
        let Some(local) = source
            .entry
            .for_memory_hydration(opts, self.inner.clock.now())?
        else {
            return Ok(HydrationOutcome::Skipped(SkipReason::PhysicallyExpired));
        };
        let local =
            local.with_hydrated_value(self.copy(local.value(), opts)?, fence.continuity_stamp());
        if !fence.passive_is_current() {
            return Ok(HydrationOutcome::Skipped(SkipReason::Superseded));
        }
        Ok(HydrationOutcome::Evaluated(
            self.inner
                .memory
                .insert_if_unchanged(
                    Arc::clone(key),
                    observed.as_ref(),
                    local,
                    self.inner.clock.now(),
                )
                .await,
        ))
    }
    pub(super) async fn read(
        &self,
        key: LookupKey,
        options: Option<EntryOptions>,
        policy: L2ReadPolicy,
        cancellation: &FactoryCancellation,
    ) -> Result<Observed<MaybeValue<V>>> {
        let opts = self.resolve_options(&key.raw, options)?;
        let key = key.full;
        self.ensure_health();
        let mut stale = None;
        if !opts.skip_memory_read() {
            match self.read_l1(&key, cancellation).await? {
                L1Read::Fresh(entry) => {
                    return self.read_hit(key, entry, &opts, HitKind::Fresh, CacheLevel::Memory);
                }
                L1Read::Stale(entry) => stale = Some(ReadStale::Memory(entry)),
                L1Read::Miss => {}
            }
        }
        if !(stale.is_some() && opts.skip_distributed_read_when_stale())
            && let Some(entry) = self
                .read_l2(
                    &key,
                    &opts,
                    if stale.is_some() {
                        FallbackAvailability::Available
                    } else {
                        FallbackAvailability::Unavailable
                    },
                    policy,
                    cancellation,
                )
                .await?
        {
            match self.tags(&entry.entry) {
                TagVerdict::Remove => {}
                verdict => {
                    self.hydrate(&key, &entry, &opts).await?.observe();
                    if verdict == TagVerdict::Valid
                        && entry.entry.freshness(self.inner.clock.now()).is_fresh()
                    {
                        return self.read_hit(
                            key,
                            entry.entry,
                            &opts,
                            HitKind::Fresh,
                            CacheLevel::Distributed,
                        );
                    }
                    stale = Some(match stale {
                        Some(previous)
                            if previous.entry().meta().created()
                                >= entry.entry.meta().created() =>
                        {
                            previous
                        }
                        Some(_) | None => ReadStale::Distributed(entry.entry),
                    });
                }
            }
        }
        if opts.allow_stale_on_read_only()
            && let Some(stale) = stale
            && (policy == L2ReadPolicy::FactoryFallback
                || (!stale.entry().is_physically_expired(self.inner.clock.now())
                    && self.tags(stale.entry()) != TagVerdict::Remove))
        {
            let (entry, level) = stale.into_parts();
            return self.read_hit(key, entry, &opts, HitKind::Stale, level);
        }
        self.emit(CacheEvent::Miss { key });
        Ok(Observed::new(
            MaybeValue::none(),
            OperationOutcome::Miss,
            None,
        ))
    }
    fn read_hit(
        &self,
        key: Arc<str>,
        entry: Entry<V>,
        opts: &EntryOptions,
        kind: HitKind,
        level: CacheLevel,
    ) -> Result<Observed<MaybeValue<V>>> {
        let value = self.copy(entry.value(), opts)?;
        self.emit(CacheEvent::Hit {
            key,
            stale: kind.is_stale(),
        });
        Ok(Observed::new(
            MaybeValue::from_value(value),
            kind.outcome(),
            Some(level),
        ))
    }
    fn served(
        &self,
        key: Arc<str>,
        entry: &Entry<V>,
        opts: &EntryOptions,
        level: CacheLevel,
    ) -> Result<Observed<CacheValue<V>>> {
        let value = self.copy(entry.value(), opts)?;
        self.emit(CacheEvent::Hit { key, stale: false });
        Ok(Observed::new(
            CacheValue {
                value,
                commit: CommitReceipt::Unchanged,
            },
            OperationOutcome::Hit,
            Some(level),
        ))
    }
    async fn acquire_lock(
        &self,
        key: &Arc<str>,
        opts: &EntryOptions,
        stale: Option<&Entry<V>>,
    ) -> Result<LockOutcome<V>> {
        let timeout = if opts.memory_lock_timeout().is_infinite()
            && opts.is_fail_safe_enabled()
            && stale.is_some()
        {
            opts.factory_soft_timeout()
        } else {
            opts.memory_lock_timeout()
        };
        let local = match bounded(timeout, self.inner.locks.lock(key)).await? {
            Some(local) => LocalParticipation::Held(local),
            None => {
                if opts.is_fail_safe_enabled()
                    && let Some(stale) = stale
                {
                    self.emit(CacheEvent::Hit {
                        key: Arc::clone(key),
                        stale: true,
                    });
                    return Ok(LockOutcome::Served(self.copy(stale.value(), opts)?));
                }
                LocalParticipation::UnlockedAfterTimeout
            }
        };
        let lease = if opts.skip_distributed_locker() {
            None
        } else if let Some(locker) = &self.inner.distributed_locker {
            let policy = match self.inner.lease_policy {
                LeasePolicy::Fenced => AcquisitionPolicy::TokenOwned,
                LeasePolicy::CooperativeLegacy => AcquisitionPolicy::LegacyBackendContract,
            };
            let lock_key: Arc<str> = Arc::from(format!("amalgam:lock:{}", self.inner.l2_key(key)));
            match acquire_owned_supervised(
                Arc::clone(locker),
                lock_key,
                self.inner.lease_ttl,
                opts.distributed_lock_timeout(),
                policy,
                self.lease_owner(key),
            )
            .await
            {
                Ok(Some(lease)) => Some(lease),
                Ok(None) => {
                    return Err(Error::LockTimeout {
                        elapsed: opts
                            .distributed_lock_timeout()
                            .as_duration()
                            .unwrap_or(Duration::ZERO),
                    });
                }
                Err(error)
                    if opts.rethrow_distributed_locker_exceptions()
                        || self.inner.lease_policy == LeasePolicy::Fenced =>
                {
                    return Err(error.into());
                }
                Err(error) => {
                    tracing::warn!(%error,"explicit cooperative locker degradation");
                    None
                }
            }
        } else {
            None
        };
        let unlocked = matches!(local, LocalParticipation::UnlockedAfterTimeout);
        let guard = FlightGuard {
            local,
            lease,
            tasks: Arc::clone(&self.inner.tasks),
            events: self.inner.events.clone(),
            key: Arc::clone(key),
            policy: self.inner.lease_policy,
        };
        Ok(if unlocked {
            LockOutcome::UnlockedAfterTimeout(guard)
        } else {
            LockOutcome::Acquired(guard)
        })
    }
    pub(super) async fn get_or_set<F, Fut>(
        &self,
        key: LookupKey,
        factory: F,
        options: Option<EntryOptions>,
        tags: Box<[Tag]>,
        default: MaybeValue<V>,
        caller: FactoryCancellation,
    ) -> Result<Observed<CacheValue<V>>>
    where
        F: FnOnce(FactoryContext<V>) -> Fut + Send + 'static,
        Fut: Future<Output = std::result::Result<FactoryProduct<V>, FactoryError>> + Send + 'static,
    {
        let opts = self.resolve_options(&key.raw, options)?;
        let raw_key = key.raw;
        let key = key.full;
        self.ensure_health();
        let mut stale = None;
        if !opts.skip_memory_read() {
            match self.read_l1(&key, &caller).await? {
                L1Read::Fresh(entry) => {
                    if entry.should_eager_refresh(self.inner.clock.now()) {
                        self.eager(
                            LookupKey {
                                raw: raw_key,
                                full: Arc::clone(&key),
                            },
                            opts.clone(),
                            entry.clone(),
                            tags,
                            factory,
                        );
                    }
                    return self.served(key, &entry, &opts, CacheLevel::Memory);
                }
                L1Read::Stale(entry) => stale = Some(entry),
                L1Read::Miss => {}
            }
        }
        self.emit(CacheEvent::Miss {
            key: Arc::clone(&key),
        });
        let guard = match self.acquire_lock(&key, &opts, stale.as_ref()).await? {
            LockOutcome::Acquired(guard) | LockOutcome::UnlockedAfterTimeout(guard) => guard,
            LockOutcome::Served(value) => {
                return Ok(Observed::new(
                    CacheValue {
                        value,
                        commit: CommitReceipt::Unchanged,
                    },
                    OperationOutcome::StaleHit,
                    Some(CacheLevel::Memory),
                ));
            }
        };
        if !opts.skip_memory_read() {
            match self.read_l1(&key, &caller).await? {
                L1Read::Fresh(entry) => {
                    return self.served(key, &entry, &opts, CacheLevel::Memory);
                }
                L1Read::Stale(entry) => stale = Some(entry),
                L1Read::Miss => {}
            }
        }
        if !(stale.is_some() && opts.skip_distributed_read_when_stale())
            && let Some(entry) = self
                .read_l2(
                    &key,
                    &opts,
                    if stale.is_some() || default.has_value() {
                        FallbackAvailability::Available
                    } else {
                        FallbackAvailability::Unavailable
                    },
                    L2ReadPolicy::FactoryFallback,
                    &caller,
                )
                .await?
        {
            let verdict = self.tags(&entry.entry);
            if verdict != TagVerdict::Remove {
                self.hydrate(&key, &entry, &opts).await?.observe();
                if verdict == TagVerdict::Valid
                    && entry.entry.freshness(self.inner.clock.now()).is_fresh()
                {
                    return self.served(key, &entry.entry, &opts, CacheLevel::Distributed);
                }
                stale = Some(newer_of(stale, entry.entry));
            }
        }
        let timeout = opts.appropriate_factory_timeout(stale.is_some() || default.has_value());
        let soft = opts.is_fail_safe_enabled()
            && (stale.is_some() || default.has_value())
            && timeout == opts.factory_soft_timeout()
            && timeout != opts.factory_hard_timeout();
        if matches!(timeout,Timeout::After(duration) if duration.is_zero()) {
            drop(guard);
            return self
                .timeout_fallback(&key, &opts, stale.as_ref(), &default, timeout)
                .await;
        }
        let source = CancellationSource::new();
        let origin_cancellation = source.token();
        let ctx = FactoryContext::with_cancellation(
            LookupKey {
                raw: raw_key,
                full: Arc::clone(&key),
            },
            opts.clone(),
            tags,
            stale
                .as_ref()
                .map(|entry| self.stale_info(entry, &opts))
                .transpose()?,
            origin_cancellation.clone(),
        );
        let started = self.inner.clock.now();
        let worker = self.clone();
        let flight_key = Arc::clone(&key);
        let cancelled = source.clone();
        let lease_state = guard.lease.as_ref().map(DistributedLease::state);
        let mut execution=self.inner.scopes.execution(async move {
            let origin=async move {factory(ctx).await};
            let product=if let Some(mut state)=lease_state {tokio::select! {biased; ()=lease_lost(&mut state)=>{cancelled.cancel_with(Reason::LeaseLost);return Err(Error::FactoryCancelled {reason:Reason::LeaseLost});},product=origin=>product}}else{origin.await};
            let product=product.map_err(Error::from)?;
            let result=worker.store_product(flight_key,product,started,guard,&origin_cancellation).await;
            if matches!(&result,Err(Error::Lease(LeaseError::Lost))){cancelled.cancel_with(Reason::LeaseLost);}result
        }.instrument(component_span(&self.inner.name,CacheLevel::Origin,"factory",Some(&key))),source);
        execution.link(&caller, LinkMode::CallerScope);
        let result = match timeout {
            Timeout::Infinite => Some(execution.await),
            Timeout::After(duration) => {
                tokio::select! {biased;result=&mut execution=>Some(result),()=tokio::time::sleep(duration)=>{
                    self.emit(CacheEvent::FactorySyntheticTimeout {key:Arc::clone(&key)});
                    if opts.allow_timed_out_factory_background_completion(){self.background_origin(Arc::clone(&key),execution);}else{execution.cancel(if soft {Reason::SoftTimeout}else{Reason::HardTimeout});}
                    None
                }}
            }
        };
        match result {
            Some(Ok(value)) => Ok(Observed::new(
                value,
                OperationOutcome::Stored,
                Some(CacheLevel::Origin),
            )),
            Some(Err(error))
                if matches!(
                    &error,
                    Error::Factory { .. } | Error::FactoryWithSource { .. }
                ) =>
            {
                self.emit(CacheEvent::FactoryError {
                    key: Arc::clone(&key),
                    message: error.to_string(),
                });
                if let Some(value) = self.fallback(&key, &opts, stale.as_ref(), &default).await? {
                    Ok(Observed::new(
                        CacheValue {
                            value,
                            commit: CommitReceipt::Unchanged,
                        },
                        OperationOutcome::StaleHit,
                        Some(CacheLevel::Memory),
                    ))
                } else {
                    Err(error)
                }
            }
            Some(Err(error)) => Err(error),
            None => {
                self.timeout_fallback(&key, &opts, stale.as_ref(), &default, timeout)
                    .await
            }
        }
    }
    async fn timeout_fallback(
        &self,
        key: &Arc<str>,
        opts: &EntryOptions,
        stale: Option<&Entry<V>>,
        default: &MaybeValue<V>,
        timeout: Timeout,
    ) -> Result<Observed<CacheValue<V>>> {
        if let Some(value) = self.fallback(key, opts, stale, default).await? {
            Ok(Observed::new(
                CacheValue {
                    value,
                    commit: CommitReceipt::Unchanged,
                },
                OperationOutcome::StaleHit,
                Some(CacheLevel::Memory),
            ))
        } else {
            Err(Error::FactoryTimeout {
                elapsed: timeout.as_duration().unwrap_or(Duration::ZERO),
            })
        }
    }
    async fn fallback(
        &self,
        key: &Arc<str>,
        opts: &EntryOptions,
        stale: Option<&Entry<V>>,
        default: &MaybeValue<V>,
    ) -> Result<Option<V>> {
        if !opts.is_fail_safe_enabled() {
            return Ok(None);
        }
        let now = self.inner.clock.now();
        let entry = if let Some(stale) = stale {
            Entry::try_throttled(stale, opts, now)?
        } else {
            None
        };
        let entry = match entry {
            Some(entry) => Some(entry),
            None => default
                .value()
                .map(|value| {
                    self.copy(value, opts)
                        .and_then(|value| Entry::try_from_fail_safe_default(value, opts, now))
                })
                .transpose()?,
        };
        let Some(entry) = entry else {
            return Ok(None);
        };
        let value = self.copy(entry.value(), opts)?;
        if !opts.skip_memory_write() {
            self.inner
                .memory
                .insert_at(Arc::clone(key), entry, now)
                .await;
        }
        self.emit(CacheEvent::FailSafeActivate {
            key: Arc::clone(key),
        });
        self.emit(CacheEvent::Hit {
            key: Arc::clone(key),
            stale: true,
        });
        Ok(Some(value))
    }
    fn background_origin(&self, key: Arc<str>, execution: Execution<CacheValue<V>>) {
        let worker = self.clone();
        let success = Arc::clone(&key);
        let _receiver = self.inner.tasks.spawn(
            ShutdownTask::Factory,
            key,
            self.inner.events.clone(),
            async move {
                let result = execution.await?;
                worker.emit(CacheEvent::BackgroundFactorySuccess { key: success });
                drop(result);
                Ok(())
            },
        );
    }
    fn eager<F, Fut>(
        &self,
        keys: LookupKey,
        opts: EntryOptions,
        current: Entry<V>,
        tags: Box<[Tag]>,
        factory: F,
    ) where
        F: FnOnce(FactoryContext<V>) -> Fut + Send + 'static,
        Fut: Future<Output = std::result::Result<FactoryProduct<V>, FactoryError>> + Send + 'static,
    {
        let key = Arc::clone(&keys.full);
        let Some(local) = self.inner.locks.try_lock(&key) else {
            return;
        };
        self.emit(CacheEvent::EagerRefresh {
            key: Arc::clone(&key),
        });
        let worker = self.clone();
        let background_key = Arc::clone(&key);
        let source = CancellationSource::new();
        let token = source.token();
        let cancelled = source.clone();
        let execution=self.inner.scopes.execution(async move {
            let mut guard=FlightGuard {local:LocalParticipation::Held(local),lease:None,tasks:Arc::clone(&worker.inner.tasks),events:worker.inner.events.clone(),key:Arc::clone(&key),policy:worker.inner.lease_policy};
            if !opts.skip_distributed_locker()&&let Some(locker)=&worker.inner.distributed_locker {
                guard.lease=acquire_owned_supervised(Arc::clone(locker),Arc::from(format!("amalgam:lock:{}",worker.inner.l2_key(&key))),worker.inner.lease_ttl,opts.distributed_lock_timeout(),match worker.inner.lease_policy {LeasePolicy::Fenced=>AcquisitionPolicy::TokenOwned,LeasePolicy::CooperativeLegacy=>AcquisitionPolicy::LegacyBackendContract},worker.lease_owner(&key)).await?;
                if guard.lease.is_none(){return Err(Error::LockTimeout {elapsed:opts.distributed_lock_timeout().as_duration().unwrap_or(Duration::ZERO)});}
            }
            if let Some(entry)=worker.read_l2(&key,&opts,FallbackAvailability::Available,L2ReadPolicy::FactoryFallback,&token).await?&&entry.entry.meta().created()>current.meta().created()&&worker.tags(&entry.entry)==TagVerdict::Valid&&entry.entry.freshness(worker.inner.clock.now()).is_fresh(){worker.hydrate(&key,&entry,&opts).await?.observe();return Ok(CacheValue {value:worker.copy(entry.entry.value(),&opts)?,commit:CommitReceipt::Unchanged});}
            let ctx=FactoryContext::with_cancellation(keys,opts.clone(),tags,Some(worker.stale_info(&current,&opts)?),token.clone());
            let started=worker.inner.clock.now();
            let origin=factory(ctx);
            let product=if let Some(mut state)=guard.lease.as_ref().map(DistributedLease::state) {
                tokio::select! {biased; ()=lease_lost(&mut state)=>return Err(Error::FactoryCancelled {reason:Reason::LeaseLost}),product=origin=>product}
            }else{origin.await};
            let product=product.map_err(Error::from)?;let result=worker.store_product(key,product,started,guard,&token).await;
            if matches!(&result,Err(Error::Lease(LeaseError::Lost))){cancelled.cancel_with(Reason::LeaseLost);}result
        },source);
        self.background_origin(background_key, execution);
    }
}
