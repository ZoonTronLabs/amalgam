//! Value reads, same-key origin work, fail-safe and eager refresh.
use super::{
    AcquisitionPolicy, Arc, CacheEvent, CacheLevel, CacheOrigin, CacheValue, CircuitComponent,
    CommitReceipt, DistributedEvent, DistributedLookup, Duration, Entry, EntryOptions, Error,
    Execution, ExecutionCheckpoint, FactoryCancellation, FactoryContext, FallbackAvailability,
    FlightGuard, HitKind, HydrationFence, HydrationOutcome, Instrument, L1Read, L2ReadPolicy,
    LayerEvent, LeaseError, LeasePolicy, LocalParticipation, LockOutcome, LookupKey,
    MarkerReadPolicy, MemoryEvent, Observed, OperationOutcome, Ordering, OriginCompletion,
    OriginKind, ReadStale, Reason, Result, ShutdownTask, SkipReason, Storage, Tag, TagVerdict,
    Timeout, Worker, acquire_owned_supervised, bounded, component_span, lease_lost, newer_of,
};

// Physical completion remains visible after a later marker timeout. The
// checkpoint borrows the child phase; the owned parent retains the whole frame.
type DistributedCheckpoint<'a, V> = ExecutionCheckpoint<'a, Option<DistributedLookup<V>>>;

// A synchronous codec cannot retain a cancellation token. Its read can share
// the already-owned parent, including provider suspension and shutdown drainage.
// An asynchronous codec retains its separate phase reason and owned token.
enum L2ReadBudget<V> {
    Completed(Result<Option<DistributedLookup<V>>>),
    TimedOut,
}
impl<V> L2ReadBudget<V> {
    fn from_bounded(result: Result<Option<Result<Option<DistributedLookup<V>>>>>) -> Self {
        match result {
            Ok(Some(result)) => Self::Completed(result),
            Ok(None) => Self::TimedOut,
            Err(error) => Self::Completed(Err(error)),
        }
    }
}

// Only a confirmed lookup miss transfers the flight guard into origin work.
// Keeping this continuation behind its own pin avoids reserving the retained
// factory/timeout/store state in every successful hybrid lookup frame.
struct OriginMiss<V, O> {
    key: LookupKey,
    origin: O,
    options: EntryOptions,
    tags: Box<[Tag]>,
    stale: Option<Entry<V>>,
    default: Option<V>,
    caller: super::origin::OriginCaller,
    guard: FlightGuard,
}

// Cache defaults are immutable and validated at construction. A lookup borrows
// them; only a real origin miss materializes its adaptive factory options.
// Overrides keep their existing allocation and validation boundary.
enum LookupOptions<'a> {
    Defaults(&'a EntryOptions),
    Selected(Box<EntryOptions>),
}
impl std::ops::Deref for LookupOptions<'_> {
    type Target = EntryOptions;
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Defaults(options) => options,
            Self::Selected(options) => options,
        }
    }
}
impl LookupOptions<'_> {
    fn into_owned(self) -> EntryOptions {
        match self {
            Self::Defaults(options) => options.clone(),
            Self::Selected(options) => *options,
        }
    }
}

impl<V: Clone + Send + Sync + 'static> Worker<V> {
    fn resolve_lookup_options(
        &self,
        key: &str,
        options: Option<Box<EntryOptions>>,
    ) -> Result<LookupOptions<'_>> {
        let selected = options.or_else(|| {
            self.inner
                .default_options_provider
                .as_ref()
                .and_then(|provider| {
                    provider
                        .options_for_with_defaults(key, &self.inner.default_options)
                        .map(Box::new)
                })
        });
        match selected {
            Some(options) => {
                self.validate_options(&options)?;
                Ok(LookupOptions::Selected(options))
            }
            None => {
                // Runtime availability belongs to this execution, not build time.
                self.inner.default_runtime.validate()?;
                Ok(LookupOptions::Defaults(&self.inner.default_options))
            }
        }
    }
    async fn read_l1(
        &self,
        key: &Arc<str>,
        cancellation: &FactoryCancellation,
    ) -> Result<L1Read<V>> {
        let captured = self.inner.epoch.load(Ordering::Acquire);
        let now = self.inner.clock.now();
        let Some(entry) = self.memory.get_at(key, now).await? else {
            self.memory.emit_layer_lazy(|| {
                LayerEvent::Memory(MemoryEvent::Miss {
                    key: Arc::clone(key),
                })
            });
            return Ok(L1Read::Miss);
        };
        self.memory.emit_layer_lazy(|| {
            LayerEvent::Memory(MemoryEvent::Hit {
                key: Arc::clone(key),
                stale: entry.is_logically_expired(now),
            })
        });
        self.reconcile_controlled_markers(&entry, cancellation)
            .await?;
        self.ensure_health()?;
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
                if self.memory.remove_if_same(key, &entry).await?.is_some() {
                    self.memory.emit_layer_lazy(|| {
                        LayerEvent::Memory(MemoryEvent::Remove {
                            key: Arc::clone(key),
                        })
                    });
                }
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
            tracing::warn!(%error,key=%key,"read-only/origin lookup skipped an open circuit");
            return Ok(None);
        }
        let timeout = opts
            .appropriate_distributed_timeout(matches!(fallback, FallbackAvailability::Available));
        let soft = opts.is_fail_safe_enabled()
            && matches!(fallback, FallbackAvailability::Available)
            && timeout == opts.distributed_soft_timeout()
            && timeout != opts.distributed_hard_timeout();
        let (budget, physically_completed) =
            self.read_l2_budget(key, timeout, soft, cancellation).await;
        let result = match budget {
            L2ReadBudget::Completed(result) => result,
            L2ReadBudget::TimedOut => {
                if opts.is_fail_safe_enabled()
                    && matches!(fallback, FallbackAvailability::Available)
                    && timeout == opts.distributed_soft_timeout()
                {
                    Ok(None)
                } else {
                    Err(Error::DistributedTimeout {
                        elapsed: timeout.as_duration().unwrap_or(Duration::ZERO),
                    })
                }
            }
        };
        match result {
            Ok(value) => {
                if value.is_none() && !physically_completed {
                    self.distributed_miss(key);
                }
                if let Some(entry) = &value {
                    self.reconcile_controlled_markers(&entry.entry, cancellation)
                        .await?;
                }
                Ok(value)
            }
            Err(error) => {
                self.failure(key, &error);
                if L2ReadPolicy::rethrow(opts, &error) {
                    Err(error)
                } else {
                    tracing::warn!(%error,key=%key,"distributed read degraded to miss");
                    if !physically_completed {
                        self.distributed_miss(key);
                    }
                    Ok(None)
                }
            }
        }
    }
    async fn read_l2_budget(
        &self,
        key: &Arc<str>,
        timeout: Timeout,
        soft: bool,
        cancellation: &FactoryCancellation,
    ) -> (L2ReadBudget<V>, bool) {
        match &self.inner.storage {
            Storage::Hybrid {
                serializer: crate::distributed::Serializer::Sync(_),
                ..
            } => self.read_l2_parent_budget(key, timeout, cancellation).await,
            Storage::Hybrid {
                serializer: crate::distributed::Serializer::Async(_),
                ..
            }
            | Storage::MemoryOnly => {
                self.read_l2_codec_budget(key, timeout, soft, cancellation)
                    .await
            }
        }
    }
    async fn read_l2_parent_budget(
        &self,
        key: &Arc<str>,
        timeout: Timeout,
        cancellation: &FactoryCancellation,
    ) -> (L2ReadBudget<V>, bool) {
        let completed = std::sync::atomic::AtomicBool::new(false);
        let checkpoint = DistributedCheckpoint::borrowing(&completed);
        let span = component_span(&self.inner.name, CacheLevel::Distributed, "read", Some(key));
        let mut work = std::pin::pin!(
            self.fetch_l2(key, cancellation, &checkpoint)
                .instrument(span)
        );
        let result = bounded(timeout, work.as_mut()).await;
        (
            L2ReadBudget::from_bounded(result),
            completed.load(Ordering::Acquire),
        )
    }
    async fn read_l2_codec_budget(
        &self,
        key: &Arc<str>,
        timeout: Timeout,
        soft: bool,
        cancellation: &FactoryCancellation,
    ) -> (L2ReadBudget<V>, bool) {
        let phase = crate::execution::BorrowedPhase::new(self.scopes(), cancellation);
        let phase_cancellation = phase.token();
        let checkpoint = phase.checkpoint();
        let span = component_span(&self.inner.name, CacheLevel::Distributed, "read", Some(key));
        let mut work = std::pin::pin!(
            self.fetch_l2(key, &phase_cancellation, &checkpoint)
                .instrument(span)
        );
        // Publish the precise codec reason before its future is destroyed.
        let _retirement = phase.retirement();
        let result = bounded(
            timeout,
            std::future::poll_fn(|cx| phase.poll(cx, work.as_mut())),
        )
        .await;
        if matches!(result, Ok(None)) {
            phase.cancel(if soft {
                Reason::SoftTimeout
            } else {
                Reason::HardTimeout
            });
        }
        (
            L2ReadBudget::from_bounded(result),
            phase.checkpoint_reached(),
        )
    }
    fn distributed_miss(&self, key: &Arc<str>) {
        self.memory.emit_layer_lazy(|| {
            LayerEvent::Distributed(DistributedEvent::Miss {
                key: Arc::clone(key),
            })
        });
    }
    async fn fetch_l2(
        &self,
        key: &Arc<str>,
        cancellation: &FactoryCancellation,
        observation: &DistributedCheckpoint<'_, V>,
    ) -> Result<Option<DistributedLookup<V>>> {
        let Storage::Hybrid {
            backend,
            serializer,
        } = &self.inner.storage
        else {
            return Ok(None);
        };
        cancellation.check()?;
        let hydration = self.hydration_fence(key).await?;
        self.memory
            .component_read(crate::events::ComponentRead::Distributed);
        let physical = self.inner.l2_key(key);
        let bytes = match backend.get_immediate(&physical) {
            crate::distributed::ImmediateRead::Completed(bytes) => bytes?,
            crate::distributed::ImmediateRead::Deferred => backend.get(&physical).await?,
        };
        self.close_circuit(CircuitComponent::Distributed);
        let Some(bytes) = bytes else {
            observation.record();
            self.distributed_miss(key);
            return Ok(None);
        };
        let snapshot = serializer
            .decode(&bytes, self.inner.serialization_mode, cancellation)
            .await?;
        let now = self.inner.clock.now();
        let source = snapshot.try_into_cache_entry(now)?;
        observation.record();
        if source.is_physically_expired(now) {
            self.distributed_miss(key);
            return Ok(None);
        }
        self.memory.emit_layer_lazy(|| {
            LayerEvent::Distributed(DistributedEvent::Hit {
                key: Arc::clone(key),
                stale: source.is_logically_expired(now),
            })
        });
        if self.inner.marker_reads.policy() == MarkerReadPolicy::DurableRequired {
            self.reconcile_markers(source.meta().tags()).await?;
        }
        Ok(Some(DistributedLookup {
            entry: source,
            hydration,
        }))
    }
    async fn hydration_fence(&self, key: &str) -> Result<HydrationFence<V>> {
        let lane = self.inner.lanes.get(key);
        let fence = {
            let Some(_guard) = lane.try_lock() else {
                // A read overlapping an already started commit may return its
                // snapshot, but cannot install it after that commit completes.
                return Ok(HydrationFence::ConcurrentMutation);
            };
            let _guard = self.memory.guard(_guard);
            lane.snapshot(&self.inner.epoch)
        };
        let observed = self.memory.get_at(key, self.inner.clock.now()).await?;
        Ok(HydrationFence::Stable { fence, observed })
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
        let Some(_guard) = fence.lane.try_lock() else {
            // Hydration is optional. A newer mutation already owning this lane
            // must not delay the read or install an older snapshot afterward.
            return Ok(HydrationOutcome::Skipped(SkipReason::Superseded));
        };
        let _guard = self.memory.guard(_guard);
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
            .prepare_memory_hydration(opts, self.inner.clock.now())?
        else {
            return Ok(HydrationOutcome::Skipped(SkipReason::PhysicallyExpired));
        };
        let local = local.with_hydrated_value(
            self.copy(source.entry.value(), opts)?,
            fence.continuity_stamp(),
        );
        if !fence.passive_is_current() {
            return Ok(HydrationOutcome::Skipped(SkipReason::Superseded));
        }
        Ok(HydrationOutcome::Evaluated(
            self.memory
                .insert_if_unchanged(
                    Arc::clone(key),
                    observed.as_ref(),
                    local,
                    self.inner.clock.now(),
                )
                .await?,
        ))
    }
    pub(super) async fn read(
        &self,
        key: LookupKey,
        options: Option<Box<EntryOptions>>,
        cancellation: &FactoryCancellation,
    ) -> Result<Observed<Option<V>>> {
        let opts = self.resolve_lookup_options(&key.raw, options)?;
        let key = key.full;
        self.ensure_health()?;
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
        {
            let (entry, level) = stale.into_parts();
            return self.read_hit(key, entry, &opts, HitKind::Stale, level);
        }
        self.emit(CacheEvent::Miss { key });
        Ok(Observed::new(None, OperationOutcome::Miss, None))
    }
    fn read_hit(
        &self,
        key: Arc<str>,
        entry: Entry<V>,
        opts: &EntryOptions,
        kind: HitKind,
        level: CacheLevel,
    ) -> Result<Observed<Option<V>>> {
        let value = self.copy(entry.value(), opts)?;
        self.emit(CacheEvent::Hit {
            key,
            stale: kind.is_stale(),
        });
        Ok(Observed::new(Some(value), kind.outcome(), Some(level)))
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
        cancellation: &FactoryCancellation,
    ) -> Result<LockOutcome<V>> {
        let timeout = if opts.memory_lock_timeout().is_infinite()
            && opts.is_fail_safe_enabled()
            && stale.is_some()
        {
            opts.factory_soft_timeout()
        } else {
            opts.memory_lock_timeout()
        };
        let acquiring = self.inner.locks.acquire(
            key,
            crate::provider::MemoryLockKind::Entry,
            timeout,
            cancellation,
            self.memory_acquire_route(),
        );
        let local = match self.inner.assist_any_origin(key, acquiring).await? {
            Some(local) => LocalParticipation::Held(self.memory.guard(local)),
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
                LeasePolicy::Cooperative => AcquisitionPolicy::LegacyBackendContract,
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
        let guard = self.flight_guard(key, local, lease, self.inner.lease_policy);
        Ok(if unlocked {
            LockOutcome::UnlockedAfterTimeout(guard)
        } else {
            LockOutcome::Acquired(guard)
        })
    }
    pub(super) async fn get_or_set<O: CacheOrigin<V>>(
        &self,
        key: LookupKey,
        origin: O,
        options: Option<Box<EntryOptions>>,
        tags: Box<[Tag]>,
        default: Option<V>,
        caller: super::origin::OriginCaller,
    ) -> Result<Observed<CacheValue<V>>> {
        let super::origin::OriginCaller {
            operation: caller,
            explicit,
        } = caller;
        let opts = self.resolve_lookup_options(&key.raw, options)?;
        let raw_key = key.raw;
        let key = key.full;
        self.ensure_health()?;
        let mut stale = None;
        if !opts.skip_memory_read() {
            match self.read_l1(&key, &caller).await? {
                L1Read::Fresh(entry) => {
                    if matches!(O::KIND, OriginKind::Factory)
                        && entry.should_eager_refresh(self.inner.clock.now())
                    {
                        self.eager(
                            LookupKey {
                                raw: raw_key,
                                full: Arc::clone(&key),
                            },
                            opts.clone(),
                            entry.clone(),
                            tags,
                            origin,
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
        let guard = match self
            .acquire_lock(&key, &opts, stale.as_ref(), &caller)
            .await?
        {
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
                    if stale.is_some() || default.is_some() {
                        FallbackAvailability::Available
                    } else {
                        FallbackAvailability::Unavailable
                    },
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
        Box::pin(self.compute_origin(OriginMiss {
            key: LookupKey {
                raw: raw_key,
                full: key,
            },
            origin,
            options: opts.into_owned(),
            tags,
            stale,
            default,
            caller: super::origin::OriginCaller {
                operation: caller,
                explicit,
            },
            guard,
        }))
        .await
    }
    async fn compute_origin<O: CacheOrigin<V>>(
        &self,
        miss: OriginMiss<V, O>,
    ) -> Result<Observed<CacheValue<V>>> {
        let OriginMiss {
            key: LookupKey {
                raw: raw_key,
                full: key,
            },
            origin,
            options: opts,
            tags,
            stale,
            default,
            caller:
                super::origin::OriginCaller {
                    operation: caller,
                    explicit,
                },
            guard,
        } = miss;
        let (timeout, soft, operation) = match O::KIND {
            OriginKind::Factory => {
                let timeout = opts.appropriate_factory_timeout(stale.is_some());
                let soft = opts.is_fail_safe_enabled()
                    && stale.is_some()
                    && timeout == opts.factory_soft_timeout()
                    && timeout != opts.factory_hard_timeout();
                (timeout, soft, "factory")
            }
            OriginKind::Constant => (Timeout::Infinite, false, "constant"),
        };
        if matches!(timeout,Timeout::After(duration) if duration.is_zero()) {
            drop(guard);
            return self
                .timeout_fallback(&key, &opts, stale.as_ref(), &default, timeout)
                .await;
        }
        let mut execution = self
            .inner
            .origin_work
            .get_or_init(crate::retained_origin::RetainedOrigins::new)
            .start(Arc::clone(&key), Arc::clone(self.scopes()));
        execution.link(&caller);
        if let Some(explicit) = &explicit {
            execution.link_explicit(explicit);
        }
        let source = execution.source();
        let origin_cancellation = source.token();
        let ctx = FactoryContext::with_cancellation(
            LookupKey {
                raw: raw_key,
                full: Arc::clone(&key),
            },
            opts.clone(),
            tags,
            match O::KIND {
                OriginKind::Factory => stale
                    .as_ref()
                    .map(|entry| self.stale_info(entry, &opts))
                    .transpose()?,
                OriginKind::Constant => None,
            },
            origin_cancellation.clone(),
        );
        let lease_state = guard.lease.state();
        let guard = self.capture_origin(&key, guard)?;
        let started = self.inner.clock.now();
        let worker = self.clone();
        let flight_key = Arc::clone(&key);
        let cancelled = source.clone();
        let reporter = execution.reporter();
        execution.begin(
            async move {
                let result = async {
                    origin_cancellation.check()?;
                    let origin = origin.invoke(ctx);
                    let product = if let Some(mut state) = lease_state {
                        tokio::select! {
                            biased;
                            () = lease_lost(&mut state) => {
                                cancelled.cancel_with(Reason::LeaseLost);
                                return Err(Error::FactoryCancelled { reason: Reason::LeaseLost });
                            },
                            product = origin => product,
                        }
                    } else {
                        origin.await
                    };
                    let product = product.map_err(Error::from)?;
                    let result = worker
                        .store_product(
                            Arc::clone(&flight_key),
                            product,
                            started,
                            guard,
                            &origin_cancellation,
                        )
                        .await;
                    if matches!(&result, Err(Error::Lease(LeaseError::Lost))) {
                        cancelled.cancel_with(Reason::LeaseLost);
                    } else {
                        origin_cancellation.check()?;
                    }
                    result
                }
                .await;
                let audience = reporter.complete(&result);
                worker.report_origin_result(flight_key, &result, audience);
                result
            }
            .instrument(component_span(
                &self.inner.name,
                CacheLevel::Origin,
                operation,
                Some(&key),
            )),
        );
        if execution.is_pending() && self.inner.tasks.can_execute() {
            let _completion = self.inner.tasks.spawn(
                ShutdownTask::Factory,
                Arc::clone(&key),
                self.inner.events.clone(),
                execution.driver(),
            );
        }
        let result = match timeout {
            Timeout::Infinite => Some(execution.await),
            Timeout::After(duration) => {
                tokio::select! {biased;result=&mut execution=>Some(result),()=tokio::time::sleep(duration)=>{
                    self.emit(CacheEvent::FactorySyntheticTimeout {key:Arc::clone(&key)});
                    if !opts.allow_timed_out_factory_background_completion(){execution.cancel(if soft {Reason::SoftTimeout}else{Reason::HardTimeout});}
                    None
                }}
            }
        };
        match result {
            Some(Ok(completion)) => {
                let (value, level) = match completion {
                    OriginCompletion::Factory(value) => (value, Some(CacheLevel::Origin)),
                    OriginCompletion::Constant(value) => (value, None),
                    OriginCompletion::Distributed(value) => (value, Some(CacheLevel::Distributed)),
                };
                Ok(Observed::new(value, OperationOutcome::Stored, level))
            }
            Some(Err(error))
                if matches!(
                    &error,
                    Error::Factory { .. } | Error::FactoryWithSource { .. }
                ) =>
            {
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
    fn report_origin_result(
        &self,
        key: Arc<str>,
        result: &Result<OriginCompletion<V>>,
        audience: crate::retained_origin::Audience,
    ) {
        match result {
            Ok(OriginCompletion::Factory(_)) => self.emit(match audience {
                crate::retained_origin::Audience::Foreground => CacheEvent::FactorySuccess { key },
                crate::retained_origin::Audience::Background => {
                    CacheEvent::BackgroundFactorySuccess { key }
                }
            }),
            Err(error)
                if matches!(
                    error,
                    Error::Factory { .. } | Error::FactoryWithSource { .. }
                ) =>
            {
                self.emit(CacheEvent::FactoryError {
                    key,
                    message: error.to_string(),
                });
            }
            Ok(OriginCompletion::Constant(_) | OriginCompletion::Distributed(_)) | Err(_) => {}
        }
    }
    async fn timeout_fallback(
        &self,
        key: &Arc<str>,
        opts: &EntryOptions,
        stale: Option<&Entry<V>>,
        default: &Option<V>,
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
        default: &Option<V>,
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
                .as_ref()
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
            self.memory.insert_at(Arc::clone(key), entry, now).await?;
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
    fn background_origin(&self, key: Arc<str>, execution: Execution<OriginCompletion<V>>) {
        let worker = self.clone();
        let success = Arc::clone(&key);
        let _receiver = self.inner.tasks.spawn(
            ShutdownTask::Factory,
            key,
            self.inner.events.clone(),
            async move {
                match execution.await? {
                    OriginCompletion::Factory(value) => {
                        worker.emit(CacheEvent::BackgroundFactorySuccess { key: success });
                        drop(value);
                    }
                    OriginCompletion::Constant(value) | OriginCompletion::Distributed(value) => {
                        drop(value)
                    }
                }
                Ok(())
            },
        );
    }
    pub(super) fn eager<O: CacheOrigin<V>>(
        &self,
        keys: LookupKey,
        opts: EntryOptions,
        current: Entry<V>,
        tags: Box<[Tag]>,
        origin: O,
    ) {
        let key = Arc::clone(&keys.full);
        let claim = match self.inner.locks.try_eager(&key) {
            Ok(Some(claim)) => claim,
            Ok(None) => return,
            Err(error) => {
                tracing::warn!(%error, "local eager lock acquisition failed");
                return;
            }
        };
        let local = claim.guard;
        let source = claim.source;
        let token = source.token();
        self.emit(CacheEvent::EagerRefresh {
            key: Arc::clone(&key),
        });
        let worker = self.clone();
        let background_key = Arc::clone(&key);
        let cancelled = source.clone();
        let execution=self.scopes().execution(async move {
            let mut guard=worker.flight_guard(&key,LocalParticipation::Held(worker.memory.guard(local)),None,worker.inner.lease_policy);
            if !opts.skip_distributed_locker()&&let Some(locker)=&worker.inner.distributed_locker {
                let lease=acquire_owned_supervised(Arc::clone(locker),Arc::from(format!("amalgam:lock:{}",worker.inner.l2_key(&key))),worker.inner.lease_ttl,opts.distributed_lock_timeout(),match worker.inner.lease_policy {LeasePolicy::Fenced=>AcquisitionPolicy::TokenOwned,LeasePolicy::Cooperative=>AcquisitionPolicy::LegacyBackendContract},worker.lease_owner(&key)).await?;guard.lease=worker.cluster_participation(&key,lease,worker.inner.lease_policy);
                if guard.lease.is_local(){return Err(Error::LockTimeout {elapsed:opts.distributed_lock_timeout().as_duration().unwrap_or(Duration::ZERO)});}
            }
            if let Some(entry)=worker.read_l2(&key,&opts,FallbackAvailability::Available,&token).await?&&entry.entry.meta().created()>current.meta().created()&&worker.tags(&entry.entry)==TagVerdict::Valid&&entry.entry.freshness(worker.inner.clock.now()).is_fresh(){worker.hydrate(&key,&entry,&opts).await?.observe();return Ok(OriginCompletion::Distributed(CacheValue {value:worker.copy(entry.entry.value(),&opts)?,commit:CommitReceipt::Unchanged}));}
            let ctx=FactoryContext::with_cancellation(keys,opts.clone(),tags,Some(worker.stale_info(&current,&opts)?),token.clone()).with_invocation(crate::factory::FactoryInvocation::EagerRefresh);
            let lease_state=guard.lease.state();
            let guard=worker.capture_origin(&key,guard)?;
            let started=worker.inner.clock.now();
            let origin=origin.invoke(ctx);
            let product=if let Some(mut state)=lease_state {
                tokio::select! {biased; ()=lease_lost(&mut state)=>return Err(Error::FactoryCancelled {reason:Reason::LeaseLost}),product=origin=>product}
            }else{origin.await};
            let product=product.map_err(Error::from)?;let result=worker.store_product(key,product,started,guard,&token).await;
            if matches!(&result,Err(Error::Lease(LeaseError::Lost))){cancelled.cancel_with(Reason::LeaseLost);}result
        },source);
        self.background_origin(background_key, execution);
    }
}

enum OriginProgress<V: Clone + Send + Sync + 'static> {
    General(crate::retained_origin::OriginProgress<OriginCompletion<V>>),
    Plain(crate::single_flight::Progress<super::inline_cold::Value<V>>),
}
impl<V: Clone + Send + Sync + 'static> std::future::Future for OriginProgress<V> {
    type Output = Result<()>;
    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match self.get_mut() {
            Self::General(progress) => std::pin::Pin::new(progress).poll(cx),
            Self::Plain(progress) => std::pin::Pin::new(progress).poll(cx),
        }
    }
}
#[derive(Clone, Copy)]
enum Assistance {
    GeneralOnly,
    Any,
}
impl<V: Clone + Send + Sync + 'static> super::CacheInner<V> {
    pub(super) async fn assist_origin<T>(
        &self,
        key: &str,
        acquiring: impl std::future::Future<Output = Result<T>>,
    ) -> Result<T> {
        // A plain flight cannot drive itself while holding its poll ownership.
        self.assist_acquisition(key, acquiring, Assistance::GeneralOnly)
            .await
    }
    pub(super) async fn assist_any_origin<T>(
        &self,
        key: &str,
        acquiring: impl std::future::Future<Output = Result<T>>,
    ) -> Result<T> {
        self.assist_acquisition(key, acquiring, Assistance::Any)
            .await
    }
    fn origin_progress(&self, key: &str, mode: Assistance) -> Option<OriginProgress<V>> {
        self.origin_work
            .get()
            .and_then(|work| work.help(key))
            .map(OriginProgress::General)
            .or_else(|| match mode {
                Assistance::GeneralOnly => None,
                Assistance::Any => self
                    .flights
                    .as_ref()?
                    .active(key)
                    .map(|flight| OriginProgress::Plain(flight.progress())),
            })
    }
    async fn assist_acquisition<T>(
        &self,
        key: &str,
        acquiring: impl std::future::Future<Output = Result<T>>,
        mode: Assistance,
    ) -> Result<T> {
        let mut helper = self.origin_progress(key, mode);
        let mut changed = None;
        let mut acquiring = std::pin::pin!(acquiring);
        std::future::poll_fn(|cx| {
            loop {
                if let Some(progress) = &mut helper {
                    match std::pin::Pin::new(progress).poll(cx) {
                        std::task::Poll::Ready(Err(error)) => {
                            return std::task::Poll::Ready(Err(error));
                        }
                        std::task::Poll::Ready(Ok(())) => helper = None,
                        std::task::Poll::Pending => {}
                    }
                }
                if let ready @ std::task::Poll::Ready(_) = acquiring.as_mut().poll(cx) {
                    return ready;
                }
                if changed.is_none() {
                    // Only a genuinely suspended acquisition allocates notification
                    // ownership. Subscribe before rechecking the active table so a
                    // factory installed after lock acquisition cannot be missed.
                    let registry = self
                        .origin_work
                        .get_or_init(crate::retained_origin::RetainedOrigins::new);
                    let mut notification = Box::pin(registry.changed());
                    notification.as_mut().enable();
                    changed = Some(notification);
                    if helper.is_none() {
                        helper = self.origin_progress(key, mode);
                        if helper.is_some() {
                            continue;
                        }
                    }
                }
                match changed
                    .as_mut()
                    .expect("notification was installed")
                    .as_mut()
                    .poll(cx)
                {
                    std::task::Poll::Ready(()) => {
                        changed = None;
                        if helper.is_none() {
                            helper = self.origin_progress(key, mode);
                        }
                    }
                    std::task::Poll::Pending => return std::task::Poll::Pending,
                }
            }
        })
        .await
    }
}
