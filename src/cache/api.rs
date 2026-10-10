//! Public operations and their observed execution boundaries.
use super::observed_execution::ObservedExecution;
use super::plain_ready::QuietStart;
use super::read_request::{ReadStart, TryGetFuture as ReadRequest};
use super::{
    Arc, Backplane, BackplaneReadiness, Cache, CacheBuilder, CacheOperation, CacheOrigin,
    CacheValue, CancellationSource, ClearMode, CloseOutcome, CommitReceipt, Cow, DistributedCache,
    DistributedLocker, Entry, EntryOptions, Error, Events, Execution, FactoryCancellation, Future,
    InlinePermit, Instrument, KeyMutation, LayerEvent, LookupKey, LookupMode, LookupStart,
    MarkerKind, MarkerLifecyclePolicy, MarkerReadPolicy, MemoryEvent, MutationReceipt,
    ObservationAdmission, Observed, OperationObservation, OperationOutcome, Ordering, OriginKind,
    Pin, Plugin, PublicLifetime, ReadyEager, ReadyHit, ReadyLookup, ReadyObservation, ReadyRefresh,
    ReadyValue, ReplayTicket, Result, ShutdownReport, Storage, Tag, TagVerdict, WorkAdmission,
    Worker,
};
use crate::marker_reads::MarkerReads;
use crate::observability::QuietObservation;

struct ReadOperation<'a> {
    key: LookupKey,
    options: Option<Box<EntryOptions>>,
    cancellation: Option<FactoryCancellation>,
    observation: OperationObservation,
    permit: InlinePermit<'a>,
}

impl<V: Clone + Send + Sync + 'static> Cache<V> {
    /// Starts validated construction.
    pub fn builder() -> CacheBuilder<V> {
        CacheBuilder::new()
    }
    /// Creates the always-valid memory-only defaults without a runtime.
    pub fn new() -> Self {
        match CacheBuilder::new().try_build() {
            Ok(cache) => cache,
            Err(error) => unreachable!("built-in memory defaults must remain valid: {error}"),
        }
    }
    /// Diagnostic name.
    pub fn name(&self) -> &str {
        &self.inner.name
    }
    /// This instance's diagnostic and backplane sender identity.
    pub fn instance_id(&self) -> &str {
        &self.inner.instance_id
    }
    /// The configured distributed byte store, when L2 is enabled.
    pub fn distributed_cache(&self) -> Option<&Arc<dyn DistributedCache>> {
        match &self.inner.storage {
            Storage::MemoryOnly => None,
            Storage::Hybrid { backend, .. } => Some(backend),
        }
    }
    /// The configured backplane, when attached.
    pub fn backplane(&self) -> Option<&Arc<dyn Backplane>> {
        self.inner.backplane.as_ref()
    }
    /// The configured distributed locker, when attached.
    pub fn distributed_locker(&self) -> Option<&Arc<dyn DistributedLocker>> {
        self.inner.distributed_locker.as_ref()
    }
    /// Unified event route, including eviction and plugins.
    pub fn events(&self) -> &Events {
        &self.inner.events
    }
    /// Original stored values retired from L1, with independent bounded cursors.
    pub fn memory_evictions(&self) -> &crate::advanced::MemoryEvictions<V> {
        self.inner.memory.evictions()
    }
    /// Starts a dynamic plugin session owned by its registration and this cache.
    /// Dropping or stopping the registration detaches it; shutdown waits callbacks.
    pub fn register_plugin(&self, plugin: Arc<dyn Plugin>) -> Result<crate::PluginRegistration> {
        let permit = self.inline();
        permit.admit()?;
        let registration = self.inner.plugins.register(plugin)?;
        permit.status(None)?;
        Ok(registration)
    }
    /// Starts a typed plugin with weak operational access to this cache.
    pub fn register_cache_plugin(
        &self,
        plugin: Arc<dyn super::CachePlugin<V>>,
    ) -> Result<crate::PluginRegistration> {
        self.register_plugin(super::plugin::adapter(plugin, &self.inner))
    }
    /// Static default options; per-key providers may override them.
    pub fn entry_options(&self) -> EntryOptions {
        self.inner.default_options.clone()
    }
    /// Independent tag/clear operation defaults; key providers do not affect them.
    pub fn tags_entry_options(&self) -> EntryOptions {
        self.inner.tags_default_options.clone()
    }

    /// The selected secondary control-read contract.
    pub fn marker_read_policy(&self) -> MarkerReadPolicy {
        self.inner.marker_reads.policy()
    }
    /// The independent marker snapshot lifetime/repair contract.
    pub fn marker_lifecycle_policy(&self) -> MarkerLifecyclePolicy {
        self.inner.marker_reads.lifecycle_policy()
    }
    /// Configured subscription readiness policy.
    pub fn wait_for_initial_backplane_subscribe(&self) -> bool {
        self.inner.wait_for_initial_backplane_subscribe
    }
    /// Waits native subscription admission. Close cancels a parked readiness wait.
    pub async fn ready(&self) -> Result<BackplaneReadiness> {
        if self.operation_scopes().is_closed() {
            return Err(Error::CacheClosed);
        }
        let worker = self.worker();
        self.operation_scopes()
            .execution(
                async move { worker.await_readiness().await },
                CancellationSource::new(),
            )
            .await
    }
    /// Number of exact queued/in-flight recovery operations (zero when disabled).
    pub fn pending_recovery(&self) -> usize {
        self.inner
            .recovery
            .as_ref()
            .map_or(0, |recovery| recovery.len())
    }
    /// Immutable diagnostic ticket for a data key; it pins the exact commit lane.
    pub fn recovery_ticket(&self, key: impl AsRef<str>) -> Option<ReplayTicket> {
        let key = self.worker().full_key(key.as_ref());
        self.inner
            .recovery
            .as_ref()
            .and_then(|recovery| recovery.snapshot(&key))
    }
    /// Immutable pending tag/clear durable, observation-population or notification stage.
    pub fn marker_recovery_ticket(&self, kind: &MarkerKind) -> Option<ReplayTicket> {
        self.inner
            .recovery
            .as_ref()
            .and_then(|recovery| recovery.marker_work(&self.inner.scope, kind))
    }
    /// Immutable finite observation repair, distinct from durable tag/clear work.
    pub fn marker_snapshot_recovery_ticket(&self, kind: &MarkerKind) -> Option<ReplayTicket> {
        self.inner
            .recovery
            .as_ref()
            .and_then(|recovery| recovery.marker_snapshot_work(&self.inner.scope, kind))
    }
    pub(super) fn worker(&self) -> Worker<V> {
        Worker {
            inner: Arc::clone(&self.inner),
            memory: self.inner.memory.for_operation(),
            admission: match &*self.lifetime {
                PublicLifetime::External(_) | PublicLifetime::CacheOwned { .. } => {
                    WorkAdmission::Ordinary
                }
                PublicLifetime::PluginAccess(access) => {
                    WorkAdmission::plugin(access.scopes(&self.inner.scopes))
                }
                PublicLifetime::NativeMemory(view) => view.work(),
            },
        }
    }
    pub(super) fn worker_seed(&self) -> super::WorkerSeed<V> {
        super::WorkerSeed {
            inner: Arc::clone(&self.inner),
            admission: match &*self.lifetime {
                PublicLifetime::External(_) | PublicLifetime::CacheOwned { .. } => {
                    WorkAdmission::Ordinary
                }
                PublicLifetime::PluginAccess(access) => {
                    WorkAdmission::plugin(access.scopes(&self.inner.scopes))
                }
                PublicLifetime::NativeMemory(view) => view.work(),
            },
        }
    }
    pub(super) fn lookup_key(&self, raw: &str, full: Arc<str>) -> LookupKey {
        let raw = if self.inner.key_prefix.as_deref().is_none_or(str::is_empty) {
            Arc::clone(&full)
        } else {
            Arc::from(raw)
        };
        LookupKey { raw, full }
    }
    async fn observed_using<T: Send + 'static>(
        &self,
        operation: CacheOperation,
        key: Option<Arc<str>>,
        token: Option<FactoryCancellation>,
        source: CancellationSource,
        work: Pin<Box<dyn Future<Output = Result<Observed<T>>> + Send>>,
    ) -> Result<T> {
        let observation = OperationObservation::new(
            self.inner.events.clone(),
            &self.inner.name,
            &self.inner.instance_id,
            operation,
            key.as_deref(),
        );
        self.execute_observed(observation, token, source, ObservationAdmission::New, work)
            .await
    }
    pub(super) fn execute_observed<T: Send + 'static>(
        &self,
        mut observation: OperationObservation,
        token: Option<FactoryCancellation>,
        source: CancellationSource,
        admission: ObservationAdmission<'_>,
        work: impl Future<Output = Result<Observed<T>>> + Send + 'static,
    ) -> ObservedExecution<T> {
        // The scope owns and pins work before borrowed admission ends. The
        // caller stores only its typed handle, without a second boxed driver.
        let prepared: Result<Execution<T>> = (|| {
            let span = observation.span();
            if self.operation_scopes().is_closed() {
                drop(work);
                observation.finish(OperationOutcome::from_error(&Error::CacheClosed));
                return Err(Error::CacheClosed);
            }
            // Startup is monotonic; avoid a temporary collector once it is running.
            if !self.inner.maintenance.load(Ordering::Acquire) {
                self.worker().start_maintenance();
            }
            let worker_seed = self.worker_seed();
            let completion_token = source.token();
            // Pin the observer and user work before admission transfers. The
            // default path promotes to a registered scope only after Pending.
            let scoped = self.operation_scopes();
            let operation = async move {
                // A collector is needed only when startup can retire memory.
                // Keep an admitted collector through the complete operation.
                let readiness_worker = (worker_seed.inner.wait_for_initial_backplane_subscribe
                    && !worker_seed
                        .inner
                        .subscription_admitted
                        .load(Ordering::Acquire))
                .then(|| worker_seed.worker());
                if let Some(worker) = &readiness_worker {
                    if let Err(error) = worker.await_readiness().await {
                        observation.finish(OperationOutcome::from_error(&error));
                        return Err(error);
                    }
                    worker
                        .inner
                        .subscription_admitted
                        .store(true, Ordering::Release);
                }
                let result = work.await;
                // Synchronous completion/destruction can close or explicitly
                // cancel this scope while it is polling. Attribute that reason
                // before finishing its single logical observation.
                let result = match completion_token.reason() {
                    Some(reason) => Err(Error::OperationCancelled { reason }),
                    None => result,
                };
                match result {
                    Ok(result) => {
                        if let Some(level) = result.level {
                            observation.set_level(level);
                        }
                        observation.finish(result.outcome);
                        Ok(result.value)
                    }
                    Err(error) => {
                        observation.finish(OperationOutcome::from_error(&error));
                        Err(error)
                    }
                }
            }
            .instrument(span);
            let execution = if token.is_some() {
                scoped.execution(operation, source)
            } else {
                scoped.ready_execution(operation, source)
            };
            // Admission or subscribed ownership covers the observer and all
            // caller work before the ready path's permit is released.
            match admission {
                ObservationAdmission::New => {}
                ObservationAdmission::Inline(permit) => drop(permit),
            }
            Ok(execution)
        })();
        ObservedExecution::new(prepared, token)
    }
    fn start_lookup<'a>(
        &'a self,
        raw: &'a str,
        options: Option<&EntryOptions>,
        token: Option<&FactoryCancellation>,
        operation: CacheOperation,
        mode: LookupMode,
    ) -> LookupStart<'a, V> {
        let permit = self.inline();
        // A ready hit needs the physical key only for the L1 probe; it is
        // assembled without allocation and owned only if work outlives the call.
        let parts = super::KeyParts::new(self.inner.key_prefix.as_ref(), raw);
        let joined = parts
            .prefix()
            .map(|prefix| super::PhysicalKey::joined(prefix, raw));
        let full: &str = joined.as_deref().unwrap_or(raw);
        let observation = ReadyObservation::new(
            &self.inner.events,
            &self.inner.name,
            &self.inner.instance_id,
            operation,
            Some(full),
        );
        let span = observation.span();
        let _entered = span.enter();
        let resolved = if options.is_none() && permit.status(token).is_ok() {
            self.inner
                .default_options_provider
                .as_ref()
                .map(|provider| {
                    provider
                        .options_for_with_defaults(raw, &self.inner.default_options)
                        .unwrap_or_else(|| self.inner.default_options.clone())
                })
        } else {
            None
        };
        let result = permit.admit().and_then(|()| {
            self.ready_value(full, resolved.as_ref().or(options), token, mode, &permit)
        });
        match result {
            Ok(None) => LookupStart::Owned {
                observation,
                key: joined.map_or(Cow::Borrowed(raw), |joined| Cow::Owned(joined.into_owned())),
                permit,
                resolved: resolved.map(Box::new),
            },
            Ok(Some(hit)) => LookupStart::Ready(ReadyLookup {
                result: Ok(ReadyValue {
                    value: hit.value,
                    key: parts,
                    refresh: hit.refresh,
                }),
                observation,
                permit,
            }),
            Err(error) => LookupStart::Ready(ReadyLookup {
                result: Err(error),
                observation,
                permit,
            }),
        }
    }
    fn ready_value(
        &self,
        key: &str,
        options: Option<&EntryOptions>,
        token: Option<&FactoryCancellation>,
        mode: LookupMode,
        permit: &InlinePermit<'_>,
    ) -> Result<Option<ReadyHit<V>>> {
        permit.status(token)?;
        let context = self.ready_context();
        let ready = context.inner();
        if self.inner.backplane.is_some()
            && self.inner.wait_for_initial_backplane_subscribe
            && !self.inner.subscription_admitted.load(Ordering::Acquire)
        {
            return Ok(None);
        }
        let opts = match options {
            Some(options) => {
                ready.validate_options(options)?;
                options
            }
            None => {
                ready.default_runtime.validate()?;
                &ready.default_options
            }
        };
        if opts.skip_memory_read() {
            return Ok(None);
        }
        context.ensure_health()?;
        permit.status(token)?;
        let now = self.inner.clock.now();
        permit.status(token)?;
        enum Copy<V> {
            Value(V),
            Owned(Entry<V>),
            Miss,
        }
        let can_copy = ready.ready_slot_copy(opts);
        let selected = self.inner.memory.with_ready(key, now, |entry, freshness| {
            if ready.tags(entry) != TagVerdict::Valid || !freshness.is_fresh() {
                return Copy::Miss;
            }
            if can_copy
                && !(matches!(mode, LookupMode::GetOrSet) && entry.should_eager_refresh(now))
            {
                Copy::Value(entry.value().clone())
            } else {
                // The slot protects this Arc clone, not the optional callback.
                Copy::Owned(entry.clone())
            }
        })?;
        permit.status(token)?;
        let (value, refresh) = match selected {
            Some(Copy::Value(value)) => (value, ReadyRefresh::Complete),
            Some(Copy::Owned(entry)) => {
                if !ready.marker_reads_ready(&entry, now)? {
                    return Ok(None);
                }
                let eager = matches!(mode, LookupMode::GetOrSet) && entry.should_eager_refresh(now);
                let value = ready.copy(entry.value(), opts)?;
                permit.status(token)?;
                ready.local_marker_ready_events(entry.meta().tags(), now)?;
                permit.status(token)?;
                let refresh = if eager {
                    ReadyRefresh::Eager(Box::new(ReadyEager {
                        current: entry,
                        options: opts.clone(),
                    }))
                } else {
                    ReadyRefresh::Complete
                };
                (value, refresh)
            }
            Some(Copy::Miss) | None => return Ok(None),
        };
        self.inner.events.emit_layer_lazy(|| {
            LayerEvent::Memory(MemoryEvent::Hit {
                key: Arc::from(key),
                stale: false,
            })
        });
        permit.status(token)?;
        Ok(Some(ReadyHit { value, refresh }))
    }

    /// Lazily retrieves or computes a value. Options edit a copy of defaults;
    /// string tags, a fail-safe default and cancellation are optional inputs.
    /// Use `with_receipt()` to inspect the actual origin commit.
    pub fn get_or_set<K, S>(&self, key: K, source: S) -> super::GetOrSetRequest<'_, K, S, V>
    where
        K: AsRef<str>,
        S: crate::source::Source<V>,
    {
        super::GetOrSetRequest::new(self, key, source)
    }
    pub(super) fn begin_origin_request<O: CacheOrigin<V>>(
        &self,
        key: &str,
        origin: O,
        options: Option<Box<EntryOptions>>,
        tags: std::result::Result<Box<[Tag]>, crate::TagError>,
        fallback: Option<V>,
        token: Option<FactoryCancellation>,
    ) -> super::inline_cold::Start<V> {
        match tags {
            Ok(tags) => self.begin_origin(key, origin, options, tags, fallback, token),
            Err(error) => {
                let permit = self.inline();
                let observation = ReadyObservation::new(
                    &self.inner.events,
                    &self.inner.name,
                    &self.inner.instance_id,
                    CacheOperation::GetOrSet,
                    Some(key),
                );
                let error = match permit.admit().and_then(|()| permit.status(token.as_ref())) {
                    Ok(()) => error.into(),
                    Err(error) => error,
                };
                drop((origin, options, fallback));
                let error = match permit.status(token.as_ref()) {
                    Ok(()) => error,
                    Err(error) => error,
                };
                observation.finish(OperationOutcome::from_error(&error));
                super::inline_cold::Start::Ready(Err(error))
            }
        }
    }

    fn begin_origin<O: CacheOrigin<V>>(
        &self,
        key: &str,
        origin: O,
        options: Option<Box<EntryOptions>>,
        tags: Box<[Tag]>,
        fallback: Option<V>,
        cancellation: Option<FactoryCancellation>,
    ) -> super::inline_cold::Start<V> {
        self.begin_origin_lazy(key, move || origin, options, tags, fallback, cancellation)
    }

    /// A native adapter constructs its executor captures only after an L1 miss.
    pub(in crate::cache) fn begin_origin_lazy<O: CacheOrigin<V>>(
        &self,
        key: &str,
        make_origin: impl FnOnce() -> O,
        options: Option<Box<EntryOptions>>,
        tags: Box<[Tag]>,
        fallback: Option<V>,
        cancellation: Option<FactoryCancellation>,
    ) -> super::inline_cold::Start<V> {
        let inputs = super::callback_free::Inputs::origin(&make_origin, &tags, &fallback);
        let lookup = match self.quiet_lookup(
            key,
            options.as_deref(),
            cancellation.as_ref(),
            CacheOperation::GetOrSet,
            match O::KIND {
                OriginKind::Factory => LookupMode::GetOrSet,
                OriginKind::Constant => LookupMode::ConstantValue,
            },
            inputs,
        ) {
            QuietStart::Complete(result) => {
                drop((make_origin, tags, fallback, options));
                return super::inline_cold::Start::Ready(result.map(|value| CacheValue {
                    value,
                    commit: CommitReceipt::Unchanged,
                }));
            }
            QuietStart::Ready(ready) => {
                // Input destructors stay within the same admitted operation.
                return super::inline_cold::Start::Ready(ready.finish(
                    key,
                    cancellation.as_ref(),
                    move |value| {
                        drop((make_origin, tags, fallback, options));
                        CacheValue {
                            value,
                            commit: CommitReceipt::Unchanged,
                        }
                    },
                ));
            }
            QuietStart::Owned {
                observation,
                permit,
            } => LookupStart::Owned {
                observation: observation.into_ready(),
                key: Cow::Borrowed(key),
                permit,
                resolved: None,
            },
            QuietStart::Recheck {
                observation,
                permit,
            } => self.resume_plain_origin(key, observation, permit, cancellation.as_ref()),
            QuietStart::General => self.start_lookup(
                key,
                options.as_deref(),
                cancellation.as_ref(),
                CacheOperation::GetOrSet,
                match O::KIND {
                    OriginKind::Factory => LookupMode::GetOrSet,
                    OriginKind::Constant => LookupMode::ConstantValue,
                },
            ),
        };
        self.start_origin_lookup(
            lookup,
            key,
            make_origin,
            options,
            tags,
            fallback,
            cancellation,
        )
    }

    fn resume_plain_origin<'a>(
        &'a self,
        key: &'a str,
        observation: QuietObservation<'a>,
        permit: InlinePermit<'a>,
        cancellation: Option<&FactoryCancellation>,
    ) -> LookupStart<'a, V> {
        let result = self.ready_value(key, None, cancellation, LookupMode::GetOrSet, &permit);
        let observation = observation.into_ready();
        match result {
            Ok(None) => LookupStart::Owned {
                observation,
                key: Cow::Borrowed(key),
                permit,
                resolved: None,
            },
            Ok(Some(hit)) => LookupStart::Ready(ReadyLookup {
                result: Ok(ReadyValue {
                    value: hit.value,
                    key: super::KeyParts::new(None, key),
                    refresh: hit.refresh,
                }),
                observation,
                permit,
            }),
            Err(error) => LookupStart::Ready(ReadyLookup {
                result: Err(error),
                observation,
                permit,
            }),
        }
    }

    #[allow(clippy::too_many_arguments)] // Preserve the admitted lookup and its caller inputs together.
    fn start_origin_lookup<'a, O: CacheOrigin<V>>(
        &'a self,
        lookup: LookupStart<'a, V>,
        key: &str,
        make_origin: impl FnOnce() -> O,
        options: Option<Box<EntryOptions>>,
        tags: Box<[Tag]>,
        fallback: Option<V>,
        cancellation: Option<FactoryCancellation>,
    ) -> super::inline_cold::Start<V> {
        let (observation, full, permit, resolved, origin, tags, fallback) = match lookup {
            LookupStart::Ready(ready) => {
                // These captures can execute user Drop code; keep them within
                // the same counted operation before its final cancellation check.
                let span = ready.observation.span();
                let _entered = span.enter();
                let ready = self.ready_origin(ready, key, make_origin, tags);
                drop((fallback, options));
                return super::inline_cold::Start::Ready(
                    ready
                        .finish(
                            cancellation.as_ref(),
                            &self.inner.events,
                            std::convert::identity,
                        )
                        .map(|value| CacheValue {
                            value,
                            commit: CommitReceipt::Unchanged,
                        }),
                );
            }
            LookupStart::Owned {
                observation,
                key,
                permit,
                resolved,
            } => {
                // The native caller has entered its executor before this miss.
                // Start incremental cleanup once, without a Worker on a hit.
                drop(self.ready_context());
                let origin = make_origin();
                if self.supports_inline_cold(resolved.as_deref().or(options.as_deref())) {
                    let full: Arc<str> = Arc::from(key.as_ref());
                    let raw = if self.inner.key_prefix.as_deref().is_none_or(str::is_empty) {
                        Arc::clone(&full)
                    } else {
                        Arc::from(
                            key.strip_prefix(self.inner.key_prefix.as_deref().unwrap_or(""))
                                .unwrap_or(&key),
                        )
                    };
                    let start = self.prepare_inline_cold(
                        super::inline_cold::Input {
                            keys: LookupKey { raw, full },
                            origin,
                            options: resolved
                                .or(options)
                                .map(|options| *options)
                                .unwrap_or_else(|| self.inner.default_options.clone()),
                            tags,
                            caller: cancellation,
                            unused: fallback,
                        },
                        observation,
                        permit,
                    );
                    return start;
                }
                let raw = key
                    .strip_prefix(self.inner.key_prefix.as_deref().unwrap_or(""))
                    .unwrap_or(&key);
                let deferred = match self.immediate_origin(
                    raw,
                    &key,
                    resolved.as_deref().or(options.as_deref()),
                    cancellation.as_ref(),
                    observation,
                    permit,
                    (origin, tags, fallback),
                ) {
                    super::immediate_read::InlineOrigin::Completed(result) => {
                        drop((resolved, options));
                        return super::inline_cold::Start::Ready(result);
                    }
                    super::immediate_read::InlineOrigin::Continued(work) => {
                        drop((resolved, options));
                        return super::inline_cold::Start::Pending(work);
                    }
                    super::immediate_read::InlineOrigin::Deferred(deferred) => deferred,
                };
                (
                    deferred.observation.into_owned(),
                    Arc::<str>::from(key.as_ref()),
                    deferred.permit,
                    resolved,
                    deferred.origin,
                    deferred.tags,
                    deferred.fallback,
                )
            }
        };
        let worker = self.worker();
        let key = self.lookup_key(key, full);
        let source = CancellationSource::for_cache(self.operation_scopes());
        let caller = source.token();
        let explicit = cancellation.clone();
        let options = resolved.or(options);
        super::inline_cold::Start::Pending(self.execute_observed(
            observation,
            cancellation,
            source,
            ObservationAdmission::Inline(permit),
            async move {
                worker
                    .get_or_set(
                        key,
                        origin,
                        options,
                        tags,
                        fallback,
                        super::origin::OriginCaller {
                            operation: caller,
                            explicit,
                        },
                    )
                    .await
            },
        ))
    }
    fn ready_origin<'a, O: CacheOrigin<V>>(
        &self,
        ready: ReadyLookup<'a, V>,
        raw: &str,
        make_origin: impl FnOnce() -> O,
        tags: Box<[Tag]>,
    ) -> ReadyLookup<'a, V> {
        let ReadyLookup {
            result,
            observation,
            permit,
        } = ready;
        let result = match result {
            Ok(mut hit) => {
                match std::mem::replace(&mut hit.refresh, ReadyRefresh::Complete) {
                    ReadyRefresh::Complete => drop((make_origin, tags)),
                    ReadyRefresh::Eager(work) => {
                        let ReadyEager { current, options } = *work;
                        self.worker().eager(
                            LookupKey {
                                raw: Arc::from(raw),
                                full: hit.key.to_shared(),
                            },
                            options,
                            current,
                            tags,
                            make_origin(),
                        );
                    }
                }
                Ok(hit)
            }
            Err(error) => {
                drop((make_origin, tags));
                Err(error)
            }
        };
        ReadyLookup {
            result,
            observation,
            permit,
        }
    }

    #[allow(clippy::too_many_arguments)] // Native and async adapters share the same admitted origin inputs.
    pub(in crate::cache) fn native_origin_lazy<
        O: CacheOrigin<V>,
        T: super::get_request::Output<V>,
    >(
        &self,
        key: &str,
        make_origin: impl FnOnce() -> O,
        options: Option<Box<EntryOptions>>,
        tags: Box<[Tag]>,
        fallback: Option<V>,
        cancellation: Option<FactoryCancellation>,
        runtime: &super::BlockingRuntime,
    ) -> Result<T> {
        let inputs = super::callback_free::Inputs::origin(&make_origin, &tags, &fallback);
        match self.quiet_lookup(
            key,
            options.as_deref(),
            cancellation.as_ref(),
            CacheOperation::GetOrSet,
            match O::KIND {
                OriginKind::Factory => LookupMode::GetOrSet,
                OriginKind::Constant => LookupMode::ConstantValue,
            },
            inputs,
        ) {
            QuietStart::Complete(result) => {
                drop((make_origin, tags, fallback, options));
                result.map(T::unchanged)
            }
            QuietStart::Ready(ready) => ready.finish(key, cancellation.as_ref(), move |value| {
                drop((make_origin, options, tags, fallback));
                T::unchanged(value)
            }),
            QuietStart::Owned {
                observation,
                permit,
            } => runtime
                .run(async {
                    match self.start_origin_lookup(
                        LookupStart::Owned {
                            observation: observation.into_ready(),
                            key: Cow::Borrowed(key),
                            permit,
                            resolved: None,
                        },
                        key,
                        make_origin,
                        options,
                        tags,
                        fallback,
                        cancellation,
                    ) {
                        super::inline_cold::Start::Ready(result) => result,
                        super::inline_cold::Start::Pending(work) => work.await,
                    }
                })
                .map(T::complete),
            QuietStart::Recheck {
                observation,
                permit,
            } => runtime
                .run(async {
                    let lookup =
                        self.resume_plain_origin(key, observation, permit, cancellation.as_ref());
                    match self.start_origin_lookup(
                        lookup,
                        key,
                        make_origin,
                        options,
                        tags,
                        fallback,
                        cancellation,
                    ) {
                        super::inline_cold::Start::Ready(result) => result,
                        super::inline_cold::Start::Pending(work) => work.await,
                    }
                })
                .map(T::complete),
            QuietStart::General => runtime
                .run(async {
                    match self.begin_origin_lazy(
                        key,
                        make_origin,
                        options,
                        tags,
                        fallback,
                        cancellation,
                    ) {
                        super::inline_cold::Start::Ready(result) => result,
                        super::inline_cold::Start::Pending(work) => work.await,
                    }
                })
                .map(T::complete),
        }
    }

    /// Lazily reads L1/L2 without invoking a factory or writing L2/backplane.
    /// A miss is Ok(None); failures retain the configured typed error channel.
    pub fn try_get<K: AsRef<str>>(&self, key: K) -> super::TryGetRequest<'_, K, V> {
        super::TryGetRequest::new(self, key)
    }
    /// Lazily reads L1/L2, using the supplied value only on a successful miss.
    pub fn get_or_default<K: AsRef<str>>(
        &self,
        key: K,
        default: V,
    ) -> super::GetOrDefaultRequest<'_, K, V> {
        super::GetOrDefaultRequest::new(self, key, default)
    }
    // A ready memory lookup borrows exactly the async admission/observation
    // path, but needs neither runtime entry nor block_in_place. True misses
    // transfer that same operation once to the driven native executor.
    pub(in crate::cache) fn native_read(
        &self,
        key: &str,
        options: Option<EntryOptions>,
        token: Option<FactoryCancellation>,
        runtime: &super::BlockingRuntime,
    ) -> Result<Option<V>> {
        match self.quiet_lookup(
            key,
            options.as_ref(),
            token.as_ref(),
            CacheOperation::TryGet,
            LookupMode::Read,
            super::callback_free::Inputs::NoCallbacks,
        ) {
            QuietStart::Complete(result) => return result.map(Some),
            QuietStart::Ready(ready) => return ready.finish(key, token.as_ref(), Some),
            QuietStart::Owned {
                observation,
                permit,
            } => {
                return runtime.run(self.prepare_read(
                    ReadOperation {
                        key: self.lookup_key(key, Arc::from(key)),
                        options: None,
                        cancellation: token,
                        observation: observation.into_owned(),
                        permit,
                    },
                    std::convert::identity,
                ));
            }
            QuietStart::Recheck { .. } => {
                unreachable!("read-only lookup never selects origin refresh")
            }
            QuietStart::General => {}
        }
        self.native_read_general(key, options, token, runtime)
    }
    #[cold]
    #[inline(never)]
    fn native_read_general(
        &self,
        key: &str,
        options: Option<EntryOptions>,
        token: Option<FactoryCancellation>,
        runtime: &super::BlockingRuntime,
    ) -> Result<Option<V>> {
        if options.is_some() || token.is_some() || !self.inner.native_inline_read() {
            return runtime.run(ReadRequest::new(self, key, options, token));
        }
        match self.start_lookup(key, None, None, CacheOperation::TryGet, LookupMode::Read) {
            LookupStart::Ready(ready) => ready.finish(None, &self.inner.events, Some),
            LookupStart::Owned {
                observation,
                key: full,
                permit,
                resolved,
            } => runtime.run(self.prepare_read(
                ReadOperation {
                    key: self.lookup_key(key, Arc::from(full.as_ref())),
                    options: resolved,
                    cancellation: token,
                    observation: observation.into_owned(),
                    permit,
                },
                std::convert::identity,
            )),
        }
    }
    pub(super) fn begin_read(
        &self,
        key: &str,
        options: Option<Box<EntryOptions>>,
        token: Option<FactoryCancellation>,
    ) -> ReadStart<Option<V>> {
        match self.quiet_lookup(
            key,
            options.as_deref(),
            token.as_ref(),
            CacheOperation::TryGet,
            LookupMode::Read,
            super::callback_free::Inputs::NoCallbacks,
        ) {
            QuietStart::Complete(result) => ReadStart::Ready(result.map(Some)),
            QuietStart::Ready(ready) => ReadStart::Ready(ready.finish(key, token.as_ref(), Some)),
            QuietStart::Owned {
                observation,
                permit,
            } => self.pending_read(ReadOperation {
                key: self.lookup_key(key, Arc::from(key)),
                options,
                cancellation: token,
                observation: observation.into_owned(),
                permit,
            }),
            QuietStart::Recheck { .. } => {
                unreachable!("read-only lookup never selects origin refresh")
            }
            QuietStart::General => self.begin_general_read(key, options, token),
        }
    }
    #[cold]
    #[inline(never)]
    fn begin_general_read(
        &self,
        key: &str,
        options: Option<Box<EntryOptions>>,
        token: Option<FactoryCancellation>,
    ) -> ReadStart<Option<V>> {
        match self.start_lookup(
            key,
            options.as_deref(),
            token.as_ref(),
            CacheOperation::TryGet,
            LookupMode::Read,
        ) {
            LookupStart::Ready(ready) => {
                ReadStart::Ready(ready.finish(token.as_ref(), &self.inner.events, Some))
            }
            LookupStart::Owned {
                observation,
                key: full,
                permit,
                resolved,
            } => {
                let options = resolved.or(options);
                let observation = match self.immediate_read(
                    key,
                    &full,
                    options.as_deref(),
                    token.as_ref(),
                    &permit,
                    observation,
                ) {
                    super::immediate_read::InlineRead::Completed(result) => {
                        return ReadStart::Ready(result);
                    }
                    super::immediate_read::InlineRead::Deferred(observation) => observation,
                };
                self.pending_read(ReadOperation {
                    key: self.lookup_key(key, Arc::from(full.as_ref())),
                    options,
                    cancellation: token,
                    observation: observation.into_owned(),
                    permit,
                })
            }
        }
    }
    #[cold]
    #[inline(never)]
    fn pending_read(&self, operation: ReadOperation<'_>) -> ReadStart<Option<V>> {
        ReadStart::Pending(self.prepare_read(operation, std::convert::identity))
    }
    pub(super) fn begin_default_read(
        &self,
        key: &str,
        options: Option<EntryOptions>,
        token: Option<FactoryCancellation>,
        default: V,
    ) -> ReadStart<V> {
        let operation = CacheOperation::GetOrDefault;
        let complete = move |value: Option<V>| value.unwrap_or(default);
        // Both probes finish or transfer their thread-bound guard synchronously.
        // No borrowed admission may remain in a parked, transferable future.
        let (observation, full, permit, resolved) = match self.quiet_lookup(
            key,
            options.as_ref(),
            token.as_ref(),
            operation,
            LookupMode::Read,
            super::callback_free::Inputs::Destructors,
        ) {
            QuietStart::Complete(_) => {
                unreachable!("completion callbacks require counted admission")
            }
            QuietStart::Ready(ready) => {
                return ReadStart::Ready(
                    ready.finish(key, token.as_ref(), move |value| complete(Some(value))),
                );
            }
            QuietStart::Owned {
                observation,
                permit,
            } => (
                observation.into_owned(),
                Arc::<str>::from(key),
                permit,
                None,
            ),
            QuietStart::Recheck { .. } => {
                unreachable!("read-only lookup never selects origin refresh")
            }
            QuietStart::General => match self.start_lookup(
                key,
                options.as_ref(),
                token.as_ref(),
                operation,
                LookupMode::Read,
            ) {
                LookupStart::Ready(ready) => {
                    return ReadStart::Ready(ready.finish(
                        token.as_ref(),
                        &self.inner.events,
                        move |value| complete(Some(value)),
                    ));
                }
                LookupStart::Owned {
                    observation,
                    key,
                    permit,
                    resolved,
                } => (
                    observation.into_owned(),
                    Arc::<str>::from(key.as_ref()),
                    permit,
                    resolved,
                ),
            },
        };
        ReadStart::Pending(self.prepare_read(
            ReadOperation {
                key: self.lookup_key(key, full),
                options: resolved.or_else(|| options.map(Box::new)),
                cancellation: token,
                observation,
                permit,
            },
            complete,
        ))
    }
    // Keep the synchronous ready path independent of the owned L2 preparation
    // body. The existing scope still owns cancellation, callbacks and drainage.
    #[cold]
    #[inline(never)]
    fn prepare_read<T: Send + 'static>(
        &self,
        operation: ReadOperation<'_>,
        complete: impl FnOnce(Option<V>) -> T + Send + 'static,
    ) -> ObservedExecution<T> {
        let ReadOperation {
            key,
            options,
            cancellation: token,
            observation,
            permit,
        } = operation;
        let worker = self.worker();
        let source = CancellationSource::for_cache(self.operation_scopes());
        let cancellation = source.token();
        self.execute_observed(
            observation,
            token,
            source,
            ObservationAdmission::Inline(permit),
            async move {
                let result = worker.read(key, options, &cancellation).await?;
                Ok(Observed::new(
                    complete(result.value),
                    result.outcome,
                    result.level,
                ))
            },
        )
    }
    pub(super) fn set_impl<T: super::mutation_request::MutationOutput>(
        &self,
        key: &str,
        value: V,
        options: Option<Box<EntryOptions>>,
        tags: std::result::Result<Box<[Tag]>, crate::TagError>,
        token: Option<FactoryCancellation>,
    ) -> super::memory_inline::MutationStart<'_, T> {
        if self.inner.write_plan.is_inline() {
            return super::memory_inline::MutationStart::Ready(
                self.inline_set(key, value, options, tags, token.as_ref())
                    .map(T::local),
            );
        }
        let worker = self.worker();
        let raw: Arc<str> = Arc::from(key);
        let full = worker.full_key(key);
        let source = CancellationSource::for_cache(self.operation_scopes());
        let cancellation = source.token();
        super::memory_inline::MutationStart::Pending(Box::pin(self.observed_using(
            CacheOperation::Set,
            Some(full),
            token,
            source,
            Box::pin(async move {
                worker
                    .set(
                        raw,
                        value,
                        options.map(|options| *options),
                        tags?,
                        &cancellation,
                    )
                    .await
                    .map(|observed| {
                        Observed::new(
                            T::pipeline(observed.value),
                            observed.outcome,
                            observed.level,
                        )
                    })
            }),
        )))
    }
    /// Stores a value lazily; options overlay this cache's defaults.
    /// Use `with_receipt()` to inspect the actual distributed commit stages.
    pub fn set<K: AsRef<str>>(&self, key: K, value: V) -> super::SetRequest<'_, K, V> {
        super::SetRequest::new(self, key, value)
    }
    pub(super) fn begin_key_mutation(
        &self,
        key: &str,
        options: Option<EntryOptions>,
        mutation: KeyMutation,
        token: Option<FactoryCancellation>,
    ) -> ObservedExecution<MutationReceipt> {
        let worker = self.worker();
        let raw: Arc<str> = Arc::from(key);
        let full = worker.full_key(key);
        let operation = match mutation {
            KeyMutation::Remove => CacheOperation::Remove,
            KeyMutation::Expire(_) => CacheOperation::Expire,
        };
        let source = CancellationSource::for_cache(self.operation_scopes());
        let cancellation = source.token();
        let observation = OperationObservation::new(
            self.inner.events.clone(),
            &self.inner.name,
            &self.inner.instance_id,
            operation,
            Some(&full),
        );
        self.execute_observed(
            observation,
            token,
            source,
            ObservationAdmission::New,
            async move {
                worker
                    .key_mutation(raw, options, mutation, &cancellation)
                    .await
            },
        )
    }
    /// Physically removes a key; awaiting preserves any typed failure.
    pub fn remove<K: AsRef<str>>(&self, key: K) -> super::RemoveRequest<'_, K, V> {
        super::RemoveRequest::new(self, key)
    }
    /// Expires L1 while removing L2 by default.
    pub fn expire<K: AsRef<str>>(&self, key: K) -> super::ExpireRequest<'_, K, V> {
        super::ExpireRequest::new(self, key)
    }
    pub(super) fn begin_markers(
        &self,
        kinds: Vec<MarkerKind>,
        options: Option<EntryOptions>,
        operation: CacheOperation,
        token: Option<FactoryCancellation>,
    ) -> ObservedExecution<MutationReceipt> {
        let worker = self.worker();
        let source = CancellationSource::for_cache(self.operation_scopes());
        let cancellation = source.token();
        let observation = OperationObservation::new(
            self.inner.events.clone(),
            &self.inner.name,
            &self.inner.instance_id,
            operation,
            None,
        );
        self.execute_observed(
            observation,
            token,
            source,
            ObservationAdmission::New,
            async move { worker.mutate_markers(kinds, options, cancellation).await },
        )
    }
    /// Invalidates a raw string tag; `and_tags` adds a batch atomically.
    pub fn remove_by_tag(&self, tag: impl AsRef<str>) -> super::TagInvalidationRequest<'_, V> {
        super::TagInvalidationRequest::new(self, tag)
    }
    /// Invalidates this cache using an explicit expiration or removal mode.
    pub fn clear(&self, mode: ClearMode) -> super::ClearRequest<'_, V> {
        super::ClearRequest::new(self, mode)
    }

    /// Initiates close, cancellation and plugin teardown once.
    pub fn close(&self) -> CloseOutcome {
        self.inner.close()
    }
    /// Waits execution scopes, supervised effects, cleanup, recovery and plugins.
    pub async fn shutdown(&self) -> Result<ShutdownReport> {
        self.check_plugin_drain(crate::advanced::DrainOperation::Shutdown)?;
        super::blocking::check_drain(
            &self.inner.scopes,
            crate::advanced::DrainOperation::Shutdown,
        )?;
        self.inner.shutdown().await
    }
    /// Runs maintenance, preserving an external L1 provider's typed failure.
    pub async fn run_pending_tasks(&self) -> Result<()> {
        self.inner.memory.run_pending_tasks().await?;
        if let MarkerReads::OptionsControlled(observations) = &self.inner.marker_reads {
            observations.memory.run_pending_tasks().await?;
            observations.locks.clean_idle(64);
        }
        self.inner.locks.clean_idle(256);
        self.inner.lanes.clean(256);
        Ok(())
    }
    /// The supplied L1 provider, when one was explicitly configured.
    pub fn memory_storage(&self) -> Option<&Arc<dyn crate::provider::MemoryStorage<V>>> {
        self.inner.memory.provider()
    }
    /// The actual supplied secondary observation L1, when configured.
    pub fn marker_memory_storage(
        &self,
    ) -> Option<&Arc<dyn crate::provider::MemoryStorage<crate::advanced::MarkerObservation>>> {
        match &self.inner.marker_reads {
            MarkerReads::DurableRequired => None,
            MarkerReads::OptionsControlled(observations) => observations.memory.provider(),
        }
    }
    /// Actual observation count/weight; absent when independent reads are disabled.
    pub fn marker_memory_usage(&self) -> Result<Option<crate::provider::MemoryUsage>> {
        match &self.inner.marker_reads {
            MarkerReads::DurableRequired => Ok(None),
            MarkerReads::OptionsControlled(observations) => observations.memory.usage().map(Some),
        }
    }
    /// Retained count/weight in the actual L1 keyspace, including shared users.
    pub fn memory_usage(&self) -> Result<crate::provider::MemoryUsage> {
        Ok(self.inner.memory.usage()?)
    }
    /// Waits currently scheduled effects and their cleanup without closing the cache.
    pub async fn flush_pending(&self) -> Result<()> {
        self.check_plugin_drain(crate::advanced::DrainOperation::FlushPending)?;
        super::blocking::check_drain(
            &self.inner.scopes,
            crate::advanced::DrainOperation::FlushPending,
        )?;
        self.inner.tasks.flush().await;
        Ok(())
    }
}
impl<V: Clone + Send + Sync + 'static> Default for Cache<V> {
    fn default() -> Self {
        Self::new()
    }
}
