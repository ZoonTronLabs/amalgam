//! Public operations and their observed execution boundaries.
use super::observed_execution::ObservedExecution;
use super::plain_ready::QuietStart;
use super::read_request::{ReadRequest, ReadStart};
use super::{
    Arc, Backplane, BackplaneReadiness, Cache, CacheBuilder, CacheOperation, CacheOrigin,
    CacheValue, CancellationSource, ClearMode, CloseOutcome, CommitReceipt, ConstantOrigin, Cow,
    DistributedCache, DistributedExpirePolicy, DistributedLocker, Entry, EntryOptions, Error,
    Events, Execution, FactoryCancellation, FactoryContext, FactoryError, FactoryOrigin,
    FactoryProduct, Future, InlinePermit, Instrument, KeyMutation, L2ReadPolicy, LayerEvent,
    LookupKey, LookupMode, LookupStart, MarkerKind, MarkerLifecyclePolicy, MarkerReadPolicy,
    MaybeValue, MemoryEvent, MutationReceipt, ObservationAdmission, Observed, OperationObservation,
    OperationOutcome, Ordering, OriginKind, Pin, Plugin, PublicLifetime, ReadyEager, ReadyHit,
    ReadyLookup, ReadyObservation, ReadyRefresh, ReadyValue, ReplayTicket, Result, ShutdownReport,
    Storage, Tag, TagVerdict, WorkAdmission, Worker,
};
use crate::marker_reads::MarkerReads;
use crate::observability::QuietObservation;

struct ReadOperation<'a> {
    key: LookupKey,
    options: Option<EntryOptions>,
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
        CacheBuilder::new().build()
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
    pub fn memory_evictions(&self) -> &crate::MemoryEvictions<V> {
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
    fn lookup_key(&self, raw: &str, full: Arc<str>) -> LookupKey {
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
            // The scope owns the observer too: a parked caller can be cancelled and
            // finish its logical observation without polling its future again.
            let execution = self.operation_scopes().execution(
                async move {
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
                .instrument(span),
                source,
            );
            // Registration owns the observer and all caller work before the ready
            // path's permit is released, closing the transfer gap against shutdown.
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
        let key = match &self.inner.key_prefix {
            Some(prefix) => Cow::Owned(format!("{prefix}{raw}")),
            None => Cow::Borrowed(raw),
        };
        let observation = ReadyObservation::new(
            &self.inner.events,
            &self.inner.name,
            &self.inner.instance_id,
            operation,
            Some(&key),
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
            self.ready_value(&key, resolved.as_ref().or(options), token, mode, &permit)
        });
        match result {
            Ok(None) => LookupStart::Owned {
                observation,
                key,
                permit,
                resolved: resolved.map(Box::new),
            },
            Ok(Some(hit)) => LookupStart::Ready(ReadyLookup {
                result: Ok(ReadyValue {
                    value: hit.value,
                    key,
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
        let selected = self.inner.memory.with_ready(key, now, |entry| {
            if ready.tags(entry) != TagVerdict::Valid || !entry.freshness(now).is_fresh() {
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
    pub fn get_or_set<K, F, Fut>(&self, key: K, factory: F) -> super::GetOrSetRequest<'_, K, F, V>
    where
        K: AsRef<str>,
        F: FnOnce(FactoryContext<V>) -> Fut + Send + 'static,
        Fut: Future<Output = std::result::Result<FactoryProduct<V>, FactoryError>> + Send + 'static,
    {
        super::GetOrSetRequest::new(self, key, factory)
    }
    /// Retrieval with explicit options.
    pub async fn get_or_set_with<F, Fut>(
        &self,
        key: impl AsRef<str>,
        factory: F,
        options: EntryOptions,
    ) -> Result<V>
    where
        F: FnOnce(FactoryContext<V>) -> Fut + Send + 'static,
        Fut: Future<Output = std::result::Result<FactoryProduct<V>, FactoryError>> + Send + 'static,
    {
        self.get_or_set_full(
            key,
            factory,
            Some(options),
            Box::from([]),
            MaybeValue::none(),
        )
        .await
    }
    /// Retrieves an existing value or stores the supplied value. User-factory
    /// timeouts, success events and eager refresh do not apply to this origin.
    pub async fn get_or_set_value(
        &self,
        key: impl AsRef<str>,
        value: V,
        options: Option<EntryOptions>,
    ) -> Result<V> {
        self.get_or_set_value_full(key, value, options, Box::from([]))
            .await
    }
    /// Supplied-value retrieval with explicit tags and per-entry options.
    pub async fn get_or_set_value_full(
        &self,
        key: impl AsRef<str>,
        value: V,
        options: Option<EntryOptions>,
        tags: Box<[Tag]>,
    ) -> Result<V> {
        Ok(self
            .get_or_set_origin_impl(
                key.as_ref(),
                ConstantOrigin::new(value),
                options,
                tags,
                MaybeValue::none(),
                None,
            )
            .await?
            .value)
    }
    /// Supplied-value retrieval with tags and explicit caller cancellation.
    pub async fn get_or_set_value_full_cancellable(
        &self,
        key: impl AsRef<str>,
        value: V,
        options: Option<EntryOptions>,
        tags: Box<[Tag]>,
        cancellation: FactoryCancellation,
    ) -> Result<V> {
        Ok(self
            .get_or_set_origin_impl(
                key.as_ref(),
                ConstantOrigin::new(value),
                options,
                tags,
                MaybeValue::none(),
                Some(cancellation),
            )
            .await?
            .value)
    }
    /// Supplied-value retrieval with observation of the actual mutation.
    pub async fn get_or_set_value_full_with_commit(
        &self,
        key: impl AsRef<str>,
        value: V,
        options: Option<EntryOptions>,
        tags: Box<[Tag]>,
    ) -> Result<CacheValue<V>> {
        self.get_or_set_origin_impl(
            key.as_ref(),
            ConstantOrigin::new(value),
            options,
            tags,
            MaybeValue::none(),
            None,
        )
        .await
    }
    /// Cancellable supplied-value retrieval with its actual mutation receipt.
    pub async fn get_or_set_value_full_with_commit_cancellable(
        &self,
        key: impl AsRef<str>,
        value: V,
        options: Option<EntryOptions>,
        tags: Box<[Tag]>,
        cancellation: FactoryCancellation,
    ) -> Result<CacheValue<V>> {
        self.get_or_set_origin_impl(
            key.as_ref(),
            ConstantOrigin::new(value),
            options,
            tags,
            MaybeValue::none(),
            Some(cancellation),
        )
        .await
    }
    /// Full retrieval. Ordinary timeout may activate fail-safe; explicit
    /// cancellation/shutdown/lease loss never masquerades as an origin failure.
    pub async fn get_or_set_full<F, Fut>(
        &self,
        key: impl AsRef<str>,
        factory: F,
        options: Option<EntryOptions>,
        tags: Box<[Tag]>,
        fail_safe_default: MaybeValue<V>,
    ) -> Result<V>
    where
        F: FnOnce(FactoryContext<V>) -> Fut + Send + 'static,
        Fut: Future<Output = std::result::Result<FactoryProduct<V>, FactoryError>> + Send + 'static,
    {
        Ok(self
            .get_or_set_impl(
                key.as_ref(),
                factory,
                options,
                tags,
                fail_safe_default,
                None,
            )
            .await?
            .value)
    }
    /// Full retrieval plus an actual factory-commit receipt.
    pub async fn get_or_set_full_with_commit<F, Fut>(
        &self,
        key: impl AsRef<str>,
        factory: F,
        options: Option<EntryOptions>,
        tags: Box<[Tag]>,
        fail_safe_default: MaybeValue<V>,
    ) -> Result<CacheValue<V>>
    where
        F: FnOnce(FactoryContext<V>) -> Fut + Send + 'static,
        Fut: Future<Output = std::result::Result<FactoryProduct<V>, FactoryError>> + Send + 'static,
    {
        self.get_or_set_impl(
            key.as_ref(),
            factory,
            options,
            tags,
            fail_safe_default,
            None,
        )
        .await
    }
    /// Full cancellable factory retrieval with its actual mutation receipt.
    pub async fn get_or_set_full_with_commit_cancellable<F, Fut>(
        &self,
        key: impl AsRef<str>,
        factory: F,
        options: Option<EntryOptions>,
        tags: Box<[Tag]>,
        fail_safe_default: MaybeValue<V>,
        cancellation: FactoryCancellation,
    ) -> Result<CacheValue<V>>
    where
        F: FnOnce(FactoryContext<V>) -> Fut + Send + 'static,
        Fut: Future<Output = std::result::Result<FactoryProduct<V>, FactoryError>> + Send + 'static,
    {
        self.get_or_set_impl(
            key.as_ref(),
            factory,
            options,
            tags,
            fail_safe_default,
            Some(cancellation),
        )
        .await
    }
    /// Retrieval with an explicit caller cancellation request.
    pub async fn get_or_set_cancellable<F, Fut>(
        &self,
        key: impl AsRef<str>,
        factory: F,
        cancellation: FactoryCancellation,
    ) -> Result<V>
    where
        F: FnOnce(FactoryContext<V>) -> Fut + Send + 'static,
        Fut: Future<Output = std::result::Result<FactoryProduct<V>, FactoryError>> + Send + 'static,
    {
        self.get_or_set_full_cancellable(
            key,
            factory,
            None,
            Box::from([]),
            MaybeValue::none(),
            cancellation,
        )
        .await
    }
    /// Full retrieval with explicit cancellation until deliberate background handoff.
    pub async fn get_or_set_full_cancellable<F, Fut>(
        &self,
        key: impl AsRef<str>,
        factory: F,
        options: Option<EntryOptions>,
        tags: Box<[Tag]>,
        fail_safe_default: MaybeValue<V>,
        cancellation: FactoryCancellation,
    ) -> Result<V>
    where
        F: FnOnce(FactoryContext<V>) -> Fut + Send + 'static,
        Fut: Future<Output = std::result::Result<FactoryProduct<V>, FactoryError>> + Send + 'static,
    {
        Ok(self
            .get_or_set_impl(
                key.as_ref(),
                factory,
                options,
                tags,
                fail_safe_default,
                Some(cancellation),
            )
            .await?
            .value)
    }
    pub(in crate::cache) async fn get_or_set_impl<F, Fut>(
        &self,
        key: &str,
        factory: F,
        options: Option<EntryOptions>,
        tags: Box<[Tag]>,
        fallback: MaybeValue<V>,
        cancellation: Option<FactoryCancellation>,
    ) -> Result<CacheValue<V>>
    where
        F: FnOnce(FactoryContext<V>) -> Fut + Send + 'static,
        Fut: Future<Output = std::result::Result<FactoryProduct<V>, FactoryError>> + Send + 'static,
    {
        self.get_or_set_origin_impl(
            key,
            FactoryOrigin::new(factory),
            options,
            tags,
            fallback,
            cancellation,
        )
        .await
    }
    async fn get_or_set_origin_impl<O: CacheOrigin<V>>(
        &self,
        key: &str,
        origin: O,
        options: Option<EntryOptions>,
        tags: Box<[Tag]>,
        fallback: MaybeValue<V>,
        cancellation: Option<FactoryCancellation>,
    ) -> Result<CacheValue<V>> {
        match self.begin_origin(key, origin, options, tags, fallback, cancellation) {
            super::inline_cold::Start::Ready(result) => result,
            super::inline_cold::Start::Pending(work) => work.await,
        }
    }
    pub(super) fn begin_origin_request<O: CacheOrigin<V>>(
        &self,
        key: &str,
        origin: O,
        options: Option<Box<EntryOptions>>,
        tags: std::result::Result<Box<[Tag]>, crate::TagError>,
        fallback: MaybeValue<V>,
        token: Option<FactoryCancellation>,
    ) -> super::inline_cold::Start<V> {
        match tags {
            Ok(tags) => self.begin_origin(
                key,
                origin,
                options.map(|options| *options),
                tags,
                fallback,
                token,
            ),
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
        options: Option<EntryOptions>,
        tags: Box<[Tag]>,
        fallback: MaybeValue<V>,
        cancellation: Option<FactoryCancellation>,
    ) -> super::inline_cold::Start<V> {
        self.begin_origin_lazy(key, move || origin, options, tags, fallback, cancellation)
    }

    /// A native adapter constructs its executor captures only after an L1 miss.
    pub(in crate::cache) fn begin_origin_lazy<O: CacheOrigin<V>>(
        &self,
        key: &str,
        make_origin: impl FnOnce() -> O,
        options: Option<EntryOptions>,
        tags: Box<[Tag]>,
        fallback: MaybeValue<V>,
        cancellation: Option<FactoryCancellation>,
    ) -> super::inline_cold::Start<V> {
        let inputs = super::callback_free::Inputs::origin(&make_origin, &tags, &fallback);
        let lookup = match self.quiet_lookup(
            key,
            options.as_ref(),
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
                options.as_ref(),
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
                    key: Cow::Borrowed(key),
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
        options: Option<EntryOptions>,
        tags: Box<[Tag]>,
        fallback: MaybeValue<V>,
        cancellation: Option<FactoryCancellation>,
    ) -> super::inline_cold::Start<V> {
        let (observation, full, permit, resolved, origin) = match lookup {
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
                if self.supports_inline_cold(resolved.as_deref().or(options.as_ref())) {
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
                                .map(|options| *options)
                                .or(options)
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
                (
                    observation.into_owned(),
                    Arc::<str>::from(key.as_ref()),
                    permit,
                    resolved,
                    origin,
                )
            }
        };
        let worker = self.worker();
        let key = self.lookup_key(key, full);
        let source = CancellationSource::new();
        let caller = source.token();
        let explicit = cancellation.clone();
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
                        resolved.map(|options| *options).or(options),
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
                                full: Arc::from(hit.key.as_ref()),
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
    pub(in crate::cache) fn native_origin_lazy<O: CacheOrigin<V>>(
        &self,
        key: &str,
        make_origin: impl FnOnce() -> O,
        options: Option<EntryOptions>,
        tags: Box<[Tag]>,
        fallback: MaybeValue<V>,
        cancellation: Option<FactoryCancellation>,
        runtime: &super::BlockingRuntime,
    ) -> Result<CacheValue<V>> {
        let inputs = super::callback_free::Inputs::origin(&make_origin, &tags, &fallback);
        match self.quiet_lookup(
            key,
            options.as_ref(),
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
                result.map(|value| CacheValue {
                    value,
                    commit: CommitReceipt::Unchanged,
                })
            }
            QuietStart::Ready(ready) => ready.finish(key, cancellation.as_ref(), move |value| {
                drop((make_origin, options, tags, fallback));
                CacheValue {
                    value,
                    commit: CommitReceipt::Unchanged,
                }
            }),
            QuietStart::Owned {
                observation,
                permit,
            } => runtime.run(async {
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
            }),
            QuietStart::Recheck {
                observation,
                permit,
            } => runtime.run(async {
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
            }),
            QuietStart::General => runtime.run(async {
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
            }),
        }
    }

    /// Canonical read-only L1/L2 lookup. Expected failures retain their typed channel.
    pub fn read<K: AsRef<str>>(
        &self,
        key: K,
        options: Option<EntryOptions>,
    ) -> impl Future<Output = Result<MaybeValue<V>>> {
        ReadRequest::new(self, key, options, None)
    }
    // A ready memory lookup borrows exactly the async admission/observation
    // path, but needs neither runtime entry nor block_in_place. True misses
    // transfer that same operation once to the driven native executor.
    pub(in crate::cache) fn native_read(
        &self,
        key: &str,
        options: Option<EntryOptions>,
        runtime: &super::BlockingRuntime,
    ) -> Result<MaybeValue<V>> {
        match self.quiet_lookup(
            key,
            options.as_ref(),
            None,
            CacheOperation::TryGet,
            LookupMode::Read,
            super::callback_free::Inputs::NoCallbacks,
        ) {
            QuietStart::Complete(result) => return result.map(MaybeValue::from_value),
            QuietStart::Ready(ready) => return ready.finish(key, None, MaybeValue::from_value),
            QuietStart::Owned {
                observation,
                permit,
            } => {
                return runtime.run(self.prepare_read(
                    ReadOperation {
                        key: self.lookup_key(key, Arc::from(key)),
                        options: None,
                        cancellation: None,
                        observation: observation.into_owned(),
                        permit,
                    },
                    L2ReadPolicy::PreserveFailure,
                    std::convert::identity,
                ));
            }
            QuietStart::Recheck { .. } => {
                unreachable!("read-only lookup never selects origin refresh")
            }
            QuietStart::General => {}
        }
        self.native_read_general(key, options, runtime)
    }
    #[cold]
    #[inline(never)]
    fn native_read_general(
        &self,
        key: &str,
        options: Option<EntryOptions>,
        runtime: &super::BlockingRuntime,
    ) -> Result<MaybeValue<V>> {
        if options.is_some() || !self.inner.native_inline_read() {
            return runtime.run(self.read(key, options));
        }
        match self.start_lookup(key, None, None, CacheOperation::TryGet, LookupMode::Read) {
            LookupStart::Ready(ready) => {
                ready.finish(None, &self.inner.events, MaybeValue::from_value)
            }
            LookupStart::Owned {
                observation,
                key: full,
                permit,
                resolved,
            } => runtime.run(self.prepare_read(
                ReadOperation {
                    key: self.lookup_key(key, Arc::from(full.as_ref())),
                    options: resolved.map(|options| *options),
                    cancellation: None,
                    observation: observation.into_owned(),
                    permit,
                },
                L2ReadPolicy::PreserveFailure,
                std::convert::identity,
            )),
        }
    }
    /// Canonical read with explicit cancellation.
    pub fn read_cancellable<K: AsRef<str>>(
        &self,
        key: K,
        options: Option<EntryOptions>,
        cancellation: FactoryCancellation,
    ) -> impl Future<Output = Result<MaybeValue<V>>> {
        ReadRequest::new(self, key, options, Some(cancellation))
    }
    pub(super) fn begin_read(
        &self,
        key: &str,
        options: Option<Box<EntryOptions>>,
        token: Option<FactoryCancellation>,
    ) -> ReadStart<V> {
        match self.quiet_lookup(
            key,
            options.as_deref(),
            token.as_ref(),
            CacheOperation::TryGet,
            LookupMode::Read,
            super::callback_free::Inputs::NoCallbacks,
        ) {
            QuietStart::Complete(result) => ReadStart::Ready(result.map(MaybeValue::from_value)),
            QuietStart::Ready(ready) => {
                ReadStart::Ready(ready.finish(key, token.as_ref(), MaybeValue::from_value))
            }
            QuietStart::Owned {
                observation,
                permit,
            } => self.pending_read(ReadOperation {
                key: self.lookup_key(key, Arc::from(key)),
                options: options.map(|options| *options),
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
    ) -> ReadStart<V> {
        match self.start_lookup(
            key,
            options.as_deref(),
            token.as_ref(),
            CacheOperation::TryGet,
            LookupMode::Read,
        ) {
            LookupStart::Ready(ready) => ReadStart::Ready(ready.finish(
                token.as_ref(),
                &self.inner.events,
                MaybeValue::from_value,
            )),
            LookupStart::Owned {
                observation,
                key: full,
                permit,
                resolved,
            } => self.pending_read(ReadOperation {
                key: self.lookup_key(key, Arc::from(full.as_ref())),
                options: resolved
                    .map(|options| *options)
                    .or_else(|| options.map(|options| *options)),
                cancellation: token,
                observation: observation.into_owned(),
                permit,
            }),
        }
    }
    #[cold]
    #[inline(never)]
    fn pending_read(&self, operation: ReadOperation<'_>) -> ReadStart<V> {
        ReadStart::Pending(self.prepare_read(
            operation,
            L2ReadPolicy::PreserveFailure,
            std::convert::identity,
        ))
    }
    async fn read_complete<T: Send + 'static>(
        &self,
        key: &str,
        options: Option<EntryOptions>,
        token: Option<FactoryCancellation>,
        operation: CacheOperation,
        policy: L2ReadPolicy,
        complete: impl FnOnce(MaybeValue<V>) -> T + Send + 'static,
    ) -> Result<T> {
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
                return ready.finish(key, token.as_ref(), move |value| {
                    complete(MaybeValue::from_value(value))
                });
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
                    return ready.finish(token.as_ref(), &self.inner.events, move |value| {
                        complete(MaybeValue::from_value(value))
                    });
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
        self.prepare_read(
            ReadOperation {
                key: self.lookup_key(key, full),
                options: resolved.map(|options| *options).or(options),
                cancellation: token,
                observation,
                permit,
            },
            policy,
            complete,
        )
        .await
    }
    // Keep the synchronous ready path independent of the owned L2 preparation
    // body. The existing scope still owns cancellation, callbacks and drainage.
    #[cold]
    #[inline(never)]
    fn prepare_read<T: Send + 'static>(
        &self,
        operation: ReadOperation<'_>,
        policy: L2ReadPolicy,
        complete: impl FnOnce(MaybeValue<V>) -> T + Send + 'static,
    ) -> ObservedExecution<T> {
        let ReadOperation {
            key,
            options,
            cancellation: token,
            observation,
            permit,
        } = operation;
        let worker = self.worker();
        let source = CancellationSource::new();
        let cancellation = source.token();
        self.execute_observed(
            observation,
            token,
            source,
            ObservationAdmission::Inline(permit),
            async move {
                let result = worker.read(key, options, policy, &cancellation).await?;
                Ok(Observed::new(
                    complete(result.value),
                    result.outcome,
                    result.level,
                ))
            },
        )
    }
    /// Returns default only after a successful miss; errors remain errors.
    pub async fn read_or_default(
        &self,
        key: impl AsRef<str>,
        default: V,
        options: Option<EntryOptions>,
    ) -> Result<V> {
        self.read_complete(
            key.as_ref(),
            options,
            None,
            CacheOperation::GetOrDefault,
            L2ReadPolicy::PreserveFailure,
            move |value| value.value_or(default),
        )
        .await
    }
    /// Default-on-successful-miss with explicit cancellation.
    pub async fn read_or_default_cancellable(
        &self,
        key: impl AsRef<str>,
        default: V,
        options: Option<EntryOptions>,
        cancellation: FactoryCancellation,
    ) -> Result<V> {
        self.read_complete(
            key.as_ref(),
            options,
            Some(cancellation),
            CacheOperation::GetOrDefault,
            L2ReadPolicy::PreserveFailure,
            move |value| value.value_or(default),
        )
        .await
    }
    /// Legacy lookup adapter. A diagnosed error maps to absent value because this
    /// historical signature cannot carry failures. Prefer read().
    pub async fn try_get(
        &self,
        key: impl AsRef<str>,
        options: Option<EntryOptions>,
    ) -> MaybeValue<V> {
        match self
            .read_complete(
                key.as_ref(),
                options,
                None,
                CacheOperation::TryGet,
                L2ReadPolicy::FactoryFallback,
                std::convert::identity,
            )
            .await
        {
            Ok(value) => value,
            Err(error) => {
                self.legacy_error(&error);
                MaybeValue::none()
            }
        }
    }
    /// Legacy default adapter; prefer read_or_default for error preservation.
    pub async fn get_or_default(
        &self,
        key: impl AsRef<str>,
        default: V,
        options: Option<EntryOptions>,
    ) -> V {
        let scopes = self.operation_scopes();
        match self
            .read_complete(
                key.as_ref(),
                options,
                None,
                CacheOperation::GetOrDefault,
                L2ReadPolicy::FactoryFallback,
                move |value| (value, scopes.inline_owned()),
            )
            .await
        {
            Ok((value, permit)) => {
                let value = value.value_or(default);
                if let Err(error) = permit.status(None) {
                    self.legacy_error(&error);
                }
                value
            }
            Err(error) => {
                self.legacy_error(&error);
                default
            }
        }
    }
    /// Canonical set using resolved per-key options, started on first poll.
    pub fn try_set<K: AsRef<str>>(
        &self,
        key: K,
        value: V,
    ) -> impl Future<Output = Result<MutationReceipt>> {
        super::mutation_request::MutationRequest::new(self, key, value, None, Box::from([]), None)
    }
    /// Canonical set with tags/options, started on first poll.
    pub fn try_set_full<K: AsRef<str>>(
        &self,
        key: K,
        value: V,
        options: Option<EntryOptions>,
        tags: Box<[Tag]>,
    ) -> impl Future<Output = Result<MutationReceipt>> {
        super::mutation_request::MutationRequest::new(self, key, value, options, tags, None)
    }
    /// Canonical set with explicit cancellation until scheduled ownership transfer.
    pub fn try_set_full_cancellable<K: AsRef<str>>(
        &self,
        key: K,
        value: V,
        options: Option<EntryOptions>,
        tags: Box<[Tag]>,
        token: FactoryCancellation,
    ) -> impl Future<Output = Result<MutationReceipt>> {
        super::mutation_request::MutationRequest::new(self, key, value, options, tags, Some(token))
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
        let source = CancellationSource::new();
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
    /// Legacy unit adapter with options and tags.
    pub async fn set_full(
        &self,
        key: impl AsRef<str>,
        value: V,
        options: Option<EntryOptions>,
        tags: Box<[Tag]>,
    ) {
        if let Err(error) = self.try_set_full(key, value, options, tags).await {
            self.legacy_error(&error);
        }
    }
    /// Canonical remove.
    pub async fn try_remove(&self, key: impl AsRef<str>) -> Result<MutationReceipt> {
        self.try_remove_with(key, None).await
    }
    /// Remove with per-key dynamic/explicit options.
    pub async fn try_remove_with(
        &self,
        key: impl AsRef<str>,
        options: Option<EntryOptions>,
    ) -> Result<MutationReceipt> {
        self.key_mutation(key.as_ref(), options, KeyMutation::Remove, None)
            .await
    }
    /// Cancellable remove.
    pub async fn try_remove_with_cancellable(
        &self,
        key: impl AsRef<str>,
        options: Option<EntryOptions>,
        token: FactoryCancellation,
    ) -> Result<MutationReceipt> {
        self.key_mutation(key.as_ref(), options, KeyMutation::Remove, Some(token))
            .await
    }
    /// Canonical logical expiration, including a cold L2 key.
    pub async fn try_expire(&self, key: impl AsRef<str>) -> Result<MutationReceipt> {
        self.try_expire_with(key, None).await
    }
    /// Expiration with per-key options.
    pub async fn try_expire_with(
        &self,
        key: impl AsRef<str>,
        options: Option<EntryOptions>,
    ) -> Result<MutationReceipt> {
        self.key_mutation(
            key.as_ref(),
            options,
            KeyMutation::Expire(DistributedExpirePolicy::default()),
            None,
        )
        .await
    }
    /// Cancellable expiration.
    pub async fn try_expire_with_cancellable(
        &self,
        key: impl AsRef<str>,
        options: Option<EntryOptions>,
        token: FactoryCancellation,
    ) -> Result<MutationReceipt> {
        self.key_mutation(
            key.as_ref(),
            options,
            KeyMutation::Expire(DistributedExpirePolicy::default()),
            Some(token),
        )
        .await
    }
    /// Expires L1 and selects the distributed retention/removal contract explicitly.
    pub async fn try_expire_with_policy(
        &self,
        key: impl AsRef<str>,
        options: Option<EntryOptions>,
        policy: DistributedExpirePolicy,
    ) -> Result<MutationReceipt> {
        self.key_mutation(key.as_ref(), options, KeyMutation::Expire(policy), None)
            .await
    }
    /// Explicit expiration policy with caller cancellation through ownership transfer.
    pub async fn try_expire_with_policy_cancellable(
        &self,
        key: impl AsRef<str>,
        options: Option<EntryOptions>,
        policy: DistributedExpirePolicy,
        token: FactoryCancellation,
    ) -> Result<MutationReceipt> {
        self.key_mutation(
            key.as_ref(),
            options,
            KeyMutation::Expire(policy),
            Some(token),
        )
        .await
    }
    async fn key_mutation(
        &self,
        key: &str,
        options: Option<EntryOptions>,
        mutation: KeyMutation,
        token: Option<FactoryCancellation>,
    ) -> Result<MutationReceipt> {
        let worker = self.worker();
        let raw: Arc<str> = Arc::from(key);
        let full = worker.full_key(key);
        let operation = match mutation {
            KeyMutation::Remove => CacheOperation::Remove,
            KeyMutation::Expire(_) => CacheOperation::Expire,
        };
        let source = CancellationSource::new();
        let cancellation = source.token();
        self.observed_using(
            operation,
            Some(full),
            token,
            source,
            Box::pin(async move {
                worker
                    .key_mutation(raw, options, mutation, &cancellation)
                    .await
            }),
        )
        .await
    }
    /// Legacy unit remove adapter.
    pub async fn remove(&self, key: impl AsRef<str>) {
        if let Err(error) = self.try_remove(key).await {
            self.legacy_error(&error);
        }
    }
    /// Legacy unit expiration adapter.
    pub async fn expire(&self, key: impl AsRef<str>) {
        if let Err(error) = self.try_expire(key).await {
            self.legacy_error(&error);
        }
    }
    /// Canonical tag invalidation with invariant-preserving Tag.
    pub async fn try_remove_by_tag(&self, tag: Tag) -> Result<MutationReceipt> {
        self.try_remove_by_tag_with(tag, None).await
    }
    /// Tag invalidation with explicit options.
    pub async fn try_remove_by_tag_with(
        &self,
        tag: Tag,
        options: Option<EntryOptions>,
    ) -> Result<MutationReceipt> {
        self.markers_impl(
            vec![MarkerKind::Tag(tag)],
            options,
            CacheOperation::RemoveByTag,
            None,
        )
        .await
    }
    /// Cancellable tag invalidation.
    pub async fn try_remove_by_tag_with_cancellable(
        &self,
        tag: Tag,
        options: Option<EntryOptions>,
        token: FactoryCancellation,
    ) -> Result<MutationReceipt> {
        self.markers_impl(
            vec![MarkerKind::Tag(tag)],
            options,
            CacheOperation::RemoveByTag,
            Some(token),
        )
        .await
    }
    /// Canonical batch tag invalidation.
    pub async fn try_remove_by_tags(
        &self,
        tags: impl IntoIterator<Item = Tag>,
    ) -> Result<MutationReceipt> {
        self.try_remove_by_tags_with(tags, None).await
    }
    /// Batch invalidation with explicit options.
    pub async fn try_remove_by_tags_with(
        &self,
        tags: impl IntoIterator<Item = Tag>,
        options: Option<EntryOptions>,
    ) -> Result<MutationReceipt> {
        self.markers_impl(
            tags.into_iter().map(MarkerKind::Tag).collect(),
            options,
            CacheOperation::RemoveByTags,
            None,
        )
        .await
    }
    /// Cancellable batch invalidation.
    pub async fn try_remove_by_tags_with_cancellable(
        &self,
        tags: impl IntoIterator<Item = Tag>,
        options: Option<EntryOptions>,
        token: FactoryCancellation,
    ) -> Result<MutationReceipt> {
        self.markers_impl(
            tags.into_iter().map(MarkerKind::Tag).collect(),
            options,
            CacheOperation::RemoveByTags,
            Some(token),
        )
        .await
    }
    /// Canonical scoped clear.
    pub async fn try_clear(&self, mode: ClearMode) -> Result<MutationReceipt> {
        self.try_clear_with(mode, None).await
    }
    /// Clear with explicit options.
    pub async fn try_clear_with(
        &self,
        mode: ClearMode,
        options: Option<EntryOptions>,
    ) -> Result<MutationReceipt> {
        self.markers_impl(
            vec![match mode {
                ClearMode::Expire => MarkerKind::ClearExpire,
                ClearMode::Remove => MarkerKind::ClearRemove,
            }],
            options,
            CacheOperation::Clear,
            None,
        )
        .await
    }
    /// Cancellable clear.
    pub async fn try_clear_with_cancellable(
        &self,
        mode: ClearMode,
        options: Option<EntryOptions>,
        token: FactoryCancellation,
    ) -> Result<MutationReceipt> {
        self.markers_impl(
            vec![match mode {
                ClearMode::Expire => MarkerKind::ClearExpire,
                ClearMode::Remove => MarkerKind::ClearRemove,
            }],
            options,
            CacheOperation::Clear,
            Some(token),
        )
        .await
    }
    async fn markers_impl(
        &self,
        kinds: Vec<MarkerKind>,
        options: Option<EntryOptions>,
        operation: CacheOperation,
        token: Option<FactoryCancellation>,
    ) -> Result<MutationReceipt> {
        let worker = self.worker();
        let source = CancellationSource::new();
        let cancellation = source.token();
        self.observed_using(
            operation,
            None,
            token,
            source,
            Box::pin(async move { worker.mutate_markers(kinds, options, cancellation).await }),
        )
        .await
    }
    /// Legacy raw-tag/unit adapter. Invalid requests are diagnosed.
    pub async fn remove_by_tag(&self, tag: impl AsRef<str>) {
        match Tag::new(tag) {
            Ok(tag) => {
                if let Err(error) = self.try_remove_by_tag(tag).await {
                    self.legacy_error(&error);
                }
            }
            Err(error) => self.legacy_error(&error.into()),
        }
    }
    /// Legacy raw batch adapter; any invalid tag rejects the batch.
    pub async fn remove_by_tags<I, S>(&self, tags: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        match crate::tags::try_collect_tags(tags) {
            Ok(tags) => {
                if let Err(error) = self.try_remove_by_tags(Vec::from(tags)).await {
                    self.legacy_error(&error);
                }
            }
            Err(error) => self.legacy_error(&error.into()),
        }
    }
    /// Legacy boolean/unit clear adapter.
    pub async fn clear(&self, allow_fail_safe: bool) {
        if let Err(error) = self
            .try_clear(if allow_fail_safe {
                ClearMode::Expire
            } else {
                ClearMode::Remove
            })
            .await
        {
            self.legacy_error(&error);
        }
    }
    fn legacy_error(&self, error: &Error) {
        tracing::warn!(cache=%self.inner.name,%error,"legacy cache adapter discarded a typed failure; use the canonical fallible API");
    }
    /// Initiates close, cancellation and plugin teardown once.
    pub fn close(&self) -> CloseOutcome {
        self.inner.close()
    }
    /// Waits execution scopes, supervised effects, cleanup, recovery and plugins.
    pub async fn shutdown(&self) -> Result<ShutdownReport> {
        self.check_plugin_drain(crate::DrainOperation::Shutdown)?;
        super::blocking::check_drain(&self.inner.scopes, crate::DrainOperation::Shutdown)?;
        self.inner.shutdown().await
    }
    /// Runs explicit memory maintenance; does not claim background commit completion.
    pub async fn run_pending_tasks(&self) {
        if let Err(error) = self.try_run_pending_tasks().await {
            self.legacy_error(&error);
        }
    }
    /// Runs maintenance, preserving an external L1 provider's typed failure.
    pub async fn try_run_pending_tasks(&self) -> Result<()> {
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
    pub fn memory_storage(&self) -> Option<&Arc<dyn crate::MemoryStorage<V>>> {
        self.inner.memory.provider()
    }
    /// The actual supplied secondary observation L1, when configured.
    pub fn marker_memory_storage(
        &self,
    ) -> Option<&Arc<dyn crate::MemoryStorage<crate::MarkerObservation>>> {
        match &self.inner.marker_reads {
            MarkerReads::DurableRequired => None,
            MarkerReads::OptionsControlled(observations) => observations.memory.provider(),
        }
    }
    /// Actual observation count/weight; absent when independent reads are disabled.
    pub fn marker_memory_usage(&self) -> Result<Option<crate::MemoryUsage>> {
        match &self.inner.marker_reads {
            MarkerReads::DurableRequired => Ok(None),
            MarkerReads::OptionsControlled(observations) => observations.memory.usage().map(Some),
        }
    }
    /// Retained count/weight in the actual L1 keyspace, including shared users.
    pub fn memory_usage(&self) -> Result<crate::MemoryUsage> {
        Ok(self.inner.memory.usage()?)
    }
    /// Waits currently scheduled effects and their cleanup without closing the cache.
    pub async fn flush_pending(&self) -> Result<()> {
        self.check_plugin_drain(crate::DrainOperation::FlushPending)?;
        super::blocking::check_drain(&self.inner.scopes, crate::DrainOperation::FlushPending)?;
        self.inner.tasks.flush().await;
        Ok(())
    }
}
impl<V: Clone + Send + Sync + 'static> Default for Cache<V> {
    fn default() -> Self {
        Self::new()
    }
}
