//! Public operations and their observed execution boundaries.
use super::{
    Arc, Backplane, BackplaneReadiness, Cache, CacheBuilder, CacheOperation, CacheValue,
    CancellationSource, ClearMode, CloseOutcome, CommitReceipt, Cow, DistributedCache,
    DistributedExpirePolicy, DistributedLocker, EntryOptions, Error, Events, FactoryCancellation,
    FactoryContext, FactoryError, FactoryProduct, Future, InlinePermit, Instrument, KeyMutation,
    L2ReadPolicy, LookupKey, LookupMode, LookupStart, MarkerKind, MarkerLifecyclePolicy,
    MarkerReadPolicy, MaybeValue, MutationReceipt, ObservationAdmission, Observed,
    OperationObservation, OperationOutcome, Ordering, Pin, Plugin, ReadyLookup, ReadyValue,
    ReplayTicket, Result, ShutdownReport, Storage, Tag, TagVerdict, Worker, drive,
};

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
    /// Starts a dynamic plugin session owned by its registration and this cache.
    /// Dropping or stopping the registration detaches it; shutdown waits callbacks.
    pub fn register_plugin(&self, plugin: Arc<dyn Plugin>) -> Result<crate::PluginRegistration> {
        let permit = self.inner.scopes.inline();
        permit.admit()?;
        let registration = self.inner.plugins.register(plugin)?;
        permit.status(None)?;
        Ok(registration)
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
        if self.inner.scopes.is_closed() {
            return Err(Error::CacheClosed);
        }
        let worker = self.worker();
        self.inner
            .scopes
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
    pub(super) fn worker(&self) -> Worker<V> {
        Worker {
            inner: Arc::clone(&self.inner),
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
    fn observed<T: Send + 'static>(
        &self,
        operation: CacheOperation,
        key: Option<Arc<str>>,
        token: Option<FactoryCancellation>,
        work: impl Future<Output = Result<Observed<T>>> + Send + 'static,
    ) -> impl Future<Output = Result<T>> + Send + '_ {
        // The scope will own this work on the heap. Erase it before composing
        // the observer futures, so callers do not embed the complete mutation
        // state machine in every async adapter and tracing layer.
        self.observed_using(
            operation,
            key,
            token,
            CancellationSource::new(),
            Box::pin(work),
        )
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
    async fn execute_observed<T: Send + 'static>(
        &self,
        mut observation: OperationObservation,
        token: Option<FactoryCancellation>,
        source: CancellationSource,
        admission: ObservationAdmission<'_>,
        work: impl Future<Output = Result<Observed<T>>> + Send + 'static,
    ) -> Result<T> {
        let span = observation.span();
        if self.inner.scopes.is_closed() {
            drop(work);
            observation.finish(OperationOutcome::from_error(&Error::CacheClosed));
            return Err(Error::CacheClosed);
        }
        self.worker().start_maintenance();
        let worker = self.worker();
        let completion_token = source.token();
        // The scope owns the observer too: a parked caller can be cancelled and
        // finish its logical observation without polling its future again.
        let execution = self.inner.scopes.execution(
            async move {
                if worker.inner.wait_for_initial_backplane_subscribe
                    && !worker.inner.subscription_admitted.load(Ordering::Acquire)
                {
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
        drive(execution, token).await
    }
    fn start_lookup<'a>(
        &'a self,
        raw: &'a str,
        options: Option<&EntryOptions>,
        token: Option<&FactoryCancellation>,
        operation: CacheOperation,
        mode: LookupMode,
    ) -> LookupStart<'a, V> {
        let permit = self.inner.scopes.inline();
        let key = match &self.inner.key_prefix {
            Some(prefix) => Cow::Owned(format!("{prefix}{raw}")),
            None => Cow::Borrowed(raw),
        };
        let observation = OperationObservation::new(
            self.inner.events.clone(),
            &self.inner.name,
            &self.inner.instance_id,
            operation,
            Some(&key),
        );
        let span = observation.span();
        let _entered = span.enter();
        let result = permit
            .admit()
            .and_then(|()| self.ready_value(&key, options, token, mode, &permit));
        match result {
            Ok(None) => LookupStart::Owned {
                observation,
                key,
                permit,
            },
            Ok(Some(value)) => LookupStart::Ready(ReadyLookup {
                result: Ok(ReadyValue { value, key }),
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
    ) -> Result<Option<V>> {
        permit.status(token)?;
        let worker = self.worker();
        worker.start_maintenance();
        if self.inner.wait_for_initial_backplane_subscribe
            && !self.inner.subscription_admitted.load(Ordering::Acquire)
            || options.is_none() && self.inner.default_options_provider.is_some()
        {
            return Ok(None);
        }
        let opts = options.unwrap_or(&self.inner.default_options);
        worker.validate_options(opts)?;
        if opts.skip_memory_read() {
            return Ok(None);
        }
        worker.ensure_health();
        permit.status(token)?;
        let now = self.inner.clock.now();
        permit.status(token)?;
        let entry = self.inner.memory.ready_at(key, now);
        permit.status(token)?;
        let Some(entry) = entry else {
            return Ok(None);
        };
        if worker.tags(&entry) != TagVerdict::Valid || !entry.freshness(now).is_fresh() {
            return Ok(None);
        }
        if !worker.marker_reads_ready(entry.meta().tags(), now) {
            return Ok(None);
        }
        if matches!(mode, LookupMode::GetOrSet) {
            let eager = entry.should_eager_refresh(self.inner.clock.now());
            permit.status(token)?;
            if eager {
                return Ok(None);
            }
        }
        let value = worker.copy(entry.value(), opts);
        permit.status(token)?;
        let value = value?;
        worker.marker_ready_events(entry.meta().tags(), now);
        permit.status(token)?;
        Ok(Some(value))
    }
    /// Returns a value or produces it. Background write policy is observable;
    /// use get_or_set_full_with_commit when its actual completion is required.
    pub async fn get_or_set<F, Fut>(&self, key: impl AsRef<str>, factory: F) -> Result<V>
    where
        F: FnOnce(FactoryContext<V>) -> Fut + Send + 'static,
        Fut: Future<Output = std::result::Result<FactoryProduct<V>, FactoryError>> + Send + 'static,
    {
        self.get_or_set_full(key, factory, None, Box::from([]), MaybeValue::none())
            .await
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
    /// Constant-value retrieval through the same coordination pipeline.
    pub async fn get_or_set_value(
        &self,
        key: impl AsRef<str>,
        value: V,
        options: Option<EntryOptions>,
    ) -> Result<V> {
        self.get_or_set_full(
            key,
            move |ctx| async move { Ok(ctx.value(value)) },
            options,
            Box::from([]),
            MaybeValue::none(),
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
    async fn get_or_set_impl<F, Fut>(
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
        let (observation, full, permit) = match self.start_lookup(
            key,
            options.as_ref(),
            cancellation.as_ref(),
            CacheOperation::GetOrSet,
            LookupMode::GetOrSet,
        ) {
            LookupStart::Ready(ready) => {
                // These captures can execute user Drop code; keep them within
                // the same counted operation before its final cancellation check.
                let span = ready.observation.span();
                let _entered = span.enter();
                drop((factory, tags, fallback, options));
                return ready
                    .finish(
                        cancellation.as_ref(),
                        &self.inner.events,
                        std::convert::identity,
                    )
                    .map(|value| CacheValue {
                        value,
                        commit: CommitReceipt::Unchanged,
                    });
            }
            LookupStart::Owned {
                observation,
                key,
                permit,
            } => (observation, Arc::<str>::from(key.as_ref()), permit),
        };
        let worker = self.worker();
        let key = self.lookup_key(key, full);
        let source = CancellationSource::new();
        let caller = source.token();
        self.execute_observed(
            observation,
            cancellation,
            source,
            ObservationAdmission::Inline(permit),
            async move {
                worker
                    .get_or_set(key, factory, options, tags, fallback, caller)
                    .await
            },
        )
        .await
    }
    /// Canonical read-only L1/L2 lookup. Expected failures retain their typed channel.
    pub async fn read(
        &self,
        key: impl AsRef<str>,
        options: Option<EntryOptions>,
    ) -> Result<MaybeValue<V>> {
        self.read_impl(key.as_ref(), options, None, CacheOperation::TryGet)
            .await
    }
    /// Canonical read with explicit cancellation.
    pub async fn read_cancellable(
        &self,
        key: impl AsRef<str>,
        options: Option<EntryOptions>,
        cancellation: FactoryCancellation,
    ) -> Result<MaybeValue<V>> {
        self.read_impl(
            key.as_ref(),
            options,
            Some(cancellation),
            CacheOperation::TryGet,
        )
        .await
    }
    async fn read_impl(
        &self,
        key: &str,
        options: Option<EntryOptions>,
        token: Option<FactoryCancellation>,
        operation: CacheOperation,
    ) -> Result<MaybeValue<V>> {
        self.read_complete(
            key,
            options,
            token,
            operation,
            L2ReadPolicy::PreserveFailure,
            std::convert::identity,
        )
        .await
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
        let (observation, full, permit) = match self.start_lookup(
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
            } => (observation, Arc::<str>::from(key.as_ref()), permit),
        };
        let worker = self.worker();
        let key = self.lookup_key(key, full);
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
        .await
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
        let scopes = Arc::clone(&self.inner.scopes);
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
    /// Canonical set using resolved per-key options.
    pub async fn try_set(&self, key: impl AsRef<str>, value: V) -> Result<MutationReceipt> {
        self.try_set_full(key, value, None, Box::from([])).await
    }
    /// Canonical set with tags/options.
    pub async fn try_set_full(
        &self,
        key: impl AsRef<str>,
        value: V,
        options: Option<EntryOptions>,
        tags: Box<[Tag]>,
    ) -> Result<MutationReceipt> {
        self.set_impl(key.as_ref(), value, options, tags, None)
            .await
    }
    /// Canonical set with explicit cancellation until scheduled ownership transfer.
    pub async fn try_set_full_cancellable(
        &self,
        key: impl AsRef<str>,
        value: V,
        options: Option<EntryOptions>,
        tags: Box<[Tag]>,
        token: FactoryCancellation,
    ) -> Result<MutationReceipt> {
        self.set_impl(key.as_ref(), value, options, tags, Some(token))
            .await
    }
    async fn set_impl(
        &self,
        key: &str,
        value: V,
        options: Option<EntryOptions>,
        tags: Box<[Tag]>,
        token: Option<FactoryCancellation>,
    ) -> Result<MutationReceipt> {
        let worker = self.worker();
        let raw: Arc<str> = Arc::from(key);
        let full = worker.full_key(key);
        let source = CancellationSource::new();
        let cancellation = source.token();
        self.observed_using(
            CacheOperation::Set,
            Some(full),
            token,
            source,
            Box::pin(async move { worker.set(raw, value, options, tags, &cancellation).await }),
        )
        .await
    }
    /// Legacy unit adapter. Prefer try_set to inspect actual completion.
    pub async fn set(&self, key: impl AsRef<str>, value: V) {
        if let Err(error) = self.try_set(key, value).await {
            self.legacy_error(&error);
        }
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
            KeyMutation::Expire(DistributedExpirePolicy::RetainStale),
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
            KeyMutation::Expire(DistributedExpirePolicy::RetainStale),
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
        self.observed(operation, None, token, async move {
            worker.mutate_markers(kinds, options).await
        })
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
        self.inner.shutdown().await
    }
    /// Runs explicit memory maintenance; does not claim background commit completion.
    pub async fn run_pending_tasks(&self) {
        self.inner.memory.run_pending_tasks().await;
        self.inner.locks.clean_idle(256);
        self.inner.lanes.clean(256);
    }
    /// Waits currently scheduled effects and their cleanup without closing the cache.
    pub async fn flush_pending(&self) -> Result<()> {
        self.inner.tasks.flush().await;
        Ok(())
    }
}
impl<V: Clone + Send + Sync + 'static> Default for Cache<V> {
    fn default() -> Self {
        Self::new()
    }
}
