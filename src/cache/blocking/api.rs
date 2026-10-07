//! Synchronous public operations over the shared cache engine.
use super::*;

impl<V: Clone + Send + Sync + 'static> BlockingCache<V> {
    /// Constructs a default memory cache and its executor.
    pub fn new() -> std::result::Result<Self, BlockingCacheBuildError> {
        Self::from_builder(CacheBuilder::new())
    }
    /// Uses the full existing builder configuration, including native providers.
    pub fn from_builder(
        builder: CacheBuilder<V>,
    ) -> std::result::Result<Self, BlockingCacheBuildError> {
        let runtime = BlockingRuntime::new().map_err(BlockingCacheBuildError::Executor)?;
        Self::on_runtime(builder, runtime).map_err(BlockingCacheBuildError::Cache)
    }
    /// Shares an explicit executor across caches without sharing value storage.
    pub fn on_runtime(builder: CacheBuilder<V>, runtime: BlockingRuntime) -> Result<Self> {
        let owned = runtime.clone();
        let cache = runtime.run(async move {
            builder.try_build_with_executor(ExecutorOwnership::CacheOwned(owned))
        })?;
        let cache = NativeMemoryView::bind(cache, &runtime);
        Ok(Self { cache, runtime })
    }
    pub(in crate::cache) fn plugin_view(cache: Cache<V>, runtime: BlockingRuntime) -> Self {
        let cache = NativeMemoryView::bind(cache, &runtime);
        Self { cache, runtime }
    }
    /// Borrows the same cache for asynchronous operations; cloned async handles
    /// retain its executor and participate in the same final-owner lifecycle.
    pub fn as_async(&self) -> &Cache<V> {
        match &*self.cache.lifetime {
            PublicLifetime::NativeMemory(view) => view.source(),
            PublicLifetime::External(_)
            | PublicLifetime::CacheOwned { .. }
            | PublicLifetime::PluginAccess(_) => &self.cache,
        }
    }
    /// Borrows the driven executor shared by this cache and its scheduled work.
    pub fn runtime(&self) -> &BlockingRuntime {
        &self.runtime
    }
    /// Cache identity shared with all clones and provider operations.
    pub fn name(&self) -> &str {
        self.cache.name()
    }
    /// Stable source identity for backplane operations.
    pub fn instance_id(&self) -> &str {
        self.cache.instance_id()
    }
    /// The full existing observer/plugin event channel.
    pub fn events(&self) -> &Events {
        self.cache.events()
    }
    /// Independent original-value eviction cursors, shared with the async view.
    pub fn memory_evictions(&self) -> &crate::MemoryEvictions<V> {
        self.cache.memory_evictions()
    }
    /// Original per-entry defaults.
    pub fn entry_options(&self) -> EntryOptions {
        self.cache.entry_options()
    }
    /// Original marker-operation defaults.
    pub fn tags_entry_options(&self) -> EntryOptions {
        self.cache.tags_entry_options()
    }
    /// Registers a plugin under the same cache lifecycle.
    pub fn register_cache_plugin(
        &self,
        plugin: Arc<dyn crate::CachePlugin<V>>,
    ) -> Result<crate::PluginRegistration> {
        self.runtime
            .run(async { self.cache.register_cache_plugin(plugin) })
    }
    /// Starts a legacy plugin using this driven executor.
    pub fn register_plugin(&self, plugin: Arc<dyn Plugin>) -> Result<crate::PluginRegistration> {
        self.runtime
            .run(async { self.cache.register_plugin(plugin) })
    }
    /// Waits for native acknowledged subscription readiness.
    pub fn ready(&self) -> Result<BackplaneReadiness> {
        self.runtime.run(self.cache.ready())
    }
    /// Canonical fallible read with no factory.
    pub fn read(
        &self,
        key: impl AsRef<str>,
        options: Option<EntryOptions>,
    ) -> Result<MaybeValue<V>> {
        self.cache.native_read(key.as_ref(), options, &self.runtime)
    }
    /// Read with explicit caller cancellation.
    pub fn read_cancellable(
        &self,
        key: impl AsRef<str>,
        options: Option<EntryOptions>,
        token: FactoryCancellation,
    ) -> Result<MaybeValue<V>> {
        self.runtime
            .run(self.cache.read_cancellable(key, options, token))
    }
    /// Fallible read retaining an explicit fallback value.
    pub fn read_or_default(
        &self,
        key: impl AsRef<str>,
        value: V,
        options: Option<EntryOptions>,
    ) -> Result<V> {
        self.runtime
            .run(self.cache.read_or_default(key, value, options))
    }
    /// Existing compatibility read policy; canonical read preserves failures.
    pub fn try_get(&self, key: impl AsRef<str>, options: Option<EntryOptions>) -> MaybeValue<V> {
        self.runtime.run(self.cache.try_get(key, options))
    }
    /// Existing compatibility default-read policy.
    pub fn get_or_default(
        &self,
        key: impl AsRef<str>,
        value: V,
        options: Option<EntryOptions>,
    ) -> V {
        self.runtime
            .run(self.cache.get_or_default(key, value, options))
    }
    /// Invokes a synchronous origin with the same cache/product rules.
    pub fn get_or_set<F, E>(&self, key: impl AsRef<str>, factory: F) -> Result<V>
    where
        F: FnOnce(FactoryContext<V>) -> std::result::Result<V, E> + Send + 'static,
        E: std::error::Error + Send + Sync + 'static,
    {
        self.get_or_set_full(
            key,
            move |context| factory(context).map_err(FactoryError::from_boundary),
            None,
            Box::from([]),
            MaybeValue::none(),
        )
    }
    /// Synchronous origin with explicit options.
    pub fn get_or_set_with<F>(
        &self,
        key: impl AsRef<str>,
        factory: F,
        options: EntryOptions,
    ) -> Result<V>
    where
        F: FnOnce(FactoryContext<V>) -> std::result::Result<V, FactoryError> + Send + 'static,
    {
        self.get_or_set_full(
            key,
            factory,
            Some(options),
            Box::from([]),
            MaybeValue::none(),
        )
    }
    /// Constant retrieval uses the same key coordination and commit rules.
    pub fn get_or_set_value(
        &self,
        key: impl AsRef<str>,
        value: V,
        options: Option<EntryOptions>,
    ) -> Result<V> {
        self.runtime
            .run(self.cache.get_or_set_value(key, value, options))
    }
    /// Full native origin, adaptive/conditional product, tags and fallback.
    pub fn get_or_set_full<F>(
        &self,
        key: impl AsRef<str>,
        factory: F,
        options: Option<EntryOptions>,
        tags: Box<[Tag]>,
        fallback: MaybeValue<V>,
    ) -> Result<V>
    where
        F: FnOnce(FactoryContext<V>) -> std::result::Result<V, FactoryError> + Send + 'static,
    {
        Ok(self
            .retrieve(key.as_ref(), factory, options, tags, fallback, None)?
            .value)
    }
    /// Full native origin with a synchronous actual commit receipt.
    pub fn get_or_set_full_with_commit<F>(
        &self,
        key: impl AsRef<str>,
        factory: F,
        options: Option<EntryOptions>,
        tags: Box<[Tag]>,
        fallback: MaybeValue<V>,
    ) -> Result<BlockingCacheValue<V>>
    where
        F: FnOnce(FactoryContext<V>) -> std::result::Result<V, FactoryError> + Send + 'static,
    {
        self.retrieve(key.as_ref(), factory, options, tags, fallback, None)
            .map(|value| self.wrap_value(value))
    }
    /// Explicit cancellation remains prompt for a running blocking origin.
    pub fn get_or_set_cancellable<F>(
        &self,
        key: impl AsRef<str>,
        factory: F,
        token: FactoryCancellation,
    ) -> Result<V>
    where
        F: FnOnce(FactoryContext<V>) -> std::result::Result<V, FactoryError> + Send + 'static,
    {
        self.get_or_set_full_cancellable(
            key,
            factory,
            None,
            Box::from([]),
            MaybeValue::none(),
            token,
        )
    }
    /// Full native origin with explicit cancellation until deliberate handoff.
    pub fn get_or_set_full_cancellable<F>(
        &self,
        key: impl AsRef<str>,
        factory: F,
        options: Option<EntryOptions>,
        tags: Box<[Tag]>,
        fallback: MaybeValue<V>,
        token: FactoryCancellation,
    ) -> Result<V>
    where
        F: FnOnce(FactoryContext<V>) -> std::result::Result<V, FactoryError> + Send + 'static,
    {
        Ok(self
            .retrieve(key.as_ref(), factory, options, tags, fallback, Some(token))?
            .value)
    }
    fn retrieve<F>(
        &self,
        key: &str,
        factory: F,
        options: Option<EntryOptions>,
        tags: Box<[Tag]>,
        fallback: MaybeValue<V>,
        token: Option<FactoryCancellation>,
    ) -> Result<CacheValue<V>>
    where
        F: FnOnce(FactoryContext<V>) -> std::result::Result<V, FactoryError> + Send + 'static,
    {
        let cancellation = match token {
            Some(_) => CallerCancellation::Explicit,
            None => CallerCancellation::Absent,
        };
        let default_present = fallback.has_value();
        self.cache.native_origin_lazy(
            key,
            || {
                let dispatch = FactoryDispatch {
                    caller: thread::current().id(),
                    cancellation,
                    default_present,
                    lineage: self.runtime.lineage(),
                };
                let seed = self.cache.worker_seed();
                let runtime = self.runtime.clone();
                FactoryOrigin::new(move |ctx: FactoryContext<V>| {
                    run_factory(seed.worker(), runtime, dispatch, factory, ctx)
                })
            },
            options,
            tags,
            fallback,
            token,
            &self.runtime,
        )
    }
    fn wrap_receipt(&self, receipt: MutationReceipt) -> BlockingMutationReceipt {
        match receipt {
            MutationReceipt::Completed(report) => BlockingMutationReceipt::Completed(report),
            MutationReceipt::Scheduled(completion) => {
                BlockingMutationReceipt::Scheduled(BlockingCommitCompletion {
                    completion,
                    runtime: self.runtime.clone(),
                })
            }
        }
    }
    fn wrap_value(&self, value: CacheValue<V>) -> BlockingCacheValue<V> {
        BlockingCacheValue {
            value: value.value,
            commit: match value.commit {
                CommitReceipt::Unchanged => BlockingCommitReceipt::Unchanged,
                CommitReceipt::Mutation(receipt) => {
                    BlockingCommitReceipt::Mutation(self.wrap_receipt(receipt))
                }
            },
        }
    }
    /// Set with resolved per-key options.
    pub fn try_set(&self, key: impl AsRef<str>, value: V) -> Result<BlockingMutationReceipt> {
        self.runtime
            .run(self.cache.try_set(key, value))
            .map(|r| self.wrap_receipt(r))
    }
    /// Set with tags and explicit options.
    pub fn try_set_full(
        &self,
        key: impl AsRef<str>,
        value: V,
        options: Option<EntryOptions>,
        tags: Box<[Tag]>,
    ) -> Result<BlockingMutationReceipt> {
        self.runtime
            .run(self.cache.try_set_full(key, value, options, tags))
            .map(|r| self.wrap_receipt(r))
    }
    /// Cancellable set; scheduled commits retain their independent ownership.
    pub fn try_set_full_cancellable(
        &self,
        key: impl AsRef<str>,
        value: V,
        options: Option<EntryOptions>,
        tags: Box<[Tag]>,
        token: FactoryCancellation,
    ) -> Result<BlockingMutationReceipt> {
        self.runtime
            .run(
                self.cache
                    .try_set_full_cancellable(key, value, options, tags, token),
            )
            .map(|r| self.wrap_receipt(r))
    }
    /// Stores a value lazily; execute the configured request explicitly.
    pub fn set<K: AsRef<str>>(
        &self,
        key: K,
        value: V,
    ) -> BlockingRequest<'_, super::super::SetRequest<'_, K, V>> {
        BlockingRequest {
            runtime: &self.runtime,
            request: self.cache.set(key, value),
        }
    }
    /// Physically removes a key with a typed result.
    pub fn remove<K: AsRef<str>>(
        &self,
        key: K,
    ) -> BlockingRequest<'_, super::super::RemoveRequest<'_, K, V>> {
        BlockingRequest {
            runtime: &self.runtime,
            request: self.cache.remove(key),
        }
    }
    /// Expires L1 and removes L2 by default.
    pub fn expire<K: AsRef<str>>(
        &self,
        key: K,
    ) -> BlockingRequest<'_, super::super::ExpireRequest<'_, K, V>> {
        BlockingRequest {
            runtime: &self.runtime,
            request: self.cache.expire(key),
        }
    }
    /// Invalidates a raw tag, optionally adding a batch before execution.
    pub fn remove_by_tag(
        &self,
        tag: impl AsRef<str>,
    ) -> BlockingRequest<'_, super::super::TagInvalidationRequest<'_, V>> {
        BlockingRequest {
            runtime: &self.runtime,
            request: self.cache.remove_by_tag(tag),
        }
    }
    /// Invalidates this cache using an explicit expiration or removal mode.
    pub fn clear(&self, mode: ClearMode) -> BlockingRequest<'_, super::super::ClearRequest<'_, V>> {
        BlockingRequest {
            runtime: &self.runtime,
            request: self.cache.clear(mode),
        }
    }
    /// Initiates cache shutdown without claiming drainage.
    pub fn close(&self) -> CloseOutcome {
        self.cache.close()
    }
    /// Waits for actual callbacks, commits, lease cleanup and plugin stop.
    pub fn shutdown(&self) -> Result<ShutdownReport> {
        self.runtime.run(self.cache.shutdown())
    }
    /// Waits for currently scheduled effects and callback completion.
    pub fn flush_pending(&self) -> Result<()> {
        self.runtime.run(self.cache.flush_pending())
    }
    /// Performs explicit physical memory/idle-lock maintenance.
    pub fn run_pending_tasks(&self) {
        self.runtime.run(self.cache.run_pending_tasks());
    }
    /// Performs maintenance while preserving a supplied L1 provider failure.
    pub fn try_run_pending_tasks(&self) -> Result<()> {
        self.runtime.run(self.cache.try_run_pending_tasks())
    }
    /// The explicitly configured shared L1 provider.
    pub fn memory_storage(&self) -> Option<&Arc<dyn crate::MemoryStorage<V>>> {
        self.cache.memory_storage()
    }
    /// The actual supplied secondary observation L1, when configured.
    pub fn marker_memory_storage(
        &self,
    ) -> Option<&Arc<dyn crate::MemoryStorage<crate::MarkerObservation>>> {
        self.cache.marker_memory_storage()
    }
    /// Actual observation count/weight; absent when independent reads are disabled.
    pub fn marker_memory_usage(&self) -> Result<Option<crate::MemoryUsage>> {
        self.cache.marker_memory_usage()
    }
    /// Diagnostic usage of the actual L1 keyspace.
    pub fn memory_usage(&self) -> Result<crate::MemoryUsage> {
        self.cache.memory_usage()
    }
    /// The configured distributed byte store, if enabled.
    pub fn distributed_cache(&self) -> Option<&Arc<dyn DistributedCache>> {
        self.cache.distributed_cache()
    }

    /// The configured peer notification provider, if enabled.
    pub fn backplane(&self) -> Option<&Arc<dyn Backplane>> {
        self.cache.backplane()
    }

    /// The configured distributed ownership provider, if enabled.
    pub fn distributed_locker(&self) -> Option<&Arc<dyn DistributedLocker>> {
        self.cache.distributed_locker()
    }

    /// Selected independent secondary marker read policy.
    pub fn marker_read_policy(&self) -> MarkerReadPolicy {
        self.cache.marker_read_policy()
    }

    /// Selected marker snapshot lifetime and renewal policy.
    pub fn marker_lifecycle_policy(&self) -> MarkerLifecyclePolicy {
        self.cache.marker_lifecycle_policy()
    }

    /// Whether native subscription readiness is required before admission.
    pub fn wait_for_initial_backplane_subscribe(&self) -> bool {
        self.cache.wait_for_initial_backplane_subscribe()
    }

    /// Exact queued and in-flight automatic recovery count.
    pub fn pending_recovery(&self) -> usize {
        self.cache.pending_recovery()
    }

    /// Immutable diagnostic ticket for the exact data-key commit lane.
    pub fn recovery_ticket(&self, key: impl AsRef<str>) -> Option<ReplayTicket> {
        self.cache.recovery_ticket(key)
    }

    /// Immutable tag or clear recovery stage.
    pub fn marker_recovery_ticket(&self, kind: &MarkerKind) -> Option<ReplayTicket> {
        self.cache.marker_recovery_ticket(kind)
    }

    /// Immutable finite marker observation repair stage.
    pub fn marker_snapshot_recovery_ticket(&self, kind: &MarkerKind) -> Option<ReplayTicket> {
        self.cache.marker_snapshot_recovery_ticket(kind)
    }

    /// Default only after a successful miss; cancellation remains an error.
    pub fn read_or_default_cancellable(
        &self,
        key: impl AsRef<str>,
        value: V,
        options: Option<EntryOptions>,
        token: FactoryCancellation,
    ) -> Result<V> {
        self.runtime.run(
            self.cache
                .read_or_default_cancellable(key, value, options, token),
        )
    }

    /// Supplied-value retrieval with tags; no factory timeouts or eager work.
    pub fn get_or_set_value_full(
        &self,
        key: impl AsRef<str>,
        value: V,
        options: Option<EntryOptions>,
        tags: Box<[Tag]>,
    ) -> Result<V> {
        self.runtime
            .run(self.cache.get_or_set_value_full(key, value, options, tags))
    }

    /// Cancellable supplied-value retrieval with tags and per-entry options.
    pub fn get_or_set_value_full_cancellable(
        &self,
        key: impl AsRef<str>,
        value: V,
        options: Option<EntryOptions>,
        tags: Box<[Tag]>,
        token: FactoryCancellation,
    ) -> Result<V> {
        self.runtime.run(
            self.cache
                .get_or_set_value_full_cancellable(key, value, options, tags, token),
        )
    }

    /// Supplied-value retrieval and its actual synchronous commit receipt.
    pub fn get_or_set_value_full_with_commit(
        &self,
        key: impl AsRef<str>,
        value: V,
        options: Option<EntryOptions>,
        tags: Box<[Tag]>,
    ) -> Result<BlockingCacheValue<V>> {
        self.runtime
            .run(
                self.cache
                    .get_or_set_value_full_with_commit(key, value, options, tags),
            )
            .map(|value| self.wrap_value(value))
    }

    /// Cancellable supplied-value retrieval and its actual commit receipt.
    pub fn get_or_set_value_full_with_commit_cancellable(
        &self,
        key: impl AsRef<str>,
        value: V,
        options: Option<EntryOptions>,
        tags: Box<[Tag]>,
        token: FactoryCancellation,
    ) -> Result<BlockingCacheValue<V>> {
        self.runtime
            .run(
                self.cache.get_or_set_value_full_with_commit_cancellable(
                    key, value, options, tags, token,
                ),
            )
            .map(|value| self.wrap_value(value))
    }

    /// Cancellable synchronous factory retrieval with actual commit completion.
    pub fn get_or_set_full_with_commit_cancellable<F>(
        &self,
        key: impl AsRef<str>,
        factory: F,
        options: Option<EntryOptions>,
        tags: Box<[Tag]>,
        fallback: MaybeValue<V>,
        token: FactoryCancellation,
    ) -> Result<BlockingCacheValue<V>>
    where
        F: FnOnce(FactoryContext<V>) -> std::result::Result<V, FactoryError> + Send + 'static,
    {
        self.retrieve(key.as_ref(), factory, options, tags, fallback, Some(token))
            .map(|value| self.wrap_value(value))
    }
}
