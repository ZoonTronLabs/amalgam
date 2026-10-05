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
        Ok(Self { cache, runtime })
    }
    pub(in crate::cache) fn plugin_view(cache: Cache<V>, runtime: BlockingRuntime) -> Self {
        Self { cache, runtime }
    }
    /// Borrows the same cache for asynchronous operations; cloned async handles
    /// retain its executor and participate in the same final-owner lifecycle.
    pub fn as_async(&self) -> &Cache<V> {
        &self.cache
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
        self.runtime.run(self.cache.read(key, options))
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
    pub fn get_or_set<F>(&self, key: impl AsRef<str>, factory: F) -> Result<V>
    where
        F: FnOnce(FactoryContext<V>) -> std::result::Result<FactoryProduct<V>, FactoryError>
            + Send
            + 'static,
    {
        self.get_or_set_full(key, factory, None, Box::from([]), MaybeValue::none())
    }
    /// Synchronous origin with explicit options.
    pub fn get_or_set_with<F>(
        &self,
        key: impl AsRef<str>,
        factory: F,
        options: EntryOptions,
    ) -> Result<V>
    where
        F: FnOnce(FactoryContext<V>) -> std::result::Result<FactoryProduct<V>, FactoryError>
            + Send
            + 'static,
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
        F: FnOnce(FactoryContext<V>) -> std::result::Result<FactoryProduct<V>, FactoryError>
            + Send
            + 'static,
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
        F: FnOnce(FactoryContext<V>) -> std::result::Result<FactoryProduct<V>, FactoryError>
            + Send
            + 'static,
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
        F: FnOnce(FactoryContext<V>) -> std::result::Result<FactoryProduct<V>, FactoryError>
            + Send
            + 'static,
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
        F: FnOnce(FactoryContext<V>) -> std::result::Result<FactoryProduct<V>, FactoryError>
            + Send
            + 'static,
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
        F: FnOnce(FactoryContext<V>) -> std::result::Result<FactoryProduct<V>, FactoryError>
            + Send
            + 'static,
    {
        let dispatch = FactoryDispatch {
            caller: thread::current().id(),
            cancellation: match token {
                Some(_) => CallerCancellation::Explicit,
                None => CallerCancellation::Absent,
            },
            default_present: fallback.has_value(),
            lineage: self.runtime.lineage(),
        };
        let worker = self.cache.worker();
        let runtime = self.runtime.clone();
        let origin =
            move |ctx: FactoryContext<V>| run_factory(worker, runtime, dispatch, factory, ctx);
        self.runtime.run(
            self.cache
                .get_or_set_impl(key, origin, options, tags, fallback, token),
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
    /// Remove with resolved per-key options.
    pub fn try_remove(&self, key: impl AsRef<str>) -> Result<BlockingMutationReceipt> {
        self.runtime
            .run(self.cache.try_remove(key))
            .map(|r| self.wrap_receipt(r))
    }
    /// Remove with explicit options and cancellation.
    pub fn try_remove_with_cancellable(
        &self,
        key: impl AsRef<str>,
        options: Option<EntryOptions>,
        token: FactoryCancellation,
    ) -> Result<BlockingMutationReceipt> {
        self.runtime
            .run(self.cache.try_remove_with_cancellable(key, options, token))
            .map(|r| self.wrap_receipt(r))
    }
    /// Expire L1 while selecting the explicit L2 contract.
    pub fn try_expire_with_policy(
        &self,
        key: impl AsRef<str>,
        options: Option<EntryOptions>,
        policy: DistributedExpirePolicy,
    ) -> Result<BlockingMutationReceipt> {
        self.runtime
            .run(self.cache.try_expire_with_policy(key, options, policy))
            .map(|r| self.wrap_receipt(r))
    }
    /// Expire using the retained-stale legacy L2 contract.
    pub fn try_expire(&self, key: impl AsRef<str>) -> Result<BlockingMutationReceipt> {
        self.runtime
            .run(self.cache.try_expire(key))
            .map(|r| self.wrap_receipt(r))
    }
    /// Invalidate one validated tag using marker defaults.
    pub fn try_remove_by_tag(&self, tag: Tag) -> Result<BlockingMutationReceipt> {
        self.runtime
            .run(self.cache.try_remove_by_tag(tag))
            .map(|r| self.wrap_receipt(r))
    }
    /// Invalidate a batch with independent marker options.
    pub fn try_remove_by_tags_with(
        &self,
        tags: impl IntoIterator<Item = Tag>,
        options: Option<EntryOptions>,
    ) -> Result<BlockingMutationReceipt> {
        self.runtime
            .run(self.cache.try_remove_by_tags_with(tags, options))
            .map(|r| self.wrap_receipt(r))
    }
    /// Cancellable batch invalidation using the same marker pipeline.
    pub fn try_remove_by_tags_with_cancellable(
        &self,
        tags: impl IntoIterator<Item = Tag>,
        options: Option<EntryOptions>,
        token: FactoryCancellation,
    ) -> Result<BlockingMutationReceipt> {
        self.runtime
            .run(
                self.cache
                    .try_remove_by_tags_with_cancellable(tags, options, token),
            )
            .map(|r| self.wrap_receipt(r))
    }
    /// Scoped expire/remove clear using marker defaults.
    pub fn try_clear(&self, mode: ClearMode) -> Result<BlockingMutationReceipt> {
        self.runtime
            .run(self.cache.try_clear(mode))
            .map(|r| self.wrap_receipt(r))
    }
    /// Cancellable scoped clear with independent marker policy.
    pub fn try_clear_with_cancellable(
        &self,
        mode: ClearMode,
        options: Option<EntryOptions>,
        token: FactoryCancellation,
    ) -> Result<BlockingMutationReceipt> {
        self.runtime
            .run(self.cache.try_clear_with_cancellable(mode, options, token))
            .map(|r| self.wrap_receipt(r))
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

    /// Remove with explicit per-entry options.
    pub fn try_remove_with(
        &self,
        key: impl AsRef<str>,
        options: Option<EntryOptions>,
    ) -> Result<BlockingMutationReceipt> {
        self.runtime
            .run(self.cache.try_remove_with(key, options))
            .map(|receipt| self.wrap_receipt(receipt))
    }

    /// Expire with explicit per-entry options and retained stale L2.
    pub fn try_expire_with(
        &self,
        key: impl AsRef<str>,
        options: Option<EntryOptions>,
    ) -> Result<BlockingMutationReceipt> {
        self.runtime
            .run(self.cache.try_expire_with(key, options))
            .map(|receipt| self.wrap_receipt(receipt))
    }

    /// Cancellable expiration with retained stale L2.
    pub fn try_expire_with_cancellable(
        &self,
        key: impl AsRef<str>,
        options: Option<EntryOptions>,
        token: FactoryCancellation,
    ) -> Result<BlockingMutationReceipt> {
        self.runtime
            .run(self.cache.try_expire_with_cancellable(key, options, token))
            .map(|receipt| self.wrap_receipt(receipt))
    }

    /// Cancellable expiration with an explicit distributed retention policy.
    pub fn try_expire_with_policy_cancellable(
        &self,
        key: impl AsRef<str>,
        options: Option<EntryOptions>,
        policy: DistributedExpirePolicy,
        token: FactoryCancellation,
    ) -> Result<BlockingMutationReceipt> {
        self.runtime
            .run(
                self.cache
                    .try_expire_with_policy_cancellable(key, options, policy, token),
            )
            .map(|receipt| self.wrap_receipt(receipt))
    }

    /// Invalidate one tag with independent explicit marker options.
    pub fn try_remove_by_tag_with(
        &self,
        tag: Tag,
        options: Option<EntryOptions>,
    ) -> Result<BlockingMutationReceipt> {
        self.runtime
            .run(self.cache.try_remove_by_tag_with(tag, options))
            .map(|receipt| self.wrap_receipt(receipt))
    }

    /// Cancellable tag invalidation with independent marker options.
    pub fn try_remove_by_tag_with_cancellable(
        &self,
        tag: Tag,
        options: Option<EntryOptions>,
        token: FactoryCancellation,
    ) -> Result<BlockingMutationReceipt> {
        self.runtime
            .run(
                self.cache
                    .try_remove_by_tag_with_cancellable(tag, options, token),
            )
            .map(|receipt| self.wrap_receipt(receipt))
    }

    /// Invalidate a batch using the independent marker defaults.
    pub fn try_remove_by_tags(
        &self,
        tags: impl IntoIterator<Item = Tag>,
    ) -> Result<BlockingMutationReceipt> {
        self.runtime
            .run(self.cache.try_remove_by_tags(tags))
            .map(|receipt| self.wrap_receipt(receipt))
    }

    /// Scoped clear with independent explicit marker options.
    pub fn try_clear_with(
        &self,
        mode: ClearMode,
        options: Option<EntryOptions>,
    ) -> Result<BlockingMutationReceipt> {
        self.runtime
            .run(self.cache.try_clear_with(mode, options))
            .map(|receipt| self.wrap_receipt(receipt))
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
        F: FnOnce(FactoryContext<V>) -> std::result::Result<FactoryProduct<V>, FactoryError>
            + Send
            + 'static,
    {
        self.retrieve(key.as_ref(), factory, options, tags, fallback, Some(token))
            .map(|value| self.wrap_value(value))
    }
}
