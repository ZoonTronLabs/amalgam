//! The [`Cache`] type — the heart of the library.
//!
//! This is the Rust counterpart of `IFusionCache`. The marquee method is
//! [`Cache::get_or_set`]; its flow encodes cache-stampede protection, fail-safe,
//! soft/hard timeouts with background completion, eager refresh, adaptive
//! caching and conditional refresh. See `docs/PARITY.md` for the mapping to
//! FusionCache.

use std::future::Future;
use std::sync::{Arc, Weak};
use std::time::Duration;

use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use async_trait::async_trait;

use crate::backplane::{Backplane, BackplaneAction, BackplaneMessage};
use crate::circuit::CircuitBreaker;
use crate::distributed::{DistributedCache, DistributedEntry, DistributedSerializer};
use crate::distributed_lock::DistributedLocker;
use crate::entry::Entry;
use crate::error::{Error, FactoryError, Result};
use crate::events::{CacheEvent, CircuitComponent, Events};
use crate::factory::{FactoryContext, FactoryProduct, StaleInfo};
use crate::locking::{KeyGuard, KeyedLock};
use crate::maybe::MaybeValue;
use crate::memory::MemoryStore;
use crate::options::{EntryOptions, KeyModifierMode, RemoveByTagBehavior};
use crate::plugins::{Plugin, PluginHost};
use crate::recovery::{
    AutoRecoveryService, RecoveryAction, RecoveryConfig, RecoveryExecutor, RecoveryItem,
};
use crate::registry::DefaultEntryOptionsProvider;
use crate::tags::{Tag, TagRegistry, TagVerdict};
use crate::time::{Clock, SystemClock, Timeout, Timestamp};

/// A robust, multi-level cache.
///
/// `Cache<V>` is generic over a single value type `V`. Unlike FusionCache (which
/// leans on .NET runtime type information to store heterogeneous values in one
/// instance), the idiomatic Rust model is one value type per cache — this makes
/// "wrong type for this key" unrepresentable instead of a run-time downcast.
/// Use several caches, or a sum type / `serde_json::Value`, for heterogeneous
/// values.
///
/// Cloning a `Cache` is cheap (it shares one underlying instance) and is how you
/// hand it to other tasks.
pub struct Cache<V: Clone + Send + Sync + 'static> {
    inner: Arc<CacheInner<V>>,
}

struct CacheInner<V: Clone + Send + Sync + 'static> {
    name: Arc<str>,
    instance_id: Arc<str>,
    memory: MemoryStore<V>,
    locks: Arc<KeyedLock>,
    tags: Arc<TagRegistry>,
    events: Events,
    clock: Arc<dyn Clock>,
    default_options: EntryOptions,
    key_prefix: Option<Arc<str>>,
    remove_by_tag_behavior: RemoveByTagBehavior,
    distributed: Option<Arc<dyn DistributedCache>>,
    serializer: Option<Arc<dyn DistributedSerializer<V>>>,
    backplane: Option<Arc<dyn Backplane>>,
    distributed_locker: Option<Arc<dyn DistributedLocker>>,
    circuit_l2: CircuitBreaker,
    circuit_backplane: CircuitBreaker,
    plugins: PluginHost,
    recovery: Option<Arc<AutoRecoveryService>>,
    default_options_provider: Option<Arc<dyn DefaultEntryOptionsProvider>>,
    ignore_incoming_backplane: bool,
    distributed_wire_version: Arc<str>,
    distributed_key_modifier_mode: KeyModifierMode,
}

impl<V: Clone + Send + Sync + 'static> Clone for Cache<V> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

/// The classification of an L1 read after tag markers are applied.
enum L1Read<V> {
    Fresh(Entry<V>),
    Stale(Entry<V>),
    Miss,
}

impl<V: Clone + Send + Sync + 'static> Cache<V> {
    /// Starts building a cache.
    pub fn builder() -> CacheBuilder<V> {
        CacheBuilder::new()
    }

    /// Creates a cache with all default settings and the real system clock.
    #[must_use]
    pub fn new() -> Self {
        CacheBuilder::new().build()
    }

    /// The cache's name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// The event hub; subscribe to observe hits, misses, fail-safe activations…
    #[must_use]
    pub fn events(&self) -> &Events {
        &self.inner.events
    }

    /// A clone of the cache's default entry options, ready to tweak per call.
    #[must_use]
    pub fn entry_options(&self) -> EntryOptions {
        self.inner.default_options.clone()
    }

    // --------------------------------------------------------------- get_or_set

    /// Returns the cached value for `key`, or runs `factory` to produce it.
    ///
    /// Uses the cache's default options, no tags and no fail-safe default. See
    /// [`get_or_set_full`](Self::get_or_set_full) for the complete form.
    pub async fn get_or_set<F, Fut>(&self, key: impl AsRef<str>, factory: F) -> Result<V>
    where
        F: FnOnce(FactoryContext<V>) -> Fut + Send + 'static,
        Fut: Future<Output = std::result::Result<FactoryProduct<V>, FactoryError>> + Send + 'static,
    {
        self.get_or_set_full(key, factory, None, Box::from([]), MaybeValue::none())
            .await
    }

    /// Like [`get_or_set`](Self::get_or_set) but with explicit per-call options.
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

    /// Returns the cached value for `key`, or stores and returns the constant
    /// `value` if it is absent.
    ///
    /// The constant-value form of [`get_or_set`](Self::get_or_set): it goes
    /// through the same L1 → L2 → single-flight flow, but the "factory" simply
    /// yields `value`.
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

    /// The full `get_or_set`: per-call `options`, `tags` for the produced entry,
    /// and a `fail_safe_default` served as a last resort when the factory fails
    /// and no stale value exists.
    #[tracing::instrument(
        level = "debug",
        name = "amalgam.get_or_set",
        skip_all,
        fields(cache = %self.inner.name, key = key.as_ref())
    )]
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
        let opts = self.resolve_options(key.as_ref(), options);
        let full_key = self.full_key(key.as_ref());
        let now = self.inner.clock.now();

        let mut stale_entry: Option<Entry<V>> = None;

        // 1. Hot path: a fresh L1 entry short-circuits everything.
        if !opts.skip_memory_read() {
            match self.read_l1(&full_key, now).await {
                L1Read::Fresh(entry) => {
                    if entry.should_eager_refresh(now) {
                        // Hand the factory to a non-blocking background refresh and
                        // return the still-fresh value immediately.
                        self.spawn_eager_refresh(
                            full_key.clone(),
                            opts.clone(),
                            entry.clone(),
                            factory,
                        );
                    }
                    self.emit(CacheEvent::Hit {
                        key: full_key,
                        stale: false,
                    });
                    return Ok(entry.value_cloned());
                }
                L1Read::Stale(entry) => stale_entry = Some(entry),
                L1Read::Miss => {}
            }
        }

        // 2. Single-flight: acquire the per-key lock.
        let guard = match self
            .acquire_lock(&full_key, &opts, stale_entry.as_ref())
            .await
        {
            LockOutcome::Acquired(guard) => guard,
            LockOutcome::ServedStale(value) => return Ok(value),
        };

        // 3. Re-check L1 now we hold the lock: another flight may have populated it.
        let now = self.inner.clock.now();
        if !opts.skip_memory_read() {
            match self.read_l1(&full_key, now).await {
                L1Read::Fresh(entry) => {
                    self.emit(CacheEvent::Hit {
                        key: full_key,
                        stale: false,
                    });
                    return Ok(entry.value_cloned());
                }
                L1Read::Stale(entry) => stale_entry = Some(entry),
                L1Read::Miss => {}
            }
        }

        // 3b. L2 read-through: a fresh L2 entry populates L1 and returns; a stale
        // one becomes a (possibly newer) fail-safe fallback.
        if self.inner.distributed.is_some()
            && !opts.skip_distributed_read()
            && !(stale_entry.is_some() && opts.skip_distributed_read_when_stale())
        {
            let now = self.inner.clock.now();
            let l2_has_fallback = stale_entry.is_some() || fail_safe_default.has_value();
            if let Some(entry) = self
                .read_l2_guarded(&full_key, now, &opts, l2_has_fallback)
                .await?
            {
                match self.inner.tags.evaluate(
                    entry.meta().created(),
                    entry.meta().tags(),
                    self.inner.remove_by_tag_behavior,
                ) {
                    TagVerdict::Remove => {
                        self.remove_l2_guarded(&full_key).await;
                    }
                    verdict => {
                        if !opts.skip_memory_write() {
                            self.inner
                                .memory
                                .insert(Arc::clone(&full_key), entry.clone())
                                .await;
                        }
                        let fresh =
                            matches!(verdict, TagVerdict::Valid) && entry.freshness(now).is_fresh();
                        if fresh {
                            self.emit(CacheEvent::Hit {
                                key: full_key,
                                stale: false,
                            });
                            return Ok(entry.value_cloned());
                        }
                        stale_entry = Some(newer_of(stale_entry, entry));
                    }
                }
            }
        }

        // 4. Run the factory under soft/hard timeout, with fail-safe behind it.
        let stale_info = stale_entry.as_ref().map(stale_info_of);
        let ctx = FactoryContext::new(full_key.clone(), opts.clone(), tags, stale_info);
        let has_fallback = stale_entry.is_some() || fail_safe_default.has_value();
        let factory_timeout = opts.appropriate_factory_timeout(has_fallback);
        let allow_bg = opts.allow_timed_out_factory_background_completion();

        match run_factory(factory, ctx, factory_timeout, allow_bg).await {
            FactoryRun::Produced(Ok(product)) => {
                let value = self.store_product(&full_key, product).await;
                drop(guard);
                Ok(value)
            }
            FactoryRun::Produced(Err(factory_err)) => {
                tracing::warn!(
                    cache = %self.inner.name,
                    key = %full_key,
                    error = factory_err.message(),
                    "factory failed"
                );
                self.emit(CacheEvent::FactoryError {
                    key: full_key.clone(),
                    message: factory_err.message().to_owned(),
                });
                let now = self.inner.clock.now();
                match self
                    .try_serve_fallback(
                        &full_key,
                        &opts,
                        now,
                        stale_entry.as_ref(),
                        &fail_safe_default,
                    )
                    .await
                {
                    Some(value) => Ok(value),
                    None => Err(factory_err.into()),
                }
            }
            FactoryRun::TimedOut(handle) => {
                self.emit(CacheEvent::FactorySyntheticTimeout {
                    key: full_key.clone(),
                });
                if let Some(handle) = handle {
                    self.spawn_background_completion(full_key.clone(), handle, guard);
                }
                let now = self.inner.clock.now();
                match self
                    .try_serve_fallback(
                        &full_key,
                        &opts,
                        now,
                        stale_entry.as_ref(),
                        &fail_safe_default,
                    )
                    .await
                {
                    Some(value) => Ok(value),
                    None => Err(Error::FactoryTimeout {
                        elapsed: factory_timeout.as_duration().unwrap_or(Duration::ZERO),
                    }),
                }
            }
        }
    }

    // ------------------------------------------------------------- simple ops

    /// Writes `value` with the cache's default options and no tags.
    pub async fn set(&self, key: impl AsRef<str>, value: V) {
        self.set_full(key, value, None, Box::from([])).await;
    }

    /// Writes `value` with explicit options and tags.
    pub async fn set_full(
        &self,
        key: impl AsRef<str>,
        value: V,
        options: Option<EntryOptions>,
        tags: Box<[Tag]>,
    ) {
        let opts = self.resolve_options(key.as_ref(), options);
        let full_key = self.full_key(key.as_ref());
        let now = self.inner.clock.now();
        let entry = Entry::fresh(value, &opts, now, tags, None, None);
        self.write_entry(&full_key, &entry, &opts).await;
        self.emit(CacheEvent::Set { key: full_key });
    }

    /// Reads a value without ever running a factory. A miss (or a stale entry,
    /// unless `allow_stale_on_read_only` is set) yields [`MaybeValue::none`].
    pub async fn try_get(
        &self,
        key: impl AsRef<str>,
        options: Option<EntryOptions>,
    ) -> MaybeValue<V> {
        let opts = self.resolve_options(key.as_ref(), options);
        let full_key = self.full_key(key.as_ref());
        if opts.skip_memory_read() {
            self.emit(CacheEvent::Miss { key: full_key });
            return MaybeValue::none();
        }
        let now = self.inner.clock.now();
        match self.read_l1(&full_key, now).await {
            L1Read::Fresh(entry) => {
                self.emit(CacheEvent::Hit {
                    key: full_key,
                    stale: false,
                });
                MaybeValue::from_value(entry.value_cloned())
            }
            L1Read::Stale(entry) if opts.allow_stale_on_read_only() => {
                self.emit(CacheEvent::Hit {
                    key: full_key,
                    stale: true,
                });
                MaybeValue::from_value(entry.value_cloned())
            }
            _ => {
                self.emit(CacheEvent::Miss { key: full_key });
                MaybeValue::none()
            }
        }
    }

    /// Reads a value or returns `default` (never runs a factory).
    pub async fn get_or_default(
        &self,
        key: impl AsRef<str>,
        default: V,
        options: Option<EntryOptions>,
    ) -> V {
        self.try_get(key, options).await.value_or(default)
    }

    /// Removes an entry from L1 and L2 and tells peers to evict it.
    pub async fn remove(&self, key: impl AsRef<str>) {
        let full_key = self.full_key(key.as_ref());
        let opts = self.inner.default_options.clone();
        self.inner.memory.remove(&full_key).await;
        self.remove_l2_guarded(&full_key).await;
        self.publish_guarded(BackplaneAction::Remove, &full_key, &opts)
            .await;
        self.emit(CacheEvent::Remove { key: full_key });
    }

    /// Logically expires an entry: it is no longer fresh, but fail-safe can still
    /// serve it as a stale fallback. Propagated to L2 and peers.
    pub async fn expire(&self, key: impl AsRef<str>) {
        let full_key = self.full_key(key.as_ref());
        let now = self.inner.clock.now();
        let opts = self.inner.default_options.clone();
        if let Some(entry) = self.inner.memory.get(&full_key).await {
            let expired = entry.with_logical_expiration(now);
            self.inner
                .memory
                .insert(Arc::clone(&full_key), expired.clone())
                .await;
            self.write_l2_guarded(&full_key, &expired, &opts).await;
        }
        self.publish_guarded(BackplaneAction::Expire, &full_key, &opts)
            .await;
        self.emit(CacheEvent::Expire { key: full_key });
    }

    /// Invalidates every entry carrying `tag` (lazily — entries are dropped on
    /// their next read). No-op for a blank tag.
    pub async fn remove_by_tag(&self, tag: impl AsRef<str>) {
        if let Some(t) = Tag::new(tag.as_ref()) {
            let now = self.inner.clock.now();
            self.inner.tags.mark_tag(t, now);
            let marker: Arc<str> = Arc::from(format!("{TAG_MARKER_PREFIX}{}", tag.as_ref()));
            self.publish_marker(marker, now).await;
            self.emit(CacheEvent::RemoveByTag {
                tag: tag.as_ref().to_owned(),
            });
        }
    }

    /// Invalidates every entry carrying any of `tags`.
    pub async fn remove_by_tags<I, S>(&self, tags: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        for tag in tags {
            self.remove_by_tag(tag).await;
        }
    }

    /// Clears the whole cache.
    ///
    /// With `allow_fail_safe = true` (the FusionCache default) every entry is
    /// logically expired so fail-safe can still serve stale values; with `false`
    /// they are hard-removed.
    pub async fn clear(&self, allow_fail_safe: bool) {
        let now = self.inner.clock.now();
        let marker: Arc<str> = if allow_fail_safe {
            self.inner.tags.mark_clear_expire(now);
            Arc::from(CLEAR_EXPIRE_KEY)
        } else {
            self.inner.tags.mark_clear_remove(now);
            self.inner.memory.invalidate_all();
            Arc::from(CLEAR_REMOVE_KEY)
        };
        self.publish_marker(marker, now).await;
        self.emit(CacheEvent::Clear);
    }

    /// Runs the L1 backend's pending maintenance (eviction, expiry). Primarily
    /// for deterministic tests.
    pub async fn run_pending_tasks(&self) {
        self.inner.memory.run_pending_tasks().await;
    }

    // ----------------------------------------------------------------- internals

    fn full_key(&self, key: &str) -> Arc<str> {
        match &self.inner.key_prefix {
            Some(prefix) => Arc::from(format!("{prefix}{key}")),
            None => Arc::from(key),
        }
    }

    fn emit(&self, event: CacheEvent) {
        if !self.inner.plugins.is_empty() {
            self.inner.plugins.notify(&event);
        }
        self.inner.events.emit(event);
    }

    /// Resolves the options for an operation: explicit per-call options, else a
    /// dynamic provider's options for this key, else the static defaults.
    fn resolve_options(&self, key: &str, options: Option<EntryOptions>) -> EntryOptions {
        if let Some(options) = options {
            return options;
        }
        if let Some(provider) = &self.inner.default_options_provider
            && let Some(options) = provider.options_for(key)
        {
            return options;
        }
        self.inner.default_options.clone()
    }

    /// Reads L1, applies tag markers, and classifies the result.
    async fn read_l1(&self, full_key: &str, now: Timestamp) -> L1Read<V> {
        let Some(entry) = self.inner.memory.get(full_key).await else {
            return L1Read::Miss;
        };
        match self.inner.tags.evaluate(
            entry.meta().created(),
            entry.meta().tags(),
            self.inner.remove_by_tag_behavior,
        ) {
            TagVerdict::Remove => {
                self.inner.memory.remove(full_key).await;
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
        }
    }

    async fn acquire_lock(
        &self,
        full_key: &str,
        opts: &EntryOptions,
        stale_entry: Option<&Entry<V>>,
    ) -> LockOutcome<V> {
        let local = match opts.lock_timeout() {
            Timeout::Infinite => self.inner.locks.lock(full_key).await,
            Timeout::After(d) => {
                match tokio::time::timeout(d, self.inner.locks.lock(full_key)).await {
                    Ok(guard) => guard,
                    Err(_) => {
                        // Lock timed out: with fail-safe and a stale value, return it
                        // rather than queue behind the lock holder.
                        if opts.is_fail_safe_enabled()
                            && let Some(stale) = stale_entry
                        {
                            self.emit(CacheEvent::Hit {
                                key: Arc::from(full_key),
                                stale: true,
                            });
                            return LockOutcome::ServedStale(stale.value_cloned());
                        }
                        // Otherwise the lock is best-effort: wait for it.
                        self.inner.locks.lock(full_key).await
                    }
                }
            }
        };
        let distributed = self.acquire_distributed_lock(full_key, opts).await;
        LockOutcome::Acquired(LockGuard {
            _local: local,
            _distributed: distributed,
        })
    }

    /// Acquires the cross-node distributed lock (if a locker is configured and not
    /// skipped). Best-effort: a timeout or error proceeds without it.
    async fn acquire_distributed_lock(
        &self,
        full_key: &str,
        opts: &EntryOptions,
    ) -> Option<DistributedReleaseGuard> {
        if opts.skip_distributed_locker() {
            return None;
        }
        let locker = self.inner.distributed_locker.as_ref()?;
        let lock_key = format!("amalgam:lock:{full_key}");
        match locker
            .acquire(&lock_key, opts.physical_ttl(), opts.lock_timeout())
            .await
        {
            Ok(Some(token)) => Some(DistributedReleaseGuard {
                locker: Arc::clone(locker),
                key: Arc::from(lock_key),
                token,
            }),
            _ => None,
        }
    }

    /// Stores a freshly-produced (or `NotModified`-reused) value as a fresh entry,
    /// fires the appropriate events, and returns the value.
    async fn store_product(&self, full_key: &Arc<str>, product: FactoryProduct<V>) -> V {
        let now = self.inner.clock.now();
        let reused = product.reused_stale;
        let value = product.value.clone();
        let opts = product.options;
        let entry = Entry::fresh(
            product.value,
            &opts,
            now,
            product.tags,
            product.etag,
            product.last_modified,
        );
        self.write_entry(full_key, &entry, &opts).await;
        if !reused {
            self.emit(CacheEvent::FactorySuccess {
                key: Arc::clone(full_key),
            });
        }
        self.emit(CacheEvent::Set {
            key: Arc::clone(full_key),
        });
        value
    }

    /// Write-through: stores `entry` into L1 and (if configured) L2, then
    /// publishes a backplane `Set` so peers drop their local copy.
    async fn write_entry(&self, full_key: &Arc<str>, entry: &Entry<V>, opts: &EntryOptions) {
        if !opts.skip_memory_write() {
            self.inner
                .memory
                .insert(Arc::clone(full_key), entry.clone())
                .await;
        }
        if !opts.skip_distributed_write() {
            self.write_l2_guarded(full_key, entry, opts).await;
        }
        if !opts.skip_backplane_notifications() {
            self.publish_guarded(BackplaneAction::Set, full_key, opts)
                .await;
        }
    }

    /// L2 write, gated by the circuit breaker, with event emission and
    /// auto-recovery enqueue on failure.
    async fn write_l2_guarded(&self, full_key: &Arc<str>, entry: &Entry<V>, opts: &EntryOptions) {
        if self.inner.distributed.is_none() {
            return;
        }
        let now = self.inner.clock.now();
        if !self.inner.circuit_l2.is_closed(now) {
            self.enqueue_recovery(full_key, RecoveryAction::Set, now);
            return;
        }
        let ttl = opts.distributed_physical_ttl();
        if opts.allow_background_distributed_operations() {
            // Fire-and-forget: don't make the caller wait on L2.
            let this = self.clone();
            let full_key = Arc::clone(full_key);
            let entry = entry.clone();
            tokio::spawn(async move {
                this.do_l2_write(&full_key, &entry, ttl).await;
            });
        } else {
            self.do_l2_write(full_key, entry, ttl).await;
        }
    }

    async fn do_l2_write(&self, full_key: &Arc<str>, entry: &Entry<V>, ttl: Duration) {
        match self.inner.l2_write(full_key, entry, ttl).await {
            Ok(()) => self.close_circuit_l2(),
            Err(err) => {
                self.on_l2_error(full_key, &err);
                self.enqueue_recovery(full_key, RecoveryAction::Set, self.inner.clock.now());
            }
        }
    }

    /// L2 read, gated by the circuit breaker and the appropriate distributed
    /// timeout (soft when fail-safe + a fallback exists, otherwise hard).
    /// Returns `Err` only when `rethrow_distributed_exceptions` is set.
    async fn read_l2_guarded(
        &self,
        full_key: &Arc<str>,
        now: Timestamp,
        opts: &EntryOptions,
        has_fallback: bool,
    ) -> Result<Option<Entry<V>>> {
        if self.inner.distributed.is_none() || !self.inner.circuit_l2.is_closed(now) {
            return Ok(None);
        }
        let read = self.inner.l2_read(full_key, now);
        let timeout = opts.appropriate_distributed_timeout(has_fallback);
        let result = match timeout {
            Timeout::After(d) => match tokio::time::timeout(d, read).await {
                Ok(r) => r,
                Err(_) => {
                    // A *soft* distributed timeout (fail-safe on + a fallback
                    // exists) is an expected, benign bail-out to the fallback: it
                    // must not mark L2 unhealthy. Only a *hard* timeout is a
                    // genuine L2 failure that trips the circuit breaker.
                    if opts.is_fail_safe_enabled()
                        && has_fallback
                        && timeout == opts.distributed_soft_timeout()
                    {
                        return Ok(None);
                    }
                    Err(Error::Distributed("l2 read timed out".to_owned()))
                }
            },
            Timeout::Infinite => read.await,
        };
        match result {
            Ok(entry) => {
                self.close_circuit_l2();
                Ok(entry)
            }
            Err(err) => {
                self.on_l2_error(full_key, &err);
                // Serialization/deserialization failures honour
                // `rethrow_serialization_exceptions`; transport failures honour
                // `rethrow_distributed_exceptions`. Otherwise an L2 hiccup is a
                // miss, not an error.
                let rethrow = match err {
                    Error::Serialization(_) | Error::Deserialization(_) => {
                        opts.rethrow_serialization_exceptions()
                    }
                    _ => opts.rethrow_distributed_exceptions(),
                };
                if rethrow { Err(err) } else { Ok(None) }
            }
        }
    }

    /// L2 remove, gated by the circuit breaker, with auto-recovery on failure.
    async fn remove_l2_guarded(&self, full_key: &Arc<str>) {
        if self.inner.distributed.is_none() {
            return;
        }
        let now = self.inner.clock.now();
        if !self.inner.circuit_l2.is_closed(now) {
            self.enqueue_recovery(full_key, RecoveryAction::Remove, now);
            return;
        }
        match self.inner.l2_remove(full_key).await {
            Ok(()) => self.close_circuit_l2(),
            Err(err) => {
                self.on_l2_error(full_key, &err);
                self.enqueue_recovery(full_key, RecoveryAction::Remove, now);
            }
        }
    }

    fn on_l2_error(&self, full_key: &Arc<str>, err: &Error) {
        match err {
            Error::Serialization(message) => self.emit(CacheEvent::SerializationError {
                key: Arc::clone(full_key),
                message: message.clone(),
            }),
            Error::Deserialization(message) => self.emit(CacheEvent::DeserializationError {
                key: Arc::clone(full_key),
                message: message.clone(),
            }),
            _ => {}
        }
        if self.inner.circuit_l2.trip(self.inner.clock.now()) {
            self.emit(CacheEvent::CircuitBreakerChange {
                component: CircuitComponent::Distributed,
                closed: false,
            });
        }
    }

    fn close_circuit_l2(&self) {
        if self.inner.circuit_l2.close() {
            self.emit(CacheEvent::CircuitBreakerChange {
                component: CircuitComponent::Distributed,
                closed: true,
            });
        }
    }

    fn close_circuit_backplane(&self) {
        if self.inner.circuit_backplane.close() {
            self.emit(CacheEvent::CircuitBreakerChange {
                component: CircuitComponent::Backplane,
                closed: true,
            });
        }
    }

    fn enqueue_recovery(&self, full_key: &Arc<str>, action: RecoveryAction, now: Timestamp) {
        if let Some(recovery) = &self.inner.recovery {
            recovery.enqueue(RecoveryItem {
                key: Arc::clone(full_key),
                action,
                timestamp: now,
                expires_at: now.saturating_add(RECOVERY_ITEM_TTL),
                remaining_retries: None,
            });
        }
    }

    /// Publishes a backplane notification, gated by the circuit breaker. Honours
    /// `allow_background_backplane_operations` (fire-and-forget vs awaited).
    async fn publish_guarded(
        &self,
        action: BackplaneAction,
        full_key: &Arc<str>,
        opts: &EntryOptions,
    ) {
        if self.inner.backplane.is_none() {
            return;
        }
        let now = self.inner.clock.now();
        if !self.inner.circuit_backplane.is_closed(now) {
            self.enqueue_recovery(full_key, recovery_action_of(action), now);
            return;
        }
        if opts.allow_background_backplane_operations() {
            let this = self.clone();
            let key = Arc::clone(full_key);
            tokio::spawn(async move {
                this.do_publish(action, &key).await;
            });
        } else {
            self.do_publish(action, full_key).await;
        }
    }

    async fn do_publish(&self, action: BackplaneAction, full_key: &Arc<str>) {
        let now = self.inner.clock.now();
        match self.inner.backplane_send(action, full_key, now).await {
            Ok(()) => {
                self.close_circuit_backplane();
                self.emit(CacheEvent::MessagePublished {
                    key: Arc::clone(full_key),
                });
            }
            Err(_) => {
                if self.inner.circuit_backplane.trip(now) {
                    self.emit(CacheEvent::CircuitBreakerChange {
                        component: CircuitComponent::Backplane,
                        closed: false,
                    });
                }
                self.enqueue_recovery(full_key, recovery_action_of(action), now);
            }
        }
    }

    /// Publishes a reserved-key tag/clear marker so peers update their tag
    /// registry (best-effort, awaited).
    async fn publish_marker(&self, key: Arc<str>, now: Timestamp) {
        if self.inner.backplane.is_none() {
            return;
        }
        if let Ok(()) = self
            .inner
            .backplane_send(BackplaneAction::Set, &key, now)
            .await
        {
            self.emit(CacheEvent::MessagePublished { key });
        }
    }

    /// Applies an incoming backplane notification to the local L1 / tag registry.
    async fn apply_backplane(&self, message: BackplaneMessage) {
        if self.inner.ignore_incoming_backplane {
            return;
        }
        self.emit(CacheEvent::MessageReceived {
            key: Arc::clone(&message.key),
        });
        // A received message proves the backplane is healthy.
        self.close_circuit_backplane();

        // Tag / clear markers ride on `Set` messages with reserved keys.
        if message.action == BackplaneAction::Set {
            if let Some(tag) = message.key.strip_prefix(TAG_MARKER_PREFIX) {
                if let Some(tag) = Tag::new(tag) {
                    self.inner.tags.mark_tag(tag, message.timestamp);
                }
                return;
            }
            if &*message.key == CLEAR_EXPIRE_KEY {
                self.inner.tags.mark_clear_expire(message.timestamp);
                return;
            }
            if &*message.key == CLEAR_REMOVE_KEY {
                self.inner.tags.mark_clear_remove(message.timestamp);
                self.inner.memory.invalidate_all();
                return;
            }
        }

        match message.action {
            BackplaneAction::Remove => {
                self.inner.memory.remove(&message.key).await;
            }
            BackplaneAction::Expire => {
                if let Some(entry) = self.inner.memory.get(&message.key).await {
                    let expired = entry.with_logical_expiration(message.timestamp);
                    self.inner
                        .memory
                        .insert(Arc::clone(&message.key), expired)
                        .await;
                }
            }
            BackplaneAction::Set => {
                // Drop the local copy; the next read re-pulls the value from L2.
                self.inner.memory.remove(&message.key).await;
            }
        }
    }

    /// Spawns the background listener that applies remote backplane messages.
    /// Requires a tokio runtime; only spawned when a backplane is configured.
    fn spawn_backplane_listener(&self) {
        let Some(backplane) = &self.inner.backplane else {
            return;
        };
        let mut receiver = backplane.subscribe();
        let weak: Weak<CacheInner<V>> = Arc::downgrade(&self.inner);
        let instance_id = Arc::clone(&self.inner.instance_id);
        tokio::spawn(async move {
            loop {
                match receiver.recv().await {
                    Ok(message) => {
                        if message.source_id == instance_id {
                            continue; // ignore our own notifications
                        }
                        match weak.upgrade() {
                            Some(inner) => Cache { inner }.apply_backplane(message).await,
                            None => break, // the cache was dropped
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                }
            }
        });
    }

    /// Attempts to serve a stale value (fail-safe). Returns the served value, or
    /// `None` if fail-safe is disabled or no fallback exists.
    async fn try_serve_fallback(
        &self,
        full_key: &Arc<str>,
        opts: &EntryOptions,
        now: Timestamp,
        stale_entry: Option<&Entry<V>>,
        fail_safe_default: &MaybeValue<V>,
    ) -> Option<V> {
        if !opts.is_fail_safe_enabled() {
            return None;
        }
        if let Some(stale) = stale_entry
            && let Some(throttled) = Entry::throttled(stale, opts, now)
        {
            let value = throttled.value_cloned();
            if !opts.skip_memory_write() {
                self.inner
                    .memory
                    .insert(Arc::clone(full_key), throttled)
                    .await;
            }
            self.emit_fail_safe(full_key);
            return Some(value);
        }
        if let Some(default) = fail_safe_default.value() {
            let value = default.clone();
            if !opts.skip_memory_write() {
                let entry = Entry::from_fail_safe_default(value.clone(), opts, now);
                self.inner.memory.insert(Arc::clone(full_key), entry).await;
            }
            self.emit_fail_safe(full_key);
            return Some(value);
        }
        None
    }

    fn emit_fail_safe(&self, full_key: &Arc<str>) {
        tracing::debug!(
            cache = %self.inner.name,
            key = %full_key,
            "fail-safe activated; serving stale value"
        );
        self.emit(CacheEvent::FailSafeActivate {
            key: Arc::clone(full_key),
        });
        self.emit(CacheEvent::Hit {
            key: Arc::clone(full_key),
            stale: true,
        });
    }

    fn spawn_background_completion(
        &self,
        full_key: Arc<str>,
        handle: JoinHandle<std::result::Result<FactoryProduct<V>, FactoryError>>,
        guard: LockGuard,
    ) {
        let this = self.clone();
        tokio::spawn(async move {
            // Hold the single-flight lock until the background factory resolves.
            let _guard = guard;
            match handle.await {
                Ok(Ok(product)) => {
                    this.store_product(&full_key, product).await;
                    this.emit(CacheEvent::BackgroundFactorySuccess { key: full_key });
                }
                Ok(Err(factory_err)) => {
                    // Per FusionCache, a background failure does NOT re-activate
                    // fail-safe; the throttled stale value already returned stands.
                    this.emit(CacheEvent::BackgroundFactoryError {
                        key: full_key,
                        message: factory_err.message().to_owned(),
                    });
                }
                Err(_) => {
                    this.emit(CacheEvent::BackgroundFactoryError {
                        key: full_key,
                        message: "factory task panicked".to_owned(),
                    });
                }
            }
        });
    }

    fn spawn_eager_refresh<F, Fut>(
        &self,
        full_key: Arc<str>,
        opts: EntryOptions,
        current: Entry<V>,
        factory: F,
    ) where
        F: FnOnce(FactoryContext<V>) -> Fut + Send + 'static,
        Fut: Future<Output = std::result::Result<FactoryProduct<V>, FactoryError>> + Send + 'static,
    {
        // Non-blocking: if another caller already holds the key, skip silently.
        let Some(guard) = self.inner.locks.try_lock(&full_key) else {
            return;
        };
        self.emit(CacheEvent::EagerRefresh {
            key: Arc::clone(&full_key),
        });
        let this = self.clone();
        tokio::spawn(async move {
            let _guard = guard;
            let ctx = FactoryContext::new(
                Arc::clone(&full_key),
                opts,
                current.meta().tags().to_vec().into_boxed_slice(),
                Some(stale_info_of(&current)),
            );
            match factory(ctx).await {
                Ok(product) => {
                    this.store_product(&full_key, product).await;
                    this.emit(CacheEvent::BackgroundFactorySuccess { key: full_key });
                }
                Err(factory_err) => {
                    this.emit(CacheEvent::BackgroundFactoryError {
                        key: full_key,
                        message: factory_err.message().to_owned(),
                    });
                }
            }
        });
    }
}

impl<V: Clone + Send + Sync + 'static> Default for Cache<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V: Clone + Send + Sync + 'static> CacheInner<V> {
    /// The L2 storage key (wire-version-prefixed so cache versions can share L2).
    fn l2_key(&self, full_key: &str) -> String {
        match self.distributed_key_modifier_mode {
            KeyModifierMode::Prefix => format!("{}:{}", self.distributed_wire_version, full_key),
            KeyModifierMode::Suffix => format!("{}:{}", full_key, self.distributed_wire_version),
            KeyModifierMode::None => full_key.to_owned(),
        }
    }

    /// Serializes and writes an entry to L2 with the given physical TTL. `Err` on
    /// serialize/backend failure.
    async fn l2_write(&self, full_key: &str, entry: &Entry<V>, ttl: Duration) -> Result<()> {
        let (Some(l2), Some(serializer)) = (&self.distributed, &self.serializer) else {
            return Ok(());
        };
        let dist = DistributedEntry::from_entry(entry);
        let bytes = serializer.serialize(&dist)?;
        l2.set(&self.l2_key(full_key), bytes, Some(ttl)).await
    }

    /// Reads and deserializes an entry from L2, rehydrated relative to `now`.
    async fn l2_read(&self, full_key: &str, now: Timestamp) -> Result<Option<Entry<V>>> {
        let (Some(l2), Some(serializer)) = (&self.distributed, &self.serializer) else {
            return Ok(None);
        };
        let Some(bytes) = l2.get(&self.l2_key(full_key)).await? else {
            return Ok(None);
        };
        let dist = serializer.deserialize(&bytes)?;
        Ok(Some(dist.into_entry(now)))
    }

    /// Removes an entry from L2.
    async fn l2_remove(&self, full_key: &str) -> Result<()> {
        if let Some(l2) = &self.distributed {
            l2.remove(&self.l2_key(full_key)).await
        } else {
            Ok(())
        }
    }

    /// Publishes a backplane message (awaited). No-op without a backplane.
    async fn backplane_send(
        &self,
        action: BackplaneAction,
        full_key: &str,
        ts: Timestamp,
    ) -> Result<()> {
        if let Some(backplane) = &self.backplane {
            backplane
                .publish(BackplaneMessage {
                    source_id: Arc::clone(&self.instance_id),
                    timestamp: ts,
                    action,
                    key: Arc::from(full_key),
                })
                .await
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl<V: Clone + Send + Sync + 'static> RecoveryExecutor for CacheInner<V> {
    async fn replay(&self, item: &RecoveryItem) -> Result<()> {
        match item.action {
            RecoveryAction::Set => {
                if let Some(entry) = self.memory.get(&item.key).await {
                    let ttl = entry.backend_ttl();
                    self.l2_write(&item.key, &entry, ttl).await?;
                }
                self.backplane_send(BackplaneAction::Set, &item.key, item.timestamp)
                    .await?;
            }
            RecoveryAction::Remove => {
                self.l2_remove(&item.key).await?;
                self.backplane_send(BackplaneAction::Remove, &item.key, item.timestamp)
                    .await?;
            }
            RecoveryAction::Expire => {
                self.backplane_send(BackplaneAction::Expire, &item.key, item.timestamp)
                    .await?;
            }
        }
        Ok(())
    }
}

/// Reserved key prefix for tag-invalidation markers propagated over the backplane.
const TAG_MARKER_PREFIX: &str = "__amalgam:t:";
/// Reserved key for a "clear (expire all)" marker.
const CLEAR_EXPIRE_KEY: &str = "__amalgam:clear:expire";
/// Reserved key for a "clear (remove all)" marker.
const CLEAR_REMOVE_KEY: &str = "__amalgam:clear:remove";
/// How long a queued auto-recovery item remains eligible for replay.
const RECOVERY_ITEM_TTL: Duration = Duration::from_secs(600);

/// Maps a backplane action to the equivalent recovery action.
fn recovery_action_of(action: BackplaneAction) -> RecoveryAction {
    match action {
        BackplaneAction::Set => RecoveryAction::Set,
        BackplaneAction::Remove => RecoveryAction::Remove,
        BackplaneAction::Expire => RecoveryAction::Expire,
    }
}

/// The combined single-flight guard: the local lock plus an optional cross-node
/// distributed lock. Dropping it releases both.
struct LockGuard {
    _local: KeyGuard,
    _distributed: Option<DistributedReleaseGuard>,
}

/// Releases a held distributed lock when dropped (best-effort, in the background).
struct DistributedReleaseGuard {
    locker: Arc<dyn DistributedLocker>,
    key: Arc<str>,
    token: String,
}

impl Drop for DistributedReleaseGuard {
    fn drop(&mut self) {
        let locker = Arc::clone(&self.locker);
        let key = Arc::clone(&self.key);
        let token = std::mem::take(&mut self.token);
        tokio::spawn(async move {
            let _ = locker.release(&key, &token).await;
        });
    }
}

/// The result of trying to acquire the single-flight lock.
enum LockOutcome<V> {
    /// The lock was acquired; proceed to read L2 / run the factory.
    Acquired(LockGuard),
    /// The lock timed out but a stale value was served instead.
    ServedStale(V),
}

/// The result of running a factory under a (possibly infinite) timeout.
///
/// This is a short-lived local return value (one per factory call), never stored
/// in bulk, so the size gap between variants is irrelevant — boxing the common
/// `Produced` path would only add a hot-path allocation.
#[allow(clippy::large_enum_variant)]
enum FactoryRun<V> {
    /// The factory completed (successfully or with a failure) within the timeout.
    Produced(std::result::Result<FactoryProduct<V>, FactoryError>),
    /// The factory exceeded its timeout. `Some(handle)` when it is still running
    /// in the background (to be completed later); `None` when it was aborted.
    TimedOut(Option<JoinHandle<std::result::Result<FactoryProduct<V>, FactoryError>>>),
}

/// Runs `factory` against `ctx`, enforcing `timeout`.
///
/// For a finite timeout the factory is spawned so it can outlive the timeout and
/// finish in the background (when `allow_background` is set); for an infinite
/// timeout it runs inline.
async fn run_factory<V, F, Fut>(
    factory: F,
    ctx: FactoryContext<V>,
    timeout: Timeout,
    allow_background: bool,
) -> FactoryRun<V>
where
    V: Send + 'static,
    F: FnOnce(FactoryContext<V>) -> Fut + Send + 'static,
    Fut: Future<Output = std::result::Result<FactoryProduct<V>, FactoryError>> + Send + 'static,
{
    match timeout {
        Timeout::Infinite => FactoryRun::Produced(factory(ctx).await),
        Timeout::After(duration) => {
            let mut handle = tokio::spawn(factory(ctx));
            tokio::select! {
                joined = &mut handle => match joined {
                    Ok(result) => FactoryRun::Produced(result),
                    Err(_) => FactoryRun::Produced(Err(FactoryError::new("factory task panicked"))),
                },
                () = tokio::time::sleep(duration) => {
                    if allow_background {
                        FactoryRun::TimedOut(Some(handle))
                    } else {
                        handle.abort();
                        FactoryRun::TimedOut(None)
                    }
                }
            }
        }
    }
}

/// Snapshots a stale entry into the conditional-refresh information handed to a
/// factory.
fn stale_info_of<V: Clone>(entry: &Entry<V>) -> StaleInfo<V> {
    StaleInfo {
        value: entry.value_cloned(),
        etag: entry.meta().etag().map(str::to_owned),
        last_modified: entry.meta().last_modified(),
        tags: entry.meta().tags().to_vec().into_boxed_slice(),
    }
}

/// Builder for a [`Cache`].
///
/// ```
/// use amalgam::{Cache, EntryOptions};
/// use std::time::Duration;
///
/// let cache: Cache<String> = Cache::builder()
///     .name("users")
///     .key_prefix("u:")
///     .default_options(EntryOptions::new(Duration::from_secs(60)))
///     .build();
/// ```
#[must_use = "a builder does nothing until `.build()` is called"]
pub struct CacheBuilder<V> {
    name: Option<Arc<str>>,
    instance_id: Option<Arc<str>>,
    key_prefix: Option<Arc<str>>,
    default_options: EntryOptions,
    clock: Option<Arc<dyn Clock>>,
    max_capacity: Option<u64>,
    lock_shards: usize,
    remove_by_tag_behavior: RemoveByTagBehavior,
    events_capacity: usize,
    distributed: Option<Arc<dyn DistributedCache>>,
    serializer: Option<Arc<dyn DistributedSerializer<V>>>,
    backplane: Option<Arc<dyn Backplane>>,
    distributed_locker: Option<Arc<dyn DistributedLocker>>,
    plugins: Vec<Arc<dyn Plugin>>,
    distributed_circuit_breaker: Duration,
    backplane_circuit_breaker: Duration,
    recovery_config: RecoveryConfig,
    default_options_provider: Option<Arc<dyn DefaultEntryOptionsProvider>>,
    ignore_incoming_backplane: bool,
    distributed_wire_version: Arc<str>,
    distributed_key_modifier_mode: KeyModifierMode,
    _marker: std::marker::PhantomData<fn() -> V>,
}

impl<V> CacheBuilder<V> {
    /// Creates a builder with default settings.
    pub fn new() -> Self {
        Self {
            name: None,
            instance_id: None,
            key_prefix: None,
            default_options: EntryOptions::default(),
            clock: None,
            max_capacity: None,
            lock_shards: 1024,
            remove_by_tag_behavior: RemoveByTagBehavior::default(),
            events_capacity: 256,
            distributed: None,
            serializer: None,
            backplane: None,
            distributed_locker: None,
            plugins: Vec::new(),
            distributed_circuit_breaker: Duration::ZERO,
            backplane_circuit_breaker: Duration::ZERO,
            recovery_config: RecoveryConfig::default(),
            default_options_provider: None,
            ignore_incoming_backplane: false,
            distributed_wire_version: Arc::from("v1"),
            distributed_key_modifier_mode: KeyModifierMode::default(),
            _marker: std::marker::PhantomData,
        }
    }

    /// Sets this instance's id (used to ignore its own backplane messages). A
    /// random id is generated if not set.
    pub fn instance_id(mut self, id: impl AsRef<str>) -> Self {
        self.instance_id = Some(Arc::from(id.as_ref()));
        self
    }

    /// Attaches an L2 distributed cache backend. Pair with
    /// [`serializer`](Self::serializer).
    pub fn distributed(mut self, distributed: Arc<dyn DistributedCache>) -> Self {
        self.distributed = Some(distributed);
        self
    }

    /// Sets the serializer used for the L2 wire format (e.g.
    /// [`JsonSerializer`](crate::JsonSerializer)).
    pub fn serializer(mut self, serializer: Arc<dyn DistributedSerializer<V>>) -> Self {
        self.serializer = Some(serializer);
        self
    }

    /// Attaches a backplane for multi-node L1 invalidation.
    ///
    /// Note: when a backplane is configured, [`build`](Self::build) spawns a
    /// listener task and so must be called from within a tokio runtime.
    pub fn backplane(mut self, backplane: Arc<dyn Backplane>) -> Self {
        self.backplane = Some(backplane);
        self
    }

    /// Sets the cache's name (used in events/diagnostics).
    pub fn name(mut self, name: impl AsRef<str>) -> Self {
        self.name = Some(Arc::from(name.as_ref()));
        self
    }

    /// Sets a prefix prepended to every key.
    pub fn key_prefix(mut self, prefix: impl AsRef<str>) -> Self {
        self.key_prefix = Some(Arc::from(prefix.as_ref()));
        self
    }

    /// Sets the default entry options merged into every operation that does not
    /// supply its own.
    pub fn default_options(mut self, options: EntryOptions) -> Self {
        self.default_options = options;
        self
    }

    /// Injects a custom [`Clock`] (e.g. [`ManualClock`](crate::ManualClock) in
    /// tests).
    pub fn clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = Some(clock);
        self
    }

    /// Caps the number of entries held in L1 (`None` = unbounded).
    pub fn max_capacity(mut self, capacity: u64) -> Self {
        self.max_capacity = Some(capacity);
        self
    }

    /// Sets the number of single-flight lock shards (rounded up to a power of two).
    pub fn lock_shards(mut self, shards: usize) -> Self {
        self.lock_shards = shards;
        self
    }

    /// Sets what `remove_by_tag` does to matched entries.
    pub fn remove_by_tag_behavior(mut self, behavior: RemoveByTagBehavior) -> Self {
        self.remove_by_tag_behavior = behavior;
        self
    }

    /// Sets the event channel's buffer capacity.
    pub fn events_capacity(mut self, capacity: usize) -> Self {
        self.events_capacity = capacity;
        self
    }

    /// Attaches a cross-node distributed locker (e.g. Redis-backed) for
    /// cluster-wide single-flight.
    pub fn distributed_locker(mut self, locker: Arc<dyn DistributedLocker>) -> Self {
        self.distributed_locker = Some(locker);
        self
    }

    /// Registers a [`Plugin`] to observe events and lifecycle.
    pub fn plugin(mut self, plugin: Arc<dyn Plugin>) -> Self {
        self.plugins.push(plugin);
        self
    }

    /// Opens the L2 circuit breaker for `duration` after an L2 failure
    /// (`Duration::ZERO` disables it — the default).
    pub fn distributed_circuit_breaker(mut self, duration: Duration) -> Self {
        self.distributed_circuit_breaker = duration;
        self
    }

    /// Opens the backplane circuit breaker for `duration` after a backplane
    /// failure (`Duration::ZERO` disables it — the default).
    pub fn backplane_circuit_breaker(mut self, duration: Duration) -> Self {
        self.backplane_circuit_breaker = duration;
        self
    }

    /// Configures (or disables) auto-recovery of failed L2 / backplane operations.
    pub fn auto_recovery(mut self, config: RecoveryConfig) -> Self {
        self.recovery_config = config;
        self
    }

    /// Sets a provider of dynamic, per-key default options (consulted when a call
    /// supplies no explicit options).
    pub fn default_options_provider(
        mut self,
        provider: Arc<dyn DefaultEntryOptionsProvider>,
    ) -> Self {
        self.default_options_provider = Some(provider);
        self
    }

    /// Drops all incoming backplane notifications (dangerous; for testing).
    pub fn ignore_incoming_backplane(mut self, ignore: bool) -> Self {
        self.ignore_incoming_backplane = ignore;
        self
    }

    /// Sets the L2 wire-format version combined with distributed keys.
    pub fn distributed_wire_version(mut self, version: impl AsRef<str>) -> Self {
        self.distributed_wire_version = Arc::from(version.as_ref());
        self
    }

    /// Sets how the wire-format version is combined with the L2 key (prefix,
    /// suffix, or none).
    pub fn distributed_key_modifier_mode(mut self, mode: KeyModifierMode) -> Self {
        self.distributed_key_modifier_mode = mode;
        self
    }
}

impl<V> Default for CacheBuilder<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V: Clone + Send + Sync + 'static> CacheBuilder<V> {
    /// Builds the cache.
    #[must_use]
    pub fn build(self) -> Cache<V> {
        let clock: Arc<dyn Clock> = self.clock.unwrap_or_else(|| Arc::new(SystemClock));
        // Auto-recovery only matters when there is an L2 or backplane to recover.
        let recovery = if self.recovery_config.enabled
            && (self.distributed.is_some() || self.backplane.is_some())
        {
            Some(AutoRecoveryService::new(
                self.recovery_config,
                Arc::clone(&clock),
            ))
        } else {
            None
        };
        let events = Events::with_capacity(self.events_capacity);
        let inner = Arc::new(CacheInner {
            name: self.name.unwrap_or_else(|| Arc::from("amalgam")),
            instance_id: self.instance_id.unwrap_or_else(generate_instance_id),
            memory: MemoryStore::new(self.max_capacity, events.clone()),
            locks: Arc::new(KeyedLock::new(self.lock_shards)),
            tags: Arc::new(TagRegistry::new()),
            events,
            clock,
            default_options: self.default_options,
            key_prefix: self.key_prefix,
            remove_by_tag_behavior: self.remove_by_tag_behavior,
            distributed: self.distributed,
            serializer: self.serializer,
            backplane: self.backplane,
            distributed_locker: self.distributed_locker,
            circuit_l2: CircuitBreaker::new(self.distributed_circuit_breaker),
            circuit_backplane: CircuitBreaker::new(self.backplane_circuit_breaker),
            plugins: PluginHost::new(self.plugins),
            recovery: recovery.clone(),
            default_options_provider: self.default_options_provider,
            ignore_incoming_backplane: self.ignore_incoming_backplane,
            distributed_wire_version: self.distributed_wire_version,
            distributed_key_modifier_mode: self.distributed_key_modifier_mode,
        });
        // Wire the recovery executor (the cache) as a Weak so it never keeps the
        // cache alive, then start the background drain loop.
        if let Some(recovery) = &recovery {
            let executor: Arc<dyn RecoveryExecutor> =
                Arc::clone(&inner) as Arc<dyn RecoveryExecutor>;
            recovery.set_executor(Arc::downgrade(&executor));
            recovery.spawn();
        }
        let cache = Cache { inner };
        cache.spawn_backplane_listener();
        cache
    }
}

/// Generates a random per-instance id for backplane self-filtering.
fn generate_instance_id() -> Arc<str> {
    Arc::from(format!("amalgam-{:016x}", fastrand::u64(..)))
}

/// Picks the entry with the later creation timestamp (the "newer" fallback).
fn newer_of<V: Clone>(existing: Option<Entry<V>>, candidate: Entry<V>) -> Entry<V> {
    match existing {
        Some(existing) if existing.meta().created() >= candidate.meta().created() => existing,
        _ => candidate,
    }
}
