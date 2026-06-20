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

use crate::backplane::{Backplane, BackplaneAction, BackplaneMessage};
use crate::distributed::{DistributedCache, DistributedEntry, DistributedSerializer};
use crate::entry::Entry;
use crate::error::{Error, FactoryError, Result};
use crate::events::{CacheEvent, Events};
use crate::factory::{FactoryContext, FactoryProduct, StaleInfo};
use crate::locking::{KeyGuard, KeyedLock};
use crate::maybe::MaybeValue;
use crate::memory::MemoryStore;
use crate::options::{EntryOptions, RemoveByTagBehavior};
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

    /// The full `get_or_set`: per-call `options`, `tags` for the produced entry,
    /// and a `fail_safe_default` served as a last resort when the factory fails
    /// and no stale value exists.
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
        let opts = options.unwrap_or_else(|| self.inner.default_options.clone());
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
            if let Some(entry) = self.read_l2(&full_key, now).await {
                match self.inner.tags.evaluate(
                    entry.meta().created(),
                    entry.meta().tags(),
                    self.inner.remove_by_tag_behavior,
                ) {
                    TagVerdict::Remove => {
                        if let Some(l2) = &self.inner.distributed {
                            let _ = l2.remove(&full_key).await;
                        }
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
        let opts = options.unwrap_or_else(|| self.inner.default_options.clone());
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
        let opts = options.unwrap_or_else(|| self.inner.default_options.clone());
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
        self.inner.memory.remove(&full_key).await;
        if let Some(l2) = &self.inner.distributed {
            let _ = l2.remove(&full_key).await;
        }
        self.publish(BackplaneAction::Remove, &full_key).await;
        self.emit(CacheEvent::Remove { key: full_key });
    }

    /// Logically expires an entry: it is no longer fresh, but fail-safe can still
    /// serve it as a stale fallback. Propagated to L2 and peers.
    pub async fn expire(&self, key: impl AsRef<str>) {
        let full_key = self.full_key(key.as_ref());
        let now = self.inner.clock.now();
        if let Some(entry) = self.inner.memory.get(&full_key).await {
            let expired = entry.with_logical_expiration(now);
            self.inner
                .memory
                .insert(Arc::clone(&full_key), expired.clone())
                .await;
            self.write_l2(&full_key, &expired).await;
        }
        self.publish(BackplaneAction::Expire, &full_key).await;
        self.emit(CacheEvent::Expire { key: full_key });
    }

    /// Invalidates every entry carrying `tag` (lazily — entries are dropped on
    /// their next read). No-op for a blank tag.
    pub async fn remove_by_tag(&self, tag: impl AsRef<str>) {
        if let Some(t) = Tag::new(tag.as_ref()) {
            let now = self.inner.clock.now();
            self.inner.tags.mark_tag(t, now);
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
        if allow_fail_safe {
            self.inner.tags.mark_clear_expire(now);
        } else {
            self.inner.tags.mark_clear_remove(now);
            self.inner.memory.invalidate_all();
        }
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
        self.inner.events.emit(event);
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
        match opts.lock_timeout() {
            Timeout::Infinite => LockOutcome::Acquired(self.inner.locks.lock(full_key).await),
            Timeout::After(d) => {
                match tokio::time::timeout(d, self.inner.locks.lock(full_key)).await {
                    Ok(guard) => LockOutcome::Acquired(guard),
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
                        LockOutcome::Acquired(self.inner.locks.lock(full_key).await)
                    }
                }
            }
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
            self.write_l2(full_key, entry).await;
        }
        if !opts.skip_backplane_notifications() {
            self.publish(BackplaneAction::Set, full_key).await;
        }
    }

    /// Serializes and writes an entry to L2 (best-effort; auto-recovery of failed
    /// writes is on the roadmap).
    async fn write_l2(&self, full_key: &str, entry: &Entry<V>) {
        let (Some(l2), Some(serializer)) = (&self.inner.distributed, &self.inner.serializer) else {
            return;
        };
        let dist = DistributedEntry::from_entry(entry);
        if let Ok(bytes) = serializer.serialize(&dist) {
            let _ = l2.set(full_key, bytes, Some(entry.backend_ttl())).await;
        }
    }

    /// Reads and deserializes an entry from L2, rehydrated relative to `now`.
    async fn read_l2(&self, full_key: &str, now: Timestamp) -> Option<Entry<V>> {
        let (Some(l2), Some(serializer)) = (&self.inner.distributed, &self.inner.serializer) else {
            return None;
        };
        let bytes = l2.get(full_key).await.ok()??;
        let dist = serializer.deserialize(&bytes).ok()?;
        Some(dist.into_entry(now))
    }

    /// Publishes a backplane notification (no-op without a backplane configured).
    async fn publish(&self, action: BackplaneAction, full_key: &Arc<str>) {
        if let Some(backplane) = &self.inner.backplane {
            let message = BackplaneMessage {
                source_id: Arc::clone(&self.inner.instance_id),
                timestamp: self.inner.clock.now(),
                action,
                key: Arc::clone(full_key),
            };
            let _ = backplane.publish(message).await;
        }
    }

    /// Applies an incoming backplane notification to the local L1.
    async fn apply_backplane(&self, message: BackplaneMessage) {
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
        guard: KeyGuard,
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

/// The result of trying to acquire the single-flight lock.
enum LockOutcome<V> {
    /// The lock was acquired; proceed to read L2 / run the factory.
    Acquired(KeyGuard),
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
        let inner = Arc::new(CacheInner {
            name: self.name.unwrap_or_else(|| Arc::from("amalgam")),
            instance_id: self.instance_id.unwrap_or_else(generate_instance_id),
            memory: MemoryStore::new(self.max_capacity),
            locks: Arc::new(KeyedLock::new(self.lock_shards)),
            tags: Arc::new(TagRegistry::new()),
            events: Events::with_capacity(self.events_capacity),
            clock,
            default_options: self.default_options,
            key_prefix: self.key_prefix,
            remove_by_tag_behavior: self.remove_by_tag_behavior,
            distributed: self.distributed,
            serializer: self.serializer,
            backplane: self.backplane,
        });
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
