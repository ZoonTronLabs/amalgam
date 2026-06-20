//! The L1 (in-memory) store, backed by [`moka`].
//!
//! We use moka purely as a concurrent, evicting storage layer with per-entry
//! physical expiration; the single-flight, fail-safe and timeout logic lives in
//! [`Cache`](crate::Cache) so we keep full control over the flow (moka's own
//! `get_with` coalescing can't express "return a stale value now and finish in
//! the background").

use std::sync::Arc;
use std::time::{Duration, Instant};

use moka::Expiry;
use moka::future::Cache as MokaCache;

use crate::entry::Entry;

/// Per-entry expiry policy: each entry carries the physical TTL the backend
/// should honour ([`Entry::backend_ttl`]).
struct EntryExpiry;

impl<V: Send + Sync + 'static> Expiry<Arc<str>, Entry<V>> for EntryExpiry {
    fn expire_after_create(
        &self,
        _key: &Arc<str>,
        value: &Entry<V>,
        _created_at: Instant,
    ) -> Option<Duration> {
        Some(value.backend_ttl())
    }

    fn expire_after_update(
        &self,
        _key: &Arc<str>,
        value: &Entry<V>,
        _updated_at: Instant,
        _duration_until_expiry: Option<Duration>,
    ) -> Option<Duration> {
        Some(value.backend_ttl())
    }

    // `expire_after_read` keeps the default (no sliding): expiration is absolute,
    // matching FusionCache's behaviour.
}

/// The L1 memory store.
#[derive(Clone)]
pub struct MemoryStore<V: Clone + Send + Sync + 'static> {
    cache: MokaCache<Arc<str>, Entry<V>>,
}

impl<V: Clone + Send + Sync + 'static> MemoryStore<V> {
    /// Creates a store with an optional maximum entry capacity (`None` =
    /// unbounded).
    #[must_use]
    pub fn new(max_capacity: Option<u64>) -> Self {
        let mut builder = MokaCache::builder().expire_after(EntryExpiry);
        if let Some(capacity) = max_capacity {
            builder = builder.max_capacity(capacity);
        }
        Self {
            cache: builder.build(),
        }
    }

    /// Reads an entry, if present and not physically expired.
    pub async fn get(&self, key: &str) -> Option<Entry<V>> {
        self.cache.get(key).await
    }

    /// Writes an entry.
    pub async fn insert(&self, key: Arc<str>, entry: Entry<V>) {
        self.cache.insert(key, entry).await;
    }

    /// Removes an entry, returning the previous value if any.
    pub async fn remove(&self, key: &str) -> Option<Entry<V>> {
        self.cache.remove(key).await
    }

    /// Invalidates every entry (lazily).
    pub fn invalidate_all(&self) {
        self.cache.invalidate_all();
    }

    /// Forces pending maintenance to run — primarily for deterministic tests.
    pub async fn run_pending_tasks(&self) {
        self.cache.run_pending_tasks().await;
    }
}
