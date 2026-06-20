//! L2 distributed cache: the wire envelope, a serializer abstraction, the
//! [`DistributedCache`] backend trait, and an in-memory reference backend.
//!
//! FusionCache's L2 is any `IDistributedCache`; here it is any implementor of
//! [`DistributedCache`] (a byte-oriented key/value store with TTL). Values cross
//! the wire as a [`DistributedEntry`] — value plus the metadata needed to
//! reconstruct freshness on another node — encoded by a [`DistributedSerializer`].
//!
//! A Redis-backed backend is intentionally left to a feature-gated adapter; the
//! [`InMemoryDistributedCache`] here is a faithful reference used by tests and
//! single-process multi-instance scenarios.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use dashmap::DashMap;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::entry::Entry;
use crate::error::{Error, Result};
use crate::tags::{Tag, collect_tags};
use crate::time::{Clock, Timestamp};

/// The serializable L2 envelope: a value together with the metadata required to
/// rebuild its freshness/fail-safe state on any node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributedEntry<V> {
    /// The cached value.
    pub value: V,
    /// Creation tick (for tag-marker comparison).
    pub created_ticks: i64,
    /// Logical-expiration tick.
    pub logical_expiration_ticks: i64,
    /// Physical-expiration tick (fail-safe boundary).
    pub physical_expiration_ticks: i64,
    /// Whether the value came from a fail-safe activation.
    pub is_from_fail_safe: bool,
    /// Optional `ETag` for conditional refresh.
    pub etag: Option<String>,
    /// Optional `LastModified` tick for conditional refresh.
    pub last_modified_ticks: Option<i64>,
    /// The tags attached to the entry.
    pub tags: Vec<String>,
}

impl<V: Clone> DistributedEntry<V> {
    /// Captures an in-memory [`Entry`] as a wire envelope.
    #[must_use]
    pub fn from_entry(entry: &Entry<V>) -> Self {
        let meta = entry.meta();
        Self {
            value: entry.value_cloned(),
            created_ticks: meta.created().ticks(),
            logical_expiration_ticks: meta.logical_expiration().ticks(),
            physical_expiration_ticks: meta.physical_expiration().ticks(),
            is_from_fail_safe: meta.is_from_fail_safe(),
            etag: meta.etag().map(str::to_owned),
            last_modified_ticks: meta.last_modified().map(Timestamp::ticks),
            tags: meta.tags().iter().map(|t| t.as_str().to_owned()).collect(),
        }
    }

    /// Rebuilds an in-memory [`Entry`] from the wire envelope, relative to `now`.
    #[must_use]
    pub fn into_entry(self, now: Timestamp) -> Entry<V> {
        let tags: Box<[Tag]> = collect_tags(self.tags);
        Entry::rehydrate(
            self.value,
            Timestamp::from_ticks(self.created_ticks),
            Timestamp::from_ticks(self.logical_expiration_ticks),
            Timestamp::from_ticks(self.physical_expiration_ticks),
            self.is_from_fail_safe,
            self.etag,
            self.last_modified_ticks.map(Timestamp::from_ticks),
            tags,
            now,
        )
    }
}

/// Encodes and decodes [`DistributedEntry`] values for the wire.
///
/// This is object-safe (no generic methods), so a cache holds it as
/// `Arc<dyn DistributedSerializer<V>>` and the serialization format is fully
/// pluggable.
pub trait DistributedSerializer<V>: Send + Sync {
    /// Serializes an envelope to bytes.
    ///
    /// # Errors
    /// Returns [`Error::Serialization`] if encoding fails.
    fn serialize(&self, entry: &DistributedEntry<V>) -> Result<Vec<u8>>;

    /// Deserializes an envelope from bytes.
    ///
    /// # Errors
    /// Returns [`Error::Deserialization`] if decoding fails.
    fn deserialize(&self, bytes: &[u8]) -> Result<DistributedEntry<V>>;
}

/// A JSON serializer built on `serde_json`.
#[derive(Debug, Clone, Copy, Default)]
pub struct JsonSerializer;

impl<V> DistributedSerializer<V> for JsonSerializer
where
    V: Serialize + DeserializeOwned,
{
    fn serialize(&self, entry: &DistributedEntry<V>) -> Result<Vec<u8>> {
        serde_json::to_vec(entry).map_err(|e| Error::Serialization(e.to_string()))
    }

    fn deserialize(&self, bytes: &[u8]) -> Result<DistributedEntry<V>> {
        serde_json::from_slice(bytes).map_err(|e| Error::Deserialization(e.to_string()))
    }
}

/// A byte-oriented L2 distributed cache backend.
///
/// Implement this over Redis, Memcached, a database, etc. The cache layer adds
/// serialization, fail-safe and stampede protection on top.
#[async_trait]
pub trait DistributedCache: Send + Sync {
    /// Reads the bytes stored at `key`, if present and unexpired.
    ///
    /// # Errors
    /// Returns [`Error::Distributed`] on backend failure.
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;

    /// Writes `value` at `key` with an optional TTL.
    ///
    /// # Errors
    /// Returns [`Error::Distributed`] on backend failure.
    async fn set(&self, key: &str, value: Vec<u8>, ttl: Option<Duration>) -> Result<()>;

    /// Removes `key`.
    ///
    /// # Errors
    /// Returns [`Error::Distributed`] on backend failure.
    async fn remove(&self, key: &str) -> Result<()>;
}

/// An in-memory reference [`DistributedCache`] — a concurrent map with TTL.
///
/// Share one instance between multiple [`Cache`](crate::Cache) instances (via
/// `Arc`) to simulate several nodes pointing at the same L2 within one process.
#[derive(Clone)]
pub struct InMemoryDistributedCache {
    map: Arc<DashMap<String, StoredBytes>>,
    clock: Arc<dyn Clock>,
}

#[derive(Clone)]
struct StoredBytes {
    bytes: Vec<u8>,
    expires_at: Option<Timestamp>,
}

impl InMemoryDistributedCache {
    /// Creates an empty backend using the given clock for TTL accounting.
    #[must_use]
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        Self {
            map: Arc::new(DashMap::new()),
            clock,
        }
    }
}

#[async_trait]
impl DistributedCache for InMemoryDistributedCache {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let now = self.clock.now();
        // Resolve to an owned value so no DashMap guard is held across `remove`.
        let hit = self.map.get(key).and_then(|stored| {
            if stored.expires_at.is_none_or(|exp| now.is_before(exp)) {
                Some(stored.bytes.clone())
            } else {
                None
            }
        });
        if hit.is_none() {
            // Absent or expired: drop any expired entry lazily.
            self.map.remove(key);
        }
        Ok(hit)
    }

    async fn set(&self, key: &str, value: Vec<u8>, ttl: Option<Duration>) -> Result<()> {
        let expires_at = ttl.map(|d| self.clock.now().saturating_add(d));
        self.map.insert(
            key.to_owned(),
            StoredBytes {
                bytes: value,
                expires_at,
            },
        );
        Ok(())
    }

    async fn remove(&self, key: &str) -> Result<()> {
        self.map.remove(key);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::time::ManualClock;

    #[tokio::test]
    async fn in_memory_l2_round_trips_and_expires() {
        let clock = Arc::new(ManualClock::default());
        let dyn_clock: Arc<dyn Clock> = clock.clone();
        let l2 = InMemoryDistributedCache::new(dyn_clock);

        l2.set("k", b"hello".to_vec(), Some(Duration::from_secs(10)))
            .await
            .unwrap();
        assert_eq!(l2.get("k").await.unwrap(), Some(b"hello".to_vec()));

        clock.advance(Duration::from_secs(11));
        assert_eq!(l2.get("k").await.unwrap(), None);
    }

    #[test]
    fn json_serializer_round_trips_envelope() {
        let entry = DistributedEntry {
            value: "v".to_owned(),
            created_ticks: 1,
            logical_expiration_ticks: 2,
            physical_expiration_ticks: 3,
            is_from_fail_safe: false,
            etag: Some("e".to_owned()),
            last_modified_ticks: None,
            tags: vec!["t".to_owned()],
        };
        let ser = JsonSerializer;
        let bytes = DistributedSerializer::<String>::serialize(&ser, &entry).unwrap();
        let back = DistributedSerializer::<String>::deserialize(&ser, &bytes).unwrap();
        assert_eq!(back.value, "v");
        assert_eq!(back.tags, vec!["t".to_owned()]);
    }
}
