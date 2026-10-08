//! Durable clear markers may travel with the value read of their own provider.

use amalgam::advanced::*;
use amalgam::provider::*;
use amalgam::*;
use async_trait::async_trait;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Durable marker storage recording every separate batch read.
struct Store {
    inner: InMemoryInvalidationStore,
    reads: Mutex<Vec<Vec<MarkerKind>>>,
}
#[async_trait]
impl InvalidationStore for Store {
    async fn read(
        &self,
        scope: &CacheScope,
        kind: &MarkerKind,
    ) -> std::result::Result<Option<MarkerVersion>, MarkerError> {
        self.inner.read(scope, kind).await
    }
    async fn advance(
        &self,
        scope: &CacheScope,
        kind: MarkerKind,
        candidate: MarkerVersion,
    ) -> std::result::Result<MarkerAdvanceOutcome, MarkerError> {
        self.inner.advance(scope, kind, candidate).await
    }
    async fn read_many(
        &self,
        scope: &CacheScope,
        kinds: &[MarkerKind],
    ) -> std::result::Result<Box<[StoredMarker]>, MarkerError> {
        self.reads.lock().unwrap().push(kinds.to_vec());
        self.inner.read_many(scope, kinds).await
    }
}

/// An asynchronous value provider owning its marker store. `get_marked`
/// reads requested markers with the value, as a pipelined backend would.
struct Provider {
    values: InMemoryDistributedCache,
    markers: Arc<Store>,
    marked: Mutex<Vec<Vec<MarkerKind>>>,
}
impl Provider {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            values: InMemoryDistributedCache::new(Arc::new(SystemClock)),
            markers: Arc::new(Store {
                inner: InMemoryInvalidationStore::default(),
                reads: Mutex::new(Vec::new()),
            }),
            marked: Mutex::new(Vec::new()),
        })
    }
    fn separate_reads(&self) -> Vec<Vec<MarkerKind>> {
        std::mem::take(&mut self.markers.reads.lock().unwrap())
    }
    fn marked_reads(&self) -> Vec<Vec<MarkerKind>> {
        std::mem::take(&mut self.marked.lock().unwrap())
    }
}
#[async_trait]
impl DistributedCache for Provider {
    async fn get(&self, key: &str) -> Result<Option<DistributedBytes>> {
        self.values.get(key).await
    }
    async fn get_marked(
        &self,
        key: &str,
        scope: &CacheScope,
        kinds: &[MarkerKind],
    ) -> Result<MarkedRead> {
        self.marked.lock().unwrap().push(kinds.to_vec());
        Ok(match self.values.get(key).await? {
            Some(bytes) => MarkedRead::Value {
                bytes,
                prefetched: Some(self.markers.inner.read_many(scope, kinds).await),
            },
            None => MarkedRead::Miss,
        })
    }
    async fn set(&self, key: &str, value: Vec<u8>, ttl: Option<Duration>) -> Result<()> {
        self.values.set(key, value, ttl).await
    }
    async fn remove(&self, key: &str) -> Result<()> {
        self.values.remove(key).await
    }
    fn invalidation_store(&self) -> Option<Arc<dyn InvalidationStore>> {
        Some(self.markers.clone())
    }
}

fn cold_reads() -> EntryOptions {
    EntryOptions::new(Duration::from_secs(60)).with_skip_memory(true, false)
}

fn node(provider: &Arc<Provider>) -> Cache<u64> {
    Cache::builder()
        .default_options(cold_reads())
        .distributed(provider.clone())
        .serializer(Arc::new(JsonSerializer))
        .try_build()
        .unwrap()
}

#[tokio::test]
async fn own_markers_prefetch_clear_markers_with_the_value() {
    let provider = Provider::new();
    let cache = node(&provider);
    cache.set("plain", 1).await.unwrap();
    cache.set("tagged", 2).tags(["users"]).await.unwrap();
    provider.separate_reads();
    provider.marked_reads();

    assert_eq!(cache.try_get("plain").await.unwrap(), Some(1));
    let clear = vec![MarkerKind::ClearRemove, MarkerKind::ClearExpire];
    assert_eq!(provider.marked_reads(), vec![clear.clone()]);
    assert!(provider.separate_reads().is_empty());

    assert_eq!(cache.try_get("tagged").await.unwrap(), Some(2));
    assert_eq!(provider.marked_reads(), vec![clear]);
    assert_eq!(
        provider.separate_reads(),
        vec![vec![MarkerKind::Tag(Tag::new("users").unwrap())]]
    );
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn prefetched_markers_invalidate_cold_reads_on_another_node() {
    let provider = Provider::new();
    let writer = node(&provider);
    let reader = node(&provider);
    writer.set("plain", 1).await.unwrap();
    writer.set("tagged", 2).tags(["users"]).await.unwrap();
    assert_eq!(reader.try_get("plain").await.unwrap(), Some(1));

    writer.remove_by_tag("users").await.unwrap();
    assert_eq!(reader.try_get("tagged").await.unwrap(), None);
    assert_eq!(reader.try_get("plain").await.unwrap(), Some(1));

    writer.clear(ClearMode::Remove).await.unwrap();
    assert_eq!(reader.try_get("plain").await.unwrap(), None);
    writer.shutdown().await.unwrap();
    reader.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_separately_supplied_marker_store_keeps_its_own_reads() {
    let provider = Provider::new();
    let separate = Arc::new(Store {
        inner: InMemoryInvalidationStore::default(),
        reads: Mutex::new(Vec::new()),
    });
    let cache: Cache<u64> = Cache::builder()
        .default_options(cold_reads())
        .distributed(provider.clone())
        .invalidation_store(separate.clone())
        .serializer(Arc::new(JsonSerializer))
        .try_build()
        .unwrap();
    cache.set("plain", 1).await.unwrap();
    separate.reads.lock().unwrap().clear();

    assert_eq!(cache.try_get("plain").await.unwrap(), Some(1));
    assert!(provider.marked_reads().is_empty());
    assert_eq!(
        std::mem::take(&mut *separate.reads.lock().unwrap()),
        vec![vec![MarkerKind::ClearRemove, MarkerKind::ClearExpire]]
    );
    cache.shutdown().await.unwrap();
}
