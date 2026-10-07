use amalgam::advanced::*;
use amalgam::provider::*;
use amalgam::*;
use async_trait::async_trait;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::{Notify, Semaphore};

struct PendingMarkers;
#[async_trait]
impl InvalidationStore for PendingMarkers {
    async fn read(
        &self,
        _: &CacheScope,
        _: &MarkerKind,
    ) -> std::result::Result<Option<MarkerVersion>, MarkerError> {
        std::future::pending().await
    }
    async fn advance(
        &self,
        _: &CacheScope,
        kind: MarkerKind,
        version: MarkerVersion,
    ) -> std::result::Result<MarkerAdvanceOutcome, MarkerError> {
        Ok(MarkerAdvanceOutcome::Advanced(StoredMarker::new(
            kind, version,
        )))
    }
}

#[tokio::test]
async fn distributed_hard_read_budget_includes_required_marker_read() {
    let clock = Arc::new(ManualClock::default());
    let backend = Arc::new(InMemoryDistributedCache::new(clock.clone()));
    let writer: Cache<u64> = Cache::builder()
        .clock(clock.clone())
        .distributed(backend.clone())
        .serializer(Arc::new(JsonSerializer))
        .try_build()
        .unwrap();
    writer
        .set("key", 7)
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    writer.shutdown().await.unwrap();
    let cache: Cache<u64> = Cache::builder()
        .clock(clock)
        .distributed(backend)
        .serializer(Arc::new(JsonSerializer))
        .invalidation_store(Arc::new(PendingMarkers))
        .try_build()
        .unwrap();
    let options = EntryOptions::default()
        .with_distributed_timeouts(Timeout::Infinite, Timeout::After(Duration::from_millis(5)));
    let result =
        tokio::time::timeout(Duration::from_secs(1), cache.read("key", Some(options))).await;
    cache.shutdown().await.unwrap();
    assert!(
        matches!(result, Ok(Err(Error::DistributedTimeout { elapsed })) if elapsed == Duration::from_millis(5)),
        "the selected hard deadline must also bound required marker I/O: {result:?}"
    );
}

struct CapturedRead {
    inner: InMemoryDistributedCache,
    armed: AtomicBool,
    entered: Notify,
    release: Semaphore,
}
#[async_trait]
impl DistributedCache for CapturedRead {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let snapshot = self.inner.get(key).await?;
        if self.armed.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.acquire().await.unwrap().forget();
        }
        Ok(snapshot)
    }
    async fn set(&self, key: &str, bytes: Vec<u8>, ttl: Option<Duration>) -> Result<()> {
        self.inner.set(key, bytes, ttl).await
    }
    async fn remove(&self, key: &str) -> Result<()> {
        self.inner.remove(key).await
    }
    fn invalidation_store(&self) -> Option<Arc<dyn InvalidationStore>> {
        self.inner.invalidation_store()
    }
}
#[derive(Clone, Copy)]
enum ReadContract {
    Canonical,
    Legacy,
}
async fn captured_stale(contract: ReadContract) {
    let clock = Arc::new(ManualClock::default());
    let backend = Arc::new(CapturedRead {
        inner: InMemoryDistributedCache::new(clock.clone()),
        armed: AtomicBool::new(false),
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let options = EntryOptions::new(Duration::from_secs(1))
        .with_fail_safe(true, Some(Duration::from_secs(2)), None)
        .with_allow_stale_on_read_only(true);
    let cache: Cache<i32> = Cache::builder()
        .clock(clock.clone())
        .distributed(backend.clone())
        .serializer(Arc::new(JsonSerializer))
        .default_options(options)
        .try_build()
        .unwrap();
    cache
        .set("key", 1)
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    clock.advance(Duration::from_millis(1500));
    backend.armed.store(true, Ordering::SeqCst);
    let read = tokio::spawn({
        let cache = cache.clone();
        async move {
            match contract {
                ReadContract::Canonical => cache.read("key", None).await.unwrap(),
                ReadContract::Legacy => cache.try_get("key", None).await,
            }
        }
    });
    tokio::time::timeout(Duration::from_secs(1), backend.entered.notified())
        .await
        .unwrap();
    clock.advance(Duration::from_secs(1));
    backend.release.add_permits(1);
    let observed = read.await.unwrap();
    cache.shutdown().await.unwrap();
    match contract {
        ReadContract::Canonical => assert!(
            !observed.has_value(),
            "canonical stale service must recheck the physical deadline after I/O"
        ),
        ReadContract::Legacy => assert_eq!(
            observed.value(),
            Some(&1),
            "the legacy adapter preserves FusionCache's eligible captured fallback"
        ),
    }
}
#[tokio::test]
async fn canonical_stale_read_rechecks_physical_retention_after_l2_wait() {
    captured_stale(ReadContract::Canonical).await;
}
#[tokio::test]
async fn legacy_stale_read_preserves_fusioncache_captured_fallback() {
    captured_stale(ReadContract::Legacy).await;
}

#[tokio::test]
async fn stale_distributed_only_read_is_attributed_to_distributed_layer() {
    let clock = Arc::new(ManualClock::default());
    let backend = Arc::new(InMemoryDistributedCache::new(clock.clone()));
    let options = EntryOptions::new(Duration::from_secs(1)).with_fail_safe(
        true,
        Some(Duration::from_secs(60)),
        None,
    );
    let cache: Cache<u64> = Cache::builder()
        .clock(clock.clone())
        .distributed(backend)
        .serializer(Arc::new(JsonSerializer))
        .default_options(options.clone())
        .try_build()
        .unwrap();
    cache
        .set("key", 7)
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    clock.advance(Duration::from_secs(2));
    let mut events = cache.events().subscribe();
    let observed = cache
        .read(
            "key",
            Some(
                options
                    .with_skip_memory(true, true)
                    .with_allow_stale_on_read_only(true),
            ),
        )
        .await
        .unwrap();
    assert_eq!(observed.value(), Some(&7));
    let level = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if let CacheEvent::OperationCompleted { level, .. } = events.recv().await.unwrap() {
                break level;
            }
        }
    })
    .await
    .unwrap();
    cache.shutdown().await.unwrap();
    assert_eq!(level, Some(CacheLevel::Distributed));
}

#[tokio::test]
async fn required_marker_read_respects_soft_budget_and_preserves_origin_fallback() {
    let clock = Arc::new(ManualClock::default());
    let backend = Arc::new(InMemoryDistributedCache::new(clock.clone()));
    let options = EntryOptions::new(Duration::from_secs(1))
        .with_fail_safe(true, Some(Duration::from_secs(60)), None)
        .with_distributed_timeouts(
            Timeout::After(Duration::from_millis(5)),
            Timeout::After(Duration::from_secs(1)),
        );
    let cache: Cache<u64> = Cache::builder()
        .clock(clock.clone())
        .distributed(backend)
        .serializer(Arc::new(JsonSerializer))
        .invalidation_store(Arc::new(PendingMarkers))
        .default_options(options)
        .try_build()
        .unwrap();
    cache
        .set("key", 7)
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    clock.advance(Duration::from_secs(2));
    let result = tokio::time::timeout(
        Duration::from_secs(1),
        cache.get_or_set(
            "key",
            amalgam::source::factory(|_ctx| async move { Err(FactoryError::new("origin fault")) }),
        ),
    )
    .await;
    cache.shutdown().await.unwrap();
    assert!(
        matches!(result, Ok(Ok(7))),
        "marker I/O escaped the selected soft budget or lost captured fail-safe: {result:?}"
    );
}

struct ParkedWriteBackend {
    inner: InMemoryDistributedCache,
    pause_read: AtomicBool,
    pause_write: AtomicBool,
    read_entered: Notify,
    write_entered: Notify,
    read_release: Semaphore,
    write_release: Semaphore,
}
#[async_trait]
impl DistributedCache for ParkedWriteBackend {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let captured = self.inner.get(key).await?;
        if self.pause_read.swap(false, Ordering::SeqCst) {
            self.read_entered.notify_one();
            self.read_release.acquire().await.unwrap().forget();
        }
        Ok(captured)
    }
    async fn set(&self, key: &str, bytes: Vec<u8>, ttl: Option<Duration>) -> Result<()> {
        if self.pause_write.swap(false, Ordering::SeqCst) {
            self.write_entered.notify_one();
            self.write_release.acquire().await.unwrap().forget();
        }
        self.inner.set(key, bytes, ttl).await
    }
    async fn remove(&self, key: &str) -> Result<()> {
        self.inner.remove(key).await
    }
    fn invalidation_store(&self) -> Option<Arc<dyn InvalidationStore>> {
        self.inner.invalidation_store()
    }
}
#[tokio::test]
async fn optional_hydration_does_not_wait_on_a_newer_parked_commit() {
    let clock = Arc::new(ManualClock::default());
    let backend = Arc::new(ParkedWriteBackend {
        inner: InMemoryDistributedCache::new(clock.clone()),
        pause_read: AtomicBool::new(false),
        pause_write: AtomicBool::new(false),
        read_entered: Notify::new(),
        write_entered: Notify::new(),
        read_release: Semaphore::new(0),
        write_release: Semaphore::new(0),
    });
    let build = || {
        Cache::<u64>::builder()
            .clock(clock.clone())
            .distributed(backend.clone())
            .serializer(Arc::new(JsonSerializer))
            .try_build()
            .unwrap()
    };
    let writer = build();
    writer
        .set("key", 1)
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    writer.shutdown().await.unwrap();
    let cache = build();
    backend.pause_read.store(true, Ordering::SeqCst);
    let mut read = tokio::spawn({
        let cache = cache.clone();
        async move { cache.read("key", None).await }
    });
    tokio::time::timeout(Duration::from_secs(1), backend.read_entered.notified())
        .await
        .unwrap();
    backend.pause_write.store(true, Ordering::SeqCst);
    let write = tokio::spawn({
        let cache = cache.clone();
        async move {
            cache
                .set("key", 2)
                .with_receipt()
                .await
                .unwrap()
                .wait()
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(1), backend.write_entered.notified())
        .await
        .unwrap();
    backend.read_release.add_permits(1);
    let completed = tokio::time::timeout(Duration::from_millis(100), &mut read).await;
    let observed = completed.ok().map(|result| result.unwrap().unwrap());
    let premature_wait = observed.is_none();
    backend.write_release.add_permits(1);
    write.await.unwrap().unwrap();
    if premature_wait {
        read.await.unwrap().unwrap();
    } else {
        assert_eq!(observed.unwrap().value(), Some(&1));
    }
    assert_eq!(cache.read("key", None).await.unwrap().value(), Some(&2));
    cache.shutdown().await.unwrap();
    assert!(
        !premature_wait,
        "optional L1 hydration parked behind a later value commit"
    );
}
