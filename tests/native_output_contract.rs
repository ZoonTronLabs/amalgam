//! Native output selection preserves actual commit ownership and evidence.
use amalgam::advanced::{BlockingCommitReceipt, BlockingMutationReceipt, BlockingRuntime};
use amalgam::provider::{
    DistributedCache, InMemoryDistributedCache, InvalidationStore, JsonSerializer, SystemClock,
};
use amalgam::{BlockingCache, Cache, EntryOptions, Result, source};
use async_trait::async_trait;
use std::convert::Infallible;
use std::sync::{Arc, mpsc};
use std::time::Duration;
use tokio::sync::Semaphore;

struct HeldWrites {
    inner: Arc<InMemoryDistributedCache>,
    started: mpsc::Sender<()>,
    release: Arc<Semaphore>,
}
#[async_trait]
impl DistributedCache for HeldWrites {
    async fn get(&self, key: &str) -> Result<Option<amalgam::provider::DistributedBytes>> {
        self.inner.get(key).await
    }
    async fn set(&self, key: &str, value: Vec<u8>, ttl: Option<Duration>) -> Result<()> {
        self.started.send(()).unwrap();
        let _permit = self.release.acquire().await.unwrap();
        self.inner.set(key, value, ttl).await
    }
    async fn remove(&self, key: &str) -> Result<()> {
        self.inner.remove(key).await
    }
    fn invalidation_store(&self) -> Option<Arc<dyn InvalidationStore>> {
        self.inner.invalidation_store()
    }
}
struct ReleaseWrites(Arc<Semaphore>);
impl Drop for ReleaseWrites {
    fn drop(&mut self) {
        self.0.add_permits(8);
    }
}

#[test]
fn value_only_native_origin_retains_a_genuinely_pending_l2_commit() {
    let runtime = BlockingRuntime::new().unwrap();
    let inner = Arc::new(InMemoryDistributedCache::new(Arc::new(SystemClock)));
    let release = ReleaseWrites(Arc::new(Semaphore::new(0)));
    let (started, began) = mpsc::channel();
    let backend = Arc::new(HeldWrites {
        inner: Arc::clone(&inner),
        started,
        release: Arc::clone(&release.0),
    });
    let cache = BlockingCache::<u64>::on_runtime(
        Cache::builder()
            .distributed(backend)
            .serializer(Arc::new(JsonSerializer))
            .default_options(
                EntryOptions::new(Duration::from_secs(60))
                    .with_allow_background_distributed_operations(true),
            ),
        runtime.clone(),
    )
    .unwrap();
    assert_eq!(
        cache
            .get_or_set("projected", source::factory(|_| Ok::<_, Infallible>(41)))
            .execute()
            .unwrap(),
        41
    );
    began.recv_timeout(Duration::from_secs(2)).unwrap();
    assert!(runtime.run(inner.get("projected")).unwrap().is_none());
    assert_eq!(cache.try_get("projected").execute().unwrap(), Some(41));
    release.0.add_permits(1);
    cache.flush_pending().unwrap();
    let peer = BlockingCache::<u64>::on_runtime(
        Cache::builder()
            .distributed(inner)
            .serializer(Arc::new(JsonSerializer)),
        runtime,
    )
    .unwrap();
    assert_eq!(peer.try_get("projected").execute().unwrap(), Some(41));
    cache.shutdown().unwrap();
    peer.shutdown().unwrap();
}

#[test]
fn native_origin_receipt_preserves_mutation_and_unchanged_evidence() {
    let cache = BlockingCache::<u64>::new().unwrap();
    let first = cache
        .get_or_set("evidence", source::factory(|_| Ok::<_, Infallible>(17)))
        .with_receipt()
        .execute()
        .unwrap();
    assert_eq!(first.value, 17);
    match first.commit {
        BlockingCommitReceipt::Mutation(receipt) => {
            assert!(matches!(receipt, BlockingMutationReceipt::Completed(_)));
            receipt.wait().unwrap();
        }
        BlockingCommitReceipt::Unchanged => panic!("the origin must retain its commit evidence"),
    }
    let hit = cache
        .get_or_set(
            "evidence",
            source::factory(|_| -> std::result::Result<u64, Infallible> {
                panic!("an existing value must not invoke the factory");
            }),
        )
        .with_receipt()
        .execute()
        .unwrap();
    assert_eq!(hit.value, 17);
    assert!(matches!(hit.commit, BlockingCommitReceipt::Unchanged));
    assert_eq!(
        cache
            .get_or_set(
                "evidence",
                source::factory(|_| -> std::result::Result<u64, Infallible> {
                    panic!("value-only retrieval must not invoke the factory");
                })
            )
            .execute()
            .unwrap(),
        17
    );
    cache.shutdown().unwrap();
}

#[test]
fn native_supplied_values_preserve_constant_origin_and_receipt_semantics() {
    let cache = BlockingCache::<u64>::new().unwrap();
    let first = cache
        .get_or_set("constant", source::value(53))
        .with_receipt()
        .execute()
        .unwrap();
    assert_eq!(first.value, 53);
    match first.commit {
        BlockingCommitReceipt::Mutation(receipt) => {
            receipt.wait().unwrap();
        }
        BlockingCommitReceipt::Unchanged => panic!("a supplied cold value must retain its commit"),
    }
    assert_eq!(
        cache
            .get_or_set("constant", source::value(99))
            .execute()
            .unwrap(),
        53
    );
    let hit = cache
        .get_or_set("constant", source::value(99))
        .with_receipt()
        .execute()
        .unwrap();
    assert_eq!(hit.value, 53);
    assert!(matches!(hit.commit, BlockingCommitReceipt::Unchanged));
    cache.shutdown().unwrap();
}

#[derive(Clone)]
struct ClosingValue {
    value: u64,
    close: Option<Cache<ClosingValue>>,
}
impl Drop for ClosingValue {
    fn drop(&mut self) {
        if let Some(cache) = self.close.take() {
            cache.close();
        }
    }
}
#[test]
fn dropping_an_unused_native_supplied_value_preserves_the_final_close_check() {
    let cache = BlockingCache::<ClosingValue>::new().unwrap();
    cache
        .set(
            "unused",
            ClosingValue {
                value: 7,
                close: None,
            },
        )
        .execute()
        .unwrap();
    assert_eq!(
        cache
            .try_get("unused")
            .execute()
            .unwrap()
            .as_ref()
            .unwrap()
            .value,
        7
    );
    let result = cache
        .get_or_set(
            "unused",
            source::value(ClosingValue {
                value: 9,
                close: Some(cache.as_async().clone()),
            }),
        )
        .execute();
    let observed = result.as_ref().map(|value| value.value);
    assert!(
        matches!(
            result,
            Err(amalgam::Error::OperationCancelled {
                reason: amalgam::FactoryCancellationReason::CacheShutdown
            })
        ),
        "actual native outcome: {observed:?}"
    );
    cache.shutdown().unwrap();
}
