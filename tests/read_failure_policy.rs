//! Canonical absence must not disguise provider failure or an open circuit.

#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use amalgam::provider::{
    DistributedBytes, DistributedCache, InMemoryDistributedCache, JsonSerializer, ManualClock,
};
use amalgam::{
    Cache, CodecError, EntryOptions, Error, FactoryError, RecoveryConfig, Result, Timeout,
};
use async_trait::async_trait;

#[derive(Default)]
struct FailedStore {
    reads: AtomicUsize,
}

#[async_trait]
impl DistributedCache for FailedStore {
    async fn get(&self, _: &str) -> Result<Option<DistributedBytes>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Err(Error::Distributed("original read failure".to_owned()))
    }

    async fn set(&self, _: &str, _: Vec<u8>, _: Option<Duration>) -> Result<()> {
        Err(Error::Distributed("original write failure".to_owned()))
    }

    async fn remove(&self, _: &str) -> Result<()> {
        Err(Error::Distributed("original remove failure".to_owned()))
    }
}

fn options() -> EntryOptions {
    EntryOptions::new(Duration::from_secs(60))
        .with_rethrow_distributed_exceptions(true)
        .with_rethrow_serialization_exceptions(true)
        .with_allow_background_backplane_operations(false)
}

fn no_recovery() -> RecoveryConfig {
    RecoveryConfig {
        enabled: false,
        ..RecoveryConfig::default()
    }
}

#[tokio::test]
async fn read_rethrow_preserves_transport_failure_and_rejects_default() {
    let cache = Cache::<i32>::builder()
        .distributed(Arc::new(FailedStore::default()))
        .serializer(Arc::new(JsonSerializer))
        .default_options(options())
        .auto_recovery(no_recovery())
        .try_build()
        .unwrap();
    assert!(
        matches!(cache.try_get("k").await,
            Err(Error::Distributed(cause)) if cause == "original read failure"),
        "canonical read fabricated a miss from a failed lookup"
    );
    assert!(
        matches!(cache.get_or_default("k", 42).await,
            Err(Error::Distributed(cause)) if cause == "original read failure"),
        "canonical default disguised failed lookup as successful absence"
    );
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn open_circuit_is_an_admission_skip_without_requerying_backend() {
    let store = Arc::new(FailedStore::default());
    let clock = Arc::new(ManualClock::default());
    let cache = Cache::<i32>::builder()
        .clock(clock)
        .distributed(store.clone())
        .serializer(Arc::new(JsonSerializer))
        .default_options(options())
        .distributed_circuit_breaker(Duration::from_secs(60))
        .auto_recovery(no_recovery())
        .try_build()
        .unwrap();
    assert!(
        matches!(
            cache.try_get("k").await,
            Err(Error::Distributed(cause)) if cause == "original read failure"
        ),
        "contract: open circuit is an admission skip without requerying backend"
    );
    assert_eq!(
        cache.get_or_default("k", 42).await.unwrap(),
        42,
        "contract: open circuit is an admission skip without requerying backend"
    );
    assert_eq!(
        store.reads.load(Ordering::SeqCst),
        1,
        "contract: open circuit is an admission skip without requerying backend"
    );
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn read_rethrow_preserves_codec_failure() {
    let clock = Arc::new(ManualClock::default());
    let store = Arc::new(InMemoryDistributedCache::new(clock.clone()));
    store
        .set("v2:k", b"invalid JSON".to_vec(), None)
        .await
        .unwrap();
    let cache = Cache::<i32>::builder()
        .clock(clock)
        .distributed(store)
        .serializer(Arc::new(JsonSerializer))
        .default_options(options())
        .auto_recovery(no_recovery())
        .try_build()
        .unwrap();
    assert!(
        matches!(
            cache.try_get("k").await,
            Err(Error::Codec(CodecError::Deserialization { .. }))
        ),
        "contract: read rethrow preserves codec failure"
    );
    assert!(
        matches!(
            cache.get_or_default("k", 42).await,
            Err(Error::Codec(CodecError::Deserialization { .. }))
        ),
        "contract: read rethrow preserves codec failure"
    );
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn configured_suppression_is_a_miss_without_discarding_typed_rethrow() {
    let calls = Arc::new(AtomicUsize::new(0));
    let cache = Cache::<i32>::builder()
        .distributed(Arc::new(FailedStore::default()))
        .serializer(Arc::new(JsonSerializer))
        .default_options(options().with_rethrow_distributed_exceptions(false))
        .auto_recovery(no_recovery())
        .try_build()
        .unwrap();
    let factory_calls = calls.clone();
    assert_eq!(
        cache
            .get_or_set(
                "k",
                amalgam::source::factory(move |context| async move {
                    factory_calls.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, FactoryError>(context.value(7))
                })
            )
            .await
            .unwrap(),
        7,
        "contract: configured suppression is a miss without discarding typed rethrow"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "contract: configured suppression is a miss without discarding typed rethrow"
    );
    assert_eq!(
        cache.try_get("k").await.unwrap(),
        Some(7),
        "contract: configured suppression is a miss without discarding typed rethrow"
    );
    assert!(
        cache.try_get("another").await.unwrap().is_none(),
        "contract: configured suppression is a miss without discarding typed rethrow"
    );
    assert_eq!(
        cache.get_or_default("another", 42).await.unwrap(),
        42,
        "contract: configured suppression is a miss without discarding typed rethrow"
    );
    cache.shutdown().await.unwrap();
}

struct PendingReadStore;

#[async_trait]
impl DistributedCache for PendingReadStore {
    fn invalidation_store(&self) -> Option<Arc<dyn amalgam::provider::InvalidationStore>> {
        Some(Arc::new(
            amalgam::provider::InMemoryInvalidationStore::default(),
        ))
    }

    async fn get(&self, _: &str) -> Result<Option<DistributedBytes>> {
        std::future::pending().await
    }

    async fn set(&self, _: &str, _: Vec<u8>, _: Option<Duration>) -> Result<()> {
        Ok(())
    }

    async fn remove(&self, _: &str) -> Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn soft_read_timeout_serves_the_eligible_captured_stale_value() {
    let clock = Arc::new(ManualClock::default());
    let opts = options()
        .with_duration(Duration::from_secs(1))
        .with_fail_safe(true, Some(Duration::from_secs(100)), None)
        .with_allow_stale_on_read_only(true)
        .with_distributed_timeouts(Timeout::After(Duration::ZERO), Timeout::Infinite);
    let cache = Cache::<i32>::builder()
        .clock(clock.clone())
        .distributed(Arc::new(PendingReadStore))
        .serializer(Arc::new(JsonSerializer))
        .default_options(opts)
        .auto_recovery(no_recovery())
        .try_build()
        .unwrap();
    cache.set("k", 7).await.unwrap();
    clock.advance(Duration::from_secs(2));
    assert_eq!(
        cache.try_get("k").await.unwrap(),
        Some(7),
        "contract: soft read timeout serves the eligible captured stale value"
    );
    assert_eq!(
        cache.get_or_default("k", 42).await.unwrap(),
        7,
        "contract: soft read timeout serves the eligible captured stale value"
    );
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn canonical_hard_read_timeout_never_becomes_successful_default() {
    let opts =
        options().with_distributed_timeouts(Timeout::Infinite, Timeout::After(Duration::ZERO));
    let cache = Cache::<i32>::builder()
        .distributed(Arc::new(PendingReadStore))
        .serializer(Arc::new(JsonSerializer))
        .default_options(opts)
        .auto_recovery(no_recovery())
        .try_build()
        .unwrap();
    assert!(
        matches!(
            cache.get_or_default("k", 42).await,
            Err(Error::DistributedTimeout { elapsed }) if elapsed == Duration::ZERO
        ),
        "contract: canonical hard read timeout never becomes successful default"
    );
    assert_eq!(
        cache
            .get_or_default("k", 42)
            .options(|o| o.with_rethrow_distributed_exceptions(false))
            .await
            .unwrap(),
        42,
        "contract: canonical hard read timeout never becomes successful default"
    );
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn explicit_skip_remains_successful_absence_without_querying_failed_backend() {
    let store = Arc::new(FailedStore::default());
    let cache = Cache::<i32>::builder()
        .distributed(store.clone())
        .serializer(Arc::new(JsonSerializer))
        .default_options(options())
        .auto_recovery(no_recovery())
        .try_build()
        .unwrap();
    assert_eq!(
        cache
            .get_or_default("k", 42)
            .options(|_| options().with_skip_distributed(true, false))
            .await
            .unwrap(),
        42,
        "contract: explicit skip remains successful absence without querying failed backend"
    );
    assert_eq!(
        store.reads.load(Ordering::SeqCst),
        0,
        "contract: explicit skip remains successful absence without querying failed backend"
    );
    cache.shutdown().await.unwrap();
}
