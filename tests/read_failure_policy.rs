//! Canonical absence must not disguise provider failure or an open circuit.

use amalgam::{
    Cache, CircuitComponent, CodecError, DistributedCache, EntryOptions, Error,
    InMemoryDistributedCache, JsonSerializer, ManualClock, RecoveryConfig, Result, Timeout,
};
use async_trait::async_trait;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

#[derive(Default)]
struct FailedStore {
    reads: AtomicUsize,
}

#[async_trait]
impl DistributedCache for FailedStore {
    async fn get(&self, _: &str) -> Result<Option<Vec<u8>>> {
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
        .with_rethrow_distributed_exceptions(false)
        .with_rethrow_serialization_exceptions(false)
        .with_allow_background_backplane_operations(false)
}

fn no_recovery() -> RecoveryConfig {
    RecoveryConfig {
        enabled: false,
        ..RecoveryConfig::default()
    }
}

#[tokio::test]
async fn canonical_reads_preserve_default_suppressed_transport_failure_and_no_default() {
    let cache = Cache::<i32>::builder()
        .distributed(Arc::new(FailedStore::default()))
        .serializer(Arc::new(JsonSerializer))
        .default_options(options())
        .auto_recovery(no_recovery())
        .try_build()
        .unwrap();
    assert!(
        matches!(cache.read("k", None).await,
            Err(Error::Distributed(cause)) if cause == "original read failure"),
        "canonical read fabricated a miss from a failed lookup"
    );
    assert!(
        matches!(cache.read_or_default("k", 42, None).await,
            Err(Error::Distributed(cause)) if cause == "original read failure"),
        "canonical default disguised failed lookup as successful absence"
    );
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn canonical_open_circuit_is_failure_without_requerying_backend() {
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
    assert!(matches!(
        cache.read("k", None).await,
        Err(Error::Distributed(cause)) if cause == "original read failure"
    ));
    assert!(
        matches!(
            cache.read_or_default("k", 42, None).await,
            Err(Error::CircuitOpen {
                component: CircuitComponent::Distributed
            })
        ),
        "circuit-blocked lookup fabricated successful absence"
    );
    assert_eq!(store.reads.load(Ordering::SeqCst), 1);
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn canonical_reads_preserve_suppressed_codec_error() {
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
    assert!(matches!(
        cache.read("k", None).await,
        Err(Error::Codec(CodecError::Deserialization { .. }))
    ));
    assert!(matches!(
        cache.read_or_default("k", 42, None).await,
        Err(Error::Codec(CodecError::Deserialization { .. }))
    ));
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn origin_fallback_policy_and_legacy_adapters_keep_their_existing_behavior() {
    let calls = Arc::new(AtomicUsize::new(0));
    let cache = Cache::<i32>::builder()
        .distributed(Arc::new(FailedStore::default()))
        .serializer(Arc::new(JsonSerializer))
        .default_options(options())
        .auto_recovery(no_recovery())
        .try_build()
        .unwrap();
    let factory_calls = calls.clone();
    assert_eq!(
        cache
            .get_or_set("k", move |context| async move {
                factory_calls.fetch_add(1, Ordering::SeqCst);
                Ok(context.value(7))
            })
            .await
            .unwrap(),
        7
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(cache.read("k", None).await.unwrap().into_value(), Some(7));
    assert!(!cache.try_get("another", None).await.has_value());
    assert_eq!(cache.get_or_default("another", 42, None).await, 42);
    cache.shutdown().await.unwrap();
}

struct PendingReadStore;

#[async_trait]
impl DistributedCache for PendingReadStore {
    async fn get(&self, _: &str) -> Result<Option<Vec<u8>>> {
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
async fn canonical_stale_read_preserves_timeout_while_legacy_read_serves_configured_fallback() {
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
    cache.set("k", 7).await;
    clock.advance(Duration::from_secs(2));
    assert!(matches!(
        cache.read("k", None).await,
        Err(Error::DistributedTimeout { elapsed }) if elapsed == Duration::ZERO
    ));
    assert!(matches!(
        cache.read_or_default("k", 42, None).await,
        Err(Error::DistributedTimeout { elapsed }) if elapsed == Duration::ZERO
    ));
    assert_eq!(cache.try_get("k", None).await.into_value(), Some(7));
    assert_eq!(cache.get_or_default("k", 42, None).await, 7);
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
    assert!(matches!(
        cache.read_or_default("k", 42, None).await,
        Err(Error::DistributedTimeout { elapsed }) if elapsed == Duration::ZERO
    ));
    assert_eq!(cache.get_or_default("k", 42, None).await, 42);
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
            .read_or_default("k", 42, Some(options().with_skip_distributed(true, false)))
            .await
            .unwrap(),
        42
    );
    assert_eq!(store.reads.load(Ordering::SeqCst), 0);
    cache.shutdown().await.unwrap();
}
