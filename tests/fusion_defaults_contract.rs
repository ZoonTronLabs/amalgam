//! FusionCache-compatible availability defaults and explicit strict capabilities.
use amalgam::provider::*;
use amalgam::*;
use async_trait::async_trait;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

struct OrdinaryStore {
    calls: AtomicUsize,
}
#[async_trait]
impl DistributedCache for OrdinaryStore {
    async fn get(&self, _: &str) -> Result<Option<amalgam::provider::DistributedBytes>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(None)
    }
    async fn set(&self, _: &str, _: Vec<u8>, _: Option<Duration>) -> Result<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn remove(&self, _: &str) -> Result<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
struct OpaqueLocker;
#[async_trait]
impl DistributedLocker for OpaqueLocker {
    async fn acquire(&self, _: &str, _: Duration, _: Timeout) -> Result<Option<String>> {
        panic!("configuration validation must precede I/O")
    }
    async fn release(&self, _: &str, _: &str) -> Result<()> {
        panic!("configuration validation must precede I/O")
    }
}
fn no_recovery() -> RecoveryConfig {
    RecoveryConfig {
        enabled: false,
        ..RecoveryConfig::default()
    }
}

#[test]
fn strict_rejects_opaque_ownership_before_acquisition() {
    let result = Cache::<u64>::builder()
        .strict()
        .distributed_locker(Arc::new(OpaqueLocker))
        .try_build();
    assert!(matches!(
        result,
        Err(Error::Config(ConfigError::FencedLockerWithoutOwnedLifetime))
    ));
    let cache = Cache::<u64>::builder()
        .distributed_locker(Arc::new(OpaqueLocker))
        .try_build()
        .unwrap();
    cache.close();
}

#[test]
fn strict_rejects_ordinary_l2_before_any_effect() {
    let store = Arc::new(OrdinaryStore {
        calls: AtomicUsize::new(0),
    });
    let result = Cache::<u64>::builder()
        .strict()
        .distributed(store.clone())
        .serializer(Arc::new(JsonSerializer))
        .auto_recovery(no_recovery())
        .try_build();
    assert!(matches!(
        result,
        Err(Error::Config(
            ConfigError::FencedDistributedWithoutAtomicWrites
        ))
    ));
    assert_eq!(store.calls.load(Ordering::SeqCst), 0);
    let cache = Cache::<u64>::builder()
        .distributed(store.clone())
        .serializer(Arc::new(JsonSerializer))
        .auto_recovery(no_recovery())
        .try_build()
        .unwrap();
    assert_eq!(store.calls.load(Ordering::SeqCst), 0);
    cache.close();
}

#[tokio::test(start_paused = true)]
async fn default_l2_only_keeps_l1_until_expiration_instead_of_clearing_each_second() {
    let clock = Arc::new(ManualClock::default());
    let store = Arc::new(OrdinaryStore {
        calls: AtomicUsize::new(0),
    });
    let cache = Cache::<u64>::builder()
        .clock(clock.clone())
        .distributed(store.clone())
        .serializer(Arc::new(JsonSerializer))
        .auto_recovery(no_recovery())
        .default_options(EntryOptions::new(Duration::from_secs(60)))
        .try_build()
        .unwrap();
    cache.set("hot", 42).await.unwrap();
    assert_eq!(
        cache
            .get_or_set("hot", amalgam::source::value(99))
            .await
            .unwrap(),
        42
    );
    let calls = store.calls.load(Ordering::SeqCst);
    for _ in 0..3 {
        clock.advance(Duration::from_secs(2));
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::task::yield_now().await;
        assert_eq!(
            cache
                .get_or_set(
                    "hot",
                    amalgam::source::factory(|ctx| async move {
                        Ok::<_, amalgam::FactoryError>(ctx.value(7))
                    })
                )
                .await
                .unwrap(),
            42
        );
    }
    assert_eq!(store.calls.load(Ordering::SeqCst), calls);
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn default_expire_removes_l2_and_retains_the_eligible_l1_fallback() {
    let clock = Arc::new(ManualClock::default());
    let store = Arc::new(InMemoryDistributedCache::new(clock.clone()));
    let options = EntryOptions::new(Duration::from_secs(60)).with_fail_safe(true, None, None);
    let cache = Cache::<u64>::builder()
        .clock(clock)
        .distributed(store.clone())
        .serializer(Arc::new(JsonSerializer))
        .default_options(options.clone())
        .auto_recovery(no_recovery())
        .try_build()
        .unwrap();
    cache.set("hot", 42).await.unwrap();
    assert!(store.get("v2:hot").await.unwrap().is_some());
    cache
        .expire("hot")
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert_eq!(store.get("v2:hot").await.unwrap(), None);
    assert_eq!(
        cache
            .try_get("hot")
            .options(|_| options.with_allow_stale_on_read_only(true))
            .await
            .unwrap(),
        Some(42)
    );
    cache.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_fail_safe_default_without_a_stale_entry_does_not_enable_soft_timeout() {
    let options = EntryOptions::new(Duration::from_secs(60))
        .with_fail_safe(true, None, None)
        .with_factory_timeouts(
            Timeout::After(Duration::from_millis(5)),
            Timeout::After(Duration::from_secs(1)),
            true,
        );
    let cache = Cache::<u64>::builder()
        .default_options(options)
        .try_build()
        .unwrap();
    let value = cache
        .get_or_set(
            "cold",
            typed_factory(|ctx| async move {
                tokio::time::sleep(Duration::from_millis(100)).await;
                Ok::<_, amalgam::FactoryError>(ctx.value(7))
            }),
        )
        .fail_safe_default(Some(99))
        .await
        .unwrap();
    assert_eq!(value, 7);
    assert_eq!(cache.try_get("cold").await.unwrap(), Some(7));
    cache.shutdown().await.unwrap();
}

#[test]
fn default_recovery_delay_is_five_seconds() {
    assert_eq!(RecoveryConfig::default().delay, Duration::from_secs(5));
}

#[test]
fn initial_subscription_wait_is_opt_in_or_selected_by_strict() {
    let ordinary = Cache::<u64>::builder().try_build().unwrap();
    assert!(!ordinary.wait_for_initial_backplane_subscribe());
    let strict = Cache::<u64>::builder().strict().try_build().unwrap();
    assert!(strict.wait_for_initial_backplane_subscribe());
    let _ = ordinary.close();
    let _ = strict.close();
}

fn typed_factory<V, F, Fut>(factory: F) -> F
where
    F: FnOnce(amalgam::FactoryContext<V>) -> Fut,
    Fut: std::future::Future<Output = std::result::Result<V, amalgam::FactoryError>>,
{
    factory
}
