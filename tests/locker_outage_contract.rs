//! Lease admission and notification continuity have independent outage policies.
use amalgam::*;
use async_trait::async_trait;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::{broadcast, watch};

struct Store {
    inner: InMemoryDistributedCache,
    down: AtomicBool,
}
#[async_trait]
impl DistributedCache for Store {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        if self.down.load(Ordering::SeqCst) {
            Err(Error::Distributed("store unavailable".into()))
        } else {
            self.inner.get(key).await
        }
    }
    async fn set(&self, key: &str, bytes: Vec<u8>, ttl: Option<Duration>) -> Result<()> {
        if self.down.load(Ordering::SeqCst) {
            Err(Error::Distributed("store unavailable".into()))
        } else {
            self.inner.set(key, bytes, ttl).await
        }
    }
    async fn remove(&self, key: &str) -> Result<()> {
        self.inner.remove(key).await
    }
    fn invalidation_store(&self) -> Option<Arc<dyn InvalidationStore>> {
        self.inner.invalidation_store()
    }
}
#[derive(Default)]
struct FailedLocker {
    calls: AtomicUsize,
}
#[async_trait]
impl DistributedLocker for FailedLocker {
    async fn acquire(&self, _: &str, _: Duration, _: Timeout) -> Result<Option<String>> {
        unreachable!("caller-selected acquisition must use the owned hook")
    }
    async fn release(&self, _: &str, _: &str) -> Result<()> {
        Ok(())
    }
    fn lease_support(&self) -> LeaseSupport {
        LeaseSupport::FixedTtl
    }
    fn token_acquisition(&self) -> TokenAcquisition {
        TokenAcquisition::CallerSelected
    }
    async fn acquire_with_token(
        &self,
        _: &str,
        _: &LeaseToken,
        _: LeaseTtl,
        _: Timeout,
    ) -> std::result::Result<bool, LeaseError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(LeaseError::backend(std::io::Error::other(
            "locker unavailable",
        )))
    }
}
struct Notifications {
    sender: broadcast::Sender<BackplaneMessage>,
    health: watch::Sender<BackplaneState>,
}
impl Notifications {
    fn new() -> Self {
        let (sender, _) = broadcast::channel(16);
        let (health, _) = watch::channel(BackplaneState::Connected {
            epoch: ContinuityEpoch::INITIAL,
        });
        Self { sender, health }
    }
    fn disconnect(&self) {
        self.health.send_replace(BackplaneState::Disconnected {
            epoch: ContinuityEpoch::INITIAL,
        });
    }
}
#[async_trait]
impl Backplane for Notifications {
    async fn publish(&self, _: BackplaneMessage) -> Result<()> {
        Ok(())
    }
    fn subscribe(&self) -> broadcast::Receiver<BackplaneMessage> {
        self.sender.subscribe()
    }
    fn connection_state(&self) -> Option<watch::Receiver<BackplaneState>> {
        Some(self.health.subscribe())
    }
}
#[derive(Clone, Copy)]
enum Coordination {
    Local,
    Fenced,
    Cooperative,
}
struct Fixture {
    cache: Cache<u64>,
    store: Arc<Store>,
    locker: Arc<FailedLocker>,
}
async fn fixture(
    coordination: Coordination,
    options: EntryOptions,
    notifications: Option<Arc<Notifications>>,
) -> Fixture {
    let clock = Arc::new(ManualClock::default());
    let store = Arc::new(Store {
        inner: InMemoryDistributedCache::new(clock.clone()),
        down: AtomicBool::new(false),
    });
    let locker = Arc::new(FailedLocker::default());
    let mut builder = Cache::builder()
        .clock(clock)
        .distributed(store.clone())
        .serializer(Arc::new(JsonSerializer))
        .default_options(options)
        .auto_recovery(RecoveryConfig {
            enabled: false,
            ..RecoveryConfig::default()
        });
    builder = match coordination {
        Coordination::Local => builder,
        // Leave the policy unspecified to exercise the actual default.
        Coordination::Fenced => builder.distributed_locker(locker.clone()),
        Coordination::Cooperative => builder
            .distributed_locker(locker.clone())
            .lease_policy(LeasePolicy::CooperativeLegacy),
    };
    if let Some(notifications) = notifications {
        builder = builder.backplane(notifications);
    }
    Fixture {
        cache: builder.try_build_ready().await.unwrap(),
        store,
        locker,
    }
}
async fn origin(cache: &Cache<u64>, calls: Arc<AtomicUsize>) -> Result<u64> {
    cache
        .get_or_set("key", move |ctx| async move {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(ctx.value(7))
        })
        .await
}
fn assert_lease_failure(result: Result<u64>) {
    assert!(
        matches!(result, Err(Error::Lease(LeaseError::Backend { source }))
        if source.to_string() == "locker unavailable")
    );
}
fn options() -> EntryOptions {
    EntryOptions::new(Duration::from_secs(60)).with_rethrow_distributed_locker_exceptions(false)
}

#[tokio::test]
async fn default_fenced_miss_rejects_failed_lease_even_when_rethrow_is_false() {
    let fixture = fixture(Coordination::Fenced, options(), None).await;
    let calls = Arc::new(AtomicUsize::new(0));
    assert_lease_failure(origin(&fixture.cache, calls.clone()).await);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.locker.calls.load(Ordering::SeqCst), 1);
    fixture.cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn cooperative_miss_can_run_origin_after_failed_lease() {
    let fixture = fixture(Coordination::Cooperative, options(), None).await;
    let calls = Arc::new(AtomicUsize::new(0));
    assert_eq!(origin(&fixture.cache, calls.clone()).await.unwrap(), 7);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.locker.calls.load(Ordering::SeqCst), 1);
    fixture.cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn cooperative_rethrow_rejects_origin_after_failed_lease() {
    let options = options().with_rethrow_distributed_locker_exceptions(true);
    let fixture = fixture(Coordination::Cooperative, options, None).await;
    let calls = Arc::new(AtomicUsize::new(0));
    assert_lease_failure(origin(&fixture.cache, calls.clone()).await);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    fixture.cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn fresh_l1_without_backplane_does_not_contact_failed_locker_or_store() {
    let fixture = fixture(Coordination::Fenced, options(), None).await;
    fixture
        .cache
        .try_set("key", 42)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    fixture.store.down.store(true, Ordering::SeqCst);
    let calls = Arc::new(AtomicUsize::new(0));
    assert_eq!(origin(&fixture.cache, calls.clone()).await.unwrap(), 42);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.locker.calls.load(Ordering::SeqCst), 0);
    fixture.cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_notification_gap_turns_old_l1_into_a_miss_under_each_lease_policy() {
    for coordination in [
        Coordination::Local,
        Coordination::Fenced,
        Coordination::Cooperative,
    ] {
        let notifications = Arc::new(Notifications::new());
        let fixture = fixture(coordination, options(), Some(notifications.clone())).await;
        fixture
            .cache
            .try_set("key", 42)
            .await
            .unwrap()
            .wait()
            .await
            .unwrap();
        fixture.store.down.store(true, Ordering::SeqCst);
        notifications.disconnect();
        let calls = Arc::new(AtomicUsize::new(0));
        let result = origin(&fixture.cache, calls.clone()).await;
        match coordination {
            Coordination::Fenced => {
                assert_lease_failure(result);
                assert_eq!(calls.load(Ordering::SeqCst), 0);
            }
            Coordination::Local | Coordination::Cooperative => {
                assert_eq!(result.unwrap(), 7);
                assert_eq!(calls.load(Ordering::SeqCst), 1);
            }
        }
        fixture.cache.shutdown().await.unwrap();
    }
}
