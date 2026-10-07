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
    reads: AtomicUsize,
}
#[async_trait]
impl DistributedCache for Store {
    fn fenced_write_support(&self) -> FencedWriteSupport {
        self.inner.fenced_write_support()
    }
    async fn write_with_lease(
        &self,
        key: &str,
        mutation: LeasedMutation,
        proof: &LeaseProof,
    ) -> std::result::Result<LeasedWriteOutcome, LeaseError> {
        if self.down.load(Ordering::SeqCst) {
            Err(LeaseError::backend(std::io::Error::other(
                "store unavailable",
            )))
        } else {
            self.inner.write_with_lease(key, mutation, proof).await
        }
    }
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
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
    fn reconnect(&self) {
        self.health.send_replace(BackplaneState::Connected {
            epoch: ContinuityEpoch::INITIAL.next().unwrap(),
        });
    }
    fn disconnect(&self) {
        self.health.send_replace(BackplaneState::Disconnected {
            epoch: ContinuityEpoch::INITIAL,
        });
    }
}
#[async_trait]
impl Backplane for Notifications {
    async fn publish(&self, message: BackplaneMessage) -> Result<()> {
        let _ = self.sender.send(message);
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
    Default,
    Local,
    Fenced,
    Cooperative,
}
struct Fixture {
    cache: Cache<u64>,
    store: Arc<Store>,
    locker: Arc<FailedLocker>,
    clock: Arc<ManualClock>,
}
async fn fixture(
    coordination: Coordination,
    options: EntryOptions,
    notifications: Option<Arc<Notifications>>,
) -> Fixture {
    fixture_with_reconciliation(coordination, options, notifications, None).await
}
async fn fixture_with_reconciliation(
    coordination: Coordination,
    options: EntryOptions,
    notifications: Option<Arc<Notifications>>,
    reconciliation: Option<ReconciliationPolicy>,
) -> Fixture {
    let clock = Arc::new(ManualClock::default());
    let store = Arc::new(Store {
        inner: InMemoryDistributedCache::new(clock.clone()),
        down: AtomicBool::new(false),
        reads: AtomicUsize::new(0),
    });
    let locker = Arc::new(FailedLocker::default());
    let mut builder = Cache::builder()
        .clock(clock.clone())
        .distributed(store.clone())
        .serializer(Arc::new(JsonSerializer))
        .default_options(options)
        .auto_recovery(RecoveryConfig {
            enabled: false,
            ..RecoveryConfig::default()
        });
    builder = match coordination {
        Coordination::Local => builder,
        Coordination::Default => builder.distributed_locker(locker.clone()),
        // Fencing is explicit; cooperative construction is now the default.
        Coordination::Fenced => builder
            .distributed_locker(locker.clone())
            .lease_policy(LeasePolicy::Fenced),
        Coordination::Cooperative => builder
            .distributed_locker(locker.clone())
            .lease_policy(LeasePolicy::Cooperative),
    };
    if let Some(notifications) = notifications {
        builder = builder.backplane(notifications);
    }
    if let Some(reconciliation) = reconciliation {
        builder = builder.reconciliation_policy(reconciliation);
    }
    Fixture {
        cache: builder.try_build_ready().await.unwrap(),
        store,
        locker,
        clock,
    }
}
async fn origin(cache: &Cache<u64>, calls: Arc<AtomicUsize>) -> Result<u64> {
    cache
        .get_or_set("key", move |ctx| async move {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok::<_, amalgam::FactoryError>(ctx.value(7))
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
async fn explicit_fenced_miss_rejects_failed_lease_even_when_rethrow_is_false() {
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
        let fixture = fixture_with_reconciliation(
            coordination,
            options(),
            Some(notifications.clone()),
            Some(ReconciliationPolicy::BackplaneContinuity),
        )
        .await;
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
            Coordination::Default | Coordination::Local | Coordination::Cooperative => {
                assert_eq!(result.unwrap(), 7);
                assert_eq!(calls.load(Ordering::SeqCst), 1);
            }
        }
        fixture.cache.shutdown().await.unwrap();
    }
}

async fn best_effort_fixture(
    coordination: Coordination,
    options: EntryOptions,
) -> (Fixture, Arc<Notifications>) {
    let notifications = Arc::new(Notifications::new());
    let fixture = fixture_with_reconciliation(
        coordination,
        options,
        Some(notifications.clone()),
        Some(ReconciliationPolicy::BackplaneBestEffort),
    )
    .await;
    (fixture, notifications)
}
async fn local_value(cache: &Cache<u64>, key: &str) -> Option<u64> {
    cache
        .read(key, Some(options().with_skip_distributed(true, false)))
        .await
        .unwrap()
        .value()
        .copied()
}

#[tokio::test]
async fn best_effort_keeps_local_l1_across_gap_and_reconnect_under_each_lease_policy() {
    for coordination in [
        Coordination::Local,
        Coordination::Fenced,
        Coordination::Cooperative,
    ] {
        let (fixture, notifications) = best_effort_fixture(coordination, options()).await;
        fixture
            .cache
            .try_set("key", 42)
            .await
            .unwrap()
            .wait()
            .await
            .unwrap();
        let reads = fixture.store.reads.load(Ordering::SeqCst);
        fixture.store.down.store(true, Ordering::SeqCst);
        notifications.disconnect();
        let calls = Arc::new(AtomicUsize::new(0));
        assert_eq!(origin(&fixture.cache, calls.clone()).await.unwrap(), 42);
        // A removal missed while disconnected is deliberately not inferred.
        fixture.store.inner.remove("v2:key").await.unwrap();
        notifications.reconnect();
        assert_eq!(origin(&fixture.cache, calls.clone()).await.unwrap(), 42);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.store.reads.load(Ordering::SeqCst), reads);
        assert_eq!(fixture.locker.calls.load(Ordering::SeqCst), 0);
        fixture.cache.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn best_effort_keeps_hydrated_l1_across_gap_in_bounded_and_unbounded_memory() {
    for bounded in [false, true] {
        let writer = fixture(Coordination::Local, options(), None).await;
        writer
            .cache
            .try_set("key", 42)
            .await
            .unwrap()
            .wait()
            .await
            .unwrap();
        let notifications = Arc::new(Notifications::new());
        let builder = Cache::<u64>::builder()
            .clock(writer.clock.clone())
            .distributed(writer.store.clone())
            .serializer(Arc::new(JsonSerializer))
            .backplane(notifications.clone())
            .reconciliation_policy(ReconciliationPolicy::BackplaneBestEffort)
            .default_options(options())
            .auto_recovery(RecoveryConfig {
                enabled: false,
                ..RecoveryConfig::default()
            });
        let reader = if bounded {
            builder.max_capacity(4)
        } else {
            builder
        };
        let reader = reader.try_build_ready().await.unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        assert_eq!(origin(&reader, calls.clone()).await.unwrap(), 42);
        let reads = writer.store.reads.load(Ordering::SeqCst);
        writer.store.down.store(true, Ordering::SeqCst);
        notifications.disconnect();
        assert_eq!(origin(&reader, calls.clone()).await.unwrap(), 42);
        notifications.reconnect();
        assert_eq!(origin(&reader, calls.clone()).await.unwrap(), 42);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(writer.store.reads.load(Ordering::SeqCst), reads);
        reader.shutdown().await.unwrap();
        writer.cache.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn best_effort_cooperative_cold_miss_obeys_the_rethrow_option() {
    for rethrow in [false, true] {
        let (fixture, notifications) = best_effort_fixture(
            Coordination::Cooperative,
            options().with_rethrow_distributed_locker_exceptions(rethrow),
        )
        .await;
        fixture.store.down.store(true, Ordering::SeqCst);
        notifications.disconnect();
        let calls = Arc::new(AtomicUsize::new(0));
        let result = origin(&fixture.cache, calls.clone()).await;
        if rethrow {
            assert_lease_failure(result);
            assert_eq!(calls.load(Ordering::SeqCst), 0);
        } else {
            assert_eq!(result.unwrap(), 7);
            assert_eq!(calls.load(Ordering::SeqCst), 1);
        }
        fixture.cache.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn best_effort_does_not_weaken_fenced_cold_admission() {
    let (fixture, notifications) = best_effort_fixture(Coordination::Fenced, options()).await;
    fixture.store.down.store(true, Ordering::SeqCst);
    notifications.disconnect();
    let calls = Arc::new(AtomicUsize::new(0));
    assert_lease_failure(origin(&fixture.cache, calls.clone()).await);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    fixture.cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn best_effort_fail_safe_keeps_stale_but_never_physically_dead_values() {
    let opts = EntryOptions::new(Duration::from_millis(100))
        .with_fail_safe(
            true,
            Some(Duration::from_secs(1)),
            Some(Duration::from_millis(100)),
        )
        .with_rethrow_distributed_locker_exceptions(false);
    let (fixture, notifications) = best_effort_fixture(Coordination::Cooperative, opts).await;
    fixture
        .cache
        .try_set("key", 42)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    fixture.clock.advance(Duration::from_millis(160));
    fixture.store.down.store(true, Ordering::SeqCst);
    notifications.disconnect();
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let stale = fixture
        .cache
        .get_or_set("key", move |ctx| async move {
            counted.fetch_add(1, Ordering::SeqCst);
            Err(ctx.fail("origin unavailable"))
        })
        .await
        .unwrap();
    assert_eq!(stale, 42);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    fixture.clock.advance(Duration::from_secs(2));
    assert_eq!(local_value(&fixture.cache, "key").await, None);
    assert_eq!(origin(&fixture.cache, calls.clone()).await.unwrap(), 7);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    fixture.cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn best_effort_does_not_serve_a_hot_value_to_an_explicitly_cancelled_caller() {
    let (fixture, notifications) = best_effort_fixture(Coordination::Cooperative, options()).await;
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
    let source = CancellationSource::new();
    source.cancel();
    let result = fixture
        .cache
        .get_or_set_cancellable(
            "key",
            |ctx| async move { Ok::<_, amalgam::FactoryError>(ctx.value(7)) },
            source.token(),
        )
        .await;
    assert!(matches!(
        result,
        Err(Error::OperationCancelled {
            reason: FactoryCancellationReason::CallerCancelled,
        })
    ));
    assert_eq!(local_value(&fixture.cache, "key").await, Some(42));
    fixture.cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn best_effort_still_applies_local_remove_tag_and_clear_during_a_gap() {
    let (fixture, notifications) = best_effort_fixture(Coordination::Local, options()).await;
    let tag = Tag::new("group").unwrap();
    fixture
        .cache
        .try_set("removed", 1)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    fixture
        .cache
        .try_set_full("tagged", 2, None, Box::from([tag.clone()]))
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    fixture
        .cache
        .try_set("cleared", 3)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    fixture.store.down.store(true, Ordering::SeqCst);
    notifications.disconnect();
    fixture.clock.advance(Duration::from_millis(1));
    fixture
        .cache
        .try_remove("removed")
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    fixture
        .cache
        .try_remove_by_tag(tag)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert_eq!(local_value(&fixture.cache, "removed").await, None);
    assert_eq!(local_value(&fixture.cache, "tagged").await, None);
    assert_eq!(local_value(&fixture.cache, "cleared").await, Some(3));
    fixture
        .cache
        .try_clear(ClearMode::Remove)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert_eq!(local_value(&fixture.cache, "cleared").await, None);
    fixture.cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn best_effort_retains_l1_after_malformed_frames_but_applies_received_removes() {
    let clock = Arc::new(ManualClock::default());
    let notifications = Arc::new(InProcessBackplane::default());
    let cache = Cache::<u64>::builder()
        .clock(clock.clone())
        .backplane(notifications.clone())
        .reconciliation_policy(ReconciliationPolicy::BackplaneBestEffort)
        .auto_recovery(RecoveryConfig {
            enabled: false,
            ..RecoveryConfig::default()
        })
        .default_options(options())
        .try_build_ready()
        .await
        .unwrap();
    cache
        .try_set("key", 42)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    cache
        .try_set("probe", 1)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    notifications
        .publish(BackplaneMessage {
            source_id: "\u{1f}amalgam-control-v2:zz".into(),
            timestamp: clock.now(),
            action: BackplaneAction::Set,
            key: "v2:key".into(),
        })
        .await
        .unwrap();
    notifications
        .publish(BackplaneMessage {
            source_id: "remote".into(),
            timestamp: clock.now(),
            action: BackplaneAction::Remove,
            key: "v2:probe".into(),
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while local_value(&cache, "probe").await.is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(local_value(&cache, "key").await, Some(42));
    cache.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn best_effort_healthless_backplanes_do_not_add_periodic_l1_discard() {
    let clock = Arc::new(ManualClock::default());
    let notifications = Arc::new(InProcessBackplane::default());
    let mut caches = Vec::with_capacity(2);
    for best_effort in [false, true] {
        let builder = Cache::<u64>::builder()
            .clock(clock.clone())
            .backplane(notifications.clone())
            .auto_recovery(RecoveryConfig {
                enabled: false,
                ..RecoveryConfig::default()
            });
        let builder = if best_effort {
            builder.reconciliation_policy(ReconciliationPolicy::BackplaneBestEffort)
        } else {
            builder
        };
        let cache = builder.try_build_ready().await.unwrap();
        cache
            .try_set("key", 42)
            .await
            .unwrap()
            .wait()
            .await
            .unwrap();
        caches.push(cache);
    }
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(2)).await;
    tokio::task::yield_now().await;
    assert_eq!(local_value(&caches[0], "key").await, None);
    assert_eq!(local_value(&caches[1], "key").await, Some(42));
    for cache in caches {
        cache.shutdown().await.unwrap();
    }
}

#[test]
fn best_effort_reconciliation_requires_a_backplane() {
    let result = Cache::<u64>::builder()
        .reconciliation_policy(ReconciliationPolicy::BackplaneBestEffort)
        .try_build();
    assert!(matches!(
        result,
        Err(Error::Config(
            ConfigError::BestEffortReconciliationWithoutBackplane
        ))
    ));
}

#[tokio::test]
async fn default_profile_keeps_hot_l1_and_computes_cold_misses_during_full_outage() {
    let notifications = Arc::new(Notifications::new());
    let fixture = fixture(
        Coordination::Default,
        options(),
        Some(notifications.clone()),
    )
    .await;
    fixture.cache.set("key", 42).await.unwrap();
    fixture.store.down.store(true, Ordering::SeqCst);
    notifications.disconnect();
    let calls = Arc::new(AtomicUsize::new(0));
    assert_eq!(origin(&fixture.cache, calls.clone()).await.unwrap(), 42);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.locker.calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        fixture
            .cache
            .get_or_set("cold", |ctx| async move {
                Ok::<_, amalgam::FactoryError>(ctx.value(7))
            })
            .await
            .unwrap(),
        7
    );
    assert_eq!(fixture.locker.calls.load(Ordering::SeqCst), 1);
    fixture.cache.shutdown().await.unwrap();
}
