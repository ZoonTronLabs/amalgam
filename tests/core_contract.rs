//! Canonical pipeline, lifecycle and boundary acceptance.
use amalgam::*;
use async_trait::async_trait;
use std::future::{Future, pending, poll_fn};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::Poll;
use std::time::Duration;
use tokio::sync::{Notify, Semaphore, oneshot};

fn opts() -> EntryOptions {
    EntryOptions::new(Duration::from_secs(60)).with_allow_background_backplane_operations(false)
}
fn no_recovery() -> RecoveryConfig {
    RecoveryConfig {
        enabled: false,
        ..RecoveryConfig::default()
    }
}
struct DropSignal(Arc<AtomicUsize>);
impl Drop for DropSignal {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn close_cancels_a_parked_origin_and_finishes_its_observer_before_repoll() {
    let cache = Cache::<i32>::new();
    let dropped = Arc::new(AtomicUsize::new(0));
    let origin_drop = dropped.clone();
    let (token_tx, mut token_rx) = oneshot::channel();
    let mut events = cache.events().subscribe();
    let mut operation = Box::pin(cache.get_or_set("parked", move |ctx| async move {
        let _drop = DropSignal(origin_drop);
        token_tx.send(ctx.cancellation().clone()).unwrap();
        pending::<()>().await;
        Ok(ctx.value(1))
    }));
    poll_fn(|cx| {
        assert!(operation.as_mut().poll(cx).is_pending());
        if token_rx.try_recv().is_ok() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
    assert_eq!(cache.close(), CloseOutcome::Started);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    let mut completions = 0;
    while let Ok(event) = events.try_recv() {
        if matches!(
            event,
            CacheEvent::OperationCompleted {
                operation: CacheOperation::GetOrSet,
                outcome: OperationOutcome::Cancelled,
                ..
            }
        ) {
            completions += 1;
        }
    }
    assert_eq!(completions, 1);
    cache.shutdown().await.unwrap();
    cache.shutdown().await.unwrap();
    assert!(matches!(
        operation.await,
        Err(Error::OperationCancelled {
            reason: FactoryCancellationReason::CacheShutdown
        })
    ));
    assert!(matches!(
        cache.read("any", None).await,
        Err(Error::CacheClosed)
    ));
}

#[tokio::test]
async fn soft_timeout_disposes_origin_with_precise_reason_and_retains_failsafe() {
    let clock = Arc::new(ManualClock::default());
    let options = opts()
        .with_duration(Duration::from_secs(1))
        .with_fail_safe(true, Some(Duration::from_secs(100)), None)
        .with_factory_timeouts(
            Timeout::After(Duration::from_millis(5)),
            Timeout::After(Duration::from_secs(1)),
            false,
        );
    let cache = Cache::<i32>::builder()
        .clock(clock.clone())
        .default_options(options)
        .try_build()
        .unwrap();
    cache.try_set("k", 1).await.unwrap().wait().await.unwrap();
    clock.advance(Duration::from_secs(2));
    let (token_tx, token_rx) = oneshot::channel();
    let dropped = Arc::new(AtomicUsize::new(0));
    let origin_drop = dropped.clone();
    let result = cache
        .get_or_set("k", move |ctx| async move {
            let _drop = DropSignal(origin_drop);
            token_tx.send(ctx.cancellation().clone()).unwrap();
            pending::<()>().await;
            Ok(ctx.value(2))
        })
        .await
        .unwrap();
    assert_eq!(result, 1);
    assert_eq!(
        token_rx.await.unwrap().cancelled().await,
        FactoryCancellationReason::SoftTimeout
    );
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn explicit_cancel_bypasses_failsafe_and_releases_origin() {
    let clock = Arc::new(ManualClock::default());
    let cache = Cache::<i32>::builder()
        .clock(clock.clone())
        .default_options(opts().with_duration(Duration::from_secs(1)).with_fail_safe(
            true,
            Some(Duration::from_secs(100)),
            None,
        ))
        .try_build()
        .unwrap();
    cache.try_set("k", 1).await.unwrap().wait().await.unwrap();
    clock.advance(Duration::from_secs(2));
    let source = CancellationSource::new();
    let (entered_tx, entered_rx) = oneshot::channel();
    let caller = {
        let cache = cache.clone();
        let token = source.token();
        tokio::spawn(async move {
            cache
                .get_or_set_cancellable(
                    "k",
                    move |ctx| async move {
                        entered_tx.send(()).unwrap();
                        pending::<()>().await;
                        Ok(ctx.value(2))
                    },
                    token,
                )
                .await
        })
    };
    entered_rx.await.unwrap();
    source.cancel();
    assert!(matches!(
        caller.await.unwrap(),
        Err(Error::OperationCancelled {
            reason: FactoryCancellationReason::CallerCancelled
        })
    ));
    assert_eq!(cache.get_or_set_value("k", 3, None).await.unwrap(), 3);
    cache.shutdown().await.unwrap();
}

struct FailingIo;
#[async_trait]
impl DistributedCache for FailingIo {
    async fn get(&self, _: &str) -> Result<Option<Vec<u8>>> {
        Err(Error::Distributed("actual read cause".into()))
    }
    async fn set(&self, _: &str, _: Vec<u8>, _: Option<Duration>) -> Result<()> {
        Err(Error::Distributed("actual write cause".into()))
    }
    async fn remove(&self, _: &str) -> Result<()> {
        Err(Error::Distributed("actual remove cause".into()))
    }
}
fn failing_cache(options: EntryOptions) -> Cache<i32> {
    Cache::builder()
        .distributed(Arc::new(FailingIo))
        .serializer(Arc::new(JsonSerializer))
        .default_options(options)
        .auto_recovery(no_recovery())
        .try_build()
        .unwrap()
}
#[tokio::test]
async fn canonical_reads_preserve_error_and_default_only_on_successful_miss() {
    let cache = failing_cache(opts().with_rethrow_distributed_exceptions(true));
    assert!(
        matches!(cache.read("k",None).await,Err(Error::Distributed(message)) if message=="actual read cause")
    );
    assert!(
        matches!(cache.read_or_default("k",42,None).await,Err(Error::Distributed(message)) if message=="actual read cause")
    );
    cache.shutdown().await.unwrap();
}
#[tokio::test]
async fn mutation_receipts_preserve_suppressed_and_rethrown_failures() {
    let cache = failing_cache(opts());
    let report = cache.try_set("k", 1).await.unwrap().wait().await.unwrap();
    assert!(
        matches!(report.distributed,EffectOutcome::FailedSuppressed {cause:Error::Distributed(message)} if message=="actual write cause")
    );
    assert_eq!(cache.read("k", None).await.unwrap().value(), Some(&1));
    let error = cache
        .try_remove_with("k", Some(opts().with_rethrow_distributed_exceptions(true)))
        .await
        .unwrap_err();
    assert!(matches!(error,Error::Distributed(message) if message=="actual remove cause"));
    cache.shutdown().await.unwrap();
}

struct GatedWrite {
    inner: InMemoryDistributedCache,
    started: Notify,
    release: Semaphore,
    pause: AtomicBool,
}
#[async_trait]
impl DistributedCache for GatedWrite {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.inner.get(key).await
    }
    async fn set(&self, key: &str, value: Vec<u8>, ttl: Option<Duration>) -> Result<()> {
        if self.pause.swap(false, Ordering::SeqCst) {
            self.started.notify_one();
            self.release.acquire().await.unwrap().forget();
        }
        self.inner.set(key, value, ttl).await
    }
    async fn remove(&self, key: &str) -> Result<()> {
        self.inner.remove(key).await
    }
}
#[tokio::test]
async fn scheduled_receipt_has_owned_completion_and_flush_ignores_listener_services() {
    let clock = Arc::new(ManualClock::default());
    let l2 = Arc::new(GatedWrite {
        inner: InMemoryDistributedCache::new(clock.clone()),
        started: Notify::new(),
        release: Semaphore::new(0),
        pause: AtomicBool::new(true),
    });
    let backplane = Arc::new(InProcessBackplane::default());
    let mut messages = backplane.subscribe();
    let cache = Cache::<i32>::builder()
        .clock(clock)
        .distributed(l2.clone())
        .serializer(Arc::new(JsonSerializer))
        .backplane(backplane)
        .default_options(opts().with_allow_background_distributed_operations(true))
        .auto_recovery(no_recovery())
        .try_build()
        .unwrap();
    let source = CancellationSource::new();
    let receipt = cache
        .try_set_full_cancellable("k", 7, None, Box::from([]), source.token())
        .await
        .unwrap();
    assert!(matches!(receipt, MutationReceipt::Scheduled(_)));
    l2.started.notified().await;
    source.cancel();
    assert!(messages.try_recv().is_err());
    l2.release.add_permits(1);
    let report = receipt.wait().await.unwrap();
    assert!(matches!(report.distributed, EffectOutcome::Applied));
    assert!(messages.recv().await.is_ok());
    tokio::time::timeout(Duration::from_millis(100), cache.flush_pending())
        .await
        .unwrap()
        .unwrap();
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn shutdown_cancels_scheduled_commit_and_retains_typed_completion() {
    let l2 = Arc::new(GatedWrite {
        inner: InMemoryDistributedCache::new(Arc::new(SystemClock)),
        started: Notify::new(),
        release: Semaphore::new(0),
        pause: AtomicBool::new(true),
    });
    let cache = Cache::<i32>::builder()
        .distributed(l2.clone())
        .serializer(Arc::new(JsonSerializer))
        .default_options(opts().with_allow_background_distributed_operations(true))
        .auto_recovery(no_recovery())
        .try_build()
        .unwrap();
    let receipt = cache.try_set("k", 7).await.unwrap();
    l2.started.notified().await;
    cache.shutdown().await.unwrap();
    assert!(matches!(
        receipt.wait().await,
        Err(Error::OperationCancelled {
            reason: FactoryCancellationReason::CacheShutdown
        })
    ));
    assert!(l2.get("v2:k").await.unwrap().is_none());
}

#[derive(Default)]
struct CountPlugin {
    start: AtomicUsize,
    stop: AtomicUsize,
    events: AtomicUsize,
}
impl Plugin for CountPlugin {
    fn name(&self) -> &str {
        "dynamic"
    }
    fn on_start(&self) {
        self.start.fetch_add(1, Ordering::SeqCst);
    }
    fn on_stop(&self) {
        self.stop.fetch_add(1, Ordering::SeqCst);
    }
    fn on_event(&self, _: &CacheEvent) {
        self.events.fetch_add(1, Ordering::SeqCst);
    }
}
#[tokio::test]
async fn dynamic_registration_stops_once_and_is_owned_by_cache_shutdown() {
    let cache = Cache::<i32>::new();
    let plugin = Arc::new(CountPlugin::default());
    let registration = cache.register_plugin(plugin.clone()).unwrap();
    assert_eq!(plugin.start.load(Ordering::SeqCst), 1);
    cache.try_set("k", 1).await.unwrap();
    assert!(plugin.events.load(Ordering::SeqCst) > 0);
    registration.stop().unwrap();
    registration.wait_stopped().await.unwrap();
    cache.shutdown().await.unwrap();
    assert_eq!(plugin.stop.load(Ordering::SeqCst), 1);
    assert!(matches!(
        cache.register_plugin(plugin),
        Err(Error::CacheClosed)
    ));
}

#[tokio::test]
async fn plain_memory_expiry_uses_controlled_clock_and_never_periodic_invalidation() {
    let clock = Arc::new(ManualClock::default());
    let cache = Cache::<i32>::builder()
        .clock(clock.clone())
        .default_options(EntryOptions::new(Duration::from_millis(20)))
        .try_build()
        .unwrap();
    cache.try_set("k", 1).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(cache.read("k", None).await.unwrap().value(), Some(&1));
    clock.advance(Duration::from_millis(21));
    assert!(!cache.read("k", None).await.unwrap().has_value());
    cache.shutdown().await.unwrap();
}
#[tokio::test]
async fn huge_finite_budget_is_typed_before_origin_or_storage_effects() {
    let huge = EntryOptions::default().with_factory_timeouts(
        Timeout::Infinite,
        Timeout::After(Duration::MAX),
        false,
    );
    let cache = Cache::<i32>::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let origin_calls = calls.clone();
    assert!(matches!(
        cache
            .get_or_set_with(
                "k",
                move |ctx| async move {
                    origin_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(ctx.value(1))
                },
                huge.clone()
            )
            .await,
        Err(Error::Config(ConfigError::DeadlineOutOfRange))
    ));
    assert!(matches!(
        cache.try_set_full("k", 1, Some(huge), Box::from([])).await,
        Err(Error::Config(ConfigError::DeadlineOutOfRange))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(!cache.read("k", None).await.unwrap().has_value());
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn adaptive_validation_and_raw_tag_failure_do_not_store_invalid_products() {
    let cache = Cache::<i32>::new();
    let result = cache
        .get_or_set("bad-size", |mut ctx| async move {
            ctx.adapt(|opts| opts.with_size(-1));
            Ok(ctx.value(1))
        })
        .await;
    assert!(matches!(
        result,
        Err(Error::Config(ConfigError::NegativeEntryWeight { .. }))
    ));
    let result = cache
        .get_or_set("bad-tag", |mut ctx| async move {
            ctx.set_tags(["valid", " "]);
            Ok(ctx.value(1))
        })
        .await;
    assert!(matches!(result, Err(Error::Tag(TagError::Blank))));
    assert!(!cache.read("bad-size", None).await.unwrap().has_value());
    assert!(!cache.read("bad-tag", None).await.unwrap().has_value());
    cache.shutdown().await.unwrap();
}

struct LostAtWrite {
    inner: InMemoryDistributedCache,
    attempts: AtomicUsize,
}
#[async_trait]
impl DistributedCache for LostAtWrite {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.inner.get(key).await
    }
    async fn set(&self, key: &str, bytes: Vec<u8>, ttl: Option<Duration>) -> Result<()> {
        self.inner.set(key, bytes, ttl).await
    }
    async fn remove(&self, key: &str) -> Result<()> {
        self.inner.remove(key).await
    }
    async fn write_with_lease(
        &self,
        _: &str,
        _: LeasedMutation,
        _: &LeaseProof,
    ) -> std::result::Result<LeasedWriteOutcome, LeaseError> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        Ok(LeasedWriteOutcome::LeaseLost)
    }
}
#[tokio::test]
async fn native_backend_fence_loss_does_not_commit_l1_l2_or_publish() {
    let clock = Arc::new(ManualClock::default());
    let l2 = Arc::new(LostAtWrite {
        inner: InMemoryDistributedCache::new(clock.clone()),
        attempts: AtomicUsize::new(0),
    });
    let locker = Arc::new(InMemoryDistributedLocker::new(clock.clone()));
    let backplane = Arc::new(InProcessBackplane::default());
    let mut messages = backplane.subscribe();
    let cache = Cache::<i32>::builder()
        .clock(clock)
        .distributed(l2.clone())
        .serializer(Arc::new(JsonSerializer))
        .distributed_locker(locker.clone())
        .backplane(backplane)
        .default_options(opts())
        .auto_recovery(no_recovery())
        .try_build()
        .unwrap();
    let (token_tx, token_rx) = oneshot::channel();
    let result = cache
        .get_or_set("k", move |ctx| async move {
            token_tx.send(ctx.cancellation().clone()).unwrap();
            Ok(ctx.value(1))
        })
        .await;
    assert!(matches!(result, Err(Error::Lease(LeaseError::Lost))));
    assert_eq!(
        token_rx.await.unwrap().cancelled().await,
        FactoryCancellationReason::LeaseLost
    );
    assert_eq!(l2.attempts.load(Ordering::SeqCst), 1);
    assert!(
        !cache
            .read("k", Some(opts().with_skip_distributed(true, false)))
            .await
            .unwrap()
            .has_value()
    );
    assert!(l2.get("v2:k").await.unwrap().is_none());
    assert!(messages.try_recv().is_err());
    cache.shutdown().await.unwrap();
    assert_eq!(locker.held_count(), 0);
}

#[test]
fn system_clock_and_disabled_recovery_build_on_std_thread() {
    std::thread::spawn(|| {
        let cache = Cache::<i32>::builder()
            .clock(Arc::new(SystemClock))
            .auto_recovery(no_recovery())
            .try_build()
            .unwrap();
        drop(cache);
    })
    .join()
    .unwrap();
}

struct RecoveryIo {
    inner: InMemoryDistributedCache,
    down: AtomicBool,
    read_down: AtomicBool,
    gate_next: AtomicBool,
    entered: Notify,
    gate: Semaphore,
    commits: AtomicUsize,
}
impl RecoveryIo {
    fn new(clock: Arc<dyn Clock>) -> Self {
        Self {
            inner: InMemoryDistributedCache::new(clock),
            down: AtomicBool::new(false),
            read_down: AtomicBool::new(false),
            gate_next: AtomicBool::new(false),
            entered: Notify::new(),
            gate: Semaphore::new(0),
            commits: AtomicUsize::new(0),
        }
    }
    async fn before_commit(&self) -> Result<()> {
        if self.down.load(Ordering::SeqCst) {
            return Err(Error::Distributed("initial fault".into()));
        }
        if self.gate_next.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            self.gate.acquire().await.unwrap().forget();
        }
        Ok(())
    }
}
#[async_trait]
impl DistributedCache for RecoveryIo {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        if self.read_down.load(Ordering::SeqCst) {
            Err(Error::Distributed("reconciliation unavailable".into()))
        } else {
            self.inner.get(key).await
        }
    }
    async fn set(&self, key: &str, bytes: Vec<u8>, ttl: Option<Duration>) -> Result<()> {
        self.before_commit().await?;
        self.inner.set(key, bytes, ttl).await?;
        self.commits.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn remove(&self, key: &str) -> Result<()> {
        self.before_commit().await?;
        self.inner.remove(key).await?;
        self.commits.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn invalidation_store(&self) -> Option<Arc<dyn InvalidationStore>> {
        self.inner.invalidation_store()
    }
}
fn recovery_config() -> RecoveryConfig {
    RecoveryConfig {
        delay: Duration::from_millis(20),
        ..RecoveryConfig::default()
    }
}
async fn until(mut predicate: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while !predicate() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn newer_completed_set_remains_after_already_started_set_or_remove_recovery() {
    for old_remove in [false, true] {
        for notifications in [false, true] {
            let clock = Arc::new(ManualClock::default());
            let backend = Arc::new(RecoveryIo::new(clock.clone()));
            let bp = Arc::new(InProcessBackplane::default());
            let mut messages = bp.subscribe();
            let mut builder = Cache::<i32>::builder()
                .clock(clock.clone())
                .distributed(backend.clone())
                .serializer(Arc::new(JsonSerializer))
                .default_options(opts())
                .auto_recovery(recovery_config());
            if notifications {
                builder = builder.backplane(bp.clone());
            }
            let cache = builder.try_build().unwrap();
            cache.try_set("k", 0).await.unwrap().wait().await.unwrap();
            while messages.try_recv().is_ok() {}
            clock.advance(Duration::from_secs(1));
            backend.down.store(true, Ordering::SeqCst);
            if old_remove {
                cache.try_remove("k").await.unwrap().wait().await.unwrap();
            } else {
                cache.try_set("k", 1).await.unwrap().wait().await.unwrap();
            }
            let ticket = cache.recovery_ticket("k").unwrap();
            backend.gate_next.store(true, Ordering::SeqCst);
            backend.down.store(false, Ordering::SeqCst);
            tokio::time::timeout(Duration::from_secs(2), backend.entered.notified())
                .await
                .unwrap();
            clock.advance(Duration::from_secs(1));
            let newer = {
                let cache = cache.clone();
                tokio::spawn(
                    async move { cache.try_set("k", 2).await.unwrap().wait().await.unwrap() },
                )
            };
            tokio::time::sleep(Duration::from_millis(10)).await;
            assert!(
                !newer.is_finished(),
                "newer commit must wait the actual older I/O lane"
            );
            backend.gate.add_permits(1);
            newer.await.unwrap();
            assert!(!ticket.fence_is_current());
            tokio::time::sleep(Duration::from_millis(70)).await;
            let bytes = backend.get("v2:k").await.unwrap().unwrap();
            let snapshot: DistributedSnapshot<i32> =
                JsonSerializer.deserialize_snapshot(&bytes).unwrap();
            assert_eq!(snapshot.entry().value, 2);
            assert_eq!(cache.pending_recovery(), 0);
            let mut last = Timestamp::MIN;
            while let Ok(message) = messages.try_recv() {
                assert!(message.timestamp >= last);
                last = message.timestamp;
            }
            cache.shutdown().await.unwrap();
        }
    }
}

struct HealthBackplane {
    inner: InProcessBackplane,
    state: tokio::sync::watch::Sender<BackplaneState>,
    published: AtomicUsize,
}
impl HealthBackplane {
    fn new() -> Self {
        Self {
            inner: InProcessBackplane::default(),
            state: tokio::sync::watch::channel(BackplaneState::Connected {
                epoch: ContinuityEpoch::INITIAL,
            })
            .0,
            published: AtomicUsize::new(0),
        }
    }
}
#[async_trait]
impl Backplane for HealthBackplane {
    async fn publish(&self, message: BackplaneMessage) -> Result<()> {
        if !matches!(*self.state.borrow(), BackplaneState::Connected { .. }) {
            return Err(Error::Backplane("gap".into()));
        }
        self.published.fetch_add(1, Ordering::SeqCst);
        self.inner.publish(message).await
    }
    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<BackplaneMessage> {
        self.inner.subscribe()
    }
    fn connection_state(&self) -> Option<tokio::sync::watch::Receiver<BackplaneState>> {
        Some(self.state.subscribe())
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn coalesced_gap_retains_exact_ticket_until_successful_l2_and_marker_reconciliation() {
    let clock = Arc::new(ManualClock::default());
    let backend = Arc::new(RecoveryIo::new(clock.clone()));
    backend.down.store(true, Ordering::SeqCst);
    let bp = Arc::new(HealthBackplane::new());
    let cache = Cache::<i32>::builder()
        .clock(clock)
        .distributed(backend.clone())
        .serializer(Arc::new(JsonSerializer))
        .backplane(bp.clone())
        .default_options(opts())
        .auto_recovery(recovery_config())
        .try_build()
        .unwrap();
    cache.try_set("k", 7).await.unwrap().wait().await.unwrap();
    let ticket = cache.recovery_ticket("k").unwrap();
    let next = ContinuityEpoch::INITIAL.next().unwrap();
    bp.state
        .send_replace(BackplaneState::Disconnected { epoch: next });
    bp.state
        .send_replace(BackplaneState::Connected { epoch: next });
    backend.down.store(false, Ordering::SeqCst);
    backend.read_down.store(true, Ordering::SeqCst);
    cache
        .read(
            "gap-trigger",
            Some(opts().with_skip_distributed(true, false)),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        cache.recovery_ticket("k").unwrap().identity(),
        ticket.identity()
    );
    assert!(ticket.fence_is_current());
    assert_eq!(backend.commits.load(Ordering::SeqCst), 0);
    assert_eq!(bp.published.load(Ordering::SeqCst), 0);
    backend.read_down.store(false, Ordering::SeqCst);
    until(|| cache.pending_recovery() == 0).await;
    assert_eq!(backend.commits.load(Ordering::SeqCst), 1);
    assert_eq!(bp.published.load(Ordering::SeqCst), 1);
    let stored: DistributedSnapshot<i32> = JsonSerializer
        .deserialize_snapshot(&backend.get("v2:k").await.unwrap().unwrap())
        .unwrap();
    assert_eq!(stored.entry().value, 7);
    cache.shutdown().await.unwrap();
}

struct FencedFault {
    inner: InMemoryDistributedCache,
    first: AtomicBool,
    fenced: AtomicUsize,
    ordinary: AtomicUsize,
    replay_entered: tokio::sync::Notify,
    replay_release: tokio::sync::Notify,
}
#[async_trait]
impl DistributedCache for FencedFault {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.inner.get(key).await
    }
    async fn set(&self, key: &str, bytes: Vec<u8>, ttl: Option<Duration>) -> Result<()> {
        self.ordinary.fetch_add(1, Ordering::SeqCst);
        self.inner.set(key, bytes, ttl).await
    }
    async fn remove(&self, key: &str) -> Result<()> {
        self.inner.remove(key).await
    }
    async fn write_with_lease(
        &self,
        key: &str,
        mutation: LeasedMutation,
        proof: &LeaseProof,
    ) -> std::result::Result<LeasedWriteOutcome, LeaseError> {
        self.fenced.fetch_add(1, Ordering::SeqCst);
        if self.first.swap(false, Ordering::SeqCst) {
            Err(LeaseError::backend(std::io::Error::other(
                "fault before commit",
            )))
        } else {
            self.replay_entered.notify_one();
            self.replay_release.notified().await;
            self.inner.write_with_lease(key, mutation, proof).await
        }
    }
    fn invalidation_store(&self) -> Option<Arc<dyn InvalidationStore>> {
        self.inner.invalidation_store()
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_origin_commit_recovery_reacquires_and_uses_native_fenced_write() {
    let clock = Arc::new(ManualClock::default());
    let backend = Arc::new(FencedFault {
        inner: InMemoryDistributedCache::new(clock.clone()),
        first: AtomicBool::new(true),
        fenced: AtomicUsize::new(0),
        ordinary: AtomicUsize::new(0),
        replay_entered: tokio::sync::Notify::new(),
        replay_release: tokio::sync::Notify::new(),
    });
    let locker = Arc::new(InMemoryDistributedLocker::new(clock.clone()));
    let cache = Cache::<i32>::builder()
        .clock(clock)
        .distributed(backend.clone())
        .serializer(Arc::new(JsonSerializer))
        .distributed_locker(locker.clone())
        .default_options(opts())
        .auto_recovery(recovery_config())
        .try_build()
        .unwrap();
    assert_eq!(cache.get_or_set_value("k", 7, None).await.unwrap(), 7);
    tokio::time::timeout(Duration::from_secs(2), backend.replay_entered.notified())
        .await
        .expect("recovery must enter the second fenced write");
    assert!(matches!(
        cache.recovery_ticket("k").unwrap().work(),
        RecoveryWork::Data {
            mutation: PendingMutation::FencedCommit { .. },
            ..
        }
    ));
    backend.replay_release.notify_one();
    until(|| cache.pending_recovery() == 0).await;
    assert_eq!(backend.fenced.load(Ordering::SeqCst), 2);
    assert_eq!(backend.ordinary.load(Ordering::SeqCst), 0);
    let stored: DistributedSnapshot<i32> = JsonSerializer
        .deserialize_snapshot(&backend.get("v2:k").await.unwrap().unwrap())
        .unwrap();
    assert_eq!(stored.entry().value, 7);
    cache.shutdown().await.unwrap();
    assert_eq!(locker.held_count(), 0);
}

#[tokio::test]
async fn initial_native_ack_gates_operations_and_close_releases_parked_admission() {
    let bp = Arc::new(HealthBackplane::new());
    bp.state.send_replace(BackplaneState::Disconnected {
        epoch: ContinuityEpoch::INITIAL,
    });
    let cache = Cache::<i32>::builder()
        .backplane(bp.clone())
        .default_options(opts())
        .auto_recovery(no_recovery())
        .try_build()
        .unwrap();
    let caller = {
        let cache = cache.clone();
        tokio::spawn(async move { cache.try_set("k", 1).await })
    };
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(!caller.is_finished());
    bp.state.send_replace(BackplaneState::Connected {
        epoch: ContinuityEpoch::INITIAL,
    });
    caller.await.unwrap().unwrap().wait().await.unwrap();
    assert!(matches!(
        cache.ready().await.unwrap(),
        BackplaneReadiness::Acknowledged(_)
    ));
    cache.shutdown().await.unwrap();
}

struct CleanupLocker {
    inner: InMemoryDistributedLocker,
    acquire_gate: AtomicBool,
    acquire_entered: Notify,
    release_gate: Semaphore,
    release_entered: Notify,
    fail_release: AtomicBool,
}
impl CleanupLocker {
    fn new(clock: Arc<dyn Clock>) -> Self {
        Self {
            inner: InMemoryDistributedLocker::new(clock),
            acquire_gate: AtomicBool::new(false),
            acquire_entered: Notify::new(),
            release_gate: Semaphore::new(0),
            release_entered: Notify::new(),
            fail_release: AtomicBool::new(false),
        }
    }
}
#[async_trait]
impl DistributedLocker for CleanupLocker {
    async fn acquire(&self, key: &str, ttl: Duration, timeout: Timeout) -> Result<Option<String>> {
        self.inner.acquire(key, ttl, timeout).await
    }
    async fn release(&self, key: &str, token: &str) -> Result<()> {
        self.release_entered.notify_one();
        self.release_gate.acquire().await.unwrap().forget();
        self.inner.release(key, token).await?;
        if self.fail_release.load(Ordering::SeqCst) {
            Err(Error::Distributed("original release cause".into()))
        } else {
            Ok(())
        }
    }
    fn lease_support(&self) -> LeaseSupport {
        LeaseSupport::Renewable
    }
    fn token_acquisition(&self) -> TokenAcquisition {
        TokenAcquisition::CallerSelected
    }
    async fn acquire_receipt(
        &self,
        key: &str,
        token: &LeaseToken,
        ttl: LeaseTtl,
        timeout: Timeout,
    ) -> std::result::Result<Option<LeaseReceipt>, LeaseError> {
        let receipt = self.inner.acquire_receipt(key, token, ttl, timeout).await?;
        if self.acquire_gate.load(Ordering::SeqCst) {
            self.acquire_entered.notify_one();
            pending::<()>().await;
        }
        Ok(receipt)
    }
    async fn renew(
        &self,
        key: &str,
        token: &LeaseToken,
        ttl: LeaseTtl,
    ) -> std::result::Result<RenewalOutcome, LeaseError> {
        self.inner.renew(key, token, ttl).await
    }
    fn ownership_check(&self, key: &str, token: &LeaseToken) -> OwnershipCheck {
        self.inner.ownership_check(key, token)
    }
    fn lease_proof(&self, key: &str, token: &LeaseToken) -> LeaseProof {
        self.inner.lease_proof(key, token)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn off_runtime_close_drains_owned_lease_release_and_retains_its_original_failure() {
    for fail in [false, true] {
        let clock = Arc::new(ManualClock::default());
        let locker = Arc::new(CleanupLocker::new(clock.clone()));
        locker.fail_release.store(fail, Ordering::SeqCst);
        let backend = Arc::new(InMemoryDistributedCache::new(clock.clone()));
        let options = opts()
            .with_duration(Duration::from_secs(1))
            .with_fail_safe(true, Some(Duration::from_secs(100)), None)
            .with_factory_timeouts(
                Timeout::After(Duration::from_millis(5)),
                Timeout::After(Duration::from_secs(1)),
                true,
            );
        let cache = Cache::<i32>::builder()
            .clock(clock.clone())
            .distributed(backend)
            .serializer(Arc::new(JsonSerializer))
            .distributed_locker(locker.clone())
            .default_options(options)
            .auto_recovery(no_recovery())
            .try_build()
            .unwrap();
        cache.try_set("k", 1).await.unwrap().wait().await.unwrap();
        clock.advance(Duration::from_secs(2));
        let (token_tx, token_rx) = oneshot::channel();
        let dropped = Arc::new(AtomicUsize::new(0));
        let origin_drop = dropped.clone();
        assert_eq!(
            cache
                .get_or_set("k", move |ctx| async move {
                    let _drop = DropSignal(origin_drop);
                    token_tx.send(ctx.cancellation().clone()).unwrap();
                    pending::<()>().await;
                    Ok(ctx.value(2))
                })
                .await
                .unwrap(),
            1
        );
        assert_eq!(locker.inner.held_count(), 1);
        let closing = cache.clone();
        assert_eq!(
            std::thread::spawn(move || closing.close()).join().unwrap(),
            CloseOutcome::Started
        );
        assert_eq!(
            token_rx.await.unwrap().cancelled().await,
            FactoryCancellationReason::CacheShutdown
        );
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        locker.release_entered.notified().await;
        let shutting = {
            let cache = cache.clone();
            tokio::spawn(async move { cache.shutdown().await })
        };
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(
            !shutting.is_finished(),
            "shutdown must drain the owned release"
        );
        locker.release_gate.add_permits(1);
        let result = shutting.await.unwrap();
        assert_eq!(locker.inner.held_count(), 0);
        if fail {
            match result {
                Err(Error::Shutdown(error)) => {
                    assert!(error.failures().iter().any(|failure|matches!(failure,ShutdownFailure::Work(Error::Lease(LeaseError::Backend {source})) if source.to_string().contains("original release cause"))));
                }
                other => panic!("cleanup failure was discarded: {other:?}"),
            };
            assert!(matches!(cache.shutdown().await, Err(Error::Shutdown(_))));
        } else {
            result.unwrap();
            cache.shutdown().await.unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_uncertain_acquisition_is_supervised_before_ownership_transfer() {
    let clock = Arc::new(ManualClock::default());
    let locker = Arc::new(CleanupLocker::new(clock.clone()));
    locker.acquire_gate.store(true, Ordering::SeqCst);
    let cache = Cache::<i32>::builder()
        .clock(clock.clone())
        .distributed(Arc::new(InMemoryDistributedCache::new(clock)))
        .serializer(Arc::new(JsonSerializer))
        .distributed_locker(locker.clone())
        .default_options(opts())
        .auto_recovery(no_recovery())
        .try_build()
        .unwrap();
    let source = CancellationSource::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let origin_calls = calls.clone();
    let mut caller = Box::pin(cache.get_or_set_cancellable(
        "k",
        move |ctx| async move {
            origin_calls.fetch_add(1, Ordering::SeqCst);
            Ok(ctx.value(2))
        },
        source.token(),
    ));
    poll_fn(|cx| {
        assert!(caller.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    locker.acquire_entered.notified().await;
    assert_eq!(locker.inner.held_count(), 1);
    source.cancel();
    locker.release_entered.notified().await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let shutting = {
        let cache = cache.clone();
        tokio::spawn(async move { cache.shutdown().await })
    };
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(!shutting.is_finished());
    locker.release_gate.add_permits(1);
    shutting.await.unwrap().unwrap();
    assert_eq!(locker.inner.held_count(), 0);
    assert!(matches!(
        caller.await,
        Err(Error::OperationCancelled {
            reason: FactoryCancellationReason::CallerCancelled
        })
    ));
}

#[tokio::test]
async fn not_modified_validates_adaptive_raw_tags_before_retention() {
    let clock = Arc::new(ManualClock::default());
    let cache = Cache::<i32>::builder()
        .clock(clock.clone())
        .default_options(opts().with_duration(Duration::from_secs(1)).with_fail_safe(
            true,
            Some(Duration::from_secs(100)),
            None,
        ))
        .try_build()
        .unwrap();
    cache.try_set("k", 1).await.unwrap().wait().await.unwrap();
    clock.advance(Duration::from_secs(2));
    assert!(matches!(
        cache
            .get_or_set("k", |mut ctx| async move {
                ctx.set_tags([" "]);
                ctx.not_modified()
            })
            .await,
        Err(Error::Tag(TagError::Blank))
    ));
    cache.shutdown().await.unwrap();
}

#[test]
fn runtime_dependent_options_are_rejected_before_effects_on_std_thread() {
    std::thread::spawn(|| {
        let cache = Cache::<i32>::builder()
            .auto_recovery(no_recovery())
            .try_build()
            .unwrap();
        let options = opts().with_factory_timeouts(
            Timeout::Infinite,
            Timeout::After(Duration::from_millis(1)),
            false,
        );
        let mut operation = Box::pin(cache.try_set_full("k", 1, Some(options), Box::from([])));
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(matches!(
            operation.as_mut().poll(&mut context),
            Poll::Ready(Err(Error::Config(ConfigError::MissingRuntime {
                component: RuntimeComponent::Execution
            })))
        ));
    })
    .join()
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cold_expire_read_failure_retains_intent_and_original_physical_deadline() {
    let clock = Arc::new(ManualClock::default());
    let backend = Arc::new(RecoveryIo::new(clock.clone()));
    let writer = Cache::<i32>::builder()
        .clock(clock.clone())
        .distributed(backend.clone())
        .serializer(Arc::new(JsonSerializer))
        .default_options(opts())
        .auto_recovery(no_recovery())
        .try_build()
        .unwrap();
    writer.try_set("k", 7).await.unwrap().wait().await.unwrap();
    let original: DistributedSnapshot<i32> = JsonSerializer
        .deserialize_snapshot(&backend.get("v2:k").await.unwrap().unwrap())
        .unwrap();
    clock.advance(Duration::from_secs(2));
    let cache = Cache::<i32>::builder()
        .clock(clock.clone())
        .distributed(backend.clone())
        .serializer(Arc::new(JsonSerializer))
        .default_options(opts())
        .auto_recovery(recovery_config())
        .try_build()
        .unwrap();
    backend.read_down.store(true, Ordering::SeqCst);
    let report = cache.try_expire("k").await.unwrap().wait().await.unwrap();
    assert!(matches!(
        report.distributed,
        EffectOutcome::RecoveryQueued { .. }
    ));
    assert!(matches!(
        cache.recovery_ticket("k").unwrap().work(),
        RecoveryWork::Data {
            mutation: PendingMutation::ColdExpire { .. },
            ..
        }
    ));
    backend.read_down.store(false, Ordering::SeqCst);
    until(|| cache.pending_recovery() == 0).await;
    let expired: DistributedSnapshot<i32> = JsonSerializer
        .deserialize_snapshot(&backend.get("v2:k").await.unwrap().unwrap())
        .unwrap();
    assert_eq!(expired.entry().value, 7);
    assert_eq!(
        expired.entry().physical_expiration_ticks,
        original.entry().physical_expiration_ticks
    );
    assert_eq!(
        expired.entry().logical_expiration_ticks,
        clock.now().ticks()
    );
    assert!(!cache.read("k", None).await.unwrap().has_value());
    cache.shutdown().await.unwrap();
    writer.shutdown().await.unwrap();
}

struct PublishFault {
    inner: InProcessBackplane,
    down: AtomicBool,
}
#[async_trait]
impl Backplane for PublishFault {
    async fn publish(&self, message: BackplaneMessage) -> Result<()> {
        if self.down.load(Ordering::SeqCst) {
            Err(Error::Backplane("notification unavailable".into()))
        } else {
            self.inner.publish(message).await
        }
    }
    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<BackplaneMessage> {
        self.inner.subscribe()
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovered_storage_then_failed_notification_never_rewrites_storage_on_retry() {
    let clock = Arc::new(ManualClock::default());
    let backend = Arc::new(RecoveryIo::new(clock.clone()));
    let bp = Arc::new(PublishFault {
        inner: InProcessBackplane::default(),
        down: AtomicBool::new(true),
    });
    backend.down.store(true, Ordering::SeqCst);
    let cache = Cache::<i32>::builder()
        .clock(clock)
        .distributed(backend.clone())
        .serializer(Arc::new(JsonSerializer))
        .backplane(bp.clone())
        .default_options(opts())
        .auto_recovery(recovery_config())
        .try_build()
        .unwrap();
    cache.try_set("k", 7).await.unwrap().wait().await.unwrap();
    backend.down.store(false, Ordering::SeqCst);
    until(|| {
        cache.recovery_ticket("k").is_some_and(|ticket| {
            matches!(
                ticket.work(),
                RecoveryWork::Data {
                    mutation: PendingMutation::Notify(_),
                    ..
                }
            )
        })
    })
    .await;
    assert_eq!(backend.commits.load(Ordering::SeqCst), 1);
    let snapshot: DistributedSnapshot<i32> = JsonSerializer
        .deserialize_snapshot(&backend.get("v2:k").await.unwrap().unwrap())
        .unwrap();
    let mut value = snapshot.entry().clone();
    value.value = 99;
    let replacement =
        DistributedSnapshot::new(value, snapshot.inserted_at(), snapshot.retention()).unwrap();
    backend
        .inner
        .set(
            "v2:k",
            JsonSerializer.serialize_snapshot(&replacement).unwrap(),
            Some(Duration::from_secs(60)),
        )
        .await
        .unwrap();
    bp.down.store(false, Ordering::SeqCst);
    until(|| cache.pending_recovery() == 0).await;
    assert_eq!(backend.commits.load(Ordering::SeqCst), 1);
    let final_value: DistributedSnapshot<i32> = JsonSerializer
        .deserialize_snapshot(&backend.get("v2:k").await.unwrap().unwrap())
        .unwrap();
    assert_eq!(final_value.entry().value, 99);
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn delayed_clear_marker_does_not_evict_a_newer_snapshot() {
    let clock = Arc::new(ManualClock::default());
    clock.advance(Duration::from_secs(1));
    let bp = Arc::new(InProcessBackplane::default());
    let cache = Cache::<i32>::builder()
        .clock(clock)
        .backplane(bp.clone())
        .default_options(opts())
        .auto_recovery(no_recovery())
        .try_build()
        .unwrap();
    cache.try_set("k", 7).await.unwrap().wait().await.unwrap();
    let scope = CacheScope::new("", "v2", KeyModifierMode::Prefix).unwrap();
    let command = MarkerCommand::new(
        "other",
        scope,
        StoredMarker::new(
            MarkerKind::ClearRemove,
            MarkerVersion::new(Timestamp::from_ticks(0)),
        ),
    )
    .unwrap();
    bp.publish_command(BackplaneCommand::Marker(command))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(cache.read("k", None).await.unwrap().value(), Some(&7));
    cache.shutdown().await.unwrap();
}
