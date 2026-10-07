//! Independent user-supplied local coordination, including owned background work.
use amalgam::locking::{KeyGuard, KeyedLock};
use amalgam::*;
use async_trait::async_trait;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{Notify, Semaphore};

#[derive(Debug, thiserror::Error)]
#[error("original local coordination failure")]
struct Cause;
#[derive(Clone, Copy)]
enum Mode {
    Lock,
    Unavailable,
    Park,
    Error,
}
#[derive(Clone, Copy)]
enum Release {
    Success,
    Error,
    Panic,
}
struct Provider {
    locks: KeyedLock,
    mode: Mutex<Mode>,
    release: Mutex<Release>,
    requests: Mutex<Vec<MemoryLockRequest>>,
    ended: Arc<Mutex<Vec<Option<FactoryCancellationReason>>>>,
    acquired: Arc<AtomicUsize>,
    released: Arc<AtomicUsize>,
    active: Arc<AtomicUsize>,
    tries: AtomicUsize,
    shutdowns: Mutex<Vec<String>>,
    shutdown_result: Mutex<Release>,
    entered: Notify,
    shutdown_entered: Notify,
    shutdown_gate: Semaphore,
}
impl Provider {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            locks: KeyedLock::default(),
            mode: Mutex::new(Mode::Lock),
            release: Mutex::new(Release::Success),
            requests: Mutex::new(Vec::new()),
            ended: Arc::new(Mutex::new(Vec::new())),
            acquired: Arc::new(AtomicUsize::new(0)),
            released: Arc::new(AtomicUsize::new(0)),
            active: Arc::new(AtomicUsize::new(0)),
            tries: AtomicUsize::new(0),
            shutdowns: Mutex::new(Vec::new()),
            shutdown_result: Mutex::new(Release::Success),
            entered: Notify::new(),
            shutdown_entered: Notify::new(),
            shutdown_gate: Semaphore::new(1024),
        })
    }
    fn guard(&self, guard: KeyGuard) -> MemoryLockOutcome {
        self.acquired.fetch_add(1, Ordering::SeqCst);
        self.active.fetch_add(1, Ordering::SeqCst);
        MemoryLockOutcome::Acquired(MemoryLock::new(Owned {
            guard,
            released: self.released.clone(),
            active: self.active.clone(),
            result: *self.release.lock().unwrap(),
        }))
    }
    fn drained(&self) {
        assert_eq!(self.active.load(Ordering::SeqCst), 0);
        assert_eq!(
            self.acquired.load(Ordering::SeqCst),
            self.released.load(Ordering::SeqCst)
        );
    }
}
struct Owned {
    guard: KeyGuard,
    released: Arc<AtomicUsize>,
    active: Arc<AtomicUsize>,
    result: Release,
}
impl MemoryLockGuard for Owned {
    fn release(self: Box<Self>) -> std::result::Result<(), MemoryLockerError> {
        let Self {
            guard,
            released,
            active,
            result,
        } = *self;
        drop(guard);
        released.fetch_add(1, Ordering::SeqCst);
        active.fetch_sub(1, Ordering::SeqCst);
        match result {
            Release::Success => Ok(()),
            Release::Error => Err(MemoryLockerError::from_source(Cause)),
            Release::Panic => panic!("provider release contract violation"),
        }
    }
}
struct Witness {
    token: FactoryCancellation,
    ended: Arc<Mutex<Vec<Option<FactoryCancellationReason>>>>,
}
impl Drop for Witness {
    fn drop(&mut self) {
        let reason = match self.token.check() {
            Ok(()) => None,
            Err(Error::OperationCancelled { reason }) => Some(reason),
            Err(other) => panic!("unexpected acquisition cancellation: {other:?}"),
        };
        self.ended.lock().unwrap().push(reason);
    }
}
#[async_trait]
impl MemoryLocker for Provider {
    async fn acquire(
        &self,
        request: MemoryLockRequest,
    ) -> std::result::Result<MemoryLockOutcome, MemoryLockerError> {
        self.requests.lock().unwrap().push(request.clone());
        let _witness = Witness {
            token: request.cancellation().clone(),
            ended: self.ended.clone(),
        };
        self.entered.notify_one();
        let mode = *self.mode.lock().unwrap();
        match mode {
            Mode::Lock => Ok(self.guard(self.locks.lock(request.coordination_key()).await)),
            Mode::Unavailable => Ok(MemoryLockOutcome::Unavailable),
            Mode::Park => std::future::pending().await,
            Mode::Error => Err(MemoryLockerError::from_source(Cause)),
        }
    }
    fn try_acquire(
        &self,
        request: MemoryLockRequest,
    ) -> std::result::Result<MemoryLockOutcome, MemoryLockerError> {
        self.tries.fetch_add(1, Ordering::SeqCst);
        self.requests.lock().unwrap().push(request.clone());
        Ok(match self.locks.try_lock(request.coordination_key()) {
            Some(guard) => self.guard(guard),
            None => MemoryLockOutcome::Unavailable,
        })
    }
    async fn shutdown(
        &self,
        context: MemoryLockerContext,
    ) -> std::result::Result<(), MemoryLockerError> {
        self.drained();
        self.shutdowns
            .lock()
            .unwrap()
            .push(context.instance_id().to_owned());
        self.shutdown_entered.notify_one();
        self.shutdown_gate.acquire().await.unwrap().forget();
        match *self.shutdown_result.lock().unwrap() {
            Release::Success => Ok(()),
            Release::Error => Err(MemoryLockerError::from_source(Cause)),
            Release::Panic => panic!("original provider shutdown panic"),
        }
    }
}
fn options() -> EntryOptions {
    EntryOptions::new(Duration::from_secs(60))
}
fn cache(provider: &Arc<Provider>) -> Cache<u64> {
    Cache::builder()
        .name("profiles")
        .instance_id("one")
        .key_prefix("p:")
        .memory_locker(provider.clone())
        .default_options(options())
        .try_build()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn same_key_shares_one_factory_and_fresh_hits_skip_the_provider() {
    let provider = Provider::new();
    let c = cache(&provider);
    let runs = Arc::new(AtomicUsize::new(0));
    let mut tasks = Vec::with_capacity(100);
    for _ in 0..100 {
        let c = c.clone();
        let runs = runs.clone();
        tasks.push(tokio::spawn(async move {
            c.get_or_set(
                "same",
                amalgam::source::factory(move |ctx| async move {
                    runs.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    Ok::<_, amalgam::FactoryError>(ctx.value(42))
                }),
            )
            .await
            .unwrap()
        }));
    }
    for task in tasks {
        assert_eq!(task.await.unwrap(), 42);
    }
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    provider.drained();
    let calls = provider.requests.lock().unwrap().len();
    assert_eq!(
        c.get_or_set::<_, _>(
            "same",
            typed_factory(|_| async { panic!("hot hit invoked origin") })
        )
        .await
        .unwrap(),
        42
    );
    assert_eq!(provider.requests.lock().unwrap().len(), calls);
    {
        let requests = provider.requests.lock().unwrap();
        assert!(requests.iter().all(|r| r.key() == "p:same"
            && r.context().cache_name() == "profiles"
            && r.context().instance_id() == "one"));
    }
    c.shutdown().await.unwrap();
    c.shutdown().await.unwrap();
    assert_eq!(*provider.shutdowns.lock().unwrap(), ["one"]);
}

#[tokio::test]
async fn distinct_keys_enter_factories_before_either_is_released() {
    let provider = Provider::new();
    let c = cache(&provider);
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let mut tasks = Vec::new();
    for key in ["a", "b"] {
        let c = c.clone();
        let entered = entered.clone();
        let release = release.clone();
        tasks.push(tokio::spawn(async move {
            c.get_or_set(
                key,
                amalgam::source::factory(move |ctx| async move {
                    entered.add_permits(1);
                    release.acquire().await.unwrap().forget();
                    Ok::<_, amalgam::FactoryError>(ctx.value(7))
                }),
            )
            .await
            .unwrap()
        }));
    }
    tokio::time::timeout(Duration::from_secs(1), entered.acquire_many(2))
        .await
        .unwrap()
        .unwrap()
        .forget();
    release.add_permits(2);
    for task in tasks {
        assert_eq!(task.await.unwrap(), 7);
    }
    provider.drained();
    c.shutdown().await.unwrap();
}

#[tokio::test]
async fn finite_wait_signals_provider_before_drop_and_factory_token_stays_active() {
    let provider = Provider::new();
    *provider.mode.lock().unwrap() = Mode::Park;
    let c = Cache::builder()
        .memory_locker(provider.clone())
        .default_options(
            options().with_memory_lock_timeout(Timeout::After(Duration::from_millis(15))),
        )
        .try_build()
        .unwrap();
    assert_eq!(
        c.get_or_set(
            "deadline",
            amalgam::source::factory(|ctx| async move {
                assert!(ctx.cancellation().check().is_ok());
                Ok::<_, amalgam::FactoryError>(ctx.value(9))
            })
        )
        .await
        .unwrap(),
        9
    );
    assert_eq!(
        *provider.ended.lock().unwrap(),
        [Some(FactoryCancellationReason::HardTimeout)]
    );
    c.shutdown().await.unwrap();
}

#[tokio::test]
async fn unavailable_guard_and_lock_timeout_preserve_existing_fallback_rules() {
    for mode in [Mode::Unavailable, Mode::Park] {
        let provider = Provider::new();
        *provider.mode.lock().unwrap() = mode;
        let clock = Arc::new(ManualClock::new(Timestamp::from_ticks(10_000)));
        let opts = EntryOptions::new(Duration::from_secs(1))
            .with_fail_safe(true, Some(Duration::from_secs(30)), None)
            .with_memory_lock_timeout(Timeout::After(Duration::from_millis(10)));
        let c = Cache::builder()
            .clock(clock.clone())
            .memory_locker(provider.clone())
            .default_options(opts)
            .try_build()
            .unwrap();
        c.try_set("stale", 17).await.unwrap().wait().await.unwrap();
        clock.advance(Duration::from_secs(2));
        assert_eq!(
            c.get_or_set::<_, _>(
                "stale",
                typed_factory(|_| async { panic!("eligible stale was ignored") })
            )
            .await
            .unwrap(),
            17
        );
        assert_eq!(
            c.get_or_set(
                "miss",
                amalgam::source::factory(|ctx| async move {
                    Ok::<_, amalgam::FactoryError>(ctx.value(19))
                })
            )
            .await
            .unwrap(),
            19
        );
        c.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn caller_cancel_and_cache_close_signal_pending_provider_before_drop() {
    for close in [false, true] {
        let provider = Provider::new();
        *provider.mode.lock().unwrap() = Mode::Park;
        let c = cache(&provider);
        let source = CancellationSource::new();
        let token = source.token();
        let request = c.clone();
        let work = tokio::spawn(async move {
            request
                .get_or_set(
                    "wait",
                    typed_factory(|_| async { panic!("cancelled wait ran factory") }),
                )
                .cancellation(token)
                .await
        });
        provider.entered.notified().await;
        let expected = if close {
            c.close();
            FactoryCancellationReason::CacheShutdown
        } else {
            source.cancel();
            FactoryCancellationReason::CallerCancelled
        };
        assert!(
            matches!(work.await.unwrap(),Err(Error::OperationCancelled {reason}) if reason==expected)
        );
        assert_eq!(*provider.ended.lock().unwrap(), [Some(expected)]);
        c.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn original_provider_failure_is_not_a_miss_or_factory_failure() {
    let provider = Provider::new();
    *provider.mode.lock().unwrap() = Mode::Error;
    let c = cache(&provider);
    let error = c
        .get_or_set::<_, _>(
            "broken",
            typed_factory(|_| async { panic!("provider failure invoked factory") }),
        )
        .await
        .unwrap_err();
    assert_eq!(
        OperationOutcome::from_error(&error),
        OperationOutcome::LockError
    );
    match error {
        Error::MemoryLocker(MemoryLockerError::Provider { source }) => {
            assert!(source.downcast_ref::<Cause>().is_some())
        }
        other => panic!("lost original cause: {other:?}"),
    }
    c.shutdown().await.unwrap();
}

#[tokio::test]
async fn release_errors_and_panics_do_not_replace_computed_values_or_double_release() {
    for result in [Release::Error, Release::Panic] {
        let provider = Provider::new();
        *provider.release.lock().unwrap() = result;
        let c = cache(&provider);
        assert_eq!(
            c.get_or_set(
                "release",
                amalgam::source::factory(|ctx| async move {
                    Ok::<_, amalgam::FactoryError>(ctx.value(23))
                })
            )
            .await
            .unwrap(),
            23
        );
        provider.drained();
        c.shutdown().await.unwrap();
    }
    let lock = KeyedLock::default();
    let released = Arc::new(AtomicUsize::new(0));
    let active = Arc::new(AtomicUsize::new(1));
    let error = MemoryLock::new(Owned {
        guard: lock.lock("direct").await,
        released: released.clone(),
        active,
        result: Release::Error,
    })
    .release()
    .unwrap_err();
    assert!(
        matches!(error,MemoryLockerError::Provider {source} if source.downcast_ref::<Cause>().is_some())
    );
    assert_eq!(released.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn soft_completion_keeps_the_custom_guard_until_background_factory_finishes() {
    let provider = Provider::new();
    let clock = Arc::new(ManualClock::new(Timestamp::from_ticks(10_000)));
    let opts = EntryOptions::new(Duration::from_secs(1))
        .with_fail_safe(true, Some(Duration::from_secs(30)), None)
        .with_factory_timeouts(
            Timeout::After(Duration::from_millis(10)),
            Timeout::Infinite,
            true,
        );
    let c = Cache::builder()
        .clock(clock.clone())
        .memory_locker(provider.clone())
        .default_options(opts)
        .try_build()
        .unwrap();
    c.try_set("background", 31)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    clock.advance(Duration::from_secs(2));
    let release = Arc::new(Semaphore::new(0));
    let factory_release = release.clone();
    assert_eq!(
        c.get_or_set(
            "background",
            amalgam::source::factory(move |ctx| async move {
                factory_release.acquire().await.unwrap().forget();
                assert!(ctx.cancellation().check().is_ok());
                Ok::<_, amalgam::FactoryError>(ctx.value(37))
            })
        )
        .await
        .unwrap(),
        31
    );
    assert_eq!(provider.active.load(Ordering::SeqCst), 1);
    release.add_permits(1);
    c.flush_pending().await.unwrap();
    provider.drained();
    assert_eq!(
        c.read("background", None).await.unwrap().into_value(),
        Some(37)
    );
    c.shutdown().await.unwrap();
}

#[tokio::test]
async fn eager_uses_nonblocking_custom_attempt_and_owned_guard() {
    let provider = Provider::new();
    let clock = Arc::new(ManualClock::new(Timestamp::from_ticks(10_000)));
    let opts = EntryOptions::new(Duration::from_secs(10))
        .with_eager_refresh(Some(EagerThreshold::new(0.5).unwrap()));
    let c = Cache::builder()
        .clock(clock.clone())
        .memory_locker(provider.clone())
        .default_options(opts)
        .try_build()
        .unwrap();
    assert_eq!(
        c.get_or_set(
            "eager",
            amalgam::source::factory(
                |ctx| async move { Ok::<_, amalgam::FactoryError>(ctx.value(41)) }
            )
        )
        .await
        .unwrap(),
        41
    );
    clock.advance(Duration::from_secs(6));
    assert_eq!(
        c.get_or_set(
            "eager",
            amalgam::source::factory(
                |ctx| async move { Ok::<_, amalgam::FactoryError>(ctx.value(43)) }
            )
        )
        .await
        .unwrap(),
        41
    );
    c.flush_pending().await.unwrap();
    provider.drained();
    assert_eq!(provider.tries.load(Ordering::SeqCst), 1);
    assert_eq!(c.read("eager", None).await.unwrap().into_value(), Some(43));
    c.shutdown().await.unwrap();
}

#[tokio::test]
async fn marker_and_entry_equal_text_keys_have_disjoint_custom_coordination() {
    let provider = Provider::new();
    let tag = Tag::new("group").unwrap();
    let c = Cache::builder()
        .memory_locker(provider.clone())
        .distributed(Arc::new(InMemoryDistributedCache::new(Arc::new(
            SystemClock,
        ))))
        .serializer(Arc::new(JsonSerializer))
        .invalidation_store(Arc::new(InMemoryInvalidationStore::default()))
        .marker_read_policy(MarkerReadPolicy::OptionsControlled)
        .tags_default_options(EntryOptions::tag_defaults().with_skip_memory(true, true))
        .try_build()
        .unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(1),
        c.get_or_set(
            "tag:group",
            typed_factory(|ctx| async move { Ok::<_, amalgam::FactoryError>(ctx.value(47)) }),
        )
        .tags(vec![tag.clone()].into_boxed_slice()),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(result, 47);
    c.flush_pending().await.unwrap();
    assert_eq!(
        tokio::time::timeout(
            Duration::from_secs(1),
            c.get_or_set(
                "tag:group",
                typed_factory(|_| async { panic!("L2 snapshot was ignored") })
            )
            .options(|_| options().with_skip_memory(true, false))
        )
        .await
        .unwrap()
        .unwrap(),
        47
    );
    {
        let requests = provider.requests.lock().unwrap();
        let entry = requests
            .iter()
            .find(|r| r.kind() == &MemoryLockKind::Entry)
            .unwrap();
        let marker = requests
            .iter()
            .find(|r| r.kind() == &MemoryLockKind::Marker(MarkerKind::Tag(tag.clone())))
            .unwrap();
        assert_eq!(entry.key(), marker.key());
        assert_ne!(entry.coordination_key(), marker.coordination_key());
    }
    provider.drained();
    c.shutdown().await.unwrap();
    assert_eq!(provider.shutdowns.lock().unwrap().len(), 1);
}

#[test]
fn native_and_async_views_use_one_custom_local_locker() {
    let provider = Provider::new();
    let native =
        BlockingCache::from_builder(Cache::builder().memory_locker(provider.clone())).unwrap();
    assert_eq!(
        native
            .get_or_set(
                "native",
                amalgam::source::factory(|ctx| Ok::<_, amalgam::FactoryError>(ctx.value(53)))
            )
            .execute()
            .unwrap(),
        53
    );
    assert_eq!(
        native
            .runtime()
            .run(native.as_async().get_or_set(
                "async",
                amalgam::source::factory(|ctx| async move {
                    Ok::<_, amalgam::FactoryError>(ctx.value(59))
                })
            ))
            .unwrap(),
        59
    );
    provider.drained();
    native.shutdown().unwrap();
    assert_eq!(provider.shutdowns.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn cached_marker_eager_refresh_uses_the_same_custom_provider() {
    let provider = Provider::new();
    let clock = Arc::new(ManualClock::new(Timestamp::from_ticks(10_000)));
    let tags = EntryOptions::tag_defaults()
        .with_memory_duration(Duration::from_secs(10))
        .with_distributed_duration(Duration::from_secs(10))
        .with_eager_refresh(Some(EagerThreshold::new(0.5).unwrap()));
    let c = Cache::builder()
        .clock(clock.clone())
        .memory_locker(provider.clone())
        .invalidation_store(Arc::new(InMemoryInvalidationStore::default()))
        .marker_read_policy(MarkerReadPolicy::OptionsControlled)
        .marker_lifecycle_policy(MarkerLifecyclePolicy::CachedSnapshots)
        .default_options(options())
        .tags_default_options(tags)
        .try_build()
        .unwrap();
    c.get_or_set(
        "marker-eager",
        typed_factory(|ctx| async move { Ok::<_, amalgam::FactoryError>(ctx.value(61)) }),
    )
    .tags(vec![Tag::new("eager-group").unwrap()].into_boxed_slice())
    .await
    .unwrap();
    assert_eq!(
        c.read("marker-eager", None).await.unwrap().into_value(),
        Some(61)
    );
    clock.advance(Duration::from_secs(6));
    assert_eq!(
        c.read("marker-eager", None).await.unwrap().into_value(),
        Some(61)
    );
    c.flush_pending().await.unwrap();
    assert!(provider.requests.lock().unwrap().iter().any(|r| matches!(
        r.kind(),
        MemoryLockKind::Marker(MarkerKind::Tag(_))
    ) && r.timeout()
        == Timeout::After(Duration::ZERO)));
    provider.drained();
    c.shutdown().await.unwrap();
}

#[tokio::test]
async fn dropped_caller_ends_the_pending_acquisition_before_destruction() {
    let provider = Provider::new();
    *provider.mode.lock().unwrap() = Mode::Park;
    let c = cache(&provider);
    let request = c.clone();
    let work = tokio::spawn(async move {
        request
            .get_or_set::<_, _>(
                "drop",
                typed_factory(|_| async { panic!("dropped caller ran factory") }),
            )
            .await
    });
    provider.entered.notified().await;
    work.abort();
    assert!(work.await.unwrap_err().is_cancelled());
    assert_eq!(
        *provider.ended.lock().unwrap(),
        [Some(FactoryCancellationReason::CallerDropped)]
    );
    c.shutdown().await.unwrap();
}

#[tokio::test]
async fn hard_factory_timeout_releases_the_acquired_local_guard() {
    let provider = Provider::new();
    let c = Cache::<u64>::builder()
        .memory_locker(provider.clone())
        .default_options(options().with_factory_timeouts(
            Timeout::Infinite,
            Timeout::After(Duration::from_millis(10)),
            false,
        ))
        .try_build()
        .unwrap();
    assert!(matches!(
        c.get_or_set::<_, _>(
            "hard",
            typed_factory(|_| async { std::future::pending().await })
        )
        .await,
        Err(Error::FactoryTimeout { .. })
    ));
    c.flush_pending().await.unwrap();
    provider.drained();
    c.shutdown().await.unwrap();
}

#[tokio::test]
async fn shutdown_preserves_provider_error_or_panic_and_does_not_repeat_the_hook() {
    for result in [Release::Error, Release::Panic] {
        let provider = Provider::new();
        *provider.shutdown_result.lock().unwrap() = result;
        let c = cache(&provider);
        for _ in 0..2 {
            let error = c.shutdown().await.unwrap_err();
            let Error::Shutdown(error) = error else {
                panic!("lost shutdown failure")
            };
            match (result, &error.failures()[0]) {
                (
                    Release::Error,
                    ShutdownFailure::Work(Error::MemoryLocker(MemoryLockerError::Provider {
                        source,
                    })),
                ) => assert!(source.downcast_ref::<Cause>().is_some()),
                (
                    Release::Panic,
                    ShutdownFailure::BackgroundTask {
                        task: ShutdownTask::MemoryLocker,
                        source,
                    },
                ) => assert!(source.is_panic()),
                (_, other) => panic!("wrong teardown cause: {other:?}"),
            }
        }
        assert_eq!(provider.shutdowns.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn provider_can_be_shared_without_one_cache_shutdown_closing_the_other() {
    let provider = Provider::new();
    let a = cache(&provider);
    let b = Cache::builder()
        .memory_locker(provider.clone())
        .instance_id("two")
        .try_build()
        .unwrap();
    a.get_or_set(
        "a",
        amalgam::source::factory(
            |ctx| async move { Ok::<_, amalgam::FactoryError>(ctx.value(67)) },
        ),
    )
    .await
    .unwrap();
    a.shutdown().await.unwrap();
    assert_eq!(
        b.get_or_set(
            "b",
            amalgam::source::factory(
                |ctx| async move { Ok::<_, amalgam::FactoryError>(ctx.value(71)) }
            )
        )
        .await
        .unwrap(),
        71
    );
    b.shutdown().await.unwrap();
    assert_eq!(*provider.shutdowns.lock().unwrap(), ["one", "two"]);
}

#[tokio::test]
async fn interrupted_shutdown_reuses_the_already_owned_provider_teardown() {
    let provider = Provider::new();
    assert_eq!(provider.shutdown_gate.forget_permits(1024), 1024);
    let c = cache(&provider);
    let request = c.clone();
    let shutdown = tokio::spawn(async move { request.shutdown().await });
    provider.shutdown_entered.notified().await;
    shutdown.abort();
    assert!(shutdown.await.unwrap_err().is_cancelled());
    provider.shutdown_gate.add_permits(1);
    tokio::time::timeout(Duration::from_secs(1), c.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(provider.shutdowns.lock().unwrap().len(), 1);
}

fn typed_factory<V, F, Fut>(factory: F) -> F
where
    F: FnOnce(amalgam::FactoryContext<V>) -> Fut,
    Fut: std::future::Future<Output = std::result::Result<V, amalgam::FactoryError>>,
{
    factory
}
