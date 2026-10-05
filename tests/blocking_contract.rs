//! Caller-thread, deadline, cancellation, ownership and actual mutation evidence.
use amalgam::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Condvar, Mutex, mpsc};
use std::thread;
use std::time::Duration;

#[derive(Clone, Default)]
struct Blocker(Arc<(Mutex<bool>, Condvar)>);
impl Blocker {
    fn wait(&self) {
        let (state, wake) = &*self.0;
        let mut open = state.lock().unwrap();
        while !*open {
            open = wake.wait(open).unwrap();
        }
    }
    fn open(&self) {
        let (state, wake) = &*self.0;
        *state.lock().unwrap() = true;
        wake.notify_all();
    }
}
struct Release(Blocker);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.open();
    }
}
fn runtime() -> BlockingRuntime {
    BlockingRuntime::with_workers(
        std::num::NonZeroUsize::new(1).unwrap(),
        std::num::NonZeroUsize::new(2).unwrap(),
    )
    .unwrap()
}
fn timed() -> EntryOptions {
    EntryOptions::new(Duration::from_secs(10)).with_factory_timeouts(
        Timeout::Infinite,
        Timeout::After(Duration::from_millis(30)),
        false,
    )
}

#[test]
fn native_calls_outside_tokio_keep_inline_affinity_tags_and_present_null() {
    let cache = BlockingCache::<Option<u64>>::new().unwrap();
    let caller = thread::current().id();
    let tag = Tag::new("native").unwrap();
    let value = cache
        .get_or_set_full(
            "key",
            move |ctx| {
                assert_eq!(thread::current().id(), caller);
                assert_eq!(ctx.invocation(), FactoryInvocation::Foreground);
                Ok(ctx.value(None))
            },
            None,
            Box::from([tag.clone()]),
            MaybeValue::none(),
        )
        .unwrap();
    assert_eq!(value, None);
    assert_eq!(cache.read("key", None).unwrap().into_value(), Some(None));
    assert_eq!(cache.read("miss", None).unwrap().into_value(), None);
    assert_eq!(
        cache
            .get_or_set("key", |_| panic!("warm value must not invoke origin"))
            .unwrap(),
        None
    );
    cache.try_remove_by_tag(tag).unwrap().wait().unwrap();
    assert_eq!(cache.read("key", None).unwrap().into_value(), None);
    cache.try_set("clear", Some(7)).unwrap().wait().unwrap();
    cache.try_clear(ClearMode::Remove).unwrap().wait().unwrap();
    assert_eq!(cache.read("clear", None).unwrap().into_value(), None);
    cache.shutdown().unwrap();
}

#[test]
fn same_key_native_callers_share_one_origin() {
    let cache = BlockingCache::<u64>::new().unwrap();
    let barrier = Arc::new(Barrier::new(12));
    let calls = Arc::new(AtomicUsize::new(0));
    let workers: Vec<_> = (0..12)
        .map(|_| {
            let cache = cache.clone();
            let barrier = barrier.clone();
            let calls = calls.clone();
            thread::spawn(move || {
                barrier.wait();
                cache
                    .get_or_set("one", move |ctx| {
                        calls.fetch_add(1, Ordering::SeqCst);
                        thread::sleep(Duration::from_millis(15));
                        Ok(ctx.value(42))
                    })
                    .unwrap()
            })
        })
        .collect();
    for worker in workers {
        assert_eq!(worker.join().unwrap(), 42);
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    cache.shutdown().unwrap();
}

#[test]
fn hard_deadline_returns_before_callback_finishes_but_shutdown_owns_it() {
    let cache = BlockingCache::<u64>::on_runtime(Cache::builder(), runtime()).unwrap();
    let release = Release(Blocker::default());
    let blocked = release.0.clone();
    let (started_tx, started_rx) = mpsc::channel();
    let (result_tx, result_rx) = mpsc::channel();
    let requester = cache.clone();
    let caller = thread::spawn(move || {
        let result = requester.get_or_set_with(
            "timed",
            move |ctx| {
                started_tx.send(ctx.cancellation().clone()).unwrap();
                blocked.wait();
                Ok(ctx.value(99))
            },
            timed(),
        );
        result_tx.send(result).unwrap();
    });
    let token = started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert!(matches!(
        result_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
        Err(Error::FactoryTimeout { .. })
    ));
    assert!(matches!(
        token.check(),
        Err(Error::OperationCancelled {
            reason: FactoryCancellationReason::HardTimeout
        })
    ));
    let (drained_tx, drained_rx) = mpsc::channel();
    let closing = cache.clone();
    let closer = thread::spawn(move || {
        drained_tx.send(closing.shutdown()).unwrap();
    });
    assert!(
        drained_rx.recv_timeout(Duration::from_millis(30)).is_err(),
        "shutdown must retain the actual blocking callback"
    );
    release.0.open();
    drained_rx
        .recv_timeout(Duration::from_secs(2))
        .unwrap()
        .unwrap();
    caller.join().unwrap();
    closer.join().unwrap();
}

#[test]
fn explicit_cancel_never_returns_failsafe_default_or_stores_late_product() {
    let cache = BlockingCache::<u64>::on_runtime(Cache::builder(), runtime()).unwrap();
    let release = Release(Blocker::default());
    let blocked = release.0.clone();
    let source = CancellationSource::new();
    let requested = source.token();
    let (started_tx, started_rx) = mpsc::channel();
    let (result_tx, result_rx) = mpsc::channel();
    let requester = cache.clone();
    let caller = thread::spawn(move || {
        let result = requester.get_or_set_full_cancellable(
            "cancel",
            move |ctx| {
                started_tx.send(ctx.cancellation().clone()).unwrap();
                blocked.wait();
                Ok(ctx.value(99))
            },
            Some(EntryOptions::new(Duration::from_secs(30)).with_fail_safe(true, None, None)),
            Box::from([]),
            MaybeValue::from_value(7),
            requested,
        );
        result_tx.send(result).unwrap();
    });
    let origin = started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    source.cancel();
    assert!(matches!(
        result_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
        Err(Error::OperationCancelled {
            reason: FactoryCancellationReason::CallerCancelled
        }) | Err(Error::FactoryCancelled {
            reason: FactoryCancellationReason::CallerCancelled
        })
    ));
    assert!(matches!(
        origin.check(),
        Err(Error::OperationCancelled {
            reason: FactoryCancellationReason::CallerCancelled
        })
    ));
    release.0.open();
    caller.join().unwrap();
    cache.flush_pending().unwrap();
    assert!(!cache.read("cancel", None).unwrap().has_value());
    cache.shutdown().unwrap();
}

#[test]
fn eager_callback_does_not_occupy_the_single_runtime_worker() {
    let driver = runtime();
    let clock = Arc::new(ManualClock::new(Timestamp::from_ticks(100_000_000)));
    let cache =
        BlockingCache::<u64>::on_runtime(Cache::builder().clock(clock.clone()), driver.clone())
            .unwrap();
    let options =
        EntryOptions::new(Duration::from_secs(60)).with_eager_refresh(EagerThreshold::new(0.5));
    cache
        .try_set_full("eager", 1, Some(options), Box::from([]))
        .unwrap()
        .wait()
        .unwrap();
    clock.set(Timestamp::from_ticks(410_000_000));
    let release = Release(Blocker::default());
    let blocked = release.0.clone();
    let (started_tx, started_rx) = mpsc::channel();
    assert_eq!(
        cache
            .get_or_set("eager", move |ctx| {
                assert_eq!(ctx.invocation(), FactoryInvocation::EagerRefresh);
                started_tx.send(()).unwrap();
                blocked.wait();
                Ok(ctx.value(2))
            })
            .unwrap(),
        1
    );
    started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    driver
        .run(async {
            tokio::time::timeout(
                Duration::from_secs(1),
                tokio::time::sleep(Duration::from_millis(5)),
            )
            .await
        })
        .unwrap();
    release.0.open();
    cache.flush_pending().unwrap();
    assert_eq!(cache.read("eager", None).unwrap().into_value(), Some(2));
    cache.shutdown().unwrap();
}

#[test]
fn scheduled_native_receipt_waits_for_actual_l2_visibility() {
    let driver = runtime();
    let backend = Arc::new(InMemoryDistributedCache::new(Arc::new(SystemClock)));
    let cache = BlockingCache::<u64>::on_runtime(
        Cache::builder()
            .distributed(backend.clone())
            .serializer(Arc::new(JsonSerializer)),
        driver.clone(),
    )
    .unwrap();
    let receipt = cache
        .try_set_full(
            "shared",
            123,
            Some(
                EntryOptions::new(Duration::from_secs(30))
                    .with_allow_background_distributed_operations(true),
            ),
            Box::from([]),
        )
        .unwrap();
    assert!(matches!(receipt, BlockingMutationReceipt::Scheduled(_)));
    receipt.wait().unwrap();
    let peer = BlockingCache::<u64>::on_runtime(
        Cache::builder()
            .distributed(backend)
            .serializer(Arc::new(JsonSerializer)),
        driver,
    )
    .unwrap();
    assert_eq!(peer.read("shared", None).unwrap().into_value(), Some(123));
    peer.try_expire_with_policy("shared", None, DistributedExpirePolicy::Remove)
        .unwrap()
        .wait()
        .unwrap();
    let third = BlockingCache::<u64>::on_runtime(
        Cache::builder()
            .distributed(peer.distributed_cache().unwrap().clone())
            .serializer(Arc::new(JsonSerializer)),
        peer.runtime().clone(),
    )
    .unwrap();
    assert!(
        !third.read("shared", None).unwrap().has_value(),
        "a cold node must observe physical L2 removal"
    );
    third.shutdown().unwrap();
    peer.shutdown().unwrap();
    cache.shutdown().unwrap();
}

#[test]
fn timed_callback_panic_retains_original_join_in_shutdown_report() {
    let cache = BlockingCache::<u64>::new().unwrap();
    let error = cache
        .get_or_set_with("panic", |_| panic!("native-original-panic"), timed())
        .unwrap_err();
    assert!(matches!(error, Error::FactoryWithSource { .. }));
    let Error::Shutdown(error) = cache.shutdown().unwrap_err() else {
        panic!("shutdown must preserve the original blocking task failure");
    };
    assert!(error.failures().iter().any(|failure| matches!(failure, ShutdownFailure::BackgroundTask { task: ShutdownTask::Factory, source } if source.is_panic())));
}

#[tokio::test(flavor = "current_thread")]
async fn native_calls_and_last_drop_work_on_foreign_current_thread_tokio() {
    let cache = BlockingCache::<u64>::new().unwrap();
    cache.try_set("native", 4).unwrap().wait().unwrap();
    assert_eq!(cache.read("native", None).unwrap().into_value(), Some(4));
    assert_eq!(
        cache
            .get_or_set_with(
                "timed",
                |ctx| {
                    thread::sleep(Duration::from_millis(2));
                    Ok(ctx.value(5))
                },
                timed()
            )
            .unwrap(),
        5
    );
    cache.shutdown().unwrap();
    drop(cache);
    let active = BlockingCache::<u64>::new().unwrap();
    active.try_set("drop", 7).unwrap().wait().unwrap();
    drop(active);
    tokio::task::yield_now().await;
}
