//! Native and async handles must share values, coordination and final lifetime.
#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

use std::future::Future;
use std::num::NonZeroUsize;
use std::result::Result as StdResult;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use amalgam::advanced::{
    BlockingRuntime, BlockingRuntimeError, BlockingThreadPool, DrainOperation,
};
use amalgam::{
    BlockingCache, Cache, CacheEvent, CancellationSource, Error, FactoryContext, FactoryError,
};

// Launch must return before a task whose completion is released by its caller.
fn spawn_driven<T: Send + 'static>(
    driver: &BlockingRuntime,
    work: impl Future<Output = T> + Send + 'static,
) -> tokio::task::JoinHandle<T> {
    struct Started<T>(tokio::task::JoinHandle<T>);
    driver.run(async { Started(tokio::spawn(work)) }).0
}

fn runtime() -> BlockingRuntime {
    BlockingRuntime::with_workers(NonZeroUsize::MIN, NonZeroUsize::MIN).unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn async_handle_keeps_the_executor_and_cache_alive_after_native_drop() {
    let native = BlockingCache::<u64>::new().unwrap();
    let _ = native
        .set("before", 7)
        .with_receipt()
        .execute()
        .unwrap()
        .wait()
        .unwrap();
    let asynchronous = native.as_async().clone();
    let identity = native.instance_id().to_owned();
    std::mem::drop(native);
    assert_eq!(
        asynchronous.instance_id(),
        identity,
        "contract: async handle keeps the executor and cache alive after native drop"
    );
    assert_eq!(
        asynchronous.try_get("before").await.unwrap(),
        Some(7),
        "contract: async handle keeps the executor and cache alive after native drop"
    );
    let value = asynchronous
        .get_or_set(
            "after",
            amalgam::source::factory(|ctx| async move {
                tokio::time::sleep(Duration::from_millis(2)).await;
                Ok::<_, FactoryError>(ctx.value(9))
            }),
        )
        .await
        .unwrap();
    assert_eq!(
        value, 9,
        "contract: async handle keeps the executor and cache alive after native drop"
    );
    asynchronous.flush_pending().await.unwrap();
    asynchronous.shutdown().await.unwrap();
}

#[test]
fn native_operations_progress_from_their_own_single_io_worker() {
    let driver = runtime();
    let cache = BlockingCache::<u64>::on_runtime(Cache::builder(), driver.clone()).unwrap();
    let request = cache.clone();
    let (sender, receiver) = mpsc::channel();
    let work = spawn_driven(&driver, async move {
        let result = request
            .get_or_set(
                "own-worker",
                typed_blocking_factory(|ctx| {
                    std::thread::sleep(Duration::from_millis(2));
                    Ok::<_, FactoryError>(ctx.value(42))
                }),
            )
            .cancellation(CancellationSource::new().token())
            .execute();
        sender.send(result).unwrap();
    });
    assert_eq!(
        receiver
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .unwrap(),
        42,
        "contract: native operations progress from their own single io worker"
    );
    driver.run(work).unwrap();
    cache.shutdown().unwrap();
}

#[test]
fn async_origin_and_native_caller_share_one_inflight_value() {
    let driver = runtime();
    let cache = BlockingCache::<u64>::on_runtime(Cache::builder(), driver.clone()).unwrap();
    let asynchronous = cache.as_async().clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let first_calls = calls.clone();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let origin = spawn_driven(&driver, async move {
        asynchronous
            .get_or_set(
                "shared-flight",
                amalgam::source::factory(move |ctx| async move {
                    first_calls.fetch_add(1, Ordering::SeqCst);
                    started_tx.send(()).unwrap();
                    release_rx.await.unwrap();
                    Ok::<_, FactoryError>(ctx.value(17))
                }),
            )
            .await
    });
    started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let mut observed = cache.events().subscribe();
    let second_calls = calls.clone();
    let request = cache.clone();
    let (entered_tx, entered_rx) = mpsc::channel();
    let caller = std::thread::spawn(move || {
        entered_tx.send(()).unwrap();
        request
            .get_or_set(
                "shared-flight",
                amalgam::source::factory(move |ctx| {
                    second_calls.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, FactoryError>(ctx.value(99))
                }),
            )
            .execute()
    });
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let miss = driver.run(async {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let CacheEvent::Miss { key } = observed.recv().await.unwrap()
                    && key.as_ref() == "shared-flight"
                {
                    return;
                }
            }
        })
        .await
    });
    release_tx.send(()).unwrap();
    miss.expect("native caller must enter the cold path while the async origin is blocked");
    assert_eq!(
        driver.run(origin).unwrap().unwrap(),
        17,
        "contract: async origin and native caller share one inflight value"
    );
    assert_eq!(
        caller.join().unwrap().unwrap(),
        17,
        "contract: async origin and native caller share one inflight value"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "contract: async origin and native caller share one inflight value"
    );
    cache.shutdown().unwrap();
}

#[test]
fn callbacks_cannot_drain_their_own_cache() {
    const CHILD: &str = "AMALGAM_NATIVE_SELF_DRAIN_CASE";
    if let Ok(case) = std::env::var(CHILD) {
        let cache = BlockingCache::<u64>::new().unwrap();
        let inside = cache.clone();
        let operation = case.clone();
        let factory = move |ctx: FactoryContext<u64>| {
            let rejection = if operation.ends_with("shutdown") {
                inside.shutdown().map(|_| ())
            } else {
                inside.flush_pending()
            };
            let expected = if operation.ends_with("shutdown") {
                DrainOperation::Shutdown
            } else {
                DrainOperation::FlushPending
            };
            assert!(
                matches!(rejection,
                    Err(Error::ReentrantDrain { operation }) if operation == expected
                ),
                "contract: callbacks cannot drain their own cache"
            );
            Ok::<_, FactoryError>(ctx.value(7))
        };
        let answer = if case.starts_with("inline") {
            cache
                .get_or_set("self-drain", amalgam::source::factory(factory))
                .execute()
        } else {
            cache
                .get_or_set("self-drain", typed_blocking_factory(factory))
                .cancellation(CancellationSource::new().token())
                .execute()
        };
        assert_eq!(
            answer.unwrap(),
            7,
            "contract: callbacks cannot drain their own cache"
        );
        assert_eq!(
            cache.try_get("self-drain").execute().unwrap(),
            Some(7),
            "contract: callbacks cannot drain their own cache"
        );
        cache.shutdown().unwrap();
        return;
    }
    for case in [
        "inline-shutdown",
        "inline-flush",
        "offloaded-shutdown",
        "offloaded-flush",
    ] {
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "callbacks_cannot_drain_their_own_cache",
                "--nocapture",
            ])
            .env(CHILD, case)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        let completed = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break Some(status);
            }
            if Instant::now() >= deadline {
                break None;
            }
            std::thread::sleep(Duration::from_millis(5));
        };
        if completed.is_none() {
            child.kill().unwrap();
        }
        let output = child.wait_with_output().unwrap();
        assert!(
            completed.is_some_and(|s| s.success()),
            "{case} did not reject self drainage: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn invalid_callback_count_is_rejected_before_any_factory_dispatch() {
    for (workers, callbacks, pool, maximum) in [
        (
            1,
            tokio::sync::Semaphore::MAX_PERMITS + 1,
            BlockingThreadPool::RootCallbacks,
            512,
        ),
        (1, 513, BlockingThreadPool::RootCallbacks, 512),
        (65, 1, BlockingThreadPool::IoWorkers, 64),
    ] {
        let error = BlockingRuntime::with_workers(
            NonZeroUsize::new(workers).unwrap(),
            NonZeroUsize::new(callbacks).unwrap(),
        )
        .err()
        .expect("invalid configuration must fail before executor creation");
        let requested_count = match pool {
            BlockingThreadPool::IoWorkers => workers,
            BlockingThreadPool::RootCallbacks => callbacks,
        };
        assert!(
            matches!(error, BlockingRuntimeError::ThreadLimit {
            pool: actual, requested, maximum: limit
        } if actual == pool && requested.get() == requested_count && limit == maximum),
            "contract: invalid callback count is rejected before any factory dispatch"
        );
    }
}

fn typed_blocking_factory<V, F>(factory: F) -> F
where
    F: FnOnce(FactoryContext<V>) -> StdResult<V, FactoryError>,
{
    factory
}
