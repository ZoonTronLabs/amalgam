//! Ownership contracts across the builtin and general factory paths.
use amalgam::*;
use std::cell::RefCell;
use std::future::{Future, IntoFuture};
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

fn once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}
fn options(general: bool) -> EntryOptions {
    let options = EntryOptions::new(Duration::from_secs(60));
    if general {
        options.with_fail_safe(true, Some(Duration::from_secs(120)), None)
    } else {
        options
    }
}

struct PinnedOrigin {
    context: RefCell<Option<FactoryContext<u64>>>,
    ready: Arc<AtomicBool>,
    address: Arc<AtomicUsize>,
    dropped: Arc<AtomicBool>,
    panics: bool,
    _pin: PhantomPinned,
}
impl Future for PinnedOrigin {
    type Output = std::result::Result<FactoryProduct<u64>, FactoryError>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_ref().get_ref();
        let address = this as *const Self as usize;
        let first = this.address.load(Ordering::SeqCst);
        if first == 0 {
            this.address.store(address, Ordering::SeqCst);
        } else {
            assert_eq!(first, address, "a previously polled future moved");
        }
        if !this.ready.load(Ordering::SeqCst) {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        if this.panics {
            std::panic::panic_any(91_u64);
        }
        Poll::Ready(Ok(this.context.borrow_mut().take().unwrap().value(9)))
    }
}
impl Drop for PinnedOrigin {
    fn drop(&mut self) {
        assert_eq!(
            self.address.load(Ordering::SeqCst),
            self as *const Self as usize
        );
        self.dropped.store(true, Ordering::SeqCst);
    }
}

#[test]
fn a_new_caller_drives_the_same_pinned_origin_across_option_paths_without_a_runtime() {
    for (first_general, next_general) in [(true, true), (true, false), (false, true)] {
        let cache = Cache::<u64>::new();
        let runs = Arc::new(AtomicUsize::new(0));
        let ready = Arc::new(AtomicBool::new(false));
        let address = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicBool::new(false));
        let count = runs.clone();
        let released = ready.clone();
        let observed = address.clone();
        let destroyed = dropped.clone();
        let mut first = Box::pin(
            cache
                .get_or_set("retained", move |context| {
                    count.fetch_add(1, Ordering::SeqCst);
                    PinnedOrigin {
                        context: RefCell::new(Some(context)),
                        ready: released,
                        address: observed,
                        dropped: destroyed,
                        panics: false,
                        _pin: PhantomPinned,
                    }
                })
                .options(move |_| options(first_general))
                .into_future(),
        );
        assert!(once(first.as_mut()).is_pending());
        assert_eq!(runs.load(Ordering::SeqCst), 1);
        drop(first);
        assert!(
            !dropped.load(Ordering::SeqCst),
            "caller destruction cancelled shared work"
        );
        ready.store(true, Ordering::SeqCst);
        let mut next = Box::pin(
            cache
                .get_or_set("retained", |_| async {
                    panic!("a replacement caller started a second factory")
                })
                .options(move |_| options(next_general))
                .into_future(),
        );
        assert!(matches!(once(next.as_mut()), Poll::Ready(Ok(9))));
        assert_eq!(runs.load(Ordering::SeqCst), 1);
        assert!(dropped.load(Ordering::SeqCst));
        cache.close();
    }
}

#[test]
fn panic_is_preserved_for_the_leader_and_typed_for_waiters_across_option_paths() {
    for (first_general, next_general) in [(true, true), (true, false), (false, true)] {
        let cache = Cache::<u64>::new();
        let ready = Arc::new(AtomicBool::new(false));
        let address = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicBool::new(false));
        let released = ready.clone();
        let destroyed = dropped.clone();
        let mut leader = Box::pin(
            cache
                .get_or_set("panic", move |context| PinnedOrigin {
                    context: RefCell::new(Some(context)),
                    ready: released,
                    address,
                    dropped: destroyed,
                    panics: true,
                    _pin: PhantomPinned,
                })
                .options(move |_| options(first_general))
                .into_future(),
        );
        assert!(once(leader.as_mut()).is_pending());
        let mut waiter = Box::pin(
            cache
                .get_or_set("panic", |_| async {
                    panic!("a waiter started its own factory")
                })
                .options(move |_| options(next_general))
                .into_future(),
        );
        assert!(once(waiter.as_mut()).is_pending());
        ready.store(true, Ordering::SeqCst);
        let panic =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| once(leader.as_mut())))
                .unwrap_err();
        assert_eq!(*panic.downcast::<u64>().unwrap(), 91);
        assert!(matches!(
            once(waiter.as_mut()),
            Poll::Ready(Err(Error::FactoryPanicked))
        ));
        assert!(dropped.load(Ordering::SeqCst));
        let mut retry = Box::pin(
            cache
                .get_or_set("panic", |ctx| async { Ok(ctx.value(7)) })
                .into_future(),
        );
        assert!(matches!(once(retry.as_mut()), Poll::Ready(Ok(7))));
        cache.close();
    }
}

fn no_recovery() -> RecoveryConfig {
    RecoveryConfig {
        enabled: false,
        ..RecoveryConfig::default()
    }
}
async fn started<F: Future>(future: Pin<&mut F>, runs: &AtomicUsize) {
    let mut future = future;
    tokio::time::timeout(
        Duration::from_secs(2),
        std::future::poll_fn(|cx| {
            assert!(future.as_mut().poll(cx).is_pending());
            if runs.load(Ordering::SeqCst) == 1 {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }),
    )
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dropped_hybrid_caller_retains_one_factory_and_commits_for_its_waiters() {
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let backend = Arc::new(InMemoryDistributedCache::new(clock.clone()));
    let locker = Arc::new(InMemoryDistributedLocker::new(clock.clone()));
    let backplane = Arc::new(InProcessBackplane::default());
    let cache = Cache::<u64>::builder()
        .clock(clock.clone())
        .distributed(backend.clone())
        .serializer(Arc::new(JsonSerializer))
        .distributed_locker(locker)
        .backplane(backplane)
        .auto_recovery(no_recovery())
        .try_build_ready()
        .await
        .unwrap();
    let runs = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let token = Arc::new(Mutex::new(None));
    let count = runs.clone();
    let released = gate.clone();
    let saved = token.clone();
    let mut first = Box::pin(
        cache
            .get_or_set("hybrid", move |ctx| async move {
                count.fetch_add(1, Ordering::SeqCst);
                *saved.lock().unwrap() = Some(ctx.cancellation().clone());
                released.acquire().await.unwrap().forget();
                ctx.cancellation().check().unwrap();
                Ok(ctx.value(9))
            })
            .into_future(),
    );
    started(first.as_mut(), &runs).await;
    drop(first);
    assert!(!token.lock().unwrap().as_ref().unwrap().is_cancelled());
    let mut waiters = Vec::with_capacity(30);
    for _ in 0..30 {
        let mut waiter = Box::pin(
            cache
                .get_or_set("hybrid", |_| async {
                    panic!("hybrid waiter started another factory")
                })
                .into_future(),
        );
        assert!(once(waiter.as_mut()).is_pending());
        waiters.push(waiter);
    }
    gate.add_permits(1);
    for waiter in waiters {
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), waiter)
                .await
                .unwrap()
                .unwrap(),
            9
        );
    }
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    let other = Cache::<u64>::builder()
        .clock(clock)
        .distributed(backend)
        .serializer(Arc::new(JsonSerializer))
        .auto_recovery(no_recovery())
        .try_build()
        .unwrap();
    assert_eq!(
        other
            .get_or_set("hybrid", |_| async {
                panic!("retained result was not committed to L2")
            })
            .await
            .unwrap(),
        9
    );
    cache.shutdown().await.unwrap();
    other.shutdown().await.unwrap();
}

struct Destroyed(Arc<AtomicUsize>);
impl Drop for Destroyed {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[tokio::test]
async fn explicit_cancellation_still_releases_a_general_factory_without_failsafe() {
    let cache = Cache::<u64>::builder()
        .default_options(options(true))
        .try_build()
        .unwrap();
    let source = CancellationSource::new();
    let runs = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let count = runs.clone();
    let guard = Destroyed(drops.clone());
    let mut first = Box::pin(
        cache
            .get_or_set("explicit", move |ctx| async move {
                let _guard = guard;
                count.fetch_add(1, Ordering::SeqCst);
                std::future::pending::<()>().await;
                Ok(ctx.value(9))
            })
            .fail_safe_default(99)
            .cancellation(source.token())
            .into_future(),
    );
    started(first.as_mut(), &runs).await;
    source.cancel();
    assert!(matches!(
        first.await,
        Err(Error::OperationCancelled {
            reason: FactoryCancellationReason::CallerCancelled
        })
    ));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(
        cache
            .get_or_set("explicit", |ctx| async { Ok(ctx.value(7)) })
            .await
            .unwrap(),
        7
    );
    cache.shutdown().await.unwrap();
}

struct ParkFirstRead {
    reads: AtomicUsize,
    ready: Arc<AtomicBool>,
}
#[async_trait::async_trait]
impl DistributedCache for ParkFirstRead {
    async fn get(&self, _: &str) -> Result<Option<Vec<u8>>> {
        if self.reads.fetch_add(1, Ordering::SeqCst) == 0 {
            std::future::poll_fn(|cx| {
                if self.ready.load(Ordering::SeqCst) {
                    Poll::Ready(())
                } else {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            })
            .await;
        }
        Ok(None)
    }
    async fn set(&self, _: &str, _: Vec<u8>, _: Option<Duration>) -> Result<()> {
        Ok(())
    }
    async fn remove(&self, _: &str) -> Result<()> {
        Ok(())
    }
}
struct Wakes(AtomicUsize);
impl std::task::Wake for Wakes {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[test]
fn a_waiter_admitted_before_factory_installation_is_not_stranded() {
    let reads_ready = Arc::new(AtomicBool::new(false));
    let store = Arc::new(ParkFirstRead {
        reads: AtomicUsize::new(0),
        ready: reads_ready.clone(),
    });
    let cache = Cache::<u64>::builder()
        .distributed(store)
        .serializer(Arc::new(JsonSerializer))
        .auto_recovery(no_recovery())
        .try_build()
        .unwrap();
    let runs = Arc::new(AtomicUsize::new(0));
    let ready = Arc::new(AtomicBool::new(false));
    let dropped = Arc::new(AtomicBool::new(false));
    let count = runs.clone();
    let released = ready.clone();
    let destroyed = dropped.clone();
    let mut first = Box::pin(
        cache
            .get_or_set("late-install", move |context| {
                count.fetch_add(1, Ordering::SeqCst);
                PinnedOrigin {
                    context: RefCell::new(Some(context)),
                    ready: released,
                    address: Arc::new(AtomicUsize::new(0)),
                    dropped: destroyed,
                    panics: false,
                    _pin: PhantomPinned,
                }
            })
            .into_future(),
    );
    assert!(once(first.as_mut()).is_pending());
    assert_eq!(
        runs.load(Ordering::SeqCst),
        0,
        "the first request is still reading L2 under its lock"
    );
    let wakes = Arc::new(Wakes(AtomicUsize::new(0)));
    let waker = Waker::from(wakes.clone());
    let mut second = Box::pin(
        cache
            .get_or_set("late-install", |_| async { panic!("second factory") })
            .into_future(),
    );
    assert!(
        second
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    reads_ready.store(true, Ordering::SeqCst);
    assert!(once(first.as_mut()).is_pending());
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    assert!(
        wakes.0.load(Ordering::SeqCst) > 0,
        "factory installation must wake the existing lock waiter"
    );
    drop(first);
    assert!(!dropped.load(Ordering::SeqCst));
    ready.store(true, Ordering::SeqCst);
    assert!(matches!(once(second.as_mut()), Poll::Ready(Ok(9))));
    assert!(dropped.load(Ordering::SeqCst));
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    cache.close();
}

#[tokio::test]
async fn explicit_cancellation_can_stop_retained_work_after_its_caller_was_dropped() {
    let cache = Cache::<u64>::builder()
        .default_options(options(true))
        .try_build()
        .unwrap();
    let source = CancellationSource::new();
    let runs = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let count = runs.clone();
    let guard = Destroyed(drops.clone());
    let token = Arc::new(Mutex::new(None));
    let saved = token.clone();
    let mut first = Box::pin(
        cache
            .get_or_set("detached-explicit", move |ctx| async move {
                let _guard = guard;
                *saved.lock().unwrap() = Some(ctx.cancellation().clone());
                count.fetch_add(1, Ordering::SeqCst);
                std::future::pending::<()>().await;
                Ok(ctx.value(9))
            })
            .cancellation(source.token())
            .into_future(),
    );
    started(first.as_mut(), &runs).await;
    drop(first);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    source.cancel();
    cache.flush_pending().await.unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let token = token.lock().unwrap().take().unwrap();
    assert_eq!(
        token.cancelled().await,
        FactoryCancellationReason::CallerCancelled
    );
    assert_eq!(
        cache
            .get_or_set("detached-explicit", |ctx| async { Ok(ctx.value(7)) })
            .await
            .unwrap(),
        7
    );
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_retained_background_panic_preserves_the_supervised_shutdown_cause() {
    let cache = Cache::<u64>::builder()
        .default_options(options(true))
        .try_build()
        .unwrap();
    let runs = Arc::new(AtomicUsize::new(0));
    let count = runs.clone();
    let (release, released) = tokio::sync::oneshot::channel::<()>();
    let mut first = Box::pin(
        cache
            .get_or_set("background-panic", move |_| async move {
                count.fetch_add(1, Ordering::SeqCst);
                released.await.unwrap();
                panic!("retained original factory panic")
            })
            .into_future(),
    );
    started(first.as_mut(), &runs).await;
    drop(first);
    release.send(()).unwrap();
    let _ = cache.flush_pending().await;
    let Error::Shutdown(report) = cache.shutdown().await.unwrap_err() else {
        panic!("shutdown must preserve the original supervised panic")
    };
    assert!(report.failures().iter().any(|failure| matches!(failure,
        ShutdownFailure::BackgroundTask { task: ShutdownTask::Factory, source } if source.is_panic()
    )));
}
