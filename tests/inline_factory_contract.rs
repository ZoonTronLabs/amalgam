use amalgam::{
    Cache, Error, FactoryCancellation, FactoryCancellationReason, FactoryContext, FactoryError,
    MutationReceipt,
};
use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

fn once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}
fn ready<F: IntoFuture>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future.into_future());
    match once(future.as_mut()) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("ready factory must not require a runtime"),
    }
}
#[test]
fn ready_factory_and_receipt_finish_without_a_runtime() {
    let cache = Cache::<u64>::new();
    let token = Arc::new(Mutex::new(None));
    let saved = token.clone();
    let value = ready(cache.get_or_set_full_with_commit(
        "ready",
        move |context| async move {
            *saved.lock().unwrap() = Some(context.cancellation().clone());
            Ok::<_, amalgam::FactoryError>(context.value(7))
        },
        None,
        Box::new([]),
        amalgam::MaybeValue::none(),
    ))
    .unwrap();
    assert_eq!(value.value, 7);
    assert!(matches!(
        value.commit,
        amalgam::CommitReceipt::Mutation(MutationReceipt::Completed(_))
    ));
    let token = token.lock().unwrap().take().unwrap();
    assert!(matches!(
        token.check(),
        Err(Error::OperationCancelled {
            reason: FactoryCancellationReason::ScopeFinished
        })
    ));
    assert_eq!(
        ready(
            cache.get_or_set::<_, _, _, amalgam::FactoryError>("ready", |_| async {
                panic!("already cached")
            })
        )
        .unwrap(),
        7
    );
}

struct PinnedFactory {
    context: std::cell::RefCell<Option<FactoryContext<u64>>>,
    ready: Arc<AtomicBool>,
    address: Arc<AtomicUsize>,
    dropped: Arc<AtomicBool>,
    _pin: std::marker::PhantomPinned,
}
impl Future for PinnedFactory {
    type Output = Result<u64, FactoryError>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_ref().get_ref();
        let address = this as *const Self as usize;
        let first = this.address.load(Ordering::SeqCst);
        if first == 0 {
            this.address.store(address, Ordering::SeqCst);
        } else {
            assert_eq!(first, address);
        }
        if this.ready.load(Ordering::SeqCst) {
            Poll::Ready(Ok(this.context.borrow_mut().take().unwrap().value(9)))
        } else {
            // Waking twice during the first poll must neither recurse nor move it.
            cx.waker().wake_by_ref();
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}
impl Drop for PinnedFactory {
    fn drop(&mut self) {
        assert_eq!(
            self.address.load(Ordering::SeqCst),
            self as *const Self as usize
        );
        self.dropped.store(true, Ordering::SeqCst);
    }
}
#[test]
fn dropping_a_caller_keeps_the_pinned_factory_for_a_new_driver() {
    let cache = Cache::new();
    let running = Arc::new(AtomicUsize::new(0));
    let ready = Arc::new(AtomicBool::new(false));
    let address = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicBool::new(false));
    let runs = running.clone();
    let release = ready.clone();
    let seen = address.clone();
    let destroyed = dropped.clone();
    let mut caller = Box::pin(
        cache
            .get_or_set::<_, _, _, amalgam::FactoryError>("pinned", move |context| {
                runs.fetch_add(1, Ordering::SeqCst);
                PinnedFactory {
                    context: std::cell::RefCell::new(Some(context)),
                    ready: release,
                    address: seen,
                    dropped: destroyed,
                    _pin: std::marker::PhantomPinned,
                }
            })
            .into_future(),
    );
    assert!(once(caller.as_mut()).is_pending());
    drop(caller);
    assert!(!dropped.load(Ordering::SeqCst));
    ready.store(true, Ordering::SeqCst);
    let value = ready_value(&cache);
    assert_eq!(value, 9);
    assert_eq!(running.load(Ordering::SeqCst), 1);
    assert!(dropped.load(Ordering::SeqCst));
}
fn ready_value(cache: &Cache<u64>) -> u64 {
    ready(
        cache.get_or_set::<_, _, _, amalgam::FactoryError>("pinned", |_| async {
            panic!("a new driver must use the existing factory")
        }),
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_waiters_share_one_typed_factory_failure() {
    let cache = Cache::<u64>::new();
    let runs = Arc::new(AtomicUsize::new(0));
    let (release, released) = tokio::sync::oneshot::channel::<()>();
    let count = runs.clone();
    let mut leader = Box::pin(
        cache
            .get_or_set::<_, _, _, amalgam::FactoryError>("failed", move |_| async move {
                count.fetch_add(1, Ordering::SeqCst);
                released.await.unwrap();
                Err(FactoryError::from_source(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "source refused",
                )))
            })
            .into_future(),
    );
    assert!(once(leader.as_mut()).is_pending());
    let mut followers = Vec::with_capacity(20);
    for _ in 0..20 {
        let mut follower = Box::pin(
            cache
                .get_or_set::<_, _, _, amalgam::FactoryError>("failed", |_| async {
                    panic!("must not start another factory")
                })
                .into_future(),
        );
        assert!(once(follower.as_mut()).is_pending());
        followers.push(follower);
    }
    release.send(()).unwrap();
    let mut failures = vec![
        tokio::time::timeout(Duration::from_secs(2), leader)
            .await
            .unwrap()
            .unwrap_err(),
    ];
    for follower in followers {
        failures.push(
            tokio::time::timeout(Duration::from_secs(2), follower)
                .await
                .unwrap()
                .unwrap_err(),
        );
    }
    for error in failures {
        let Error::FactoryWithSource { source, .. } = error else {
            panic!("original error family must survive coalescing: {error:?}")
        };
        let cause = std::error::Error::source(&source)
            .unwrap()
            .downcast_ref::<std::io::Error>()
            .unwrap();
        assert_eq!(cause.kind(), std::io::ErrorKind::PermissionDenied);
    }
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    cache.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leader_gets_original_panic_and_followers_get_a_typed_outcome() {
    let cache = Cache::<u64>::new();
    let (release, released) = tokio::sync::oneshot::channel::<()>();
    let mut leader = Box::pin(
        cache
            .get_or_set::<_, _, _, amalgam::FactoryError>("panic", |_| async move {
                released.await.unwrap();
                std::panic::panic_any(91_u64)
            })
            .into_future(),
    );
    assert!(once(leader.as_mut()).is_pending());
    let mut follower = Box::pin(
        cache
            .get_or_set::<_, _, _, amalgam::FactoryError>("panic", |_| async {
                panic!("second factory")
            })
            .into_future(),
    );
    assert!(once(follower.as_mut()).is_pending());
    release.send(()).unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        std::future::poll_fn(|cx| {
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                leader.as_mut().poll(cx)
            })) {
                Ok(Poll::Pending) => Poll::Pending,
                Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
                Err(payload) => Poll::Ready(Err(payload)),
            }
        }),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(*result.downcast::<u64>().unwrap(), 91);
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), follower)
            .await
            .unwrap(),
        Err(Error::FactoryPanicked)
    ));
    cache.shutdown().await.unwrap();
}

struct Tracked(Arc<AtomicUsize>);
impl Drop for Tracked {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[test]
fn a_retained_factory_token_does_not_pin_a_completed_value() {
    let cache = Cache::<Arc<Tracked>>::new();
    let count = Arc::new(AtomicUsize::new(0));
    let token: Arc<Mutex<Option<FactoryCancellation>>> = Arc::new(Mutex::new(None));
    let saved = token.clone();
    let destroyed = count.clone();
    let value = ready(cache.get_or_set::<_, _, _, amalgam::FactoryError>(
        "drop",
        move |context| async move {
            *saved.lock().unwrap() = Some(context.cancellation().clone());
            Ok::<_, amalgam::FactoryError>(context.value(Arc::new(Tracked(destroyed))))
        },
    ))
    .unwrap();
    let weak = Arc::downgrade(&value);
    drop(value);
    ready(cache.try_remove("drop")).unwrap();
    assert!(weak.upgrade().is_none());
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert!(token.lock().unwrap().as_ref().unwrap().is_cancelled());
}

#[tokio::test]
async fn disabling_fail_safe_preserves_an_existing_conditional_snapshot() {
    let clock = Arc::new(amalgam::ManualClock::default());
    let cache = Cache::<u64>::builder()
        .clock(clock.clone())
        .try_build()
        .unwrap();
    let normal = amalgam::EntryOptions::new(Duration::from_secs(1));
    let retained = normal
        .clone()
        .with_fail_safe(true, Some(Duration::from_secs(60)), None);
    cache
        .get_or_set_with(
            "conditional",
            |ctx| async { Ok::<_, amalgam::FactoryError>(ctx.modified(7).etag("saved").done()) },
            retained,
        )
        .await
        .unwrap();
    clock.advance(Duration::from_secs(2));
    let refreshed = cache
        .get_or_set_with(
            "conditional",
            |ctx| async {
                assert_eq!(ctx.stale_value(), Some(&7));
                assert_eq!(ctx.stale_etag(), Some("saved"));
                ctx.not_modified()
            },
            normal,
        )
        .await
        .unwrap();
    assert_eq!(refreshed, 7);
    cache.shutdown().await.unwrap();
}
