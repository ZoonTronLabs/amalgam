//! The plain origin hit preserves user callbacks and cleanup without an owned frame.
use amalgam::{
    BlockingCache, Cache, CacheEvent, CacheLevel, CacheOperation, EntryOptions, Error,
    FactoryCancellationReason, OperationOutcome,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

enum AfterSubscription {
    Continue,
    Close,
}
struct SubscribeOnDrop {
    cache: Cache<u64>,
    receiver: Arc<Mutex<Option<tokio::sync::broadcast::Receiver<CacheEvent>>>>,
    next: AfterSubscription,
}
impl Drop for SubscribeOnDrop {
    fn drop(&mut self) {
        *self.receiver.lock().unwrap() = Some(self.cache.events().subscribe());
        match self.next {
            AfterSubscription::Continue => {}
            AfterSubscription::Close => {
                let _ = self.cache.close();
            }
        }
    }
}

#[tokio::test]
async fn a_ready_origin_hit_reaches_an_observer_attached_by_the_unused_factory_drop() {
    let cache = Cache::new();
    cache.set("hit", 7).await.unwrap();
    let receiver = Arc::new(Mutex::new(None));
    let capture = SubscribeOnDrop {
        cache: cache.clone(),
        receiver: receiver.clone(),
        next: AfterSubscription::Continue,
    };
    assert_eq!(
        cache
            .get_or_set("hit", move |ctx| async move {
                drop(capture);
                Ok::<_, amalgam::FactoryError>(ctx.value(99))
            })
            .await
            .unwrap(),
        7,
    );
    let mut receiver = receiver.lock().unwrap().take().unwrap();
    assert!(
        matches!(receiver.try_recv().unwrap(), CacheEvent::Hit { key, stale: false } if key.as_ref() == "hit")
    );
    assert!(matches!(
        receiver.try_recv().unwrap(),
        CacheEvent::OperationCompleted {
            operation: CacheOperation::GetOrSet,
            outcome: OperationOutcome::Hit,
            level: Some(CacheLevel::Memory),
            ..
        }
    ));
    assert!(receiver.try_recv().is_err());
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn closing_from_the_unused_factory_drop_rejects_the_ready_value_once() {
    let cache = Cache::new();
    cache.set("hit", 7).await.unwrap();
    let receiver = Arc::new(Mutex::new(None));
    let capture = SubscribeOnDrop {
        cache: cache.clone(),
        receiver: receiver.clone(),
        next: AfterSubscription::Close,
    };
    assert!(matches!(
        cache
            .get_or_set("hit", move |ctx| async move {
                drop(capture);
                Ok::<_, amalgam::FactoryError>(ctx.value(99))
            })
            .await,
        Err(Error::OperationCancelled {
            reason: FactoryCancellationReason::CacheShutdown
        })
    ));
    let mut receiver = receiver.lock().unwrap().take().unwrap();
    assert!(matches!(
        receiver.try_recv().unwrap(),
        CacheEvent::OperationCompleted {
            operation: CacheOperation::GetOrSet,
            outcome: OperationOutcome::Cancelled,
            level: None,
            ..
        }
    ));
    assert!(receiver.try_recv().is_err());
    cache.shutdown().await.unwrap();
}

#[test]
fn native_ready_origins_preserve_unused_capture_events_and_close() {
    for next in [AfterSubscription::Continue, AfterSubscription::Close] {
        let cache = BlockingCache::new().unwrap();
        cache.try_set("hit", 7).unwrap().wait().unwrap();
        let receiver = Arc::new(Mutex::new(None));
        let closes = matches!(next, AfterSubscription::Close);
        let capture = SubscribeOnDrop {
            cache: cache.as_async().clone(),
            receiver: receiver.clone(),
            next,
        };
        let result = cache.get_or_set("hit", move |ctx| {
            drop(capture);
            Ok::<_, amalgam::FactoryError>(ctx.value(99))
        });
        let mut receiver = receiver.lock().unwrap().take().unwrap();
        let (outcome, level) = if closes {
            assert!(matches!(
                result,
                Err(Error::OperationCancelled {
                    reason: FactoryCancellationReason::CacheShutdown
                })
            ));
            (OperationOutcome::Cancelled, None)
        } else {
            assert_eq!(result.unwrap(), 7);
            assert!(
                matches!(receiver.try_recv().unwrap(), CacheEvent::Hit { key, stale: false } if key.as_ref() == "hit")
            );
            (OperationOutcome::Hit, Some(CacheLevel::Memory))
        };
        assert!(
            matches!(receiver.try_recv().unwrap(), CacheEvent::OperationCompleted {
            operation: CacheOperation::GetOrSet, outcome: actual, level: actual_level, ..
        } if actual == outcome && actual_level == level)
        );
        assert!(receiver.try_recv().is_err());
        cache.shutdown().unwrap();
    }
}

struct BlockingClone {
    armed: Arc<AtomicBool>,
    entered: mpsc::Sender<()>,
    release: Arc<Mutex<mpsc::Receiver<()>>>,
}
impl Clone for BlockingClone {
    fn clone(&self) -> Self {
        if self.armed.swap(false, Ordering::SeqCst) {
            self.entered.send(()).unwrap();
            self.release.lock().unwrap().recv().unwrap();
        }
        Self {
            armed: self.armed.clone(),
            entered: self.entered.clone(),
            release: self.release.clone(),
        }
    }
}
struct MarkDrop(Arc<AtomicBool>);
impl Drop for MarkDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_drains_ready_clone_and_the_unused_origin_capture() {
    let cache = Cache::new();
    let armed = Arc::new(AtomicBool::new(false));
    let (entered, entry) = mpsc::channel();
    let (release, released) = mpsc::channel();
    cache
        .set(
            "hit",
            BlockingClone {
                armed: armed.clone(),
                entered,
                release: Arc::new(Mutex::new(released)),
            },
        )
        .await
        .unwrap();
    let dropped = Arc::new(AtomicBool::new(false));
    let capture = MarkDrop(dropped.clone());
    let reader_cache = cache.clone();
    armed.store(true, Ordering::SeqCst);
    let reader = std::thread::spawn(move || {
        let request = reader_cache.get_or_set("hit", move |ctx| async move {
            drop(capture);
            Err(ctx.fail("the hot factory must not run"))
        });
        let mut future = std::pin::pin!(std::future::IntoFuture::into_future(request));
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        match std::future::Future::poll(future.as_mut(), &mut context) {
            std::task::Poll::Ready(result) => result,
            std::task::Poll::Pending => panic!("a ready hit must finish without a runtime"),
        }
    });
    entry.recv_timeout(Duration::from_secs(2)).unwrap();
    cache.close();
    let mut shutdown = Box::pin(cache.shutdown());
    assert!(
        tokio::time::timeout(Duration::from_millis(25), &mut shutdown)
            .await
            .is_err()
    );
    assert!(!dropped.load(Ordering::SeqCst));
    release.send(()).unwrap();
    assert!(matches!(
        reader.join().unwrap(),
        Err(Error::OperationCancelled {
            reason: FactoryCancellationReason::CacheShutdown
        })
    ));
    tokio::time::timeout(Duration::from_secs(2), shutdown)
        .await
        .unwrap()
        .unwrap();
    assert!(dropped.load(Ordering::SeqCst));
}

#[tokio::test]
async fn the_first_inline_factory_miss_starts_physical_cleanup() {
    let cache = Cache::builder()
        .default_options(EntryOptions::new(Duration::from_millis(5)))
        .try_build()
        .unwrap();
    let source = Arc::new(7_u64);
    let weak = Arc::downgrade(&source);
    drop(
        cache
            .get_or_set("expires", move |ctx| async move {
                Ok::<_, amalgam::FactoryError>(ctx.value(source))
            })
            .await
            .unwrap(),
    );
    tokio::time::timeout(Duration::from_secs(3), async {
        while weak.upgrade().is_some() {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("physical cleanup must run without a later cache read");
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn an_individual_eager_entry_refreshes_only_for_a_factory_origin() {
    use amalgam::{EagerThreshold, FactoryInvocation, ManualClock};
    let clock = Arc::new(ManualClock::default());
    let cache = Cache::builder().clock(clock.clone()).try_build().unwrap();
    cache
        .set("eager", 1)
        .options(|options| {
            options
                .with_duration(Duration::from_secs(60))
                .with_eager_refresh(EagerThreshold::new(0.5))
        })
        .await
        .unwrap();
    clock.advance(Duration::from_secs(31));
    assert_eq!(cache.get_or_set_value("eager", 99, None).await.unwrap(), 1);
    assert_eq!(
        cache
            .get_or_set("eager", |ctx| async move {
                assert_eq!(ctx.invocation(), FactoryInvocation::EagerRefresh);
                Ok::<_, amalgam::FactoryError>(ctx.value(2))
            })
            .await
            .unwrap(),
        1
    );
    cache.flush_pending().await.unwrap();
    assert_eq!(
        cache.read("eager", None).await.unwrap().into_value(),
        Some(2)
    );
    cache.shutdown().await.unwrap();
}

struct NoDropClone(u64);
struct CloneHook {
    entered: mpsc::Sender<()>,
    release: mpsc::Receiver<()>,
}
static NO_DROP_CLONE_HOOK: Mutex<Option<CloneHook>> = Mutex::new(None);
impl Clone for NoDropClone {
    fn clone(&self) -> Self {
        let hook = NO_DROP_CLONE_HOOK.lock().unwrap().take();
        if let Some(hook) = hook {
            hook.entered.send(()).unwrap();
            hook.release.recv().unwrap();
        }
        Self(self.0)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_value_without_drop_can_still_have_a_user_clone() {
    assert!(!std::mem::needs_drop::<NoDropClone>());
    for native in [false, true] {
        let cache = BlockingCache::new().unwrap();
        cache
            .try_set("hit", NoDropClone(7))
            .unwrap()
            .wait()
            .unwrap();
        let (entered, entry) = mpsc::channel();
        let (release, released) = mpsc::channel();
        *NO_DROP_CLONE_HOOK.lock().unwrap() = Some(CloneHook {
            entered,
            release: released,
        });
        let reader_cache = cache.clone();
        let reader = std::thread::spawn(move || {
            if native {
                reader_cache.read("hit", None)
            } else {
                let request = reader_cache.as_async().read("hit", None);
                let mut future = std::pin::pin!(std::future::IntoFuture::into_future(request));
                let mut context = std::task::Context::from_waker(std::task::Waker::noop());
                match std::future::Future::poll(future.as_mut(), &mut context) {
                    std::task::Poll::Ready(result) => result,
                    std::task::Poll::Pending => panic!("a ready hit needs no runtime"),
                }
            }
        });
        entry.recv_timeout(Duration::from_secs(2)).unwrap();
        cache.as_async().close();
        let mut shutdown = Box::pin(cache.as_async().shutdown());
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut shutdown)
                .await
                .is_err()
        );
        release.send(()).unwrap();
        assert!(matches!(
            reader.join().unwrap(),
            Err(Error::OperationCancelled {
                reason: FactoryCancellationReason::CacheShutdown,
            })
        ));
        tokio::time::timeout(Duration::from_secs(2), shutdown)
            .await
            .unwrap()
            .unwrap();
    }
}
