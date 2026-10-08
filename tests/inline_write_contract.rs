//! Inline L1 writes still own close attribution and deterministic reclamation.
use amalgam::{Cache, EntryOptions, advanced::MutationReceipt};
use std::future::Future;
use std::future::IntoFuture;
use std::pin::pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

fn ready<T>(work: impl Future<Output = T>) -> T {
    let mut work = pin!(work);
    match work.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("memory-only mutation suspended without a runtime"),
    }
}
type Owner = Arc<Mutex<Weak<Cache<Arc<Probe>>>>>;

struct Probe {
    drops: Arc<AtomicUsize>,
    action: Option<Owner>,
}
impl Drop for Probe {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        if let Some(action) = &self.action {
            let owner = action.lock().unwrap().upgrade().unwrap();
            let nested = Arc::new(Self {
                drops: self.drops.clone(),
                action: None,
            });
            assert!(matches!(
                ready(owner.set("same", nested).with_receipt().into_future()).unwrap(),
                MutationReceipt::Completed(_)
            ));
        }
    }
}

#[test]
fn replacement_is_inline_and_reclaims_the_unpinned_value_before_return() {
    let cache: Cache<Arc<Probe>> = Cache::builder()
        .default_options(EntryOptions::new(Duration::from_secs(60)))
        .try_build()
        .unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    let old = Arc::new(Probe {
        drops: drops.clone(),
        action: None,
    });
    let old_weak = Arc::downgrade(&old);
    assert!(matches!(
        ready(cache.set("same", old).with_receipt().into_future()).unwrap(),
        MutationReceipt::Completed(_)
    ));
    let current = Arc::new(Probe {
        drops: drops.clone(),
        action: None,
    });
    assert!(matches!(
        ready(cache.set("same", current).with_receipt().into_future()).unwrap(),
        MutationReceipt::Completed(_)
    ));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(
        old_weak.upgrade().is_none(),
        "retired values must not wait for a maintenance tick"
    );
}

#[test]
fn retired_value_drop_can_reenter_the_same_key_after_the_commit() {
    for capacity in [None, Some(2)] {
        let builder = Cache::builder().default_options(EntryOptions::new(Duration::from_secs(60)));
        let cache: Arc<Cache<Arc<Probe>>> = Arc::new(match capacity {
            Some(capacity) => builder.max_capacity(capacity).try_build().unwrap(),
            None => builder.try_build().unwrap(),
        });
        let drops = Arc::new(AtomicUsize::new(0));
        let old = Arc::new(Probe {
            drops: drops.clone(),
            action: Some(Arc::new(Mutex::new(Arc::downgrade(&cache)))),
        });
        ready(cache.set("same", old).with_receipt().into_future()).unwrap();
        ready(
            cache
                .set(
                    "same",
                    Arc::new(Probe {
                        drops: drops.clone(),
                        action: None,
                    }),
                )
                .with_receipt()
                .into_future(),
        )
        .unwrap();
        assert_eq!(
            drops.load(Ordering::SeqCst),
            2,
            "the reentrant write replaces the outer candidate"
        );
        let value = ready(cache.try_get("same").into_future()).unwrap().unwrap();
        assert!(value.action.is_none());
    }
}

#[test]
fn unpolled_standalone_set_keeps_its_input_and_never_mutates_storage() {
    let cache = amalgam::Cache::<std::sync::Arc<u64>>::new();
    let incoming = std::sync::Arc::new(17);
    let weak = std::sync::Arc::downgrade(&incoming);
    let pending = cache.set("lazy", incoming).with_receipt().into_future();
    assert!(weak.upgrade().is_some());
    let mut read = std::pin::pin!(cache.try_get("lazy").into_future());
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    let std::task::Poll::Ready(Ok(value)) = std::future::Future::poll(read.as_mut(), &mut context)
    else {
        panic!("a standalone miss must complete without a runtime");
    };
    assert!(
        value.is_none(),
        "constructing a write must not store its value"
    );
    drop(pending);
    assert!(
        weak.upgrade().is_none(),
        "an abandoned input must not stay in storage"
    );
}

#[tokio::test]
async fn scalar_zero_jitter_callback_still_drains_before_shutdown_completes() {
    use amalgam::{Error, FactoryCancellationReason, provider::JitterSource};
    use std::sync::mpsc;

    struct BlockingJitter {
        started: mpsc::Sender<()>,
        release: Mutex<mpsc::Receiver<()>>,
    }
    impl JitterSource for BlockingJitter {
        fn sample(&self, maximum: Duration) -> Duration {
            assert!(maximum.is_zero());
            self.started.send(()).unwrap();
            self.release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(2))
                .unwrap();
            Duration::ZERO
        }
    }
    let (started, entered) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let cache = Cache::builder()
        .default_options(EntryOptions::new(Duration::from_secs(60)))
        .jitter_source(Arc::new(BlockingJitter {
            started,
            release: Mutex::new(released),
        }))
        .try_build()
        .unwrap();
    let writing = cache.clone();
    let writer = std::thread::spawn(move || {
        ready(std::future::IntoFuture::into_future(
            writing.set("key", 9_u64),
        ))
    });
    entered.recv_timeout(Duration::from_secs(2)).unwrap();
    cache.close();
    let mut shutdown = Box::pin(cache.shutdown());
    assert!(
        tokio::time::timeout(Duration::from_millis(25), &mut shutdown)
            .await
            .is_err()
    );
    release.send(()).unwrap();
    assert!(matches!(
        writer.join().unwrap(),
        Err(Error::OperationCancelled {
            reason: FactoryCancellationReason::CacheShutdown,
        })
    ));
    tokio::time::timeout(Duration::from_secs(2), shutdown)
        .await
        .unwrap()
        .unwrap();
}
