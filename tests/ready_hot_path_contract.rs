//! Ready hits preserve raw-key options, eager ownership and local-only startup.
use amalgam::{Cache, DefaultEntryOptionsProvider, EagerThreshold, EntryOptions, ManualClock};
use std::future::{Future, IntoFuture, poll_fn};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;
use tokio::sync::Notify;

#[derive(Default)]
struct Defaults(Mutex<Vec<String>>);
impl DefaultEntryOptionsProvider for Defaults {
    fn options_for_with_defaults(&self, key: &str, _: &EntryOptions) -> Option<EntryOptions> {
        self.0.lock().unwrap().push(key.to_owned());
        None
    }
}

#[tokio::test]
async fn provider_runs_once_on_miss_and_hit_uses_raw_key_and_explicit_options_bypass_it() {
    let provider = Arc::new(Defaults::default());
    let cache = Cache::builder()
        .key_prefix("tenant:")
        .default_options_provider(provider.clone())
        .try_build()
        .unwrap();
    let factories = Arc::new(AtomicUsize::new(0));
    for _ in 0..2 {
        let factories = factories.clone();
        assert_eq!(
            cache
                .get_or_set("key", move |ctx| async move {
                    factories.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(ctx.original_key(), "key");
                    Ok::<_, amalgam::FactoryError>(ctx.value(7))
                })
                .await
                .unwrap(),
            7
        );
    }
    assert_eq!(&*provider.0.lock().unwrap(), &["key", "key"]);
    assert_eq!(factories.load(Ordering::SeqCst), 1);
    assert_eq!(
        cache
            .read("key", Some(EntryOptions::default()))
            .await
            .unwrap()
            .into_value(),
        Some(7)
    );
    assert_eq!(provider.0.lock().unwrap().len(), 2);
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn eager_hit_finishes_in_one_poll_while_its_factory_is_pending() {
    let clock = Arc::new(ManualClock::default());
    let cache = Cache::builder()
        .clock(clock.clone())
        .default_options(
            EntryOptions::new(Duration::from_secs(10)).with_eager_refresh(EagerThreshold::new(0.5)),
        )
        .try_build()
        .unwrap();
    cache.try_set("key", 7).await.unwrap().wait().await.unwrap();
    clock.advance(Duration::from_secs(6));
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let began = started.clone();
    let released = release.clone();
    let mut hit = Box::pin(
        cache
            .get_or_set("key", move |ctx| async move {
                began.notify_one();
                released.notified().await;
                Ok::<_, amalgam::FactoryError>(ctx.value(8))
            })
            .into_future(),
    );
    poll_fn(|cx| match hit.as_mut().poll(cx) {
        Poll::Ready(value) => {
            assert_eq!(value.unwrap(), 7);
            Poll::Ready(())
        }
        Poll::Pending => panic!("a fresh eager hit parked the caller"),
    })
    .await;
    tokio::time::timeout(Duration::from_secs(2), started.notified())
        .await
        .unwrap();
    assert_eq!(cache.get_or_set_value("key", 99, None).await.unwrap(), 7);
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(2), async {
        while cache.read("key", None).await.unwrap().into_value() != Some(8) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    cache.shutdown().await.unwrap();
}

struct RuntimeCheckedValue {
    value: u64,
    armed: Arc<std::sync::atomic::AtomicBool>,
}
impl Clone for RuntimeCheckedValue {
    fn clone(&self) -> Self {
        if self.armed.load(Ordering::Acquire) {
            assert!(tokio::runtime::Handle::try_current().is_err());
        }
        Self {
            value: self.value,
            armed: self.armed.clone(),
        }
    }
}

#[test]
fn native_and_async_ready_reads_copy_on_the_caller_without_entering_a_runtime() {
    let armed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let cache = amalgam::BlockingCache::new().unwrap();
    cache
        .try_set(
            "key",
            RuntimeCheckedValue {
                value: 42,
                armed: armed.clone(),
            },
        )
        .unwrap()
        .wait()
        .unwrap();
    armed.store(true, Ordering::Release);
    assert_eq!(
        cache.read("key", None).unwrap().into_value().unwrap().value,
        42
    );
    let mut lookup = std::pin::pin!(cache.as_async().read("key", None));
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    let std::task::Poll::Ready(result) = std::future::Future::poll(lookup.as_mut(), &mut context)
    else {
        panic!("a fresh hit must complete without a runtime");
    };
    assert_eq!(result.unwrap().into_value().unwrap().value, 42);
    // The same native operation transfers a real miss once to its executor.
    assert!(!cache.read("absent", None).unwrap().has_value());
    cache.shutdown().unwrap();
}
