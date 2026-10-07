//! A recipient arriving inside ordinary Clone sees the same terminal operation.
use amalgam::{Cache, CacheEvent, Events, OperationOutcome};
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

#[derive(Default)]
struct Observation {
    armed: AtomicBool,
    events: Mutex<Option<Events>>,
    receiver: Mutex<Option<broadcast::Receiver<CacheEvent>>>,
}
struct Value(Arc<Observation>);
impl Clone for Value {
    fn clone(&self) -> Self {
        if self.0.armed.swap(false, Ordering::SeqCst) {
            let receiver = self.0.events.lock().unwrap().as_ref().unwrap().subscribe();
            *self.0.receiver.lock().unwrap() = Some(receiver);
            panic!("ordinary value clone failed after admitting an observer");
        }
        Self(self.0.clone())
    }
}

#[tokio::test]
async fn late_observer_sees_one_panic_and_cache_is_usable_after_clone_unwinds() {
    let state = Arc::new(Observation::default());
    let cache = Cache::new();
    *state.events.lock().unwrap() = Some(cache.events().clone());
    cache
        .set("key", Value(state.clone()))
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    cache.read("key", None).await.unwrap().into_value().unwrap();
    state.armed.store(true, Ordering::SeqCst);
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut read = std::pin::pin!(cache.read("key", None));
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        read.as_mut().poll(&mut cx)
    }));
    assert!(panicked.is_err());
    let mut receiver = state.receiver.lock().unwrap().take().unwrap();
    let mut outcomes = Vec::new();
    while let Ok(event) = receiver.try_recv() {
        if let CacheEvent::OperationCompleted { outcome, .. } = event {
            outcomes.push(outcome);
        }
    }
    assert_eq!(outcomes, vec![OperationOutcome::Panicked]);
    drop(receiver);
    assert!(cache.read("key", None).await.unwrap().has_value());
    cache
        .remove("key")
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    cache.shutdown().await.unwrap();
}

#[derive(Clone)]
struct CompletionValue {
    number: u64,
    subscribe_on_drop: Option<Arc<Observation>>,
}
impl Drop for CompletionValue {
    fn drop(&mut self) {
        if let Some(state) = &self.subscribe_on_drop {
            let receiver = state.events.lock().unwrap().as_ref().unwrap().subscribe();
            *state.receiver.lock().unwrap() = Some(receiver);
        }
    }
}

#[tokio::test]
async fn observer_admitted_by_unused_default_drop_receives_the_hit_completion() {
    let state = Arc::new(Observation::default());
    let cache = Cache::new();
    *state.events.lock().unwrap() = Some(cache.events().clone());
    cache
        .set(
            "key",
            CompletionValue {
                number: 7,
                subscribe_on_drop: None,
            },
        )
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    let value = cache
        .read_or_default(
            "key",
            CompletionValue {
                number: 99,
                subscribe_on_drop: Some(state.clone()),
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(value.number, 7);
    let mut receiver = state.receiver.lock().unwrap().take().unwrap();
    assert!(
        matches!(receiver.try_recv().unwrap(), CacheEvent::Hit { key, stale: false } if &*key == "key")
    );
    assert!(matches!(
        receiver.try_recv().unwrap(),
        CacheEvent::OperationCompleted {
            operation: amalgam::CacheOperation::GetOrDefault,
            outcome: OperationOutcome::Hit,
            level: Some(amalgam::CacheLevel::Memory),
            ..
        }
    ));
    assert!(matches!(
        receiver.try_recv(),
        Err(broadcast::error::TryRecvError::Empty)
    ));
    drop(receiver);
    cache.shutdown().await.unwrap();
}
