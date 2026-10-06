//! Reader slots release values deterministically and keep optional cloners outside locks.
use amalgam::{Cache, CloneError, EntryOptions, Error, FactoryCancellationReason, ValueCloner};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

struct Value(u64);

#[tokio::test]
async fn remove_and_replace_release_unobserved_values_before_receipt_completion() {
    let cache = Cache::<Arc<Value>>::new();
    let original = Arc::new(Value(1));
    let old = Arc::downgrade(&original);
    cache
        .try_set("replace", original)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    cache
        .try_set("replace", Arc::new(Value(2)))
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(
        old.upgrade().is_none(),
        "retired map metadata must not retain the old user value"
    );
    let removed = Arc::new(Value(3));
    let old = Arc::downgrade(&removed);
    cache
        .try_set("remove", removed)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    cache
        .try_remove("remove")
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(
        old.upgrade().is_none(),
        "a completed remove must release its unobserved value"
    );
    cache.shutdown().await.unwrap();
}

#[derive(Default)]
struct CopyGate {
    armed: AtomicBool,
    entered: Notify,
    released: Mutex<bool>,
    release: Condvar,
}
impl ValueCloner<Arc<Value>> for CopyGate {
    fn clone_value(&self, value: &Arc<Value>) -> Result<Arc<Value>, CloneError> {
        if self.armed.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            drop(
                self.release
                    .wait_while(self.released.lock().unwrap(), |released| !*released)
                    .unwrap(),
            );
        }
        Ok(value.clone())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replacement_keeps_an_owned_cloner_source_alive_and_shutdown_drains_its_last_drop() {
    let gate = Arc::new(CopyGate::default());
    let cache = Cache::builder()
        .value_cloner(gate.clone())
        .default_options(EntryOptions::default().with_enable_auto_clone(true))
        .try_build()
        .unwrap();
    let original = Arc::new(Value(1));
    let old = Arc::downgrade(&original);
    cache
        .try_set("key", original)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert_eq!(
        cache
            .read("key", None)
            .await
            .unwrap()
            .into_value()
            .unwrap()
            .0,
        1
    );
    gate.armed.store(true, Ordering::SeqCst);
    let reading = cache.clone();
    let read = tokio::task::spawn_blocking(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(reading.read("key", None))
    });
    tokio::time::timeout(Duration::from_secs(2), gate.entered.notified())
        .await
        .unwrap();
    cache
        .try_set("key", Arc::new(Value(2)))
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(
        old.upgrade().is_some(),
        "an active copy must still own its source"
    );
    cache.close();
    let closing = cache.clone();
    let mut shutdown = tokio::spawn(async move { closing.shutdown().await });
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut shutdown)
            .await
            .is_err()
    );
    *gate.released.lock().unwrap() = true;
    gate.release.notify_all();
    assert!(matches!(
        read.await.unwrap(),
        Err(Error::OperationCancelled {
            reason: FactoryCancellationReason::CacheShutdown
        })
    ));
    tokio::time::timeout(Duration::from_secs(2), shutdown)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        old.upgrade().is_none(),
        "completed drainage must include the retired source value"
    );
}
