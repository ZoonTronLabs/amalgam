#![cfg(target_arch = "x86_64")]
use amalgam::{Cache, Error, FactoryCancellationReason};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

fn ready<T: std::future::IntoFuture>(request: T) -> T::Output {
    let mut future = std::pin::pin!(request.into_future());
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    match std::future::Future::poll(future.as_mut(), &mut context) {
        std::task::Poll::Ready(value) => value,
        std::task::Poll::Pending => panic!("a ready local operation must finish without a runtime"),
    }
}

struct Value {
    block: Arc<AtomicBool>,
    entered: mpsc::Sender<()>,
    release: Arc<Mutex<mpsc::Receiver<()>>>,
}
impl Clone for Value {
    fn clone(&self) -> Self {
        if self.block.swap(false, Ordering::SeqCst) {
            self.entered.send(()).unwrap();
            self.release.lock().unwrap().recv().unwrap();
        }
        Self {
            block: self.block.clone(),
            entered: self.entered.clone(),
            release: self.release.clone(),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_slot_publication_drains_a_started_clone_and_rejects_its_result() {
    let cache = Cache::new();
    let block = Arc::new(AtomicBool::new(false));
    let (entered, entry) = mpsc::channel();
    let (release, released) = mpsc::channel();
    cache
        .set(
            "hit",
            Value {
                block: block.clone(),
                entered,
                release: Arc::new(Mutex::new(released)),
            },
        )
        .await
        .unwrap();
    block.store(true, Ordering::SeqCst);
    let reader_cache = cache.clone();
    let reader = std::thread::spawn(move || ready(reader_cache.read("hit", None)));
    entry.recv_timeout(Duration::from_secs(2)).unwrap();
    cache.close();
    let mut shutdown = Box::pin(cache.shutdown());
    assert!(
        tokio::time::timeout(Duration::from_millis(25), &mut shutdown)
            .await
            .is_err()
    );
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
}

#[test]
fn a_closed_plain_read_does_not_invoke_value_clone() {
    let cache = Cache::new();
    let block = Arc::new(AtomicBool::new(false));
    let (entered, entry) = mpsc::channel();
    let (_, released) = mpsc::channel();
    ready(cache.set(
        "hit",
        Value {
            block: block.clone(),
            entered,
            release: Arc::new(Mutex::new(released)),
        },
    ))
    .unwrap();
    block.store(true, Ordering::SeqCst);
    cache.close();
    assert!(matches!(
        ready(cache.read("hit", None)),
        Err(Error::CacheClosed)
    ));
    assert!(entry.try_recv().is_err());
}
