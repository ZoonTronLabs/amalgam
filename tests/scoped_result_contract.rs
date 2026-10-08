//! Original receipt causes may reenter shutdown; retire scope admission first.
use amalgam::advanced::{CommitReceipt, MutationReceipt};
use amalgam::provider::{
    DistributedCache, InMemoryDistributedCache, InvalidationStore, JsonSerializer, SystemClock,
};
use amalgam::{Cache, EntryOptions, Error, RecoveryConfig, Result, source};
use async_trait::async_trait;
use std::convert::Infallible;
use std::fmt;
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;
use tokio::sync::{Notify, Semaphore};

#[derive(Debug, PartialEq, Eq)]
enum DrainOutcome {
    Complete,
    TimedOut,
    Failed,
}
struct Probe {
    cache: Mutex<Option<Cache<u64>>>,
    outcomes: Mutex<Vec<DrainOutcome>>,
    threads: Mutex<Vec<std::thread::JoinHandle<()>>>,
}
struct DrainCause(Arc<Probe>);
impl fmt::Debug for DrainCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("DrainCause")
    }
}
impl fmt::Display for DrainCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("retire original provider cause")
    }
}
impl std::error::Error for DrainCause {}
impl Drop for DrainCause {
    fn drop(&mut self) {
        let cache = self
            .0
            .cache
            .lock()
            .unwrap()
            .take()
            .expect("one original cause owns drainage");
        let (sent, completed) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let result = runtime.block_on(cache.shutdown()).map(|_| ());
            let _ = sent.send(result);
        });
        self.0.threads.lock().unwrap().push(thread);
        let outcome = match completed.recv_timeout(Duration::from_secs(2)) {
            Ok(Ok(())) => DrainOutcome::Complete,
            Ok(Err(_)) | Err(mpsc::RecvTimeoutError::Disconnected) => DrainOutcome::Failed,
            Err(mpsc::RecvTimeoutError::Timeout) => DrainOutcome::TimedOut,
        };
        self.0.outcomes.lock().unwrap().push(outcome);
    }
}
struct FailingWrite {
    inner: Arc<InMemoryDistributedCache>,
    probe: Arc<Probe>,
}
#[async_trait]
impl DistributedCache for FailingWrite {
    async fn get(&self, key: &str) -> Result<Option<amalgam::provider::DistributedBytes>> {
        self.inner.get(key).await
    }
    async fn set(&self, key: &str, value: Vec<u8>, ttl: Option<Duration>) -> Result<()> {
        if key.ends_with("scoped") {
            Err(Error::distributed(DrainCause(Arc::clone(&self.probe))))
        } else {
            self.inner.set(key, value, ttl).await
        }
    }
    async fn remove(&self, key: &str) -> Result<()> {
        self.inner.remove(key).await
    }
    fn invalidation_store(&self) -> Option<Arc<dyn InvalidationStore>> {
        self.inner.invalidation_store()
    }
}
fn fixture() -> (Cache<u64>, Arc<Probe>) {
    let probe = Arc::new(Probe {
        cache: Mutex::new(None),
        outcomes: Mutex::new(Vec::new()),
        threads: Mutex::new(Vec::new()),
    });
    let backend = Arc::new(FailingWrite {
        inner: Arc::new(InMemoryDistributedCache::new(Arc::new(SystemClock))),
        probe: Arc::clone(&probe),
    });
    let cache = Cache::builder()
        .distributed(backend)
        .serializer(Arc::new(JsonSerializer))
        .auto_recovery(RecoveryConfig {
            enabled: false,
            ..RecoveryConfig::default()
        })
        .default_options(
            EntryOptions::new(Duration::from_secs(60))
                .with_rethrow_distributed_exceptions(false)
                .with_allow_background_distributed_operations(false),
        )
        .try_build()
        .unwrap();
    *probe.cache.lock().unwrap() = Some(cache.clone());
    (cache, probe)
}
fn assert_drained(probe: &Probe) {
    for thread in std::mem::take(&mut *probe.threads.lock().unwrap()) {
        thread.join().unwrap();
    }
    assert_eq!(
        *probe.outcomes.lock().unwrap(),
        vec![DrainOutcome::Complete]
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn value_only_owned_completion_retires_original_causes_after_scope_admission() {
    let (cache, probe) = fixture();
    assert_eq!(
        cache
            .get_or_set(
                "scoped",
                source::factory(|_| async { Ok::<_, Infallible>(17) })
            )
            .await
            .unwrap(),
        17
    );
    assert_drained(&probe);
    assert!(matches!(
        cache.try_get("scoped").await,
        Err(Error::CacheClosed)
    ));
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn requested_receipt_retains_original_causes_until_the_caller_retires_it() {
    let (cache, probe) = fixture();
    let result = cache
        .get_or_set(
            "scoped",
            source::factory(|_| async { Ok::<_, Infallible>(17) }),
        )
        .with_receipt()
        .await
        .unwrap();
    assert_eq!(result.value, 17);
    assert!(probe.outcomes.lock().unwrap().is_empty());
    match result.commit {
        CommitReceipt::Mutation(MutationReceipt::Completed(report)) => drop(report),
        CommitReceipt::Mutation(MutationReceipt::Scheduled(_)) | CommitReceipt::Unchanged => {
            panic!("foreground mutation must retain its actual completed receipt")
        }
    }
    assert_drained(&probe);
    assert!(matches!(
        cache.try_get("scoped").await,
        Err(Error::CacheClosed)
    ));
}

struct PendingWrite {
    inner: Arc<InMemoryDistributedCache>,
    started: Arc<Notify>,
    release: Arc<Semaphore>,
}
#[async_trait]
impl DistributedCache for PendingWrite {
    async fn get(&self, key: &str) -> Result<Option<amalgam::provider::DistributedBytes>> {
        self.inner.get(key).await
    }
    async fn set(&self, key: &str, value: Vec<u8>, ttl: Option<Duration>) -> Result<()> {
        if key.ends_with("pending") {
            self.started.notify_one();
            let _permit = self.release.acquire().await.unwrap();
            self.inner.set(key, value, ttl).await
        } else {
            self.inner.set(key, value, ttl).await
        }
    }
    async fn remove(&self, key: &str) -> Result<()> {
        self.inner.remove(key).await
    }
    fn invalidation_store(&self) -> Option<Arc<dyn InvalidationStore>> {
        self.inner.invalidation_store()
    }
}
struct ReleaseWrite(Arc<Semaphore>);
impl Drop for ReleaseWrite {
    fn drop(&mut self) {
        self.0.add_permits(8);
    }
}
#[tokio::test]
async fn value_only_completion_retains_a_genuinely_pending_scheduled_l2_write() {
    let inner = Arc::new(InMemoryDistributedCache::new(Arc::new(SystemClock)));
    let started = Arc::new(Notify::new());
    let release = ReleaseWrite(Arc::new(Semaphore::new(0)));
    let cache = Cache::<u64>::builder()
        .distributed(Arc::new(PendingWrite {
            inner: Arc::clone(&inner),
            started: Arc::clone(&started),
            release: Arc::clone(&release.0),
        }))
        .serializer(Arc::new(JsonSerializer))
        .default_options(
            EntryOptions::new(Duration::from_secs(60))
                .with_allow_background_distributed_operations(true),
        )
        .try_build()
        .unwrap();
    assert_eq!(
        cache
            .get_or_set(
                "pending",
                source::factory(|_| async {
                    tokio::task::yield_now().await;
                    Ok::<_, Infallible>(61)
                })
            )
            .await
            .unwrap(),
        61
    );
    tokio::time::timeout(Duration::from_secs(2), started.notified())
        .await
        .expect("the background L2 write must start");
    assert!(inner.get("pending").await.unwrap().is_none());
    assert_eq!(cache.try_get("pending").await.unwrap(), Some(61));
    release.0.add_permits(1);
    cache.flush_pending().await.unwrap();
    let peer = Cache::<u64>::builder()
        .distributed(inner)
        .serializer(Arc::new(JsonSerializer))
        .try_build()
        .unwrap();
    assert_eq!(peer.try_get("pending").await.unwrap(), Some(61));
    cache.shutdown().await.unwrap();
    peer.shutdown().await.unwrap();
}
