#![cfg(feature = "redis")]
//! Native core acceptance: repair ownership, release and replaced-token rejection.
use amalgam::redis_backend::{RedisDistributedCache, RedisDistributedLocker};
use amalgam::*;
use async_trait::async_trait;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{Notify, Semaphore};
mod support {
    pub mod redis_fixture;
}

fn time(seconds: i64) -> Timestamp {
    Timestamp::from_ticks(seconds * 10_000_000)
}
fn kind() -> MarkerKind {
    MarkerKind::Tag(Tag::new("snapshot-group").unwrap())
}
fn token() -> FactoryCancellation {
    CancellationSource::new().token()
}
struct Gate {
    entered: Notify,
    released: Semaphore,
}
impl Gate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: Notify::new(),
            released: Semaphore::new(0),
        })
    }
    async fn park(&self) {
        self.entered.notify_one();
        self.released.acquire().await.unwrap().forget();
    }
    async fn entered(&self) {
        tokio::time::timeout(Duration::from_secs(5), self.entered.notified())
            .await
            .unwrap();
    }
    fn allow(&self) {
        self.released.add_permits(1);
    }
}
#[derive(Clone)]
enum Write {
    Pass,
    Park(Arc<Gate>),
}
struct Snapshots {
    inner: Arc<dyn MarkerSnapshotCache>,
    reads: AtomicUsize,
    proofs: Mutex<Vec<LeaseProof>>,
    write: Mutex<Write>,
}
impl Snapshots {
    fn last_proof(&self) -> LeaseProof {
        self.proofs.lock().unwrap().last().unwrap().clone()
    }
}
#[async_trait]
impl MarkerSnapshotCache for Snapshots {
    async fn read_snapshot(
        &self,
        scope: &CacheScope,
        marker: &MarkerKind,
        now: Timestamp,
        cancellation: FactoryCancellation,
    ) -> std::result::Result<MarkerSnapshotRead, MarkerSnapshotCacheError> {
        if *marker == kind() {
            self.reads.fetch_add(1, Ordering::SeqCst);
        }
        self.inner
            .read_snapshot(scope, marker, now, cancellation)
            .await
    }
    async fn renew_snapshot(
        &self,
        scope: &CacheScope,
        marker: &MarkerKind,
        snapshot: MarkerSnapshot,
        now: Timestamp,
        cancellation: FactoryCancellation,
    ) -> std::result::Result<MarkerSnapshotRenewal, MarkerSnapshotCacheError> {
        self.inner
            .renew_snapshot(scope, marker, snapshot, now, cancellation)
            .await
    }
    async fn renew_snapshot_with_lease(
        &self,
        scope: &CacheScope,
        marker: &MarkerKind,
        snapshot: MarkerSnapshot,
        now: Timestamp,
        proof: &LeaseProof,
        cancellation: FactoryCancellation,
    ) -> std::result::Result<MarkerSnapshotRenewal, MarkerSnapshotCacheError> {
        if *marker == kind() {
            self.proofs.lock().unwrap().push(proof.clone());
            let mode = self.write.lock().unwrap().clone();
            match mode {
                Write::Pass => {}
                Write::Park(gate) => gate.park().await,
            }
        }
        self.inner
            .renew_snapshot_with_lease(scope, marker, snapshot, now, proof, cancellation)
            .await
    }
}
struct Store {
    journal: Arc<dyn InvalidationStore>,
    snapshots: Arc<Snapshots>,
}
#[async_trait]
impl InvalidationStore for Store {
    fn snapshot_cache(&self) -> Option<Arc<dyn MarkerSnapshotCache>> {
        Some(self.snapshots.clone())
    }
    async fn read(
        &self,
        scope: &CacheScope,
        marker: &MarkerKind,
    ) -> std::result::Result<Option<MarkerVersion>, MarkerError> {
        self.journal.read(scope, marker).await
    }
    async fn advance(
        &self,
        scope: &CacheScope,
        marker: MarkerKind,
        candidate: MarkerVersion,
    ) -> std::result::Result<MarkerAdvanceOutcome, MarkerError> {
        self.journal.advance(scope, marker, candidate).await
    }
}
struct Fixture {
    cache: Cache<u64>,
    scope: CacheScope,
    store: Arc<Store>,
    locker: Arc<RedisDistributedLocker>,
}
async fn fixture(url: &str, test: &str) -> Fixture {
    let scope = CacheScope::new(
        format!("marker-core-{test}-{}:", SystemClock.now().ticks()),
        "v2",
        KeyModifierMode::Prefix,
    )
    .unwrap();
    let clock = Arc::new(ManualClock::new(time(10)));
    let backend = Arc::new(RedisDistributedCache::connect(url).await.unwrap());
    let writer = Cache::builder()
        .clock(clock.clone())
        .key_prefix(scope.prefix())
        .distributed(backend.clone())
        .serializer(Arc::new(JsonSerializer))
        .try_build()
        .unwrap();
    writer
        .try_set_full(
            "key",
            7_u64,
            Some(EntryOptions::new(Duration::from_secs(3600))),
            vec![Tag::new("snapshot-group").unwrap()].into_boxed_slice(),
        )
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    writer.shutdown().await.unwrap();
    let journal = backend.invalidation_store().unwrap();
    journal
        .advance(&scope, kind(), MarkerVersion::new(time(1)))
        .await
        .unwrap();
    let store = Arc::new(Store {
        snapshots: Arc::new(Snapshots {
            inner: journal.snapshot_cache().unwrap(),
            reads: AtomicUsize::new(0),
            proofs: Mutex::new(Vec::new()),
            write: Mutex::new(Write::Pass),
        }),
        journal,
    });
    let locker = Arc::new(RedisDistributedLocker::connect(url).await.unwrap());
    let cache = Cache::builder()
        .clock(clock)
        .key_prefix(scope.prefix())
        .distributed(backend)
        .invalidation_store(store.clone())
        .serializer(Arc::new(JsonSerializer))
        .tags_default_options(
            EntryOptions::tag_defaults()
                .with_memory_duration(Duration::from_secs(2))
                .with_distributed_duration(Duration::from_secs(5))
                .with_skip_distributed_locker(false),
        )
        .marker_read_policy(MarkerReadPolicy::OptionsControlled)
        .marker_lifecycle_policy(MarkerLifecyclePolicy::CachedSnapshots)
        .distributed_locker(locker.clone())
        .lease_policy(LeasePolicy::Fenced)
        .lease_ttl(Duration::from_secs(30))
        .reconciliation_policy(ReconciliationPolicy::Periodic(Duration::from_secs(3600)))
        .try_build()
        .unwrap();
    Fixture {
        cache,
        scope,
        store,
        locker,
    }
}
async fn snapshot(f: &Fixture) -> MarkerSnapshotRead {
    f.store
        .snapshots
        .inner
        .read_snapshot(&f.scope, &kind(), time(10), token())
        .await
        .unwrap()
}
async fn replacement(f: &Fixture, proof: &LeaseProof) -> DistributedLease {
    acquire_owned(
        f.locker.clone(),
        Arc::from(proof.key()),
        LeaseTtl::new(Duration::from_secs(30)).unwrap(),
        Timeout::After(Duration::from_millis(300)),
        AcquisitionPolicy::TokenOwned,
    )
    .await
    .unwrap()
    .expect("the actual native lease must be available")
}

#[tokio::test]
async fn native_core_rechecks_fences_repairs_and_releases_before_returning() {
    let Some(url) = support::redis_fixture::redis_url() else {
        return;
    };
    let f = fixture(&url, "normal").await;
    assert_eq!(f.cache.read("key", None).await.unwrap().value(), Some(&7));
    assert_eq!(f.store.snapshots.reads.load(Ordering::SeqCst), 2);
    assert_eq!(f.store.snapshots.proofs.lock().unwrap().len(), 1);
    assert_eq!(
        snapshot(&f).await.snapshot().unwrap().version(),
        MarkerVersion::new(time(1))
    );
    let next = replacement(&f, &f.store.snapshots.last_proof()).await;
    next.release().await.unwrap();
    assert_eq!(f.cache.read("key", None).await.unwrap().value(), Some(&7));
    assert_eq!(
        f.store.snapshots.reads.load(Ordering::SeqCst),
        2,
        "fresh L1 observation should be reused"
    );
    f.cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn native_core_replaced_token_cannot_repair_or_create_fresh_local_authority() {
    let Some(url) = support::redis_fixture::redis_url() else {
        return;
    };
    let f = fixture(&url, "replaced").await;
    let gate = Gate::new();
    *f.store.snapshots.write.lock().unwrap() = Write::Park(gate.clone());
    let cache = f.cache.clone();
    let call = tokio::spawn(async move { cache.read("key", None).await });
    gate.entered().await;
    let old = f.store.snapshots.last_proof();
    f.locker
        .release(old.key(), old.token().as_str())
        .await
        .unwrap();
    let next = replacement(&f, &old).await;
    let next_proof = next.proof().unwrap();
    gate.allow();
    assert!(matches!(
        call.await.unwrap(),
        Err(Error::Lease(LeaseError::Lost))
    ));
    assert_eq!(
        snapshot(&f).await,
        MarkerSnapshotRead::Missing {
            maximum: Some(MarkerVersion::new(time(1)))
        }
    );
    assert_eq!(
        f.locker
            .renew(
                next_proof.key(),
                next_proof.token(),
                LeaseTtl::new(Duration::from_secs(30)).unwrap()
            )
            .await
            .unwrap(),
        RenewalOutcome::Renewed,
        "old-token cleanup must preserve the real replacement lease"
    );
    next.release().await.unwrap();
    *f.store.snapshots.write.lock().unwrap() = Write::Pass;
    assert_eq!(f.cache.read("key", None).await.unwrap().value(), Some(&7));
    assert_eq!(
        f.store.snapshots.reads.load(Ordering::SeqCst),
        4,
        "a rejected native repair must not bypass a fresh remote retry through L1"
    );
    assert_eq!(
        snapshot(&f).await.snapshot().unwrap().version(),
        MarkerVersion::new(time(1))
    );
    assert_eq!(
        f.store.read(&f.scope, &kind()).await.unwrap(),
        Some(MarkerVersion::new(time(1)))
    );
    f.cache.shutdown().await.unwrap();
}
