//! Public ownership, recheck and cancellation contracts for marker refresh.
use amalgam::*;
use async_trait::async_trait;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{Notify, Semaphore};

fn time(seconds: i64) -> Timestamp {
    Timestamp::from_ticks(seconds * 10_000_000)
}
fn version(seconds: i64) -> MarkerVersion {
    MarkerVersion::new(time(seconds))
}
fn group() -> MarkerKind {
    MarkerKind::Tag(Tag::new("group").unwrap())
}
fn scope(prefix: &str) -> CacheScope {
    CacheScope::new(prefix, "v2", KeyModifierMode::Prefix).unwrap()
}
fn new_token() -> FactoryCancellation {
    CancellationSource::new().token()
}
fn tag_lock(key: &str) -> bool {
    key.ends_with("/7461673a67726f7570")
}
fn options() -> EntryOptions {
    EntryOptions::tag_defaults()
        .with_memory_duration(Duration::from_secs(2))
        .with_distributed_duration(Duration::from_secs(5))
        .with_fail_safe(true, Some(Duration::from_secs(20)), None)
        .with_skip_distributed_locker(false)
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
        tokio::time::timeout(Duration::from_secs(2), self.entered.notified())
            .await
            .unwrap();
    }
    fn allow(&self) {
        self.released.add_permits(1);
    }
}
#[derive(Clone)]
enum Read {
    Pass,
    Backend,
    Protocol,
    Park(Arc<Gate>),
}
#[derive(Debug, thiserror::Error)]
#[error("original marker snapshot read fault")]
struct ReadCause;
#[derive(Clone)]
enum Write {
    Pass,
    Park(Arc<Gate>),
    Unsupported,
}
struct Snapshots {
    inner: InMemoryInvalidationStore,
    reads: Mutex<Vec<MarkerKind>>,
    writes: Mutex<Vec<(MarkerKind, bool)>>,
    tokens: Mutex<Vec<FactoryCancellation>>,
    mode: Mutex<Write>,
    read_mode: Mutex<Read>,
    read_tokens: Mutex<Vec<FactoryCancellation>>,
}
impl Snapshots {
    fn reads(&self) -> usize {
        self.reads
            .lock()
            .unwrap()
            .iter()
            .filter(|k| **k == group())
            .count()
    }
    fn writes(&self) -> usize {
        self.writes
            .lock()
            .unwrap()
            .iter()
            .filter(|(k, _)| *k == group())
            .count()
    }
    async fn before_write(
        &self,
        kind: &MarkerKind,
        fenced: bool,
        cancellation: FactoryCancellation,
    ) -> std::result::Result<(), MarkerSnapshotCacheError> {
        self.writes.lock().unwrap().push((kind.clone(), fenced));
        if *kind == group() {
            self.tokens.lock().unwrap().push(cancellation);
            let mode = self.mode.lock().unwrap().clone();
            match mode {
                Write::Pass => {}
                Write::Park(gate) => gate.park().await,
                Write::Unsupported => return Err(LeaseError::UnsupportedFencing.into()),
            }
        }
        Ok(())
    }
}
#[async_trait]
impl MarkerSnapshotCache for Snapshots {
    async fn read_snapshot(
        &self,
        scope: &CacheScope,
        kind: &MarkerKind,
        now: Timestamp,
        cancellation: FactoryCancellation,
    ) -> std::result::Result<MarkerSnapshotRead, MarkerSnapshotCacheError> {
        self.reads.lock().unwrap().push(kind.clone());
        if *kind == group() {
            self.read_tokens.lock().unwrap().push(cancellation.clone());
            let mode = self.read_mode.lock().unwrap().clone();
            match mode {
                Read::Pass => {}
                Read::Backend => return Err(MarkerError::backend(ReadCause).into()),
                Read::Protocol => return Err(MarkerError::protocol(ReadCause).into()),
                Read::Park(gate) => gate.park().await,
            }
        }
        self.inner
            .read_snapshot(scope, kind, now, cancellation)
            .await
    }
    async fn renew_snapshot(
        &self,
        scope: &CacheScope,
        kind: &MarkerKind,
        snapshot: MarkerSnapshot,
        now: Timestamp,
        cancellation: FactoryCancellation,
    ) -> std::result::Result<MarkerSnapshotRenewal, MarkerSnapshotCacheError> {
        self.before_write(kind, false, cancellation.clone()).await?;
        self.inner
            .renew_snapshot(scope, kind, snapshot, now, cancellation)
            .await
    }
    async fn renew_snapshot_with_lease(
        &self,
        scope: &CacheScope,
        kind: &MarkerKind,
        snapshot: MarkerSnapshot,
        now: Timestamp,
        proof: &LeaseProof,
        cancellation: FactoryCancellation,
    ) -> std::result::Result<MarkerSnapshotRenewal, MarkerSnapshotCacheError> {
        self.before_write(kind, true, cancellation.clone()).await?;
        self.inner
            .renew_snapshot_with_lease(scope, kind, snapshot, now, proof, cancellation)
            .await
    }
}
struct Store {
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
        kind: &MarkerKind,
    ) -> std::result::Result<Option<MarkerVersion>, MarkerError> {
        self.snapshots.inner.read(scope, kind).await
    }
    async fn advance(
        &self,
        scope: &CacheScope,
        kind: MarkerKind,
        candidate: MarkerVersion,
    ) -> std::result::Result<MarkerAdvanceOutcome, MarkerError> {
        self.snapshots.inner.advance(scope, kind, candidate).await
    }
}
#[derive(Clone)]
enum Acquire {
    Pass,
    Contended,
    Fault,
    Park(Arc<Gate>),
    PeerRefresh(MarkerSnapshot),
}
#[derive(Clone)]
enum Release {
    Pass,
    Fault,
}
#[derive(Debug, thiserror::Error)]
#[error("original marker locker fault")]
struct Cause;
struct Locker {
    inner: Arc<InMemoryDistributedLocker>,
    store: Arc<Store>,
    clock: Arc<ManualClock>,
    scope: CacheScope,
    acquisition: Mutex<Acquire>,
    release: Mutex<Release>,
    acquired: Mutex<Vec<(String, LeaseToken, Timeout)>>,
    released: Mutex<Vec<String>>,
    release_finished: Notify,
}
impl Locker {
    fn acquisitions(&self) -> usize {
        self.acquired
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _, _)| tag_lock(key))
            .count()
    }
    fn releases(&self) -> usize {
        self.released
            .lock()
            .unwrap()
            .iter()
            .filter(|key| tag_lock(key))
            .count()
    }
    fn last_tag(&self) -> (String, LeaseToken) {
        let held = self.acquired.lock().unwrap();
        let (key, token, _) = held.iter().rev().find(|(key, _, _)| tag_lock(key)).unwrap();
        (key.clone(), token.clone())
    }
}
#[async_trait]
impl DistributedLocker for Locker {
    async fn acquire(&self, _: &str, _: Duration, _: Timeout) -> Result<Option<String>> {
        panic!("native receipt must be used")
    }
    async fn release(&self, key: &str, token: &str) -> Result<()> {
        self.released.lock().unwrap().push(key.to_owned());
        self.inner.release(key, token).await?;
        let mode = self.release.lock().unwrap().clone();
        if tag_lock(key) {
            self.release_finished.notify_one();
            if matches!(mode, Release::Fault) {
                return Err(Error::distributed(Cause));
            }
        }
        Ok(())
    }
    fn lease_support(&self) -> LeaseSupport {
        LeaseSupport::FixedTtl
    }
    fn token_acquisition(&self) -> TokenAcquisition {
        TokenAcquisition::CallerSelected
    }
    fn ownership_check(&self, key: &str, token: &LeaseToken) -> OwnershipCheck {
        self.inner.ownership_check(key, token)
    }
    fn lease_proof(&self, key: &str, token: &LeaseToken) -> LeaseProof {
        self.inner.lease_proof(key, token)
    }
    async fn acquire_receipt(
        &self,
        key: &str,
        token: &LeaseToken,
        ttl: LeaseTtl,
        timeout: Timeout,
    ) -> std::result::Result<Option<LeaseReceipt>, LeaseError> {
        self.acquired
            .lock()
            .unwrap()
            .push((key.to_owned(), token.clone(), timeout));
        let mode = if tag_lock(key) {
            self.acquisition.lock().unwrap().clone()
        } else {
            Acquire::Pass
        };
        match &mode {
            Acquire::Contended => return Ok(None),
            Acquire::Fault => return Err(LeaseError::backend(Cause)),
            Acquire::Park(gate) => gate.park().await,
            Acquire::Pass | Acquire::PeerRefresh(_) => {}
        }
        let receipt = self.inner.acquire_receipt(key, token, ttl, timeout).await?;
        if let Acquire::PeerRefresh(snapshot) = mode {
            self.store
                .snapshots
                .inner
                .renew_snapshot(
                    &self.scope,
                    &group(),
                    snapshot,
                    self.clock.now(),
                    new_token(),
                )
                .await
                .unwrap();
        }
        Ok(receipt)
    }
}
struct Fixture {
    clock: Arc<ManualClock>,
    store: Arc<Store>,
    locker: Arc<Locker>,
    cache: Cache<u64>,
}
async fn fixture(options: EntryOptions, policy: LeasePolicy, prefix: &str) -> Fixture {
    let clock = Arc::new(ManualClock::new(time(10)));
    let backend = Arc::new(InMemoryDistributedCache::new(clock.clone()));
    let writer = Cache::builder()
        .clock(clock.clone())
        .key_prefix(prefix)
        .distributed(backend.clone())
        .serializer(Arc::new(JsonSerializer))
        .try_build()
        .unwrap();
    writer
        .try_set_full(
            "key",
            7_u64,
            Some(EntryOptions::new(Duration::from_secs(3600))),
            vec![Tag::new("group").unwrap()].into_boxed_slice(),
        )
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    writer.shutdown().await.unwrap();
    let store = Arc::new(Store {
        snapshots: Arc::new(Snapshots {
            inner: InMemoryInvalidationStore::default(),
            reads: Mutex::new(Vec::new()),
            writes: Mutex::new(Vec::new()),
            tokens: Mutex::new(Vec::new()),
            mode: Mutex::new(Write::Pass),
            read_mode: Mutex::new(Read::Pass),
            read_tokens: Mutex::new(Vec::new()),
        }),
    });
    store
        .advance(&scope(prefix), group(), version(1))
        .await
        .unwrap();
    let locker = Arc::new(Locker {
        inner: Arc::new(InMemoryDistributedLocker::new(clock.clone())),
        store: store.clone(),
        clock: clock.clone(),
        scope: scope(prefix),
        acquisition: Mutex::new(Acquire::Pass),
        release: Mutex::new(Release::Pass),
        acquired: Mutex::new(Vec::new()),
        released: Mutex::new(Vec::new()),
        release_finished: Notify::new(),
    });
    let cache = Cache::builder()
        .clock(clock.clone())
        .key_prefix(prefix)
        .distributed(backend)
        .invalidation_store(store.clone())
        .serializer(Arc::new(JsonSerializer))
        .tags_default_options(options)
        .marker_read_policy(MarkerReadPolicy::OptionsControlled)
        .marker_lifecycle_policy(MarkerLifecyclePolicy::CachedSnapshots)
        .distributed_locker(locker.clone())
        .lease_policy(policy)
        .lease_ttl(Duration::from_secs(1))
        .reconciliation_policy(ReconciliationPolicy::Periodic(Duration::from_secs(3600)))
        .try_build()
        .unwrap();
    Fixture {
        clock,
        store,
        locker,
        cache,
    }
}
async fn found(cache: &Cache<u64>) {
    assert_eq!(cache.read("key", None).await.unwrap().value(), Some(&7));
}

#[tokio::test]
async fn acquired_refresh_rechecks_and_fences_before_local_admission_and_release() {
    let f = fixture(options(), LeasePolicy::Fenced, "").await;
    found(&f.cache).await;
    assert_eq!(f.store.snapshots.reads(), 2);
    assert_eq!(f.store.snapshots.writes(), 1);
    assert!(
        f.store
            .snapshots
            .writes
            .lock()
            .unwrap()
            .iter()
            .any(|(kind, fenced)| *kind == group() && *fenced)
    );
    assert_eq!((f.locker.acquisitions(), f.locker.releases()), (1, 1));
    let (key, token) = f.locker.last_tag();
    assert_eq!(
        f.locker.inner.ownership_check(&key, &token),
        OwnershipCheck::Lost
    );
    found(&f.cache).await;
    assert_eq!(f.store.snapshots.reads(), 2);
    f.cache.shutdown().await.unwrap();
}
#[tokio::test]
async fn peer_fresh_recheck_avoids_duplicate_renewal_and_preserves_age() {
    let f = fixture(options(), LeasePolicy::Fenced, "").await;
    let peer = MarkerSnapshot::new(version(1), time(9), time(11), time(15)).unwrap();
    *f.locker.acquisition.lock().unwrap() = Acquire::PeerRefresh(peer);
    found(&f.cache).await;
    assert_eq!(f.store.snapshots.reads(), 2);
    assert_eq!(f.store.snapshots.writes(), 0);
    assert_eq!(f.locker.releases(), 1);
    assert_eq!(
        f.store
            .snapshots
            .inner
            .read_snapshot(&scope(""), &group(), time(10), new_token())
            .await
            .unwrap()
            .snapshot(),
        Some(peer)
    );
    f.clock.advance(Duration::from_secs(1));
    *f.locker.acquisition.lock().unwrap() = Acquire::Pass;
    found(&f.cache).await;
    assert_eq!(
        f.store.snapshots.reads(),
        4,
        "hydration must not reset the peer logical age"
    );
    assert_eq!(f.store.snapshots.writes(), 1);
    f.cache.shutdown().await.unwrap();
}
#[tokio::test]
async fn skip_locker_uses_one_read_and_unfenced_renewal() {
    let f = fixture(
        options().with_skip_distributed_locker(true),
        LeasePolicy::Fenced,
        "",
    )
    .await;
    found(&f.cache).await;
    assert_eq!(f.store.snapshots.reads(), 1);
    assert_eq!(f.store.snapshots.writes(), 1);
    assert_eq!((f.locker.acquisitions(), f.locker.releases()), (0, 0));
    assert!(
        f.store
            .snapshots
            .writes
            .lock()
            .unwrap()
            .iter()
            .all(|(_, fenced)| !fenced)
    );
    f.cache.shutdown().await.unwrap();
}
#[tokio::test]
async fn contention_is_explicit_strict_rejection_or_deliberate_cooperative_factory() {
    for policy in [LeasePolicy::Fenced, LeasePolicy::CooperativeLegacy] {
        let f = fixture(options(), policy, "").await;
        *f.locker.acquisition.lock().unwrap() = Acquire::Contended;
        let read = f.cache.read("key", None).await;
        match policy {
            LeasePolicy::Fenced => {
                assert!(matches!(
                    read,
                    Err(Error::Lease(LeaseError::AcquisitionTimeout))
                ));
                assert_eq!(f.store.snapshots.writes(), 0);
            }
            LeasePolicy::CooperativeLegacy => {
                assert_eq!(read.unwrap().value(), Some(&7));
                assert_eq!(f.store.snapshots.writes(), 1);
            }
        }
        assert_eq!(f.store.snapshots.reads(), 1);
        assert_eq!(f.locker.releases(), 0);
        f.cache.shutdown().await.unwrap();
    }
}
#[tokio::test]
async fn acquisition_original_fault_policy_is_independent_from_value_defaults() {
    for (policy, rethrow, fails) in [
        (LeasePolicy::CooperativeLegacy, false, false),
        (LeasePolicy::CooperativeLegacy, true, true),
        (LeasePolicy::Fenced, false, true),
    ] {
        let f = fixture(
            options().with_rethrow_distributed_locker_exceptions(rethrow),
            policy,
            "",
        )
        .await;
        *f.locker.acquisition.lock().unwrap() = Acquire::Fault;
        let read = f.cache.read("key", None).await;
        if fails {
            assert!(
                matches!(read, Err(Error::Lease(LeaseError::Backend { source })) if source.downcast_ref::<Cause>().is_some())
            );
            assert_eq!(f.store.snapshots.writes(), 0);
        } else {
            assert_eq!(read.unwrap().value(), Some(&7));
            assert_eq!(f.store.snapshots.writes(), 1);
        }
        f.cache.shutdown().await.unwrap();
        assert_eq!(
            f.locker.releases(),
            1,
            "an uncertain caller-selected token is cleaned"
        );
    }
}
#[tokio::test]
async fn explicit_release_honors_tag_fault_flag_and_preserves_cause() {
    for rethrow in [false, true] {
        let f = fixture(
            options().with_rethrow_distributed_locker_exceptions(rethrow),
            LeasePolicy::CooperativeLegacy,
            "",
        )
        .await;
        *f.locker.release.lock().unwrap() = Release::Fault;
        let read = f.cache.read("key", None).await;
        if rethrow {
            assert!(
                matches!(read, Err(Error::Lease(LeaseError::Backend { source })) if source.to_string().contains("original marker locker fault"))
            );
        } else {
            assert_eq!(read.unwrap().value(), Some(&7));
        }
        assert_eq!(f.store.snapshots.writes(), 1);
        assert_eq!(f.locker.releases(), 1);
        f.cache.shutdown().await.unwrap();
    }
}
#[tokio::test]
async fn locker_deadline_does_not_use_value_or_distributed_read_deadlines() {
    let selected =
        options().with_distributed_lock_timeout(Timeout::After(Duration::from_millis(20)));
    let f = fixture(selected, LeasePolicy::Fenced, "").await;
    let gate = Gate::new();
    *f.locker.acquisition.lock().unwrap() = Acquire::Park(gate);
    let read = tokio::time::timeout(Duration::from_secs(2), f.cache.read("key", None))
        .await
        .unwrap();
    assert!(matches!(
        read,
        Err(Error::Lease(LeaseError::AcquisitionTimeout))
    ));
    assert_eq!(f.store.snapshots.reads(), 1);
    assert_eq!(f.store.snapshots.writes(), 0);
    {
        let held = f.locker.acquired.lock().unwrap();
        assert_eq!(
            held.iter().find(|(key, _, _)| tag_lock(key)).unwrap().2,
            Timeout::After(Duration::from_millis(20))
        );
    }
    f.cache.shutdown().await.unwrap();
    assert_eq!(f.locker.releases(), 1);
}
#[tokio::test]
async fn cancelling_parked_acquisition_cleans_known_token_and_does_not_degrade() {
    let f = fixture(options(), LeasePolicy::CooperativeLegacy, "").await;
    let gate = Gate::new();
    *f.locker.acquisition.lock().unwrap() = Acquire::Park(gate.clone());
    let source = CancellationSource::new();
    let call = tokio::spawn({
        let cache = f.cache.clone();
        let token = source.token();
        async move { cache.read_cancellable("key", None, token).await }
    });
    gate.entered().await;
    source.cancel();
    assert!(matches!(
        call.await.unwrap(),
        Err(Error::OperationCancelled {
            reason: FactoryCancellationReason::CallerCancelled
        })
    ));
    assert_eq!(f.store.snapshots.writes(), 0);
    f.cache.shutdown().await.unwrap();
    assert_eq!(f.locker.releases(), 1);
}
#[tokio::test]
async fn lost_atomic_ownership_cannot_leave_a_fresh_l1_observation() {
    let f = fixture(options(), LeasePolicy::Fenced, "").await;
    let gate = Gate::new();
    *f.store.snapshots.mode.lock().unwrap() = Write::Park(gate.clone());
    let call = tokio::spawn({
        let cache = f.cache.clone();
        async move { cache.read("key", None).await }
    });
    gate.entered().await;
    f.clock.advance(Duration::from_secs(1));
    gate.allow();
    assert!(matches!(
        call.await.unwrap(),
        Err(Error::Lease(LeaseError::Lost))
    ));
    assert_eq!(f.store.snapshots.inner.snapshot_count(), 0);
    *f.store.snapshots.mode.lock().unwrap() = Write::Pass;
    found(&f.cache).await;
    assert_eq!(
        f.store.snapshots.reads(),
        4,
        "failed fencing cannot be bypassed by a fresh local marker"
    );
    assert_eq!((f.locker.acquisitions(), f.locker.releases()), (2, 2));
    f.cache.shutdown().await.unwrap();
}
#[tokio::test]
async fn unsupported_atomic_provider_does_not_create_local_refresh_authority() {
    let f = fixture(options(), LeasePolicy::Fenced, "").await;
    *f.store.snapshots.mode.lock().unwrap() = Write::Unsupported;
    assert!(matches!(
        f.cache.read("key", None).await,
        Err(Error::Lease(LeaseError::UnsupportedFencing))
    ));
    assert_eq!(f.store.snapshots.inner.snapshot_count(), 0);
    *f.store.snapshots.mode.lock().unwrap() = Write::Pass;
    found(&f.cache).await;
    assert_eq!(f.store.snapshots.reads(), 4);
    assert_eq!(f.locker.releases(), 2);
    f.cache.shutdown().await.unwrap();
}
#[tokio::test]
async fn background_refresh_keeps_lease_after_caller_completion_until_write_and_release() {
    let f = fixture(
        options().with_allow_background_distributed_operations(true),
        LeasePolicy::Fenced,
        "",
    )
    .await;
    let gate = Gate::new();
    *f.store.snapshots.mode.lock().unwrap() = Write::Park(gate.clone());
    found(&f.cache).await;
    gate.entered().await;
    let (key, token) = f.locker.last_tag();
    assert_eq!(
        f.locker.inner.ownership_check(&key, &token),
        OwnershipCheck::Held
    );
    assert_eq!(f.locker.releases(), 0);
    gate.allow();
    tokio::time::timeout(Duration::from_secs(2), f.locker.release_finished.notified())
        .await
        .unwrap();
    assert_eq!(f.locker.releases(), 1);
    assert_eq!(f.store.snapshots.inner.snapshot_count(), 1);
    found(&f.cache).await;
    assert_eq!(f.store.snapshots.reads(), 2);
    f.cache.shutdown().await.unwrap();
}
#[tokio::test]
async fn shutdown_cancels_parked_background_refresh_and_drains_exactly_one_release() {
    let f = fixture(
        options().with_allow_background_distributed_operations(true),
        LeasePolicy::Fenced,
        "",
    )
    .await;
    let gate = Gate::new();
    *f.store.snapshots.mode.lock().unwrap() = Write::Park(gate.clone());
    found(&f.cache).await;
    gate.entered().await;
    f.cache.shutdown().await.unwrap();
    assert_eq!(f.locker.releases(), 1);
    assert_eq!(f.store.snapshots.inner.snapshot_count(), 0);
    assert!(matches!(
        f.store
            .snapshots
            .tokens
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .check(),
        Err(Error::OperationCancelled {
            reason: FactoryCancellationReason::CacheShutdown
        })
    ));
}
#[tokio::test]
async fn marker_lock_identities_separate_scopes_and_ordinary_value_flights() {
    let first = fixture(options(), LeasePolicy::Fenced, "tenant/one:").await;
    let second = fixture(options(), LeasePolicy::Fenced, "tenant/one:/two:").await;
    found(&first.cache).await;
    found(&second.cache).await;
    assert_ne!(first.locker.last_tag().0, second.locker.last_tag().0);
    assert_eq!(
        first
            .cache
            .get_or_set("new-value", |ctx| async move { Ok(ctx.value(11)) })
            .await
            .unwrap(),
        11
    );
    {
        let acquired = first.locker.acquired.lock().unwrap();
        let value_key = &acquired
            .iter()
            .find(|(key, _, _)| !key.contains("marker-locks/"))
            .unwrap()
            .0;
        for (key, _, _) in acquired
            .iter()
            .filter(|(key, _, _)| key.contains("marker-locks/"))
        {
            assert_ne!(key, value_key);
        }
    }
    first.cache.shutdown().await.unwrap();
    second.cache.shutdown().await.unwrap();
}

fn eager_options() -> EntryOptions {
    options()
        .with_memory_duration(Duration::from_secs(10))
        .with_distributed_duration(Duration::from_secs(10))
        .with_eager_refresh(EagerThreshold::new(0.5))
}
async fn until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
async fn seed_known_without_remote_read(f: &Fixture) {
    f.clock.set(time(1));
    f.cache
        .try_remove_by_tag(Tag::new("group").unwrap())
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    f.clock.set(time(10));
}
#[tokio::test]
async fn skipped_read_still_runs_known_factory_and_independent_locker_or_write() {
    for skip_locker in [false, true] {
        let f = fixture(
            options()
                .with_skip_distributed(true, false)
                .with_skip_distributed_locker(skip_locker),
            LeasePolicy::Fenced,
            "",
        )
        .await;
        seed_known_without_remote_read(&f).await;
        found(&f.cache).await;
        assert_eq!(
            f.store.snapshots.reads(),
            0,
            "read remains skipped, including leased recheck"
        );
        assert_eq!(
            f.store.snapshots.writes(),
            1,
            "known revision renews despite skipped read"
        );
        assert_eq!(f.locker.acquisitions(), usize::from(!skip_locker));
        assert_eq!(f.locker.releases(), usize::from(!skip_locker));
        let snapshot = f
            .store
            .snapshots
            .inner
            .read_snapshot(&scope(""), &group(), time(10), new_token())
            .await
            .unwrap()
            .snapshot()
            .unwrap();
        assert_eq!(
            (snapshot.version(), snapshot.created()),
            (version(1), time(10))
        );
        assert_eq!(
            f.store.read(&scope(""), &group()).await.unwrap(),
            Some(version(1))
        );
        f.cache.shutdown().await.unwrap();
    }
    let f = fixture(
        options().with_skip_distributed(true, true),
        LeasePolicy::Fenced,
        "",
    )
    .await;
    seed_known_without_remote_read(&f).await;
    found(&f.cache).await;
    assert_eq!(
        (
            f.store.snapshots.reads(),
            f.store.snapshots.writes(),
            f.locker.acquisitions()
        ),
        (0, 0, 0)
    );
    f.cache.shutdown().await.unwrap();
}
#[tokio::test]
async fn a_cold_skipped_or_failed_read_never_becomes_fresh_negative_authority() {
    for skipped in [true, false] {
        let f = fixture(
            options()
                .with_skip_distributed(skipped, false)
                .with_skip_distributed_locker(true),
            LeasePolicy::Fenced,
            "",
        )
        .await;
        if !skipped {
            *f.store.snapshots.read_mode.lock().unwrap() = Read::Backend;
        }
        let mut events = f.cache.events().subscribe();
        found(&f.cache).await;
        found(&f.cache).await;
        assert_eq!(f.store.snapshots.writes(), 0);
        assert_eq!(
            f.store.snapshots.reads(),
            if skipped { 0 } else { 2 },
            "failed absence must be retried"
        );
        let mut outcomes = Vec::new();
        while let Ok(event) = events.try_recv() {
            if let CacheEvent::MarkerRead { kind, outcome } = event
                && kind == group()
            {
                outcomes.push(outcome);
            }
        }
        let expected = if skipped {
            MarkerReadOutcome::Skipped
        } else {
            MarkerReadOutcome::Unavailable(MarkerReadFailure::Backend)
        };
        assert_eq!(outcomes, vec![expected, expected]);
        f.cache.shutdown().await.unwrap();
    }
}
#[tokio::test]
async fn suppressed_read_fault_renews_known_fact_even_without_fail_safe() {
    for fail_safe in [true, false] {
        for protocol in [false, true] {
            let f = fixture(
                options().with_fail_safe(fail_safe, Some(Duration::from_secs(20)), None),
                LeasePolicy::Fenced,
                "",
            )
            .await;
            found(&f.cache).await;
            f.clock.set(time(12));
            *f.store.snapshots.read_mode.lock().unwrap() = if protocol {
                Read::Protocol
            } else {
                Read::Backend
            };
            found(&f.cache).await;
            assert_eq!(
                (f.store.snapshots.reads(), f.store.snapshots.writes()),
                (4, 2)
            );
            assert_eq!((f.locker.acquisitions(), f.locker.releases()), (2, 2));
            let snapshot = f
                .store
                .snapshots
                .inner
                .read_snapshot(&scope(""), &group(), time(12), new_token())
                .await
                .unwrap()
                .snapshot()
                .unwrap();
            assert_eq!(
                (snapshot.version(), snapshot.created()),
                (version(1), time(12))
            );
            found(&f.cache).await;
            assert_eq!(f.store.snapshots.reads(), 4);
            f.cache.shutdown().await.unwrap();
        }
    }
}
#[tokio::test]
async fn propagated_marker_read_fault_stops_before_locker_and_preserves_cause() {
    for protocol in [false, true] {
        let f = fixture(
            options()
                .with_rethrow_distributed_exceptions(!protocol)
                .with_rethrow_serialization_exceptions(protocol),
            LeasePolicy::Fenced,
            "",
        )
        .await;
        *f.store.snapshots.read_mode.lock().unwrap() = if protocol {
            Read::Protocol
        } else {
            Read::Backend
        };
        let error = f.cache.read("key", None).await.unwrap_err();
        let Error::Marker(error) = error else {
            panic!("expected original marker error")
        };
        assert!(std::error::Error::source(&error).unwrap().is::<ReadCause>());
        assert_eq!(
            (f.locker.acquisitions(), f.store.snapshots.writes()),
            (0, 0)
        );
        f.cache.shutdown().await.unwrap();
    }
}
#[tokio::test]
async fn excluded_foreground_marker_factory_awaits_release_before_error() {
    let f = fixture(
        options()
            .with_fail_safe(false, None, None)
            .with_factory_timeouts(Timeout::Infinite, Timeout::After(Duration::ZERO), false),
        LeasePolicy::Fenced,
        "",
    )
    .await;
    assert!(matches!(
        f.cache.read("key", None).await,
        Err(Error::FactoryTimeout { .. })
    ));
    assert_eq!(f.locker.acquired.lock().unwrap().len(), 1);
    assert_eq!(
        f.locker.released.lock().unwrap().len(),
        1,
        "release completes before error reaches caller"
    );
    f.cache.shutdown().await.unwrap();
}
#[tokio::test]
async fn due_marker_eager_refresh_returns_immediately_and_owns_one_fenced_write() {
    let f = fixture(eager_options(), LeasePolicy::Fenced, "").await;
    found(&f.cache).await;
    f.clock.set(time(16));
    let gate = Gate::new();
    *f.store.snapshots.mode.lock().unwrap() = Write::Park(gate.clone());
    found(&f.cache).await;
    gate.entered().await;
    for _ in 0..8 {
        found(&f.cache).await;
    }
    assert_eq!((f.locker.acquisitions(), f.locker.releases()), (2, 1));
    assert_eq!(
        f.locker
            .acquired
            .lock()
            .unwrap()
            .iter()
            .rfind(|(key, _, _)| tag_lock(key))
            .unwrap()
            .2,
        Timeout::After(Duration::ZERO)
    );
    gate.allow();
    until(|| f.locker.releases() == 2).await;
    assert_eq!(f.store.snapshots.writes(), 2);
    let snapshot = f
        .store
        .snapshots
        .inner
        .read_snapshot(&scope(""), &group(), time(16), new_token())
        .await
        .unwrap()
        .snapshot()
        .unwrap();
    assert_eq!(
        (snapshot.version(), snapshot.created()),
        (version(1), time(16))
    );
    assert_eq!(
        f.store.read(&scope(""), &group()).await.unwrap(),
        Some(version(1))
    );
    f.cache.shutdown().await.unwrap();
}
#[tokio::test]
async fn a_newer_fresh_peer_snapshot_hydrates_eager_without_locker_or_duplicate_write() {
    let f = fixture(eager_options(), LeasePolicy::Fenced, "").await;
    found(&f.cache).await;
    let peer = MarkerSnapshot::new(version(1), time(15), time(25), time(35)).unwrap();
    f.store
        .snapshots
        .inner
        .renew_snapshot(&scope(""), &group(), peer, time(15), new_token())
        .await
        .unwrap();
    f.clock.set(time(16));
    found(&f.cache).await;
    until(|| f.store.snapshots.reads() == 3).await;
    tokio::task::yield_now().await;
    assert_eq!(
        (f.locker.acquisitions(), f.store.snapshots.writes()),
        (1, 1)
    );
    f.clock.set(time(20));
    found(&f.cache).await;
    assert_eq!(
        f.store.snapshots.reads(),
        3,
        "peer hydration extends only to peer source lifetime"
    );
    f.cache.shutdown().await.unwrap();
}
#[tokio::test]
async fn eager_contention_stops_and_consumes_attempt_for_both_lease_policies() {
    for policy in [LeasePolicy::Fenced, LeasePolicy::CooperativeLegacy] {
        let f = fixture(eager_options(), policy, "").await;
        found(&f.cache).await;
        *f.locker.acquisition.lock().unwrap() = Acquire::Contended;
        f.clock.set(time(16));
        found(&f.cache).await;
        until(|| f.locker.acquisitions() == 2).await;
        tokio::task::yield_now().await;
        for _ in 0..4 {
            found(&f.cache).await;
        }
        assert_eq!(
            (f.store.snapshots.reads(), f.store.snapshots.writes()),
            (3, 1)
        );
        assert_eq!(f.locker.acquisitions(), 2);
        f.cache.shutdown().await.unwrap();
    }
}
#[tokio::test]
async fn eager_ignores_factory_zero_and_disabled_timed_out_completion() {
    let f = fixture(
        eager_options().with_factory_timeouts(
            Timeout::After(Duration::ZERO),
            Timeout::After(Duration::ZERO),
            false,
        ),
        LeasePolicy::Fenced,
        "",
    )
    .await;
    for kind in [MarkerKind::ClearRemove, group(), MarkerKind::ClearExpire] {
        f.store
            .advance(&scope(""), kind.clone(), version(1))
            .await
            .unwrap();
        f.store
            .snapshots
            .inner
            .renew_snapshot(
                &scope(""),
                &kind,
                MarkerSnapshot::new(version(1), time(9), time(19), time(29)).unwrap(),
                time(10),
                new_token(),
            )
            .await
            .unwrap();
    }
    found(&f.cache).await;
    assert_eq!(
        f.store.snapshots.writes(),
        0,
        "fresh snapshots avoid foreground factory"
    );
    f.clock.set(time(15));
    found(&f.cache).await;
    until(|| f.locker.releases() == 1).await;
    assert_eq!(
        f.store.snapshots.writes(),
        1,
        "eager runs independently of foreground factory deadlines"
    );
    assert_eq!(
        f.store
            .snapshots
            .inner
            .read_snapshot(&scope(""), &group(), time(15), new_token())
            .await
            .unwrap()
            .snapshot()
            .unwrap()
            .created(),
        time(15)
    );
    f.cache.shutdown().await.unwrap();
}
#[tokio::test]
async fn shutdown_cancels_eager_preflight_read_without_acquiring_or_writing() {
    let f = fixture(eager_options(), LeasePolicy::Fenced, "").await;
    found(&f.cache).await;
    let gate = Gate::new();
    *f.store.snapshots.read_mode.lock().unwrap() = Read::Park(gate.clone());
    f.clock.set(time(16));
    found(&f.cache).await;
    gate.entered().await;
    f.cache.shutdown().await.unwrap();
    assert_eq!(
        (f.locker.acquisitions(), f.store.snapshots.writes()),
        (1, 1)
    );
    assert!(matches!(
        f.store
            .snapshots
            .read_tokens
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .check(),
        Err(Error::OperationCancelled {
            reason: FactoryCancellationReason::CacheShutdown
        })
    ));
}
#[tokio::test]
async fn shutdown_cancels_eager_write_and_drains_its_native_lease() {
    let f = fixture(eager_options(), LeasePolicy::Fenced, "").await;
    found(&f.cache).await;
    let gate = Gate::new();
    *f.store.snapshots.mode.lock().unwrap() = Write::Park(gate.clone());
    f.clock.set(time(16));
    found(&f.cache).await;
    gate.entered().await;
    f.cache.shutdown().await.unwrap();
    assert_eq!((f.locker.acquisitions(), f.locker.releases()), (2, 2));
    assert!(matches!(
        f.store
            .snapshots
            .tokens
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .check(),
        Err(Error::OperationCancelled {
            reason: FactoryCancellationReason::CacheShutdown
        })
    ));
    assert_eq!(
        f.store
            .snapshots
            .inner
            .read_snapshot(&scope(""), &group(), time(16), new_token())
            .await
            .unwrap()
            .snapshot()
            .unwrap()
            .created(),
        time(10)
    );
}

#[tokio::test]
async fn same_created_longer_l2_lifetime_hydrates_eager_without_duplicate_factory() {
    let f = fixture(
        options()
            .with_distributed_duration(Duration::from_secs(10))
            .with_eager_refresh(EagerThreshold::new(0.5)),
        LeasePolicy::Fenced,
        "",
    )
    .await;
    found(&f.cache).await;
    assert_eq!(
        (f.store.snapshots.reads(), f.store.snapshots.writes()),
        (2, 1)
    );
    f.clock.set(time(11));
    found(&f.cache).await;
    until(|| f.store.snapshots.reads() >= 3).await;
    tokio::task::yield_now().await;
    assert_eq!(
        (f.locker.acquisitions(), f.store.snapshots.writes()),
        (1, 1)
    );
    f.clock.set(time(12));
    found(&f.cache).await;
    assert_eq!(
        f.store.snapshots.reads(),
        3,
        "L2 hydration extends only the local deadline"
    );
    let snapshot = f
        .store
        .snapshots
        .inner
        .read_snapshot(&scope(""), &group(), time(12), new_token())
        .await
        .unwrap()
        .snapshot()
        .unwrap();
    assert_eq!(
        (
            snapshot.version(),
            snapshot.created(),
            snapshot.logical_expiration()
        ),
        (version(1), time(10), time(20))
    );
    f.cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn skip_when_stale_also_suppresses_eager_preflight_and_owned_recheck() {
    let f = fixture(
        eager_options()
            .with_skip_distributed_read_when_stale(true)
            .with_rethrow_distributed_exceptions(true),
        LeasePolicy::Fenced,
        "",
    )
    .await;
    found(&f.cache).await;
    assert_eq!(f.store.snapshots.reads(), 2);
    *f.store.snapshots.read_mode.lock().unwrap() = Read::Backend;
    f.clock.set(time(16));
    found(&f.cache).await;
    until(|| f.locker.releases() == 2).await;
    assert_eq!(
        (f.store.snapshots.reads(), f.store.snapshots.writes()),
        (2, 2)
    );
    assert_eq!(f.locker.acquisitions(), 2);
    let snapshot = f
        .store
        .snapshots
        .inner
        .read_snapshot(&scope(""), &group(), time(16), new_token())
        .await
        .unwrap()
        .snapshot()
        .unwrap();
    assert_eq!(
        (snapshot.version(), snapshot.created()),
        (version(1), time(16))
    );
    f.cache.shutdown().await.unwrap();
}
