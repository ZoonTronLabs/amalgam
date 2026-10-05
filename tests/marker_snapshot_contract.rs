//! Public expiring-observation contracts; permanent invalidations never expire.
use amalgam::*;
use async_trait::async_trait;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{Notify, Semaphore};

fn time(seconds: i64) -> Timestamp {
    Timestamp::from_ticks(seconds * 10_000_000)
}
fn version(seconds: i64) -> MarkerVersion {
    MarkerVersion::new(time(seconds))
}
fn kind() -> MarkerKind {
    MarkerKind::Tag(Tag::new("group").unwrap())
}
fn scope() -> CacheScope {
    CacheScope::new("", "v2", KeyModifierMode::Prefix).unwrap()
}
fn token() -> FactoryCancellation {
    CancellationSource::new().token()
}
fn tags() -> EntryOptions {
    EntryOptions::tag_defaults()
        .with_memory_duration(Duration::from_secs(2))
        .with_distributed_duration(Duration::from_secs(5))
        .with_fail_safe(true, Some(Duration::from_secs(20)), None)
        .with_distributed_fail_safe_max_duration(Duration::from_secs(12))
}

struct Gate {
    entered: Notify,
    release: Semaphore,
}
impl Gate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: Notify::new(),
            release: Semaphore::new(0),
        })
    }
}
#[derive(Clone)]
enum Mode {
    Pass,
    Miss,
    Backend,
    Protocol,
    Park(Arc<Gate>),
}
#[derive(Debug, thiserror::Error)]
#[error("original snapshot failure")]
struct Cause;
struct Witness {
    token: FactoryCancellation,
    ended: Arc<Mutex<Vec<Option<FactoryCancellationReason>>>>,
}
impl Drop for Witness {
    fn drop(&mut self) {
        let reason = match self.token.check() {
            Ok(()) => None,
            Err(Error::OperationCancelled { reason }) => Some(reason),
            Err(other) => panic!("unexpected cancellation: {other:?}"),
        };
        self.ended.lock().unwrap().push(reason);
    }
}

struct SnapshotCache {
    inner: InMemoryInvalidationStore,
    read_mode: Mutex<Mode>,
    write_mode: Mutex<Mode>,
    reads: AtomicUsize,
    writes: Mutex<Vec<MarkerSnapshot>>,
    ended: Arc<Mutex<Vec<Option<FactoryCancellationReason>>>>,
}
impl SnapshotCache {
    fn new(inner: InMemoryInvalidationStore) -> Arc<Self> {
        Arc::new(Self {
            inner,
            read_mode: Mutex::new(Mode::Pass),
            write_mode: Mutex::new(Mode::Pass),
            reads: AtomicUsize::new(0),
            writes: Mutex::new(Vec::new()),
            ended: Arc::new(Mutex::new(Vec::new())),
        })
    }
    fn count(&self) -> usize {
        self.writes.lock().unwrap().len()
    }
    async fn behavior(mode: Mode) -> std::result::Result<bool, MarkerSnapshotCacheError> {
        match mode {
            Mode::Pass => Ok(true),
            Mode::Miss => Ok(false),
            Mode::Backend => Err(MarkerError::backend(Cause).into()),
            Mode::Protocol => Err(MarkerError::protocol(Cause).into()),
            Mode::Park(gate) => {
                gate.entered.notify_one();
                gate.release.acquire().await.unwrap().forget();
                Ok(true)
            }
        }
    }
}
#[async_trait]
impl MarkerSnapshotCache for SnapshotCache {
    async fn read_snapshot(
        &self,
        scope: &CacheScope,
        selected: &MarkerKind,
        now: Timestamp,
        cancellation: FactoryCancellation,
    ) -> std::result::Result<MarkerSnapshotRead, MarkerSnapshotCacheError> {
        if *selected == kind() {
            self.reads.fetch_add(1, Ordering::Relaxed);
            let mode = self.read_mode.lock().unwrap().clone();
            if !Self::behavior(mode).await? {
                return Ok(MarkerSnapshotRead::Missing {
                    maximum: self.inner.read(scope, selected).await?,
                });
            }
        }
        self.inner
            .read_snapshot(scope, selected, now, cancellation)
            .await
    }
    async fn renew_snapshot(
        &self,
        scope: &CacheScope,
        selected: &MarkerKind,
        snapshot: MarkerSnapshot,
        now: Timestamp,
        cancellation: FactoryCancellation,
    ) -> std::result::Result<MarkerSnapshotRenewal, MarkerSnapshotCacheError> {
        if *selected == kind() {
            self.writes.lock().unwrap().push(snapshot);
            let _witness = Witness {
                token: cancellation.clone(),
                ended: self.ended.clone(),
            };
            let mode = self.write_mode.lock().unwrap().clone();
            Self::behavior(mode).await?;
            return self
                .inner
                .renew_snapshot(scope, selected, snapshot, now, cancellation)
                .await;
        }
        self.inner
            .renew_snapshot(scope, selected, snapshot, now, cancellation)
            .await
    }
}
struct Store {
    inner: InMemoryInvalidationStore,
    cache: Arc<SnapshotCache>,
}
impl Store {
    fn new() -> Arc<Self> {
        let inner = InMemoryInvalidationStore::default();
        Arc::new(Self {
            cache: SnapshotCache::new(inner.clone()),
            inner,
        })
    }
}
#[async_trait]
impl InvalidationStore for Store {
    fn snapshot_cache(&self) -> Option<Arc<dyn MarkerSnapshotCache>> {
        Some(self.cache.clone())
    }
    async fn read(
        &self,
        scope: &CacheScope,
        kind: &MarkerKind,
    ) -> std::result::Result<Option<MarkerVersion>, MarkerError> {
        self.inner.read(scope, kind).await
    }
    async fn advance(
        &self,
        scope: &CacheScope,
        kind: MarkerKind,
        candidate: MarkerVersion,
    ) -> std::result::Result<MarkerAdvanceOutcome, MarkerError> {
        self.inner.advance(scope, kind, candidate).await
    }
}
async fn seeded(clock: Arc<ManualClock>) -> Arc<InMemoryDistributedCache> {
    let backend = Arc::new(InMemoryDistributedCache::new(clock.clone()));
    let writer = Cache::<u64>::builder()
        .clock(clock)
        .distributed(backend.clone())
        .serializer(Arc::new(JsonSerializer))
        .try_build()
        .unwrap();
    writer
        .try_set_full(
            "key",
            7,
            Some(EntryOptions::new(Duration::from_secs(3600))),
            vec![Tag::new("group").unwrap()].into_boxed_slice(),
        )
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    writer.shutdown().await.unwrap();
    backend
}
fn reader(
    clock: Arc<ManualClock>,
    backend: Arc<InMemoryDistributedCache>,
    store: Arc<Store>,
    options: EntryOptions,
) -> Cache<u64> {
    Cache::builder()
        .clock(clock)
        .distributed(backend)
        .invalidation_store(store)
        .serializer(Arc::new(JsonSerializer))
        .tags_default_options(options)
        .marker_read_policy(MarkerReadPolicy::OptionsControlled)
        .marker_lifecycle_policy(MarkerLifecyclePolicy::CachedSnapshots)
        .reconciliation_policy(ReconciliationPolicy::Periodic(Duration::from_secs(3600)))
        .try_build()
        .unwrap()
}
async fn fixture(
    options: EntryOptions,
    marked: bool,
) -> (
    Arc<ManualClock>,
    Arc<InMemoryDistributedCache>,
    Arc<Store>,
    Cache<u64>,
) {
    let clock = Arc::new(ManualClock::new(time(10)));
    let backend = seeded(clock.clone()).await;
    let store = Store::new();
    if marked {
        store.advance(&scope(), kind(), version(1)).await.unwrap();
    }
    let cache = reader(clock.clone(), backend.clone(), store.clone(), options);
    (clock, backend, store, cache)
}

#[test]
fn validated_lifetimes_and_explicit_capability_errors() {
    assert_eq!(
        MarkerSnapshot::new(version(1), time(3), time(2), time(5)),
        Err(MarkerSnapshotValidationError::InvalidDeadlines)
    );
    assert_eq!(
        MarkerSnapshot::new(version(1), time(1), time(4), time(3)),
        Err(MarkerSnapshotValidationError::InvalidDeadlines)
    );
    assert_eq!(
        MarkerSnapshotLimits::new(0),
        Err(MarkerSnapshotValidationError::ZeroCapacity)
    );
    assert!(matches!(
        Cache::<u64>::builder()
            .marker_lifecycle_policy(MarkerLifecyclePolicy::CachedSnapshots)
            .try_build(),
        Err(Error::Config(
            ConfigError::MarkerLifecycleRequiresControlledReads
        ))
    ));
    assert!(matches!(
        Cache::<u64>::builder()
            .marker_read_policy(MarkerReadPolicy::OptionsControlled)
            .marker_lifecycle_policy(MarkerLifecyclePolicy::CachedSnapshots)
            .try_build(),
        Err(Error::Config(
            ConfigError::MarkerSnapshotCapabilityUnavailable
        ))
    ));
    assert_eq!(
        Cache::<u64>::builder()
            .try_build()
            .unwrap()
            .marker_lifecycle_policy(),
        MarkerLifecyclePolicy::DurableOnly
    );
}

#[tokio::test]
async fn provider_renewal_keeps_higher_facts_and_later_insertions_without_expiring_journal() {
    let store = InMemoryInvalidationStore::default();
    store.advance(&scope(), kind(), version(5)).await.unwrap();
    let low = MarkerSnapshot::new(version(1), time(10), time(12), time(20)).unwrap();
    let MarkerSnapshotRenewal::Stored(merged) = store
        .renew_snapshot(&scope(), &kind(), low, time(10), token())
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(merged.version(), version(5));
    let newer = MarkerSnapshot::new(version(5), time(11), time(13), time(21)).unwrap();
    store
        .renew_snapshot(&scope(), &kind(), newer, time(11), token())
        .await
        .unwrap();
    assert_eq!(
        store
            .renew_snapshot(&scope(), &kind(), merged, time(11), token())
            .await
            .unwrap(),
        MarkerSnapshotRenewal::KeptNewer(newer)
    );
    assert_eq!(
        store
            .read_snapshot(&scope(), &kind(), time(21), token())
            .await
            .unwrap()
            .snapshot(),
        None
    );
    assert_eq!(
        store.read(&scope(), &kind()).await.unwrap(),
        Some(version(5))
    );
    assert_eq!(
        store
            .renew_snapshot(&scope(), &kind(), low, time(20), token())
            .await
            .unwrap(),
        MarkerSnapshotRenewal::Expired
    );
}

#[tokio::test]
async fn snapshot_eviction_is_bounded_and_cannot_evict_durable_facts_or_other_scopes() {
    let store = InMemoryInvalidationStore::with_snapshot_limits(
        MarkerStoreLimits::default(),
        MarkerSnapshotLimits::new(1).unwrap(),
    );
    let other = CacheScope::new("other", "v2", KeyModifierMode::Prefix).unwrap();
    store.advance(&scope(), kind(), version(1)).await.unwrap();
    store.advance(&other, kind(), version(2)).await.unwrap();
    let snapshot = MarkerSnapshot::fresh(version(1), &tags(), time(10));
    store
        .renew_snapshot(&scope(), &kind(), snapshot, time(10), token())
        .await
        .unwrap();
    store
        .renew_snapshot(&other, &kind(), snapshot, time(10), token())
        .await
        .unwrap();
    assert_eq!(store.snapshot_count(), 1);
    assert_eq!(
        store.read(&scope(), &kind()).await.unwrap(),
        Some(version(1))
    );
    assert_eq!(store.read(&other, &kind()).await.unwrap(), Some(version(2)));
    assert_eq!(
        store
            .read_snapshot(&other, &kind(), time(10), token())
            .await
            .unwrap()
            .snapshot()
            .unwrap()
            .version(),
        version(2)
    );
}

#[tokio::test]
async fn nonzero_miss_repairs_independent_distributed_deadlines_without_backplane_or_revision_change()
 {
    let (_, _, store, cache) = fixture(tags(), true).await;
    let mut events = cache.events().subscribe();
    assert_eq!(cache.read("key", None).await.unwrap().value(), Some(&7));
    let writes = store.cache.writes.lock().unwrap().clone();
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].version(), version(1));
    assert_eq!(writes[0].logical_expiration(), time(15));
    assert_eq!(writes[0].physical_expiration(), time(22));
    drop(writes);
    let mut completed = false;
    while let Ok(event) = events.try_recv() {
        if let CacheEvent::MarkerSnapshotWrite {
            kind: selected,
            outcome: MarkerSnapshotWriteOutcome::Stored,
        } = &event
        {
            completed |= *selected == kind();
        }
        assert!(!matches!(event, CacheEvent::MessagePublished { .. }));
    }
    assert!(completed);
    assert_eq!(
        store.read(&scope(), &kind()).await.unwrap(),
        Some(version(1))
    );
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn absent_revision_is_locally_cached_but_never_written_to_snapshot_storage() {
    let (_, _, store, cache) = fixture(tags(), false).await;
    for _ in 0..3 {
        assert_eq!(cache.read("key", None).await.unwrap().value(), Some(&7));
    }
    assert_eq!(store.cache.count(), 0);
    assert_eq!(store.cache.reads.load(Ordering::Relaxed), 1);
    assert_eq!(store.inner.snapshot_count(), 0);
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn local_factory_lifetime_is_independent_of_shorter_l2_and_fresh_hydration_never_renews_l2() {
    let options = tags()
        .with_memory_duration(Duration::from_secs(10))
        .with_distributed_duration(Duration::from_secs(1));
    let (clock, backend, store, cache) = fixture(options.clone(), true).await;
    assert_eq!(cache.read("key", None).await.unwrap().value(), Some(&7));
    clock.advance(Duration::from_secs(2));
    assert_eq!(cache.read("key", None).await.unwrap().value(), Some(&7));
    assert_eq!(store.cache.reads.load(Ordering::Relaxed), 1);
    assert_eq!(store.cache.count(), 1);
    cache.shutdown().await.unwrap();
    let fresh = MarkerSnapshot::new(version(1), time(12), time(17), time(22)).unwrap();
    store
        .inner
        .renew_snapshot(&scope(), &kind(), fresh, time(12), token())
        .await
        .unwrap();
    let second = reader(clock.clone(), backend, store.clone(), options);
    second.read("key", None).await.unwrap();
    assert_eq!(store.cache.count(), 1);
    clock.advance(Duration::from_secs(5));
    second.read("key", None).await.unwrap();
    assert_eq!(
        store.cache.count(),
        2,
        "hydration must retain the source logical deadline"
    );
    second.shutdown().await.unwrap();
}

#[tokio::test]
async fn logically_stale_and_physically_expired_snapshots_repair_without_forgetting_facts() {
    let (clock, _, store, cache) = fixture(tags(), true).await;
    let stale = MarkerSnapshot::new(version(1), time(2), time(3), time(15)).unwrap();
    store
        .inner
        .renew_snapshot(&scope(), &kind(), stale, time(10), token())
        .await
        .unwrap();
    cache.read("key", None).await.unwrap();
    assert_eq!(store.cache.count(), 1);
    assert_eq!(store.cache.writes.lock().unwrap()[0].version(), version(1));
    clock.advance(Duration::from_secs(12));
    *store.cache.read_mode.lock().unwrap() = Mode::Miss;
    cache.read("key", None).await.unwrap();
    assert_eq!(store.cache.count(), 2);
    assert_eq!(
        store.read(&scope(), &kind()).await.unwrap(),
        Some(version(1))
    );
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn all_observations_expiring_cannot_resurrect_an_invalidated_value() {
    let (clock, backend, store, cache) = fixture(tags(), true).await;
    cache.read("key", None).await.unwrap();
    store.advance(&scope(), kind(), version(11)).await.unwrap();
    cache.shutdown().await.unwrap();
    clock.advance(Duration::from_secs(100));
    let fresh_reader = reader(clock, backend, store.clone(), tags());
    assert!(!fresh_reader.read("key", None).await.unwrap().has_value());
    assert_eq!(
        store.read(&scope(), &kind()).await.unwrap(),
        Some(version(11))
    );
    fresh_reader.shutdown().await.unwrap();
}

#[tokio::test]
async fn explicit_write_skip_and_read_degradation_do_not_renew() {
    let (_, _, store, cache) = fixture(tags().with_skip_distributed(false, true), true).await;
    cache.read("key", None).await.unwrap();
    assert_eq!(store.cache.count(), 0);
    cache.shutdown().await.unwrap();
    let (clock, _, store, cache) = fixture(tags(), true).await;
    cache.read("key", None).await.unwrap();
    clock.advance(Duration::from_secs(2));
    *store.cache.read_mode.lock().unwrap() = Mode::Backend;
    cache.read("key", None).await.unwrap();
    assert_eq!(
        store.cache.count(),
        1,
        "a read fail-safe result is not a successful shared factory"
    );
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn renewal_faults_honor_independent_flags_and_preserve_original_causes() {
    use std::error::Error as _;
    for (mode, protocol) in [(Mode::Backend, false), (Mode::Protocol, true)] {
        for strict in [false, true] {
            let options = if protocol {
                tags().with_rethrow_serialization_exceptions(strict)
            } else {
                tags().with_rethrow_distributed_exceptions(strict)
            };
            let (_, _, store, cache) = fixture(options, true).await;
            *store.cache.write_mode.lock().unwrap() = mode.clone();
            let result = cache.read("key", None).await;
            if strict {
                let Error::Marker(error) = result.unwrap_err() else {
                    panic!()
                };
                assert!(error.source().unwrap().downcast_ref::<Cause>().is_some());
            } else {
                assert_eq!(result.unwrap().value(), Some(&7));
            }
            assert_eq!(store.cache.count(), 1);
            cache.shutdown().await.unwrap();
        }
    }
}

#[tokio::test(start_paused = true)]
async fn foreground_renewal_is_awaited_and_not_capped_by_read_timeout() {
    let (_, _, store, cache) = fixture(
        tags()
            .with_distributed_timeouts(Timeout::Infinite, Timeout::After(Duration::from_millis(2))),
        true,
    )
    .await;
    let gate = Gate::new();
    *store.cache.write_mode.lock().unwrap() = Mode::Park(gate.clone());
    let call = tokio::spawn({
        let cache = cache.clone();
        async move { cache.read("key", None).await }
    });
    gate.entered.notified().await;
    tokio::time::advance(Duration::from_millis(50)).await;
    assert!(!call.is_finished());
    gate.release.add_permits(1);
    assert_eq!(call.await.unwrap().unwrap().value(), Some(&7));
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn background_renewal_survives_completed_foreground_and_shutdown_cancels_its_real_token() {
    let (_, _, store, cache) = fixture(
        tags().with_allow_background_distributed_operations(true),
        true,
    )
    .await;
    let gate = Gate::new();
    *store.cache.write_mode.lock().unwrap() = Mode::Park(gate.clone());
    assert_eq!(cache.read("key", None).await.unwrap().value(), Some(&7));
    gate.entered.notified().await;
    assert!(store.cache.ended.lock().unwrap().is_empty());
    cache.shutdown().await.unwrap();
    assert_eq!(
        *store.cache.ended.lock().unwrap(),
        vec![Some(FactoryCancellationReason::CacheShutdown)]
    );
    assert_eq!(store.inner.snapshot_count(), 0);
}

#[tokio::test]
async fn parked_old_renewal_cannot_overwrite_concurrent_higher_invalidation_or_later_snapshot() {
    let (_, _, store, cache) = fixture(tags(), true).await;
    let gate = Gate::new();
    *store.cache.write_mode.lock().unwrap() = Mode::Park(gate.clone());
    let call = tokio::spawn({
        let cache = cache.clone();
        async move { cache.read("key", None).await }
    });
    gate.entered.notified().await;
    store.advance(&scope(), kind(), version(11)).await.unwrap();
    let newer = MarkerSnapshot::new(version(11), time(11), time(16), time(23)).unwrap();
    store
        .inner
        .renew_snapshot(&scope(), &kind(), newer, time(11), token())
        .await
        .unwrap();
    gate.release.add_permits(1);
    assert!(!call.await.unwrap().unwrap().has_value());
    assert_eq!(
        store
            .inner
            .read_snapshot(&scope(), &kind(), time(11), token())
            .await
            .unwrap()
            .snapshot(),
        Some(newer)
    );
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn zero_hard_factory_budget_does_not_run_renewal_and_cold_soft_is_ineligible() {
    let (_, _, store, cache) = fixture(
        tags().with_factory_timeouts(Timeout::Infinite, Timeout::After(Duration::ZERO), false),
        true,
    )
    .await;
    cache.read("key", None).await.unwrap();
    assert_eq!(store.cache.count(), 0);
    cache.shutdown().await.unwrap();
    let options = tags()
        .with_fail_safe(false, None, None)
        .with_factory_timeouts(Timeout::Infinite, Timeout::After(Duration::ZERO), false);
    let (_, _, store, cache) = fixture(options, true).await;
    assert!(matches!(
        cache.read("key", None).await,
        Err(Error::FactoryTimeout {
            elapsed: Duration::ZERO
        })
    ));
    assert_eq!(store.cache.count(), 0);
    cache.shutdown().await.unwrap();
    let (_, _, store, cache) = fixture(
        tags().with_factory_timeouts(Timeout::After(Duration::ZERO), Timeout::Infinite, false),
        true,
    )
    .await;
    cache.read("key", None).await.unwrap();
    assert_eq!(store.cache.count(), 1);
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn zero_soft_factory_budget_is_eligible_for_retained_l2_without_local_observation() {
    let options =
        tags().with_factory_timeouts(Timeout::After(Duration::ZERO), Timeout::Infinite, false);
    let (_, _, store, cache) = fixture(options, true).await;
    let stale = MarkerSnapshot::new(version(1), time(2), time(3), time(20)).unwrap();
    store
        .inner
        .renew_snapshot(&scope(), &kind(), stale, time(10), token())
        .await
        .unwrap();
    let mut events = cache.events().subscribe();
    assert_eq!(cache.read("key", None).await.unwrap().value(), Some(&7));
    assert_eq!(store.cache.count(), 0);
    assert!(
        std::iter::from_fn(|| events.try_recv().ok()).any(|event| matches!(
            event,
            CacheEvent::MarkerRead {
                kind: MarkerKind::Tag(_),
                outcome: MarkerReadOutcome::StaleFallback(MarkerReadFailure::FactorySoftTimeout)
            }
        ))
    );
    assert_eq!(
        store
            .inner
            .read_snapshot(&scope(), &kind(), time(10), token())
            .await
            .unwrap()
            .snapshot(),
        Some(stale)
    );
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn explicit_cancellation_of_foreground_write_cannot_be_suppressed_as_a_storage_fault() {
    let (_, _, store, cache) = fixture(tags(), true).await;
    let gate = Gate::new();
    *store.cache.write_mode.lock().unwrap() = Mode::Park(gate.clone());
    let source = CancellationSource::new();
    let call = tokio::spawn({
        let cache = cache.clone();
        let token = source.token();
        async move { cache.read_cancellable("key", None, token).await }
    });
    gate.entered.notified().await;
    source.cancel();
    assert!(matches!(
        call.await.unwrap(),
        Err(Error::OperationCancelled {
            reason: FactoryCancellationReason::CallerCancelled
        })
    ));
    assert_eq!(
        *store.cache.ended.lock().unwrap(),
        vec![Some(FactoryCancellationReason::CallerCancelled)]
    );
    assert_eq!(store.inner.snapshot_count(), 0);
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn native_memory_snapshot_renewal_requires_current_atomic_lease_ownership() {
    let clock = Arc::new(ManualClock::new(time(10)));
    let locker = Arc::new(InMemoryDistributedLocker::new(clock.clone()));
    let store = InMemoryInvalidationStore::default();
    let lease = acquire_owned(
        locker,
        "snapshot".into(),
        LeaseTtl::new(Duration::from_secs(1)).unwrap(),
        Timeout::Infinite,
        AcquisitionPolicy::TokenOwned,
    )
    .await
    .unwrap()
    .unwrap();
    let proof = lease.proof().unwrap();
    let snapshot = MarkerSnapshot::fresh(version(1), &tags(), time(10));
    assert!(matches!(
        store
            .renew_snapshot_with_lease(&scope(), &kind(), snapshot, time(10), &proof, token())
            .await
            .unwrap(),
        MarkerSnapshotRenewal::Stored(_)
    ));
    clock.advance(Duration::from_secs(1));
    assert!(matches!(
        store
            .renew_snapshot_with_lease(&scope(), &kind(), snapshot, time(11), &proof, token())
            .await,
        Err(MarkerSnapshotCacheError::Lease(LeaseError::Lost))
    ));
    lease.release().await.unwrap();
}
