//! Public contracts for tag/clear population and immutable automatic recovery.
use amalgam::*;
use async_trait::async_trait;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{Notify, Semaphore, broadcast};

fn time(seconds: i64) -> Timestamp {
    Timestamp::from_ticks(seconds * 10_000_000)
}
fn version(seconds: i64) -> MarkerVersion {
    MarkerVersion::new(time(seconds))
}
fn group() -> MarkerKind {
    MarkerKind::Tag(Tag::new("group").unwrap())
}
fn scope() -> CacheScope {
    CacheScope::new("", "v2", KeyModifierMode::Prefix).unwrap()
}
fn token() -> FactoryCancellation {
    CancellationSource::new().token()
}
fn options() -> EntryOptions {
    EntryOptions::tag_defaults()
        .with_memory_duration(Duration::from_secs(2))
        .with_fail_safe(false, None, None)
        .with_distributed_duration(Duration::from_secs(5))
        .with_allow_background_distributed_operations(false)
        .with_allow_background_backplane_operations(false)
        .with_rethrow_distributed_exceptions(false)
}

#[derive(Debug, thiserror::Error)]
#[error("original injected marker storage fault")]
struct Cause;
struct Park {
    entered: Notify,
    release: Semaphore,
}
impl Park {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: Notify::new(),
            release: Semaphore::new(0),
        })
    }
}
#[derive(Clone)]
enum WriteMode {
    Pass,
    Fail,
    Park(Arc<Park>),
}
struct Snapshots {
    inner: InMemoryInvalidationStore,
    mode: Mutex<WriteMode>,
    writes: Mutex<Vec<(MarkerKind, MarkerSnapshot)>>,
    signals: Mutex<Vec<FactoryCancellation>>,
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
        self.writes.lock().unwrap().push((kind.clone(), snapshot));
        self.signals.lock().unwrap().push(cancellation.clone());
        let mode = self.mode.lock().unwrap().clone();
        match mode {
            WriteMode::Pass => {}
            WriteMode::Fail => return Err(MarkerError::backend(Cause).into()),
            WriteMode::Park(park) => {
                park.entered.notify_one();
                park.release.acquire().await.unwrap().forget();
            }
        }
        self.inner
            .renew_snapshot(scope, kind, snapshot, now, cancellation)
            .await
    }
}
struct Store {
    snapshots: Arc<Snapshots>,
    advance_fails: AtomicBool,
    advances: AtomicUsize,
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
        version: MarkerVersion,
    ) -> std::result::Result<MarkerAdvanceOutcome, MarkerError> {
        self.advances.fetch_add(1, Ordering::SeqCst);
        if self.advance_fails.load(Ordering::SeqCst) {
            return Err(MarkerError::backend(Cause));
        }
        self.snapshots.inner.advance(scope, kind, version).await
    }
}
struct Bus {
    fails: AtomicBool,
    commands: Mutex<Vec<BackplaneCommand>>,
    sender: broadcast::Sender<BackplaneMessage>,
}
#[async_trait]
impl Backplane for Bus {
    async fn publish(&self, message: BackplaneMessage) -> Result<()> {
        self.publish_command(BackplaneCommand::from_message(message)?)
            .await
    }
    async fn publish_command(&self, command: BackplaneCommand) -> Result<()> {
        if self.fails.load(Ordering::SeqCst) {
            return Err(Error::distributed(Cause));
        }
        self.commands.lock().unwrap().push(command);
        Ok(())
    }
    fn subscribe(&self) -> broadcast::Receiver<BackplaneMessage> {
        self.sender.subscribe()
    }
}
struct Fixture {
    clock: Arc<ManualClock>,
    store: Arc<Store>,
    bus: Arc<Bus>,
    cache: Cache<u64>,
}
fn fixture(configuration: RecoveryConfig) -> Fixture {
    fixture_with_limit(configuration, 4096)
}
fn fixture_with_limit(configuration: RecoveryConfig, max_tags: usize) -> Fixture {
    let clock = Arc::new(ManualClock::new(time(10)));
    let store = Arc::new(Store {
        snapshots: Arc::new(Snapshots {
            inner: InMemoryInvalidationStore::new(MarkerStoreLimits::new(max_tags, 16).unwrap()),
            mode: Mutex::new(WriteMode::Pass),
            writes: Mutex::new(Vec::new()),
            signals: Mutex::new(Vec::new()),
        }),
        advance_fails: AtomicBool::new(false),
        advances: AtomicUsize::new(0),
    });
    let bus = Arc::new(Bus {
        fails: AtomicBool::new(false),
        commands: Mutex::new(Vec::new()),
        sender: broadcast::channel(32).0,
    });
    let cache = Cache::builder()
        .clock(clock.clone())
        .invalidation_store(store.clone())
        .backplane(bus.clone())
        .tags_default_options(options())
        .marker_read_policy(MarkerReadPolicy::OptionsControlled)
        .marker_lifecycle_policy(MarkerLifecyclePolicy::CachedSnapshots)
        .reconciliation_policy(ReconciliationPolicy::Periodic(Duration::from_secs(3600)))
        .auto_recovery(configuration)
        .try_build()
        .unwrap();
    Fixture {
        clock,
        store,
        bus,
        cache,
    }
}
fn recovery() -> RecoveryConfig {
    RecoveryConfig {
        delay: Duration::from_millis(30),
        ..RecoveryConfig::default()
    }
}
async fn until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
}
async fn mutate(
    cache: &Cache<u64>,
    kind: &MarkerKind,
    options: Option<EntryOptions>,
) -> Result<CommitReport> {
    let receipt = match kind {
        MarkerKind::Tag(tag) => {
            cache
                .remove_by_tag(tag.clone())
                .options(|defaults| (options).unwrap_or(defaults))
                .with_receipt()
                .await?
        }
        MarkerKind::ClearExpire => {
            cache
                .clear(ClearMode::Expire)
                .options(|defaults| (options).unwrap_or(defaults))
                .with_receipt()
                .await?
        }
        MarkerKind::ClearRemove => {
            cache
                .clear(ClearMode::Remove)
                .options(|defaults| (options).unwrap_or(defaults))
                .with_receipt()
                .await?
        }
    };
    receipt.wait().await
}
fn has_queued(effect: &EffectOutcome) -> bool {
    match effect {
        EffectOutcome::RecoveryQueued { cause } => {
            matches!(cause, Error::Marker(MarkerError::Backend { .. }))
        }
        EffectOutcome::Batch(batch) => batch.stages().iter().any(has_queued),
        EffectOutcome::Applied
        | EffectOutcome::NotConfigured
        | EffectOutcome::Skipped(_)
        | EffectOutcome::FailedSuppressed { .. } => false,
    }
}

#[tokio::test]
async fn explicit_tag_and_both_clear_modes_populate_original_policy() {
    let f = fixture(recovery());
    for kind in [group(), MarkerKind::ClearExpire, MarkerKind::ClearRemove] {
        mutate(
            &f.cache,
            &kind,
            Some(options().with_distributed_duration(Duration::from_secs(7))),
        )
        .await
        .unwrap();
        let stored = f
            .store
            .snapshots
            .inner
            .read_snapshot(&scope(), &kind, time(10), token())
            .await
            .unwrap()
            .snapshot()
            .unwrap();
        assert_eq!(stored.version(), version(10));
        assert_eq!(stored.created(), time(10));
        assert_eq!(stored.logical_expiration(), time(17));
        assert_eq!(stored.physical_expiration(), time(17));
    }
    assert_eq!(f.cache.pending_recovery(), 0);
    f.cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn snapshot_failure_preserves_fact_and_publication_and_original_expiry() {
    let f = fixture(recovery());
    *f.store.snapshots.mode.lock().unwrap() = WriteMode::Fail;
    let report = mutate(&f.cache, &group(), None).await.unwrap();
    assert!(has_queued(&report.distributed));
    assert_eq!(
        f.store.read(&scope(), &group()).await.unwrap(),
        Some(version(10))
    );
    assert_eq!(f.bus.commands.lock().unwrap().len(), 1);
    let ticket = f.cache.marker_snapshot_recovery_ticket(&group()).unwrap();
    let RecoveryWork::MarkerSnapshot(work) = ticket.work() else {
        panic!("expected finite observation repair");
    };
    assert_eq!(work.snapshot().created(), time(10));
    assert_eq!(work.snapshot().physical_expiration(), time(15));
    assert_eq!(work.participation(), MarkerSnapshotParticipation::Unleased);
    f.clock.set(time(12));
    *f.store.snapshots.mode.lock().unwrap() = WriteMode::Pass;
    until(|| f.cache.pending_recovery() == 0).await;
    let stored = f
        .store
        .snapshots
        .inner
        .read_snapshot(&scope(), &group(), time(12), token())
        .await
        .unwrap()
        .snapshot()
        .unwrap();
    assert_eq!(stored.created(), time(10));
    assert_eq!(stored.physical_expiration(), time(15));
    assert_eq!(
        f.store.advances.load(Ordering::SeqCst),
        1,
        "snapshot repair must never advance a fact"
    );
    assert_eq!(
        f.bus.commands.lock().unwrap().len(),
        1,
        "observation repair must never publish an invalidation"
    );
    f.cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn strict_snapshot_failure_keeps_notification_obligation() {
    let f = fixture(recovery());
    *f.store.snapshots.mode.lock().unwrap() = WriteMode::Fail;
    let result = mutate(
        &f.cache,
        &MarkerKind::ClearRemove,
        Some(options().with_rethrow_distributed_exceptions(true)),
    )
    .await;
    assert!(matches!(
        result,
        Err(Error::Marker(MarkerError::Backend { .. }))
    ));
    assert_eq!(
        f.store
            .read(&scope(), &MarkerKind::ClearRemove)
            .await
            .unwrap(),
        Some(version(10))
    );
    assert!(
        f.cache
            .marker_snapshot_recovery_ticket(&MarkerKind::ClearRemove)
            .is_some()
    );
    *f.store.snapshots.mode.lock().unwrap() = WriteMode::Pass;
    until(|| f.cache.pending_recovery() == 0).await;
    assert_eq!(f.bus.commands.lock().unwrap().len(), 1);
    f.cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn mutation_replay_retains_population_then_notification_stages() {
    let f = fixture(recovery());
    f.store.advance_fails.store(true, Ordering::SeqCst);
    *f.store.snapshots.mode.lock().unwrap() = WriteMode::Fail;
    f.bus.fails.store(true, Ordering::SeqCst);
    mutate(
        &f.cache,
        &group(),
        Some(options().with_distributed_duration(Duration::from_secs(9))),
    )
    .await
    .unwrap();
    let ticket = f.cache.marker_recovery_ticket(&group()).unwrap();
    let RecoveryWork::MarkerMutation(work) = ticket.work() else {
        panic!("expected captured mutation");
    };
    assert!(
        matches!(work.stage(), MarkerMutationStage::Advance { snapshot, .. } if snapshot.physical_expiration() == time(19))
    );
    f.clock.set(time(12));
    f.store.advance_fails.store(false, Ordering::SeqCst);
    until(|| matches!(f.cache.marker_recovery_ticket(&group()).as_ref().map(ReplayTicket::work), Some(RecoveryWork::MarkerMutation(work)) if matches!(work.stage(), MarkerMutationStage::Populate { .. }))).await;
    let advances = f.store.advances.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(70)).await;
    assert_eq!(f.store.advances.load(Ordering::SeqCst), advances);
    *f.store.snapshots.mode.lock().unwrap() = WriteMode::Pass;
    until(|| matches!(f.cache.marker_recovery_ticket(&group()).as_ref().map(ReplayTicket::work), Some(RecoveryWork::MarkerMutation(work)) if matches!(work.stage(), MarkerMutationStage::Notify { .. }))).await;
    let writes = f.store.snapshots.writes.lock().unwrap().len();
    tokio::time::sleep(Duration::from_millis(70)).await;
    assert_eq!(f.store.snapshots.writes.lock().unwrap().len(), writes);
    f.bus.fails.store(false, Ordering::SeqCst);
    until(|| f.cache.pending_recovery() == 0).await;
    let stored = f
        .store
        .snapshots
        .inner
        .read_snapshot(&scope(), &group(), time(12), token())
        .await
        .unwrap()
        .snapshot()
        .unwrap();
    assert_eq!(stored.created(), time(10));
    assert_eq!(stored.physical_expiration(), time(19));
    assert_eq!(f.bus.commands.lock().unwrap().len(), 1);
    f.cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn finite_repairs_expire_but_pending_durable_facts_do_not() {
    let f = fixture(recovery());
    *f.store.snapshots.mode.lock().unwrap() = WriteMode::Fail;
    mutate(&f.cache, &group(), None).await.unwrap();
    f.clock.set(time(15));
    *f.store.snapshots.mode.lock().unwrap() = WriteMode::Pass;
    until(|| f.cache.pending_recovery() == 0).await;
    assert!(
        f.store
            .snapshots
            .inner
            .read_snapshot(&scope(), &group(), time(15), token())
            .await
            .unwrap()
            .snapshot()
            .is_none()
    );
    assert_eq!(
        f.store.read(&scope(), &group()).await.unwrap(),
        Some(version(10))
    );
    f.store.advance_fails.store(true, Ordering::SeqCst);
    *f.store.snapshots.mode.lock().unwrap() = WriteMode::Fail;
    mutate(
        &f.cache,
        &MarkerKind::ClearExpire,
        Some(options().with_skip_backplane_notifications(true)),
    )
    .await
    .unwrap();
    f.clock.set(time(100));
    f.store.advance_fails.store(false, Ordering::SeqCst);
    until(|| f.cache.pending_recovery() == 0).await;
    assert_eq!(
        f.store
            .read(&scope(), &MarkerKind::ClearExpire)
            .await
            .unwrap(),
        Some(version(15))
    );
    assert!(
        f.store
            .snapshots
            .inner
            .read_snapshot(&scope(), &MarkerKind::ClearExpire, time(100), token())
            .await
            .unwrap()
            .snapshot()
            .is_none()
    );
    f.cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn failed_replay_consumes_budget_instead_of_becoming_success() {
    let f = fixture(RecoveryConfig {
        max_retries: Some(1),
        ..recovery()
    });
    *f.store.snapshots.mode.lock().unwrap() = WriteMode::Fail;
    mutate(&f.cache, &group(), None).await.unwrap();
    until(|| f.cache.pending_recovery() == 0).await;
    assert_eq!(
        f.store.snapshots.writes.lock().unwrap().len(),
        3,
        "one original attempt plus two actual failed retries"
    );
    assert!(
        f.store
            .snapshots
            .inner
            .read_snapshot(&scope(), &group(), time(10), token())
            .await
            .unwrap()
            .snapshot()
            .is_none()
    );
    f.cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn skipped_writes_and_disabled_recovery_are_explicit() {
    let f = fixture(RecoveryConfig {
        enabled: false,
        ..recovery()
    });
    *f.store.snapshots.mode.lock().unwrap() = WriteMode::Fail;
    let report = mutate(&f.cache, &group(), None).await.unwrap();
    assert!(!has_queued(&report.distributed));
    assert_eq!(f.cache.pending_recovery(), 0);
    let attempts = f.store.snapshots.writes.lock().unwrap().len();
    mutate(
        &f.cache,
        &MarkerKind::ClearRemove,
        Some(options().with_skip_distributed(false, true)),
    )
    .await
    .unwrap();
    assert_eq!(f.store.snapshots.writes.lock().unwrap().len(), attempts);
    assert_eq!(
        f.store
            .read(&scope(), &MarkerKind::ClearRemove)
            .await
            .unwrap(),
        None
    );
    f.cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn shutdown_signals_actual_explicit_population_provider() {
    let f = fixture(recovery());
    let park = Park::new();
    *f.store.snapshots.mode.lock().unwrap() = WriteMode::Park(park.clone());
    let cache = f.cache.clone();
    let task = tokio::spawn(async move { mutate(&cache, &group(), None).await });
    tokio::time::timeout(Duration::from_secs(2), park.entered.notified())
        .await
        .unwrap();
    let cancellation = f.store.snapshots.signals.lock().unwrap()[0].clone();
    f.cache.shutdown().await.unwrap();
    assert!(matches!(
        task.await.unwrap(),
        Err(Error::OperationCancelled {
            reason: FactoryCancellationReason::CacheShutdown
        })
    ));
    assert!(matches!(
        cancellation.check(),
        Err(Error::OperationCancelled {
            reason: FactoryCancellationReason::CacheShutdown
        })
    ));
}

#[tokio::test]
async fn replacing_tag_recovery_cannot_forget_a_committed_compacted_clear() {
    let f = fixture_with_limit(recovery(), 1);
    f.store
        .snapshots
        .inner
        .advance(
            &scope(),
            MarkerKind::Tag(Tag::new("other").unwrap()),
            version(1),
        )
        .await
        .unwrap();
    f.store.advance_fails.store(true, Ordering::SeqCst);
    *f.store.snapshots.mode.lock().unwrap() = WriteMode::Fail;
    mutate(&f.cache, &group(), None).await.unwrap();
    f.store.advance_fails.store(false, Ordering::SeqCst);
    until(|| matches!(f.cache.marker_recovery_ticket(&group()).as_ref().map(ReplayTicket::work), Some(RecoveryWork::MarkerMutation(work)) if matches!(work.stage(), MarkerMutationStage::Populate { additional, .. } if !additional.is_empty()))).await;
    assert_eq!(
        f.store
            .read(&scope(), &MarkerKind::ClearRemove)
            .await
            .unwrap(),
        Some(version(10))
    );
    f.clock.set(time(11));
    f.store.advance_fails.store(true, Ordering::SeqCst);
    mutate(&f.cache, &group(), None).await.unwrap();
    f.store.advance_fails.store(false, Ordering::SeqCst);
    *f.store.snapshots.mode.lock().unwrap() = WriteMode::Pass;
    until(|| f.cache.pending_recovery() == 0).await;
    assert!(f.bus.commands.lock().unwrap().iter().any(|command| matches!(command, BackplaneCommand::Marker(marker) if *marker.marker().kind() == MarkerKind::ClearRemove && marker.marker().version() >= version(10))), "newer tag work must retain the prior global clear notification");
    assert!(
        f.store
            .snapshots
            .inner
            .read_snapshot(&scope(), &MarkerKind::ClearRemove, time(11), token())
            .await
            .unwrap()
            .snapshot()
            .is_some(),
        "committed compacted clear population must survive tag work replacement"
    );
    f.cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn notification_replacement_retains_compaction_with_one_queue_slot() {
    let f = fixture_with_limit(
        RecoveryConfig {
            max_items: Some(1),
            delay: Duration::from_millis(200),
            ..recovery()
        },
        1,
    );
    f.store
        .snapshots
        .inner
        .advance(
            &scope(),
            MarkerKind::Tag(Tag::new("other").unwrap()),
            version(1),
        )
        .await
        .unwrap();
    f.store.advance_fails.store(true, Ordering::SeqCst);
    *f.store.snapshots.mode.lock().unwrap() = WriteMode::Fail;
    mutate(&f.cache, &group(), None).await.unwrap();
    f.store.advance_fails.store(false, Ordering::SeqCst);
    until(|| {
        matches!(f.cache.marker_recovery_ticket(&group()).as_ref().map(ReplayTicket::work),
        Some(RecoveryWork::MarkerMutation(work)) if matches!(work.stage(),
            MarkerMutationStage::Populate { additional, .. } if !additional.is_empty()))
    })
    .await;

    f.clock.set(time(11));
    f.bus.fails.store(true, Ordering::SeqCst);
    *f.store.snapshots.mode.lock().unwrap() = WriteMode::Pass;
    let report = mutate(&f.cache, &group(), None).await.unwrap();
    assert!(matches!(
        report.backplane,
        EffectOutcome::RecoveryQueued { .. }
    ));
    assert_eq!(
        f.cache.pending_recovery(),
        1,
        "both obligations fit the one-slot bound"
    );
    let ticket = f.cache.marker_recovery_ticket(&group()).unwrap();
    let RecoveryWork::MarkerMutation(work) = ticket.work() else {
        panic!("notification replacement discarded the prior committed global clear");
    };
    assert!(matches!(work.stage(), MarkerMutationStage::Notify { .. }));
    let child = work
        .pending_compaction()
        .expect("the committed clear must survive replacement");
    assert_eq!(*child.command().marker().kind(), MarkerKind::ClearRemove);
    assert!(matches!(
        child.stage(),
        MarkerMutationStage::Populate { .. }
    ));
    assert!(
        child.pending_compaction().is_none(),
        "inherited debt stays flat and bounded"
    );

    until(|| {
        matches!(f.cache.marker_recovery_ticket(&group()).as_ref().map(ReplayTicket::work),
        Some(RecoveryWork::MarkerMutation(work)) if work.pending_compaction().is_some_and(|child|
            matches!(child.stage(), MarkerMutationStage::Notify { .. })))
    })
    .await;
    let clear_writes = f
        .store
        .snapshots
        .writes
        .lock()
        .unwrap()
        .iter()
        .filter(|(kind, _)| *kind == MarkerKind::ClearRemove)
        .count();
    assert_eq!(clear_writes, 1);
    f.bus.fails.store(false, Ordering::SeqCst);
    until(|| f.cache.pending_recovery() == 0).await;
    assert_eq!(
        f.store
            .snapshots
            .writes
            .lock()
            .unwrap()
            .iter()
            .filter(|(kind, _)| *kind == MarkerKind::ClearRemove)
            .count(),
        clear_writes,
        "a failed inherited publication must not repeat successful population"
    );
    assert!(
        f.bus
            .commands
            .lock()
            .unwrap()
            .iter()
            .any(|command| matches!(command,
        BackplaneCommand::Marker(marker) if *marker.marker().kind() == MarkerKind::ClearRemove
            && marker.marker().version() == version(10)))
    );
    let clear = f
        .store
        .snapshots
        .inner
        .read_snapshot(&scope(), &MarkerKind::ClearRemove, time(11), token())
        .await
        .unwrap()
        .snapshot()
        .unwrap();
    assert_eq!(clear.created(), time(10));
    assert_eq!(clear.logical_expiration(), time(15));
    assert_eq!(clear.physical_expiration(), time(15));
    f.cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn strict_population_replacement_retains_prior_compaction() {
    let f = fixture_with_limit(
        RecoveryConfig {
            max_items: Some(1),
            ..recovery()
        },
        1,
    );
    f.store
        .snapshots
        .inner
        .advance(
            &scope(),
            MarkerKind::Tag(Tag::new("other").unwrap()),
            version(1),
        )
        .await
        .unwrap();
    f.store.advance_fails.store(true, Ordering::SeqCst);
    *f.store.snapshots.mode.lock().unwrap() = WriteMode::Fail;
    mutate(&f.cache, &group(), None).await.unwrap();
    f.store.advance_fails.store(false, Ordering::SeqCst);
    until(|| {
        matches!(f.cache.marker_recovery_ticket(&group()).as_ref().map(ReplayTicket::work),
        Some(RecoveryWork::MarkerMutation(work)) if matches!(work.stage(),
            MarkerMutationStage::Populate { additional, .. } if !additional.is_empty()))
    })
    .await;
    f.clock.set(time(11));
    let result = mutate(
        &f.cache,
        &group(),
        Some(options().with_rethrow_distributed_exceptions(true)),
    )
    .await;
    assert!(matches!(
        result,
        Err(Error::Marker(MarkerError::Backend { .. }))
    ));
    assert_eq!(f.cache.pending_recovery(), 1);
    let ticket = f.cache.marker_recovery_ticket(&group()).unwrap();
    let RecoveryWork::MarkerMutation(work) = ticket.work() else {
        panic!("strict notification must retain prior clear");
    };
    assert!(matches!(work.stage(), MarkerMutationStage::Notify { .. }));
    let clear = work.pending_compaction().unwrap();
    assert_eq!(*clear.command().marker().kind(), MarkerKind::ClearRemove);
    assert!(
        matches!(clear.stage(), MarkerMutationStage::Populate { snapshot, .. } if snapshot.created() == time(10))
    );
    *f.store.snapshots.mode.lock().unwrap() = WriteMode::Pass;
    until(|| f.cache.pending_recovery() == 0).await;
    assert!(
        f.bus
            .commands
            .lock()
            .unwrap()
            .iter()
            .any(|command| matches!(command,
        BackplaneCommand::Marker(marker) if *marker.marker().kind() == MarkerKind::ClearRemove))
    );
    f.cache.shutdown().await.unwrap();
}
