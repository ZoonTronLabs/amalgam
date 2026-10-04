//! Native protocol acceptance. Cache orchestration is tested in the audit gates.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use amalgam::entry::Entry;
use amalgam::tags::{TagRegistry, TagVerdict, try_collect_tags};
use amalgam::{
    AcquisitionPolicy, AutoRecoveryService, Backplane, BackplaneAction, BackplaneCommand,
    BackplaneMessage, BackplaneState, CacheScope, DataMutation, DistributedCache, DistributedEntry,
    DistributedLocker, DistributedSerializer, DistributedSnapshot, EnqueueOutcome, EntryOptions,
    EntryWeight, Error, InMemoryDistributedCache, InMemoryDistributedLocker,
    InMemoryInvalidationStore, InProcessBackplane, InvalidationStore, JsonSerializer,
    KeyModifierMode, LeaseError, LeaseSupport, LeaseToken, LeaseTtl, LeasedMutation,
    LeasedWriteOutcome, ManualClock, MarkerAdvanceOutcome, MarkerCommand, MarkerError, MarkerKind,
    MarkerStoreLimits, MarkerVersion, OperationGeneration, PendingMutation, Priority,
    RecoveryAction, RecoveryConfig, RecoveryExecutor, RecoveryFence, RecoveryItem, RecoveryStart,
    RecoveryWork, RemoveByTagBehavior, Result, SnapshotRetention, StoredMarker, SupersedeOutcome,
    SystemClock, Tag, TagError, Timeout, Timestamp, TokenAcquisition, acquire_owned,
};
use async_trait::async_trait;
use tokio::sync::{Notify, Semaphore};

fn at(ticks: i64) -> Timestamp {
    Timestamp::from_ticks(ticks)
}
fn scope(prefix: &str, version: &str, mode: KeyModifierMode) -> CacheScope {
    CacheScope::new(prefix, version, mode).unwrap()
}
fn item(ticks: i64) -> RecoveryItem {
    RecoveryItem {
        key: "key".into(),
        action: RecoveryAction::Set,
        timestamp: at(ticks),
        expires_at: Timestamp::MAX,
        remaining_retries: None,
    }
}
fn work(ticks: i64) -> RecoveryWork {
    RecoveryWork::Data {
        item: item(ticks),
        mutation: PendingMutation::Commit {
            mutation: DataMutation::Set {
                bytes: Arc::from([ticks as u8]),
                physical_expiration: Timestamp::MAX,
            },
            notification: None,
        },
    }
}

#[test]
fn tags_reject_blank_at_every_typed_boundary_and_include_minimum_revision() {
    assert!(matches!(Tag::new("\t "), Err(TagError::Blank)));
    assert!(serde_json::from_str::<Tag>("\" \"").is_err());
    assert!(try_collect_tags(["valid", ""]).is_err());
    let registry = TagRegistry::new();
    assert_eq!(
        registry.evaluate(Timestamp::MIN, &[], RemoveByTagBehavior::Expire),
        TagVerdict::Valid
    );
    registry.mark_clear_remove(Timestamp::MIN);
    assert_eq!(
        registry.evaluate(Timestamp::MIN, &[], RemoveByTagBehavior::Expire),
        TagVerdict::Remove
    );
}

#[tokio::test]
async fn markers_share_effective_namespace_and_compact_without_forgetting_a_tombstone() {
    let store = Arc::new(InMemoryInvalidationStore::new(
        MarkerStoreLimits::new(2, 2).unwrap(),
    ));
    let a = scope("v2:tenant:", "ignored", KeyModifierMode::None);
    let same = scope("tenant:", "v2", KeyModifierMode::Prefix);
    let other = scope("tenant:", "v2", KeyModifierMode::Suffix);
    assert_eq!(a, same);
    assert_ne!(a, other);
    let tag = MarkerKind::Tag(Tag::new("group").unwrap());
    let mut tasks = Vec::new();
    for ticks in [i64::MIN, 9_007_199_254_740_993, i64::MAX, 0, -1] {
        let store = store.clone();
        let namespace = a.clone();
        let kind = tag.clone();
        tasks.push(tokio::spawn(async move {
            store
                .advance(&namespace, kind, MarkerVersion::new(at(ticks)))
                .await
                .unwrap()
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    assert_eq!(
        store.read(&same, &tag).await.unwrap(),
        Some(MarkerVersion::new(Timestamp::MAX))
    );
    assert_eq!(store.read(&other, &tag).await.unwrap(), None);
    store
        .advance(
            &a,
            MarkerKind::Tag(Tag::new("second").unwrap()),
            MarkerVersion::new(at(3)),
        )
        .await
        .unwrap();
    let outcome = store
        .advance(
            &a,
            MarkerKind::Tag(Tag::new("third").unwrap()),
            MarkerVersion::new(at(5)),
        )
        .await
        .unwrap();
    assert!(
        matches!(outcome, MarkerAdvanceOutcome::Compacted { clear_remove, .. } if clear_remove.timestamp() == Timestamp::MAX)
    );
    assert_eq!(
        store.read(&same, &MarkerKind::ClearRemove).await.unwrap(),
        Some(MarkerVersion::new(Timestamp::MAX))
    );
    store
        .advance(&other, MarkerKind::ClearExpire, MarkerVersion::new(at(1)))
        .await
        .unwrap();
    assert!(matches!(
        store
            .advance(
                &scope("third", "v2", KeyModifierMode::None),
                MarkerKind::ClearExpire,
                MarkerVersion::new(at(1))
            )
            .await,
        Err(MarkerError::ScopeCapacity { limit: 2 })
    ));
}

#[tokio::test]
async fn typed_controls_round_trip_without_reinterpreting_reserved_ordinary_data() {
    let backplane = InProcessBackplane::default();
    let mut receiver = backplane.subscribe();
    let mut health = backplane.connection_state().unwrap();
    assert!(matches!(*health.borrow(), BackplaneState::Connected { .. }));
    assert!(
        tokio::time::timeout(Duration::from_millis(10), health.changed())
            .await
            .is_err(),
        "live health stream must retain its sender"
    );
    let source = "\u{1f}amalgam-control-v2:source|with|delimiters";
    let data = BackplaneMessage {
        source_id: source.into(),
        key: "__amalgam:clear:remove".into(),
        timestamp: Timestamp::MIN,
        action: BackplaneAction::Set,
    };
    backplane
        .publish_command(BackplaneCommand::Data(data.clone()))
        .await
        .unwrap();
    let BackplaneCommand::Data(decoded) =
        BackplaneCommand::from_message(receiver.recv().await.unwrap()).unwrap()
    else {
        panic!("ordinary data became a marker")
    };
    assert_eq!(decoded.source_id, data.source_id);
    assert_eq!(decoded.key, data.key);
    let command = MarkerCommand::new(
        source,
        scope("tenant|:", "v2", KeyModifierMode::Prefix),
        StoredMarker::new(
            MarkerKind::Tag(Tag::new("tag|:").unwrap()),
            MarkerVersion::new(Timestamp::MAX),
        ),
    )
    .unwrap();
    backplane
        .publish_command(BackplaneCommand::Marker(command.clone()))
        .await
        .unwrap();
    let BackplaneCommand::Marker(decoded) =
        BackplaneCommand::from_message(receiver.recv().await.unwrap()).unwrap()
    else {
        panic!("marker became data")
    };
    assert_eq!(decoded.scope(), command.scope());
    assert_eq!(decoded.marker(), command.marker());
    let mut corrupted = BackplaneCommand::Marker(command).into_message().unwrap();
    corrupted.key = "another namespace".into();
    assert!(BackplaneCommand::from_message(corrupted).is_err());
}

fn snapshot(codec: &dyn DistributedSerializer<i32>) {
    let options = EntryOptions::new(Duration::from_secs(1))
        .with_distributed_duration(Duration::from_secs(10))
        .with_size(7)
        .with_priority(Priority::High);
    let inserted = at(100);
    let entry = Entry::fresh_at(42, &options, at(20), inserted, Box::new([]), None, None);
    let source = DistributedSnapshot::from_entry_with_options(&entry, &options, inserted).unwrap();
    let raw = codec.serialize(source.entry()).unwrap();
    let legacy = codec.deserialize_snapshot(&raw).unwrap();
    assert_eq!(legacy.retention(), SnapshotRetention::Unspecified);
    let bytes = codec.serialize_snapshot(&source).unwrap();
    assert!(bytes.starts_with(b"AMALGAM\0"));
    let roundtrip = codec.deserialize_snapshot(&bytes).unwrap();
    assert_eq!(roundtrip.inserted_at(), inserted);
    assert_eq!(
        roundtrip.retention(),
        SnapshotRetention::Specified {
            size: Some(EntryWeight::new(7)),
            priority: Priority::High
        }
    );
    let hydrated_at = inserted.saturating_add(Duration::from_secs(2));
    let hydrated = roundtrip
        .for_memory_hydration(
            &EntryOptions::new(Duration::from_secs(1)).with_priority(Priority::Low),
            hydrated_at,
        )
        .unwrap()
        .unwrap();
    assert_eq!(hydrated.value(), &42);
    assert_eq!(hydrated.meta().created(), at(20));
    assert_eq!(
        hydrated.meta().logical_expiration(),
        hydrated_at.saturating_add(Duration::from_secs(1))
    );
    assert_eq!(hydrated.meta().priority(), Priority::High);
    assert_eq!(hydrated.meta().size(), Some(EntryWeight::new(7)));
    assert_eq!(source.backend_ttl_at(hydrated_at), Duration::from_secs(8));
    let mut unknown = bytes.clone();
    unknown[8] = 99;
    assert!(matches!(
        codec.deserialize_snapshot(&unknown),
        Err(Error::Deserialization(_))
    ));
    assert!(codec.deserialize_snapshot(&bytes[..12]).is_err());
    let mut corrupt = bytes;
    corrupt[13] = 0xff;
    assert!(codec.deserialize_snapshot(&corrupt).is_err());
}

#[test]
fn v2_snapshot_preserves_legacy_json_and_independent_l2_metadata() {
    snapshot(&JsonSerializer);
}
#[cfg(feature = "messagepack")]
#[test]
fn v2_snapshot_preserves_legacy_positional_messagepack() {
    snapshot(&amalgam::MessagePackSerializer);
}
#[cfg(feature = "postcard")]
#[test]
fn v2_snapshot_preserves_legacy_positional_postcard() {
    snapshot(&amalgam::PostcardSerializer);
}

struct Fence {
    generation: OperationGeneration,
    current: Arc<AtomicU64>,
    pin: Arc<()>,
}
impl RecoveryFence for Fence {
    fn generation(&self) -> OperationGeneration {
        self.generation
    }
    fn is_current(&self) -> bool {
        let _pin = &self.pin;
        self.current.load(Ordering::SeqCst) == self.generation.value()
    }
}
fn fence(generation: u64, current: Arc<AtomicU64>, pin: Arc<()>) -> Arc<dyn RecoveryFence> {
    Arc::new(Fence {
        generation: OperationGeneration::new(generation),
        current,
        pin,
    })
}
struct Gate {
    entered: Notify,
    release: Semaphore,
    calls: AtomicUsize,
    fail: AtomicBool,
}
impl Gate {
    fn new() -> Self {
        Self {
            entered: Notify::new(),
            release: Semaphore::new(0),
            calls: AtomicUsize::new(0),
            fail: AtomicBool::new(false),
        }
    }
}
#[async_trait]
impl RecoveryExecutor for Gate {
    async fn replay(&self, _item: &RecoveryItem) -> Result<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_one();
        self.release.acquire().await.unwrap().forget();
        if self.fail.load(Ordering::SeqCst) {
            Err(Error::Distributed("fixture failure".into()))
        } else {
            Ok(())
        }
    }
}

async fn replacement_survives_old_completion(fail_old: bool) {
    let queue = AutoRecoveryService::try_new(
        RecoveryConfig {
            max_retries: Some(0),
            ..RecoveryConfig::default()
        },
        Arc::new(ManualClock::new(at(0))),
    )
    .unwrap();
    let current = Arc::new(AtomicU64::new(1));
    let pin = Arc::new(());
    let weak = Arc::downgrade(&pin);
    queue
        .enqueue_versioned(work(10), fence(1, current.clone(), pin.clone()))
        .unwrap();
    let old = queue.snapshot("key").unwrap();
    drop(pin);
    assert!(
        weak.upgrade().is_some(),
        "queued ticket must pin its original lane"
    );
    let gate = Arc::new(Gate::new());
    gate.fail.store(fail_old, Ordering::SeqCst);
    let drain = {
        let queue = queue.clone();
        let gate = gate.clone();
        tokio::spawn(async move { queue.drain_once(gate.as_ref()).await })
    };
    gate.entered.notified().await;
    current.store(2, Ordering::SeqCst);
    assert!(
        matches!(
            queue
                .enqueue_versioned(work(10), fence(2, current.clone(), Arc::new(())))
                .unwrap(),
            EnqueueOutcome::Replaced(_)
        ),
        "equal timestamp must not hide a newer generation"
    );
    gate.release.add_permits(1);
    drain.await.unwrap();
    let newest = queue.snapshot("key").unwrap();
    assert_eq!(newest.generation(), OperationGeneration::new(2));
    assert!(!queue.is_current(&old));
    assert_eq!(queue.complete(&old), SupersedeOutcome::Unchanged);
    assert_eq!(
        queue.supersede_through("key", OperationGeneration::new(1)),
        SupersedeOutcome::Unchanged
    );
    assert_eq!(
        queue.supersede_through("key", OperationGeneration::new(2)),
        SupersedeOutcome::Removed
    );
    drop(old);
    assert!(weak.upgrade().is_none());
}

#[tokio::test]
async fn exact_recovery_completion_preserves_replacement_and_strong_lane_pin() {
    replacement_survives_old_completion(false).await;
}
#[tokio::test]
async fn exact_recovery_failure_does_not_consume_replacement_retry_budget() {
    replacement_survives_old_completion(true).await;
}

#[tokio::test]
async fn cancelled_recovery_claim_returns_pending_and_concurrent_drains_do_not_duplicate() {
    let queue =
        AutoRecoveryService::new(RecoveryConfig::default(), Arc::new(ManualClock::new(at(0))));
    queue.enqueue(item(1));
    let gate = Arc::new(Gate::new());
    let drain = {
        let queue = queue.clone();
        let gate = gate.clone();
        tokio::spawn(async move { queue.drain_once(gate.as_ref()).await })
    };
    gate.entered.notified().await;
    queue.drain_once(gate.as_ref()).await;
    assert_eq!(gate.calls.load(Ordering::SeqCst), 1);
    drain.abort();
    assert!(drain.await.unwrap_err().is_cancelled());
    gate.release.add_permits(1);
    queue.drain_once(gate.as_ref()).await;
    assert_eq!(gate.calls.load(Ordering::SeqCst), 2);
    assert!(queue.is_empty());
}

#[tokio::test]
async fn recovery_reconnect_barrier_uses_real_time_and_shutdown_releases_pins() {
    let clock = Arc::new(ManualClock::new(at(0)));
    let queue = AutoRecoveryService::new(
        RecoveryConfig {
            delay: Duration::from_millis(20),
            ..RecoveryConfig::default()
        },
        clock.clone(),
    );
    let gate = Arc::new(Gate::new());
    gate.release.add_permits(2);
    let executor: Arc<dyn RecoveryExecutor> = gate.clone();
    queue.try_set_executor(Arc::downgrade(&executor)).unwrap();
    queue.enqueue(item(1));
    queue.suspend();
    queue.drain_once(gate.as_ref()).await;
    assert_eq!(gate.calls.load(Ordering::SeqCst), 0);
    clock.advance(Duration::from_secs(100));
    queue.drain_once(gate.as_ref()).await;
    assert_eq!(
        gate.calls.load(Ordering::SeqCst),
        0,
        "domain clock cannot end an I/O continuity gap"
    );
    queue.pause_after_reconnect().unwrap();
    queue.drain_once(gate.as_ref()).await;
    assert_eq!(gate.calls.load(Ordering::SeqCst), 0);
    tokio::time::sleep(Duration::from_millis(30)).await;
    queue.drain_once(gate.as_ref()).await;
    assert_eq!(gate.calls.load(Ordering::SeqCst), 1);
    assert_eq!(queue.try_spawn().unwrap(), RecoveryStart::Started);
    assert_eq!(queue.try_spawn().unwrap(), RecoveryStart::AlreadyRunning);
    queue.shutdown().await.unwrap();
    queue.shutdown().await.unwrap();
    assert!(matches!(
        queue.try_spawn(),
        Err(amalgam::RecoveryError::Stopped)
    ));
    assert!(matches!(
        OperationGeneration::new(u64::MAX).next(),
        Err(amalgam::RecoveryError::GenerationExhausted)
    ));
}

#[tokio::test]
async fn reference_lease_deadline_never_grants_a_late_expired_lease() {
    let locker = InMemoryDistributedLocker::new(Arc::new(SystemClock));
    let first = locker
        .acquire(
            "key",
            Duration::from_millis(5),
            Timeout::After(Duration::ZERO),
        )
        .await
        .unwrap()
        .unwrap();
    let started = tokio::time::Instant::now();
    assert!(
        locker
            .acquire(
                "key",
                Duration::from_secs(1),
                Timeout::After(Duration::from_millis(1))
            )
            .await
            .unwrap()
            .is_none()
    );
    assert!(started.elapsed() < Duration::from_millis(50));
    locker.release("key", &first).await.unwrap();
}

#[tokio::test]
async fn atomic_reference_fence_rejects_expired_and_replaced_owners() {
    let clock = Arc::new(ManualClock::new(at(0)));
    let locker = Arc::new(InMemoryDistributedLocker::new(clock.clone()));
    let backend = InMemoryDistributedCache::new(clock.clone());
    let lease = acquire_owned(
        locker.clone(),
        "key".into(),
        LeaseTtl::new(Duration::from_secs(1)).unwrap(),
        Timeout::Infinite,
        AcquisitionPolicy::TokenOwned,
    )
    .await
    .unwrap()
    .unwrap();
    let proof = lease.proof().unwrap();
    assert_eq!(
        backend
            .write_with_lease(
                "key",
                LeasedMutation::Set {
                    bytes: vec![1],
                    ttl: None
                },
                &proof
            )
            .await
            .unwrap(),
        LeasedWriteOutcome::Committed
    );
    clock.advance(Duration::from_secs(2));
    let replacement = locker
        .acquire(
            "key",
            Duration::from_secs(1),
            Timeout::After(Duration::ZERO),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        backend
            .write_with_lease("key", LeasedMutation::Remove, &proof)
            .await
            .unwrap(),
        LeasedWriteOutcome::LeaseLost
    );
    assert_eq!(backend.get("key").await.unwrap(), Some(vec![1]));
    assert!(matches!(lease.proof(), Err(LeaseError::Lost)));
    lease.release().await.unwrap();
    assert_eq!(
        locker.held_count(),
        1,
        "old release must preserve a replacement token"
    );
    locker.release("key", &replacement).await.unwrap();
}

struct LegacyLocker;
#[async_trait]
impl DistributedLocker for LegacyLocker {
    async fn acquire(&self, _: &str, _: Duration, _: Timeout) -> Result<Option<String>> {
        Ok(Some("legacy".into()))
    }
    async fn release(&self, _: &str, _: &str) -> Result<()> {
        Ok(())
    }
}
#[tokio::test]
async fn legacy_locker_is_source_compatible_without_inventing_a_lifetime() {
    let locker: Arc<dyn DistributedLocker> = Arc::new(LegacyLocker);
    assert_eq!(locker.lease_support(), LeaseSupport::OpaqueLegacy);
    assert!(matches!(
        acquire_owned(
            locker.clone(),
            "key".into(),
            LeaseTtl::new(Duration::from_secs(1)).unwrap(),
            Timeout::Infinite,
            AcquisitionPolicy::TokenOwned
        )
        .await,
        Err(LeaseError::UnsupportedTokenAcquisition)
    ));
    let lease = acquire_owned(
        locker,
        "key".into(),
        LeaseTtl::new(Duration::from_secs(1)).unwrap(),
        Timeout::Infinite,
        AcquisitionPolicy::LegacyBackendContract,
    )
    .await
    .unwrap()
    .unwrap();
    assert!(matches!(lease.proof(), Err(LeaseError::OpaqueLifetime)));
    lease.release().await.unwrap();
}

struct CancelledAcquire {
    token: Mutex<Option<LeaseToken>>,
    entered: Notify,
    released: Notify,
}
#[async_trait]
impl DistributedLocker for CancelledAcquire {
    async fn acquire(&self, _: &str, _: Duration, _: Timeout) -> Result<Option<String>> {
        unreachable!("native token path required")
    }
    async fn release(&self, _: &str, token: &str) -> Result<()> {
        let mut held = self.token.lock().unwrap();
        if held.as_ref().is_some_and(|held| held.as_str() == token) {
            *held = None;
            self.released.notify_one();
        }
        Ok(())
    }
    fn token_acquisition(&self) -> TokenAcquisition {
        TokenAcquisition::CallerSelected
    }
    fn lease_support(&self) -> LeaseSupport {
        LeaseSupport::HeldUntilRelease
    }
    async fn acquire_with_token(
        &self,
        _: &str,
        token: &LeaseToken,
        _: LeaseTtl,
        _: Timeout,
    ) -> std::result::Result<bool, LeaseError> {
        *self.token.lock().unwrap() = Some(token.clone());
        self.entered.notify_one();
        std::future::pending().await
    }
}
#[tokio::test]
async fn cancelled_owned_acquire_releases_the_preselected_token_without_a_reply() {
    let locker = Arc::new(CancelledAcquire {
        token: Mutex::new(None),
        entered: Notify::new(),
        released: Notify::new(),
    });
    let task = {
        let locker = locker.clone();
        tokio::spawn(async move {
            acquire_owned(
                locker,
                "key".into(),
                LeaseTtl::new(Duration::from_secs(10)).unwrap(),
                Timeout::Infinite,
                AcquisitionPolicy::TokenOwned,
            )
            .await
        })
    };
    locker.entered.notified().await;
    task.abort();
    assert!(matches!(task.await, Err(error) if error.is_cancelled()));
    tokio::time::timeout(Duration::from_secs(1), locker.released.notified())
        .await
        .unwrap();
    assert!(locker.token.lock().unwrap().is_none());
}

#[test]
fn invalid_distributed_payloads_are_rejected_before_hydration() {
    let mut dto = DistributedEntry {
        value: 1,
        created_ticks: 0,
        logical_expiration_ticks: 2,
        physical_expiration_ticks: 1,
        is_from_fail_safe: false,
        etag: None,
        last_modified_ticks: None,
        tags: Vec::new(),
    };
    assert!(dto.clone().try_into_entry(at(0)).is_err());
    dto.logical_expiration_ticks = 1;
    dto.tags.push(" ".into());
    assert!(matches!(
        dto.try_into_entry(at(0)),
        Err(Error::Tag(TagError::Blank))
    ));
}

#[test]
fn typed_module_wrappers_preserve_backend_sources_and_expected_rejections() {
    use amalgam::{OperationOutcome, RecoveryError};
    use std::error::Error as _;
    let marker: Error = MarkerError::backend(std::io::Error::new(
        std::io::ErrorKind::ConnectionReset,
        "marker socket reset",
    ))
    .into();
    assert_eq!(
        OperationOutcome::from_error(&marker),
        OperationOutcome::DistributedError
    );
    assert_eq!(
        marker
            .source()
            .unwrap()
            .downcast_ref::<std::io::Error>()
            .unwrap()
            .kind(),
        std::io::ErrorKind::ConnectionReset
    );
    let lease: Error = LeaseError::backend(std::io::Error::new(
        std::io::ErrorKind::ConnectionRefused,
        "lease socket refused",
    ))
    .into();
    assert_eq!(
        OperationOutcome::from_error(&lease),
        OperationOutcome::LockError
    );
    assert_eq!(
        lease
            .source()
            .unwrap()
            .downcast_ref::<std::io::Error>()
            .unwrap()
            .kind(),
        std::io::ErrorKind::ConnectionRefused
    );
    for rejected in [
        Error::Marker(MarkerError::ScopeCapacity { limit: 1 }),
        Error::Recovery(RecoveryError::Stopped),
        Error::Recovery(RecoveryError::GenerationExhausted),
    ] {
        assert_eq!(
            OperationOutcome::from_error(&rejected),
            OperationOutcome::Rejected
        );
    }
    assert_eq!(OperationOutcome::Rejected.as_str(), "rejected");
}

#[tokio::test]
async fn owned_worker_task_failures_distinguish_cancellation_and_contract_panic() {
    use amalgam::{OperationOutcome, RecoveryError};
    let cancelled = tokio::spawn(std::future::pending::<()>());
    cancelled.abort();
    let error: Error = RecoveryError::Task {
        source: cancelled.await.unwrap_err(),
    }
    .into();
    assert_eq!(
        OperationOutcome::from_error(&error),
        OperationOutcome::Cancelled
    );
    let panicked = tokio::spawn(async { panic!("worker contract violation fixture") });
    let error: Error = LeaseError::Task {
        source: panicked.await.unwrap_err(),
    }
    .into();
    assert_eq!(
        OperationOutcome::from_error(&error),
        OperationOutcome::Panicked
    );
}
