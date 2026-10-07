//! Marker storage uses an independent, externally owned public-protocol provider.
use amalgam::advanced::*;
use amalgam::provider::*;
use amalgam::*;
use std::future::IntoFuture;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::Duration;
#[path = "support/memory_storage_fixture.rs"]
mod storage_fixture;
use storage_fixture::{ClearGate, Fault, Limit, MapStorage};

fn options() -> EntryOptions {
    EntryOptions::new(Duration::from_secs(120))
}
fn tag() -> Tag {
    Tag::new("group:/тег").unwrap()
}
fn node(
    store: &Arc<MapStorage<MarkerObservation>>,
    clock: &Arc<ManualClock>,
    prefix: &str,
) -> Cache<u64> {
    Cache::builder()
        .clock(clock.clone())
        .key_prefix(prefix)
        .default_options(options())
        .marker_read_policy(MarkerReadPolicy::OptionsControlled)
        .marker_memory_storage(store.clone())
        .try_build()
        .unwrap()
}
async fn tagged(c: &Cache<u64>, value: u64) {
    c.set("key", value)
        .tags([tag()])
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
}
fn cause(error: &Error) -> Fault {
    let Error::MarkerMemoryStorage(MemoryStorageError::Provider { source }) = error else {
        panic!("unexpected marker error: {error:?}");
    };
    *source.downcast_ref::<Fault>().unwrap()
}

#[tokio::test]
async fn actual_records_include_local_authority_independent_metadata_and_ready_reads() {
    let store = MapStorage::new();
    let clock = Arc::new(ManualClock::default());
    let c: Cache<u64> = Cache::builder()
        .clock(clock.clone())
        .key_prefix("raw:/λ:")
        .default_options(options())
        .marker_read_policy(MarkerReadPolicy::OptionsControlled)
        .marker_memory_storage(store.clone())
        .tags_default_options(options().with_size(11).with_priority(Priority::High))
        .try_build()
        .unwrap();
    let provider: Arc<dyn MemoryStorage<MarkerObservation>> = store.clone();
    assert!(Arc::ptr_eq(c.marker_memory_storage().unwrap(), &provider));
    tagged(&c, 7).await;
    assert_eq!(c.read("key", None).await.unwrap().into_value(), Some(7));
    {
        let state = store.state.lock().unwrap();
        assert_eq!(state.records.len(), 3);
        for record in state.records.values() {
            assert!(record.key().contains("/local/"));
            assert_eq!(
                *record.entry().value(),
                MarkerObservation::Local(MarkerPresence::Absent)
            );
            assert_eq!(record.entry().meta().created(), clock.now());
            assert_eq!(record.weight(), 11);
            assert_eq!(record.entry().meta().priority(), Priority::High);
            assert!(record.entry().meta().tags().is_empty());
        }
        assert!(state.epochs.keys().any(|n| n.purpose()
            == MemoryNamespacePurpose::LocalMarkerObservations
            && n.key_prefix() == "raw:/λ:"));
    }
    let gets = store.lookups.load(Ordering::SeqCst);
    let ready = store.ready.load(Ordering::SeqCst);
    assert_eq!(
        c.get_or_set::<_, _>(
            "key",
            typed_factory(|_| forbidden_factory("hot value ran origin"))
        )
        .await
        .unwrap(),
        7
    );
    assert_eq!(store.lookups.load(Ordering::SeqCst), gets);
    assert!(store.ready.load(Ordering::SeqCst) >= ready + 3);
    assert_eq!(c.marker_memory_usage().unwrap().unwrap().weight, 33);
    c.shutdown().await.unwrap();
}

#[tokio::test]
async fn shared_local_tag_fact_is_accepted_before_a_ready_value_is_returned() {
    let store = MapStorage::new();
    let clock = Arc::new(ManualClock::default());
    let a = node(&store, &clock, "shared:");
    let b = node(&store, &clock, "shared:");
    tagged(&b, 13).await;
    assert_eq!(b.read("key", None).await.unwrap().into_value(), Some(13));
    clock.advance(Duration::from_millis(1));
    a.remove_by_tag(tag())
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let runs = calls.clone();
    assert_eq!(
        b.get_or_set(
            "key",
            typed_factory(move |ctx| async move {
                runs.fetch_add(1, Ordering::SeqCst);
                Ok::<_, amalgam::FactoryError>(ctx.value(17))
            })
        )
        .tags([tag()])
        .await
        .unwrap(),
        17
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[tokio::test]
async fn shared_remove_and_expire_clear_reach_independent_value_stores() {
    for mode in [ClearMode::Remove, ClearMode::Expire] {
        let store = MapStorage::new();
        let clock = Arc::new(ManualClock::default());
        let a = node(&store, &clock, "same:");
        let b = node(&store, &clock, "same:");
        b.set("key", 19)
            .with_receipt()
            .await
            .unwrap()
            .wait()
            .await
            .unwrap();
        assert_eq!(b.read("key", None).await.unwrap().into_value(), Some(19));
        clock.advance(Duration::from_millis(1));
        a.clear(mode)
            .with_receipt()
            .await
            .unwrap()
            .wait()
            .await
            .unwrap();
        assert!(!b.read("key", None).await.unwrap().has_value(), "{mode:?}");
        a.shutdown().await.unwrap();
        b.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn a_foreign_fact_remains_a_host_boundary_after_provider_eviction() {
    let store = MapStorage::new();
    let values = MapStorage::new();
    let clock = Arc::new(ManualClock::default());
    let a = node(&store, &clock, "");
    let b: Cache<u64> = Cache::builder()
        .clock(clock.clone())
        .memory_storage(values.clone())
        .default_options(options())
        .marker_read_policy(MarkerReadPolicy::OptionsControlled)
        .marker_memory_storage(store.clone())
        .try_build()
        .unwrap();
    tagged(&b, 23).await;
    let original = values.state.lock().unwrap().records["key"].clone();
    assert!(b.read("key", None).await.unwrap().has_value());
    clock.advance(Duration::from_millis(1));
    a.remove_by_tag(tag())
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(!b.read("key", None).await.unwrap().has_value());
    store.state.lock().unwrap().records.clear();
    values
        .state
        .lock()
        .unwrap()
        .records
        .insert(Arc::from("key"), original);
    assert!(!b.read("key", None).await.unwrap().has_value());
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[tokio::test]
async fn distinct_prefixes_and_local_durable_authorities_do_not_share_facts() {
    let store = MapStorage::new();
    let clock = Arc::new(ManualClock::default());
    let a = node(&store, &clock, "a:/λ");
    let b = node(&store, &clock, "a:/λ/");
    let durable: Cache<u64> = Cache::builder()
        .clock(clock.clone())
        .key_prefix("a:/λ")
        .default_options(options())
        .marker_read_policy(MarkerReadPolicy::OptionsControlled)
        .marker_memory_storage(store.clone())
        .invalidation_store(Arc::new(InMemoryInvalidationStore::default()))
        .try_build()
        .unwrap();
    tagged(&b, 29).await;
    tagged(&durable, 31).await;
    assert!(b.read("key", None).await.unwrap().has_value());
    assert!(durable.read("key", None).await.unwrap().has_value());
    clock.advance(Duration::from_millis(1));
    a.remove_by_tag(tag())
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert_eq!(b.read("key", None).await.unwrap().into_value(), Some(29));
    assert_eq!(
        durable.read("key", None).await.unwrap().into_value(),
        Some(31)
    );
    {
        let state = store.state.lock().unwrap();
        assert!(state.records.keys().any(|key| key.contains("/local/")));
        assert!(state.records.keys().any(|key| key.contains("/durable/")));
        assert!(
            state
                .epochs
                .keys()
                .any(|n| n.purpose() == MemoryNamespacePurpose::DurableMarkerObservations)
        );
    }
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
    durable.shutdown().await.unwrap();
}

#[tokio::test]
async fn durable_wire_scopes_are_isolated_in_one_observation_provider() {
    let store = MapStorage::new();
    let clock = Arc::new(ManualClock::default());
    let journal = Arc::new(InMemoryInvalidationStore::default());
    let mut caches = Vec::with_capacity(3);
    for (version, mode) in [
        ("v2", KeyModifierMode::Prefix),
        ("v3", KeyModifierMode::Prefix),
        ("v2", KeyModifierMode::Suffix),
    ] {
        caches.push(
            Cache::<u64>::builder()
                .clock(clock.clone())
                .key_prefix("scope:")
                .default_options(options())
                .distributed_wire_version(version)
                .distributed_key_modifier_mode(mode)
                .invalidation_store(journal.clone())
                .marker_read_policy(MarkerReadPolicy::OptionsControlled)
                .marker_memory_storage(store.clone())
                .try_build()
                .unwrap(),
        );
    }
    for c in &caches {
        tagged(c, 37).await;
        assert!(c.read("key", None).await.unwrap().has_value());
    }
    clock.advance(Duration::from_millis(1));
    caches[0]
        .remove_by_tag(tag())
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(!caches[0].read("key", None).await.unwrap().has_value());
    for c in &caches[1..] {
        assert_eq!(c.read("key", None).await.unwrap().into_value(), Some(37));
    }
    assert_eq!(store.state.lock().unwrap().records.len(), 9);
    for c in caches {
        c.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn marker_lookup_and_admission_failures_keep_causes_and_do_not_run_origin() {
    for fault in [Fault::Get, Fault::Insert] {
        let store = MapStorage::new();
        let clock = Arc::new(ManualClock::default());
        let c = node(&store, &clock, "");
        c.set("key", 41)
            .with_receipt()
            .await
            .unwrap()
            .wait()
            .await
            .unwrap();
        *store.fault.lock().unwrap() = Some(fault);
        let called = Arc::new(AtomicUsize::new(0));
        let runs = called.clone();
        let error = c
            .get_or_set::<_, _>(
                "key",
                typed_factory(move |ctx| async move {
                    runs.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, amalgam::FactoryError>(ctx.value(43))
                }),
            )
            .await
            .unwrap_err();
        assert_eq!(cause(&error), fault);
        assert_eq!(called.load(Ordering::SeqCst), 0);
        *store.fault.lock().unwrap() = None;
        c.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn marker_maintenance_and_usage_keep_their_original_channels() {
    let store = MapStorage::new();
    let clock = Arc::new(ManualClock::default());
    let c = node(&store, &clock, "");
    c.set("key", 47)
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(c.read("key", None).await.unwrap().has_value());
    *store.fault.lock().unwrap() = Some(Fault::Maintain);
    assert_eq!(
        cause(&c.run_pending_tasks().await.unwrap_err()),
        Fault::Maintain
    );
    *store.fault.lock().unwrap() = Some(Fault::Usage);
    assert_eq!(cause(&c.marker_memory_usage().unwrap_err()), Fault::Usage);
    *store.fault.lock().unwrap() = None;
    c.shutdown().await.unwrap();
}

#[tokio::test]
async fn wrong_key_and_wrong_epoch_are_explicit_provider_contract_failures() {
    for violation in [MemoryRecordViolation::Key, MemoryRecordViolation::Namespace] {
        let store = MapStorage::new();
        let source = MapStorage::new();
        let clock = Arc::new(ManualClock::default());
        let c = node(&store, &clock, "same:");
        let foreign = node(
            &source,
            &clock,
            if violation == MemoryRecordViolation::Key {
                "other:"
            } else {
                "same:"
            },
        );
        c.set("key", 53)
            .with_receipt()
            .await
            .unwrap()
            .wait()
            .await
            .unwrap();
        foreign
            .set("key", 59)
            .with_receipt()
            .await
            .unwrap()
            .wait()
            .await
            .unwrap();
        assert!(c.read("key", None).await.unwrap().has_value());
        assert!(foreign.read("key", None).await.unwrap().has_value());
        let target = store
            .state
            .lock()
            .unwrap()
            .records
            .keys()
            .next()
            .unwrap()
            .clone();
        let record = if violation == MemoryRecordViolation::Namespace {
            source.state.lock().unwrap().records[&target].clone()
        } else {
            source
                .state
                .lock()
                .unwrap()
                .records
                .values()
                .next()
                .unwrap()
                .clone()
        };
        store.state.lock().unwrap().records.insert(target, record);
        assert!(
            matches!(c.read("key", None).await.unwrap_err(), Error::MarkerMemoryStorage(MemoryStorageError::InvalidRecord { violation: got }) if got == violation)
        );
        c.shutdown().await.unwrap();
        foreign.shutdown().await.unwrap();
    }
}

#[test]
fn provider_policy_limits_and_epoch_identity_are_validated_independent_of_setter_order() {
    let provider = MapStorage::<MarkerObservation>::new();
    assert!(matches!(
        Cache::<u64>::builder()
            .marker_memory_storage(provider.clone())
            .try_build()
            .err()
            .unwrap(),
        Error::Config(ConfigError::SuppliedMarkerMemoryRequiresControlledReads)
    ));
    for first in [true, false] {
        let b = Cache::<u64>::builder().marker_read_policy(MarkerReadPolicy::OptionsControlled);
        let b = if first {
            b.marker_memory_storage(provider.clone())
                .marker_read_limits(MemoryLimits::new(Some(3), None))
        } else {
            b.marker_read_limits(MemoryLimits::new(Some(3), None))
                .marker_memory_storage(provider.clone())
        };
        assert!(matches!(
            b.try_build().err().unwrap(),
            Error::Config(ConfigError::SuppliedMemoryWithBuiltinLimits)
        ));
    }
    assert!(
        Cache::<u64>::builder()
            .marker_read_policy(MarkerReadPolicy::OptionsControlled)
            .marker_memory_storage(provider)
            .try_build()
            .is_ok()
    );
}

#[tokio::test]
async fn provider_owned_capacity_and_expiry_do_not_create_a_shadow_observation_store() {
    let store = MapStorage::limited(Limit::Disabled);
    let clock = Arc::new(ManualClock::default());
    let c = node(&store, &clock, "");
    c.set("key", 61)
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(c.read("key", None).await.unwrap().has_value());
    assert_eq!(c.marker_memory_usage().unwrap().unwrap().entries, 0);
    let writes = store.writes.load(Ordering::SeqCst);
    assert!(c.read("key", None).await.unwrap().has_value());
    assert!(store.writes.load(Ordering::SeqCst) > writes);
    c.shutdown().await.unwrap();
    let store = MapStorage::new();
    let c: Cache<u64> = Cache::builder()
        .clock(clock.clone())
        .default_options(options())
        .marker_read_policy(MarkerReadPolicy::OptionsControlled)
        .tags_default_options(EntryOptions::new(Duration::from_secs(1)))
        .marker_memory_storage(store.clone())
        .try_build()
        .unwrap();
    c.set("key", 67)
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(c.read("key", None).await.unwrap().has_value());
    clock.advance(Duration::from_secs(2));
    c.run_pending_tasks().await.unwrap();
    assert_eq!(c.marker_memory_usage().unwrap().unwrap().entries, 0);
    assert_eq!(c.read("key", None).await.unwrap().into_value(), Some(67));
    assert_eq!(c.marker_memory_usage().unwrap().unwrap().entries, 2);
    c.shutdown().await.unwrap();
}

#[tokio::test]
async fn marker_skips_and_zero_factory_budgets_are_independent_of_value_options() {
    let store = MapStorage::new();
    let clock = Arc::new(ManualClock::default());
    let c: Cache<u64> = Cache::builder()
        .clock(clock.clone())
        .default_options(options())
        .marker_read_policy(MarkerReadPolicy::OptionsControlled)
        .tags_default_options(options().with_skip_memory(true, true))
        .marker_memory_storage(store.clone())
        .try_build()
        .unwrap();
    c.set("key", 71)
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(c.read("key", None).await.unwrap().has_value());
    assert_eq!(store.lookups.load(Ordering::SeqCst), 0);
    assert_eq!(store.ready.load(Ordering::SeqCst), 0);
    assert_eq!(store.writes.load(Ordering::SeqCst), 0);
    c.shutdown().await.unwrap();
    let c: Cache<u64> = Cache::builder()
        .clock(clock.clone())
        .default_options(options())
        .marker_read_policy(MarkerReadPolicy::OptionsControlled)
        .tags_default_options(options().with_factory_timeouts(
            Timeout::Infinite,
            Timeout::After(Duration::ZERO),
            false,
        ))
        .marker_memory_storage(store)
        .try_build()
        .unwrap();
    c.set("key", 73)
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(
        matches!(c.read("key", None).await.unwrap_err(), Error::FactoryTimeout { elapsed } if elapsed.is_zero())
    );
    c.shutdown().await.unwrap();
}

#[test]
fn native_and_async_views_share_actual_marker_records_and_provider_lifetime() {
    let store = MapStorage::new();
    let clock = Arc::new(ManualClock::default());
    let c = BlockingCache::<u64>::from_builder(
        Cache::builder()
            .clock(clock.clone())
            .default_options(options())
            .marker_read_policy(MarkerReadPolicy::OptionsControlled)
            .marker_memory_storage(store.clone()),
    )
    .unwrap();
    c.set("key", 79)
        .with_receipt()
        .execute()
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(c.read("key", None).unwrap().into_value(), Some(79));
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    assert_eq!(
        rt.block_on(c.as_async().read("key", None))
            .unwrap()
            .into_value(),
        Some(79)
    );
    assert_eq!(store.state.lock().unwrap().records.len(), 2);
    c.shutdown().unwrap();
    let other = node(&store, &clock, "");
    rt.block_on(other.set("key", 83).with_receipt().into_future())
        .unwrap();
    assert_eq!(
        rt.block_on(other.read("key", None)).unwrap().into_value(),
        Some(83)
    );
    rt.block_on(other.shutdown()).unwrap();
}

#[tokio::test]
async fn eager_local_marker_refresh_is_owned_and_replaces_actual_records() {
    let store = MapStorage::new();
    let clock = Arc::new(ManualClock::default());
    let c: Cache<u64> = Cache::builder()
        .clock(clock.clone())
        .default_options(options())
        .marker_read_policy(MarkerReadPolicy::OptionsControlled)
        .tags_default_options(
            EntryOptions::new(Duration::from_secs(10)).with_eager_refresh(EagerThreshold::new(0.5)),
        )
        .marker_memory_storage(store.clone())
        .try_build()
        .unwrap();
    c.set("key", 89)
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(c.read("key", None).await.unwrap().has_value());
    let created = clock.now();
    clock.advance(Duration::from_secs(6));
    let mut events = c.events().subscribe();
    assert_eq!(c.read("key", None).await.unwrap().into_value(), Some(89));
    c.flush_pending().await.unwrap();
    let mut count = 0;
    while let Ok(event) = events.try_recv() {
        if matches!(event, CacheEvent::MarkerEagerRefresh { .. }) {
            count += 1;
        }
    }
    assert_eq!(count, 2);
    assert!(
        store
            .state
            .lock()
            .unwrap()
            .records
            .values()
            .all(|record| record.entry().meta().created() > created)
    );
    c.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_delayed_lower_admission_accepts_the_concurrent_shared_maximum_before_serving() {
    let store = MapStorage::new();
    let clock = Arc::new(ManualClock::default());
    let a = node(&store, &clock, "cas:");
    let b = node(&store, &clock, "cas:");
    tagged(&a, 97).await;
    assert!(a.read("key", None).await.unwrap().has_value());
    let key = store
        .state
        .lock()
        .unwrap()
        .records
        .keys()
        .find(|key| key.ends_with(&hex(format!("tag:{}", tag().as_str()).as_bytes())))
        .unwrap()
        .clone();
    store.state.lock().unwrap().records.remove(&key);
    let gate = Arc::new(ClearGate {
        entered: Barrier::new(2),
        resume: Barrier::new(2),
    });
    *store.write_gate.lock().unwrap() = Some(gate.clone());
    let reader = a.clone();
    let pending = tokio::spawn(async move { reader.read("key", None).await });
    gate.entered.wait();
    clock.advance(Duration::from_millis(1));
    b.remove_by_tag(tag())
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    gate.resume.wait();
    assert!(!pending.await.unwrap().unwrap().has_value());
    assert!(matches!(
        *store.state.lock().unwrap().records[&key].entry().value(),
        MarkerObservation::KnownMaximum(_)
    ));
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

// A provider may reveal a changed continuity snapshot to an operation before
// its subscription sends a health notification. Keep those watch owners alive.
struct SnapshotBackplane {
    down: std::sync::atomic::AtomicBool,
    frames: tokio::sync::broadcast::Sender<BackplaneMessage>,
    snapshots: Mutex<Vec<tokio::sync::watch::Sender<BackplaneState>>>,
}
impl SnapshotBackplane {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            down: std::sync::atomic::AtomicBool::new(false),
            frames: tokio::sync::broadcast::channel(16).0,
            snapshots: Mutex::new(Vec::new()),
        })
    }
}
#[async_trait::async_trait]
impl Backplane for SnapshotBackplane {
    async fn publish(&self, message: BackplaneMessage) -> Result<()> {
        let _ = self.frames.send(message);
        Ok(())
    }
    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<BackplaneMessage> {
        self.frames.subscribe()
    }
    fn connection_state(&self) -> Option<tokio::sync::watch::Receiver<BackplaneState>> {
        let epoch = ContinuityEpoch::INITIAL;
        let state = if self.down.load(Ordering::SeqCst) {
            BackplaneState::Disconnected { epoch }
        } else {
            BackplaneState::Connected { epoch }
        };
        let (sender, receiver) = tokio::sync::watch::channel(state);
        self.snapshots.lock().unwrap().push(sender);
        Some(receiver)
    }
}
async fn full_node(
    values: &Arc<MapStorage<u64>>,
    markers: &Arc<MapStorage<MarkerObservation>>,
    clock: &Arc<ManualClock>,
    bp: &Arc<SnapshotBackplane>,
) -> Cache<u64> {
    Cache::builder()
        .strict()
        .clock(clock.clone())
        .default_options(options())
        .memory_storage(values.clone())
        .marker_read_policy(MarkerReadPolicy::OptionsControlled)
        .marker_memory_storage(markers.clone())
        .backplane(bp.clone())
        .try_build_ready()
        .await
        .unwrap()
}
#[tokio::test]
async fn failed_gap_cleanup_advances_both_layers_and_preserves_one_or_both_causes() {
    for both in [false, true] {
        let values = MapStorage::new();
        let markers = MapStorage::new();
        let clock = Arc::new(ManualClock::default());
        let bp = SnapshotBackplane::new();
        let c = full_node(&values, &markers, &clock, &bp).await;
        c.set("key", 101)
            .with_receipt()
            .await
            .unwrap()
            .wait()
            .await
            .unwrap();
        assert!(c.read("key", None).await.unwrap().has_value());
        let old = values.state.lock().unwrap().records["key"].clone();
        let old_markers: Vec<_> = markers
            .state
            .lock()
            .unwrap()
            .records
            .values()
            .cloned()
            .collect();
        *markers.fault.lock().unwrap() = Some(Fault::Clear);
        if both {
            *values.fault.lock().unwrap() = Some(Fault::Clear);
        }
        bp.down.store(true, Ordering::SeqCst);
        let error = c.read("key", None).await.unwrap_err();
        if both {
            let Error::MemoryInvalidation(failures) = error else {
                panic!("both failures lost: {error:?}");
            };
            for source in [failures.values(), failures.markers()] {
                let MemoryStorageError::Provider { source } = source else {
                    panic!("cause lost")
                };
                assert_eq!(*source.downcast_ref::<Fault>().unwrap(), Fault::Clear);
            }
        } else {
            assert_eq!(cause(&error), Fault::Clear);
        }
        assert!(!old.is_live_at(clock.now()));
        assert!(
            old_markers
                .iter()
                .all(|record| !record.is_live_at(clock.now()))
        );
        *values.fault.lock().unwrap() = None;
        *markers.fault.lock().unwrap() = None;
        assert!(!c.read("key", None).await.unwrap().has_value());
        c.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delayed_value_cleanup_already_hides_old_markers_and_preserves_new_writes_in_both_layers() {
    let values = MapStorage::new();
    let markers = MapStorage::new();
    let clock = Arc::new(ManualClock::default());
    let bp = SnapshotBackplane::new();
    let a = full_node(&values, &markers, &clock, &bp).await;
    let b: Cache<u64> = Cache::builder()
        .clock(clock.clone())
        .default_options(options())
        .memory_storage(values.clone())
        .marker_read_policy(MarkerReadPolicy::OptionsControlled)
        .marker_memory_storage(markers.clone())
        .try_build()
        .unwrap();
    a.set("key", 103)
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(a.read("key", None).await.unwrap().has_value());
    let old: Vec<_> = markers
        .state
        .lock()
        .unwrap()
        .records
        .values()
        .cloned()
        .collect();
    let gate = Arc::new(ClearGate {
        entered: Barrier::new(2),
        resume: Barrier::new(2),
    });
    *values.clear_gate.lock().unwrap() = Some(gate.clone());
    bp.down.store(true, Ordering::SeqCst);
    let reader = a.clone();
    let pending = tokio::spawn(async move { reader.read("key", None).await });
    gate.entered.wait();
    assert!(old.iter().all(|record| !record.is_live_at(clock.now())));
    clock.advance(Duration::from_millis(1));
    b.set("key", 107)
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert_eq!(b.read("key", None).await.unwrap().into_value(), Some(107));
    let new: Vec<_> = markers
        .state
        .lock()
        .unwrap()
        .records
        .values()
        .filter(|record| record.is_live_at(clock.now()))
        .cloned()
        .collect();
    assert_eq!(new.len(), 2);
    gate.resume.wait();
    assert_eq!(pending.await.unwrap().unwrap().into_value(), Some(107));
    assert!(new.iter().all(|record| record.is_live_at(clock.now())));
    assert_eq!(a.read("key", None).await.unwrap().into_value(), Some(107));
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

enum BadEpoch {
    Unstable,
    Aliased(MemoryStorageEpoch),
}
#[allow(clippy::double_must_use)]
impl MemoryStorage<MarkerObservation> for BadEpoch {
    fn epoch(&self, _: &MemoryNamespace) -> MemoryStorageEpoch {
        match self {
            Self::Unstable => MemoryStorageEpoch::new(),
            Self::Aliased(epoch) => epoch.clone(),
        }
    }
    fn get(
        &self,
        _: &str,
    ) -> std::result::Result<Option<MemoryRecord<MarkerObservation>>, MemoryStorageError> {
        unreachable!("rejected before lookup")
    }
    fn insert(
        &self,
        _: MemoryRecord<MarkerObservation>,
        _: MemoryCondition<'_, MarkerObservation>,
        _: Timestamp,
    ) -> std::result::Result<MemoryStorageWrite<MarkerObservation>, MemoryStorageError> {
        unreachable!("rejected before insertion")
    }
    fn remove(
        &self,
        _: &str,
        _: Option<&amalgam::entry::Entry<MarkerObservation>>,
    ) -> std::result::Result<Option<MemoryRecord<MarkerObservation>>, MemoryStorageError> {
        unreachable!("rejected before removal")
    }
    fn clear_before(
        &self,
        _: &MemoryGeneration,
    ) -> std::result::Result<Box<[MemoryRecord<MarkerObservation>]>, MemoryStorageError> {
        unreachable!("rejected before clearing")
    }
    fn usage(&self) -> std::result::Result<MemoryUsage, MemoryStorageError> {
        unreachable!("rejected before usage")
    }
}
#[test]
fn marker_generation_cannot_be_unstable_or_alias_the_value_namespace() {
    for (provider, expected) in [
        (BadEpoch::Unstable, ConfigError::UnstableMemoryStorageEpoch),
        (
            BadEpoch::Aliased(MemoryStorageEpoch::new()),
            ConfigError::AliasedMarkerMemoryStorageEpoch,
        ),
    ] {
        let error = Cache::<u64>::builder()
            .marker_read_policy(MarkerReadPolicy::OptionsControlled)
            .marker_memory_storage(Arc::new(provider))
            .try_build()
            .err()
            .unwrap();
        assert!(matches!(error, Error::Config(error) if error == expected));
    }
}

#[derive(Default)]
struct ProbeLocker {
    native: Arc<AtomicUsize>,
    asynchronous: Arc<AtomicUsize>,
}
#[async_trait::async_trait]
impl MemoryLocker for ProbeLocker {
    fn blocking_acquirer(&self) -> Option<Arc<dyn BlockingMemoryLocker>> {
        Some(Arc::new(ProbeNative(self.native.clone())))
    }
    async fn acquire(
        &self,
        request: MemoryLockRequest,
    ) -> std::result::Result<MemoryLockOutcome, MemoryLockerError> {
        assert!(matches!(request.kind(), MemoryLockKind::Marker(_)));
        assert!(request.coordination_key().contains("/local/"));
        self.asynchronous.fetch_add(1, Ordering::SeqCst);
        Ok(MemoryLockOutcome::Unavailable)
    }
    fn try_acquire(
        &self,
        _: MemoryLockRequest,
    ) -> std::result::Result<MemoryLockOutcome, MemoryLockerError> {
        Ok(MemoryLockOutcome::Unavailable)
    }
}
struct ProbeNative(Arc<AtomicUsize>);
impl BlockingMemoryLocker for ProbeNative {
    fn acquire(
        &self,
        request: MemoryLockRequest,
    ) -> std::result::Result<MemoryLockOutcome, MemoryLockerError> {
        assert!(matches!(request.kind(), MemoryLockKind::Marker(_)));
        assert!(request.coordination_key().contains("/local/"));
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(MemoryLockOutcome::Unavailable)
    }
}
#[test]
fn supplied_marker_store_keeps_native_and_async_locker_method_selection() {
    let markers = MapStorage::new();
    let locker = Arc::new(ProbeLocker::default());
    let c = BlockingCache::<u64>::from_builder(
        Cache::builder()
            .memory_locker(locker.clone())
            .default_options(options())
            .marker_read_policy(MarkerReadPolicy::OptionsControlled)
            .marker_memory_storage(markers.clone()),
    )
    .unwrap();
    c.set("key", 109)
        .with_receipt()
        .execute()
        .unwrap()
        .wait()
        .unwrap();
    assert!(c.read("key", None).unwrap().has_value());
    assert_eq!(locker.native.load(Ordering::SeqCst), 2);
    assert_eq!(locker.asynchronous.load(Ordering::SeqCst), 0);
    markers.state.lock().unwrap().records.clear();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    assert!(
        rt.block_on(c.as_async().read("key", None))
            .unwrap()
            .has_value()
    );
    assert_eq!(locker.native.load(Ordering::SeqCst), 2);
    assert_eq!(locker.asynchronous.load(Ordering::SeqCst), 2);
    c.shutdown().unwrap();
}

#[tokio::test]
async fn distributed_snapshot_hydration_and_repair_use_the_actual_external_observation_store() {
    let store = MapStorage::new();
    let journal = Arc::new(InMemoryInvalidationStore::default());
    let clock = Arc::new(ManualClock::default());
    let c: Cache<u64> = Cache::builder()
        .clock(clock.clone())
        .default_options(options())
        .invalidation_store(journal.clone())
        .marker_read_policy(MarkerReadPolicy::OptionsControlled)
        .marker_lifecycle_policy(MarkerLifecyclePolicy::CachedSnapshots)
        .tags_default_options(
            EntryOptions::new(Duration::from_secs(2))
                .with_distributed_duration(Duration::from_secs(30)),
        )
        .marker_memory_storage(store.clone())
        .try_build()
        .unwrap();
    c.remove_by_tag(tag())
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    clock.advance(Duration::from_millis(1));
    tagged(&c, 113).await;
    assert!(c.read("key", None).await.unwrap().has_value());
    let key = store
        .state
        .lock()
        .unwrap()
        .records
        .keys()
        .find(|key| key.ends_with(&hex(format!("tag:{}", tag().as_str()).as_bytes())))
        .unwrap()
        .clone();
    let version = match store.state.lock().unwrap().records[&key]
        .entry()
        .value()
        .presence()
    {
        MarkerPresence::Present(version) => version,
        MarkerPresence::Absent => panic!("tag fact missing"),
    };
    clock.advance(Duration::from_secs(3));
    c.run_pending_tasks().await.unwrap();
    assert!(c.read("key", None).await.unwrap().has_value());
    assert_eq!(
        store.state.lock().unwrap().records[&key]
            .entry()
            .value()
            .presence(),
        MarkerPresence::Present(version)
    );
    clock.advance(Duration::from_secs(31));
    c.run_pending_tasks().await.unwrap();
    assert!(c.read("key", None).await.unwrap().has_value());
    assert_eq!(
        store.state.lock().unwrap().records[&key]
            .entry()
            .value()
            .presence(),
        MarkerPresence::Present(version)
    );
    c.shutdown().await.unwrap();
}

#[tokio::test]
async fn ready_clear_invalidation_stops_before_a_later_provider_contract_fault() {
    let store = MapStorage::new();
    let clock = Arc::new(ManualClock::default());
    let a = node(&store, &clock, "stop:");
    let b = node(&store, &clock, "stop:");
    tagged(&b, 127).await;
    assert!(b.read("key", None).await.unwrap().has_value());
    clock.advance(Duration::from_millis(1));
    a.clear(ClearMode::Remove)
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    {
        let mut state = store.state.lock().unwrap();
        let target = state
            .records
            .keys()
            .find(|key| key.ends_with(&hex(format!("tag:{}", tag().as_str()).as_bytes())))
            .unwrap()
            .clone();
        let unrelated = state
            .records
            .values()
            .find(|record| record.key() != target.as_ref())
            .unwrap()
            .clone();
        state.records.insert(target, unrelated);
    }
    assert!(!b.read("key", None).await.unwrap().has_value());
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[cfg(feature = "redis")]
#[path = "support/redis_fixture.rs"]
mod redis_fixture;
#[cfg(feature = "redis")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_redis_l2_backplane_fenced_locker_and_external_marker_storage_interoperate() {
    let Some(url) = redis_fixture::redis_url() else {
        return;
    };
    let markers = MapStorage::new();
    let l2 = Arc::new(RedisDistributedCache::connect(&url).await.unwrap());
    let bp = Arc::new(RedisBackplane::connect(&url).await.unwrap());
    let lk = Arc::new(RedisDistributedLocker::connect(&url).await.unwrap());
    let prefix = format!(
        "supplied-markers-{}-{}:",
        SystemClock.now().ticks(),
        fastrand::u64(..)
    );
    let mut nodes = Vec::with_capacity(2);
    for id in ["external-marker-a", "external-marker-b"] {
        nodes.push(
            Cache::<u64>::builder()
                .instance_id(id)
                .key_prefix(&prefix)
                .default_options(options())
                .distributed(l2.clone())
                .serializer(Arc::new(JsonSerializer))
                .backplane(bp.clone())
                .distributed_locker(lk.clone())
                .marker_read_policy(MarkerReadPolicy::OptionsControlled)
                .marker_lifecycle_policy(MarkerLifecyclePolicy::CachedSnapshots)
                .tags_default_options(
                    EntryOptions::new(Duration::from_millis(80))
                        .with_distributed_duration(Duration::from_secs(30)),
                )
                .marker_memory_storage(markers.clone())
                .try_build_ready()
                .await
                .unwrap(),
        );
    }
    let a = &nodes[0];
    let b = &nodes[1];
    tagged(a, 131).await;
    assert_eq!(
        b.get_or_set::<_, _>(
            "key",
            typed_factory(|_| forbidden_factory("cold L1 must reuse Redis value"))
        )
        .await
        .unwrap(),
        131
    );
    assert!(b.read("key", None).await.unwrap().has_value());
    a.remove_by_tag(tag())
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(!b.read("key", None).await.unwrap().has_value());
    tokio::time::sleep(Duration::from_millis(100)).await;
    tagged(a, 137).await;
    assert_eq!(
        b.get_or_set::<_, _>(
            "key",
            typed_factory(|_| async {
                panic!("newer Redis value must survive the retained marker")
            })
        )
        .await
        .unwrap(),
        137
    );
    b.run_pending_tasks().await.unwrap();
    assert!(
        markers
            .state
            .lock()
            .unwrap()
            .records
            .values()
            .all(|record| record.key().contains("/durable/"))
    );
    a.clear(ClearMode::Remove)
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    let started = tokio::time::Instant::now();
    while b.read("key", None).await.unwrap().has_value() {
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "backplane clear did not converge"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    for node in nodes {
        node.shutdown().await.unwrap();
    }
}

async fn forbidden_factory(
    message: &'static str,
) -> std::result::Result<u64, amalgam::FactoryError> {
    panic!("{message}");
}

fn typed_factory<V, F, Fut>(factory: F) -> F
where
    F: FnOnce(amalgam::FactoryContext<V>) -> Fut,
    Fut: std::future::Future<Output = std::result::Result<V, amalgam::FactoryError>>,
{
    factory
}
