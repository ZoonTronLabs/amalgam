//! External provider contracts, independent of the built-in memory algorithms.
use amalgam::entry::Entry;
use amalgam::locking::KeyedLock;
use amalgam::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::Duration;

#[path = "support/memory_storage_fixture.rs"]
mod storage_fixture;
use storage_fixture::{ClearGate, Fault, Limit, MapStorage};

fn opts() -> EntryOptions {
    EntryOptions::new(Duration::from_secs(60))
}
fn cache<V: Clone + Send + Sync + 'static>(storage: &Arc<MapStorage<V>>) -> Cache<V> {
    Cache::builder()
        .memory_storage(storage.clone())
        .default_options(opts())
        .build()
}
fn cause(error: &Error) -> Fault {
    let Error::MemoryStorage(MemoryStorageError::Provider { source }) = error else {
        panic!("wrong error: {error:?}")
    };
    *source.downcast_ref::<Fault>().unwrap()
}

#[tokio::test]
async fn supplied_store_is_the_actual_l1_for_all_basic_operations() {
    let store = MapStorage::new();
    let c = cache(&store);
    assert!(Arc::ptr_eq(
        c.memory_storage().unwrap(),
        &(store.clone() as Arc<dyn MemoryStorage<u64>>)
    ));
    c.try_set("key", 7).await.unwrap().wait().await.unwrap();
    assert_eq!(
        store.state.lock().unwrap().records["key"].entry().value(),
        &7
    );
    assert_eq!(c.read("key", None).await.unwrap().into_value(), Some(7));
    assert_eq!(
        c.get_or_set::<_, _>(
            "key",
            typed_factory(|_| async { panic!("L1 hit ran factory") })
        )
        .await
        .unwrap(),
        7
    );
    c.expire("key")
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(c.read("key", None).await.unwrap().into_value().is_none());
    c.try_set("key", 8).await.unwrap().wait().await.unwrap();
    c.remove("key")
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(!store.state.lock().unwrap().records.contains_key("key"));
    c.try_set("other", 9).await.unwrap().wait().await.unwrap();
    c.clear(ClearMode::Remove)
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert_eq!(c.memory_usage().unwrap().entries, 0);
    assert!(store.ready.load(Ordering::SeqCst) > 0);
    assert!(store.lookups.load(Ordering::SeqCst) > 0);
    assert!(store.writes.load(Ordering::SeqCst) >= 4);
    c.shutdown().await.unwrap();
}
#[tokio::test]
async fn disabled_store_returns_computed_value_with_rejected_admission() {
    let store = MapStorage::limited(Limit::Disabled);
    let c = cache(&store);
    let report = c.try_set("key", 11).await.unwrap().wait().await.unwrap();
    assert!(matches!(
        report.local,
        LocalEffect::Stored(MemoryAdmission::Rejected(CapacityRejection::Oversized))
    ));
    assert_eq!(
        c.get_or_set(
            "key",
            amalgam::source::factory(
                |ctx| async move { Ok::<_, amalgam::FactoryError>(ctx.value(12)) }
            )
        )
        .await
        .unwrap(),
        12
    );
    assert!(c.read("key", None).await.unwrap().into_value().is_none());
    assert_eq!(c.memory_usage().unwrap().entries, 0);
    c.shutdown().await.unwrap();
}
#[tokio::test]
async fn provider_owns_capacity_and_priority_without_collateral_pinned_eviction() {
    let store = MapStorage::limited(Limit::Entries(1));
    let c = cache(&store);
    c.try_set_full(
        "pinned",
        13,
        Some(opts().with_priority(Priority::NeverRemove)),
        Box::new([]),
    )
    .await
    .unwrap()
    .wait()
    .await
    .unwrap();
    let report = c.try_set("new", 14).await.unwrap().wait().await.unwrap();
    assert!(matches!(
        report.local,
        LocalEffect::Stored(MemoryAdmission::Rejected(
            CapacityRejection::ProtectedCapacity
        ))
    ));
    assert_eq!(c.read("pinned", None).await.unwrap().into_value(), Some(13));
    c.remove("pinned")
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    c.try_set("low", 15).await.unwrap().wait().await.unwrap();
    let mut evictions = c.memory_evictions().subscribe();
    c.try_set_full(
        "high",
        16,
        Some(opts().with_priority(Priority::High)),
        Box::new([]),
    )
    .await
    .unwrap()
    .wait()
    .await
    .unwrap();
    // low was inserted before the subscription: select retirement-time capture
    // separately below rather than claiming insertion-time diagnostics exist.
    assert!(c.read("low", None).await.unwrap().into_value().is_none());
    assert_eq!(c.read("high", None).await.unwrap().into_value(), Some(16));
    assert!(matches!(
        evictions.try_recv(),
        Err(EvictionReceiveError::Empty)
    ));
    c.shutdown().await.unwrap();
}
#[tokio::test]
async fn weight_accounting_uses_full_u64_units_and_rejection_is_explicit() {
    let store = MapStorage::limited(Limit::Weight(u128::from(u32::MAX) + 10));
    let c = cache(&store);
    c.try_set_full(
        "large",
        17,
        Some(opts().with_size(i64::from(u32::MAX) + 5)),
        Box::new([]),
    )
    .await
    .unwrap()
    .wait()
    .await
    .unwrap();
    assert_eq!(c.memory_usage().unwrap().weight, u128::from(u32::MAX) + 5);
    let report = c
        .try_set_full(
            "oversized",
            18,
            Some(opts().with_size(i64::from(u32::MAX) + 11)),
            Box::new([]),
        )
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(matches!(
        report.local,
        LocalEffect::Stored(MemoryAdmission::Rejected(CapacityRejection::Oversized))
    ));
    assert_eq!(c.read("large", None).await.unwrap().into_value(), Some(17));
    c.shutdown().await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn supplied_memory_preserves_same_key_single_flight() {
    let store = MapStorage::new();
    let c = cache(&store);
    let runs = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(tokio::sync::Barrier::new(100));
    let mut work = Vec::with_capacity(100);
    for _ in 0..100 {
        let c = c.clone();
        let runs = runs.clone();
        let barrier = barrier.clone();
        work.push(tokio::spawn(async move {
            barrier.wait().await;
            c.get_or_set(
                "stampede",
                amalgam::source::factory(move |ctx| async move {
                    runs.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    Ok::<_, amalgam::FactoryError>(ctx.value(19))
                }),
            )
            .await
            .unwrap()
        }));
    }
    for task in work {
        assert_eq!(task.await.unwrap(), 19);
    }
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    c.shutdown().await.unwrap();
}
#[tokio::test]
async fn unrelated_factories_enter_before_either_completes() {
    let store = MapStorage::new();
    let c = cache(&store);
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let work = |key| {
        let c = c.clone();
        let barrier = barrier.clone();
        async move {
            c.get_or_set(
                key,
                amalgam::source::factory(move |ctx| async move {
                    barrier.wait().await;
                    Ok::<_, amalgam::FactoryError>(ctx.value(23))
                }),
            )
            .await
            .unwrap()
        }
    };
    let (a, b) = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(work("a"), work("b"))
    })
    .await
    .unwrap();
    assert_eq!((a, b), (23, 23));
    c.shutdown().await.unwrap();
}
#[tokio::test]
async fn controlled_clock_keeps_values_until_the_actual_physical_deadline() {
    let store = MapStorage::new();
    let clock = Arc::new(ManualClock::new(Timestamp::from_ticks(10000)));
    let c = Cache::builder()
        .clock(clock.clone())
        .memory_storage(store.clone())
        .memory_eviction_capture(EvictionCapture::AtRetirement)
        .default_options(EntryOptions::new(Duration::from_millis(20)))
        .build();
    c.try_set("clock", 29).await.unwrap().wait().await.unwrap();
    let mut evictions = c.memory_evictions().subscribe();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(c.read("clock", None).await.unwrap().into_value(), Some(29));
    clock.advance(Duration::from_millis(20));
    assert!(c.read("clock", None).await.unwrap().into_value().is_none());
    let event = evictions.try_recv().unwrap();
    assert_eq!(event.value(), &29);
    assert_eq!(event.reason(), MemoryEvictionReason::Expired);
    c.shutdown().await.unwrap();
}
#[tokio::test]
async fn system_clock_retires_physically_expired_records() {
    let store = MapStorage::new();
    let c = Cache::builder()
        .memory_storage(store.clone())
        .default_options(EntryOptions::new(Duration::from_millis(20)))
        .build();
    c.try_set("clock", 31).await.unwrap().wait().await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(c.read("clock", None).await.unwrap().into_value().is_none());
    assert_eq!(c.memory_usage().unwrap().entries, 0);
    c.shutdown().await.unwrap();
}
#[tokio::test]
async fn fail_safe_keeps_the_supplied_representation() {
    let store = MapStorage::new();
    let clock = Arc::new(ManualClock::new(Timestamp::from_ticks(10000)));
    let options = EntryOptions::new(Duration::from_millis(200)).with_fail_safe(
        true,
        Some(Duration::from_secs(60)),
        Some(Duration::from_secs(1)),
    );
    let c = Cache::builder()
        .clock(clock.clone())
        .memory_storage(store.clone())
        .default_options(options.clone())
        .build();
    c.get_or_set(
        "stale",
        amalgam::source::factory(
            |ctx| async move { Ok::<_, amalgam::FactoryError>(ctx.value(37)) },
        ),
    )
    .await
    .unwrap();
    clock.advance(Duration::from_millis(250));
    assert_eq!(
        c.get_or_set(
            "stale",
            amalgam::source::factory(|ctx| async move { Err(ctx.fail("origin down")) })
        )
        .await
        .unwrap(),
        37
    );
    assert_eq!(
        store.state.lock().unwrap().records["stale"].entry().value(),
        &37
    );
    c.shutdown().await.unwrap();
}
#[tokio::test]
async fn eager_refresh_updates_the_supplied_store_in_the_background() {
    let store = MapStorage::new();
    let clock = Arc::new(ManualClock::new(Timestamp::from_ticks(10000)));
    let c = Cache::builder()
        .clock(clock.clone())
        .memory_storage(store.clone())
        .default_options(
            EntryOptions::new(Duration::from_secs(10)).with_eager_refresh(EagerThreshold::new(0.5)),
        )
        .build();
    c.try_set("eager", 41).await.unwrap().wait().await.unwrap();
    clock.advance(Duration::from_secs(6));
    assert_eq!(
        c.get_or_set(
            "eager",
            amalgam::source::factory(
                |ctx| async move { Ok::<_, amalgam::FactoryError>(ctx.value(43)) }
            )
        )
        .await
        .unwrap(),
        41
    );
    c.flush_pending().await.unwrap();
    assert_eq!(c.read("eager", None).await.unwrap().into_value(), Some(43));
    assert_eq!(
        store.state.lock().unwrap().records["eager"].entry().value(),
        &43
    );
    c.shutdown().await.unwrap();
}
#[tokio::test]
async fn lookup_failure_preserves_cause_and_does_not_run_origin_or_emit_miss() {
    let store = MapStorage::<u64>::new();
    let c = cache(&store);
    *store.fault.lock().unwrap() = Some(Fault::Get);
    let mut events = c.events().subscribe();
    let error = c
        .get_or_set::<_, _>(
            "broken",
            typed_factory(|_| async { panic!("storage failure ran origin") }),
        )
        .await
        .unwrap_err();
    assert_eq!(cause(&error), Fault::Get);
    assert_eq!(
        OperationOutcome::from_error(&error),
        OperationOutcome::MemoryError
    );
    let mut completed = 0;
    while let Ok(event) = events.try_recv() {
        match event {
            CacheEvent::Miss { .. } => panic!("storage error became miss"),
            CacheEvent::OperationCompleted { outcome, .. } => {
                assert_eq!(outcome, OperationOutcome::MemoryError);
                completed += 1;
            }
            _ => {}
        }
    }
    assert_eq!(completed, 1);
    *store.fault.lock().unwrap() = None;
    c.shutdown().await.unwrap();
}
#[tokio::test]
async fn write_remove_maintenance_and_usage_errors_remain_typed() {
    let store = MapStorage::new();
    let c = cache(&store);
    *store.fault.lock().unwrap() = Some(Fault::Insert);
    assert_eq!(cause(&c.try_set("k", 47).await.unwrap_err()), Fault::Insert);
    *store.fault.lock().unwrap() = None;
    c.try_set("k", 53).await.unwrap().wait().await.unwrap();
    *store.fault.lock().unwrap() = Some(Fault::Remove);
    assert_eq!(
        cause(&c.remove("k").with_receipt().await.unwrap_err()),
        Fault::Remove
    );
    *store.fault.lock().unwrap() = Some(Fault::Maintain);
    assert_eq!(
        cause(&c.try_run_pending_tasks().await.unwrap_err()),
        Fault::Maintain
    );
    *store.fault.lock().unwrap() = Some(Fault::Usage);
    assert_eq!(cause(&c.memory_usage().unwrap_err()), Fault::Usage);
    *store.fault.lock().unwrap() = None;
    assert_eq!(c.read("k", None).await.unwrap().into_value(), Some(53));
    c.shutdown().await.unwrap();
}
#[tokio::test]
async fn failed_physical_clear_still_prevents_old_records_from_becoming_visible() {
    let store = MapStorage::new();
    let c = cache(&store);
    c.try_set("k", 59).await.unwrap().wait().await.unwrap();
    *store.fault.lock().unwrap() = Some(Fault::Clear);
    assert_eq!(
        cause(&c.clear(ClearMode::Remove).with_receipt().await.unwrap_err()),
        Fault::Clear
    );
    assert!(store.state.lock().unwrap().records.contains_key("k"));
    *store.fault.lock().unwrap() = None;
    assert!(c.read("k", None).await.unwrap().into_value().is_none());
    assert!(!store.state.lock().unwrap().records.contains_key("k"));
    c.shutdown().await.unwrap();
}
#[tokio::test]
async fn shared_store_is_reused_and_shutdown_does_not_dispose_another_cache() {
    let store = MapStorage::new();
    let a = cache(&store);
    let b = cache(&store);
    a.try_set("shared", 61).await.unwrap().wait().await.unwrap();
    assert_eq!(
        b.get_or_set::<_, _>(
            "shared",
            typed_factory(|_| async { panic!("shared L1 missed") })
        )
        .await
        .unwrap(),
        61
    );
    a.shutdown().await.unwrap();
    drop(a);
    b.try_set("shared", 67).await.unwrap().wait().await.unwrap();
    assert_eq!(b.read("shared", None).await.unwrap().into_value(), Some(67));
    b.shutdown().await.unwrap();
}
#[tokio::test]
async fn shared_provider_clear_is_isolated_by_the_actual_key_prefix() {
    let store = MapStorage::new();
    let a = Cache::builder()
        .key_prefix("a:")
        .memory_storage(store.clone())
        .build();
    let b = Cache::builder()
        .key_prefix("b:")
        .memory_storage(store.clone())
        .build();
    a.try_set("key", 71).await.unwrap().wait().await.unwrap();
    b.try_set("key", 73).await.unwrap().wait().await.unwrap();
    a.clear(ClearMode::Remove)
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(a.read("key", None).await.unwrap().into_value().is_none());
    assert_eq!(b.read("key", None).await.unwrap().into_value(), Some(73));
    assert_eq!(b.memory_usage().unwrap().entries, 1);
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}
#[test]
fn overlapping_clear_preserves_a_write_admitted_after_its_barrier() {
    let store = MapStorage::new();
    let clock = Arc::new(ManualClock::new(Timestamp::from_ticks(10000)));
    let a = BlockingCache::from_builder(
        Cache::builder()
            .clock(clock.clone())
            .memory_storage(store.clone()),
    )
    .unwrap();
    let b = BlockingCache::from_builder(
        Cache::builder()
            .clock(clock.clone())
            .memory_storage(store.clone()),
    )
    .unwrap();
    a.try_set("key", 79).unwrap().wait().unwrap();
    clock.advance(Duration::from_secs(1));
    let gate = Arc::new(ClearGate {
        entered: Barrier::new(2),
        resume: Barrier::new(2),
    });
    *store.clear_gate.lock().unwrap() = Some(gate.clone());
    let clearing = a.clone();
    let work = std::thread::spawn(move || {
        clearing
            .clear(ClearMode::Remove)
            .with_receipt()
            .execute()
            .unwrap()
            .wait()
            .unwrap()
    });
    gate.entered.wait();
    clock.advance(Duration::from_secs(1));
    b.try_set("key", 83).unwrap().wait().unwrap();
    gate.resume.wait();
    work.join().unwrap();
    assert_eq!(b.read("key", None).unwrap().into_value(), Some(83));
    assert_eq!(a.read("key", None).unwrap().into_value(), Some(83));
    a.shutdown().unwrap();
    b.shutdown().unwrap();
}
#[test]
fn native_and_async_views_use_the_same_supplied_store_and_typed_failure() {
    let store = MapStorage::new();
    let native =
        BlockingCache::from_builder(Cache::builder().memory_storage(store.clone())).unwrap();
    native.try_set("native", 89).unwrap().wait().unwrap();
    assert_eq!(
        native
            .get_or_set(
                "native",
                typed_blocking_factory(|_| panic!("native hot hit ran origin"))
            )
            .execute()
            .unwrap(),
        89
    );
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    assert_eq!(
        runtime
            .block_on(native.as_async().read("native", None))
            .unwrap()
            .into_value(),
        Some(89)
    );
    *store.fault.lock().unwrap() = Some(Fault::Get);
    assert_eq!(cause(&native.read("native", None).unwrap_err()), Fault::Get);
    *store.fault.lock().unwrap() = None;
    native.shutdown().unwrap();
}
#[tokio::test]
async fn skipped_memory_options_do_not_touch_the_provider() {
    let store = MapStorage::new();
    let c = cache(&store);
    *store.fault.lock().unwrap() = Some(Fault::Get);
    let skipped = opts().with_skip_memory(true, true);
    assert_eq!(
        c.get_or_set(
            "skip",
            typed_factory(|ctx| async move { Ok::<_, amalgam::FactoryError>(ctx.value(97)) })
        )
        .options(|_| skipped.clone())
        .await
        .unwrap(),
        97
    );
    *store.fault.lock().unwrap() = Some(Fault::Insert);
    c.try_set_full("skip", 101, Some(skipped), Box::new([]))
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert_eq!(store.lookups.load(Ordering::SeqCst), 0);
    assert_eq!(store.ready.load(Ordering::SeqCst), 0);
    assert_eq!(store.writes.load(Ordering::SeqCst), 0);
    *store.fault.lock().unwrap() = None;
    c.shutdown().await.unwrap();
}
#[test]
fn builtin_capacity_settings_are_not_silently_ignored_for_supplied_storage() {
    let store = MapStorage::<u64>::new();
    let error = match Cache::builder()
        .max_capacity(1)
        .memory_storage(store)
        .try_build()
    {
        Ok(_) => panic!("conflicting limits accepted"),
        Err(e) => e,
    };
    assert!(matches!(
        error,
        Error::Config(ConfigError::SuppliedMemoryWithBuiltinLimits)
    ));
}
struct Unstable;
impl MemoryStorage<u64> for Unstable {
    fn epoch(&self, _: &MemoryNamespace) -> MemoryStorageEpoch {
        MemoryStorageEpoch::new()
    }
    fn get(&self, _: &str) -> std::result::Result<Option<MemoryRecord<u64>>, MemoryStorageError> {
        panic!("invalid provider reached lookup")
    }
    fn insert(
        &self,
        _: MemoryRecord<u64>,
        _: MemoryCondition<'_, u64>,
        _: Timestamp,
    ) -> std::result::Result<MemoryStorageWrite<u64>, MemoryStorageError> {
        panic!("invalid provider reached write")
    }
    fn remove(
        &self,
        _: &str,
        _: Option<&Entry<u64>>,
    ) -> std::result::Result<Option<MemoryRecord<u64>>, MemoryStorageError> {
        panic!("invalid provider reached remove")
    }
    fn clear_before(
        &self,
        _: &MemoryGeneration,
    ) -> std::result::Result<Box<[MemoryRecord<u64>]>, MemoryStorageError> {
        panic!("invalid provider reached clear")
    }
    fn usage(&self) -> std::result::Result<MemoryUsage, MemoryStorageError> {
        panic!("invalid provider reached usage")
    }
}
#[test]
fn unstable_generation_identity_is_rejected_before_cache_construction() {
    let error = match Cache::builder()
        .memory_storage(Arc::new(Unstable))
        .try_build()
    {
        Ok(_) => panic!("unstable epoch accepted"),
        Err(e) => e,
    };
    assert!(matches!(
        error,
        Error::Config(ConfigError::UnstableMemoryStorageEpoch)
    ));
}
#[tokio::test]
async fn retired_values_are_reported_to_the_inserting_cache_of_a_shared_store() {
    let store = MapStorage::new();
    let a = Cache::builder()
        .memory_storage(store.clone())
        .memory_eviction_capture(EvictionCapture::AtRetirement)
        .build();
    let b = cache(&store);
    let mut original = a.memory_evictions().subscribe();
    let mut replacing = b.memory_evictions().subscribe();
    a.try_set("key", 103).await.unwrap().wait().await.unwrap();
    b.try_set("key", 107).await.unwrap().wait().await.unwrap();
    let event = original.try_recv().unwrap();
    assert_eq!(event.key(), "key");
    assert_eq!(event.value(), &103);
    assert_eq!(event.reason(), MemoryEvictionReason::Replaced);
    assert!(matches!(
        replacing.try_recv(),
        Err(EvictionReceiveError::Empty)
    ));
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

struct PausedL2 {
    backend: Arc<InMemoryDistributedCache>,
    entered: tokio::sync::Notify,
    resume: tokio::sync::Semaphore,
}
#[async_trait::async_trait]
impl DistributedCache for PausedL2 {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let bytes = self.backend.get(key).await?;
        if key.ends_with("value") {
            self.entered.notify_one();
            self.resume.acquire().await.unwrap().forget();
        }
        Ok(bytes)
    }
    async fn set(&self, key: &str, value: Vec<u8>, ttl: Option<Duration>) -> Result<()> {
        self.backend.set(key, value, ttl).await
    }
    async fn remove(&self, key: &str) -> Result<()> {
        self.backend.remove(key).await
    }
    fn invalidation_store(&self) -> Option<Arc<dyn InvalidationStore>> {
        self.backend.invalidation_store()
    }
}
#[tokio::test]
async fn older_l2_hydration_cannot_replace_another_cache_shared_l1_write() {
    let clock = Arc::new(ManualClock::new(Timestamp::from_ticks(10000)));
    let backend = Arc::new(InMemoryDistributedCache::new(clock.clone()));
    let source = Cache::builder()
        .clock(clock.clone())
        .distributed(backend.clone())
        .serializer(Arc::new(JsonSerializer))
        .build();
    source
        .try_set("value", 109_u64)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    let store = MapStorage::new();
    let pause = Arc::new(PausedL2 {
        backend,
        entered: tokio::sync::Notify::new(),
        resume: tokio::sync::Semaphore::new(0),
    });
    let a = Cache::builder()
        .clock(clock.clone())
        .memory_storage(store.clone())
        .distributed(pause.clone())
        .serializer(Arc::new(JsonSerializer))
        .build();
    let b = Cache::builder()
        .clock(clock.clone())
        .memory_storage(store.clone())
        .build();
    let reading = a.clone();
    let work = tokio::spawn(async move { reading.read("value", None).await });
    pause.entered.notified().await;
    clock.advance(Duration::from_secs(1));
    b.try_set("value", 113_u64)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    let selected = store.state.lock().unwrap().records["value"].entry().clone();
    pause.resume.add_permits(1);
    assert!(work.await.unwrap().unwrap().has_value());
    assert!(store.state.lock().unwrap().records["value"].is_same_entry(&selected));
    assert_eq!(a.read("value", None).await.unwrap().into_value(), Some(113));
    source.shutdown().await.unwrap();
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[tokio::test]
async fn soft_timeout_retains_background_completion_in_supplied_storage() {
    let store = MapStorage::new();
    let clock = Arc::new(ManualClock::new(Timestamp::from_ticks(10000)));
    let options = EntryOptions::new(Duration::from_millis(200))
        .with_fail_safe(true, Some(Duration::from_secs(60)), None)
        .with_factory_timeouts(
            Timeout::After(Duration::from_millis(20)),
            Timeout::Infinite,
            true,
        );
    let c = Cache::builder()
        .memory_storage(store.clone())
        .clock(clock.clone())
        .default_options(options)
        .build();
    c.try_set("key", 127).await.unwrap().wait().await.unwrap();
    clock.advance(Duration::from_millis(250));
    let entered = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Semaphore::new(0));
    let started = entered.clone();
    let waiting = resume.clone();
    assert_eq!(
        c.get_or_set(
            "key",
            amalgam::source::factory(move |ctx| async move {
                started.notify_one();
                waiting.acquire().await.unwrap().forget();
                Ok::<_, amalgam::FactoryError>(ctx.value(131))
            })
        )
        .await
        .unwrap(),
        127
    );
    entered.notified().await;
    resume.add_permits(1);
    c.flush_pending().await.unwrap();
    assert_eq!(c.read("key", None).await.unwrap().into_value(), Some(131));
    c.shutdown().await.unwrap();
}
#[tokio::test]
async fn hard_timeout_does_not_admit_a_late_value_in_supplied_storage() {
    let store = MapStorage::new();
    let c = Cache::builder()
        .memory_storage(store.clone())
        .default_options(opts().with_factory_timeouts(
            Timeout::Infinite,
            Timeout::After(Duration::from_millis(20)),
            false,
        ))
        .build();
    let error = c
        .get_or_set(
            "key",
            amalgam::source::factory(|ctx| async move {
                tokio::time::sleep(Duration::from_millis(80)).await;
                Ok::<_, amalgam::FactoryError>(ctx.value(137))
            }),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, Error::FactoryTimeout { .. }));
    c.flush_pending().await.unwrap();
    assert_eq!(c.memory_usage().unwrap().entries, 0);
    c.shutdown().await.unwrap();
}

struct ReentrantDrop {
    store: std::sync::Weak<MapStorage<Arc<ReentrantDrop>>>,
    locker: Arc<KeyedLock>,
    drops: Arc<AtomicUsize>,
}
impl Drop for ReentrantDrop {
    fn drop(&mut self) {
        if let Some(store) = self.store.upgrade() {
            assert!(
                store.state.try_lock().is_ok(),
                "V::drop ran under provider guard"
            );
        }
        assert!(
            self.locker.try_lock("entry:key").is_some(),
            "V::drop ran under cache factory guard"
        );
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
#[tokio::test]
async fn original_value_destruction_follows_provider_and_factory_guards() {
    let store = MapStorage::new();
    let locker = Arc::new(KeyedLock::new(8));
    let drops = Arc::new(AtomicUsize::new(0));
    let clock = Arc::new(ManualClock::new(Timestamp::from_ticks(10000)));
    let options = EntryOptions::new(Duration::from_millis(200)).with_fail_safe(
        true,
        Some(Duration::from_secs(60)),
        None,
    );
    let c = Cache::builder()
        .clock(clock.clone())
        .memory_storage(store.clone())
        .memory_locker(locker.clone())
        .default_options(options)
        .build();
    c.try_set(
        "key",
        Arc::new(ReentrantDrop {
            store: Arc::downgrade(&store),
            locker: locker.clone(),
            drops: drops.clone(),
        }),
    )
    .await
    .unwrap()
    .wait()
    .await
    .unwrap();
    clock.advance(Duration::from_millis(250));
    let next = Arc::new(ReentrantDrop {
        store: Arc::downgrade(&store),
        locker,
        drops: drops.clone(),
    });
    let value = c
        .get_or_set(
            "key",
            amalgam::source::factory(move |ctx| async move {
                Ok::<_, amalgam::FactoryError>(ctx.value(next))
            }),
        )
        .await
        .unwrap();
    drop(value);
    c.flush_pending().await.unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    c.shutdown().await.unwrap();
}

struct ReentrantObserver {
    store: Arc<MapStorage<u64>>,
    target: Mutex<std::sync::Weak<BlockingCache<u64>>>,
    callbacks: AtomicUsize,
}
impl Plugin for ReentrantObserver {
    fn name(&self) -> &str {
        "shared-original-observer"
    }
    fn observations(&self) -> PluginObservations {
        PluginObservations::All
    }
    fn on_event(&self, _: &CacheEvent) {}
    fn on_layer_event(&self, event: &LayerEvent) {
        if matches!(event, LayerEvent::Memory(MemoryEvent::Eviction { .. })) {
            assert!(
                self.store.state.try_lock().is_ok(),
                "callback ran under provider guard"
            );
            let target = self.target.lock().unwrap().upgrade().unwrap();
            let source = CancellationSource::new();
            let abort = source.clone();
            let timer = std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(250));
                abort.cancel();
            });
            target
                .try_set_full_cancellable("key", 149, None, Box::new([]), source.token())
                .unwrap()
                .wait()
                .unwrap();
            timer.join().unwrap();
            self.callbacks.fetch_add(1, Ordering::SeqCst);
        }
    }
}
#[test]
fn shared_original_observer_can_reenter_replacing_cache_after_its_commit_guard() {
    let store = MapStorage::new();
    let observer = Arc::new(ReentrantObserver {
        store: store.clone(),
        target: Mutex::new(std::sync::Weak::new()),
        callbacks: AtomicUsize::new(0),
    });
    let a = BlockingCache::from_builder(
        Cache::builder()
            .memory_storage(store.clone())
            .plugin(observer.clone()),
    )
    .unwrap();
    let b = Arc::new(
        BlockingCache::from_builder(Cache::builder().memory_storage(store.clone())).unwrap(),
    );
    *observer.target.lock().unwrap() = Arc::downgrade(&b);
    a.try_set("key", 139).unwrap().wait().unwrap();
    b.try_set("key", 143).unwrap().wait().unwrap();
    assert_eq!(observer.callbacks.load(Ordering::SeqCst), 1);
    assert_eq!(b.read("key", None).unwrap().into_value(), Some(149));
    a.shutdown().unwrap();
    b.shutdown().unwrap();
}

#[tokio::test]
async fn conditional_refresh_preserves_replaced_validators_in_supplied_records() {
    let store = MapStorage::new();
    let clock = Arc::new(ManualClock::new(Timestamp::from_ticks(10000)));
    let c = Cache::builder()
        .clock(clock.clone())
        .memory_storage(store.clone())
        .default_options(
            EntryOptions::new(Duration::from_millis(200)).with_fail_safe(
                true,
                Some(Duration::from_secs(60)),
                None,
            ),
        )
        .build();
    c.get_or_set(
        "conditional",
        amalgam::source::factory(|ctx| async move {
            Ok::<_, amalgam::FactoryError>(ctx.modified(151).etag("v1").done())
        }),
    )
    .await
    .unwrap();
    clock.advance(Duration::from_millis(250));
    assert_eq!(
        c.get_or_set(
            "conditional",
            amalgam::source::factory(|ctx| async move {
                Ok::<_, amalgam::FactoryError>(
                    ctx.not_modified_builder()?
                        .etag(ValidatorUpdate::Replace("v2".into()))
                        .done(),
                )
            })
        )
        .await
        .unwrap(),
        151
    );
    assert_eq!(
        store.state.lock().unwrap().records["conditional"]
            .entry()
            .meta()
            .etag(),
        Some("v2")
    );
    c.shutdown().await.unwrap();
}

#[tokio::test]
async fn non_copy_values_use_the_same_public_storage_contract() {
    let store = MapStorage::<String>::new();
    let c = Cache::builder().memory_storage(store.clone()).build();
    c.try_set("key", "first".to_owned())
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    let selected = store.state.lock().unwrap().records["key"].entry().clone();
    let condition = MemoryCondition::Same(&selected);
    let copied = condition;
    let again = condition;
    assert!(copied.matches(
        store.state.lock().unwrap().records.get("key"),
        SystemClock.now()
    ));
    assert!(again.matches(
        store.state.lock().unwrap().records.get("key"),
        SystemClock.now()
    ));
    assert_eq!(
        c.read("key", None).await.unwrap().into_value().as_deref(),
        Some("first")
    );
    c.remove("key")
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert_eq!(c.memory_usage().unwrap().entries, 0);
    c.shutdown().await.unwrap();
}

struct CancelUnusedFactory(CancellationSource);
impl Drop for CancelUnusedFactory {
    fn drop(&mut self) {
        self.0.cancel();
    }
}
#[test]
fn native_warm_unused_factory_drop_precedes_final_cancellation_check() {
    let store = MapStorage::new();
    let c = BlockingCache::from_builder(Cache::builder().memory_storage(store)).unwrap();
    c.try_set("key", 157).unwrap().wait().unwrap();
    let cancellation = CancellationSource::new();
    let capture = CancelUnusedFactory(cancellation.clone());
    let result = c
        .get_or_set(
            "key",
            typed_blocking_factory(move |_| {
                drop(capture);
                panic!("warm L1 must not invoke origin");
            }),
        )
        .cancellation(cancellation.token())
        .execute();
    assert!(matches!(
        result,
        Err(Error::OperationCancelled {
            reason: FactoryCancellationReason::CallerCancelled
        })
    ));
    c.shutdown().unwrap();
}

fn typed_factory<V, F, Fut>(factory: F) -> F
where
    F: FnOnce(amalgam::FactoryContext<V>) -> Fut,
    Fut: std::future::Future<Output = std::result::Result<V, amalgam::FactoryError>>,
{
    factory
}

fn typed_blocking_factory<V, F>(factory: F) -> F
where
    F: FnOnce(amalgam::FactoryContext<V>) -> std::result::Result<V, amalgam::FactoryError>,
{
    factory
}
