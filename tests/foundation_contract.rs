//! Foundation acceptance at the storage, copy, registry and plugin seams.
//! Cache orchestration has separate end-to-end integration tests.

use std::error::Error as _;
use std::sync::atomic::{AtomicI32, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex, Weak};
use std::time::Duration;

use amalgam::entry::Entry;
use amalgam::observability::OperationObservation;
use amalgam::serializers::copy_value;
use amalgam::{
    Cache, CacheEvent, CacheLevel, CacheOperation, CacheRegistry, CapacityRejection, Clock,
    CloneError, ConfigError, DistributedSerializer, EagerThreshold, EntryOptions, EntryWeight,
    Error, Events, FactoryCancellationReason, FactoryError, JitterSample, JsonSerializer,
    ManualClock, MemoryAdmission, MemoryLimits, MemoryStore, OperationOutcome, Plugin,
    PluginContext, PluginError, PluginHost, PluginSession, PluginStage, PluginStopOutcome,
    Priority, RegistryError, Timestamp, ValueCloner,
};

fn at(millis: u64) -> Timestamp {
    Timestamp::from_ticks(i64::try_from(millis * 10_000).unwrap())
}

fn entry<V>(value: V, options: &EntryOptions, now: Timestamp) -> Entry<V> {
    Entry::try_fresh_with_jitter(
        value,
        options,
        now,
        now,
        JitterSample::ZERO,
        Box::from([]),
        None,
        None,
    )
    .unwrap()
}

fn memory<V: Clone + Send + Sync + 'static>(
    limits: MemoryLimits,
) -> (Arc<ManualClock>, MemoryStore<V>) {
    let clock = Arc::new(ManualClock::new(at(1_000)));
    let store = MemoryStore::with_clock(limits, Events::default(), clock.clone());
    (clock, store)
}

#[test]
fn same_name_registry_initializes_once_outside_runtime() {
    let registry = Arc::new(CacheRegistry::<i32>::new());
    let barrier = Arc::new(Barrier::new(12));
    let builds = Arc::new(AtomicUsize::new(0));
    let handles: Vec<_> = (0..12)
        .map(|_| {
            let (registry, barrier, builds) = (registry.clone(), barrier.clone(), builds.clone());
            std::thread::spawn(move || {
                barrier.wait();
                registry
                    .try_get_or_create("one", || {
                        builds.fetch_add(1, Ordering::SeqCst);
                        std::thread::sleep(Duration::from_millis(10));
                        Ok(Cache::new())
                    })
                    .unwrap()
            })
        })
        .collect();
    let caches: Vec<_> = handles
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .collect();
    assert_eq!(builds.load(Ordering::SeqCst), 1);
    assert_eq!(registry.len(), 1);
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        caches[0].set("shared", 42).await.unwrap();
        for cache in caches {
            assert_eq!(cache.try_get("shared", None).await.into_value(), Some(42));
        }
    });
}

#[test]
fn registry_different_names_progress_independently() {
    let registry = Arc::new(CacheRegistry::<i32>::new());
    let inside = Arc::new(Barrier::new(2));
    let threads: Vec<_> = ["a", "b"]
        .into_iter()
        .map(|name| {
            let (registry, inside) = (registry.clone(), inside.clone());
            std::thread::spawn(move || {
                registry
                    .try_get_or_create(name, || {
                        inside.wait();
                        Ok(Cache::new())
                    })
                    .unwrap()
            })
        })
        .collect();
    for thread in threads {
        drop(thread.join().unwrap());
    }
    assert_eq!(registry.len(), 2);
}

#[test]
fn registry_failures_panics_and_recursion_release_initializer() {
    let registry = CacheRegistry::<i32>::new();
    let failure = registry
        .try_get_or_create("retry", || Err(ConfigError::AutoCloneWithoutCloner.into()))
        .err()
        .unwrap();
    assert!(
        matches!(failure, RegistryError::Build { source, .. } if matches!(*source, Error::Config(ConfigError::AutoCloneWithoutCloner)))
    );
    assert!(registry.is_empty());
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            registry.get_or_create("retry", || panic!("external builder contract"));
        }))
        .is_err()
    );
    let cache = registry
        .try_get_or_create("retry", || {
            assert!(matches!(
                registry.try_get_or_create("retry", || Ok(Cache::new())),
                Err(RegistryError::RecursiveInitialization { .. })
            ));
            Ok(Cache::new())
        })
        .unwrap();
    assert_eq!(registry.len(), 1);
    assert_eq!(cache.name(), "amalgam");
    assert!(matches!(
        registry.try_register("  ", cache),
        Err(RegistryError::BlankName)
    ));
}

#[test]
fn explicit_registry_registration_supersedes_inflight_builder() {
    let registry = Arc::new(CacheRegistry::<i32>::new());
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    let building = registry.clone();
    let thread = std::thread::spawn(move || {
        building.get_or_create("name", || {
            started_tx.send(()).unwrap();
            resume_rx.recv().unwrap();
            Cache::builder().name("old-candidate").build()
        })
    });
    started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    registry
        .try_register("name", Cache::builder().name("registered").build())
        .unwrap();
    resume_tx.send(()).unwrap();
    assert_eq!(thread.join().unwrap().name(), "registered");
    assert_eq!(registry.get("name").unwrap().name(), "registered");
}

struct AtomicCloner;
impl ValueCloner<Arc<AtomicI32>> for AtomicCloner {
    fn clone_value(&self, value: &Arc<AtomicI32>) -> Result<Arc<AtomicI32>, CloneError> {
        Ok(Arc::new(AtomicI32::new(value.load(Ordering::SeqCst))))
    }
}

#[test]
fn nonserde_cloner_isolates_input_and_every_read() {
    let opts = EntryOptions::default().with_enable_auto_clone(true);
    let input = Arc::new(AtomicI32::new(7));
    let cached = entry(
        copy_value(&input, &opts, Some(&AtomicCloner)).unwrap(),
        &opts,
        at(0),
    );
    input.store(100, Ordering::SeqCst);
    let first = cached
        .value_with_options(&opts, Some(&AtomicCloner))
        .unwrap();
    assert_eq!(first.load(Ordering::SeqCst), 7);
    first.store(200, Ordering::SeqCst);
    let second = cached
        .value_with_options(&opts, Some(&AtomicCloner))
        .unwrap();
    assert_eq!(second.load(Ordering::SeqCst), 7);
    assert!(!Arc::ptr_eq(cached.value(), &first));
    assert!(matches!(
        copy_value(&input, &opts, None),
        Err(Error::Config(ConfigError::AutoCloneWithoutCloner))
    ));
    let legacy = copy_value(&input, &opts.with_enable_auto_clone(false), None).unwrap();
    assert!(Arc::ptr_eq(&input, &legacy));
}

#[test]
fn copy_failures_preserve_source_and_invalid_options_skip_external_copy() {
    struct Failing(AtomicUsize);
    impl ValueCloner<i32> for Failing {
        fn clone_value(&self, _value: &i32) -> Result<i32, CloneError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Err(CloneError::from_source(std::io::Error::other(
                "copy graph failed",
            )))
        }
    }
    let cloner = Failing(AtomicUsize::new(0));
    let opts = EntryOptions::default().with_enable_auto_clone(true);
    let error = copy_value(&1, &opts, Some(&cloner)).unwrap_err();
    assert!(matches!(error, Error::Clone(CloneError::Custom { .. })));
    assert_eq!(error.source().unwrap().to_string(), "copy graph failed");
    assert!(matches!(
        copy_value(&1, &opts.with_size(-1), Some(&cloner)),
        Err(Error::Config(ConfigError::NegativeEntryWeight { size: -1 }))
    ));
    assert_eq!(cloner.0.load(Ordering::SeqCst), 1);
}

#[derive(Clone)]
struct SharedValue(Arc<AtomicI32>);
impl serde::Serialize for SharedValue {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_i32(self.0.load(Ordering::SeqCst))
    }
}
impl<'de> serde::Deserialize<'de> for SharedValue {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(Self(Arc::new(AtomicI32::new(
            <i32 as serde::Deserialize>::deserialize(deserializer)?,
        ))))
    }
}

fn codec_isolation(serializer: &dyn DistributedSerializer<SharedValue>) {
    let cloner = serializer
        .value_cloner()
        .expect("builtin codec supplies its isolated copier");
    let input = SharedValue(Arc::new(AtomicI32::new(11)));
    let output = cloner.clone_value(&input).unwrap();
    input.0.store(22, Ordering::SeqCst);
    assert_eq!(output.0.load(Ordering::SeqCst), 11);
    assert!(!Arc::ptr_eq(&input.0, &output.0));
}

#[test]
fn json_codec_supplies_real_isolation() {
    codec_isolation(&JsonSerializer);
}
#[cfg(feature = "messagepack")]
#[test]
fn messagepack_codec_supplies_real_isolation() {
    codec_isolation(&amalgam::MessagePackSerializer);
}
#[cfg(feature = "postcard")]
#[test]
fn postcard_codec_supplies_real_isolation() {
    codec_isolation(&amalgam::PostcardSerializer);
}

#[test]
fn factory_failure_and_cancellation_keep_distinct_typed_sources() {
    let error: Error = FactoryError::from_source(std::io::Error::other("io cause")).into();
    assert_eq!(
        error.source().unwrap().source().unwrap().to_string(),
        "io cause"
    );
    assert_eq!(
        OperationOutcome::from_error(&error),
        OperationOutcome::FactoryError
    );
    for reason in [
        FactoryCancellationReason::CallerCancelled,
        FactoryCancellationReason::HardTimeout,
        FactoryCancellationReason::CacheShutdown,
        FactoryCancellationReason::LeaseLost,
    ] {
        let error: Error = FactoryError::cancelled(reason).into();
        assert!(matches!(error, Error::FactoryCancelled { reason: actual } if actual == reason));
        assert_eq!(
            OperationOutcome::from_error(&error),
            OperationOutcome::Cancelled
        );
    }
}

#[test]
fn snapshot_ordering_insertion_ttl_and_jitter_are_independent() {
    let opts = EntryOptions::new(Duration::from_secs(10)).with_jitter_max(Duration::from_secs(2));
    let jitter = JitterSample::new(Duration::from_secs(1), opts.jitter_max()).unwrap();
    let value = Entry::try_fresh_with_jitter(
        5,
        &opts,
        at(1_000),
        at(7_000),
        jitter,
        Box::from([]),
        None,
        None,
    )
    .unwrap();
    assert_eq!(value.meta().created(), at(1_000));
    assert_eq!(value.meta().inserted_at(), at(7_000));
    assert_eq!(value.meta().logical_expiration(), at(18_000));
    assert_eq!(value.backend_ttl(), Duration::from_secs(11));
    let retried = value.at_insertion(at(12_000));
    assert_eq!(retried.backend_ttl(), Duration::from_secs(6));
    assert_eq!(
        retried.meta().physical_expiration(),
        value.meta().physical_expiration()
    );
    assert_eq!(retried.meta().created(), at(1_000));
    assert!(matches!(
        JitterSample::new(Duration::from_secs(3), opts.jitter_max()),
        Err(ConfigError::InvalidJitterSample { .. })
    ));
    let wrong_bound = JitterSample::new(Duration::from_secs(3), Duration::from_secs(4)).unwrap();
    assert!(matches!(
        Entry::try_fresh_with_jitter(
            5,
            &opts,
            at(0),
            at(0),
            wrong_bound,
            Box::from([]),
            None,
            None
        ),
        Err(Error::Config(ConfigError::InvalidJitterSample { .. }))
    ));
}

#[test]
fn hydration_caps_local_deadlines_recomputes_eager_and_keeps_retention() {
    let source_opts = EntryOptions::new(Duration::from_secs(60))
        .with_fail_safe(
            true,
            Some(Duration::from_secs(90)),
            Some(Duration::from_secs(1)),
        )
        .with_size(8)
        .with_priority(Priority::High);
    let source = entry(42, &source_opts, at(1_000));
    let local = EntryOptions::new(Duration::from_secs(5))
        .with_fail_safe(
            true,
            Some(Duration::from_secs(10)),
            Some(Duration::from_secs(1)),
        )
        .with_size(2)
        .with_priority(Priority::Low)
        .with_eager_refresh(EagerThreshold::new(0.5));
    let hydrated = source
        .for_memory_hydration(&local, at(11_000))
        .unwrap()
        .unwrap();
    assert_eq!(hydrated.meta().created(), at(1_000));
    assert_eq!(hydrated.meta().logical_expiration(), at(16_000));
    assert_eq!(hydrated.meta().physical_expiration(), at(21_000));
    assert_eq!(hydrated.meta().eager_refresh_at(), Some(at(13_500)));
    assert_eq!(hydrated.meta().size(), Some(EntryWeight::new(8)));
    assert_eq!(hydrated.meta().priority(), Priority::High);
    let short_source = entry(
        1,
        &EntryOptions::new(Duration::from_secs(4)).with_fail_safe(
            true,
            Some(Duration::from_secs(30)),
            Some(Duration::from_secs(1)),
        ),
        at(1_000),
    );
    let stale = short_source
        .for_memory_hydration(&local, at(11_000))
        .unwrap()
        .unwrap();
    assert_eq!(stale.meta().logical_expiration(), at(5_000));
    assert_eq!(stale.meta().eager_refresh_at(), None);
    assert!(stale.is_logically_expired(at(11_000)));
    assert!(
        short_source
            .for_memory_hydration(&local, at(31_000))
            .unwrap()
            .is_none()
    );
    let plain = entry(1, &EntryOptions::new(Duration::from_secs(60)), at(1_000));
    let adopted = plain
        .for_memory_hydration(&local, at(11_000))
        .unwrap()
        .unwrap();
    assert_eq!(adopted.meta().size(), Some(EntryWeight::new(2)));
    assert_eq!(adopted.meta().priority(), Priority::Low);
    let explicit_normal = source
        .with_retention(None, Priority::Normal)
        .for_memory_hydration(&local, at(11_000))
        .unwrap()
        .unwrap();
    assert_eq!(explicit_normal.meta().priority(), Priority::Normal);
    assert_eq!(explicit_normal.meta().size(), Some(EntryWeight::new(2)));
}

#[test]
fn invalid_weights_and_deadline_order_are_typed_and_throttle_cannot_extend_physical_life() {
    let invalid = EntryOptions::default().with_size(-3);
    assert!(matches!(
        invalid.validate(),
        Err(ConfigError::NegativeEntryWeight { size: -3 })
    ));
    assert!(matches!(
        Entry::try_fresh_at(1, &invalid, at(0), at(0), Box::from([]), None, None),
        Err(Error::Config(ConfigError::NegativeEntryWeight { .. }))
    ));
    assert!(matches!(
        Entry::try_rehydrate(
            1,
            at(0),
            at(20),
            at(10),
            false,
            None,
            None,
            Box::from([]),
            at(0)
        ),
        Err(Error::Config(ConfigError::InvalidEntryDeadlines))
    ));
    let opts = EntryOptions::new(Duration::from_secs(1)).with_fail_safe(
        true,
        Some(Duration::from_secs(5)),
        Some(Duration::from_secs(10)),
    );
    let source = entry(1, &opts, at(0));
    let throttled = Entry::try_throttled(&source, &opts, at(4_000))
        .unwrap()
        .unwrap();
    assert_eq!(throttled.meta().logical_expiration(), at(5_000));
    assert_eq!(throttled.meta().physical_expiration(), at(5_000));
    assert_eq!(throttled.meta().eager_refresh_at(), None);
    assert!(
        Entry::try_throttled(&source, &opts, at(5_000))
            .unwrap()
            .is_none()
    );
    assert!(Entry::try_throttled(&source, &invalid, at(1_000)).is_err());
}

#[tokio::test]
async fn weighted_limits_reject_oversized_without_affecting_count_budget() {
    let (clock, store) = memory(MemoryLimits::new(Some(3), Some(5)));
    let now = clock.now();
    let opts = EntryOptions::default().with_size(3);
    assert_eq!(
        store
            .insert_at(Arc::from("a"), entry(1, &opts, now), now)
            .await,
        MemoryAdmission::Admitted
    );
    assert_eq!(
        store
            .insert_at(
                Arc::from("oversized"),
                entry(2, &opts.with_size(6), now),
                now
            )
            .await,
        MemoryAdmission::Rejected(CapacityRejection::Oversized)
    );
    assert_eq!(store.get("a").await.unwrap().value_cloned(), 1);
    assert_eq!(store.usage().weight, 3);
    let (clock, count_only) = memory(MemoryLimits::new(Some(2), None));
    for key in ["a", "b"] {
        count_only
            .insert_at(
                Arc::from(key),
                entry(
                    1,
                    &EntryOptions::default().with_entry_weight(EntryWeight::new(u64::MAX)),
                    clock.now(),
                ),
                clock.now(),
            )
            .await;
    }
    assert_eq!(count_only.usage().entries, 2);
    assert_eq!(count_only.usage().weight, u128::from(u64::MAX) * 2);
}

#[tokio::test]
async fn first_admitted_pin_survives_rejection_explicit_remove_and_physical_expiry_work() {
    let (clock, store) = memory(MemoryLimits::new(Some(1), Some(1)));
    let opts = EntryOptions::new(Duration::from_secs(1))
        .with_size(1)
        .with_priority(Priority::NeverRemove);
    assert_eq!(
        store
            .insert_at(
                Arc::from("first"),
                entry(42, &opts, clock.now()),
                clock.now()
            )
            .await,
        MemoryAdmission::Admitted
    );
    for index in 0..20 {
        assert_eq!(
            store
                .insert_at(
                    Arc::from(format!("later-{index}")),
                    entry(index, &opts, clock.now()),
                    clock.now()
                )
                .await,
            MemoryAdmission::Rejected(CapacityRejection::ProtectedCapacity)
        );
    }
    assert_eq!(store.get("first").await.unwrap().value_cloned(), 42);
    assert_eq!(store.usage().entries, 1);
    assert_eq!(
        store
            .insert_at(
                Arc::from("large"),
                entry(0, &opts.clone().with_size(1_000_000), clock.now()),
                clock.now()
            )
            .await,
        MemoryAdmission::Rejected(CapacityRejection::Oversized)
    );
    clock.advance(Duration::from_secs(1));
    store.run_pending_tasks().await;
    assert_eq!(store.usage().entries, 0);
    store
        .insert_at(
            Arc::from("explicit"),
            entry(7, &opts, clock.now()),
            clock.now(),
        )
        .await;
    assert_eq!(store.remove("explicit").await.unwrap().value_cloned(), 7);
}

#[tokio::test]
async fn incomplete_admission_plan_preserves_all_live_contents_and_replacements() {
    let (clock, store) = memory(MemoryLimits::new(None, Some(4)));
    let now = clock.now();
    let normal = EntryOptions::default().with_size(1);
    store
        .insert_at(
            Arc::from("pinned"),
            entry(
                3,
                &normal
                    .clone()
                    .with_size(3)
                    .with_priority(Priority::NeverRemove),
                now,
            ),
            now,
        )
        .await;
    store
        .insert_at(Arc::from("normal"), entry(1, &normal, now), now)
        .await;
    assert_eq!(
        store
            .insert_at(
                Arc::from("candidate"),
                entry(2, &normal.clone().with_size(2), now),
                now
            )
            .await,
        MemoryAdmission::Rejected(CapacityRejection::ProtectedCapacity)
    );
    assert_eq!(store.get("normal").await.unwrap().value_cloned(), 1);
    assert_eq!(store.get("pinned").await.unwrap().value_cloned(), 3);
    assert_eq!(
        store
            .insert_at(
                Arc::from("normal"),
                entry(9, &normal.with_size(5), now),
                now
            )
            .await,
        MemoryAdmission::Rejected(CapacityRejection::Oversized)
    );
    assert_eq!(store.get("normal").await.unwrap().value_cloned(), 1);
    assert_eq!(store.usage().weight, 4);
}

#[tokio::test]
async fn capacity_evicts_low_then_normal_then_high_using_lru_ties() {
    let (clock, store) = memory(MemoryLimits::new(Some(3), None));
    let now = clock.now();
    let options = EntryOptions::default();
    for (key, priority) in [
        ("low-a", Priority::Low),
        ("low-b", Priority::Low),
        ("high-c", Priority::High),
    ] {
        store
            .insert_at(
                Arc::from(key),
                entry(1, &options.clone().with_priority(priority), now),
                now,
            )
            .await;
    }
    assert!(store.get("low-a").await.is_some());
    store
        .insert_at(Arc::from("normal-d"), entry(1, &options, now), now)
        .await;
    assert!(store.get("low-b").await.is_none());
    assert!(store.get("low-a").await.is_some());
    store
        .insert_at(
            Arc::from("high-e"),
            entry(1, &options.clone().with_priority(Priority::High), now),
            now,
        )
        .await;
    assert!(store.get("low-a").await.is_none());
    store
        .insert_at(Arc::from("normal-f"), entry(1, &options, now), now)
        .await;
    assert!(store.get("normal-d").await.is_none());
    assert_eq!(
        store
            .insert_at(
                Arc::from("low-g"),
                entry(1, &options.with_priority(Priority::Low), now),
                now
            )
            .await,
        MemoryAdmission::Rejected(CapacityRejection::ProtectedCapacity)
    );
    assert!(store.get("high-c").await.is_some());
    assert!(store.get("high-e").await.is_some());
    assert!(store.get("normal-f").await.is_some());
}

#[tokio::test]
async fn frozen_clock_survives_real_ttl_in_both_storage_modes() {
    for limits in [MemoryLimits::default(), MemoryLimits::new(Some(2), None)] {
        let (clock, store) = memory(limits);
        let opts = EntryOptions::new(Duration::from_millis(10));
        store
            .insert_at(
                Arc::from("frozen"),
                entry(42, &opts, clock.now()),
                clock.now(),
            )
            .await;
        tokio::time::sleep(Duration::from_millis(45)).await;
        store.run_pending_tasks().await;
        assert_eq!(store.get("frozen").await.unwrap().value_cloned(), 42);
        clock.advance(Duration::from_millis(10));
        assert!(store.get("frozen").await.is_none());
        store.run_pending_tasks().await;
        assert_eq!(store.usage().entries, 0);
    }
}

#[tokio::test]
async fn exact_snapshot_cleanup_and_passive_commit_preserve_equal_timestamp_replacement() {
    for limits in [MemoryLimits::default(), MemoryLimits::new(Some(2), None)] {
        let (clock, store) = memory(limits);
        let now = clock.now();
        let short = EntryOptions::new(Duration::from_millis(1));
        store
            .insert_at(Arc::from("key"), entry(1, &short, now), now)
            .await;
        let old = store.get("key").await.unwrap();
        clock.advance(Duration::from_millis(2));
        let replacement = Entry::try_fresh_with_jitter(
            2,
            &EntryOptions::default(),
            now,
            clock.now(),
            JitterSample::ZERO,
            Box::from([]),
            None,
            None,
        )
        .unwrap();
        store
            .insert_at(Arc::from("key"), replacement, clock.now())
            .await;
        let current = store.get("key").await.unwrap();
        assert_eq!(current.meta().created(), old.meta().created());
        assert!(!current.is_same_instance(&old));
        assert!(store.remove_if_same("key", &old).await.is_none());
        assert_eq!(
            store
                .insert_if_unchanged(
                    Arc::from("key"),
                    Some(&old),
                    entry(3, &EntryOptions::default(), clock.now()),
                    clock.now()
                )
                .await,
            MemoryAdmission::Rejected(CapacityRejection::VersionChanged)
        );
        assert_eq!(
            store
                .insert_if_unchanged(
                    Arc::from("key"),
                    None,
                    entry(4, &EntryOptions::default(), clock.now()),
                    clock.now()
                )
                .await,
            MemoryAdmission::Rejected(CapacityRejection::VersionChanged)
        );
        assert_eq!(store.get("key").await.unwrap().value_cloned(), 2);
    }
}

#[derive(Default)]
struct SessionCounts {
    starts: AtomicUsize,
    stops: AtomicUsize,
    events: Mutex<Vec<(String, CacheEvent)>>,
}

struct SessionPlugin {
    counts: Arc<SessionCounts>,
    fail_start: bool,
    fail_event: bool,
    panic_event: bool,
}
impl SessionPlugin {
    fn healthy(counts: Arc<SessionCounts>) -> Self {
        Self {
            counts,
            fail_start: false,
            fail_event: false,
            panic_event: false,
        }
    }
}
impl Plugin for SessionPlugin {
    fn name(&self) -> &str {
        "session-probe"
    }
    fn on_event(&self, _: &CacheEvent) {
        panic!("session attachment must not call shared legacy events");
    }
    fn attach(
        &self,
        context: &PluginContext,
    ) -> Result<Option<Box<dyn PluginSession>>, PluginError> {
        self.counts.starts.fetch_add(1, Ordering::SeqCst);
        if self.fail_start {
            return Err(PluginError::from_source(
                self.name(),
                PluginStage::Start,
                std::io::Error::other("attachment failed"),
            ));
        }
        Ok(Some(Box::new(ProbeSession {
            counts: self.counts.clone(),
            context: context.clone(),
            fail_event: self.fail_event,
            panic_event: self.panic_event,
        })))
    }
}
struct ProbeSession {
    counts: Arc<SessionCounts>,
    context: PluginContext,
    fail_event: bool,
    panic_event: bool,
}
impl PluginSession for ProbeSession {
    fn on_event(&self, event: &CacheEvent) -> Result<(), PluginError> {
        if self.panic_event {
            panic!("external event panic");
        }
        if self.fail_event {
            return Err(PluginError::from_source(
                "session-probe",
                PluginStage::Event,
                std::io::Error::other("event failure"),
            ));
        }
        self.counts
            .events
            .lock()
            .unwrap()
            .push((self.context.cache_name().to_owned(), event.clone()));
        Ok(())
    }
    fn stop(&self) -> Result<(), PluginError> {
        self.counts.stops.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn host(name: &str, events: Events, plugins: Vec<Arc<dyn Plugin>>) -> PluginHost {
    PluginHost::try_new(
        PluginContext::new(name, format!("{name}-instance"), events).unwrap(),
        plugins,
    )
    .unwrap()
}

#[test]
fn shared_plugin_has_independent_sessions_and_event_handles_do_not_extend_lifecycle() {
    let counts = Arc::new(SessionCounts::default());
    let plugin: Arc<dyn Plugin> = Arc::new(SessionPlugin::healthy(counts.clone()));
    let (events_a, events_b) = (Events::default(), Events::default());
    let host_a = host("alpha", events_a.clone(), vec![plugin.clone()]);
    let host_b = host("beta", events_b.clone(), vec![plugin]);
    assert_eq!(
        events_a
            .emit_checked(CacheEvent::Miss {
                key: Arc::from("a")
            })
            .subscribers,
        0
    );
    events_b.emit(CacheEvent::Miss {
        key: Arc::from("b"),
    });
    assert_eq!(counts.starts.load(Ordering::SeqCst), 2);
    assert_eq!(
        counts
            .events
            .lock()
            .unwrap()
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>(),
        ["alpha", "beta"]
    );
    drop(host_a);
    assert_eq!(counts.stops.load(Ordering::SeqCst), 1);
    events_a.emit(CacheEvent::Miss {
        key: Arc::from("stopped"),
    });
    assert_eq!(counts.events.lock().unwrap().len(), 2);
    drop(host_b);
    assert_eq!(counts.stops.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn registration_guard_detaches_once_and_stopped_host_has_no_new_start_side_effects() {
    let events = Events::default();
    let owner = host("dynamic", events.clone(), vec![]);
    let counts = Arc::new(SessionCounts::default());
    let registration = owner
        .register(Arc::new(SessionPlugin::healthy(counts.clone())))
        .unwrap();
    events.emit(CacheEvent::Set {
        key: Arc::from("k"),
    });
    assert_eq!(registration.stop().unwrap(), PluginStopOutcome::Stopped);
    assert_eq!(
        registration.stop().unwrap(),
        PluginStopOutcome::AlreadyStopped
    );
    registration.wait_stopped().await.unwrap();
    drop(registration);
    events.emit(CacheEvent::Set {
        key: Arc::from("after"),
    });
    assert_eq!(counts.events.lock().unwrap().len(), 1);
    assert_eq!(counts.stops.load(Ordering::SeqCst), 1);
    owner.stop_all();
    assert!(matches!(
        owner.register(Arc::new(SessionPlugin::healthy(counts.clone()))),
        Err(PluginError::HostStopped)
    ));
    assert_eq!(counts.starts.load(Ordering::SeqCst), 1);
}

#[test]
fn failed_attachment_stops_completed_sessions_and_preserves_cause() {
    let counts = Arc::new(SessionCounts::default());
    let mut failing = SessionPlugin::healthy(Arc::new(SessionCounts::default()));
    failing.fail_start = true;
    let context = PluginContext::new("transaction", "id", Events::default()).unwrap();
    let error = PluginHost::try_new(
        context.clone(),
        vec![
            Arc::new(SessionPlugin::healthy(counts.clone())),
            Arc::new(failing),
        ],
    )
    .unwrap_err();
    assert_eq!(error.source().unwrap().to_string(), "attachment failed");
    assert_eq!(counts.starts.load(Ordering::SeqCst), 1);
    assert_eq!(counts.stops.load(Ordering::SeqCst), 1);
    assert!(context.is_stopped());
}

#[test]
fn event_errors_and_panics_are_isolated_from_other_attachments() {
    let counts = Arc::new(SessionCounts::default());
    let mut failing = SessionPlugin::healthy(Arc::new(SessionCounts::default()));
    failing.fail_event = true;
    let mut panicking = SessionPlugin::healthy(Arc::new(SessionCounts::default()));
    panicking.panic_event = true;
    let events = Events::default();
    let owner = host(
        "errors",
        events.clone(),
        vec![
            Arc::new(failing),
            Arc::new(panicking),
            Arc::new(SessionPlugin::healthy(counts.clone())),
        ],
    );
    let emitted = events.emit_checked(CacheEvent::Miss {
        key: Arc::from("key"),
    });
    assert_eq!(emitted.plugin_errors.len(), 2);
    assert!(matches!(
        emitted.plugin_errors[0],
        PluginError::Failure {
            stage: PluginStage::Event,
            ..
        }
    ));
    assert!(matches!(
        emitted.plugin_errors[1],
        PluginError::Panicked {
            stage: PluginStage::Event,
            ..
        }
    ));
    assert_eq!(counts.events.lock().unwrap().len(), 1);
    drop(owner);
}

#[test]
fn plugin_diagnostic_panics_and_required_runtime_are_typed() {
    struct PanickingName;
    impl Plugin for PanickingName {
        fn name(&self) -> &str {
            panic!("invalid diagnostic implementation");
        }
        fn on_event(&self, _: &CacheEvent) {}
    }
    struct AsyncPlugin;
    impl Plugin for AsyncPlugin {
        fn name(&self) -> &str {
            "async"
        }
        fn on_event(&self, _: &CacheEvent) {}
        fn requires_runtime(&self) -> bool {
            true
        }
    }
    let context = || PluginContext::new("cache", "id", Events::default()).unwrap();
    assert!(matches!(
        PluginHost::try_new(context(), vec![Arc::new(PanickingName)]),
        Err(PluginError::Panicked {
            stage: PluginStage::Start,
            ..
        })
    ));
    assert!(matches!(
        PluginHost::try_new(context(), vec![Arc::new(AsyncPlugin)]),
        Err(PluginError::MissingRuntime { .. })
    ));
}

#[tokio::test]
async fn plugin_can_stop_its_own_registration_without_deadlocking() {
    struct SelfStopping {
        guard: Arc<Mutex<Option<amalgam::PluginRegistration>>>,
        outcomes: Arc<Mutex<Vec<PluginStopOutcome>>>,
        stopped: Arc<AtomicUsize>,
    }
    impl Plugin for SelfStopping {
        fn name(&self) -> &str {
            "self-stopping"
        }
        fn on_event(&self, _: &CacheEvent) {
            let guard = self.guard.lock().unwrap();
            self.outcomes
                .lock()
                .unwrap()
                .push(guard.as_ref().unwrap().stop().unwrap());
        }
        fn on_stop(&self) {
            self.stopped.fetch_add(1, Ordering::SeqCst);
        }
    }
    let events = Events::default();
    let owner = host("stop", events.clone(), vec![]);
    let guard = Arc::new(Mutex::new(None));
    let outcomes = Arc::new(Mutex::new(Vec::new()));
    let stopped = Arc::new(AtomicUsize::new(0));
    *guard.lock().unwrap() = Some(
        owner
            .register(Arc::new(SelfStopping {
                guard: guard.clone(),
                outcomes: outcomes.clone(),
                stopped: stopped.clone(),
            }))
            .unwrap(),
    );
    events.emit(CacheEvent::Set {
        key: Arc::from("k"),
    });
    assert_eq!(*outcomes.lock().unwrap(), [PluginStopOutcome::Pending]);
    assert_eq!(stopped.load(Ordering::SeqCst), 1);
    let registration = guard.lock().unwrap().take().unwrap();
    registration.wait_stopped().await.unwrap();
}

#[tokio::test]
async fn storage_eviction_reaches_plugin_and_observer_once() {
    let events = Events::default();
    let mut observer = events.subscribe();
    let counts = Arc::new(SessionCounts::default());
    let owner = host(
        "eviction",
        events.clone(),
        vec![Arc::new(SessionPlugin::healthy(counts.clone()))],
    );
    let clock = Arc::new(ManualClock::new(at(1_000)));
    let store = MemoryStore::with_clock(MemoryLimits::new(Some(1), None), events, clock.clone());
    for key in ["old", "new"] {
        store
            .insert_at(
                Arc::from(key),
                entry(1, &EntryOptions::default(), clock.now()),
                clock.now(),
            )
            .await;
    }
    assert_eq!(
        observer.try_recv().unwrap(),
        CacheEvent::Eviction {
            key: Arc::from("old")
        }
    );
    assert!(observer.try_recv().is_err());
    assert_eq!(counts.events.lock().unwrap().len(), 1);
    drop(owner);
}

#[test]
fn retained_eviction_callbacks_and_value_destructors_reenter_after_unlock() {
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        struct ReentrantPlugin {
            store: Arc<Mutex<Weak<MemoryStore<i32>>>>,
            calls: Arc<AtomicUsize>,
        }
        impl Plugin for ReentrantPlugin {
            fn name(&self) -> &str {
                "reentrant"
            }
            fn on_event(&self, event: &CacheEvent) {
                if let CacheEvent::Eviction { .. } = event {
                    assert_eq!(
                        self.store
                            .lock()
                            .unwrap()
                            .upgrade()
                            .unwrap()
                            .usage()
                            .entries,
                        0
                    );
                    self.calls.fetch_add(1, Ordering::SeqCst);
                }
            }
        }
        struct ReentrantDrop {
            store: Mutex<Weak<MemoryStore<Arc<ReentrantDrop>>>>,
            drops: Arc<AtomicUsize>,
        }
        impl Drop for ReentrantDrop {
            fn drop(&mut self) {
                if let Some(store) = self.store.lock().unwrap().upgrade() {
                    let _ = store.usage();
                }
                self.drops.fetch_add(1, Ordering::SeqCst);
            }
        }
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let clock = Arc::new(ManualClock::new(at(1_000)));
            let events = Events::default();
            let weak = Arc::new(Mutex::new(Weak::new()));
            let calls = Arc::new(AtomicUsize::new(0));
            let owner = host(
                "reenter",
                events.clone(),
                vec![Arc::new(ReentrantPlugin {
                    store: weak.clone(),
                    calls: calls.clone(),
                })],
            );
            let store = Arc::new(MemoryStore::with_clock(
                MemoryLimits::new(Some(1), None),
                events,
                clock.clone(),
            ));
            *weak.lock().unwrap() = Arc::downgrade(&store);
            let opts = EntryOptions::new(Duration::from_millis(1));
            store
                .insert_at(
                    Arc::from("expired"),
                    entry(1, &opts, clock.now()),
                    clock.now(),
                )
                .await;
            clock.advance(Duration::from_millis(1));
            assert!(store.get("expired").await.is_none());
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            drop(owner);
            let drops = Arc::new(AtomicUsize::new(0));
            let store = Arc::new(MemoryStore::with_clock(
                MemoryLimits::new(Some(1), None),
                Events::default(),
                clock.clone(),
            ));
            let value = Arc::new(ReentrantDrop {
                store: Mutex::new(Arc::downgrade(&store)),
                drops: drops.clone(),
            });
            store
                .insert_at(
                    Arc::from("drop"),
                    entry(value, &opts, clock.now()),
                    clock.now(),
                )
                .await;
            clock.advance(Duration::from_millis(1));
            store.run_pending_tasks().await;
            assert_eq!(drops.load(Ordering::SeqCst), 1);
        });
        done_tx.send(()).unwrap();
    });
    done_rx
        .recv_timeout(Duration::from_secs(3))
        .expect("retention must release its lock before external code");
}

#[tokio::test]
async fn resilient_observer_counts_lag_and_continues_to_live_events() {
    let events = Events::with_capacity(2);
    let mut observer = events.subscribe_resilient();
    for index in 0..10 {
        events.emit(CacheEvent::Set {
            key: Arc::from(index.to_string()),
        });
    }
    assert_eq!(
        observer.recv().await.unwrap(),
        CacheEvent::Set {
            key: Arc::from("8")
        }
    );
    assert_eq!(observer.lost_events(), 8);
    assert_eq!(
        observer.recv().await.unwrap(),
        CacheEvent::Set {
            key: Arc::from("9")
        }
    );
    events.emit(CacheEvent::Miss {
        key: Arc::from("live"),
    });
    assert_eq!(
        observer.recv().await.unwrap(),
        CacheEvent::Miss {
            key: Arc::from("live")
        }
    );
    drop(events);
    assert!(observer.recv().await.is_err());
}

#[test]
fn operation_observation_finishes_once_and_drop_reports_cancellation() {
    let events = Events::default();
    let mut observer = events.subscribe();
    let mut operation = OperationObservation::new(
        events.clone(),
        "cache",
        "instance",
        CacheOperation::TryGet,
        Some("key"),
    );
    operation.set_level(CacheLevel::Distributed);
    operation.finish(OperationOutcome::Hit);
    assert!(matches!(
        observer.try_recv().unwrap(),
        CacheEvent::OperationCompleted {
            operation: CacheOperation::TryGet,
            outcome: OperationOutcome::Hit,
            level: Some(CacheLevel::Distributed),
            ..
        }
    ));
    assert!(observer.try_recv().is_err());
    drop(OperationObservation::new(
        events,
        "cache",
        "instance",
        CacheOperation::Set,
        None,
    ));
    assert!(matches!(
        observer.try_recv().unwrap(),
        CacheEvent::OperationCompleted {
            operation: CacheOperation::Set,
            outcome: OperationOutcome::Cancelled,
            ..
        }
    ));
    assert!(observer.try_recv().is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn owning_shutdown_waits_detached_draining_callbacks_and_retains_failures() {
    struct BlockingPlugin {
        entered: std::sync::mpsc::Sender<()>,
        release: Mutex<Option<std::sync::mpsc::Receiver<()>>>,
        stops: Arc<AtomicUsize>,
    }
    struct BlockingSession {
        entered: std::sync::mpsc::Sender<()>,
        release: Mutex<std::sync::mpsc::Receiver<()>>,
        stops: Arc<AtomicUsize>,
    }
    impl Plugin for BlockingPlugin {
        fn name(&self) -> &str {
            "blocking"
        }
        fn on_event(&self, _: &CacheEvent) {
            unreachable!();
        }
        fn attach(&self, _: &PluginContext) -> Result<Option<Box<dyn PluginSession>>, PluginError> {
            Ok(Some(Box::new(BlockingSession {
                entered: self.entered.clone(),
                release: Mutex::new(self.release.lock().unwrap().take().unwrap()),
                stops: self.stops.clone(),
            })))
        }
    }
    impl PluginSession for BlockingSession {
        fn on_event(&self, _: &CacheEvent) -> Result<(), PluginError> {
            self.entered.send(()).unwrap();
            self.release.lock().unwrap().recv().unwrap();
            Ok(())
        }
        fn stop(&self) -> Result<(), PluginError> {
            self.stops.fetch_add(1, Ordering::SeqCst);
            Err(PluginError::from_source(
                "blocking",
                PluginStage::Stop,
                std::io::Error::other("teardown source"),
            ))
        }
    }
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let stops = Arc::new(AtomicUsize::new(0));
    let events = Events::default();
    let owner = host("drain", events.clone(), vec![]);
    let registration = owner
        .register(Arc::new(BlockingPlugin {
            entered: entered_tx,
            release: Mutex::new(Some(release_rx)),
            stops: stops.clone(),
        }))
        .unwrap();
    let callback = std::thread::spawn(move || {
        events.emit(CacheEvent::Set {
            key: Arc::from("k"),
        })
    });
    entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(registration.stop().unwrap(), PluginStopOutcome::Pending);
    let closing = owner.clone();
    let shutdown = tokio::spawn(async move { closing.shutdown().await });
    tokio::time::sleep(Duration::from_millis(15)).await;
    assert!(!shutdown.is_finished());
    release_tx.send(()).unwrap();
    callback.join().unwrap();
    let errors = shutdown.await.unwrap();
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].source().unwrap().to_string(), "teardown source");
    assert_eq!(
        owner.shutdown().await[0].source().unwrap().to_string(),
        "teardown source"
    );
    assert_eq!(stops.load(Ordering::SeqCst), 1);
}

#[test]
fn cloned_shutdown_report_keeps_the_same_nonempty_failure_sources() {
    let report = amalgam::ShutdownError::new(
        amalgam::ShutdownFailure::Plugin(PluginError::from_source(
            "shutdown",
            PluginStage::Stop,
            std::io::Error::other("retained source"),
        )),
        [],
    );
    let again = report.clone();
    assert_eq!(report.failures().len(), 1);
    assert!(std::ptr::eq(
        report.failures().as_ptr(),
        again.failures().as_ptr()
    ));
    let error: Error = report.into();
    assert_eq!(
        OperationOutcome::from_error(&error),
        OperationOutcome::ShutdownError
    );
    assert_eq!(
        error.source().unwrap().source().unwrap().to_string(),
        "retained source"
    );
    assert_eq!(
        OperationOutcome::from_error(&Error::CacheClosed),
        OperationOutcome::CacheClosed
    );
    assert_eq!(
        OperationOutcome::from_error(&Error::OperationCancelled {
            reason: FactoryCancellationReason::CallerCancelled
        }),
        OperationOutcome::Cancelled
    );
}
