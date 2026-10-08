//! Tag/clear over a genuine byte-only L2, with no atomic capability adapter.
use amalgam::advanced::{
    EffectOutcome, LeasePolicy, MarkerError, MarkerKind, MarkerLifecyclePolicy, MarkerReadPolicy,
    MarkerVersion,
};
use amalgam::provider::{
    DistributedCache, DistributedSerializer, DistributedSnapshot, Entry, InMemoryDistributedCache,
    JsonSerializer, ManualClock,
};
use amalgam::{
    Cache, ClearMode, ConfigError, EntryOptions, Error, FactoryError, RecoveryConfig,
    RemoveByTagBehavior, Result, Tag, Timestamp,
};
use async_trait::async_trait;
use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

#[derive(Debug, thiserror::Error)]
#[error("original byte-only backend failure")]
struct BackendDown;
#[derive(Clone)]
struct Write {
    key: String,
    bytes: Vec<u8>,
    ttl: Option<Duration>,
}
struct Bytes {
    inner: InMemoryDistributedCache,
    down: AtomicBool,
    reads: AtomicUsize,
    removes: AtomicUsize,
    writes: Mutex<Vec<Write>>,
}
impl Bytes {
    fn new(clock: Arc<ManualClock>) -> Arc<Self> {
        Arc::new(Self {
            inner: InMemoryDistributedCache::new(clock),
            down: AtomicBool::new(false),
            reads: AtomicUsize::new(0),
            removes: AtomicUsize::new(0),
            writes: Mutex::new(Vec::new()),
        })
    }
    fn check(&self) -> Result<()> {
        if self.down.load(Ordering::SeqCst) {
            Err(Error::distributed(BackendDown))
        } else {
            Ok(())
        }
    }
    fn marker_writes(&self) -> Vec<Write> {
        self.writes
            .lock()
            .iter()
            .filter(|write| write.key.starts_with("amalgam:byte-marker:"))
            .cloned()
            .collect()
    }
}
#[async_trait]
impl DistributedCache for Bytes {
    async fn get(&self, key: &str) -> Result<Option<amalgam::provider::DistributedBytes>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.check()?;
        self.inner.get(key).await
    }
    async fn set(&self, key: &str, bytes: Vec<u8>, ttl: Option<Duration>) -> Result<()> {
        self.writes.lock().push(Write {
            key: key.to_owned(),
            bytes: bytes.clone(),
            ttl,
        });
        self.check()?;
        self.inner.set(key, bytes, ttl).await
    }
    async fn remove(&self, key: &str) -> Result<()> {
        self.removes.fetch_add(1, Ordering::SeqCst);
        self.check()?;
        self.inner.remove(key).await
    }
    // The actual provider intentionally has neither atomic invalidation nor fencing.
}
fn no_recovery() -> RecoveryConfig {
    RecoveryConfig {
        enabled: false,
        ..RecoveryConfig::default()
    }
}
fn defaults() -> EntryOptions {
    EntryOptions::new(Duration::from_secs(60)).with_fail_safe(
        true,
        Some(Duration::from_secs(120)),
        Some(Duration::from_secs(1)),
    )
}
fn node(clock: &Arc<ManualClock>, bytes: &Arc<Bytes>, prefix: &str) -> Cache<u64> {
    Cache::builder()
        .clock(clock.clone())
        .distributed(bytes.clone())
        .serializer(Arc::new(JsonSerializer))
        .key_prefix(prefix)
        .default_options(defaults())
        .auto_recovery(no_recovery())
        .try_build()
        .unwrap()
}
async fn store(cache: &Cache<u64>) {
    cache
        .set("key", 42)
        .tags(["group"])
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
}
fn has_backend_cause(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut current = Some(error);
    while let Some(cause) = current {
        if cause.is::<BackendDown>() {
            return true;
        }
        current = cause.source();
    }
    false
}

#[tokio::test]
async fn lazy_tag_write_invalidates_a_cold_node_without_touching_value_bytes() {
    let clock = Arc::new(ManualClock::default());
    let bytes = Bytes::new(clock.clone());
    let first = node(&clock, &bytes, "scope:");
    assert!(bytes.invalidation_store().is_none());
    assert_eq!(
        first.marker_read_policy(),
        MarkerReadPolicy::OptionsControlled
    );
    store(&first).await;
    clock.advance(Duration::from_millis(1));
    let request = first.remove_by_tag("group");
    assert!(
        bytes.marker_writes().is_empty(),
        "construction must stay lazy"
    );
    request.with_receipt().await.unwrap().wait().await.unwrap();
    assert!(bytes.inner.get("v2:scope:key").await.unwrap().is_some());
    assert_eq!(bytes.removes.load(Ordering::SeqCst), 0);
    let writes = bytes.marker_writes();
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].ttl, Some(Duration::from_secs(10 * 24 * 60 * 60)));
    let cold = node(&clock, &bytes, "scope:");
    let calls = Arc::new(AtomicUsize::new(0));
    let factory_calls = calls.clone();
    let value = cold
        .get_or_set("key", move |_| async move {
            factory_calls.fetch_add(1, Ordering::SeqCst);
            Ok::<_, FactoryError>(7)
        })
        .await
        .unwrap();
    assert_eq!(value, 7);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    first.shutdown().await.unwrap();
    cold.shutdown().await.unwrap();
}

#[tokio::test]
async fn equal_timestamp_invalidation_is_visible_and_prefixes_are_isolated() {
    let clock = Arc::new(ManualClock::default());
    let bytes = Bytes::new(clock.clone());
    let first = node(&clock, &bytes, "a:");
    let isolated = node(&clock, &bytes, "b:");
    store(&first).await;
    store(&isolated).await;
    first.remove_by_tag("group").await.unwrap();
    let cold = node(&clock, &bytes, "a:");
    assert_eq!(
        cold.get_or_set("key", |_| async { Ok::<_, FactoryError>(7) })
            .await
            .unwrap(),
        7
    );
    let untouched = node(&clock, &bytes, "b:");
    assert_eq!(
        untouched
            .get_or_set("key", |_| async {
                Err::<u64, _>(FactoryError::new("isolated factory must not run"))
            })
            .await
            .unwrap(),
        42
    );
    for cache in [first, isolated, cold, untouched] {
        cache.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn expire_keeps_stale_for_fail_safe_for_tag_and_clear() {
    for clear in [false, true] {
        let clock = Arc::new(ManualClock::default());
        let bytes = Bytes::new(clock.clone());
        let first = node(&clock, &bytes, "");
        store(&first).await;
        clock.advance(Duration::from_millis(1));
        if clear {
            first.clear(ClearMode::Expire).await.unwrap();
        } else {
            first.remove_by_tag("group").await.unwrap();
        }
        let cold = node(&clock, &bytes, "");
        assert_eq!(
            cold.get_or_set("key", |_| async {
                Err::<u64, _>(FactoryError::new("origin unavailable"))
            })
            .await
            .unwrap(),
            42
        );
        first.shutdown().await.unwrap();
        cold.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn remove_rejects_stale_for_fail_safe_for_tag_and_clear() {
    for clear in [false, true] {
        let clock = Arc::new(ManualClock::default());
        let bytes = Bytes::new(clock.clone());
        let first = node(&clock, &bytes, "");
        store(&first).await;
        clock.advance(Duration::from_millis(1));
        if clear {
            first.clear(ClearMode::Remove).await.unwrap();
        } else {
            first.remove_by_tag("group").await.unwrap();
        }
        let cold = Cache::builder()
            .clock(clock.clone())
            .distributed(bytes)
            .serializer(Arc::new(JsonSerializer))
            .default_options(defaults())
            .remove_by_tag_behavior(RemoveByTagBehavior::Remove)
            .auto_recovery(no_recovery())
            .try_build()
            .unwrap();
        assert!(matches!(
            cold.get_or_set("key", |_| async {
                Err::<u64, _>(FactoryError::from_source(BackendDown))
            })
            .await,
            Err(Error::FactoryWithSource { .. })
        ));
        first.shutdown().await.unwrap();
        cold.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn observation_reloads_keep_the_original_remote_revision() {
    let clock = Arc::new(ManualClock::default());
    let bytes = Bytes::new(clock.clone());
    let first = node(&clock, &bytes, "");
    store(&first).await;
    clock.advance(Duration::from_millis(1));
    first.remove_by_tag("group").await.unwrap();
    let original = bytes.marker_writes()[0].bytes.clone();
    let cold = Cache::builder()
        .clock(clock.clone())
        .distributed(bytes.clone())
        .serializer(Arc::new(JsonSerializer))
        .default_options(defaults())
        .tags_default_options(
            EntryOptions::tag_defaults().with_memory_duration(Duration::from_millis(10)),
        )
        .auto_recovery(no_recovery())
        .try_build()
        .unwrap();
    for _ in 0..3 {
        assert_eq!(
            cold.get_or_set("key", |_| async {
                Err::<u64, _>(FactoryError::new("origin unavailable"))
            })
            .await
            .unwrap(),
            42
        );
        clock.advance(Duration::from_secs(2));
    }
    let writes = bytes.marker_writes();
    assert_eq!(
        writes.len(),
        1,
        "re-observation must not rewrite remote marker lifetime"
    );
    assert_eq!(writes[0].bytes, original);
    MarkerVersion::from_ordered_hex(std::str::from_utf8(&original).unwrap()).unwrap();
    first.shutdown().await.unwrap();
    cold.shutdown().await.unwrap();
}

#[tokio::test]
async fn stronger_policies_fail_at_construction_without_provider_io() {
    let clock = Arc::new(ManualClock::default());
    let bytes = Bytes::new(clock.clone());
    let build = || {
        Cache::<u64>::builder()
            .clock(clock.clone())
            .distributed(bytes.clone())
            .serializer(Arc::new(JsonSerializer))
            .auto_recovery(no_recovery())
    };
    assert!(matches!(
        build()
            .marker_read_policy(MarkerReadPolicy::DurableRequired)
            .try_build(),
        Err(Error::Config(ConfigError::AtomicInvalidationUnavailable))
    ));
    assert!(matches!(
        build()
            .strict()
            .lease_policy(LeasePolicy::Cooperative)
            .try_build(),
        Err(Error::Config(ConfigError::AtomicInvalidationUnavailable))
    ));
    assert!(matches!(
        build()
            .marker_lifecycle_policy(MarkerLifecyclePolicy::CachedSnapshots)
            .try_build(),
        Err(Error::Config(
            ConfigError::MarkerSnapshotCapabilityUnavailable
        ))
    ));
    assert_eq!(bytes.reads.load(Ordering::SeqCst), 0);
    assert!(bytes.writes.lock().is_empty());
}

#[tokio::test]
async fn marker_write_failure_preserves_cause_in_suppression_and_rethrow() {
    let clock = Arc::new(ManualClock::default());
    let bytes = Bytes::new(clock.clone());
    let cache = node(&clock, &bytes, "");
    bytes.down.store(true, Ordering::SeqCst);
    let report = cache
        .remove_by_tag("group")
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    match report.distributed {
        EffectOutcome::FailedSuppressed { cause } => assert!(has_backend_cause(&cause)),
        other => panic!("unexpected distributed outcome: {other:?}"),
    }
    let error = cache
        .remove_by_tag("other")
        .options(|options| options.with_rethrow_distributed_exceptions(true))
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Marker(MarkerError::Backend { .. })));
    assert!(has_backend_cause(&error));
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn recovery_keeps_the_captured_revision_and_write_policy() {
    let clock = Arc::new(ManualClock::default());
    let bytes = Bytes::new(clock.clone());
    let cache = Cache::<u64>::builder()
        .clock(clock.clone())
        .distributed(bytes.clone())
        .serializer(Arc::new(JsonSerializer))
        .tags_default_options(
            EntryOptions::tag_defaults()
                .with_fail_safe(false, None, None)
                .with_distributed_duration(Duration::from_secs(30)),
        )
        .auto_recovery(RecoveryConfig {
            delay: Duration::from_millis(30),
            ..RecoveryConfig::default()
        })
        .try_build()
        .unwrap();
    bytes.down.store(true, Ordering::SeqCst);
    let report = cache
        .remove_by_tag("group")
        .options(|options| options.with_distributed_duration(Duration::from_secs(5)))
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(matches!(
        report.distributed,
        EffectOutcome::RecoveryQueued { .. }
    ));
    let original = bytes.marker_writes()[0].bytes.clone();
    clock.advance(Duration::from_secs(2));
    bytes.down.store(false, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let writes = bytes.marker_writes();
            if writes.len() >= 2 && writes.last().unwrap().ttl == Some(Duration::from_secs(5)) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    let writes = bytes.marker_writes();
    assert_eq!(
        writes.last().unwrap().bytes,
        original,
        "recovery cannot refresh the invalidation timestamp"
    );
    assert_eq!(writes.last().unwrap().ttl, Some(Duration::from_secs(5)));
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn corrupt_marker_is_a_typed_protocol_error_with_the_original_utf8_cause() {
    let clock = Arc::new(ManualClock::default());
    let bytes = Bytes::new(clock.clone());
    let first = node(&clock, &bytes, "");
    store(&first).await;
    first.remove_by_tag("group").await.unwrap();
    let key = bytes.marker_writes()[0].key.clone();
    bytes
        .inner
        .set(&key, vec![0xff], Some(Duration::from_secs(60)))
        .await
        .unwrap();
    let cold = Cache::builder()
        .clock(clock)
        .distributed(bytes)
        .serializer(Arc::new(JsonSerializer))
        .tags_default_options(
            EntryOptions::tag_defaults().with_rethrow_serialization_exceptions(true),
        )
        .auto_recovery(no_recovery())
        .try_build()
        .unwrap();
    let error = cold
        .get_or_set("key", |_| async { Ok::<_, FactoryError>(7) })
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        Error::Marker(MarkerError::ProtocolWithSource { .. })
    ));
    let mut source: Option<&dyn std::error::Error> = Some(&error);
    let mut found = false;
    while let Some(cause) = source {
        found |= cause.is::<std::str::Utf8Error>();
        source = cause.source();
    }
    assert!(found);
    first.shutdown().await.unwrap();
    cold.shutdown().await.unwrap();
}

#[tokio::test]
async fn first_ordinary_hot_read_initializes_clear_markers_then_reuses_them() {
    let clock = Arc::new(ManualClock::default());
    let bytes = Bytes::new(clock.clone());
    let cache = node(&clock, &bytes, "");
    cache.set("hot", 42).await.unwrap();
    let before = bytes.reads.load(Ordering::SeqCst);
    assert_eq!(
        cache
            .get_or_set("hot", |_| async { Err::<u64, _>(BackendDown) })
            .await
            .unwrap(),
        42
    );
    let initialized = bytes.reads.load(Ordering::SeqCst);
    assert_eq!(
        initialized - before,
        2,
        "the first untagged read initializes two clear controls, as FC does"
    );
    for _ in 0..3 {
        clock.advance(Duration::from_secs(2));
        assert_eq!(
            cache
                .get_or_set("hot", |_| async { Err::<u64, _>(BackendDown) })
                .await
                .unwrap(),
            42
        );
    }
    assert_eq!(bytes.reads.load(Ordering::SeqCst), initialized);
    cache.shutdown().await.unwrap();
}

#[test]
fn initialized_ordinary_hot_hit_does_not_require_a_runtime() {
    let clock = Arc::new(ManualClock::default());
    let bytes = Bytes::new(clock.clone());
    let cache = {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let cache = node(&clock, &bytes, "");
            cache.set("hot", 42).await.unwrap();
            assert_eq!(
                cache
                    .get_or_set("hot", |_| async { Err::<u64, _>(BackendDown) })
                    .await
                    .unwrap(),
                42
            );
            cache
        })
    };
    assert!(tokio::runtime::Handle::try_current().is_err());
    let reads = bytes.reads.load(Ordering::SeqCst);
    let request = cache.get_or_set("hot", |_| async { Err::<u64, _>(BackendDown) });
    let mut work = std::pin::pin!(std::future::IntoFuture::into_future(request));
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(matches!(
        std::future::Future::poll(work.as_mut(), &mut context),
        std::task::Poll::Ready(Ok(42))
    ));
    assert_eq!(bytes.reads.load(Ordering::SeqCst), reads);
    cache.close();
}

#[tokio::test]
async fn ordinary_marker_boundaries_match_the_released_reference_for_all_three_kinds() {
    for kind in [
        MarkerKind::Tag(Tag::new("group").unwrap()),
        MarkerKind::ClearExpire,
        MarkerKind::ClearRemove,
    ] {
        for difference in [-1, 0, 1] {
            let clock = Arc::new(ManualClock::default());
            let bytes = Bytes::new(clock.clone());
            let writer = node(&clock, &bytes, "");
            store(&writer).await;
            clock.advance(Duration::from_millis(1));
            match &kind {
                MarkerKind::Tag(tag) => writer.remove_by_tag(tag.clone()).await.unwrap(),
                MarkerKind::ClearExpire => writer.clear(ClearMode::Expire).await.unwrap(),
                MarkerKind::ClearRemove => writer.clear(ClearMode::Remove).await.unwrap(),
            }
            let marker = bytes.marker_writes().last().unwrap().clone();
            let revision =
                MarkerVersion::from_ordered_hex(std::str::from_utf8(&marker.bytes).unwrap())
                    .unwrap();
            let created = Timestamp::from_ticks(revision.timestamp().ticks() + difference);
            let options = EntryOptions::new(Duration::from_secs(60));
            let tags = match &kind {
                MarkerKind::Tag(tag) => vec![tag.clone()].into_boxed_slice(),
                MarkerKind::ClearExpire | MarkerKind::ClearRemove => Box::new([]),
            };
            let entry =
                Entry::try_fresh_at(42_u64, &options, created, created, tags, None, None).unwrap();
            let snapshot =
                DistributedSnapshot::from_entry_with_options(&entry, &options, created).unwrap();
            bytes
                .inner
                .set(
                    "v2:key",
                    JsonSerializer.serialize_snapshot(&snapshot).unwrap(),
                    Some(Duration::from_secs(60)),
                )
                .await
                .unwrap();
            clock.advance(Duration::from_secs(1));
            let cold = node(&clock, &bytes, "");
            let result = cold.try_get("key").await.unwrap();
            assert_eq!(
                result.is_some(),
                difference > 0,
                "marker={kind:?}, created-minus-marker={difference}"
            );
            if result.is_some() {
                assert_eq!(result.as_ref(), Some(&42));
            }
            writer.shutdown().await.unwrap();
            cold.shutdown().await.unwrap();
        }
    }
}
