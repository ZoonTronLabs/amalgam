//! Selected layer hooks are counted and dispatched after cache coordination.
use amalgam::advanced::*;
use amalgam::provider::*;
use amalgam::*;
use async_trait::async_trait;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::{Notify, Semaphore};

#[derive(Default)]
struct Counts {
    hits: AtomicUsize,
    sets: AtomicUsize,
    reads: AtomicUsize,
    starts: AtomicUsize,
    stops: AtomicUsize,
    evictions: AtomicUsize,
}
struct Observer {
    counts: Arc<Counts>,
    observations: PluginObservations,
}
impl Plugin for Observer {
    fn name(&self) -> &str {
        "layer-probe"
    }
    fn observations(&self) -> PluginObservations {
        self.observations
    }
    fn on_event(&self, _: &CacheEvent) {}
    fn on_layer_event(&self, event: &LayerEvent) {
        match event {
            LayerEvent::Memory(MemoryEvent::Hit { .. }) => {
                self.counts.hits.fetch_add(1, Ordering::SeqCst);
            }
            LayerEvent::Memory(MemoryEvent::Set { .. }) => {
                self.counts.sets.fetch_add(1, Ordering::SeqCst);
            }
            LayerEvent::Memory(MemoryEvent::Eviction { .. }) => {
                self.counts.evictions.fetch_add(1, Ordering::SeqCst);
            }
            LayerEvent::Memory(
                MemoryEvent::Miss { .. } | MemoryEvent::Remove { .. } | MemoryEvent::Expire { .. },
            )
            | LayerEvent::Distributed(_)
            | LayerEvent::Backplane(_) => {}
        }
    }
    fn on_component_read(&self, _: ComponentRead) {
        self.counts.reads.fetch_add(1, Ordering::SeqCst);
    }
    fn on_operation_started(&self, _: CacheOperation) {
        self.counts.starts.fetch_add(1, Ordering::SeqCst);
    }
    fn on_stop(&self) {
        self.counts.stops.fetch_add(1, Ordering::SeqCst);
    }
}
fn observer(counts: Arc<Counts>, observations: PluginObservations) -> Arc<Observer> {
    Arc::new(Observer {
        counts,
        observations,
    })
}

#[tokio::test]
async fn legacy_interest_has_no_new_callbacks_and_selected_counts_survive_stream_lag() {
    for observations in [PluginObservations::Logical, PluginObservations::All] {
        let counts = Arc::new(Counts::default());
        let cache = Cache::<u64>::builder()
            .events_capacity(1)
            .plugin(observer(counts.clone(), observations))
            .try_build()
            .unwrap();
        let mut stream = cache.events().subscribe_layers();
        cache
            .set("key", 17)
            .with_receipt()
            .await
            .unwrap()
            .wait()
            .await
            .unwrap();
        for _ in 0..1000 {
            assert_eq!(cache.try_get("key").await.unwrap().as_ref(), Some(&17));
        }
        assert!(matches!(
            stream.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_))
        ));
        let expected = match observations {
            PluginObservations::Logical => 0,
            PluginObservations::All => 1000,
        };
        assert_eq!(counts.hits.load(Ordering::SeqCst), expected);
        assert_eq!(counts.reads.load(Ordering::SeqCst), expected);
        assert_eq!(
            counts.starts.load(Ordering::SeqCst),
            if expected == 0 { 0 } else { 1001 }
        );
        cache.shutdown().await.unwrap();
        assert_eq!(counts.stops.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn late_layer_attachment_counts_retirement_of_preexisting_values() {
    let cache = Cache::<u64>::builder().try_build().unwrap();
    cache
        .set("key", 17)
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    let counts = Arc::new(Counts::default());
    let registration = cache
        .register_plugin(observer(counts.clone(), PluginObservations::All))
        .unwrap();
    cache
        .remove("key")
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert_eq!(counts.evictions.load(Ordering::SeqCst), 1);
    registration.stop().unwrap();
    cache.shutdown().await.unwrap();
}

struct Reentrant {
    runtime: BlockingRuntime,
    calls: Arc<AtomicUsize>,
}
struct ReentrantSession {
    context: CachePluginContext<u64>,
    runtime: BlockingRuntime,
    calls: Arc<AtomicUsize>,
}
impl CachePlugin<u64> for Reentrant {
    fn name(&self) -> &str {
        "reentrant-layer"
    }
    fn attach(
        &self,
        context: &CachePluginContext<u64>,
    ) -> std::result::Result<Box<dyn PluginSession>, PluginError> {
        Ok(Box::new(ReentrantSession {
            context: context.clone(),
            runtime: self.runtime.clone(),
            calls: self.calls.clone(),
        }))
    }
}
impl PluginSession for ReentrantSession {
    fn observations(&self) -> PluginObservations {
        PluginObservations::All
    }
    fn on_event(&self, _: &CacheEvent) -> std::result::Result<(), PluginError> {
        Ok(())
    }
    fn on_layer_event(&self, event: &LayerEvent) -> std::result::Result<(), PluginError> {
        if let LayerEvent::Memory(MemoryEvent::Set { key }) = event
            && key.as_ref() == "caller"
        {
            let cache = self.context.cache()?.blocking(self.runtime.clone());
            let map =
                |error| PluginError::from_source("reentrant-layer", PluginStage::Event, error);
            assert_eq!(
                cache.try_get("caller").execute().map_err(map)?.as_ref(),
                Some(&23)
            );
            cache
                .remove("caller")
                .with_receipt()
                .execute()
                .map_err(map)?
                .wait()
                .map_err(map)?;
            self.calls.fetch_add(1, Ordering::SeqCst);
        }
        Ok(())
    }
}

#[test]
fn cache_aware_layer_hook_can_read_and_mutate_the_same_key_after_lane_release() {
    for capacity in [None, Some(2)] {
        let runtime = BlockingRuntime::new().unwrap();
        let builder = Cache::<u64>::builder();
        let builder = match capacity {
            None => builder,
            Some(capacity) => builder.max_capacity(capacity),
        };
        let cache = BlockingCache::from_builder(builder).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let registration = cache
            .register_cache_plugin(Arc::new(Reentrant {
                runtime,
                calls: calls.clone(),
            }))
            .unwrap();
        cache
            .set("caller", 23)
            .with_receipt()
            .execute()
            .unwrap()
            .wait()
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(cache.try_get("caller").execute().unwrap().is_none());
        registration.stop().unwrap();
        cache.shutdown().unwrap();
    }
}

struct GatedBackend {
    entered: Notify,
    released: Semaphore,
}
#[async_trait]
impl DistributedCache for GatedBackend {
    async fn get(&self, _: &str) -> Result<Option<amalgam::provider::DistributedBytes>> {
        Ok(None)
    }
    async fn set(&self, _: &str, _: Vec<u8>, _: Option<Duration>) -> Result<()> {
        self.entered.notify_one();
        self.released.acquire().await.unwrap().forget();
        Ok(())
    }
    async fn remove(&self, _: &str) -> Result<()> {
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn detach_waits_for_captured_layer_callbacks_and_cancellation_does_not_lose_them() {
    for complete in [false, true] {
        let backend = Arc::new(GatedBackend {
            entered: Notify::new(),
            released: Semaphore::new(0),
        });
        let cache = Cache::<u64>::builder()
            .distributed(backend.clone())
            .serializer(Arc::new(JsonSerializer))
            .try_build()
            .unwrap();
        let counts = Arc::new(Counts::default());
        let registration = cache
            .register_plugin(observer(counts.clone(), PluginObservations::All))
            .unwrap();
        let owned = cache.clone();
        let writer = tokio::spawn(async move {
            owned
                .set("key", 17)
                .with_receipt()
                .await
                .unwrap()
                .wait()
                .await
                .unwrap()
        });
        tokio::time::timeout(Duration::from_secs(5), backend.entered.notified())
            .await
            .unwrap();
        assert_eq!(
            counts.sets.load(Ordering::SeqCst),
            0,
            "layer callback must wait for the lane"
        );
        assert_eq!(registration.stop().unwrap(), PluginStopOutcome::Pending);
        assert_eq!(counts.stops.load(Ordering::SeqCst), 0);
        if complete {
            backend.released.add_permits(1);
            writer.await.unwrap();
        } else {
            writer.abort();
            assert!(writer.await.unwrap_err().is_cancelled());
            backend.released.add_permits(1);
        }
        tokio::time::timeout(Duration::from_secs(5), registration.wait_stopped())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(counts.sets.load(Ordering::SeqCst), 1);
        assert_eq!(counts.stops.load(Ordering::SeqCst), 1);
        cache.shutdown().await.unwrap();
    }
}

struct Failing {
    source: Arc<std::io::Error>,
    stops: Arc<AtomicUsize>,
}
struct FailureSession {
    source: Arc<std::io::Error>,
    stops: Arc<AtomicUsize>,
}
impl Plugin for Failing {
    fn name(&self) -> &str {
        "failing-layer"
    }
    fn on_event(&self, _: &CacheEvent) {}
    fn attach(
        &self,
        _: &PluginContext,
    ) -> std::result::Result<Option<Box<dyn PluginSession>>, PluginError> {
        Ok(Some(Box::new(FailureSession {
            source: self.source.clone(),
            stops: self.stops.clone(),
        })))
    }
}
impl PluginSession for FailureSession {
    fn observations(&self) -> PluginObservations {
        PluginObservations::All
    }
    fn on_event(&self, _: &CacheEvent) -> std::result::Result<(), PluginError> {
        Ok(())
    }
    fn on_layer_event(&self, _: &LayerEvent) -> std::result::Result<(), PluginError> {
        Err(PluginError::from_source(
            "failing-layer",
            PluginStage::Event,
            self.source.clone(),
        ))
    }
    fn stop(&self) -> std::result::Result<(), PluginError> {
        self.stops.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[test]
fn checked_layer_emission_retains_the_original_failure_and_still_sends_the_stream() {
    let events = Events::default();
    let source = Arc::new(std::io::Error::other("original layer failure"));
    let stops = Arc::new(AtomicUsize::new(0));
    let plugin = Arc::new(Failing {
        source: source.clone(),
        stops: stops.clone(),
    });
    let context = PluginContext::new("name", "node", events.clone()).unwrap();
    let host = PluginHost::try_new(context, vec![plugin]).unwrap();

    let mut stream = events.subscribe_layers();
    let event = LayerEvent::Memory(MemoryEvent::Miss {
        key: Arc::from("key"),
    });
    let result = events.emit_layer_checked(event.clone());
    assert_eq!(result.subscribers, 1);
    assert_eq!(stream.try_recv().unwrap(), event);
    assert_eq!(result.plugin_errors.len(), 1);
    let PluginError::Failure {
        source: original,
        stage: PluginStage::Event,
        ..
    } = &result.plugin_errors[0]
    else {
        panic!("original handler cause must be retained")
    };
    assert!(Arc::ptr_eq(
        original.downcast_ref::<Arc<std::io::Error>>().unwrap(),
        &source
    ));
    assert!(host.stop_all().is_empty());
    assert_eq!(stops.load(Ordering::SeqCst), 1);
}

struct PanickingInterest {
    stops: Arc<AtomicUsize>,
}
impl PluginSession for PanickingInterest {
    fn observations(&self) -> PluginObservations {
        panic!("observer contract failure")
    }
    fn on_event(&self, _: &CacheEvent) -> std::result::Result<(), PluginError> {
        Ok(())
    }
    fn stop(&self) -> std::result::Result<(), PluginError> {
        self.stops.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
struct BadInterest {
    stops: Arc<AtomicUsize>,
}
impl Plugin for BadInterest {
    fn name(&self) -> &str {
        "bad-interest"
    }
    fn on_event(&self, _: &CacheEvent) {}
    fn attach(
        &self,
        _: &PluginContext,
    ) -> std::result::Result<Option<Box<dyn PluginSession>>, PluginError> {
        Ok(Some(Box::new(PanickingInterest {
            stops: self.stops.clone(),
        })))
    }
}
#[test]
fn rejected_interest_introspection_stops_the_already_created_session() {
    let cache = Cache::<u64>::builder().try_build().unwrap();
    let stops = Arc::new(AtomicUsize::new(0));
    assert!(matches!(
        cache.register_plugin(Arc::new(BadInterest {
            stops: stops.clone()
        })),
        Err(Error::Plugin(PluginError::Panicked {
            stage: PluginStage::Start,
            ..
        }))
    ));
    assert_eq!(stops.load(Ordering::SeqCst), 1);
}

#[derive(Clone, Copy)]
enum MarkerFact {
    Read,
    SnapshotWrite,
}
impl MarkerFact {
    fn selects(self, event: &CacheEvent) -> bool {
        let kind = match (self, event) {
            (Self::Read, CacheEvent::MarkerRead { kind, .. })
            | (Self::SnapshotWrite, CacheEvent::MarkerSnapshotWrite { kind, .. }) => kind,
            _ => return false,
        };
        matches!(kind, MarkerKind::Tag(tag) if tag.as_str() == "watched")
    }
}
struct MarkerReentrant {
    runtime: BlockingRuntime,
    fact: MarkerFact,
    succeeded: Arc<AtomicUsize>,
    seen: Arc<AtomicUsize>,
}
struct MarkerSession {
    context: CachePluginContext<u64>,
    runtime: BlockingRuntime,
    fact: MarkerFact,
    seen: Arc<AtomicUsize>,
    succeeded: Arc<AtomicUsize>,
}
impl CachePlugin<u64> for MarkerReentrant {
    fn name(&self) -> &str {
        "marker-reentrant"
    }
    fn attach(
        &self,
        context: &CachePluginContext<u64>,
    ) -> std::result::Result<Box<dyn PluginSession>, PluginError> {
        Ok(Box::new(MarkerSession {
            context: context.clone(),
            runtime: self.runtime.clone(),
            fact: self.fact,
            seen: self.seen.clone(),
            succeeded: self.succeeded.clone(),
        }))
    }
}
impl PluginSession for MarkerSession {
    fn observations(&self) -> PluginObservations {
        PluginObservations::All
    }
    fn on_event(&self, event: &CacheEvent) -> std::result::Result<(), PluginError> {
        if self.fact.selects(event) && self.seen.fetch_add(1, Ordering::SeqCst) == 0 {
            let value = self
                .context
                .cache()?
                .blocking(self.runtime.clone())
                .get_or_set(
                    "caller",
                    typed_blocking_factory(|_| {
                        panic!("completed origin must already be visible to its marker observer")
                    }),
                )
                .options(|_| {
                    EntryOptions::new(Duration::from_secs(60))
                        .with_lock_timeout(Timeout::After(Duration::from_millis(20)))
                })
                .execute()
                .map_err(|error| {
                    PluginError::from_source("marker-reentrant", PluginStage::Event, error)
                })?;
            if value == 29 {
                self.succeeded.fetch_add(1, Ordering::SeqCst);
            }
        }
        Ok(())
    }
}
fn marker_observer_after_value_commit(fact: MarkerFact) {
    let clock = Arc::new(ManualClock::new(Timestamp::from_ticks(100)));
    let backend = Arc::new(InMemoryDistributedCache::new(clock.clone()));
    let store = backend.invalidation_store().unwrap();
    let runtime = BlockingRuntime::new().unwrap();
    runtime
        .run(store.advance(
            &CacheScope::new("", "v2", KeyModifierMode::Prefix).unwrap(),
            MarkerKind::Tag(Tag::new("watched").unwrap()),
            MarkerVersion::new(Timestamp::from_ticks(50)),
        ))
        .unwrap();
    let opts = EntryOptions::new(Duration::from_millis(200))
        .with_fail_safe(
            true,
            Some(Duration::from_secs(60)),
            Some(Duration::from_millis(1)),
        )
        .with_skip_memory(true, false)
        .with_skip_distributed_read_when_stale(false);
    let seed = BlockingCache::<u64>::from_builder(
        Cache::builder()
            .clock(clock.clone())
            .distributed(backend.clone())
            .serializer(Arc::new(JsonSerializer))
            .default_options(opts.clone()),
    )
    .unwrap();
    seed.set("caller", 17)
        .tags([Tag::new("watched").unwrap()])
        .with_receipt()
        .execute()
        .unwrap()
        .wait()
        .unwrap();
    clock.advance(Duration::from_millis(201));
    let succeeded = Arc::new(AtomicUsize::new(0));
    let seen = Arc::new(AtomicUsize::new(0));
    let cache = BlockingCache::<u64>::from_builder(
        Cache::builder()
            .clock(clock)
            .distributed(backend)
            .invalidation_store(store)
            .serializer(Arc::new(JsonSerializer))
            .default_options(opts)
            .tags_default_options(
                EntryOptions::tag_defaults()
                    .with_skip_memory(true, false)
                    .with_lock_timeout(Timeout::After(Duration::from_millis(20))),
            )
            .marker_read_policy(MarkerReadPolicy::OptionsControlled)
            .marker_lifecycle_policy(MarkerLifecyclePolicy::CachedSnapshots)
            .cache_plugin(Arc::new(MarkerReentrant {
                runtime,
                fact,
                succeeded: succeeded.clone(),
                seen: seen.clone(),
            })),
    )
    .unwrap();
    assert_eq!(
        cache
            .get_or_set(
                "caller",
                amalgam::source::factory(|ctx| Ok::<_, amalgam::FactoryError>(ctx.value(29)))
            )
            .execute()
            .unwrap(),
        29
    );
    cache.flush_pending().unwrap();
    assert!(
        seen.load(Ordering::SeqCst) >= 1,
        "fixture must produce the selected marker fact"
    );
    assert_eq!(
        succeeded.load(Ordering::SeqCst),
        1,
        "the marker callback must follow the outer value flight's commit and release"
    );
    cache.shutdown().unwrap();
    seed.shutdown().unwrap();
}
#[test]
fn selected_marker_read_observer_runs_after_the_ordinary_value_flight_and_commit() {
    marker_observer_after_value_commit(MarkerFact::Read);
}
#[test]
fn selected_marker_snapshot_observer_runs_after_the_ordinary_value_flight_and_commit() {
    marker_observer_after_value_commit(MarkerFact::SnapshotWrite);
}

fn typed_blocking_factory<V, F>(factory: F) -> F
where
    F: FnOnce(amalgam::FactoryContext<V>) -> std::result::Result<V, amalgam::FactoryError>,
{
    factory
}
