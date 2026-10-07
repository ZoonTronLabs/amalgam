//! Cache-aware plugins use the exact cache without keeping its public owner alive.
use amalgam::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Default)]
struct Record {
    contexts: Mutex<Vec<CachePluginContext<i32>>>,
    held: Mutex<Option<PluginCache<i32>>>,
    starts: AtomicUsize,
    events: AtomicUsize,
    stops: AtomicUsize,
}
#[derive(Clone, Copy)]
enum Start {
    Succeed,
    Fail,
}
#[derive(Clone, Copy)]
enum Stop {
    Succeed,
    Fail,
}
struct Probe {
    runtime: BlockingRuntime,
    record: Arc<Record>,
    start: Start,
    stop: Stop,
}
fn plugin(record: Arc<Record>) -> Arc<Probe> {
    Arc::new(Probe {
        runtime: BlockingRuntime::new().unwrap(),
        record,
        start: Start::Succeed,
        stop: Stop::Succeed,
    })
}
fn boundary<T>(stage: PluginStage, result: Result<T>) -> std::result::Result<T, PluginError> {
    result.map_err(|error| PluginError::from_source("operational-probe", stage, error))
}
impl CachePlugin<i32> for Probe {
    fn name(&self) -> &str {
        "operational-probe"
    }
    fn attach(
        &self,
        context: &CachePluginContext<i32>,
    ) -> std::result::Result<Box<dyn PluginSession>, PluginError> {
        assert_eq!(context.cache_name(), context.cache()?.name());
        assert_eq!(context.instance_id(), context.cache()?.instance_id());
        let cache = context.cache()?.blocking(self.runtime.clone());
        assert_eq!(
            boundary(
                PluginStage::Start,
                cache.get_or_set("plugin-start", |ctx| Ok::<_, amalgam::FactoryError>(
                    ctx.value(17)
                ))
            )?,
            17
        );
        boundary(
            PluginStage::Start,
            cache.try_set_full(
                "plugin-tagged",
                18,
                None,
                Box::from([Tag::new("plugin-tag").unwrap()]),
            ),
        )?
        .wait()
        .map_err(|e| PluginError::from_source(self.name(), PluginStage::Start, e))?;
        assert_eq!(
            boundary(PluginStage::Start, cache.read("plugin-start", None))?.value(),
            Some(&17)
        );
        self.record.contexts.lock().unwrap().push(context.clone());
        self.record.starts.fetch_add(1, Ordering::SeqCst);
        match self.start {
            Start::Succeed => Ok(Box::new(Session {
                context: context.clone(),
                runtime: self.runtime.clone(),
                record: self.record.clone(),
                stop: self.stop,
            })),
            Start::Fail => Err(PluginError::from_source(
                self.name(),
                PluginStage::Start,
                std::io::Error::other("original start failure"),
            )),
        }
    }
}
struct Session {
    context: CachePluginContext<i32>,
    runtime: BlockingRuntime,
    record: Arc<Record>,
    stop: Stop,
}
impl PluginSession for Session {
    fn on_event(&self, event: &CacheEvent) -> std::result::Result<(), PluginError> {
        if let CacheEvent::Set { key } = event
            && key.ends_with("caller")
        {
            let cache = self.context.cache()?.blocking(self.runtime.clone());
            assert_eq!(
                boundary(PluginStage::Event, cache.read("caller", None))?.value(),
                Some(&23)
            );
            boundary(PluginStage::Event, cache.try_set("plugin-event", 24))?
                .wait()
                .map_err(|e| {
                    PluginError::from_source("operational-probe", PluginStage::Event, e)
                })?;
            self.record.events.fetch_add(1, Ordering::SeqCst);
        }
        Ok(())
    }
    fn stop(&self) -> std::result::Result<(), PluginError> {
        let cache = self.context.cache()?.blocking(self.runtime.clone());
        let value = boundary(
            PluginStage::Stop,
            cache.get_or_set("plugin-stop-factory", |ctx| {
                Ok::<_, amalgam::FactoryError>(ctx.value(29))
            }),
        )?;
        assert_eq!(value, 29);
        boundary(PluginStage::Stop, cache.try_set("plugin-stop", 30))?
            .wait()
            .map_err(|e| PluginError::from_source("operational-probe", PluginStage::Stop, e))?;
        boundary(
            PluginStage::Stop,
            cache.try_remove_by_tag(Tag::new("plugin-tag").unwrap()),
        )?
        .wait()
        .map_err(|e| PluginError::from_source("operational-probe", PluginStage::Stop, e))?;
        assert!(!boundary(PluginStage::Stop, cache.read("plugin-tagged", None))?.has_value());
        assert!(matches!(
            cache.shutdown(),
            Err(Error::ReentrantDrain {
                operation: DrainOperation::Shutdown
            })
        ));
        assert!(matches!(
            cache.flush_pending(),
            Err(Error::ReentrantDrain {
                operation: DrainOperation::FlushPending
            })
        ));
        self.record.stops.fetch_add(1, Ordering::SeqCst);
        match self.stop {
            Stop::Succeed => Ok(()),
            Stop::Fail => Err(PluginError::from_source(
                "operational-probe",
                PluginStage::Stop,
                std::io::Error::other("original stop failure"),
            )),
        }
    }
}

#[test]
fn start_event_stop_and_retained_detached_view_use_one_cache() {
    let record = Arc::new(Record::default());
    let cache =
        BlockingCache::from_builder(Cache::<i32>::builder().name("same").key_prefix("p:")).unwrap();
    let registration = cache.register_cache_plugin(plugin(record.clone())).unwrap();
    assert_eq!(cache.read("plugin-start", None).unwrap().value(), Some(&17));
    cache.try_set("caller", 23).unwrap().wait().unwrap();
    assert_eq!(record.events.load(Ordering::SeqCst), 1);
    assert_eq!(cache.read("plugin-event", None).unwrap().value(), Some(&24));
    let retained = record.contexts.lock().unwrap()[0].cache().unwrap();
    registration.stop().unwrap();
    assert_eq!(cache.read("plugin-stop", None).unwrap().value(), Some(&30));
    assert_eq!(
        retained
            .blocking(cache.runtime().clone())
            .read("plugin-stop-factory", None)
            .unwrap()
            .value(),
        Some(&29)
    );
    assert_eq!(record.stops.load(Ordering::SeqCst), 1);
    cache.shutdown().unwrap();
    assert!(matches!(
        retained
            .blocking(cache.runtime().clone())
            .read("plugin-start", None),
        Err(Error::CacheClosed)
    ));
    assert!(matches!(
        record.contexts.lock().unwrap()[0].cache(),
        Err(PluginError::HostStopped)
    ));
}

fn hybrid(backend: Arc<InMemoryDistributedCache>) -> CacheBuilder<i32> {
    Cache::builder()
        .distributed(backend)
        .serializer(Arc::new(JsonSerializer))
}
#[test]
fn shutdown_stop_can_mutate_real_l2_after_normal_operations_close() {
    let record = Arc::new(Record::default());
    let backend = Arc::new(InMemoryDistributedCache::new(Arc::new(SystemClock)));
    let cache =
        BlockingCache::from_builder(hybrid(backend.clone()).cache_plugin(plugin(record.clone())))
            .unwrap();
    cache.shutdown().unwrap();
    assert_eq!(record.stops.load(Ordering::SeqCst), 1);
    assert!(matches!(
        cache.read("plugin-stop", None),
        Err(Error::CacheClosed)
    ));
    let peer = BlockingCache::from_builder(hybrid(backend)).unwrap();
    assert_eq!(
        peer.read("plugin-stop-factory", None).unwrap().value(),
        Some(&29)
    );
    assert_eq!(peer.read("plugin-stop", None).unwrap().value(), Some(&30));
    assert!(!peer.read("plugin-tagged", None).unwrap().has_value());
    peer.shutdown().unwrap();
}

#[test]
fn weak_context_does_not_keep_cache_or_public_lifetime_alive() {
    let record = Arc::new(Record::default());
    let backend = Arc::new(InMemoryDistributedCache::new(Arc::new(SystemClock)));
    {
        let cache = BlockingCache::from_builder(
            hybrid(backend.clone()).cache_plugin(plugin(record.clone())),
        )
        .unwrap();
        assert_eq!(record.starts.load(Ordering::SeqCst), 1);
        cache.shutdown().unwrap();
    }
    assert!(matches!(
        record.contexts.lock().unwrap()[0].cache(),
        Err(PluginError::HostStopped)
    ));
    assert_eq!(Arc::strong_count(&backend), 1);
}

#[test]
fn retained_operational_view_cannot_prevent_last_application_owner_from_closing() {
    let record = Arc::new(Record::default());
    let cache =
        BlockingCache::from_builder(Cache::builder().cache_plugin(plugin(record.clone()))).unwrap();
    let runtime = cache.runtime().clone();
    *record.held.lock().unwrap() = Some(record.contexts.lock().unwrap()[0].cache().unwrap());
    drop(cache);
    assert_eq!(record.stops.load(Ordering::SeqCst), 1);
    let held = record.held.lock().unwrap().take().unwrap();
    assert!(matches!(
        held.blocking(runtime.clone()).read("plugin-start", None),
        Err(Error::CacheClosed)
    ));
    runtime.run(held.shutdown()).unwrap();
}

#[test]
fn external_async_owner_remains_available_inside_its_final_stop_hook() {
    let record = Arc::new(Record::default());
    let backend = Arc::new(InMemoryDistributedCache::new(Arc::new(SystemClock)));
    let runtime = BlockingRuntime::new().unwrap();
    let cache = runtime.run(async {
        hybrid(backend.clone())
            .cache_plugin(plugin(record.clone()))
            .try_build()
            .unwrap()
    });
    drop(cache);
    assert_eq!(record.stops.load(Ordering::SeqCst), 1);
    let peer = BlockingCache::from_builder(hybrid(backend)).unwrap();
    assert_eq!(peer.read("plugin-stop", None).unwrap().value(), Some(&30));
    peer.shutdown().unwrap();
}

#[test]
fn dynamic_start_failure_preserves_source_and_writes_without_invented_stop() {
    let record = Arc::new(Record::default());
    let cache = BlockingCache::<i32>::new().unwrap();
    let failed = Arc::new(Probe {
        runtime: cache.runtime().clone(),
        record: record.clone(),
        start: Start::Fail,
        stop: Stop::Succeed,
    });
    let error = cache.register_cache_plugin(failed).err().unwrap();
    assert!(
        matches!(&error, Error::Plugin(PluginError::Failure { stage: PluginStage::Start, source, .. }) if source.downcast_ref::<std::io::Error>().unwrap().to_string() == "original start failure")
    );
    assert_eq!(record.stops.load(Ordering::SeqCst), 0);
    assert_eq!(cache.read("plugin-start", None).unwrap().value(), Some(&17));
    cache.shutdown().unwrap();
}

#[test]
fn stop_failure_is_preserved_and_cannot_skip_owned_shutdown() {
    let record = Arc::new(Record::default());
    let probe = Arc::new(Probe {
        runtime: BlockingRuntime::new().unwrap(),
        record: record.clone(),
        start: Start::Succeed,
        stop: Stop::Fail,
    });
    let cache = BlockingCache::from_builder(Cache::builder().cache_plugin(probe)).unwrap();
    let error = cache.shutdown().unwrap_err();
    let Error::Shutdown(error) = error else {
        panic!("expected owned shutdown failure")
    };
    assert!(error.failures().iter().any(|failure| matches!(failure, ShutdownFailure::Plugin(PluginError::Failure { stage: PluginStage::Stop, source, .. }) if source.downcast_ref::<std::io::Error>().unwrap().to_string() == "original stop failure")));
    assert_eq!(record.stops.load(Ordering::SeqCst), 1);
    assert!(cache.shutdown().is_err());
    assert_eq!(record.stops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn ordinary_plugin_factory_is_cancelled_by_owner_shutdown() {
    let record = Arc::new(Record::default());
    let cache = Cache::builder()
        .cache_plugin(plugin(record.clone()))
        .try_build()
        .unwrap();
    let view = record.contexts.lock().unwrap()[0].cache().unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let factory_entered = entered.clone();
    let work = tokio::spawn(async move {
        view.get_or_set("parked", move |ctx| async move {
            factory_entered.notify_one();
            ctx.cancellation().cancelled().await;
            Err(ctx.fail("cancelled"))
        })
        .await
    });
    tokio::time::timeout(Duration::from_secs(3), entered.notified())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), cache.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        work.await.unwrap(),
        Err(Error::OperationCancelled {
            reason: FactoryCancellationReason::CacheShutdown
        })
    ));
    assert_eq!(record.stops.load(Ordering::SeqCst), 1);
}

struct ParkedStart {
    probe: Arc<Probe>,
    entered: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    gate: Arc<(Mutex<bool>, std::sync::Condvar)>,
}
impl CachePlugin<i32> for ParkedStart {
    fn name(&self) -> &str {
        "parked-cache-aware-start"
    }
    fn attach(
        &self,
        context: &CachePluginContext<i32>,
    ) -> std::result::Result<Box<dyn PluginSession>, PluginError> {
        let session = self.probe.attach(context)?;
        self.entered
            .lock()
            .unwrap()
            .take()
            .unwrap()
            .send(())
            .unwrap();
        let mut released = self.gate.0.lock().unwrap();
        while !*released {
            released = self.gate.1.wait(released).unwrap();
        }
        Ok(session)
    }
}
#[tokio::test]
async fn startup_that_finishes_after_close_still_receives_operational_stop_and_drains() {
    let record = Arc::new(Record::default());
    let backend = Arc::new(InMemoryDistributedCache::new(Arc::new(SystemClock)));
    let cache = hybrid(backend.clone()).try_build().unwrap();
    let gate = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
    let (entered, started) = tokio::sync::oneshot::channel();
    let adder = cache.clone();
    let pending = Arc::new(ParkedStart {
        probe: plugin(record.clone()),
        entered: Mutex::new(Some(entered)),
        gate: gate.clone(),
    });
    let attachment = tokio::task::spawn_blocking(move || adder.register_cache_plugin(pending));
    tokio::time::timeout(Duration::from_secs(3), started)
        .await
        .unwrap()
        .unwrap();
    cache.close();
    *gate.0.lock().unwrap() = true;
    gate.1.notify_all();
    assert!(attachment.await.unwrap().is_err());
    tokio::time::timeout(Duration::from_secs(3), cache.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.stops.load(Ordering::SeqCst), 1);
    let peer = hybrid(backend).try_build().unwrap();
    assert_eq!(
        peer.read("plugin-stop", None).await.unwrap().value(),
        Some(&30)
    );
    peer.shutdown().await.unwrap();
}

#[derive(Default)]
struct Ordered {
    sequence: Mutex<Vec<i32>>,
}
struct TypedOrder {
    order: Arc<Ordered>,
    position: i32,
    runtime: BlockingRuntime,
}
struct Noop;
impl PluginSession for Noop {
    fn on_event(&self, _: &CacheEvent) -> std::result::Result<(), PluginError> {
        Ok(())
    }
}
impl CachePlugin<i32> for TypedOrder {
    fn name(&self) -> &str {
        "typed-order"
    }
    fn attach(
        &self,
        context: &CachePluginContext<i32>,
    ) -> std::result::Result<Box<dyn PluginSession>, PluginError> {
        let cache = context.cache()?.blocking(self.runtime.clone());
        match self.position {
            1 => assert!(!cache.read("ordered", None).unwrap().has_value()),
            3 => assert_eq!(cache.read("ordered", None).unwrap().value(), Some(&1)),
            _ => panic!("fixture uses first/third positions"),
        }
        cache
            .try_set("ordered", self.position)
            .unwrap()
            .wait()
            .unwrap();
        self.order.sequence.lock().unwrap().push(self.position);
        Ok(Box::new(Noop))
    }
}
struct LegacyOrder(Arc<Ordered>);
impl Plugin for LegacyOrder {
    fn name(&self) -> &str {
        "legacy-order"
    }
    fn on_start(&self) {
        self.0.sequence.lock().unwrap().push(2);
    }
    fn on_event(&self, _: &CacheEvent) {}
}
#[test]
fn builder_preserves_interleaved_legacy_and_operational_plugin_order() {
    let order = Arc::new(Ordered::default());
    let runtime = BlockingRuntime::new().unwrap();
    let cache = BlockingCache::from_builder(
        Cache::builder()
            .cache_plugin(Arc::new(TypedOrder {
                order: order.clone(),
                position: 1,
                runtime: runtime.clone(),
            }))
            .plugin(Arc::new(LegacyOrder(order.clone())))
            .cache_plugin(Arc::new(TypedOrder {
                order: order.clone(),
                position: 3,
                runtime,
            })),
    )
    .unwrap();
    assert_eq!(*order.sequence.lock().unwrap(), [1, 2, 3]);
    assert_eq!(cache.read("ordered", None).unwrap().value(), Some(&3));
    cache.shutdown().unwrap();
}

#[test]
fn one_plugin_definition_gets_independent_operational_sessions_per_cache() {
    let record = Arc::new(Record::default());
    let probe = plugin(record.clone());
    let a = BlockingCache::from_builder(
        Cache::builder()
            .name("a")
            .instance_id("a-owner")
            .cache_plugin(probe.clone()),
    )
    .unwrap();
    let b = BlockingCache::from_builder(
        Cache::builder()
            .name("b")
            .instance_id("b-owner")
            .cache_plugin(probe),
    )
    .unwrap();
    a.try_set("caller", 23).unwrap().wait().unwrap();
    assert_eq!(a.read("plugin-event", None).unwrap().value(), Some(&24));
    assert!(!b.read("plugin-event", None).unwrap().has_value());
    assert_eq!(record.contexts.lock().unwrap()[0].instance_id(), "a-owner");
    assert_eq!(record.contexts.lock().unwrap()[1].instance_id(), "b-owner");
    a.shutdown().unwrap();
    b.shutdown().unwrap();
    assert_eq!(record.stops.load(Ordering::SeqCst), 2);
}

struct GatedStore {
    inner: InMemoryDistributedCache,
    entered: tokio::sync::Notify,
    dropped: std::sync::atomic::AtomicBool,
}
struct DropSignal<'a>(&'a std::sync::atomic::AtomicBool);
impl Drop for DropSignal<'_> {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}
#[async_trait::async_trait]
impl DistributedCache for GatedStore {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.inner.get(key).await
    }
    async fn set(&self, key: &str, bytes: Vec<u8>, ttl: Option<Duration>) -> Result<()> {
        if key.contains("plugin-bg") {
            let _guard = DropSignal(&self.dropped);
            self.entered.notify_one();
            std::future::pending().await
        } else {
            self.inner.set(key, bytes, ttl).await
        }
    }
    async fn remove(&self, key: &str) -> Result<()> {
        self.inner.remove(key).await
    }
    fn invalidation_store(&self) -> Option<Arc<dyn InvalidationStore>> {
        self.inner.invalidation_store()
    }
}
struct BackgroundStop {
    runtime: BlockingRuntime,
    backend: Arc<GatedStore>,
}
struct BackgroundSession {
    context: CachePluginContext<i32>,
    runtime: BlockingRuntime,
    backend: Arc<GatedStore>,
}
impl CachePlugin<i32> for BackgroundStop {
    fn name(&self) -> &str {
        "background-stop"
    }
    fn attach(
        &self,
        context: &CachePluginContext<i32>,
    ) -> std::result::Result<Box<dyn PluginSession>, PluginError> {
        Ok(Box::new(BackgroundSession {
            context: context.clone(),
            runtime: self.runtime.clone(),
            backend: self.backend.clone(),
        }))
    }
}
impl PluginSession for BackgroundSession {
    fn on_event(&self, _: &CacheEvent) -> std::result::Result<(), PluginError> {
        Ok(())
    }
    fn stop(&self) -> std::result::Result<(), PluginError> {
        let cache = self.context.cache()?.blocking(self.runtime.clone());
        let receipt = cache
            .try_set_full(
                "plugin-bg",
                1,
                Some(EntryOptions::default().with_allow_background_distributed_operations(true)),
                Box::from([]),
            )
            .unwrap();
        assert!(matches!(receipt, BlockingMutationReceipt::Scheduled(_)));
        self.runtime.run(async {
            tokio::time::timeout(Duration::from_secs(3), self.backend.entered.notified())
                .await
                .unwrap();
        });
        Ok(())
    }
}
#[test]
fn unfinished_cleanup_work_is_cancelled_and_owned_shutdown_waits_its_destruction() {
    let backend = Arc::new(GatedStore {
        inner: InMemoryDistributedCache::new(Arc::new(SystemClock)),
        entered: tokio::sync::Notify::new(),
        dropped: std::sync::atomic::AtomicBool::new(false),
    });
    let probe = Arc::new(BackgroundStop {
        runtime: BlockingRuntime::new().unwrap(),
        backend: backend.clone(),
    });
    let cache = BlockingCache::<i32>::from_builder(
        Cache::builder()
            .distributed(backend.clone())
            .serializer(Arc::new(JsonSerializer))
            .cache_plugin(probe),
    )
    .unwrap();
    cache.runtime().run(async {
        tokio::time::timeout(Duration::from_secs(3), cache.as_async().shutdown())
            .await
            .unwrap()
            .unwrap();
    });
    assert!(backend.dropped.load(Ordering::SeqCst));
}

#[cfg(feature = "redis")]
#[path = "support/redis_fixture.rs"]
mod redis_fixture;

#[cfg(feature = "redis")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn operational_start_event_and_shutdown_stop_use_native_redis_components() {
    let Some(url) = redis_fixture::redis_url() else {
        return;
    };
    let label = format!("plugin_cache_contract_{:016x}", fastrand::u64(..));
    let prefix = format!("{label}:");
    let backend = Arc::new(RedisDistributedCache::connect(url.clone()).await.unwrap());
    let backplane = Arc::new(RedisBackplane::connect(url.clone()).await.unwrap());
    let locker = Arc::new(RedisDistributedLocker::connect(url).await.unwrap());
    let record = Arc::new(Record::default());
    let cache = Cache::<i32>::builder()
        .name(&label)
        .key_prefix(&prefix)
        .instance_id(format!("{label}-a"))
        .distributed(backend.clone())
        .serializer(Arc::new(JsonSerializer))
        .backplane(backplane.clone())
        .distributed_locker(locker)
        .cache_plugin(plugin(record.clone()))
        .try_build_ready()
        .await
        .unwrap();
    let view = record.contexts.lock().unwrap()[0].cache().unwrap();
    assert!(Arc::ptr_eq(
        cache.distributed_cache().unwrap(),
        view.distributed_cache().unwrap()
    ));
    assert!(Arc::ptr_eq(
        cache.backplane().unwrap(),
        view.backplane().unwrap()
    ));
    assert!(Arc::ptr_eq(
        cache.distributed_locker().unwrap(),
        view.distributed_locker().unwrap()
    ));
    cache
        .try_set("caller", 23)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert_eq!(record.events.load(Ordering::SeqCst), 1);
    assert_eq!(
        cache.read("plugin-event", None).await.unwrap().value(),
        Some(&24)
    );
    tokio::time::timeout(Duration::from_secs(6), cache.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.stops.load(Ordering::SeqCst), 1);
    let peer = Cache::<i32>::builder()
        .name(&label)
        .key_prefix(&prefix)
        .instance_id(format!("{label}-b"))
        .distributed(backend)
        .serializer(Arc::new(JsonSerializer))
        .try_build()
        .unwrap();
    assert_eq!(
        peer.read("plugin-start", None).await.unwrap().value(),
        Some(&17)
    );
    assert_eq!(
        peer.read("plugin-event", None).await.unwrap().value(),
        Some(&24)
    );
    assert_eq!(
        peer.read("plugin-stop-factory", None)
            .await
            .unwrap()
            .value(),
        Some(&29)
    );
    assert_eq!(
        peer.read("plugin-stop", None).await.unwrap().value(),
        Some(&30)
    );
    assert!(!peer.read("plugin-tagged", None).await.unwrap().has_value());
    peer.shutdown().await.unwrap();
    backplane.shutdown().await.unwrap();
}
