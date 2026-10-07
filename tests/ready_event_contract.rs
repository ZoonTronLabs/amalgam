//! Lazy ready-hit emission retains observers admitted by synchronous user work.
use amalgam::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

#[derive(Default)]
struct RecordingPlugin(Mutex<Vec<CacheEvent>>);
impl Plugin for RecordingPlugin {
    fn name(&self) -> &str {
        "ready-event-recorder"
    }
    fn on_event(&self, event: &CacheEvent) {
        self.0.lock().unwrap().push(event.clone());
    }
}

struct AttachingCloner {
    armed: AtomicBool,
    cache: Mutex<Weak<Cache<u64>>>,
    plugin: Arc<RecordingPlugin>,
    registrations: Mutex<Vec<PluginRegistration>>,
}
impl ValueCloner<u64> for AttachingCloner {
    fn clone_value(&self, value: &u64) -> std::result::Result<u64, CloneError> {
        if self.armed.swap(false, Ordering::SeqCst) {
            let cache = self.cache.lock().unwrap().upgrade().unwrap();
            let registration = cache.register_plugin(self.plugin.clone()).unwrap();
            self.registrations.lock().unwrap().push(registration);
        }
        Ok(*value)
    }
}

fn assert_hit(events: &[CacheEvent], key: &str) {
    assert_eq!(
        events.iter().filter(|event| matches!(event, CacheEvent::Hit { key: actual, stale: false } if actual.as_ref() == key)).count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event,
                CacheEvent::OperationCompleted {
                    outcome: OperationOutcome::Hit,
                    level: Some(CacheLevel::Memory),
                    ..
                }
            ))
            .count(),
        1
    );
}

#[tokio::test]
async fn ready_hit_reaches_a_plugin_attached_by_its_clone_callback() {
    let plugin = Arc::new(RecordingPlugin::default());
    let cloner = Arc::new(AttachingCloner {
        armed: AtomicBool::new(false),
        cache: Mutex::new(Weak::new()),
        plugin: plugin.clone(),
        registrations: Mutex::new(Vec::with_capacity(1)),
    });
    let cache = Arc::new(
        Cache::builder()
            .key_prefix("tenant:")
            .value_cloner(cloner.clone())
            .default_options(EntryOptions::default().with_enable_auto_clone(true))
            .try_build()
            .unwrap(),
    );
    *cloner.cache.lock().unwrap() = Arc::downgrade(&cache);
    cache
        .set("key", 42)
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert_eq!(cache.read("key", None).await.unwrap().value_or(0), 42);
    cloner.armed.store(true, Ordering::SeqCst);
    assert_eq!(cache.read("key", None).await.unwrap().value_or(0), 42);
    assert_hit(&plugin.0.lock().unwrap(), "tenant:key");
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn observers_added_after_an_unobserved_hit_receive_the_next_hit() {
    let cache = Cache::builder().key_prefix("tenant:").try_build().unwrap();
    cache
        .set("key", 42)
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert_eq!(cache.read("key", None).await.unwrap().value_or(0), 42);
    let mut subscriber = cache.events().subscribe();
    let plugin = Arc::new(RecordingPlugin::default());
    let registration = cache.register_plugin(plugin.clone()).unwrap();
    assert_eq!(cache.read("key", None).await.unwrap().value_or(0), 42);
    let mut received = Vec::with_capacity(2);
    while let Ok(event) = subscriber.try_recv() {
        received.push(event);
    }
    assert_hit(&received, "tenant:key");
    assert_hit(&plugin.0.lock().unwrap(), "tenant:key");
    drop(registration);
    cache.shutdown().await.unwrap();
}

struct CloneObservers {
    logical: tokio::sync::broadcast::Receiver<CacheEvent>,
    layers: tokio::sync::broadcast::Receiver<LayerEvent>,
}

struct SubscribingCloner {
    armed: AtomicBool,
    cache: Mutex<Weak<Cache<u64>>>,
    observers: Mutex<Option<CloneObservers>>,
}

impl ValueCloner<u64> for SubscribingCloner {
    fn clone_value(&self, value: &u64) -> std::result::Result<u64, CloneError> {
        if self.armed.swap(false, Ordering::SeqCst) {
            let cache = self.cache.lock().unwrap().upgrade().unwrap();
            *self.observers.lock().unwrap() = Some(CloneObservers {
                logical: cache.events().subscribe(),
                layers: cache.events().subscribe_layers(),
            });
        }
        Ok(*value)
    }
}

#[tokio::test]
async fn ready_hit_reaches_broadcast_observers_created_by_its_clone_callback() {
    let cloner = Arc::new(SubscribingCloner {
        armed: AtomicBool::new(false),
        cache: Mutex::new(Weak::new()),
        observers: Mutex::new(None),
    });
    let cache = Arc::new(
        Cache::builder()
            .key_prefix("tenant:")
            .value_cloner(cloner.clone())
            .default_options(EntryOptions::default().with_enable_auto_clone(true))
            .try_build()
            .unwrap(),
    );
    *cloner.cache.lock().unwrap() = Arc::downgrade(&cache);
    cache
        .set("key", 42)
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert_eq!(cache.read("key", None).await.unwrap().value_or(0), 42);
    cloner.armed.store(true, Ordering::SeqCst);
    assert_eq!(cache.read("key", None).await.unwrap().value_or(0), 42);
    let mut observers = cloner.observers.lock().unwrap().take().unwrap();
    let mut logical = Vec::with_capacity(2);
    while let Ok(event) = observers.logical.try_recv() {
        logical.push(event);
    }
    assert_hit(&logical, "tenant:key");
    assert!(matches!(
        observers.layers.try_recv().unwrap(),
        LayerEvent::Memory(MemoryEvent::Hit { key, stale: false }) if key.as_ref() == "tenant:key"
    ));
    assert!(observers.layers.try_recv().is_err());
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn raw_broadcast_resubscription_after_other_receivers_drop_keeps_emission() {
    let cache = Cache::new();
    cache
        .set("key", 42)
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    let initial = cache.events().subscribe();
    let retained = initial.resubscribe();
    drop(initial);
    let mut current = retained.resubscribe();
    drop(retained);
    assert_eq!(cache.read("key", None).await.unwrap().value_or(0), 42);
    let mut received = Vec::with_capacity(2);
    while let Ok(event) = current.try_recv() {
        received.push(event);
    }
    assert_hit(&received, "key");
    cache.shutdown().await.unwrap();
}
