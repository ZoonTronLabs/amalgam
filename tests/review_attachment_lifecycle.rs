use amalgam::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;

#[derive(Clone, Copy)]
enum Teardown {
    Succeed,
    Fail,
}
struct LateSession {
    stopped: Arc<AtomicUsize>,
    teardown: Teardown,
}
impl PluginSession for LateSession {
    fn on_event(&self, _: &CacheEvent) -> std::result::Result<(), PluginError> {
        Ok(())
    }
    fn stop(&self) -> std::result::Result<(), PluginError> {
        self.stopped.fetch_add(1, Ordering::SeqCst);
        match self.teardown {
            Teardown::Succeed => Ok(()),
            Teardown::Fail => Err(PluginError::from_source(
                "late-session",
                PluginStage::Stop,
                std::io::Error::other("original late attachment stop failure"),
            )),
        }
    }
}
struct LatePlugin {
    entered: Mutex<Option<oneshot::Sender<()>>>,
    gate: Arc<(Mutex<bool>, Condvar)>,
    stopped: Arc<AtomicUsize>,
    teardown: Teardown,
}
impl Plugin for LatePlugin {
    fn name(&self) -> &str {
        "late-session"
    }
    fn on_event(&self, _: &CacheEvent) {}
    fn attach(
        &self,
        _: &PluginContext,
    ) -> std::result::Result<Option<Box<dyn PluginSession>>, PluginError> {
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
        Ok(Some(Box::new(LateSession {
            stopped: self.stopped.clone(),
            teardown: self.teardown,
        })))
    }
}
#[derive(Clone)]
enum Owner {
    Cache(Cache<i32>),
    Standalone(PluginHost),
}
impl Owner {
    fn close(&self) {
        match self {
            Self::Cache(cache) => {
                cache.close();
            }
            Self::Standalone(host) => {
                host.stop_all();
            }
        }
    }
    fn notify(&self) {
        let event = CacheEvent::Miss {
            key: Arc::from("drop-boundary"),
        };
        match self {
            Self::Cache(cache) => cache.events().emit(event),
            Self::Standalone(host) => host.notify(&event),
        }
    }
    fn register(&self, plugin: Arc<dyn Plugin>) -> Result<PluginRegistration> {
        match self {
            Self::Cache(cache) => cache.register_plugin(plugin),
            Self::Standalone(host) => host.register(plugin).map_err(Error::from),
        }
    }
    async fn shutdown(&self) -> Vec<PluginError> {
        match self {
            Self::Standalone(host) => host.shutdown().await,
            Self::Cache(cache) => match cache.shutdown().await {
                Ok(_) => Vec::new(),
                Err(Error::Shutdown(error)) => error
                    .failures()
                    .iter()
                    .map(|failure| match failure {
                        ShutdownFailure::Plugin(error) => error.clone(),
                        other => panic!("unexpected shutdown failure: {other:?}"),
                    })
                    .collect(),
                Err(other) => panic!("unexpected shutdown outcome: {other:?}"),
            },
        }
    }
}
async fn late_attachment(owner: Owner, teardown: Teardown) {
    let (entered, started) = oneshot::channel();
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let stopped = Arc::new(AtomicUsize::new(0));
    let plugin = Arc::new(LatePlugin {
        entered: Mutex::new(Some(entered)),
        gate: gate.clone(),
        stopped: stopped.clone(),
        teardown,
    });
    let registration = std::thread::spawn({
        let owner = owner.clone();
        move || owner.register(plugin)
    });
    tokio::time::timeout(Duration::from_secs(1), started)
        .await
        .unwrap()
        .unwrap();
    let premature = tokio::time::timeout(Duration::from_millis(30), owner.shutdown())
        .await
        .is_ok();
    *gate.0.lock().unwrap() = true;
    gate.1.notify_all();
    assert!(registration.join().unwrap().is_err());
    let first = owner.shutdown().await;
    let repeated = owner.shutdown().await;
    assert_eq!(stopped.load(Ordering::SeqCst), 1);
    assert!(
        !premature,
        "shutdown completed while an admitted startup callback still owned resources"
    );
    match teardown {
        Teardown::Succeed => {
            assert!(first.is_empty());
            assert!(repeated.is_empty());
        }
        Teardown::Fail => {
            assert_eq!(first.len(), 1);
            assert_eq!(repeated.len(), 1);
            for errors in [&first, &repeated] {
                assert!(
                    matches!(&errors[0], PluginError::Failure { stage: PluginStage::Stop, source, .. } if source.to_string() == "original late attachment stop failure")
                );
            }
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cache_shutdown_retains_late_attachment_teardown_failure() {
    late_attachment(Owner::Cache(Cache::new()), Teardown::Fail).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standalone_host_shutdown_drains_admitted_attachment() {
    late_attachment(Owner::Standalone(PluginHost::default()), Teardown::Succeed).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standalone_host_retains_late_attachment_teardown_failure() {
    late_attachment(Owner::Standalone(PluginHost::default()), Teardown::Fail).await;
}

struct BlockingBoundary {
    entered: Mutex<Option<oneshot::Sender<()>>>,
    gate: (Mutex<bool>, Condvar),
}
impl BlockingBoundary {
    fn new() -> (Arc<Self>, oneshot::Receiver<()>) {
        let (entered, started) = oneshot::channel();
        (
            Arc::new(Self {
                entered: Mutex::new(Some(entered)),
                gate: (Mutex::new(false), Condvar::new()),
            }),
            started,
        )
    }
    fn block(&self) {
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
    }
    fn release(&self) {
        *self.gate.0.lock().unwrap() = true;
        self.gate.1.notify_all();
    }
}
struct OwnedDropPlugin {
    event: Arc<BlockingBoundary>,
    destruction: Arc<BlockingBoundary>,
    armed: Arc<std::sync::atomic::AtomicBool>,
    dropped: Arc<AtomicUsize>,
}
struct OwnedDropSession {
    event: Arc<BlockingBoundary>,
    destruction: Arc<BlockingBoundary>,
    armed: Arc<std::sync::atomic::AtomicBool>,
    dropped: Arc<AtomicUsize>,
}
impl Plugin for OwnedDropPlugin {
    fn name(&self) -> &str {
        "owned-drop"
    }
    fn on_event(&self, _: &CacheEvent) {}
    fn attach(
        &self,
        _: &PluginContext,
    ) -> std::result::Result<Option<Box<dyn PluginSession>>, PluginError> {
        Ok(Some(Box::new(OwnedDropSession {
            event: self.event.clone(),
            destruction: self.destruction.clone(),
            armed: self.armed.clone(),
            dropped: self.dropped.clone(),
        })))
    }
}
impl PluginSession for OwnedDropSession {
    fn on_event(&self, _: &CacheEvent) -> std::result::Result<(), PluginError> {
        if self.armed.swap(false, Ordering::SeqCst) {
            self.event.block();
        }
        Ok(())
    }
    // Deliberately use the default successful stop hook: RAII is still owned cleanup.
}
impl Drop for OwnedDropSession {
    fn drop(&mut self) {
        self.destruction.block();
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}
#[derive(Clone, Copy)]
enum StopBoundary {
    Idle,
    LastCallback,
}

async fn owned_destruction(owner: Owner, boundary: StopBoundary) {
    let (event, event_started) = BlockingBoundary::new();
    let (destruction, destruction_started) = BlockingBoundary::new();
    let armed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let dropped = Arc::new(AtomicUsize::new(0));
    let registration = owner
        .register(Arc::new(OwnedDropPlugin {
            event: event.clone(),
            destruction: destruction.clone(),
            armed: armed.clone(),
            dropped: dropped.clone(),
        }))
        .unwrap();
    let worker = match boundary {
        StopBoundary::Idle => std::thread::spawn({
            let owner = owner.clone();
            move || owner.close()
        }),
        StopBoundary::LastCallback => {
            armed.store(true, Ordering::SeqCst);
            let worker = std::thread::spawn({
                let owner = owner.clone();
                move || owner.notify()
            });
            tokio::time::timeout(Duration::from_secs(1), event_started)
                .await
                .unwrap()
                .unwrap();
            owner.close();
            event.release();
            worker
        }
    };
    tokio::time::timeout(Duration::from_secs(1), destruction_started)
        .await
        .unwrap()
        .unwrap();
    let early_shutdown = tokio::time::timeout(Duration::from_millis(30), owner.shutdown())
        .await
        .is_ok();
    let early_registration =
        tokio::time::timeout(Duration::from_millis(30), registration.wait_stopped())
            .await
            .is_ok();
    destruction.release();
    worker.join().unwrap();
    assert!(owner.shutdown().await.is_empty());
    registration.wait_stopped().await.unwrap();
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert!(
        !early_shutdown,
        "shutdown completed while the owning session destructor was still running"
    );
    assert!(
        !early_registration,
        "registration reported stopped before the session destructor completed"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cache_shutdown_waits_for_owned_session_destruction() {
    owned_destruction(Owner::Cache(Cache::new()), StopBoundary::Idle).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standalone_shutdown_waits_for_owned_session_destruction() {
    owned_destruction(Owner::Standalone(PluginHost::default()), StopBoundary::Idle).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cache_last_callback_releases_its_pin_before_teardown_completes() {
    owned_destruction(Owner::Cache(Cache::new()), StopBoundary::LastCallback).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standalone_last_callback_releases_its_pin_before_teardown_completes() {
    owned_destruction(
        Owner::Standalone(PluginHost::default()),
        StopBoundary::LastCallback,
    )
    .await;
}
