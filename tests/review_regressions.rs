use amalgam::*;
use async_trait::async_trait;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use tokio::sync::{Notify, Semaphore, oneshot};

fn opts() -> EntryOptions {
    EntryOptions::new(Duration::from_secs(60)).with_allow_background_backplane_operations(false)
}
fn no_recovery() -> RecoveryConfig {
    RecoveryConfig {
        enabled: false,
        ..RecoveryConfig::default()
    }
}
struct PausedRead {
    inner: InMemoryDistributedCache,
    pause_next: AtomicBool,
    started: Notify,
    release: Semaphore,
}
#[async_trait]
impl DistributedCache for PausedRead {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let snapshot = self.inner.get(key).await?;
        if self.pause_next.swap(false, Ordering::SeqCst) {
            self.started.notify_one();
            self.release.acquire().await.unwrap().forget();
        }
        Ok(snapshot)
    }
    async fn set(&self, key: &str, bytes: Vec<u8>, ttl: Option<Duration>) -> Result<()> {
        self.inner.set(key, bytes, ttl).await
    }
    async fn remove(&self, key: &str) -> Result<()> {
        self.inner.remove(key).await
    }
    fn invalidation_store(&self) -> Option<Arc<dyn InvalidationStore>> {
        self.inner.invalidation_store()
    }
}
#[derive(Clone, Copy, Debug)]
enum Lookup {
    Read,
    Legacy,
    Origin,
}
#[derive(Clone, Copy, Debug)]
enum Mutation {
    Set,
    Remove,
}
#[derive(Clone, Copy)]
enum Revision {
    Advance,
    Equal,
}
async fn hydration_race(lookup: Lookup, mutation: Mutation, revision: Revision) {
    let clock = Arc::new(ManualClock::default());
    let backend = Arc::new(PausedRead {
        inner: InMemoryDistributedCache::new(clock.clone()),
        pause_next: AtomicBool::new(false),
        started: Notify::new(),
        release: Semaphore::new(0),
    });
    let build = || {
        Cache::<i32>::builder()
            .clock(clock.clone())
            .distributed(backend.clone())
            .serializer(Arc::new(JsonSerializer))
            .default_options(opts())
            .auto_recovery(no_recovery())
            .try_build()
            .unwrap()
    };
    let writer = build();
    writer.try_set("k", 1).await.unwrap().wait().await.unwrap();
    let reader = build();
    backend.pause_next.store(true, Ordering::SeqCst);
    let flight = {
        let reader = reader.clone();
        tokio::spawn(async move {
            match lookup {
                Lookup::Read => {
                    let _ = reader.read("k", None).await.unwrap();
                }
                Lookup::Legacy => {
                    let _ = reader.try_get("k", None).await;
                }
                Lookup::Origin => {
                    let _ = reader.get_or_set_value("k", 9, None).await.unwrap();
                }
            }
        })
    };
    tokio::time::timeout(Duration::from_secs(1), backend.started.notified())
        .await
        .unwrap();
    match revision {
        Revision::Advance => clock.advance(Duration::from_secs(1)),
        Revision::Equal => {}
    }
    match mutation {
        Mutation::Set => {
            reader.try_set("k", 2).await.unwrap().wait().await.unwrap();
        }
        Mutation::Remove => {
            reader.try_remove("k").await.unwrap().wait().await.unwrap();
        }
    }
    backend.release.add_permits(1);
    flight.await.unwrap();
    let observed = reader
        .read("k", Some(opts().with_skip_distributed(true, false)))
        .await
        .unwrap();
    reader.shutdown().await.unwrap();
    writer.shutdown().await.unwrap();
    match mutation {
        Mutation::Set => assert_eq!(
            observed.into_value(),
            Some(2),
            "{lookup:?} late hydration overwrote newer local Set"
        ),
        Mutation::Remove => assert!(
            !observed.has_value(),
            "{lookup:?} late hydration resurrected awaited Remove"
        ),
    }
}
#[tokio::test]
async fn canonical_hydration_does_not_overwrite_set() {
    hydration_race(Lookup::Read, Mutation::Set, Revision::Advance).await;
}
#[tokio::test]
async fn canonical_hydration_does_not_resurrect_remove() {
    hydration_race(Lookup::Read, Mutation::Remove, Revision::Advance).await;
}
#[tokio::test]
async fn legacy_hydration_does_not_overwrite_set() {
    hydration_race(Lookup::Legacy, Mutation::Set, Revision::Advance).await;
}
#[tokio::test]
async fn legacy_hydration_does_not_resurrect_remove() {
    hydration_race(Lookup::Legacy, Mutation::Remove, Revision::Advance).await;
}
#[tokio::test]
async fn origin_hydration_does_not_overwrite_set() {
    hydration_race(Lookup::Origin, Mutation::Set, Revision::Advance).await;
}
#[tokio::test]
async fn origin_hydration_does_not_resurrect_remove() {
    hydration_race(Lookup::Origin, Mutation::Remove, Revision::Advance).await;
}

async fn overlapping_hydration(lookup: Lookup, bounded: bool) {
    for advance in [Duration::ZERO, Duration::from_secs(1)] {
        let clock = Arc::new(ManualClock::default());
        let backend = Arc::new(PausedRead {
            inner: InMemoryDistributedCache::new(clock.clone()),
            pause_next: AtomicBool::new(false),
            started: Notify::new(),
            release: Semaphore::new(0),
        });
        let build = || {
            let builder = Cache::<i32>::builder()
                .clock(clock.clone())
                .distributed(backend.clone())
                .serializer(Arc::new(JsonSerializer))
                .default_options(opts())
                .auto_recovery(no_recovery());
            if bounded {
                builder.max_capacity(4).try_build().unwrap()
            } else {
                builder.try_build().unwrap()
            }
        };
        let writer = build();
        writer.try_set("k", 1).await.unwrap().wait().await.unwrap();
        let reader = build();
        backend.pause_next.store(true, Ordering::SeqCst);
        let older = tokio::spawn({
            let reader = reader.clone();
            async move {
                match lookup {
                    Lookup::Read => {
                        reader.read("k", None).await.unwrap();
                    }
                    Lookup::Legacy => {
                        reader.try_get("k", None).await;
                    }
                    Lookup::Origin => {
                        reader.get_or_set_value("k", 9, None).await.unwrap();
                    }
                }
            }
        });
        tokio::time::timeout(Duration::from_secs(1), backend.started.notified())
            .await
            .unwrap();
        clock.advance(advance);
        writer.try_set("k", 2).await.unwrap().wait().await.unwrap();
        assert_eq!(reader.read("k", None).await.unwrap().value(), Some(&2));
        backend.release.add_permits(1);
        older.await.unwrap();
        let after = reader
            .read("k", Some(opts().with_skip_distributed(true, false)))
            .await
            .unwrap();
        reader.shutdown().await.unwrap();
        writer.shutdown().await.unwrap();
        assert_eq!(
            after.value(),
            Some(&2),
            "{lookup:?}, bounded={bounded}, delta={advance:?}: older read replaced newer hydration"
        );
    }
}

#[tokio::test]
async fn canonical_overlapping_hydration_preserves_newer_unbounded_entry() {
    overlapping_hydration(Lookup::Read, false).await;
}
#[tokio::test]
async fn canonical_overlapping_hydration_preserves_newer_bounded_entry() {
    overlapping_hydration(Lookup::Read, true).await;
}
#[tokio::test]
async fn legacy_overlapping_hydration_preserves_newer_unbounded_entry() {
    overlapping_hydration(Lookup::Legacy, false).await;
}
#[tokio::test]
async fn legacy_overlapping_hydration_preserves_newer_bounded_entry() {
    overlapping_hydration(Lookup::Legacy, true).await;
}
#[tokio::test]
async fn origin_overlapping_hydration_preserves_newer_unbounded_entry() {
    overlapping_hydration(Lookup::Origin, false).await;
}
#[tokio::test]
async fn origin_overlapping_hydration_preserves_newer_bounded_entry() {
    overlapping_hydration(Lookup::Origin, true).await;
}

struct Session(Arc<AtomicUsize>);
impl PluginSession for Session {
    fn on_event(&self, _: &CacheEvent) -> std::result::Result<(), PluginError> {
        Ok(())
    }
    fn stop(&self) -> std::result::Result<(), PluginError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
struct BlockingAttach {
    entered: Mutex<Option<oneshot::Sender<()>>>,
    gate: Arc<(Mutex<bool>, Condvar)>,
    stopped: Arc<AtomicUsize>,
}
impl Plugin for BlockingAttach {
    fn name(&self) -> &str {
        "blocked-attach"
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
        let (mutex, cv) = &*self.gate;
        let mut released = mutex.lock().unwrap();
        while !*released {
            released = cv.wait(released).unwrap();
        }
        Ok(Some(Box::new(Session(self.stopped.clone()))))
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_waits_for_already_admitted_plugin_attachment() {
    let cache = Cache::<i32>::new();
    let (tx, rx) = oneshot::channel();
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let stopped = Arc::new(AtomicUsize::new(0));
    let plugin = Arc::new(BlockingAttach {
        entered: Mutex::new(Some(tx)),
        gate: gate.clone(),
        stopped: stopped.clone(),
    });
    let registering = {
        let cache = cache.clone();
        std::thread::spawn(move || cache.register_plugin(plugin))
    };
    rx.await.unwrap();
    let premature = tokio::time::timeout(Duration::from_millis(30), cache.shutdown())
        .await
        .is_ok();
    *gate.0.lock().unwrap() = true;
    gate.1.notify_all();
    assert!(registering.join().unwrap().is_err());
    cache.shutdown().await.unwrap();
    assert_eq!(stopped.load(Ordering::SeqCst), 1);
    assert!(
        !premature,
        "shutdown returned before an admitted attachment and its stop hook finished"
    );
}

struct WriteFault {
    inner: InMemoryDistributedCache,
    down: AtomicBool,
    commits: AtomicUsize,
}
#[async_trait]
impl DistributedCache for WriteFault {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.inner.get(key).await
    }
    async fn set(&self, key: &str, bytes: Vec<u8>, ttl: Option<Duration>) -> Result<()> {
        if self.down.load(Ordering::SeqCst) {
            return Err(Error::Distributed("temporary write fault".into()));
        }
        self.inner.set(key, bytes, ttl).await?;
        self.commits.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn remove(&self, key: &str) -> Result<()> {
        self.inner.remove(key).await
    }
    fn invalidation_store(&self) -> Option<Arc<dyn InvalidationStore>> {
        self.inner.invalidation_store()
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_control_does_not_permanently_suspend_connected_recovery() {
    let clock = Arc::new(ManualClock::default());
    let backend = Arc::new(WriteFault {
        inner: InMemoryDistributedCache::new(clock.clone()),
        down: AtomicBool::new(true),
        commits: AtomicUsize::new(0),
    });
    let bp = Arc::new(InProcessBackplane::default());
    let cache = Cache::<i32>::builder()
        .clock(clock.clone())
        .distributed(backend.clone())
        .serializer(Arc::new(JsonSerializer))
        .backplane(bp.clone())
        .reconciliation_policy(ReconciliationPolicy::BackplaneContinuity)
        .default_options(opts())
        .auto_recovery(RecoveryConfig {
            delay: Duration::from_millis(20),
            ..RecoveryConfig::default()
        })
        .try_build()
        .unwrap();
    cache
        .try_set_full(
            "continuity-proof",
            3,
            Some(opts().with_skip_distributed(false, true)),
            Box::from([]),
        )
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    cache.try_set("k", 7).await.unwrap().wait().await.unwrap();
    assert_eq!(cache.pending_recovery(), 1);
    bp.publish(BackplaneMessage {
        source_id: Arc::from("\u{1f}amalgam-control-v2:zz"),
        timestamp: clock.now(),
        action: BackplaneAction::Set,
        key: Arc::from("v2:k"),
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if !cache
                .read(
                    "continuity-proof",
                    Some(opts().with_skip_distributed(true, false)),
                )
                .await
                .unwrap()
                .has_value()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    backend.down.store(false, Ordering::SeqCst);
    clock.advance(Duration::from_secs(1));
    let drained = tokio::time::timeout(Duration::from_secs(1), async {
        while cache.pending_recovery() != 0 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .is_ok();
    let commits = backend.commits.load(Ordering::SeqCst);
    cache.shutdown().await.unwrap();
    assert!(
        drained,
        "connected recovery stayed suspended after rejecting a malformed frame"
    );
    assert_eq!(commits, 1);
}

#[tokio::test]
async fn equal_timestamp_hydration_does_not_overwrite_set() {
    hydration_race(Lookup::Read, Mutation::Set, Revision::Equal).await;
}
#[tokio::test]
async fn equal_timestamp_hydration_does_not_resurrect_remove() {
    hydration_race(Lookup::Read, Mutation::Remove, Revision::Equal).await;
}
