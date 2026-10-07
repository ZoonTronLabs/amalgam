use amalgam::*;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

fn options() -> EntryOptions {
    EntryOptions::new(Duration::from_secs(60))
}
fn recovery() -> RecoveryConfig {
    RecoveryConfig {
        enabled: false,
        ..RecoveryConfig::default()
    }
}

#[derive(Default)]
struct CopyBoundary {
    armed: AtomicBool,
    entered: Notify,
    released: Mutex<bool>,
    release: Condvar,
}
impl CopyBoundary {
    fn block_once(&self) {
        if self.armed.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            drop(
                self.release
                    .wait_while(self.released.lock().unwrap(), |released| !*released)
                    .unwrap(),
            );
        }
    }
    async fn wait(&self) {
        tokio::time::timeout(Duration::from_secs(2), self.entered.notified())
            .await
            .unwrap();
    }
    fn unblock(&self) {
        *self.released.lock().unwrap() = true;
        self.release.notify_all();
    }
}
impl ValueCloner<i32> for CopyBoundary {
    fn clone_value(&self, value: &i32) -> std::result::Result<i32, CloneError> {
        self.block_once();
        Ok(*value)
    }
}

async fn lose_continuity<V: Clone + Send + Sync + 'static>(
    cache: &Cache<V>,
    backplane: &InProcessBackplane,
    clock: &ManualClock,
) {
    backplane
        .publish(BackplaneMessage {
            source_id: "\u{1f}amalgam-control-v2:zz".into(),
            timestamp: clock.now(),
            action: BackplaneAction::Set,
            key: "v2:k".into(),
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if !cache
                .read("proof", Some(options().with_skip_distributed(true, false)))
                .await
                .unwrap()
                .has_value()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[derive(Clone, Copy)]
enum Hydration {
    Read,
    Passive,
}

async fn gap_during_external_cloner(hydration: Hydration, bounded: bool) {
    let clock = Arc::new(ManualClock::default());
    let backend = Arc::new(InMemoryDistributedCache::new(clock.clone()));
    let writer = Cache::<i32>::builder()
        .clock(clock.clone())
        .distributed(backend.clone())
        .serializer(Arc::new(JsonSerializer))
        .auto_recovery(recovery())
        .default_options(options())
        .build();
    writer
        .set("k", 1)
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    let backplane = Arc::new(InProcessBackplane::default());
    let boundary = Arc::new(CopyBoundary::default());
    let builder = Cache::<i32>::builder()
        .clock(clock.clone())
        .distributed(backend.clone())
        .serializer(Arc::new(JsonSerializer))
        .value_cloner(boundary.clone())
        .backplane(backplane.clone())
        .reconciliation_policy(ReconciliationPolicy::BackplaneContinuity)
        .auto_recovery(recovery())
        .default_options(options().with_enable_auto_clone(true));
    let reader = if bounded {
        builder.max_capacity(4).build()
    } else {
        builder.build()
    };
    reader
        .set("proof", 3)
        .options(|_| options().with_skip_distributed(false, true))
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    let read = match hydration {
        Hydration::Read => {
            boundary.armed.store(true, Ordering::SeqCst);
            Some(tokio::spawn({
                let reader = reader.clone();
                async move { reader.read("k", None).await }
            }))
        }
        Hydration::Passive => {
            reader
                .set("k", 0)
                .options(|_| options().with_skip_distributed(false, true))
                .with_receipt()
                .await
                .unwrap()
                .wait()
                .await
                .unwrap();
            boundary.armed.store(true, Ordering::SeqCst);
            backplane
                .publish(BackplaneMessage {
                    source_id: "remote".into(),
                    timestamp: clock.now(),
                    action: BackplaneAction::Set,
                    key: "v2:k".into(),
                })
                .await
                .unwrap();
            None
        }
    };
    boundary.wait().await;
    lose_continuity(&reader, &backplane, &clock).await;
    writer
        .remove("k")
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    boundary.unblock();
    if let Some(read) = read {
        read.await.unwrap().unwrap();
    }
    // Drain passive work before observing a subsequent read.
    reader.flush_pending().await.unwrap();
    let after = reader.read("k", None).await.unwrap();
    reader.shutdown().await.unwrap();
    writer.shutdown().await.unwrap();
    assert!(
        !after.has_value(),
        "hydration crossing a continuity gap repopulated L1"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordinary_cloner_cannot_repopulate_unbounded_memory_after_a_gap() {
    gap_during_external_cloner(Hydration::Read, false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordinary_cloner_cannot_repopulate_bounded_memory_after_a_gap() {
    gap_during_external_cloner(Hydration::Read, true).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn passive_cloner_cannot_repopulate_unbounded_memory_after_a_gap() {
    gap_during_external_cloner(Hydration::Passive, false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn passive_cloner_cannot_repopulate_bounded_memory_after_a_gap() {
    gap_during_external_cloner(Hydration::Passive, true).await;
}

#[derive(Serialize, Deserialize)]
struct Payload {
    number: i32,
    #[serde(skip)]
    insertion_copy: Option<Arc<CopyBoundary>>,
}
impl Clone for Payload {
    fn clone(&self) -> Self {
        if let Some(boundary) = &self.insertion_copy {
            boundary.block_once();
        }
        Self {
            number: self.number,
            insertion_copy: self.insertion_copy.clone(),
        }
    }
}
struct InsertionCloner {
    armed: AtomicBool,
    clock: Arc<ManualClock>,
    boundary: Arc<CopyBoundary>,
}
impl ValueCloner<Payload> for InsertionCloner {
    fn clone_value(&self, value: &Payload) -> std::result::Result<Payload, CloneError> {
        let insertion_copy = if self.armed.swap(false, Ordering::SeqCst) {
            // Force at_insertion to copy after hydration's final fence check.
            self.clock.advance(Duration::from_millis(1));
            self.boundary.armed.store(true, Ordering::SeqCst);
            Some(self.boundary.clone())
        } else {
            None
        };
        Ok(Payload {
            number: value.number,
            insertion_copy,
        })
    }
}
async fn gap_during_storage_insertion(bounded: bool) {
    let clock = Arc::new(ManualClock::default());
    let backend = Arc::new(InMemoryDistributedCache::new(clock.clone()));
    let writer = Cache::<Payload>::builder()
        .clock(clock.clone())
        .distributed(backend.clone())
        .serializer(Arc::new(JsonSerializer))
        .auto_recovery(recovery())
        .default_options(options())
        .build();
    writer
        .set(
            "k",
            Payload {
                number: 1,
                insertion_copy: None,
            },
        )
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    let backplane = Arc::new(InProcessBackplane::default());
    let boundary = Arc::new(CopyBoundary::default());
    let cloner = Arc::new(InsertionCloner {
        armed: AtomicBool::new(false),
        clock: clock.clone(),
        boundary: boundary.clone(),
    });
    let builder = Cache::<Payload>::builder()
        .clock(clock.clone())
        .distributed(backend.clone())
        .serializer(Arc::new(JsonSerializer))
        .value_cloner(cloner.clone())
        .backplane(backplane.clone())
        .reconciliation_policy(ReconciliationPolicy::BackplaneContinuity)
        .auto_recovery(recovery())
        .default_options(options().with_enable_auto_clone(true));
    let reader = if bounded {
        builder.max_capacity(4).build()
    } else {
        builder.build()
    };
    reader
        .set(
            "proof",
            Payload {
                number: 3,
                insertion_copy: None,
            },
        )
        .options(|_| options().with_skip_distributed(false, true))
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    cloner.armed.store(true, Ordering::SeqCst);
    let read = tokio::spawn({
        let reader = reader.clone();
        async move { reader.read("k", None).await }
    });
    boundary.wait().await;
    lose_continuity(&reader, &backplane, &clock).await;
    writer
        .remove("k")
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    boundary.unblock();
    read.await.unwrap().unwrap();
    let after = reader.read("k", None).await.unwrap();
    reader.shutdown().await.unwrap();
    writer.shutdown().await.unwrap();
    assert!(
        !after.has_value(),
        "storage insertion after the fence check made a pre-gap value readable"
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unbounded_storage_insertion_keeps_continuity_eligibility() {
    gap_during_storage_insertion(false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bounded_storage_insertion_keeps_continuity_eligibility() {
    gap_during_storage_insertion(true).await;
}
