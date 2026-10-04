//! Repeated public-handle lifetime checks while external observers stay alive.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use amalgam::{Cache, CacheEvent, EntryOptions, InProcessBackplane, Plugin};

#[derive(Default)]
struct LifecycleCounts {
    started: AtomicUsize,
    stopped: AtomicUsize,
}

impl Plugin for LifecycleCounts {
    fn name(&self) -> &str {
        "lifetime-soak"
    }

    fn on_start(&self) {
        self.started.fetch_add(1, Ordering::SeqCst);
    }

    fn on_event(&self, _: &CacheEvent) {}

    fn on_stop(&self) {
        self.stopped.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeated_last_handle_drop_releases_workers_even_with_live_event_observers() {
    let backplane = Arc::new(InProcessBackplane::default());
    let counts = Arc::new(LifecycleCounts::default());
    for round in 0..8 {
        let mut caches = Vec::with_capacity(64);
        let mut observers = Vec::with_capacity(64);
        for index in 0..64 {
            let cache: Cache<u64> = Cache::builder()
                .name(format!("soak-{round}-{index}"))
                .key_prefix(format!("soak:{round}:{index}:"))
                .backplane(backplane.clone())
                .plugin(counts.clone())
                .default_options(EntryOptions::new(Duration::from_secs(60)))
                .try_build()
                .expect("valid cache");
            cache
                .try_set("value", 42)
                .await
                .expect("set")
                .wait()
                .await
                .expect("commit");
            observers.push(cache.events().clone());
            let surviving_handle = cache.clone();
            drop(cache);
            caches.push(surviving_handle);
        }
        let expected = (round + 1) * 64;
        assert_eq!(counts.started.load(Ordering::SeqCst), expected);
        assert_eq!(counts.stopped.load(Ordering::SeqCst), round * 64);
        drop(caches);
        tokio::time::timeout(Duration::from_secs(3), async {
            while counts.stopped.load(Ordering::SeqCst) != expected
                || Arc::strong_count(&backplane) != 1
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("last public handle must release cache-owned sessions and providers");
        assert_eq!(observers.len(), 64, "external event observers remain alive");
        assert_eq!(Arc::strong_count(&counts), 1);
    }
    assert_eq!(counts.started.load(Ordering::SeqCst), 512);
    assert_eq!(counts.stopped.load(Ordering::SeqCst), 512);
}
