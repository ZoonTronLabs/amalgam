//! Maintenance clocks are caller code and can reenter the same store.
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak, mpsc};
use std::time::Duration;

use amalgam::{Events, Timestamp, provider::Clock, provider::MemoryLimits, provider::MemoryStore};

struct ReentrantClock {
    store: Mutex<Weak<MemoryStore<i32>>>,
    calls: AtomicUsize,
}

impl Clock for ReentrantClock {
    fn now(&self) -> Timestamp {
        let store = self.store.lock().unwrap().upgrade();
        if let Some(store) = store {
            assert_eq!(store.usage().entries, 0);
            self.calls.fetch_add(1, Ordering::SeqCst);
        }
        Timestamp::from_ticks(10_000_000)
    }
}

#[test]
fn bounded_maintenance_samples_custom_clock_before_storage_lock() {
    let (done, wait) = mpsc::channel();
    std::thread::spawn(move || {
        let clock = Arc::new(ReentrantClock {
            store: Mutex::new(Weak::new()),
            calls: AtomicUsize::new(0),
        });
        let store = Arc::new(MemoryStore::with_clock(
            MemoryLimits::new(Some(1), None),
            Events::default(),
            clock.clone(),
        ));
        *clock.store.lock().unwrap() = Arc::downgrade(&store);
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(store.run_pending_tasks());
        assert_eq!(clock.calls.load(Ordering::SeqCst), 1);
        done.send(()).unwrap();
    });
    wait.recv_timeout(Duration::from_secs(3))
        .expect("a custom maintenance clock must reenter after unlock");
}
