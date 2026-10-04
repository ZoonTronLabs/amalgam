//! Single-flight locking for cache-stampede protection.
//!
//! Same-key requests normally share one factory. A configured finite lock wait
//! can permit best-effort origin work. Each live key has its own async mutex. Hash-map
//! shards protect only mutex lookup, never the awaited factory: distinct keys
//! cannot deadlock merely because their hashes collide.
//!
//! The map keeps weak references, so removing an idle map slot never replaces a
//! mutex still owned by a holder or waiter. Periodic cleanup bounds idle slots.
//! The guard is an [`OwnedMutexGuard`] so it is `'static + Send` and can
//! be moved into a spawned background task (needed for background factory
//! completion and eager refresh).

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

use dashmap::{DashMap, mapref::entry::Entry};
use tokio::sync::{Mutex, OwnedMutexGuard};

/// Weak slots with bounded incremental reclamation. Each lookup visits at most
/// four queued slots, independent of the number of simultaneous live keys.
pub(crate) struct WeakSlots<T> {
    slots: DashMap<Arc<str>, Weak<T>>,
    sweep: std::sync::Mutex<VecDeque<Arc<str>>>,
    lookups: AtomicUsize,
}

impl<T> WeakSlots<T> {
    pub(crate) fn new() -> Self {
        Self {
            slots: DashMap::new(),
            sweep: std::sync::Mutex::new(VecDeque::new()),
            lookups: AtomicUsize::new(0),
        }
    }

    pub(crate) fn get(&self, key: &str, make: impl FnOnce() -> T) -> Arc<T> {
        if self
            .lookups
            .fetch_add(1, Ordering::Relaxed)
            .is_multiple_of(16)
        {
            self.clean(4);
        }
        let key: Arc<str> = Arc::from(key);
        let (value, inserted) = match self.slots.entry(Arc::clone(&key)) {
            Entry::Occupied(mut slot) => match slot.get().upgrade() {
                Some(value) => (value, false),
                None => {
                    let value = Arc::new(make());
                    slot.insert(Arc::downgrade(&value));
                    (value, false)
                }
            },
            Entry::Vacant(slot) => {
                let value = Arc::new(make());
                slot.insert(Arc::downgrade(&value));
                (value, true)
            }
        };
        if inserted {
            self.sweep
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push_back(key);
            self.clean(4);
        }
        value
    }

    pub(crate) fn clean(&self, budget: usize) {
        let Ok(mut sweep) = self.sweep.try_lock() else {
            return;
        };
        let count = budget.min(sweep.len());
        for _ in 0..count {
            let Some(key) = sweep.pop_front() else {
                break;
            };
            match self.slots.entry(Arc::clone(&key)) {
                Entry::Occupied(slot) if slot.get().strong_count() == 0 => {
                    slot.remove();
                }
                Entry::Occupied(_) => sweep.push_back(key),
                Entry::Vacant(_) => {}
            }
        }
    }
}

impl<T> std::fmt::Debug for WeakSlots<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WeakSlots")
            .field("slots", &self.slots.len())
            .finish()
    }
}

/// Per-key async mutexes with weak, safely reclaimable lookup slots.
#[derive(Debug)]
pub struct KeyedLock {
    locks: WeakSlots<Mutex<()>>,
}

/// The guard returned by acquiring a key's lock. Releasing it (on drop) lets the
/// next waiter for that key proceed.
pub type KeyGuard = OwnedMutexGuard<()>;

impl KeyedLock {
    /// Creates per-key locks. The legacy `shards` argument is retained for source
    /// compatibility; lookup sharding is selected by the map implementation.
    #[must_use]
    pub fn new(_shards: usize) -> Self {
        Self {
            locks: WeakSlots::new(),
        }
    }

    fn mutex_for(&self, key: &str) -> Arc<Mutex<()>> {
        self.locks.get(key, || Mutex::new(()))
    }

    /// Reclaims at most `budget` idle lookup slots.
    pub fn clean_idle(&self, budget: usize) {
        self.locks.clean(budget);
    }

    /// Acquires the lock for `key`, waiting if necessary.
    pub async fn lock(&self, key: &str) -> KeyGuard {
        self.mutex_for(key).lock_owned().await
    }

    /// Tries to acquire the lock for `key` without waiting.
    ///
    /// Returns `None` if another caller currently holds this key — used by
    /// non-blocking paths (eager refresh) that must not stall the caller.
    #[must_use]
    pub fn try_lock(&self, key: &str) -> Option<KeyGuard> {
        self.mutex_for(key).try_lock_owned().ok()
    }
}

impl Default for KeyedLock {
    fn default() -> Self {
        Self::new(1024)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn same_key_serializes() {
        let lock = Arc::new(KeyedLock::new(64));
        let counter = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::with_capacity(16);
        for _ in 0..16 {
            let lock = Arc::clone(&lock);
            let counter = Arc::clone(&counter);
            let max_seen = Arc::clone(&max_seen);
            handles.push(tokio::spawn(async move {
                let _g = lock.lock("hot-key").await;
                let inside = counter.fetch_add(1, Ordering::SeqCst) + 1;
                max_seen.fetch_max(inside, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(5)).await;
                counter.fetch_sub(1, Ordering::SeqCst);
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        // Never more than one holder of the same key at once.
        assert_eq!(max_seen.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn fifty_thousand_active_slots_keep_identity_and_reclaim_after_drop() {
        let slots = WeakSlots::new();
        let keys: Vec<_> = (0..50_000).map(|key| key.to_string()).collect();
        let values: Vec<_> = keys
            .iter()
            .map(|key| slots.get(key, || Mutex::new(())))
            .collect();
        for (key, value) in keys.iter().zip(&values) {
            assert!(Arc::ptr_eq(value, &slots.get(key, || Mutex::new(()))));
        }
        assert_eq!(slots.slots.len(), keys.len());
        assert_eq!(slots.sweep.lock().unwrap().len(), keys.len());
        drop(values);
        slots.clean(keys.len());
        assert!(slots.slots.is_empty());
        assert!(slots.sweep.lock().unwrap().is_empty());
    }
}
