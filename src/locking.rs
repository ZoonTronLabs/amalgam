//! Single-flight locking for cache-stampede protection.
//!
//! FusionCache guarantees that, per key, only one factory runs at a time; other
//! callers await that result. We implement this with a fixed bank of sharded
//! async mutexes: a key always maps to the same shard, so concurrent calls for
//! the same key serialize on the same lock. Distinct keys that happen to share a
//! shard may serialize too, but this only affects throughput, never
//! correctness — after acquiring the lock each caller re-checks its *own* key.
//!
//! Sharding (rather than a per-key map) keeps memory bounded and side-steps the
//! notoriously race-prone "remove the lock entry when the last waiter leaves"
//! cleanup. The guard is an [`OwnedMutexGuard`] so it is `'static + Send` and can
//! be moved into a spawned background task (needed for background factory
//! completion and eager refresh).

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use tokio::sync::{Mutex, OwnedMutexGuard};

/// A bank of sharded async mutexes providing per-key single-flight.
#[derive(Debug)]
pub struct KeyedLock {
    shards: Box<[Arc<Mutex<()>>]>,
    mask: usize,
}

/// The guard returned by acquiring a key's lock. Releasing it (on drop) lets the
/// next waiter for that shard proceed.
pub type KeyGuard = OwnedMutexGuard<()>;

impl KeyedLock {
    /// Creates a lock bank with at least `shards` shards (rounded up to a power
    /// of two for fast masking).
    #[must_use]
    pub fn new(shards: usize) -> Self {
        let count = shards.max(1).next_power_of_two();
        let shards = (0..count).map(|_| Arc::new(Mutex::new(()))).collect();
        Self {
            shards,
            mask: count - 1,
        }
    }

    fn shard_for(&self, key: &str) -> &Arc<Mutex<()>> {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let idx = (hasher.finish() as usize) & self.mask;
        &self.shards[idx]
    }

    /// Acquires the lock for `key`, waiting if necessary.
    pub async fn lock(&self, key: &str) -> KeyGuard {
        Arc::clone(self.shard_for(key)).lock_owned().await
    }

    /// Tries to acquire the lock for `key` without waiting.
    ///
    /// Returns `None` if another caller currently holds the shard — used by
    /// non-blocking paths (eager refresh) that must not stall the caller.
    #[must_use]
    pub fn try_lock(&self, key: &str) -> Option<KeyGuard> {
        Arc::clone(self.shard_for(key)).try_lock_owned().ok()
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
}
