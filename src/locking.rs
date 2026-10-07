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

use ahash::RandomState;
use hashbrown::{HashMap, hash_map::RawEntryMut};
use parking_lot::Mutex as ShardMutex;
use tokio::sync::{Mutex, OwnedMutexGuard};

const SLOT_SHARDS: usize = 64;
struct SlotShard<T> {
    entries: HashMap<Arc<str>, Weak<T>, RandomState>,
    sweep: VecDeque<Arc<str>>,
}
impl<T> SlotShard<T> {
    fn clean(&mut self, budget: usize) -> usize {
        let count = budget.min(self.sweep.len());
        for _ in 0..count {
            let key = self.sweep.pop_front().expect("count bounds the live queue");
            if self
                .entries
                .get(key.as_ref())
                .is_some_and(|slot| slot.strong_count() == 0)
            {
                self.entries.remove(key.as_ref());
            } else if self.entries.contains_key(key.as_ref()) {
                self.sweep.push_back(key);
            }
        }
        count
    }
}

/// Weak identity slots, with local bounded reclamation in the same shard.
/// No lookup writes a process-wide counter or locks a separate sweep queue.
pub(crate) struct WeakSlots<T> {
    // Only explicit maintenance advances this cursor, never a lookup.
    maintenance: AtomicUsize,
    hash: RandomState,
    shards: Box<[ShardMutex<SlotShard<T>>]>,
}
impl<T> WeakSlots<T> {
    pub(crate) fn new() -> Self {
        let hash = RandomState::new();
        Self {
            maintenance: AtomicUsize::new(0),
            shards: (0..SLOT_SHARDS)
                .map(|_| {
                    ShardMutex::new(SlotShard {
                        entries: HashMap::with_hasher(hash.clone()),
                        sweep: VecDeque::new(),
                    })
                })
                .collect(),
            hash,
        }
    }
    pub(crate) fn get(&self, key: &str, make: impl FnOnce() -> T) -> Arc<T> {
        self.get_with(key, make, Arc::clone)
    }
    pub(crate) fn get_with<R>(
        &self,
        key: &str,
        make: impl FnOnce() -> T,
        inspect: impl FnOnce(&Arc<T>) -> R,
    ) -> R {
        self.lookup(key, || Arc::from(key), make, inspect)
    }
    pub(crate) fn get_arc_with<R>(
        &self,
        key: Arc<str>,
        make: impl FnOnce() -> T,
        inspect: impl FnOnce(&Arc<T>) -> R,
    ) -> R {
        // A supplied key's allocation is reused only for a new lookup slot.
        self.lookup(key.as_ref(), || Arc::clone(&key), make, inspect)
    }
    fn lookup<R>(
        &self,
        key: &str,
        owned_key: impl FnOnce() -> Arc<str>,
        make: impl FnOnce() -> T,
        inspect: impl FnOnce(&Arc<T>) -> R,
    ) -> R {
        let hash = self.hash.hash_one(key);
        let mut shard = self.shards[hash as usize & (SLOT_SHARDS - 1)].lock();
        shard.clean(4);
        let SlotShard { entries, sweep } = &mut *shard;
        match entries
            .raw_entry_mut()
            .from_hash(hash, |stored| stored.as_ref() == key)
        {
            RawEntryMut::Occupied(mut slot) => match slot.get().upgrade() {
                Some(value) => inspect(&value),
                None => {
                    let value = Arc::new(make());
                    slot.insert(Arc::downgrade(&value));
                    inspect(&value)
                }
            },
            RawEntryMut::Vacant(slot) => {
                let key = owned_key();
                let value = Arc::new(make());
                sweep.push_back(Arc::clone(&key));
                slot.insert_hashed_nocheck(hash, key, Arc::downgrade(&value));
                inspect(&value)
            }
        }
    }
    pub(crate) fn clean(&self, budget: usize) {
        // Maintenance is infrequent. A complete pass respects the caller's
        // total budget and does not contend with an unrelated lookup shard.
        let start = self.maintenance.fetch_add(1, Ordering::Relaxed) & (SLOT_SHARDS - 1);
        let mut remaining = budget;
        for offset in 0..SLOT_SHARDS {
            let shard = &self.shards[(start + offset) & (SLOT_SHARDS - 1)];
            if remaining == 0 {
                break;
            }
            if let Some(mut shard) = shard.try_lock() {
                remaining -= shard.clean(remaining);
            }
        }
    }
}
impl<T> std::fmt::Debug for WeakSlots<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WeakSlots")
            .field("shards", &SLOT_SHARDS)
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

    pub(crate) async fn lock_shared(&self, key: Arc<str>) -> KeyGuard {
        self.locks
            .get_arc_with(key, || Mutex::new(()), Arc::clone)
            .lock_owned()
            .await
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
    fn bounded_maintenance_visits_idle_shards_despite_a_live_first_shard() {
        let slots = WeakSlots::new();
        let keys: Vec<_> = (0..SLOT_SHARDS)
            .map(|shard| {
                (0..)
                    .map(|index| format!("{shard}-{index}"))
                    .find(|key| {
                        slots.hash.hash_one(key.as_str()) as usize & (SLOT_SHARDS - 1) == shard
                    })
                    .unwrap()
            })
            .collect();
        let values: Vec<_> = keys
            .iter()
            .map(|key| slots.get(key, || Mutex::new(())))
            .collect();
        let first = values[0].clone();
        drop(values);
        for _ in 0..SLOT_SHARDS {
            slots.clean(1);
        }
        let remaining: usize = slots
            .shards
            .iter()
            .map(|shard| shard.lock().entries.len())
            .sum();
        assert_eq!(remaining, 1);
        assert!(Arc::ptr_eq(&first, &slots.get(&keys[0], || Mutex::new(()))));
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
        let entries: usize = slots
            .shards
            .iter()
            .map(|shard| shard.lock().entries.len())
            .sum();
        let queued: usize = slots
            .shards
            .iter()
            .map(|shard| shard.lock().sweep.len())
            .sum();
        assert_eq!(entries, keys.len());
        assert_eq!(queued, keys.len());
        drop(values);
        slots.clean(keys.len());
        assert!(slots.shards.iter().all(|shard| {
            let shard = shard.lock();
            shard.entries.is_empty() && shard.sweep.is_empty()
        }));
    }
}
