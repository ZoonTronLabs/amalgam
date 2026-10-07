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
const RETAINED_CONTROLS_PER_SHARD: usize = 16;

/// Distributed coordination reuses scalar owners; standalone L1 keeps its
/// transient single-flight path without bounded-pool maintenance per cold key.
#[derive(Clone, Copy)]
pub(crate) enum CoordinationPlan {
    Transient,
    Reuse,
}
impl CoordinationPlan {
    pub(crate) fn slots<T: ScalarControl>(self) -> WeakSlots<T> {
        match self {
            Self::Transient => WeakSlots::new(),
            Self::Reuse => WeakSlots::reusable(),
        }
    }
}

// Only these scalar controls may be destroyed under lookup coordination.
// In particular, Flight<V> and user-value owners cannot select idle reuse.
mod scalar {
    pub trait Sealed {}
    impl Sealed for tokio::sync::Mutex<()> {}
    impl Sealed for crate::commit::KeyLane {}
}
pub(crate) trait ScalarControl: scalar::Sealed {}
impl ScalarControl for Mutex<()> {}
impl ScalarControl for crate::commit::KeyLane {}

enum ControlRetention<T> {
    Weak,
    Reuse(VecDeque<Arc<T>>),
}
impl<T> ControlRetention<T> {
    fn retain(&mut self, control: &Arc<T>) {
        if let Self::Reuse(recent) = self {
            if recent.len() == RETAINED_CONTROLS_PER_SHARD {
                recent.pop_front();
            }
            recent.push_back(Arc::clone(control));
        }
    }
    fn clean_idle(&mut self, budget: usize) {
        if let Self::Reuse(recent) = self {
            for _ in 0..budget.min(recent.len()) {
                let control = recent.pop_front().expect("count bounds the live queue");
                if Arc::strong_count(&control) > 1 {
                    recent.push_back(control);
                }
            }
        }
    }
}
struct SlotShard<T> {
    entries: HashMap<Arc<str>, Weak<T>, RandomState>,
    sweep: VecDeque<Arc<str>>,
    controls: ControlRetention<T>,
}
impl<T: ScalarControl> SlotShard<T> {
    fn create(
        &mut self,
        hash: u64,
        key: &str,
        owned_key: impl FnOnce() -> Arc<str>,
        make: impl FnOnce() -> T,
    ) -> Arc<T> {
        let control = Arc::new(make());
        match self
            .entries
            .raw_entry_mut()
            .from_hash(hash, |stored| stored.as_ref() == key)
        {
            RawEntryMut::Occupied(mut slot) => {
                // The same shard guard observed this weak owner as dead.
                // A zero strong count cannot regain a live holder or waiter.
                slot.insert(Arc::downgrade(&control));
            }
            RawEntryMut::Vacant(slot) => {
                let key = owned_key();
                self.sweep.push_back(Arc::clone(&key));
                slot.insert_hashed_nocheck(hash, key, Arc::downgrade(&control));
            }
        }
        self.controls.retain(&control);
        control
    }
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
pub(crate) struct WeakSlots<T: ScalarControl> {
    // Only explicit maintenance advances this cursor, never a lookup.
    maintenance: AtomicUsize,
    hash: RandomState,
    shards: Box<[ShardMutex<SlotShard<T>>]>,
}
impl<T: ScalarControl> WeakSlots<T> {
    pub(crate) fn new() -> Self {
        let hash = RandomState::new();
        Self {
            maintenance: AtomicUsize::new(0),
            shards: (0..SLOT_SHARDS)
                .map(|_| {
                    ShardMutex::new(SlotShard {
                        entries: HashMap::with_hasher(hash.clone()),
                        sweep: VecDeque::new(),
                        controls: ControlRetention::Weak,
                    })
                })
                .collect(),
            hash,
        }
    }
    /// A bounded shard-local pool amortizes allocation of scalar coordination.
    /// Active holders/waiters still own their identity even after pool eviction.
    pub(crate) fn reusable() -> Self
    where
        T: ScalarControl,
    {
        let mut slots = Self::new();
        for shard in slots.shards.iter_mut() {
            shard.get_mut().controls = ControlRetention::Reuse(VecDeque::new());
        }
        slots
    }
    pub(crate) fn get(&self, key: &str, make: impl FnOnce() -> T) -> Arc<T> {
        self.get_with(key, make, std::convert::identity)
    }
    pub(crate) fn get_with<R>(
        &self,
        key: &str,
        make: impl FnOnce() -> T,
        inspect: impl FnOnce(Arc<T>) -> R,
    ) -> R {
        self.lookup(key, || Arc::from(key), make, inspect)
    }
    pub(crate) fn get_arc_with<R>(
        &self,
        key: Arc<str>,
        make: impl FnOnce() -> T,
        inspect: impl FnOnce(Arc<T>) -> R,
    ) -> R {
        // A supplied key's allocation is reused only for a new lookup slot.
        self.lookup(key.as_ref(), || Arc::clone(&key), make, inspect)
    }
    fn lookup<R>(
        &self,
        key: &str,
        owned_key: impl FnOnce() -> Arc<str>,
        make: impl FnOnce() -> T,
        inspect: impl FnOnce(Arc<T>) -> R,
    ) -> R {
        let hash = self.hash.hash_one(key);
        let mut shard = self.shards[hash as usize & (SLOT_SHARDS - 1)].lock();
        if let Some(control) = shard
            .entries
            .raw_entry()
            .from_hash(hash, |stored| stored.as_ref() == key)
            .and_then(|(_, control)| control.upgrade())
        {
            // Move the upgraded owner to its consumer. Reusing an identity
            // creates no idle slot and needs neither a sweep nor another clone.
            return inspect(control);
        }
        shard.clean(4);
        inspect(shard.create(hash, key, owned_key, make))
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
                shard.controls.clean_idle(remaining);
                remaining -= shard.clean(remaining);
            }
        }
    }
}
impl<T: ScalarControl> std::fmt::Debug for WeakSlots<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WeakSlots")
            .field("shards", &SLOT_SHARDS)
            .finish()
    }
}

/// Per-key async mutexes with safely reclaimable identity slots.
#[derive(Debug)]
pub struct KeyedLock {
    locks: WeakSlots<Mutex<()>>,
}

/// The guard returned by acquiring a key's lock. Releasing it (on drop) lets the
/// next waiter for that key proceed.
pub type KeyGuard = OwnedMutexGuard<()>;

impl KeyedLock {
    /// Creates independent per-key locks with implementation-selected lookup sharding.
    #[must_use]
    pub fn new() -> Self {
        Self::with_plan(CoordinationPlan::Transient)
    }
    pub(crate) fn with_plan(plan: CoordinationPlan) -> Self {
        Self {
            locks: plan.slots(),
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
        let mutex = self
            .locks
            .get_arc_with(key, || Mutex::new(()), std::convert::identity);
        Self::acquire_owned(mutex).await
    }
    async fn acquire_owned(mutex: Arc<Mutex<()>>) -> KeyGuard {
        // Ready coordination does not need a suspended waiter or consume a
        // scheduler budget. Tokio reserves permits for queued waiters, so a
        // failed synchronous claim waits normally instead of bypassing them.
        match Arc::clone(&mutex).try_lock_owned() {
            Ok(guard) => guard,
            Err(_) => mutex.lock_owned().await,
        }
    }
    /// Acquires the lock for `key`, waiting if necessary.
    pub async fn lock(&self, key: &str) -> KeyGuard {
        Self::acquire_owned(self.mutex_for(key)).await
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
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    #[tokio::test]
    async fn free_key_acquisitions_never_suspend_for_an_exhausted_cooperative_budget() {
        let locks = KeyedLock::new();
        for _ in 0..512 {
            let mut work = std::pin::pin!(locks.lock("ready-key"));
            let initial =
                std::future::poll_fn(|cx| std::task::Poll::Ready(work.as_mut().poll(cx))).await;
            assert!(
                initial.is_ready(),
                "a free key required scheduler participation"
            );
            drop(initial);
        }
    }

    #[tokio::test]
    async fn ready_acquisition_does_not_bypass_an_already_queued_waiter() {
        let locks = KeyedLock::new();
        let first = locks.lock("ordered-key").await;
        let mut queued = std::pin::pin!(locks.lock("ordered-key"));
        let initial =
            std::future::poll_fn(|cx| std::task::Poll::Ready(queued.as_mut().poll(cx))).await;
        assert!(initial.is_pending());
        drop(first);
        assert!(locks.try_lock("ordered-key").is_none());
        let second = queued.await;
        assert!(locks.try_lock("ordered-key").is_none());
        drop(second);
        assert!(locks.try_lock("ordered-key").is_some());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn same_key_serializes() {
        let lock = Arc::new(KeyedLock::new());
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
    fn scalar_controls_reuse_idle_identity_and_explicit_maintenance_reclaims_it() {
        let slots = WeakSlots::<Mutex<()>>::reusable();
        let first = slots.get("reused", || Mutex::new(()));
        let identity = Arc::downgrade(&first);
        drop(first);
        let next = slots.get("reused", || panic!("an idle scalar control was rebuilt"));
        assert!(Arc::ptr_eq(&identity.upgrade().unwrap(), &next));
        drop(next);
        slots.clean(1024);
        assert!(identity.upgrade().is_none());
        assert!(
            slots
                .shards
                .iter()
                .all(|shard| shard.lock().entries.is_empty())
        );
    }

    #[test]
    fn scalar_control_retention_is_bounded_per_shard() {
        let slots = WeakSlots::<Mutex<()>>::reusable();
        let identities: Vec<_> = (0..)
            .map(|index| format!("bounded-{index}"))
            .filter(|key| slots.hash.hash_one(key.as_str()) as usize & (SLOT_SHARDS - 1) == 0)
            .take(RETAINED_CONTROLS_PER_SHARD + 1)
            .map(|key| Arc::downgrade(&slots.get(&key, || Mutex::new(()))))
            .collect();
        assert!(identities[0].upgrade().is_none());
        assert_eq!(
            identities
                .iter()
                .filter(|identity| identity.upgrade().is_some())
                .count(),
            RETAINED_CONTROLS_PER_SHARD
        );
        let shard = slots.shards[0].lock();
        let ControlRetention::Reuse(controls) = &shard.controls else {
            panic!("scalar controls must select bounded reuse")
        };
        assert_eq!(controls.len(), RETAINED_CONTROLS_PER_SHARD);
    }

    #[tokio::test]
    async fn pool_eviction_and_maintenance_preserve_holder_and_queued_waiter_identity() {
        let locks = KeyedLock::with_plan(CoordinationPlan::Reuse);
        let first = locks.lock("held").await;
        let identity = Arc::downgrade(&locks.mutex_for("held"));
        let mut queued = std::pin::pin!(locks.lock("held"));
        let initial =
            std::future::poll_fn(|cx| std::task::Poll::Ready(queued.as_mut().poll(cx))).await;
        assert!(initial.is_pending());
        let shard = locks.locks.hash.hash_one("held") as usize & (SLOT_SHARDS - 1);
        for key in (0..)
            .map(|index| format!("eviction-{index}"))
            .filter(|key| {
                locks.locks.hash.hash_one(key.as_str()) as usize & (SLOT_SHARDS - 1) == shard
            })
            .take(RETAINED_CONTROLS_PER_SHARD + 1)
        {
            drop(locks.mutex_for(&key));
        }
        locks.clean_idle(1024);
        assert!(Arc::ptr_eq(
            &identity.upgrade().unwrap(),
            &locks.mutex_for("held")
        ));
        drop(first);
        assert!(
            locks.try_lock("held").is_none(),
            "a queued waiter was bypassed"
        );
        let second = queued.await;
        assert!(locks.try_lock("held").is_none());
        drop(second);
        assert!(locks.try_lock("held").is_some());
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
