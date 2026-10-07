//! Suspended mutations wait in shard queues. An uncontended claim uses only
//! the key's scalar admission word; it creates neither a task nor a waiter.
use super::KeyLane;
use ahash::RandomState;
use hashbrown::HashMap;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll, Waker};

const QUEUE_SHARDS: usize = 64;

#[derive(Clone, Copy)]
#[repr(u8)]
enum GateState {
    Free = 0,
    Held = 1,
    Queued = 2,
}

pub(super) struct Admission {
    state: AtomicU8,
    queue: QueueRef,
}
impl Admission {
    pub(super) fn new(queue: QueueRef) -> Self {
        Self {
            state: AtomicU8::new(GateState::Free as u8),
            queue,
        }
    }
    fn claim(&self) -> bool {
        self.state
            .compare_exchange(
                GateState::Free as u8,
                GateState::Held as u8,
                Ordering::Acquire,
                Ordering::Relaxed,
            )
            .is_ok()
    }
    fn state(&self) -> GateState {
        match self.state.load(Ordering::Relaxed) {
            0 => GateState::Free,
            1 => GateState::Held,
            2 => GateState::Queued,
            _ => unreachable!("only closed gate states are published"),
        }
    }
    fn mark_queued(&self) -> bool {
        self.state
            .compare_exchange(
                GateState::Held as u8,
                GateState::Queued as u8,
                Ordering::Relaxed,
                Ordering::Relaxed,
            )
            .is_ok()
    }
    fn release_ready(&self) -> bool {
        self.state
            .compare_exchange(
                GateState::Held as u8,
                GateState::Free as u8,
                Ordering::Release,
                Ordering::Relaxed,
            )
            .is_ok()
    }
}

// Every holder and waiter owns the same Arc<KeyLane>, so its address cannot be
// reused until its queue entry was removed. The queue retains no value payload.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct LaneKey(usize);
impl LaneKey {
    fn of(lane: &Arc<KeyLane>) -> Self {
        Self(Arc::as_ptr(lane) as usize)
    }
}
type Waiters = HashMap<LaneKey, VecDeque<Arc<Waiter>>, RandomState>;
struct Shards {
    hash: RandomState,
    queues: Box<[Mutex<Waiters>]>,
}
impl Shards {
    fn new() -> Self {
        let hash = RandomState::new();
        Self {
            queues: (0..QUEUE_SHARDS)
                .map(|_| Mutex::new(HashMap::with_hasher(hash.clone())))
                .collect(),
            hash,
        }
    }
}

/// Inline-only caches never initialize the suspended-mutation queue storage.
pub(super) struct QueueMap(OnceLock<Arc<Shards>>);
impl QueueMap {
    pub(super) fn new() -> Self {
        Self(OnceLock::new())
    }
    pub(super) fn bind(&self, key: &str) -> QueueRef {
        let shards = self.0.get_or_init(|| Arc::new(Shards::new()));
        QueueRef {
            index: shards.hash.hash_one(key) as usize & (QUEUE_SHARDS - 1),
            shards: Arc::clone(shards),
        }
    }
}
pub(super) struct QueueRef {
    shards: Arc<Shards>,
    index: usize,
}
enum Enqueued {
    Claimed,
    Waiting,
}
impl QueueRef {
    fn shard(&self) -> &Mutex<Waiters> {
        &self.shards.queues[self.index]
    }
    fn enqueue(&self, lane: &Arc<KeyLane>, waiter: &Arc<Waiter>) -> Enqueued {
        let mut queues = self.shard().lock();
        loop {
            match lane.admission.state() {
                GateState::Free if lane.admission.claim() => return Enqueued::Claimed,
                GateState::Held if lane.admission.mark_queued() => break,
                GateState::Queued => break,
                GateState::Free | GateState::Held => continue,
            }
        }
        queues
            .entry(LaneKey::of(lane))
            .or_default()
            .push_back(Arc::clone(waiter));
        Enqueued::Waiting
    }
    fn release(&self, lane: &Arc<KeyLane>) {
        if lane.admission.release_ready() {
            return;
        }
        let next = self.handoff(lane);
        if let Some(waiter) = next {
            waiter.wake();
        }
    }
    fn handoff(&self, lane: &Arc<KeyLane>) -> Option<Arc<Waiter>> {
        let mut queues = self.shard().lock();
        let key = LaneKey::of(lane);
        let next = queues.get_mut(&key).and_then(VecDeque::pop_front);
        if queues.get(&key).is_some_and(VecDeque::is_empty) {
            queues.remove(&key);
        }
        let state = match &next {
            Some(_) if queues.contains_key(&key) => GateState::Queued,
            Some(_) => GateState::Held,
            None => GateState::Free,
        };
        lane.admission.state.store(state as u8, Ordering::Release);
        if let Some(waiter) = &next {
            // Reserve ownership before waking; a new ready claim cannot steal it.
            waiter.granted.store(true, Ordering::Release);
        }
        next
    }
    fn withdraw(&self, lane: &Arc<KeyLane>, waiter: &Arc<Waiter>) -> bool {
        let mut queues = self.shard().lock();
        if waiter.granted.load(Ordering::Acquire) {
            return true;
        }
        let key = LaneKey::of(lane);
        let queue = queues.get_mut(&key).expect("a waiting claim is queued");
        let index = queue
            .iter()
            .position(|stored| Arc::ptr_eq(stored, waiter))
            .expect("a waiting claim keeps its queue identity");
        // The future still owns this node. Removing the queue reference cannot
        // destroy its user-supplied waker while shard coordination is held.
        queue.remove(index);
        if queue.is_empty() {
            queues.remove(&key);
            lane.admission
                .state
                .store(GateState::Held as u8, Ordering::Relaxed);
        }
        false
    }
}

struct Waiter {
    granted: AtomicBool,
    waker: Mutex<Option<Waker>>,
}
impl Waiter {
    fn new() -> Self {
        Self {
            granted: AtomicBool::new(false),
            waker: Mutex::new(None),
        }
    }
    fn register(&self, waker: &Waker) {
        let replacement = waker.clone();
        let previous = self.waker.lock().replace(replacement);
        drop(previous);
    }
    fn wake(&self) {
        let waker = self.waker.lock().take();
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

enum AcquisitionState {
    New(Arc<KeyLane>),
    Queued {
        lane: Arc<KeyLane>,
        waiter: Arc<Waiter>,
    },
    Finished,
}
pub(crate) struct LaneAcquisition(AcquisitionState);
enum ClaimStep {
    Ready(LaneGuard),
    Waiting,
}
impl LaneAcquisition {
    fn start(&mut self) -> ClaimStep {
        let AcquisitionState::New(lane) =
            std::mem::replace(&mut self.0, AcquisitionState::Finished)
        else {
            unreachable!("a new claim was matched");
        };
        if lane.admission.claim() {
            ClaimStep::Ready(LaneGuard(lane))
        } else {
            self.enqueue(lane)
        }
    }
    fn enqueue(&mut self, lane: Arc<KeyLane>) -> ClaimStep {
        let waiter = Arc::new(Waiter::new());
        match lane.admission.queue.enqueue(&lane, &waiter) {
            Enqueued::Claimed => ClaimStep::Ready(LaneGuard(lane)),
            Enqueued::Waiting => {
                self.0 = AcquisitionState::Queued { lane, waiter };
                ClaimStep::Waiting
            }
        }
    }
    fn complete(&mut self) -> LaneGuard {
        let AcquisitionState::Queued { lane, waiter } =
            std::mem::replace(&mut self.0, AcquisitionState::Finished)
        else {
            unreachable!("a granted claim was matched");
        };
        let guard = LaneGuard(lane);
        drop(waiter);
        guard
    }
    fn poll_waiter(&mut self, cx: &mut Context<'_>) -> Poll<LaneGuard> {
        let AcquisitionState::Queued { waiter, .. } = &self.0 else {
            panic!("a completed mutation claim was polled again");
        };
        waiter.register(cx.waker());
        if waiter.granted.load(Ordering::Acquire) {
            Poll::Ready(self.complete())
        } else {
            Poll::Pending
        }
    }
}
impl Future for LaneAcquisition {
    type Output = LaneGuard;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if matches!(&this.0, AcquisitionState::New(_)) {
            match this.start() {
                ClaimStep::Ready(guard) => return Poll::Ready(guard),
                ClaimStep::Waiting => return this.poll_waiter(cx),
            }
        }
        this.poll_waiter(cx)
    }
}
impl Drop for LaneAcquisition {
    fn drop(&mut self) {
        if let AcquisitionState::Queued { lane, waiter } = &self.0
            && lane.admission.queue.withdraw(lane, waiter)
        {
            // Granted but not yet polled is a real owner; cancelling it must
            // release the reservation and wake the next key-local claimant.
            lane.admission.queue.release(lane);
        }
    }
}

pub(crate) struct LaneGuard(Arc<KeyLane>);
impl Drop for LaneGuard {
    fn drop(&mut self) {
        self.0.admission.queue.release(&self.0);
    }
}
impl KeyLane {
    pub(crate) fn lock(self: &Arc<Self>) -> LaneAcquisition {
        LaneAcquisition(AcquisitionState::New(Arc::clone(self)))
    }
    pub(crate) fn try_lock(self: &Arc<Self>) -> Option<LaneGuard> {
        self.admission.claim().then(|| LaneGuard(Arc::clone(self)))
    }
}

#[cfg(test)]
mod tests;
