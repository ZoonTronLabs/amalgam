use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Wake;
use std::time::Duration;

fn lane() -> Arc<KeyLane> {
    Arc::new(KeyLane::new(QueueMap::new().bind("key")))
}
fn poll(work: &mut LaneAcquisition) -> Poll<LaneGuard> {
    Pin::new(work).poll(&mut Context::from_waker(Waker::noop()))
}
fn ready(work: &mut LaneAcquisition) -> LaneGuard {
    match poll(work) {
        Poll::Ready(guard) => guard,
        Poll::Pending => panic!("the claim should be ready"),
    }
}

#[test]
fn uncontended_mutations_require_neither_a_runtime_nor_a_waiter_queue() {
    let queues = QueueMap::new();
    assert!(queues.0.get().is_none());
    let lane = Arc::new(KeyLane::new(queues.bind("key")));
    for _ in 0..512 {
        let mut claim = lane.lock();
        let guard = ready(&mut claim);
        assert!(lane.admission.queue.shard().lock().is_empty());
        assert!(lane.try_lock().is_none());
        drop(guard);
    }
    assert!(lane.try_lock().is_some());
}

#[test]
fn handoff_reserves_fifo_ownership_until_the_waiter_is_polled() {
    let lane = lane();
    let holder = lane.try_lock().unwrap();
    let mut first = lane.lock();
    let mut second = lane.lock();
    assert!(poll(&mut first).is_pending());
    assert!(poll(&mut second).is_pending());
    drop(holder);
    assert!(lane.try_lock().is_none());
    assert!(poll(&mut second).is_pending());
    let first_guard = ready(&mut first);
    assert!(lane.try_lock().is_none());
    drop(first_guard);
    assert!(lane.try_lock().is_none());
    drop(ready(&mut second));
    assert!(lane.try_lock().is_some());
    assert!(lane.admission.queue.shard().lock().is_empty());
}

#[test]
fn a_cancelled_waiter_never_reorders_the_remaining_key_queue() {
    let lane = lane();
    let holder = lane.try_lock().unwrap();
    let mut first = lane.lock();
    let mut cancelled = lane.lock();
    let mut last = lane.lock();
    assert!(poll(&mut first).is_pending());
    assert!(poll(&mut cancelled).is_pending());
    assert!(poll(&mut last).is_pending());
    drop(cancelled);
    drop(holder);
    assert!(poll(&mut last).is_pending());
    drop(ready(&mut first));
    drop(ready(&mut last));
    assert!(lane.try_lock().is_some());
}

#[test]
fn cancelling_an_unpolled_grant_releases_its_real_reservation() {
    let lane = lane();
    let holder = lane.try_lock().unwrap();
    let mut first = lane.lock();
    let mut second = lane.lock();
    assert!(poll(&mut first).is_pending());
    assert!(poll(&mut second).is_pending());
    drop(holder);
    assert!(lane.try_lock().is_none());
    drop(first);
    assert!(lane.try_lock().is_none());
    drop(ready(&mut second));
    assert!(lane.try_lock().is_some());
}

#[test]
fn dropping_the_last_waiter_restores_the_holders_fast_release() {
    let lane = lane();
    let holder = lane.try_lock().unwrap();
    let mut waiting = lane.lock();
    assert!(poll(&mut waiting).is_pending());
    drop(waiting);
    assert!(lane.admission.queue.shard().lock().is_empty());
    assert!(lane.try_lock().is_none());
    drop(holder);
    assert!(lane.try_lock().is_some());
}

#[test]
fn distinct_keys_in_one_shard_never_wait_for_each_others_io() {
    let first = lane();
    let second = Arc::new(KeyLane::new(QueueRef {
        shards: Arc::clone(&first.admission.queue.shards),
        index: first.admission.queue.index,
    }));
    let held = first.try_lock().unwrap();
    let mut queued = first.lock();
    assert!(poll(&mut queued).is_pending());
    let mut independent = second.lock();
    drop(ready(&mut independent));
    assert!(poll(&mut queued).is_pending());
    drop(held);
    drop(ready(&mut queued));
}

struct ReentrantWaker {
    queue: Arc<Shards>,
    index: usize,
    woke: Arc<AtomicUsize>,
    destroyed: Arc<AtomicUsize>,
}
impl ReentrantWaker {
    fn assert_unlocked(&self) {
        assert!(
            self.queue.queues[self.index].try_lock().is_some(),
            "user waker ran under mutation-queue coordination"
        );
    }
}
impl Wake for ReentrantWaker {
    fn wake(self: Arc<Self>) {
        self.assert_unlocked();
        self.woke.fetch_add(1, Ordering::SeqCst);
    }
}
impl Drop for ReentrantWaker {
    fn drop(&mut self) {
        self.assert_unlocked();
        self.destroyed.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn user_waker_wake_and_destruction_run_after_queue_coordination() {
    for complete in [false, true] {
        let lane = lane();
        let holder = lane.try_lock().unwrap();
        let woke = Arc::new(AtomicUsize::new(0));
        let destroyed = Arc::new(AtomicUsize::new(0));
        let waker = Waker::from(Arc::new(ReentrantWaker {
            queue: Arc::clone(&lane.admission.queue.shards),
            index: lane.admission.queue.index,
            woke: Arc::clone(&woke),
            destroyed: Arc::clone(&destroyed),
        }));
        let mut claim = lane.lock();
        assert!(
            Pin::new(&mut claim)
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        drop(waker);
        if complete {
            drop(holder);
            drop(ready(&mut claim));
            assert_eq!(woke.load(Ordering::SeqCst), 1);
        } else {
            drop(claim);
            drop(holder);
            assert_eq!(woke.load(Ordering::SeqCst), 0);
        }
        assert_eq!(destroyed.load(Ordering::SeqCst), 1);
        assert!(lane.try_lock().is_some());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_handoffs_and_cancellations_keep_one_owner() {
    let lane = lane();
    let active = Arc::new(AtomicUsize::new(0));
    let visits = Arc::new(AtomicUsize::new(0));
    let mut tasks = Vec::with_capacity(32);
    for task in 0..32 {
        let lane = Arc::clone(&lane);
        let active = Arc::clone(&active);
        let visits = Arc::clone(&visits);
        tasks.push(tokio::spawn(async move {
            for round in 0..64 {
                let mut claim = lane.lock();
                let guard = if (task + round) % 3 == 0 {
                    match poll(&mut claim) {
                        Poll::Ready(guard) => guard,
                        Poll::Pending => {
                            tokio::task::yield_now().await;
                            drop(claim);
                            continue;
                        }
                    }
                } else {
                    claim.await
                };
                assert_eq!(active.fetch_add(1, Ordering::SeqCst), 0);
                visits.fetch_add(1, Ordering::SeqCst);
                tokio::task::yield_now().await;
                assert_eq!(active.fetch_sub(1, Ordering::SeqCst), 1);
                drop(guard);
            }
        }));
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        for task in tasks {
            task.await.unwrap();
        }
    })
    .await
    .unwrap();
    assert!(visits.load(Ordering::SeqCst) > 512);
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert!(lane.try_lock().is_some());
    assert!(lane.admission.queue.shard().lock().is_empty());
}
