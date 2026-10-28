use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use super::{KeyLane, LaneAcquisition, LaneGuard, QueueMap, QueueRef, Shards};

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
    assert!(
        queues.0.get().is_none(),
        "contract: uncontended mutations require neither a runtime nor a waiter queue"
    );
    let lane = Arc::new(KeyLane::new(queues.bind("key")));
    for _ in 0..512 {
        let mut claim = lane.lock();
        let guard = ready(&mut claim);
        assert!(
            lane.admission.queue.shard().lock().is_empty(),
            "contract: uncontended mutations require neither a runtime nor a waiter queue"
        );
        assert!(
            lane.try_lock().is_none(),
            "contract: uncontended mutations require neither a runtime nor a waiter queue"
        );
        std::mem::drop(guard);
    }
    assert!(
        lane.try_lock().is_some(),
        "contract: uncontended mutations require neither a runtime nor a waiter queue"
    );
}

#[test]
fn handoff_reserves_fifo_ownership_until_the_waiter_is_polled() {
    let lane = lane();
    let holder = lane.try_lock().unwrap();
    let mut first = lane.lock();
    let mut second = lane.lock();
    assert!(
        poll(&mut first).is_pending(),
        "contract: handoff reserves fifo ownership until the waiter is polled"
    );
    assert!(
        poll(&mut second).is_pending(),
        "contract: handoff reserves fifo ownership until the waiter is polled"
    );
    std::mem::drop(holder);
    assert!(
        lane.try_lock().is_none(),
        "contract: handoff reserves fifo ownership until the waiter is polled"
    );
    assert!(
        poll(&mut second).is_pending(),
        "contract: handoff reserves fifo ownership until the waiter is polled"
    );
    let first_guard = ready(&mut first);
    assert!(
        lane.try_lock().is_none(),
        "contract: handoff reserves fifo ownership until the waiter is polled"
    );
    std::mem::drop(first_guard);
    assert!(
        lane.try_lock().is_none(),
        "contract: handoff reserves fifo ownership until the waiter is polled"
    );
    std::mem::drop(ready(&mut second));
    assert!(
        lane.try_lock().is_some(),
        "contract: handoff reserves fifo ownership until the waiter is polled"
    );
    assert!(
        lane.admission.queue.shard().lock().is_empty(),
        "contract: handoff reserves fifo ownership until the waiter is polled"
    );
}

#[test]
fn a_cancelled_waiter_never_reorders_the_remaining_key_queue() {
    let lane = lane();
    let holder = lane.try_lock().unwrap();
    let mut first = lane.lock();
    let mut cancelled = lane.lock();
    let mut last = lane.lock();
    assert!(
        poll(&mut first).is_pending(),
        "contract: a cancelled waiter never reorders the remaining key queue"
    );
    assert!(
        poll(&mut cancelled).is_pending(),
        "contract: a cancelled waiter never reorders the remaining key queue"
    );
    assert!(
        poll(&mut last).is_pending(),
        "contract: a cancelled waiter never reorders the remaining key queue"
    );
    std::mem::drop(cancelled);
    std::mem::drop(holder);
    assert!(
        poll(&mut last).is_pending(),
        "contract: a cancelled waiter never reorders the remaining key queue"
    );
    std::mem::drop(ready(&mut first));
    std::mem::drop(ready(&mut last));
    assert!(
        lane.try_lock().is_some(),
        "contract: a cancelled waiter never reorders the remaining key queue"
    );
}

#[test]
fn cancelling_an_unpolled_grant_releases_its_real_reservation() {
    let lane = lane();
    let holder = lane.try_lock().unwrap();
    let mut first = lane.lock();
    let mut second = lane.lock();
    assert!(
        poll(&mut first).is_pending(),
        "contract: cancelling an unpolled grant releases its real reservation"
    );
    assert!(
        poll(&mut second).is_pending(),
        "contract: cancelling an unpolled grant releases its real reservation"
    );
    std::mem::drop(holder);
    assert!(
        lane.try_lock().is_none(),
        "contract: cancelling an unpolled grant releases its real reservation"
    );
    std::mem::drop(first);
    assert!(
        lane.try_lock().is_none(),
        "contract: cancelling an unpolled grant releases its real reservation"
    );
    std::mem::drop(ready(&mut second));
    assert!(
        lane.try_lock().is_some(),
        "contract: cancelling an unpolled grant releases its real reservation"
    );
}

#[test]
fn dropping_the_last_waiter_restores_the_holders_fast_release() {
    let lane = lane();
    let holder = lane.try_lock().unwrap();
    let mut waiting = lane.lock();
    assert!(
        poll(&mut waiting).is_pending(),
        "contract: dropping the last waiter restores the holders fast release"
    );
    std::mem::drop(waiting);
    assert!(
        lane.admission.queue.shard().lock().is_empty(),
        "contract: dropping the last waiter restores the holders fast release"
    );
    assert!(
        lane.try_lock().is_none(),
        "contract: dropping the last waiter restores the holders fast release"
    );
    std::mem::drop(holder);
    assert!(
        lane.try_lock().is_some(),
        "contract: dropping the last waiter restores the holders fast release"
    );
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
    assert!(
        poll(&mut queued).is_pending(),
        "contract: distinct keys in one shard never wait for each others io"
    );
    let mut independent = second.lock();
    std::mem::drop(ready(&mut independent));
    assert!(
        poll(&mut queued).is_pending(),
        "contract: distinct keys in one shard never wait for each others io"
    );
    std::mem::drop(held);
    std::mem::drop(ready(&mut queued));
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
                .is_pending(),
            "contract: user waker wake and destruction run after queue coordination"
        );
        std::mem::drop(waker);
        if complete {
            std::mem::drop(holder);
            std::mem::drop(ready(&mut claim));
            assert_eq!(
                woke.load(Ordering::SeqCst),
                1,
                "contract: user waker wake and destruction run after queue coordination"
            );
        } else {
            std::mem::drop(claim);
            std::mem::drop(holder);
            assert_eq!(
                woke.load(Ordering::SeqCst),
                0,
                "contract: user waker wake and destruction run after queue coordination"
            );
        }
        assert_eq!(
            destroyed.load(Ordering::SeqCst),
            1,
            "contract: user waker wake and destruction run after queue coordination"
        );
        assert!(
            lane.try_lock().is_some(),
            "contract: user waker wake and destruction run after queue coordination"
        );
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
                            std::mem::drop(claim);
                            continue;
                        }
                    }
                } else {
                    claim.await
                };
                assert_eq!(
                    active.fetch_add(1, Ordering::SeqCst),
                    0,
                    "contract: concurrent handoffs and cancellations keep one owner"
                );
                visits.fetch_add(1, Ordering::SeqCst);
                tokio::task::yield_now().await;
                assert_eq!(
                    active.fetch_sub(1, Ordering::SeqCst),
                    1,
                    "contract: concurrent handoffs and cancellations keep one owner"
                );
                std::mem::drop(guard);
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
    assert!(
        visits.load(Ordering::SeqCst) > 512,
        "contract: concurrent handoffs and cancellations keep one owner"
    );
    assert_eq!(
        active.load(Ordering::SeqCst),
        0,
        "contract: concurrent handoffs and cancellations keep one owner"
    );
    assert!(
        lane.try_lock().is_some(),
        "contract: concurrent handoffs and cancellations keep one owner"
    );
    assert!(
        lane.admission.queue.shard().lock().is_empty(),
        "contract: concurrent handoffs and cancellations keep one owner"
    );
}
