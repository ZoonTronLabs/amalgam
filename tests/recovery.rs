//! Focused tests for [`AutoRecoveryService`] driven through its public API.
//!
//! These deliberately avoid the spawned background loop: `drain_once` is public,
//! so every test replays on demand against a hand-written mock executor. That
//! keeps the queue's behaviour — latest-wins dedup, the `max_items` bound, the
//! `max_retries` budget, and "remove replayed / keep failed" — fully
//! deterministic (no timers, no real clock).

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;

use amalgam::{
    AutoRecoveryService, Clock, ManualClock, RecoveryAction, RecoveryConfig, RecoveryExecutor,
    RecoveryItem, Result, Timestamp,
};

/// A configurable [`RecoveryExecutor`] mock.
///
/// It tallies every `replay` call, records the key/timestamp it last saw, and
/// can be made to fail either globally (`fail_all`) or for a fixed set of keys
/// (`failing_keys`). All other replays succeed.
struct MockExecutor {
    replays: AtomicUsize,
    fail_all: AtomicBool,
    failing_keys: HashSet<String>,
    last_timestamp: Mutex<Option<Timestamp>>,
    seen_keys: Mutex<Vec<Arc<str>>>,
}

impl MockExecutor {
    fn new() -> Self {
        Self {
            replays: AtomicUsize::new(0),
            fail_all: AtomicBool::new(false),
            failing_keys: HashSet::new(),
            last_timestamp: Mutex::new(None),
            seen_keys: Mutex::new(Vec::new()),
        }
    }

    fn failing_keys<I: IntoIterator<Item = &'static str>>(keys: I) -> Self {
        let mut me = Self::new();
        me.failing_keys = keys.into_iter().map(str::to_owned).collect();
        me
    }

    fn replay_count(&self) -> usize {
        self.replays.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl RecoveryExecutor for MockExecutor {
    async fn replay(&self, item: &RecoveryItem) -> Result<()> {
        self.replays.fetch_add(1, Ordering::SeqCst);
        *self.last_timestamp.lock().unwrap() = Some(item.timestamp);
        self.seen_keys.lock().unwrap().push(Arc::clone(&item.key));

        if self.fail_all.load(Ordering::SeqCst) || self.failing_keys.contains(&*item.key) {
            Err(amalgam::Error::Distributed("mock replay failure".into()))
        } else {
            Ok(())
        }
    }
}

/// A clock fixed at tick 0, so any positive `expires_at` is in the future and no
/// queued item is ever dropped as expired by `drain_once`.
fn fixed_clock() -> Arc<dyn Clock> {
    Arc::new(ManualClock::new(Timestamp::from_ticks(0)))
}

/// Far enough in the future (relative to [`fixed_clock`]) that items never expire.
const FAR_FUTURE: Timestamp = Timestamp::from_ticks(1_000_000_000);

fn item(
    key: &str,
    action: RecoveryAction,
    timestamp_ticks: i64,
    expires_at: Timestamp,
) -> RecoveryItem {
    RecoveryItem {
        key: Arc::from(key),
        action,
        timestamp: Timestamp::from_ticks(timestamp_ticks),
        expires_at,
        // `None` lets the service seed the budget from `RecoveryConfig.max_retries`,
        // which is the path under test.
        remaining_retries: None,
    }
}

fn config(max_items: Option<usize>, max_retries: Option<u32>) -> RecoveryConfig {
    RecoveryConfig {
        enabled: true,
        delay: Duration::from_millis(100),
        max_items,
        max_retries,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn enqueue_dedups_latest_timestamp_wins() {
    let service = AutoRecoveryService::new(config(None, None), fixed_clock());
    let mock = Arc::new(MockExecutor::new());

    // Exercise `set_executor` (part of the public surface) — it only records a
    // Weak; we still drive replay explicitly below for determinism.
    let as_exec: Arc<dyn RecoveryExecutor> = mock.clone();
    service.set_executor(Arc::downgrade(&as_exec));

    // Three enqueues for one key. Newest timestamp wins; the older one that
    // arrives afterwards is ignored. The queue holds a single deduplicated item.
    service.enqueue(item("k", RecoveryAction::Set, 10, FAR_FUTURE));
    service.enqueue(item("k", RecoveryAction::Set, 20, FAR_FUTURE)); // newer ⇒ replaces
    service.enqueue(item("k", RecoveryAction::Set, 5, FAR_FUTURE)); // older ⇒ ignored
    assert_eq!(service.len(), 1, "dedup collapses one key to one item");

    service.drain_once(mock.as_ref()).await;

    assert_eq!(mock.replay_count(), 1, "exactly one item was replayed");
    assert_eq!(
        *mock.last_timestamp.lock().unwrap(),
        Some(Timestamp::from_ticks(20)),
        "the newest-timestamp item survived dedup"
    );
    assert_eq!(service.len(), 0, "the successful replay was removed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn max_items_rejects_new_item_when_full_and_nothing_evictable() {
    // Capacity 2. All three items share the same (far-future) expiry, so no
    // existing item expires *before* the newcomer's bound ⇒ none is evictable ⇒
    // the third enqueue is rejected.
    let service = AutoRecoveryService::new(config(Some(2), None), fixed_clock());

    service.enqueue(item("a", RecoveryAction::Set, 1, FAR_FUTURE));
    service.enqueue(item("b", RecoveryAction::Set, 1, FAR_FUTURE));
    assert_eq!(service.len(), 2);

    service.enqueue(item("c", RecoveryAction::Set, 1, FAR_FUTURE));
    assert_eq!(service.len(), 2, "the queue stayed at its bound");

    // Confirm "c" was the one rejected (a & b are intact): drain and inspect the
    // keys the executor saw.
    let mock = Arc::new(MockExecutor::new());
    service.drain_once(mock.as_ref()).await;
    let seen: HashSet<String> = mock
        .seen_keys
        .lock()
        .unwrap()
        .iter()
        .map(|k| k.to_string())
        .collect();
    assert_eq!(seen, HashSet::from(["a".to_owned(), "b".to_owned()]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn max_items_evicts_sooner_expiring_item_to_admit_newer() {
    // Capacity 2. "a" expires soon; "b" far away. Enqueuing "c" (far expiry) is
    // full, so the service evicts the soonest-expiring item that expires before
    // "c"'s bound — that is "a" — and admits "c".
    let service = AutoRecoveryService::new(config(Some(2), None), fixed_clock());

    let soon = Timestamp::from_ticks(10); // > now(0) but well before FAR_FUTURE
    service.enqueue(item("a", RecoveryAction::Set, 1, soon));
    service.enqueue(item("b", RecoveryAction::Set, 1, FAR_FUTURE));
    assert_eq!(service.len(), 2);

    service.enqueue(item("c", RecoveryAction::Set, 1, FAR_FUTURE));
    assert_eq!(service.len(), 2, "still at capacity after the swap");

    // "a" was evicted; "b" and "c" remain.
    let mock = Arc::new(MockExecutor::new());
    service.drain_once(mock.as_ref()).await;
    let seen: HashSet<String> = mock
        .seen_keys
        .lock()
        .unwrap()
        .iter()
        .map(|k| k.to_string())
        .collect();
    assert_eq!(seen, HashSet::from(["b".to_owned(), "c".to_owned()]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn max_retries_exhaustion_drops_item() {
    // Budget = 1 retry. The item is seeded with remaining_retries = Some(1).
    // `record_failure` keeps it on the first failure (1 ⇒ 0) and drops it on the
    // second (0 ⇒ gone). So it survives one failed drain and is dropped on the next.
    let service = AutoRecoveryService::new(config(None, Some(1)), fixed_clock());
    let mock = Arc::new(MockExecutor::new());
    mock.fail_all.store(true, Ordering::SeqCst);

    service.enqueue(item("k", RecoveryAction::Set, 1, FAR_FUTURE));
    assert_eq!(service.len(), 1);

    service.drain_once(mock.as_ref()).await; // failure #1: budget 1 ⇒ 0, kept
    assert_eq!(service.len(), 1, "item retained while retries remain");

    service.drain_once(mock.as_ref()).await; // failure #2: budget 0 ⇒ dropped
    assert_eq!(
        service.len(),
        0,
        "item dropped once its retry budget is exhausted"
    );

    assert_eq!(mock.replay_count(), 2, "two replay attempts were made");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drain_once_removes_succeeded_and_keeps_failed() {
    // Unbounded retries: a failed item is kept (not dropped by budget), letting us
    // assert "remove succeeded / keep failed" purely from drain semantics.
    let service = AutoRecoveryService::new(config(None, None), fixed_clock());
    let mock = Arc::new(MockExecutor::failing_keys(["bad"]));

    service.enqueue(item("ok", RecoveryAction::Set, 1, FAR_FUTURE));
    service.enqueue(item("bad", RecoveryAction::Remove, 1, FAR_FUTURE));
    assert_eq!(service.len(), 2);

    service.drain_once(mock.as_ref()).await;
    assert_eq!(
        service.len(),
        1,
        "the succeeded item was removed; the failed one was retained"
    );

    // Prove the survivor is "bad": let the mock succeed for everyone, drain again,
    // and the queue empties.
    let healthy = Arc::new(MockExecutor::new());
    service.drain_once(healthy.as_ref()).await;
    assert_eq!(
        service.len(),
        0,
        "the previously-failed item replayed on recovery"
    );
    assert_eq!(
        healthy.replay_count(),
        1,
        "only the retained item was replayed"
    );
}
