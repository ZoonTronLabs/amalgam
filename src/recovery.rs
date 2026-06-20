//! Auto-recovery: retry L2 / backplane operations that failed transiently.
//!
//! When a write to L2 or a backplane publish fails, the operation is queued here
//! (deduplicated by key — latest wins) and replayed by a background task once the
//! dependency recovers. This mirrors FusionCache's auto-recovery: a bounded
//! queue, a retry budget, and a barrier delay after reconnection so the whole
//! cluster settles before replay.

use std::sync::{Arc, Weak};
use std::time::Duration;

use async_trait::async_trait;
use dashmap::DashMap;

use crate::error::Result;
use crate::time::{Clock, Timestamp};

/// The kind of operation to replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryAction {
    /// Re-write the entry to L2 and re-publish a `Set` notification.
    Set,
    /// Re-remove the key from L2 and re-publish a `Remove` notification.
    Remove,
    /// Re-publish an `Expire` notification.
    Expire,
}

/// A queued operation awaiting replay.
#[derive(Debug, Clone)]
pub struct RecoveryItem {
    /// The (prefixed) cache key.
    pub key: Arc<str>,
    /// What to replay.
    pub action: RecoveryAction,
    /// When the original operation happened (newer wins on dedup).
    pub timestamp: Timestamp,
    /// When this item should be abandoned (drop if `now >= expires_at`).
    pub expires_at: Timestamp,
    /// How many replay attempts remain (`None` ⇒ unbounded).
    pub remaining_retries: Option<u32>,
}

/// Configuration for the auto-recovery service.
#[derive(Debug, Clone)]
pub struct RecoveryConfig {
    /// Master switch.
    pub enabled: bool,
    /// Delay between drain passes (and the post-reconnect barrier).
    pub delay: Duration,
    /// Maximum queued items (`None` ⇒ unbounded).
    pub max_items: Option<usize>,
    /// Maximum replay attempts per item (`None` ⇒ unbounded).
    pub max_retries: Option<u32>,
}

impl Default for RecoveryConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            delay: Duration::from_secs(5),
            max_items: None,
            max_retries: None,
        }
    }
}

/// Replays a recovery item. Implemented by the cache (it re-does the L2 write /
/// backplane publish the item describes).
#[async_trait]
pub trait RecoveryExecutor: Send + Sync {
    /// Replays one item.
    ///
    /// # Errors
    /// Returns an error if the replay fails (the item is kept and retried).
    async fn replay(&self, item: &RecoveryItem) -> Result<()>;
}

/// The auto-recovery queue + background drain loop.
pub struct AutoRecoveryService {
    config: RecoveryConfig,
    clock: Arc<dyn Clock>,
    queue: DashMap<Arc<str>, RecoveryItem>,
    executor: std::sync::OnceLock<Weak<dyn RecoveryExecutor>>,
}

impl AutoRecoveryService {
    /// Creates a service. Call [`set_executor`](Self::set_executor) then
    /// [`spawn`](Self::spawn) to start draining.
    #[must_use]
    pub fn new(config: RecoveryConfig, clock: Arc<dyn Clock>) -> Arc<Self> {
        Arc::new(Self {
            config,
            clock,
            queue: DashMap::new(),
            executor: std::sync::OnceLock::new(),
        })
    }

    /// Wires the executor that replays items (the cache). Held as a `Weak` so the
    /// service never keeps the cache alive.
    pub fn set_executor(&self, executor: Weak<dyn RecoveryExecutor>) {
        let _ = self.executor.set(executor);
    }

    /// Number of queued items.
    #[must_use]
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    /// `true` if nothing is queued.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// Queues an operation for replay (deduplicated by key — the newest
    /// `timestamp` wins). Respects `max_items` by dropping the soonest-expiring
    /// item when full.
    pub fn enqueue(&self, item: RecoveryItem) {
        if !self.config.enabled {
            return;
        }
        // Latest-wins dedup.
        match self.queue.get(&item.key) {
            Some(existing) if existing.timestamp >= item.timestamp => return,
            _ => {}
        }
        if let Some(max) = self.config.max_items
            && self.queue.len() >= max
            && !self.queue.contains_key(&item.key)
        {
            self.evict_one_before(item.expires_at);
            if self.queue.len() >= max {
                return; // still full ⇒ reject the new item
            }
        }
        let mut item = item;
        if item.remaining_retries.is_none() {
            item.remaining_retries = self.config.max_retries;
        }
        self.queue.insert(Arc::clone(&item.key), item);
    }

    /// Drops the queued item that expires soonest, if it expires before `bound`.
    fn evict_one_before(&self, bound: Timestamp) {
        let victim = self
            .queue
            .iter()
            .min_by_key(|e| e.value().expires_at.ticks())
            .map(|e| (e.key().clone(), e.value().expires_at));
        if let Some((key, expires_at)) = victim
            && expires_at < bound
        {
            self.queue.remove(&key);
        }
    }

    /// Starts the background drain loop. Idempotent-safe to call once after
    /// [`set_executor`](Self::set_executor). Requires a tokio runtime.
    pub fn spawn(self: &Arc<Self>) {
        if !self.config.enabled {
            return;
        }
        let service = Arc::clone(self);
        let delay = self.config.delay;
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(delay.max(Duration::from_millis(50)));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                ticker.tick().await;
                // Exit when the cache (executor) is gone.
                let Some(executor) = service.executor.get().and_then(Weak::upgrade) else {
                    // No executor yet (or cache dropped). If the slot was never
                    // set we keep waiting; if it was set and is now dead, stop.
                    if service.executor.get().is_some() {
                        break;
                    }
                    continue;
                };
                service.drain_once(executor.as_ref()).await;
            }
        });
    }

    /// Replays every currently-queued item once. Public for deterministic tests.
    pub async fn drain_once(&self, executor: &dyn RecoveryExecutor) {
        let now = self.clock.now();
        let keys: Vec<Arc<str>> = self.queue.iter().map(|e| e.key().clone()).collect();
        for key in keys {
            let Some(item) = self.queue.get(&key).map(|e| e.value().clone()) else {
                continue;
            };
            if now >= item.expires_at {
                self.queue.remove(&key);
                continue;
            }
            match executor.replay(&item).await {
                Ok(()) => {
                    self.queue.remove(&key);
                }
                Err(_) => self.record_failure(&key),
            }
        }
    }

    /// Decrements an item's retry budget, dropping it when exhausted.
    fn record_failure(&self, key: &Arc<str>) {
        let mut drop_it = false;
        if let Some(mut entry) = self.queue.get_mut(key) {
            match entry.remaining_retries {
                Some(0) => drop_it = true,
                Some(n) => entry.remaining_retries = Some(n - 1),
                None => {}
            }
        }
        if drop_it {
            self.queue.remove(key);
        }
    }
}
