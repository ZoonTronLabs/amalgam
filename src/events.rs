//! The event hub.
//!
//! FusionCache exposes a rich set of events (hits, misses, fail-safe
//! activations, factory errors, …) fired on background threads. The idiomatic
//! Rust equivalent is a broadcast channel: subscribers receive a stream of
//! [`CacheEvent`]s without blocking the cache's hot path, and handler execution
//! is naturally decoupled from the operation that produced the event.

use std::sync::Arc;

use tokio::sync::broadcast;

/// An observable cache event.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum CacheEvent {
    /// A value was served. `stale` is `true` when it came from a fail-safe /
    /// stale fallback rather than a fresh entry.
    Hit {
        /// The cache key.
        key: Arc<str>,
        /// Whether the served value was stale.
        stale: bool,
    },
    /// Nothing servable was found for the key.
    Miss {
        /// The cache key.
        key: Arc<str>,
    },
    /// A value was written to the cache.
    Set {
        /// The cache key.
        key: Arc<str>,
    },
    /// A key was removed.
    Remove {
        /// The cache key.
        key: Arc<str>,
    },
    /// A key was logically expired.
    Expire {
        /// The cache key.
        key: Arc<str>,
    },
    /// The factory completed successfully on the foreground path.
    FactorySuccess {
        /// The cache key.
        key: Arc<str>,
    },
    /// The factory returned an error.
    FactoryError {
        /// The cache key.
        key: Arc<str>,
        /// The failure message.
        message: String,
    },
    /// The factory exceeded a (soft or hard) timeout.
    FactorySyntheticTimeout {
        /// The cache key.
        key: Arc<str>,
    },
    /// A stale value was served because the factory failed or timed out.
    FailSafeActivate {
        /// The cache key.
        key: Arc<str>,
    },
    /// A proactive background refresh was started.
    EagerRefresh {
        /// The cache key.
        key: Arc<str>,
    },
    /// A timed-out factory later completed successfully in the background.
    BackgroundFactorySuccess {
        /// The cache key.
        key: Arc<str>,
    },
    /// A timed-out factory later failed in the background.
    BackgroundFactoryError {
        /// The cache key.
        key: Arc<str>,
        /// The failure message.
        message: String,
    },
    /// All entries carrying a tag were invalidated.
    RemoveByTag {
        /// The tag.
        tag: String,
    },
    /// The whole cache was cleared.
    Clear,
}

/// A broadcaster of [`CacheEvent`]s.
///
/// Cloning an `Events` shares the same underlying channel.
#[derive(Debug, Clone)]
pub struct Events {
    sender: broadcast::Sender<CacheEvent>,
}

impl Events {
    /// Creates a hub with the given subscriber buffer capacity.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity.max(1));
        Self { sender }
    }

    /// Subscribes to the event stream.
    ///
    /// A subscriber that falls behind by more than the buffer capacity will
    /// observe a `Lagged` error from the receiver — events are best-effort
    /// observability, never a correctness mechanism.
    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<CacheEvent> {
        self.sender.subscribe()
    }

    /// Emits an event. Does nothing if there are no subscribers.
    pub fn emit(&self, event: CacheEvent) {
        // A send error only means "no live subscribers"; that is expected.
        let _ = self.sender.send(event);
    }
}

impl Default for Events {
    fn default() -> Self {
        Self::with_capacity(256)
    }
}
