//! The backplane: multi-node invalidation messaging.
//!
//! In a multi-node deployment each node has its own L1. When one node changes a
//! key, it publishes a small [`BackplaneMessage`] (never the value itself) so the
//! other nodes can invalidate or expire their local L1 copy and re-pull the
//! authoritative value from L2 on the next read.
//!
//! The [`InProcessBackplane`] here is a faithful reference backed by a broadcast
//! channel — perfect for tests and single-process multi-instance setups. A Redis
//! pub/sub adapter is a feature-gated follow-up.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::broadcast;

use crate::error::Result;
use crate::time::Timestamp;

/// The kind of change a backplane message announces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackplaneAction {
    /// A key was written; receivers should drop their local copy and re-pull L2.
    Set,
    /// A key was removed; receivers should hard-evict it.
    Remove,
    /// A key was logically expired; receivers should expire (keep for fail-safe).
    Expire,
}

/// A backplane notification. Carries no value — only what changed and when.
#[derive(Debug, Clone)]
pub struct BackplaneMessage {
    /// The id of the publishing cache instance (so receivers ignore their own).
    pub source_id: Arc<str>,
    /// When the change happened (for "newer wins" expiration).
    pub timestamp: Timestamp,
    /// What kind of change occurred.
    pub action: BackplaneAction,
    /// The (prefixed) key affected.
    pub key: Arc<str>,
}

/// A multi-node notification channel.
///
/// Implement over Redis pub/sub, NATS, etc. The cache publishes on local changes
/// and subscribes to apply remote ones.
#[async_trait]
pub trait Backplane: Send + Sync {
    /// Publishes a message to all other subscribers.
    ///
    /// # Errors
    /// Returns [`Error::Backplane`](crate::Error::Backplane) on transport failure.
    async fn publish(&self, message: BackplaneMessage) -> Result<()>;

    /// Subscribes to the message stream.
    fn subscribe(&self) -> broadcast::Receiver<BackplaneMessage>;
}

/// An in-process reference [`Backplane`] backed by a broadcast channel.
///
/// Share one instance (via `Arc`) between several [`Cache`](crate::Cache)
/// instances to simulate a multi-node cluster within one process.
#[derive(Clone)]
pub struct InProcessBackplane {
    sender: broadcast::Sender<BackplaneMessage>,
}

impl InProcessBackplane {
    /// Creates a backplane with the given message buffer capacity.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity.max(1));
        Self { sender }
    }
}

impl Default for InProcessBackplane {
    fn default() -> Self {
        Self::with_capacity(256)
    }
}

#[async_trait]
impl Backplane for InProcessBackplane {
    async fn publish(&self, message: BackplaneMessage) -> Result<()> {
        // A send error only means "no live subscribers"; that is fine.
        let _ = self.sender.send(message);
        Ok(())
    }

    fn subscribe(&self) -> broadcast::Receiver<BackplaneMessage> {
        self.sender.subscribe()
    }
}
