//! Component facts are distinct from the final logical operation outcome.

use std::sync::Arc;

use crate::backplane::{BackplaneCommand, BackplaneMessage};

/// A physically observed component operation. Delivery is best-effort, and
/// receiving a hit does not prove that the logical read ultimately served it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LayerEvent {
    /// An in-process entry operation.
    Memory(MemoryEvent),
    /// A distributed entry or codec operation.
    Distributed(DistributedEvent),
    /// A peer notification or notification circuit transition.
    Backplane(BackplaneEvent),
}

/// Facts observed at the memory component boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemoryEvent {
    /// A physically live entry was found before tag/clear validation.
    Hit {
        /// The processed logical key.
        key: Arc<str>,
        /// Whether the found entry was already logically expired.
        stale: bool,
    },
    /// No physically live entry was found.
    Miss {
        /// The processed logical key.
        key: Arc<str>,
    },
    /// A candidate actually entered memory, including passive hydration.
    Set {
        /// The processed logical key.
        key: Arc<str>,
    },
    /// A remove was applied to memory, including an already absent key.
    Remove {
        /// The processed logical key.
        key: Arc<str>,
    },
    /// A physically live entry was logically expired in memory.
    Expire {
        /// The processed logical key.
        key: Arc<str>,
    },
}

/// Facts observed at the distributed component boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DistributedEvent {
    /// A physically live decoded entry was found before tag/clear validation.
    Hit {
        /// The processed logical key, without a storage envelope prefix.
        key: Arc<str>,
        /// Whether the decoded entry was already logically expired.
        stale: bool,
    },
    /// An attempted read ended without a decoded entry, including a deliberately
    /// suppressed provider/deadline/codec failure. A skipped read emits nothing.
    Miss {
        /// The processed logical key.
        key: Arc<str>,
    },
    /// A value write actually completed, including replay and retained expiry.
    Set {
        /// The processed logical key.
        key: Arc<str>,
    },
    /// A removal actually completed, including remove-on-expire.
    Remove {
        /// The processed logical key.
        key: Arc<str>,
    },
    /// The distributed circuit actually changed admission state.
    CircuitBreakerChange {
        /// True denotes healthy/closed admission.
        closed: bool,
    },
    /// Encoding an entry failed; operation/receipt errors retain their cause.
    SerializationError {
        /// The processed logical key.
        key: Arc<str>,
    },
    /// Decoding an entry failed; operation/receipt errors retain their cause.
    DeserializationError {
        /// The processed logical key.
        key: Arc<str>,
    },
}

/// Notification facts, independent of whether a received command was applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackplaneEvent {
    /// The notification circuit actually changed admission state.
    CircuitBreakerChange {
        /// True denotes healthy/closed admission.
        closed: bool,
    },
    /// A provider successfully accepted the exact typed command.
    MessagePublished {
        /// Data source/revision/action/key or the complete scoped marker command.
        command: BackplaneCommand,
    },
    /// A foreign transport frame arrived before validation, scope filtering or
    /// conflict checks. Self frames and IgnoreIncoming frames are excluded.
    MessageReceived {
        /// The actual received envelope; receipt does not imply acceptance.
        message: BackplaneMessage,
    },
}
