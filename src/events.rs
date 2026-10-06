//! The event hub.
//!
//! FusionCache exposes a rich set of events (hits, misses, fail-safe
//! activations, factory errors, …) fired on background threads. The idiomatic
//! Rust equivalent is a broadcast channel: subscribers receive a stream of
//! [`CacheEvent`]s without blocking the cache's hot path, and handler execution
//! is naturally decoupled from the operation that produced the event.

mod eviction;
pub use eviction::{
    EvictionCapture, EvictionReceiveError, MemoryEviction, MemoryEvictionReason,
    MemoryEvictionSubscription, MemoryEvictions,
};
mod layers;
pub use layers::{BackplaneEvent, DistributedEvent, LayerEvent, MemoryEvent};

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use tokio::sync::broadcast;

use crate::memory::CapacityRejection;
use crate::plugins::{
    PendingPluginEvent, PluginError, PluginEventRoute, PluginHost, PluginHostInner,
    PluginObservations,
};

/// A bounded logical-operation label used by spans and metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheOperation {
    /// Read through or run an origin factory.
    GetOrSet,
    /// Read without running an origin factory.
    TryGet,
    /// Read or return a caller-provided default.
    GetOrDefault,
    /// Store a value.
    Set,
    /// Remove a key.
    Remove,
    /// Logically expire a key.
    Expire,
    /// Invalidate one tag.
    RemoveByTag,
    /// Invalidate multiple tags.
    RemoveByTags,
    /// Invalidate the cache.
    Clear,
}

impl CacheOperation {
    /// A stable bounded label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::GetOrSet => "get_or_set",
            Self::TryGet => "try_get",
            Self::GetOrDefault => "get_or_default",
            Self::Set => "set",
            Self::Remove => "remove",
            Self::Expire => "expire",
            Self::RemoveByTag => "remove_by_tag",
            Self::RemoveByTags => "remove_by_tags",
            Self::Clear => "clear",
        }
    }
}

/// A component whose value read was actually attempted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComponentRead {
    /// An in-process lookup, including a nonblocking ready lookup.
    Memory,
    /// A distributed backend get admitted through its read policy.
    Distributed,
}

/// A bounded cache component label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheLevel {
    /// The in-process store.
    Memory,
    /// The distributed store.
    Distributed,
    /// The origin factory.
    Origin,
    /// Peer notifications.
    Backplane,
}

impl CacheLevel {
    /// A stable bounded label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Distributed => "distributed",
            Self::Origin => "origin",
            Self::Backplane => "backplane",
        }
    }
}

/// A finite diagnostic outcome, including separate cancellation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationOutcome {
    /// A fresh value was served.
    Hit,
    /// A stale value was served.
    StaleHit,
    /// No usable value existed.
    Miss,
    /// A value was stored.
    Stored,
    /// A key was removed.
    Removed,
    /// A key was expired.
    Expired,
    /// Tags or the cache were invalidated.
    Invalidated,
    /// Configuration was rejected.
    ConfigurationError,
    /// Valid work was rejected by capacity, lifecycle or ordering state.
    Rejected,
    /// Requested clone isolation failed.
    CloneError,
    /// An origin factory failed.
    FactoryError,
    /// A lock failed.
    LockError,
    /// A backend operation failed.
    DistributedError,
    /// A codec operation failed.
    CodecError,
    /// A backplane operation failed.
    BackplaneError,
    /// A plugin lifecycle failed.
    PluginError,
    /// A deadline elapsed.
    TimedOut,
    /// The caller cancelled or dropped the operation.
    Cancelled,
    /// The cache no longer accepts operations.
    CacheClosed,
    /// Owned resources could not all be stopped cleanly.
    ShutdownError,
    /// External code violated its contract by panicking.
    Panicked,
}

impl OperationOutcome {
    /// A stable bounded label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Hit => "hit",
            Self::StaleHit => "stale_hit",
            Self::Miss => "miss",
            Self::Stored => "stored",
            Self::Removed => "removed",
            Self::Expired => "expired",
            Self::Invalidated => "invalidated",
            Self::ConfigurationError => "configuration_error",
            Self::Rejected => "rejected",
            Self::CloneError => "clone_error",
            Self::FactoryError => "factory_error",
            Self::LockError => "lock_error",
            Self::DistributedError => "distributed_error",
            Self::CodecError => "codec_error",
            Self::BackplaneError => "backplane_error",
            Self::TimedOut => "timed_out",
            Self::Cancelled => "cancelled",
            Self::PluginError => "plugin_error",
            Self::CacheClosed => "cache_closed",
            Self::ShutdownError => "shutdown_error",
            Self::Panicked => "panicked",
        }
    }

    /// Classifies the crate's closed failure family for logical-operation metrics.
    #[must_use]
    pub fn from_error(error: &crate::Error) -> Self {
        match error {
            crate::Error::ReentrantDrain { .. } => Self::Rejected,
            crate::Error::CircuitOpen { component } => match component {
                CircuitComponent::Distributed => Self::DistributedError,
                CircuitComponent::Backplane => Self::BackplaneError,
            },
            crate::Error::Config(_) => Self::ConfigurationError,
            crate::Error::Tag(crate::tags::TagError::Blank) => Self::ConfigurationError,
            crate::Error::Marker(error) => match error {
                crate::tags::MarkerError::Backend { .. } => Self::DistributedError,
                crate::tags::MarkerError::Protocol { .. }
                | crate::tags::MarkerError::ProtocolWithSource { .. } => Self::CodecError,
                crate::tags::MarkerError::ScopeCapacity { .. } => Self::Rejected,
                crate::tags::MarkerError::Unsupported
                | crate::tags::MarkerError::BlankWireVersion
                | crate::tags::MarkerError::ZeroCapacity => Self::ConfigurationError,
            },
            crate::Error::MemoryLocker(error) => match error {
                crate::MemoryLockerError::Provider { .. } => Self::LockError,
                crate::MemoryLockerError::Cancelled { .. } => Self::Cancelled,
            },
            crate::Error::Lease(error) => match error {
                crate::distributed_lock::LeaseError::Lost
                | crate::distributed_lock::LeaseError::Cancelled => Self::Cancelled,
                crate::distributed_lock::LeaseError::AcquisitionTimeout
                | crate::distributed_lock::LeaseError::CleanupTimeout => Self::TimedOut,
                crate::distributed_lock::LeaseError::Backend { .. } => Self::LockError,
                crate::distributed_lock::LeaseError::Task { source } => {
                    if source.is_cancelled() {
                        Self::Cancelled
                    } else {
                        Self::Panicked
                    }
                }
                crate::distributed_lock::LeaseError::MissingRuntime
                | crate::distributed_lock::LeaseError::InvalidTtl
                | crate::distributed_lock::LeaseError::InvalidToken
                | crate::distributed_lock::LeaseError::InvalidDeadline
                | crate::distributed_lock::LeaseError::UnsupportedTokenAcquisition
                | crate::distributed_lock::LeaseError::UnsupportedRenewal
                | crate::distributed_lock::LeaseError::OpaqueLifetime
                | crate::distributed_lock::LeaseError::UnsupportedFencing => {
                    Self::ConfigurationError
                }
            },
            crate::Error::Recovery(error) => match error {
                crate::recovery::RecoveryError::Task { source } => {
                    if source.is_cancelled() {
                        Self::Cancelled
                    } else {
                        Self::Panicked
                    }
                }
                crate::recovery::RecoveryError::GenerationExhausted
                | crate::recovery::RecoveryError::IdentityExhausted
                | crate::recovery::RecoveryError::ExecutorAlreadyConfigured
                | crate::recovery::RecoveryError::Stopped
                | crate::recovery::RecoveryError::SnapshotWritesSkipped
                | crate::recovery::RecoveryError::MarkerIdentityChanged
                | crate::recovery::RecoveryError::InvalidMarkerStage => Self::Rejected,
                crate::recovery::RecoveryError::ZeroDelay
                | crate::recovery::RecoveryError::MissingRuntime
                | crate::recovery::RecoveryError::UnsupportedMarkerExecutor
                | crate::recovery::RecoveryError::InvalidBarrier => Self::ConfigurationError,
            },
            crate::Error::Clone(_) => Self::CloneError,
            crate::Error::Plugin(_) => Self::PluginError,
            crate::Error::Registry(error) => match error {
                crate::RegistryError::Build { source, .. } => Self::from_error(source),
                crate::RegistryError::BlankName
                | crate::RegistryError::RecursiveInitialization { .. } => Self::ConfigurationError,
            },
            crate::Error::Factory { .. } | crate::Error::FactoryWithSource { .. } => {
                Self::FactoryError
            }
            crate::Error::FactoryCancelled { .. } | crate::Error::OperationCancelled { .. } => {
                Self::Cancelled
            }
            crate::Error::CacheClosed => Self::CacheClosed,
            crate::Error::Shutdown(_) => Self::ShutdownError,
            crate::Error::FactoryTimeout { .. }
            | crate::Error::LockTimeout { .. }
            | crate::Error::DistributedTimeout { .. } => Self::TimedOut,
            crate::Error::Serialization(_)
            | crate::Error::Deserialization(_)
            | crate::Error::Codec(_) => Self::CodecError,
            crate::Error::Distributed(_) => Self::DistributedError,
            crate::Error::Backplane(_) => Self::BackplaneError,
            crate::Error::Transport(error) => match error {
                crate::TransportError::Distributed { .. } => Self::DistributedError,
                crate::TransportError::Backplane { .. } => Self::BackplaneError,
            },
        }
    }
}

/// An observable cache event.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum CacheEvent {
    /// A fresh control observation began an owned, nonblocking eager refresh.
    MarkerEagerRefresh {
        /// The control identity; ordinary value factories are not involved.
        kind: crate::MarkerKind,
    },
    /// An expiring control snapshot write finished, separate from invalidation.
    MarkerSnapshotWrite {
        /// The scoped marker category; ordinary value keys are not parsed.
        kind: crate::MarkerKind,
        /// The actual completion, including deliberately suppressed provider faults.
        outcome: crate::MarkerSnapshotWriteOutcome,
    },
    /// A typed peer invalidation was applied and its observation cache updated.
    MarkerReceived {
        /// The full scoped control command, including the source and revision.
        command: crate::MarkerCommand,
    },
    /// A secondary invalidation read completed with explicit authority.
    MarkerRead {
        /// The control identity, independent of value keys.
        kind: crate::MarkerKind,
        /// Confirmed, reused, skipped or deliberately degraded control authority.
        outcome: crate::MarkerReadOutcome,
    },
    /// A supervised deferred storage/publication pipeline failed.
    BackgroundCommitError {
        /// The affected data key.
        key: Arc<str>,
        /// Original failure diagnostic.
        message: String,
    },
    /// One logical operation finished; keys and instance IDs are deliberately
    /// absent so this event can feed bounded metric labels.
    OperationCompleted {
        /// The logical operation.
        operation: CacheOperation,
        /// Its explicit final outcome.
        outcome: OperationOutcome,
        /// Monotonic elapsed duration.
        elapsed: Duration,
        /// A single servicing component, when applicable.
        level: Option<CacheLevel>,
    },
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
    /// A timed-out or eager-refresh factory completed successfully in the background.
    BackgroundFactorySuccess {
        /// The cache key.
        key: Arc<str>,
    },
    /// A timed-out or eager-refresh factory failed in the background.
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
    /// An entry was evicted from L1 by the backend's size/expiry policy.
    Eviction {
        /// The cache key.
        key: Arc<str>,
    },
    /// L1 rejected a candidate while preserving already-retained valid entries.
    MemoryAdmissionRejected {
        /// The candidate's cache key.
        key: Arc<str>,
        /// The explicit admission rejection.
        reason: CapacityRejection,
    },
    /// A distributed-cache or backplane circuit breaker opened or closed.
    CircuitBreakerChange {
        /// Which component the breaker guards.
        component: CircuitComponent,
        /// `true` if the breaker is now closed (healthy), `false` if open.
        closed: bool,
    },
    /// A value failed to serialize for L2.
    SerializationError {
        /// The cache key.
        key: Arc<str>,
        /// The error message.
        message: String,
    },
    /// A value failed to deserialize from L2.
    DeserializationError {
        /// The cache key.
        key: Arc<str>,
        /// The error message.
        message: String,
    },
    /// A backplane notification was published to peers.
    MessagePublished {
        /// The cache key.
        key: Arc<str>,
    },
    /// A backplane notification was received from a peer.
    MessageReceived {
        /// The cache key.
        key: Arc<str>,
    },
}

/// Which subsystem a [`CacheEvent::CircuitBreakerChange`] refers to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitComponent {
    /// The L2 distributed cache.
    Distributed,
    /// The multi-node backplane.
    Backplane,
}

/// A broadcaster of [`CacheEvent`]s.
///
/// Cloning an `Events` shares the same underlying channel.
#[derive(Clone)]
pub struct Events {
    inner: Arc<EventHub>,
}

struct EventHub {
    sender: broadcast::Sender<CacheEvent>,
    // Monotonic: raw broadcast receivers can resubscribe without this hub.
    // Before the first subscription, emission need not lock the channel.
    broadcast_armed: AtomicBool,
    layers: OnceLock<broadcast::Sender<LayerEvent>>,
    capacity: usize,
    plugins: OnceLock<PluginEventRoute>,
}

/// The explicit result of routing an event to observers and plugins.
#[derive(Debug)]
pub struct EventEmission {
    /// Number of broadcast observers; zero is a legitimate count.
    pub subscribers: usize,
    /// Independent plugin failures, with their original sources.
    pub plugin_errors: Vec<PluginError>,
}

/// An event stream whose loss accounting survives broadcast buffer lag.
#[derive(Debug)]
pub struct EventSubscription {
    receiver: broadcast::Receiver<CacheEvent>,
    lost_events: u64,
}

/// The owning event stream closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("cache event stream closed")]
pub struct EventStreamClosed;

impl EventSubscription {
    /// Receives the next available event, continuing after any buffer lag.
    pub async fn recv(&mut self) -> Result<CacheEvent, EventStreamClosed> {
        loop {
            match self.receiver.recv().await {
                Ok(event) => return Ok(event),
                Err(broadcast::error::RecvError::Lagged(lost)) => {
                    self.lost_events = self.lost_events.saturating_add(lost);
                }
                Err(broadcast::error::RecvError::Closed) => return Err(EventStreamClosed),
            }
        }
    }

    /// The total number of events lost while this subscription was lagging.
    #[must_use]
    pub fn lost_events(&self) -> u64 {
        self.lost_events
    }
}

/// A component event stream whose loss accounting survives buffer lag.
#[derive(Debug)]
pub struct LayerEventSubscription {
    receiver: broadcast::Receiver<LayerEvent>,
    lost_events: u64,
}

impl LayerEventSubscription {
    /// Receives the next component fact, continuing after lag.
    pub async fn recv(&mut self) -> Result<LayerEvent, EventStreamClosed> {
        loop {
            match self.receiver.recv().await {
                Ok(event) => return Ok(event),
                Err(broadcast::error::RecvError::Lagged(lost)) => {
                    self.lost_events = self.lost_events.saturating_add(lost);
                }
                Err(broadcast::error::RecvError::Closed) => return Err(EventStreamClosed),
            }
        }
    }

    /// Total facts lost while this receiver was lagging.
    #[must_use]
    pub fn lost_events(&self) -> u64 {
        self.lost_events
    }
}

impl Events {
    /// Creates a hub with the given subscriber buffer capacity.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity.max(1));
        Self {
            inner: Arc::new(EventHub {
                sender,
                broadcast_armed: AtomicBool::new(false),
                layers: OnceLock::new(),
                capacity: capacity.max(1),
                plugins: OnceLock::new(),
            }),
        }
    }

    /// Subscribes to the event stream.
    ///
    /// A subscriber that falls behind by more than the buffer capacity will
    /// observe a `Lagged` error from the receiver — events are best-effort
    /// observability, never a correctness mechanism.
    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<CacheEvent> {
        // Publish before channel admission. A racing emission may precede
        // admission, but later emissions cannot skip an admitted receiver.
        self.inner.broadcast_armed.store(true, Ordering::Release);
        self.inner.sender.subscribe()
    }

    /// Subscribes with automatic lag recovery and explicit lost-event accounting.
    #[must_use]
    pub fn subscribe_resilient(&self) -> EventSubscription {
        EventSubscription {
            receiver: self.subscribe(),
            lost_events: 0,
        }
    }

    /// Subscribes to physical component facts independently of logical events.
    /// The channel is allocated only on subscription; keys/payloads are built
    /// while a stream reader or an opted-in plugin can receive them.
    #[must_use]
    pub fn subscribe_layers(&self) -> broadcast::Receiver<LayerEvent> {
        self.inner
            .layers
            .get_or_init(|| {
                let (sender, _) = broadcast::channel(self.inner.capacity);
                sender
            })
            .subscribe()
    }

    /// Subscribes to component facts with lag recovery and explicit loss counts.
    #[must_use]
    pub fn subscribe_layers_resilient(&self) -> LayerEventSubscription {
        LayerEventSubscription {
            receiver: self.subscribe_layers(),
            lost_events: 0,
        }
    }

    /// Emits a physical fact to selected plugins and the independent stream.
    pub fn emit_layer(&self, event: LayerEvent) -> usize {
        let emission = self.emit_layer_checked(event);
        for error in emission.plugin_errors {
            tracing::warn!(%error, "amalgam: layer plugin event failed");
        }
        emission.subscribers
    }
    /// Preserves original plugin failures independently from stream delivery.
    pub fn emit_layer_checked(&self, event: LayerEvent) -> EventEmission {
        let plugin_errors = self
            .plugin_host()
            .map_or_else(Vec::new, |host| host.notify_layer(&event));
        let subscribers = self
            .inner
            .layers
            .get()
            .map_or(0, |sender| sender.send(event).unwrap_or(0));
        EventEmission {
            subscribers,
            plugin_errors,
        }
    }
    pub(crate) fn operation_started(&self, operation: CacheOperation) {
        if self
            .inner
            .plugins
            .get()
            .is_some_and(PluginEventRoute::has_observers)
            && let Some(host) = self.plugin_host()
        {
            for error in host.notify_started(operation) {
                tracing::warn!(%error, "amalgam: operation observer failed");
            }
        }
    }

    pub(crate) fn component_read_deferred(
        &self,
        component: ComponentRead,
        defer: impl FnOnce(PendingPluginEvent),
    ) {
        if self
            .inner
            .plugins
            .get()
            .is_some_and(PluginEventRoute::has_observers)
            && let Some(batch) = self
                .plugin_host()
                .and_then(|host| host.capture(PluginObservations::All))
        {
            defer(batch.component_read(component));
        }
    }
    pub(crate) fn capacity(&self) -> usize {
        self.inner.capacity
    }
    pub(crate) fn has_layer_receivers(&self) -> bool {
        self.inner
            .layers
            .get()
            .is_some_and(|sender| sender.receiver_count() > 0)
            || self
                .inner
                .plugins
                .get()
                .is_some_and(PluginEventRoute::has_observers)
    }

    pub(crate) fn emit_layer_lazy(&self, make: impl FnOnce() -> LayerEvent) {
        self.emit_layer_deferred(make, PendingPluginEvent::deliver);
    }
    pub(crate) fn emit_layer_deferred(
        &self,
        make: impl FnOnce() -> LayerEvent,
        defer: impl FnOnce(PendingPluginEvent),
    ) {
        let batch = if self
            .inner
            .plugins
            .get()
            .is_some_and(PluginEventRoute::has_observers)
        {
            self.plugin_host()
                .and_then(|host| host.capture(PluginObservations::All))
        } else {
            None
        };
        let sender = self
            .inner
            .layers
            .get()
            .filter(|sender| sender.receiver_count() > 0);
        if batch.is_none() && sender.is_none() {
            return;
        }
        let event = make();
        if let Some(sender) = sender {
            let _ = sender.send(event.clone());
        }
        if let Some(batch) = batch {
            defer(batch.layer(event));
        }
    }
    pub(crate) fn emit_deferred(&self, event: CacheEvent, defer: impl FnOnce(PendingPluginEvent)) {
        let host = self.plugin_host();
        let batch = host
            .as_ref()
            .and_then(|host| host.capture(PluginObservations::All));
        // Existing Logical hooks retain their pre-effect timing. Opted-in All
        // sessions use post-coordination delivery for logical and layer facts.
        if let Some(host) = host {
            for error in host.notify_legacy(&event) {
                tracing::warn!(%error, "amalgam: plugin event failed");
            }
        }
        if self.has_broadcast_receivers() {
            let _ = self.inner.sender.send(event.clone());
        }
        if let Some(batch) = batch {
            defer(batch.logical(event));
        }
    }

    /// Keeps optional worker facts lazy while retaining selected callback leases.
    pub(crate) fn emit_deferred_lazy(
        &self,
        make: impl FnOnce() -> CacheEvent,
        defer: impl FnOnce(PendingPluginEvent),
    ) {
        let host = self.plugin_host();
        if !self.has_broadcast_receivers() && host.as_ref().is_none_or(|host| !host.has_listeners())
        {
            return;
        }
        self.emit_deferred(make(), defer);
    }

    /// Associates this hub with exactly one owning host. The reference is weak,
    /// so keeping an event hub/observer alive cannot prolong plugin lifecycle.
    pub fn attach_plugins(&self, host: &PluginHost) -> Result<(), PluginError> {
        self.inner
            .plugins
            .set(host.event_route())
            .map_err(|_| PluginError::EventsAlreadyAttached)
    }

    /// Emits an event to attached plugins and any broadcast subscribers.
    pub fn emit(&self, event: CacheEvent) {
        for error in self.emit_checked(event).plugin_errors {
            tracing::warn!(error = %error, "amalgam: plugin event failed");
        }
    }

    /// The sole event route, including memory eviction and background sources.
    pub fn emit_checked(&self, event: CacheEvent) -> EventEmission {
        self.emit_using(event, self.plugin_host())
    }

    /// Constructs allocation-bearing events only when an observer can receive
    /// them. Admission is checked at emission, after any user clone callback.
    pub(crate) fn emit_lazy(&self, make: impl FnOnce() -> CacheEvent) {
        let host = self.plugin_host();
        if !self.has_broadcast_receivers() && host.as_ref().is_none_or(|host| !host.has_listeners())
        {
            return;
        }
        for error in self.emit_using(make(), host).plugin_errors {
            tracing::warn!(error = %error, "amalgam: plugin event failed");
        }
    }

    fn plugin_host(&self) -> Option<Arc<PluginHostInner>> {
        self.inner.plugins.get().and_then(PluginEventRoute::upgrade)
    }

    fn has_broadcast_receivers(&self) -> bool {
        self.inner.broadcast_armed.load(Ordering::Acquire) && self.inner.sender.receiver_count() > 0
    }

    fn emit_using(&self, event: CacheEvent, host: Option<Arc<PluginHostInner>>) -> EventEmission {
        let plugin_errors = host.map_or_else(Vec::new, |host| host.notify(&event));
        // A send failure means there are no observers, a normal lifecycle state.
        let subscribers = if self.inner.broadcast_armed.load(Ordering::Acquire) {
            self.inner.sender.send(event).unwrap_or(0)
        } else {
            0
        };
        EventEmission {
            subscribers,
            plugin_errors,
        }
    }
}

impl std::fmt::Debug for Events {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Events")
            .field("subscribers", &self.inner.sender.receiver_count())
            .field("plugins_attached", &self.inner.plugins.get().is_some())
            .finish()
    }
}

impl Default for Events {
    fn default() -> Self {
        Self::with_capacity(256)
    }
}

#[cfg(test)]
mod layer_tests {
    use super::*;

    #[test]
    fn layer_payloads_are_lazy_and_late_subscription_sees_only_new_facts() {
        let hub = Events::default();
        hub.emit_layer_lazy(|| panic!("no receiver must not build a payload"));
        assert!(hub.inner.layers.get().is_none());
        let mut receiver = hub.subscribe_layers();
        assert!(matches!(
            receiver.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));
        hub.emit_layer_lazy(|| {
            LayerEvent::Memory(MemoryEvent::Miss {
                key: Arc::from("after"),
            })
        });
        assert_eq!(
            receiver.try_recv().unwrap(),
            LayerEvent::Memory(MemoryEvent::Miss {
                key: Arc::from("after")
            })
        );
        drop(receiver);
        hub.emit_layer_lazy(|| panic!("last receiver dropped must not build a payload"));
    }
}
