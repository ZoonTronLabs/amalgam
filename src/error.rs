//! Error types.
//!
//! Following the project guideline that libraries expose typed errors via
//! `thiserror`, every fallible boundary returns [`Error`]. Business outcomes
//! that are *not* failures (a cache miss, a factory choosing to reuse a stale
//! value) are modelled in the return *type*, never as errors.

use std::time::Duration;

/// Cache drainage operations that cannot await their own synchronous factory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainOperation {
    /// Close the cache and wait for all owned work.
    Shutdown,
    /// Wait for currently scheduled commits and factories.
    FlushPending,
}

/// A rejected cache configuration. Configuration is checked before work starts.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConfigError {
    /// Local-only reconciliation cannot cover an external data/notification provider.
    #[error("local-only reconciliation requires memory-only storage and no backplane")]
    LocalReconciliationWithExternalStorage,
    /// Strict continuity requires an acknowledged health stream.
    #[error("backplane continuity policy requires a connection-state provider")]
    UnavailableBackplaneContinuity,
    /// Best-effort notification reconciliation requires a configured backplane.
    #[error("best-effort backplane reconciliation requires a backplane")]
    BestEffortReconciliationWithoutBackplane,
    /// A finite timer cannot be represented by the monotonic runtime clock.
    #[error("finite deadline exceeds the monotonic clock range")]
    DeadlineOutOfRange,
    /// Reconciliation requires a positive interval.
    #[error("reconciliation interval must be positive")]
    ZeroReconciliationInterval,
    /// A legacy size request was negative.
    #[error("entry weight must be nonnegative, got {size}")]
    NegativeEntryWeight {
        /// The rejected legacy size.
        size: i64,
    },
    /// L2 was configured without a codec.
    #[error("a distributed cache requires a serializer")]
    DistributedWithoutSerializer,
    /// Expiring marker snapshots require their explicit options-controlled read mode.
    #[error("cached marker snapshots require OptionsControlled marker reads")]
    MarkerLifecycleRequiresControlledReads,
    /// The selected durable provider does not supply expiring marker snapshots.
    #[error("the invalidation provider does not supply a marker snapshot cache")]
    MarkerSnapshotCapabilityUnavailable,
    /// Deep cloning was requested without an implementation.
    #[error("auto-clone requires a value cloner or a serializer that supplies one")]
    AutoCloneWithoutCloner,
    /// A configured background service requires a Tokio runtime.
    #[error("{component:?} requires a Tokio runtime")]
    MissingRuntime {
        /// The service requiring the runtime.
        component: RuntimeComponent,
    },
    /// A diagnostic or wire identity was empty.
    #[error("{field:?} must not be blank")]
    BlankIdentity {
        /// The rejected identity field.
        field: IdentityField,
    },
    /// Expiration metadata would make a value fresh after it is physically dead.
    #[error("logical expiration exceeds physical expiration")]
    InvalidEntryDeadlines,
    /// A recovery interval would create a continuously running retry loop.
    #[error("enabled auto-recovery requires a positive interval")]
    ZeroRecoveryInterval,
    /// An external jitter source produced a sample outside the configured bound.
    #[error("jitter sample {sample:?} exceeds maximum {maximum:?}")]
    InvalidJitterSample {
        /// The rejected sample.
        sample: Duration,
        /// The configured upper bound.
        maximum: Duration,
    },
}

/// Background services with runtime requirements.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeComponent {
    /// Monotonic budgets or cache-owned asynchronous work.
    Execution,
    /// The backplane listener.
    Backplane,
    /// The automatic recovery worker.
    Recovery,
    /// A plugin session.
    Plugin,
}

/// Validated identity fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentityField {
    /// The cache's diagnostic name.
    CacheName,
    /// The instance identifier used for peer filtering.
    InstanceId,
    /// The distributed wire namespace.
    WireVersion,
}

/// The reason a factory's execution scope was cancelled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FactoryCancellationReason {
    /// The caller explicitly requested cancellation.
    CallerCancelled,
    /// The caller dropped the operation future.
    CallerDropped,
    /// The soft wait deadline elapsed without background completion.
    SoftTimeout,
    /// The hard execution deadline elapsed.
    HardTimeout,
    /// The owning cache shut down.
    CacheShutdown,
    /// The distributed ownership lease was lost.
    LeaseLost,
    /// The execution scope ended, including successful completion.
    ScopeFinished,
}

impl FactoryCancellationReason {
    /// A stable diagnostic description.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CallerCancelled => "caller requested cancellation",
            Self::CallerDropped => "caller dropped the operation",
            Self::SoftTimeout => "factory soft wait deadline elapsed",
            Self::HardTimeout => "factory hard deadline elapsed",
            Self::CacheShutdown => "cache shut down",
            Self::LeaseLost => "distributed ownership lease was lost",
            Self::ScopeFinished => "factory execution scope ended",
        }
    }
}

/// A deep-copy implementation failed. Its original cause remains available.
#[derive(Debug, thiserror::Error)]
pub enum CloneError {
    /// Encoding the value failed.
    #[error("deep-copy serialization failed: {source}")]
    Serialization {
        /// The codec's original failure.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// Decoding the copied value failed.
    #[error("deep-copy deserialization failed: {source}")]
    Deserialization {
        /// The codec's original failure.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// A custom cloning strategy failed.
    #[error("deep-copy failed: {source}")]
    Custom {
        /// The strategy's original failure.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

impl CloneError {
    /// Preserves a custom strategy's source error.
    pub fn from_source(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Custom {
            source: Box::new(source),
        }
    }
}

/// A distributed codec failure with its original concrete source.
#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    /// Encoding a value or its snapshot metadata failed.
    #[error("serialization failed: {source}")]
    Serialization {
        /// The codec's unchanged failure.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// Decoding a value or its snapshot metadata failed.
    #[error("deserialization failed: {source}")]
    Deserialization {
        /// The codec's unchanged failure.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

/// An external storage or notification failure with its original source.
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    /// Value storage failed.
    #[error("distributed cache error: {source}")]
    Distributed {
        /// The provider's unchanged failure.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// Peer notification failed.
    #[error("backplane error: {source}")]
    Backplane {
        /// The provider's unchanged failure.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

/// The crate-wide result alias.
pub type Result<T> = std::result::Result<T, Error>;

/// An owned task or cleanup stage whose shutdown can fail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownTask {
    /// An origin factory and its completion work.
    Factory,
    /// A distributed storage operation.
    Distributed,
    /// Peer notification work.
    Backplane,
    /// The recovery worker.
    Recovery,
    /// Physical-expiry maintenance.
    Maintenance,
    /// Releasing an owned distributed lease.
    LeaseRelease,
}

impl ShutdownTask {
    /// A finite diagnostic stage label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Factory => "factory",
            Self::Distributed => "distributed",
            Self::Backplane => "backplane",
            Self::Recovery => "recovery",
            Self::Maintenance => "maintenance",
            Self::LeaseRelease => "lease_release",
        }
    }
}

/// A closed failure family retained by concurrent/idempotent cache shutdown.
#[derive(Debug, thiserror::Error)]
pub enum ShutdownFailure {
    /// An attachment's teardown failed.
    #[error(transparent)]
    Plugin(#[from] crate::plugins::PluginError),
    /// An owned task violated its contract or unexpectedly lost its join handle.
    #[error("{description} task could not be joined: {source}", description = task.as_str())]
    BackgroundTask {
        /// The owned task's role.
        task: ShutdownTask,
        /// The original join failure, distinct from origin failure.
        #[source]
        source: std::sync::Arc<tokio::task::JoinError>,
    },
    /// An awaited cleanup/backend operation failed.
    #[error(transparent)]
    Work(Error),
}

/// A nonempty immutable set of shutdown failures. Cloning keeps the same
/// original sources so every concurrent or repeated caller sees one report.
#[derive(Debug, Clone)]
pub struct ShutdownError {
    failures: std::sync::Arc<[ShutdownFailure]>,
}

impl ShutdownError {
    /// Creates a valid report with at least one failure.
    #[must_use]
    pub fn new(
        first: ShutdownFailure,
        remaining: impl IntoIterator<Item = ShutdownFailure>,
    ) -> Self {
        let mut failures = vec![first];
        failures.extend(remaining);
        Self {
            failures: failures.into(),
        }
    }

    /// Every retained failure, in shutdown observation order.
    #[must_use]
    pub fn failures(&self) -> &[ShutdownFailure] {
        &self.failures
    }
}

impl std::fmt::Display for ShutdownError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "cache shutdown failed in {} stage(s): {}",
            self.failures.len(),
            self.failures[0]
        )
    }
}

impl std::error::Error for ShutdownError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.failures[0])
    }
}

/// An error surfaced from a cache operation.
///
/// Note what is deliberately *absent*: a "cache miss" is not an error (it is a
/// `None`/`MaybeValue::none`), and a factory that fails while fail-safe rescues
/// a stale value never produces an `Error` at all.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// A synchronous callback would wait for its own cache execution to finish.
    #[error("a factory cannot {operation:?} its own cache before returning")]
    ReentrantDrain {
        /// The rejected operation; no close or drainage was started.
        operation: DrainOperation,
    },

    /// A raw tag boundary rejected invalid input.
    #[error(transparent)]
    Tag(#[from] crate::tags::TagError),

    /// Typed invalidation storage or control protocol failure.
    #[error(transparent)]
    Marker(#[from] crate::tags::MarkerError),

    /// Typed distributed ownership or fencing failure.
    #[error(transparent)]
    Lease(#[from] crate::distributed_lock::LeaseError),

    /// Typed recovery construction/lifecycle failure.
    #[error(transparent)]
    Recovery(#[from] crate::recovery::RecoveryError),

    /// One or more owned teardown operations failed.
    #[error(transparent)]
    Shutdown(#[from] ShutdownError),

    /// Configuration was rejected before this operation started.
    #[error(transparent)]
    Config(#[from] ConfigError),

    /// The requested isolation could not be produced.
    #[error(transparent)]
    Clone(#[from] CloneError),

    /// A configured plugin attachment/lifecycle failed.
    #[error(transparent)]
    Plugin(#[from] crate::plugins::PluginError),

    /// Named-cache initialization failed.
    #[error(transparent)]
    Registry(#[from] crate::registry::RegistryError),

    /// The factory failed and no stale value or fail-safe default was available
    /// to fall back to. Carries the message reported by the factory.
    #[error("factory failed: {message}")]
    Factory {
        /// The failure message reported by the factory.
        message: String,
    },

    /// A factory failure with its complete original error chain.
    #[error("factory failed: {message}")]
    FactoryWithSource {
        /// The factory's diagnostic message.
        message: String,
        /// The factory failure, including its original source.
        #[source]
        source: FactoryError,
    },

    /// Cancellation is distinct from origin failure and fail-safe activation.
    #[error("factory cancelled: {description}", description = reason.as_str())]
    FactoryCancelled {
        /// The reason the execution scope ended.
        reason: FactoryCancellationReason,
    },

    /// A public read, mutation or lifecycle operation was cancelled.
    #[error("cache operation cancelled: {description}", description = reason.as_str())]
    OperationCancelled {
        /// The reason the operation's execution scope ended.
        reason: FactoryCancellationReason,
    },

    /// The cache has begun closing and no longer accepts operations.
    #[error("cache is closed")]
    CacheClosed,

    /// An eligible backend was not queried because its transport circuit is
    /// open. This is unavailable storage, distinct from a successful absence.
    #[error("{component:?} circuit is open")]
    CircuitOpen {
        /// The circuit which refused this operation.
        component: crate::events::CircuitComponent,
    },

    /// The factory exceeded its hard timeout and no fallback value existed.
    #[error("factory timed out after {elapsed:?}")]
    FactoryTimeout {
        /// How long the factory was allowed to run before timing out.
        elapsed: Duration,
    },

    /// Acquiring the per-key single-flight lock exceeded its timeout and no
    /// fallback value existed.
    #[error("lock acquisition timed out after {elapsed:?}")]
    LockTimeout {
        /// How long the caller waited for the lock.
        elapsed: Duration,
    },

    /// A value could not be serialized for the distributed (L2) cache.
    /// Legacy message-only adapter; use [`Error::serialization`] to retain a source.
    #[error("serialization failed: {0}")]
    Serialization(String),

    /// A value could not be deserialized from the distributed (L2) cache.
    #[error("deserialization failed: {0}")]
    Deserialization(String),

    /// Encoding or decoding failed with its original concrete cause.
    #[error(transparent)]
    Codec(#[from] CodecError),

    /// The distributed (L2) cache backend returned an error.
    #[error("distributed cache error: {0}")]
    Distributed(String),

    /// The backplane backend returned an error.
    #[error("backplane error: {0}")]
    Backplane(String),

    /// External storage or notification failed with its original concrete cause.
    #[error(transparent)]
    Transport(#[from] TransportError),
}

impl Error {
    /// Preserves an encoding failure instead of converting it into a message.
    pub fn serialization(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        CodecError::Serialization {
            source: Box::new(source),
        }
        .into()
    }

    /// Preserves a decoding failure instead of converting it into a message.
    pub fn deserialization(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        CodecError::Deserialization {
            source: Box::new(source),
        }
        .into()
    }

    /// Preserves a value-storage failure at the provider boundary.
    pub fn distributed(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        TransportError::Distributed {
            source: Box::new(source),
        }
        .into()
    }

    /// Preserves a peer-notification failure at the provider boundary.
    pub fn backplane(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        TransportError::Backplane {
            source: Box::new(source),
        }
        .into()
    }
}

/// The error a user-supplied factory returns to signal failure.
///
/// Returning this (or calling [`FactoryContext::fail`](crate::FactoryContext::fail))
/// triggers the fail-safe path: a stale value or the `fail_safe_default` is
/// served if available, otherwise the failure is surfaced as [`Error::Factory`].
///
/// It can wrap an arbitrary source error so the original cause is preserved in
/// the error chain.
#[derive(Debug)]
pub struct FactoryError {
    detail: FactoryErrorDetail,
}

#[derive(Debug)]
enum FactoryErrorDetail {
    Message(String),
    Source {
        message: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    Cancelled(FactoryCancellationReason),
}

impl FactoryError {
    /// Creates a factory error with a human-readable message.
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            detail: FactoryErrorDetail::Message(into_nonblank(message.into())),
        }
    }

    /// Creates a factory error from an arbitrary source error, preserving it in
    /// the error chain.
    #[must_use]
    pub fn from_source<E>(source: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self {
            detail: FactoryErrorDetail::Source {
                message: into_nonblank(source.to_string()),
                source: Box::new(source),
            },
        }
    }

    /// Reports cancellation without turning it into an origin failure.
    #[must_use]
    pub fn cancelled(reason: FactoryCancellationReason) -> Self {
        Self {
            detail: FactoryErrorDetail::Cancelled(reason),
        }
    }

    /// The cancellation reason, when this is a cancellation outcome.
    #[must_use]
    pub fn cancellation_reason(&self) -> Option<FactoryCancellationReason> {
        match self.detail {
            FactoryErrorDetail::Cancelled(reason) => Some(reason),
            FactoryErrorDetail::Message(_) | FactoryErrorDetail::Source { .. } => None,
        }
    }

    /// The failure message.
    #[must_use]
    pub fn message(&self) -> &str {
        match &self.detail {
            FactoryErrorDetail::Message(message) | FactoryErrorDetail::Source { message, .. } => {
                message
            }
            FactoryErrorDetail::Cancelled(reason) => reason.as_str(),
        }
    }
}

impl std::fmt::Display for FactoryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message())
    }
}

impl std::error::Error for FactoryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.detail {
            FactoryErrorDetail::Source { source, .. } => Some(source.as_ref()),
            FactoryErrorDetail::Message(_) | FactoryErrorDetail::Cancelled(_) => None,
        }
    }
}

fn into_nonblank(message: String) -> String {
    if message.trim().is_empty() {
        // Mirrors FusionCache's default factory-failure message.
        "an error occurred while running the factory".to_owned()
    } else {
        message
    }
}

impl From<FactoryError> for Error {
    fn from(err: FactoryError) -> Self {
        match &err.detail {
            FactoryErrorDetail::Message(message) => Self::Factory {
                message: message.clone(),
            },
            FactoryErrorDetail::Source { message, .. } => Self::FactoryWithSource {
                message: message.clone(),
                source: err,
            },
            FactoryErrorDetail::Cancelled(reason) => Self::FactoryCancelled { reason: *reason },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blank_factory_message_is_defaulted() {
        assert_eq!(
            FactoryError::new("   ").message(),
            "an error occurred while running the factory"
        );
    }

    #[test]
    fn factory_error_preserves_source() {
        let io = std::io::Error::other("boom");
        let err = FactoryError::from_source(io);
        assert_eq!(err.message(), "boom");
        assert!(std::error::Error::source(&err).is_some());
    }
}
