//! `amalgam` — a hybrid cache with async and native sync APIs for Rust, inspired by .NET
//! [FusionCache](https://github.com/ZiggyCreatures/FusionCache).
//!
//! Local caching combines optional distributed storage, fail-safe retention,
//! controlled origin work and observable mutations. Same-key ownership prevents
//! ordinary stampedes; an explicitly finite lock wait can permit best-effort
//! origin work. Different keys have independent flights.
//!
//! New callers should use [`CacheBuilder::try_build`], fallible [`Cache::read`],
//! typed mutation receipts and [`Cache::shutdown`]. Ordinary origin failure can
//! activate an eligible stale fallback; cancellation remains a typed error.
//! Soft-timeout continuation and eager refresh have explicit ownership, while
//! distributed effects retain their actual completion and recovery stage.
//!
//! Stores, codecs, backplanes, plugins and value-copy strategies are extensible
//! traits. Durable invalidation and strict stale-owner rejection require their
//! corresponding atomic provider capabilities. `Clone` alone does not promise
//! isolation of shared mutable state.
//!
//! The 0.3 source version uses a v2 distributed namespace. New codecs accept
//! legacy payloads; older running readers need a coordinated fresh namespace.
//! See the packaged README, `docs/PARITY.md`, `docs/AUDIT.md` and `PORTING.md`
//! for supported contracts, migration and verification limits.

#![deny(unsafe_code)]
#![warn(missing_docs)]

pub mod backplane;
pub mod cache;
pub mod circuit;
pub mod commit;
pub mod distributed;
pub mod distributed_lock;
pub mod entry;
pub mod error;
pub mod events;
mod execution;
pub mod factory;
mod lifecycle;
pub mod locking;
mod marker_leases;
pub mod marker_reads;
pub mod marker_snapshots;
pub mod maybe;
pub mod memory;
pub mod memory_locker;
pub mod memory_storage;
pub mod observability;
pub mod options;
pub mod plugins;
mod single_flight;
// Owner-approved private performance boundary. Cache logic keeps the unsafe ban.
#[allow(unsafe_code)]
mod reader_slots;
pub mod recovery;
pub mod registry;
pub mod serializers;
pub mod tags;
pub mod time;

#[cfg(feature = "opentelemetry")]
pub mod otel;

#[cfg(feature = "redis")]
pub mod redis_backend;

pub use backplane::{
    Backplane, BackplaneAction, BackplaneCommand, BackplaneMessage, BackplaneState,
    ContinuityEpoch, InProcessBackplane, MarkerCommand,
};
pub use cache::{
    BackplaneReadiness, BlockingCache, BlockingCacheBuildError, BlockingCacheValue,
    BlockingCommitCompletion, BlockingCommitReceipt, BlockingDispatchError,
    BlockingMutationReceipt, BlockingRuntime, BlockingRuntimeError, BlockingThreadPool, Cache,
    CacheBuilder, CachePlugin, CachePluginContext, ClearMode, CloseOutcome,
    DistributedExpirePolicy, LeasePolicy, PluginCache, ReconciliationPolicy, ShutdownReport,
};
pub use circuit::{CircuitBreaker, CircuitCheck};
pub use commit::{
    CacheValue, CommitCompletion, CommitReceipt, CommitReport, EffectBatch, EffectOutcome,
    LocalEffect, MutationReceipt, SkipReason,
};
pub use distributed::{
    AsyncDistributedSerializer, DistributedCache, DistributedEntry, DistributedSerializer,
    DistributedSnapshot, FencedWriteSupport, InMemoryDistributedCache, InMemoryInvalidationStore,
    InvalidationStore, JsonSerializer, LeasedMutation, LeasedWriteOutcome, MarkerReadError,
    SerializationMode, SnapshotRetention,
};
pub use distributed_lock::{
    AcquisitionPolicy, DistributedLease, DistributedLocker, InMemoryDistributedLocker, LeaseError,
    LeaseProof, LeaseReceipt, LeaseState, LeaseSupport, LeaseTask, LeaseTaskOwner, LeaseToken,
    LeaseTtl, OwnershipCheck, RenewalOutcome, TokenAcquisition, acquire_owned,
    acquire_owned_supervised,
};
pub use error::{
    CloneError, CodecError, ConfigError, DrainOperation, Error, FactoryCancellationReason,
    FactoryError, IdentityField, Result, RuntimeComponent, ShutdownError, ShutdownFailure,
    ShutdownTask, TransportError,
};
pub use events::{
    BackplaneEvent, CacheEvent, CacheLevel, CacheOperation, CircuitComponent, ComponentRead,
    DistributedEvent, EventEmission, EventStreamClosed, EventSubscription, Events, LayerEvent,
    LayerEventSubscription, MemoryEvent, OperationOutcome,
};
pub use execution::{CancellationRequest, CancellationSource, FactoryCancellation};
pub use factory::{
    ConditionalRefreshError, FactoryContext, FactoryInvocation, FactoryProduct, ModifiedBuilder,
    NotModifiedBuilder, ValidatorUpdate,
};
pub use marker_reads::{
    MarkerObservation, MarkerPresence, MarkerReadFailure, MarkerReadOutcome, MarkerReadPolicy,
};
pub use marker_snapshots::{
    MarkerLifecyclePolicy, MarkerSnapshot, MarkerSnapshotCache, MarkerSnapshotCacheError,
    MarkerSnapshotLimits, MarkerSnapshotRead, MarkerSnapshotRenewal, MarkerSnapshotValidationError,
    MarkerSnapshotWriteOutcome,
};
pub use maybe::MaybeValue;
pub use memory::{
    CapacityRejection, MemoryAdmission, MemoryExpiry, MemoryLimits, MemoryStore, MemoryUsage,
};
pub use memory_locker::{
    BlockingMemoryLocker, MemoryLock, MemoryLockGuard, MemoryLockKind, MemoryLockOutcome,
    MemoryLockRequest, MemoryLocker, MemoryLockerContext, MemoryLockerError,
};
pub use memory_storage::{
    MemoryCondition, MemoryGeneration, MemoryInvalidationFailure, MemoryNamespace,
    MemoryNamespacePurpose, MemoryRecord, MemoryRecordViolation, MemoryRetirement, MemoryStorage,
    MemoryStorageEpoch, MemoryStorageError, MemoryStorageWrite,
};
pub use options::{
    EagerThreshold, EntryOptions, EntryWeight, JitterSample, JitterSource, KeyModifierMode,
    Priority, RandomJitterSource, RemoveByTagBehavior,
};
pub use plugins::{
    Plugin, PluginContext, PluginError, PluginHost, PluginObservations, PluginRegistration,
    PluginSession, PluginStage, PluginStopOutcome,
};
pub use recovery::{
    AutoRecoveryService, DataMutation, EnqueueOutcome, MarkerMutationRecovery, MarkerMutationStage,
    MarkerReplay, MarkerSnapshotParticipation, MarkerSnapshotReplay, OperationGeneration,
    PendingMutation, RecoveryAction, RecoveryConfig, RecoveryError, RecoveryExecutor,
    RecoveryFence, RecoveryId, RecoveryItem, RecoveryStageTransition, RecoveryStart, RecoveryWork,
    ReplayOutcome, ReplayTicket, SupersedeOutcome,
};
pub use registry::{CacheRegistry, DefaultEntryOptionsProvider, RegistryError};
pub use serializers::{ImmutableValue, ValueCloner};
pub use tags::{
    CacheScope, MarkerAdvanceOutcome, MarkerError, MarkerKind, MarkerStoreLimits, MarkerVersion,
    StoredMarker, Tag, TagError,
};
pub use time::{Clock, ClockTiming, ManualClock, SystemClock, Timeout, Timestamp};

#[cfg(feature = "messagepack")]
pub use serializers::MessagePackSerializer;

#[cfg(feature = "postcard")]
pub use serializers::PostcardSerializer;

#[cfg(any(feature = "metrics", feature = "opentelemetry"))]
pub use observability::CacheLabelBudget;
#[cfg(feature = "metrics")]
pub use observability::MetricsPlugin;
#[cfg(feature = "opentelemetry")]
pub use observability::{MetricTags, OtelMetricMeters, OtelMetricsPlugin};

#[cfg(feature = "opentelemetry")]
pub use otel::{
    OtelGuard, OtelInitError, init_otlp, otlp_layer, otlp_meter_provider, try_init_otlp,
};

#[cfg(feature = "redis")]
pub use redis_backend::{
    RedisBackplane, RedisBackplaneStats, RedisClientId, RedisDistributedCache,
    RedisDistributedLocker, RedisInvalidationStore, RedisIoOptions,
};

/// Typed original-value memory observations.
pub use events::{
    EvictionCapture, EvictionReceiveError, MemoryEviction, MemoryEvictionReason,
    MemoryEvictionSubscription, MemoryEvictions,
};
