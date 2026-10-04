//! `amalgam` — an async hybrid cache for Rust, inspired by .NET
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

#![forbid(unsafe_code)]
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
pub mod maybe;
pub mod memory;
pub mod observability;
pub mod options;
pub mod plugins;
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
    BackplaneReadiness, Cache, CacheBuilder, ClearMode, CloseOutcome, LeasePolicy,
    ReconciliationPolicy, ShutdownReport,
};
pub use circuit::{CircuitBreaker, CircuitCheck};
pub use commit::{
    CacheValue, CommitCompletion, CommitReceipt, CommitReport, EffectBatch, EffectOutcome,
    LocalEffect, MutationReceipt, SkipReason,
};
pub use distributed::{
    DistributedCache, DistributedEntry, DistributedSerializer, DistributedSnapshot,
    InMemoryDistributedCache, InMemoryInvalidationStore, InvalidationStore, JsonSerializer,
    LeasedMutation, LeasedWriteOutcome, SnapshotRetention,
};
pub use distributed_lock::{
    AcquisitionPolicy, DistributedLease, DistributedLocker, InMemoryDistributedLocker, LeaseError,
    LeaseProof, LeaseReceipt, LeaseState, LeaseSupport, LeaseTask, LeaseTaskOwner, LeaseToken,
    LeaseTtl, OwnershipCheck, RenewalOutcome, TokenAcquisition, acquire_owned,
    acquire_owned_supervised,
};
pub use error::{
    CloneError, ConfigError, Error, FactoryCancellationReason, FactoryError, IdentityField, Result,
    RuntimeComponent, ShutdownError, ShutdownFailure, ShutdownTask,
};
pub use events::{
    CacheEvent, CacheLevel, CacheOperation, CircuitComponent, EventEmission, EventStreamClosed,
    EventSubscription, Events, OperationOutcome,
};
pub use execution::{CancellationRequest, CancellationSource, FactoryCancellation};
pub use factory::{FactoryContext, FactoryProduct, ModifiedBuilder};
pub use maybe::MaybeValue;
pub use memory::{
    CapacityRejection, MemoryAdmission, MemoryExpiry, MemoryLimits, MemoryStore, MemoryUsage,
};
pub use options::{
    EagerThreshold, EntryOptions, EntryWeight, JitterSample, JitterSource, KeyModifierMode,
    Priority, RandomJitterSource, RemoveByTagBehavior,
};
pub use plugins::{
    Plugin, PluginContext, PluginError, PluginHost, PluginRegistration, PluginSession, PluginStage,
    PluginStopOutcome,
};
pub use recovery::{
    AutoRecoveryService, DataMutation, EnqueueOutcome, MarkerReplay, OperationGeneration,
    PendingMutation, RecoveryAction, RecoveryConfig, RecoveryError, RecoveryExecutor,
    RecoveryFence, RecoveryId, RecoveryItem, RecoveryStageTransition, RecoveryStart, RecoveryWork,
    ReplayOutcome, ReplayTicket, SupersedeOutcome,
};
pub use registry::{CacheRegistry, DefaultEntryOptionsProvider, RegistryError};
pub use serializers::ValueCloner;
pub use tags::{
    CacheScope, MarkerAdvanceOutcome, MarkerError, MarkerKind, MarkerStoreLimits, MarkerVersion,
    StoredMarker, Tag, TagError,
};
pub use time::{Clock, ClockTiming, ManualClock, SystemClock, Timeout, Timestamp};

#[cfg(feature = "messagepack")]
pub use serializers::MessagePackSerializer;

#[cfg(feature = "postcard")]
pub use serializers::PostcardSerializer;

#[cfg(feature = "metrics")]
pub use observability::{CacheLabelBudget, MetricsPlugin};

#[cfg(feature = "opentelemetry")]
pub use otel::{OtelGuard, OtelInitError, init_otlp, otlp_layer, try_init_otlp};

#[cfg(feature = "redis")]
pub use redis_backend::{
    RedisBackplane, RedisBackplaneStats, RedisClientId, RedisDistributedCache,
    RedisDistributedLocker, RedisInvalidationStore, RedisIoOptions,
};
