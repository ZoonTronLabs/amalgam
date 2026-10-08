//! Explicit completion evidence, stronger policies and detailed observations.
//!
//! Receipts, fencing/reconciliation, marker snapshots, recovery inspection and
//! low-level event payloads extend ordinary cache operations. Enabling one keeps
//! its existing contract; importing these types does not enable a runtime feature.
pub use crate::cache::{
    BlockingCacheValue, BlockingCommitCompletion, BlockingCommitReceipt, BlockingDispatchError,
    BlockingMutationReceipt, BlockingRuntime, BlockingRuntimeError, BlockingThreadPool,
    CachePlugin, CachePluginContext, DistributedExpirePolicy, LeasePolicy, PluginCache,
    ReconciliationPolicy,
};
pub use crate::circuit::{CircuitBreaker, CircuitCheck};
pub use crate::commit::{
    CacheValue, CommitCompletion, CommitReceipt, CommitReport, EffectBatch, EffectOutcome,
    LocalEffect, MutationReceipt, SkipReason,
};
pub use crate::error::{
    DrainOperation, IdentityField, RuntimeComponent, ShutdownFailure, ShutdownTask,
};
pub use crate::events::{
    BackplaneEvent, CircuitComponent, ComponentRead, DistributedEvent, EventEmission, LayerEvent,
    LayerEventSubscription, MemoryEvent,
};
/// Typed original-value memory observations.
pub use crate::events::{
    EvictionCapture, EvictionReceiveError, MemoryEviction, MemoryEvictionReason,
    MemoryEvictionSubscription, MemoryEvictions,
};
pub use crate::execution::CancellationRequest;
pub use crate::factory::{
    FactoryInvocation, FactoryOptionsMut, ModifiedBuilder, NotModifiedBuilder, ValidatorUpdate,
};
pub use crate::marker_reads::{
    MarkerObservation, MarkerPresence, MarkerReadFailure, MarkerReadOutcome, MarkerReadPolicy,
};
pub use crate::marker_snapshots::{
    MarkerLifecyclePolicy, MarkerSnapshot, MarkerSnapshotCache, MarkerSnapshotCacheError,
    MarkerSnapshotLimits, MarkerSnapshotRead, MarkerSnapshotRenewal, MarkerSnapshotValidationError,
    MarkerSnapshotWriteOutcome,
};
#[cfg(any(feature = "metrics", feature = "opentelemetry"))]
pub use crate::observability::CacheLabelBudget;
#[cfg(feature = "opentelemetry")]
pub use crate::observability::{MetricTags, OtelMetricMeters};
pub use crate::options::KeyModifierMode;
#[cfg(feature = "opentelemetry")]
pub use crate::otel::{
    OtelGuard, OtelInitError, init_otlp, otlp_layer, otlp_meter_provider, try_init_otlp,
};
pub use crate::plugins::{PluginHost, PluginObservations};
pub use crate::recovery::{
    AutoRecoveryService, DataMutation, EnqueueOutcome, MarkerMutationRecovery, MarkerMutationStage,
    MarkerReplay, MarkerSnapshotParticipation, MarkerSnapshotReplay, OperationGeneration,
    PendingMutation, RecoveryAction, RecoveryError, RecoveryFence, RecoveryId, RecoveryItem,
    RecoveryStageTransition, RecoveryStart, RecoveryWork, ReplayOutcome, ReplayTicket,
    SupersedeOutcome,
};
pub use crate::tags::{
    CacheScope, MarkerAdvanceOutcome, MarkerError, MarkerKind, MarkerStoreLimits, MarkerVersion,
    StoredMarker,
};
