//! Multi-level orchestration with fallible boundaries and owned work.
mod advice;
mod api;
pub(crate) mod blocking;
pub(crate) use blocking::MemoryAcquireRoute;
use blocking::{NativeMemoryView, NativeMemoryWork};
mod builder;
mod clock;
mod distributed_key;
mod get_request;
mod immediate_read;
mod inline_cold;
pub use get_request::{GetOrSetFuture, GetOrSetRequest, ReceiptGetOrSetRequest};
mod invalidation_request;
mod markers;
mod ordinary_markers;
use ordinary_markers::{MarkerReadStorage, MarkerWriteOutcome, OrdinaryMarkers};
mod memory_inline;
mod mutation_request;
pub use invalidation_request::{
    ClearRequest, ExpireRequest, InvalidationFuture, ReceiptInvalidationFuture,
    ReceiptInvalidationRequest, RemoveRequest, TagInvalidationRequest,
};
mod set_request;
pub use set_request::{ReceiptSetFuture, ReceiptSetRequest, SetFuture, SetRequest};
mod callback_free;
mod observed_execution;
pub(crate) mod origin;
mod plain_ready;
mod plugin;
mod read;
mod read_request;
pub use read_request::{
    BlockingTryGetRequest, GetOrDefaultFuture, GetOrDefaultRequest, TryGetFuture, TryGetRequest,
};
mod ready;
mod recovery;
mod runtime;
mod write;

pub use blocking::{
    BlockingCache, BlockingCacheBuildError, BlockingCacheValue, BlockingCommitCompletion,
    BlockingCommitReceipt, BlockingDispatchError, BlockingGetOrSetRequest, BlockingMutationReceipt,
    BlockingReceiptGetOrSetRequest, BlockingReceiptRequest, BlockingRequest, BlockingRuntime,
    BlockingRuntimeError, BlockingThreadPool,
};
pub use builder::CacheBuilder;
use origin::{CacheOrigin, FactoryOrigin, OriginCompletion, OriginKind};
pub use plugin::{CachePlugin, CachePluginContext, PluginCache};
use plugin::{InitialPlugin, PluginAccess, WorkAdmission};

use crate::backplane::{
    Backplane, BackplaneAction, BackplaneCommand, BackplaneMessage, BackplaneState, MarkerCommand,
};
use crate::circuit::{CircuitBreaker, CircuitCheck};
use crate::commit::{
    CacheValue, CommitCompletion, CommitReceipt, CommitReport, EffectOutcome, Fence, Lanes,
    LocalEffect, MutationReceipt, SkipReason,
};
use crate::distributed::{
    DistributedCache, DistributedSerializer, DistributedSnapshot, InvalidationStore,
    LeasedMutation, LeasedWriteOutcome,
};
use crate::distributed_lock::{
    AcquisitionPolicy, DistributedLease, DistributedLocker, LeaseError, LeaseState, LeaseTask,
    LeaseTaskOwner, LeaseTtl, acquire_owned_supervised,
};
use crate::entry::{ContinuityStamp, Entry};
use crate::error::{
    ConfigError, Error, FactoryCancellationReason as Reason, FactoryError, IdentityField, Result,
    RuntimeComponent, ShutdownError, ShutdownFailure, ShutdownTask,
};
use crate::events::{
    BackplaneEvent, CacheEvent, CacheLevel, CacheOperation, CircuitComponent, DistributedEvent,
    Events, LayerEvent, MemoryEvent, OperationOutcome,
};
use crate::execution::{
    CancellationSource, Execution, ExecutionCheckpoint, FactoryCancellation, InlinePermit,
    LinkMode, Scopes, lock,
};
use crate::factory::{FactoryContext, FactoryProduct, ProductOrigin, StaleInfo};
use crate::lifecycle::Tasks;
use crate::marker_leases::{MarkerLease, MarkerLeaseKey};
use crate::marker_reads::{
    MarkerLifecycleAccess, MarkerObservation, MarkerObservations, MarkerPresence,
    MarkerReadFailure, MarkerReadOutcome, MarkerReadPolicy, MarkerReads,
};
use crate::marker_snapshots::{
    MarkerLifecyclePolicy, MarkerSnapshot, MarkerSnapshotCache, MarkerSnapshotRead,
    MarkerSnapshotRenewal,
};
use crate::memory::{CacheMemory, MemoryAdmission, MemoryExpiry, MemoryLimits};
use crate::memory_locker::{LocalGuard, LocalLocks};
use crate::observability::{
    OperationObservation, QuietObservation, ReadyObservation, component_span,
};
use crate::options::{
    EntryOptions, JitterSample, JitterSource, KeyModifierMode, RemoveByTagBehavior,
};
use crate::plugins::{Plugin, PluginContext, PluginHost};
use crate::recovery::{
    AutoRecoveryService, DataMutation, EnqueueOutcome, MarkerReplay, PendingMutation,
    RecoveryAction, RecoveryConfig, RecoveryExecutor, RecoveryFence, RecoveryItem, RecoveryWork,
    ReplayOutcome, ReplayTicket,
};
use crate::registry::DefaultEntryOptionsProvider;
use crate::serializers::ValueCloner;
use crate::tags::{
    CacheScope, MarkerAdvanceOutcome, MarkerError, MarkerKind, MarkerVersion, StoredMarker, Tag,
    TagRegistry, TagVerdict,
};
use crate::time::{Clock, ClockTiming, SystemClock, Timeout, Timestamp};
use async_trait::async_trait;
use std::borrow::Cow;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, watch};
use tracing::Instrument;

/// Cache-wide invalidation mode, replacing the legacy boolean parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClearMode {
    /// Keep physically live values for fail-safe.
    Expire,
    /// Remove cached values.
    Remove,
}

/// The distributed effect of logically expiring a key. L1 remains stale in both modes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DistributedExpirePolicy {
    /// Preserve the physically live L2 snapshot for fail-safe, explicitly.
    RetainStale,
    /// Physically remove L2 while expiring L1, matching FusionCache 2.9.
    #[default]
    Remove,
}
/// Explicit cluster-lock compatibility contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeasePolicy {
    /// Require owned acquisition and atomic backend fencing.
    Fenced,
    /// Cooperative ownership, as in FusionCache; backend errors follow rethrow options.
    /// Does not promise partition-safe writes. This is the builder default.
    Cooperative,
}
/// Durable reconciliation contract when notification continuity is unavailable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconciliationPolicy {
    /// No external storage or notifications exist; all invalidation is local.
    LocalOnly,
    /// Retain L1 until its normal expiration, without periodic reconciliation.
    /// External values may change without notifications; explicit invalidations
    /// and normal tag/clear checks still apply.
    Expiration,
    /// Periodically discard L1 so subsequent reads reconcile durable markers.
    Periodic(Duration),
    /// Trust an acknowledged continuous native backplane; gaps still discard L1.
    BackplaneContinuity,
    /// Retain L1 over notification gaps, as in FusionCache's best-effort backplane.
    ///
    /// Requires a backplane, with or without a health stream. Received
    /// invalidations still apply; missed invalidations can leave an old value
    /// readable until its normal expiration. No periodic L1 discard is added.
    /// Lease admission remains governed separately by [`LeasePolicy`].
    BackplaneBestEffort,
}
impl ReconciliationPolicy {
    fn invalidates_on_gap(self) -> bool {
        match self {
            Self::LocalOnly | Self::Periodic(_) | Self::BackplaneContinuity => true,
            Self::Expiration | Self::BackplaneBestEffort => false,
        }
    }
}
/// Result of requesting close.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseOutcome {
    /// This request initiated close.
    Started,
    /// Close is already draining.
    AlreadyClosing,
    /// Shutdown previously completed.
    AlreadyClosed,
}
/// Completed owning-cache drainage. Repeated shutdown returns the same report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShutdownReport;
/// Subscription admission without inventing acknowledgements for legacy adapters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackplaneReadiness {
    /// No notification provider is configured.
    NotConfigured,
    /// Native provider acknowledged the current subscription epoch.
    Acknowledged(crate::provider::ContinuityEpoch),
    /// Legacy provider has no health/ACK facet; periodic reconciliation applies.
    BestEffort,
}

/// A typed value cache. Public clones share lifecycle; workers never own public handles.
///
/// Ordinary `V::clone()` on a built-in L1 hit runs under its short reader slot.
/// A value's `Clone` implementation must not reenter the same cache. Custom
/// [`ValueCloner`] callbacks, factories, event handlers and value destructors
/// execute outside storage locks; configure auto-clone for a custom cloner.
pub struct Cache<V: Clone + Send + Sync + 'static> {
    inner: Arc<CacheInner<V>>,
    lifetime: Arc<PublicLifetime<V>>,
}
enum ExecutorOwnership {
    External,
    CacheOwned(BlockingRuntime),
}
enum PublicLifetime<V: Clone + Send + Sync + 'static> {
    NativeMemory(NativeMemoryView<V>),
    PluginAccess(Arc<PluginAccess>),
    External(Arc<CacheInner<V>>),
    CacheOwned {
        inner: Arc<CacheInner<V>>,
        executor: BlockingRuntime,
    },
}
impl<V: Clone + Send + Sync + 'static> Drop for PublicLifetime<V> {
    fn drop(&mut self) {
        match self {
            Self::PluginAccess(_) | Self::NativeMemory(_) => {}
            Self::External(inner) => {
                inner.close();
            }
            Self::CacheOwned { inner, executor } => {
                if inner.close() == CloseOutcome::AlreadyClosed {
                    return;
                }
                let inner = Arc::clone(inner);
                let retained = executor.clone();
                executor.handle().spawn(async move {
                    if let Err(error) = inner.shutdown().await {
                        tracing::warn!(%error, "cache-owned executor drainage failed");
                    }
                    drop(retained);
                });
            }
        }
    }
}
impl<V: Clone + Send + Sync + 'static> Clone for Cache<V> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            lifetime: Arc::clone(&self.lifetime),
        }
    }
}
// Capture factory ownership without allocating an unused retirement collector.
struct WorkerSeed<V: Clone + Send + Sync + 'static> {
    inner: Arc<CacheInner<V>>,
    admission: WorkAdmission,
}
impl<V: Clone + Send + Sync + 'static> WorkerSeed<V> {
    fn worker(self) -> Worker<V> {
        Worker {
            memory: self.inner.memory.for_operation(),
            inner: self.inner,
            admission: self.admission,
        }
    }
}

struct Worker<V: Clone + Send + Sync + 'static> {
    inner: Arc<CacheInner<V>>,
    admission: WorkAdmission,
    memory: CacheMemory<V>,
}
impl<V: Clone + Send + Sync + 'static> Clone for Worker<V> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            admission: self.admission.clone(),
            memory: self.memory.clone(),
        }
    }
}
enum Storage<V> {
    MemoryOnly,
    Hybrid {
        backend: Arc<dyn DistributedCache>,
        serializer: crate::distributed::Serializer<V>,
    },
}
enum MarkerAccess {
    Local,
    Durable(Arc<dyn InvalidationStore>),
    Ordinary(Arc<OrdinaryMarkers>),
    Unavailable,
}
enum Lifecycle {
    Running,
    Closing,
    Closed(std::result::Result<ShutdownReport, ShutdownError>),
}
struct CacheInner<V: Clone + Send + Sync + 'static> {
    owner: Weak<CacheInner<V>>,
    name: Arc<str>,
    instance_id: Arc<str>,
    memory: CacheMemory<V>,
    locks: LocalLocks,
    lanes: Lanes,
    tags: TagRegistry,
    events: Events,
    clock: crate::time::local::CacheClock,
    default_options: EntryOptions,
    default_runtime: ready::RuntimeRequirement,
    ready_plan: plain_ready::ReadyPlan,
    distributed_read_plan: immediate_read::DistributedReadPlan,
    marker_prefetch: markers::MarkerPrefetch,
    write_plan: memory_inline::WritePlan,
    default_fresh_plan: crate::entry::DefaultFreshPlan,
    default_copy: crate::serializers::DefaultValueCopy<V>,
    flights: Option<Arc<crate::single_flight::Flights<inline_cold::Value<V>>>>,
    origin_work: std::sync::OnceLock<crate::retained_origin::RetainedOrigins<OriginCompletion<V>>>,
    tags_default_options: EntryOptions,
    marker_reads: MarkerReads,
    key_prefix: Option<Arc<str>>,
    remove_by_tag_behavior: RemoveByTagBehavior,
    storage: Storage<V>,
    serialization_mode: crate::distributed::SerializationMode,
    markers: MarkerAccess,
    scope: CacheScope,
    backplane: Option<Arc<dyn Backplane>>,
    distributed_locker: Option<Arc<dyn DistributedLocker>>,
    lease_policy: LeasePolicy,
    lease_ttl: LeaseTtl,
    circuit_l2: CircuitBreaker,
    circuit_backplane: CircuitBreaker,
    plugins: PluginHost,
    recovery: Option<Arc<AutoRecoveryService>>,
    default_options_provider: Option<Arc<dyn DefaultEntryOptionsProvider>>,
    ignore_incoming_backplane: bool,
    distributed_key: distributed_key::DistributedKey,
    disable_tagging: bool,
    wait_for_initial_backplane_subscribe: bool,
    cloner: Option<Arc<dyn ValueCloner<V>>>,
    jitter: crate::options::JitterPlan,
    scopes: Arc<Scopes>,
    tasks: Arc<Tasks>,
    epoch: Arc<AtomicU64>,
    maintenance: AtomicBool,
    subscription_admitted: AtomicBool,
    reconciliation: ReconciliationPolicy,
    lifecycle: std::sync::Mutex<Lifecycle>,
    shutdown_gate: tokio::sync::Mutex<()>,
    marker_lane: Arc<tokio::sync::Mutex<()>>,
    health_seen: std::sync::Mutex<Option<BackplaneState>>,
    last_reconcile: std::sync::Mutex<Instant>,
}
struct Observed<T> {
    value: T,
    outcome: OperationOutcome,
    level: Option<CacheLevel>,
}
impl<T> Observed<T> {
    fn new(value: T, outcome: OperationOutcome, level: Option<CacheLevel>) -> Self {
        Self {
            value,
            outcome,
            level,
        }
    }
}
enum L1Read<V> {
    Fresh(Entry<V>),
    Stale(Entry<V>),
    Miss,
}
enum HydrationFence<V> {
    Stable {
        fence: Fence,
        observed: Option<Entry<V>>,
    },
    ConcurrentMutation,
}
struct DistributedLookup<V> {
    entry: Entry<V>,
    hydration: HydrationFence<V>,
}
enum HydrationOutcome {
    Evaluated(MemoryAdmission),
    Skipped(SkipReason),
}
impl HydrationOutcome {
    fn observe(self) {
        match self {
            Self::Evaluated(admission) => {
                tracing::trace!(?admission, "distributed hydration memory admission")
            }
            Self::Skipped(reason) => tracing::trace!(?reason, "distributed hydration skipped"),
        }
    }
}
enum ReadStale<V> {
    Memory(Entry<V>),
    Distributed(Entry<V>),
}
impl<V> ReadStale<V> {
    fn entry(&self) -> &Entry<V> {
        match self {
            Self::Memory(entry) | Self::Distributed(entry) => entry,
        }
    }
    fn into_parts(self) -> (Entry<V>, CacheLevel) {
        match self {
            Self::Memory(entry) => (entry, CacheLevel::Memory),
            Self::Distributed(entry) => (entry, CacheLevel::Distributed),
        }
    }
}
#[derive(Clone, Copy)]
enum FallbackAvailability {
    Available,
    Unavailable,
}
#[derive(Clone, Copy)]
enum OptionsTarget {
    Value,
    Marker,
}
struct L2ReadPolicy;
impl L2ReadPolicy {
    fn rethrow(options: &EntryOptions, error: &Error) -> bool {
        match error {
            Error::Serialization(_) | Error::Deserialization(_) | Error::Codec(_) => {
                options.rethrow_serialization_exceptions()
            }
            Error::Config(_)
            | Error::Clone(_)
            | Error::Tag(_)
            | Error::OperationCancelled { .. }
            | Error::FactoryCancelled { .. } => true,
            _ => options.rethrow_distributed_exceptions(),
        }
    }
}
#[derive(Clone, Copy)]
enum HitKind {
    Fresh,
    Stale,
}
impl HitKind {
    fn is_stale(self) -> bool {
        match self {
            Self::Fresh => false,
            Self::Stale => true,
        }
    }
    fn outcome(self) -> OperationOutcome {
        match self {
            Self::Fresh => OperationOutcome::Hit,
            Self::Stale => OperationOutcome::StaleHit,
        }
    }
}
#[derive(Clone, Copy)]
enum CommitMode {
    Foreground,
    Background,
}

#[derive(Clone, Copy)]
enum LookupMode {
    Read,
    GetOrSet,
    ConstantValue,
}
struct ReadyValue<'key, V> {
    value: V,
    key: Cow<'key, str>,
    refresh: ReadyRefresh<V>,
}
enum ReadyRefresh<V> {
    Complete,
    Eager(Box<ReadyEager<V>>),
}
struct ReadyEager<V> {
    current: Entry<V>,
    options: EntryOptions,
}
struct ReadyHit<V> {
    value: V,
    refresh: ReadyRefresh<V>,
}
type LookupKey = crate::factory::FactoryKeys;
// Ready operations cannot park. Their counted permit covers all synchronous
// user code, including unused factory/fallback destructors and event callbacks.
struct ReadyLookup<'a, V> {
    result: Result<ReadyValue<'a, V>>,
    observation: ReadyObservation<'a>,
    permit: InlinePermit<'a>,
}
enum LookupStart<'a, V> {
    Ready(ReadyLookup<'a, V>),
    Owned {
        observation: ReadyObservation<'a>,
        key: Cow<'a, str>,
        permit: InlinePermit<'a>,
        resolved: Option<Box<EntryOptions>>,
    },
}
enum ObservationAdmission<'a> {
    New,
    Inline(InlinePermit<'a>),
}
impl<V> ReadyLookup<'_, V> {
    fn finish<T>(
        self,
        token: Option<&FactoryCancellation>,
        events: &Events,
        complete: impl FnOnce(V) -> T,
    ) -> Result<T> {
        let permit = self.permit;
        let mut observation = self.observation;
        let span = observation.span();
        let _entered = span.enter();
        // Completion owns any default input, including its destructor on a hit
        // or an error. No caller input survives outside the counted boundary.
        let result = match self.result {
            Ok(ready) => {
                // Optional background handoff is consumed by factory lookups;
                // any unused ownership is reclaimed while the permit is live.
                drop(ready.refresh);
                Ok(ReadyValue {
                    value: complete(ready.value),
                    key: ready.key,
                    refresh: ReadyRefresh::Complete,
                })
            }
            Err(error) => {
                drop(complete);
                Err(error)
            }
        };
        let result = match result {
            Err(Error::CacheClosed) => Err(Error::CacheClosed),
            result => permit.status(token).and(result),
        }
        .and_then(|ready| {
            events.emit_lazy(|| CacheEvent::Hit {
                key: Arc::from(ready.key.as_ref()),
                stale: false,
            });
            permit.status(token)?;
            Ok(ready.value)
        });
        let outcome = match &result {
            Ok(_) => {
                observation.set_level(CacheLevel::Memory);
                OperationOutcome::Hit
            }
            Err(error) => OperationOutcome::from_error(error),
        };
        observation.finish(outcome);
        result
    }
}

#[derive(Clone, Copy)]
enum KeyMutation {
    Remove,
    Expire(DistributedExpirePolicy),
}
async fn drive<T: Send + 'static>(
    mut execution: Execution<T>,
    token: Option<FactoryCancellation>,
) -> Result<T> {
    if let Some(token) = token {
        execution.link(&token, LinkMode::Explicit);
        tokio::select! {biased; reason=token.cancelled()=>{execution.cancel(reason);execution.await},result=&mut execution=>result}
    } else {
        execution.await
    }
}
pub(crate) fn validate_budget(timeout: Timeout) -> Result<()> {
    if let Timeout::After(duration) = timeout
        && Instant::now().checked_add(duration).is_none()
    {
        return Err(ConfigError::DeadlineOutOfRange.into());
    }
    Ok(())
}
pub(crate) async fn bounded<T>(
    timeout: Timeout,
    work: impl Future<Output = T>,
) -> Result<Option<T>> {
    validate_budget(timeout)?;
    match timeout {
        Timeout::Infinite => Ok(Some(work.await)),
        Timeout::After(duration) if duration.is_zero() => Ok(None),
        Timeout::After(duration) => Ok(tokio::time::timeout(duration, work).await.ok()),
    }
}

// An origin carries the version observed before user factory creation/poll.
// Its active revision keeps identity stable across an intervening mutation.
struct OriginCommit<V> {
    guard: FlightGuard,
    started_at: OriginVersion<V>,
}
enum OriginVersion<V> {
    Memory(crate::memory::MemoryOrigin<V>),
    Ordered(Fence),
}
impl<V> OriginVersion<V> {
    fn is_current(&self) -> bool {
        match self {
            Self::Memory(version) => version.is_current(),
            Self::Ordered(version) => version.passive_is_current(),
        }
    }
}

struct FlightGuard {
    local: LocalParticipation,
    lease: ClusterParticipation,
    _reclamation: Option<Arc<dyn crate::memory::ReclamationFence>>,
}
enum LocalParticipation {
    Held(crate::memory::ReclamationGuard<LocalGuard>),
    UnlockedAfterTimeout,
    ReplayOnly,
}
impl LocalParticipation {
    fn release(self) {
        match self {
            Self::Held(guard) => drop(guard),
            Self::UnlockedAfterTimeout | Self::ReplayOnly => {}
        }
    }
}
struct CacheLeaseOwner {
    tasks: Arc<Tasks>,
    events: Events,
    key: Arc<str>,
}
impl LeaseTaskOwner for CacheLeaseOwner {
    fn supervise(&self, work: LeaseTask) {
        self.tasks
            .cleanup(Arc::clone(&self.key), self.events.clone(), async move {
                work.await.map_err(Error::from)
            });
    }
}
// Disabled distributed locking retains no cluster cleanup owners. Each leased
// variant owns both its exact participation policy and the resources needed to
// release it; an absent lease cannot carry unused task/event/key ownership.
enum ClusterParticipation {
    Local,
    Cooperative(ClusterLease),
    Fenced(ClusterLease),
}
struct ClusterLease {
    lease: DistributedLease,
    owner: CacheLeaseOwner,
}
impl ClusterParticipation {
    fn new(
        lease: Option<DistributedLease>,
        policy: LeasePolicy,
        owner: impl FnOnce() -> CacheLeaseOwner,
    ) -> Self {
        let Some(lease) = lease else {
            return Self::Local;
        };
        let lease = ClusterLease {
            lease,
            owner: owner(),
        };
        match policy {
            LeasePolicy::Cooperative => Self::Cooperative(lease),
            LeasePolicy::Fenced => Self::Fenced(lease),
        }
    }
    fn is_local(&self) -> bool {
        matches!(self, Self::Local)
    }
    fn is_fenced(&self) -> bool {
        matches!(self, Self::Fenced(_))
    }
    fn state(&self) -> Option<tokio::sync::watch::Receiver<LeaseState>> {
        match self {
            Self::Local => None,
            Self::Cooperative(owned) | Self::Fenced(owned) => Some(owned.lease.state()),
        }
    }
    fn proof(&self) -> Result<Option<crate::provider::LeaseProof>> {
        match self {
            Self::Local => Ok(None),
            Self::Fenced(owned) => Ok(Some(owned.lease.proof()?)),
            Self::Cooperative(owned) => {
                if *owned.lease.state().borrow() == LeaseState::Lost {
                    Err(LeaseError::Lost.into())
                } else {
                    Ok(None)
                }
            }
        }
    }
    fn release(self) {
        match self {
            Self::Local => {}
            Self::Cooperative(owned) | Self::Fenced(owned) => {
                owned
                    .owner
                    .tasks
                    .cleanup(owned.owner.key, owned.owner.events, async move {
                        owned.lease.release().await.map_err(Error::from)
                    });
            }
        }
    }
}
impl FlightGuard {
    fn proof(&self) -> Result<Option<crate::provider::LeaseProof>> {
        self.lease.proof()
    }
}
impl Drop for FlightGuard {
    fn drop(&mut self) {
        std::mem::replace(&mut self.lease, ClusterParticipation::Local).release();
        std::mem::replace(&mut self.local, LocalParticipation::ReplayOnly).release();
    }
}
enum LockOutcome<V> {
    Acquired(FlightGuard),
    UnlockedAfterTimeout(FlightGuard),
    Served(V),
}
impl<V: Clone + Send + Sync + 'static> Worker<V> {
    fn capture_origin(&self, key: &Arc<str>, guard: FlightGuard) -> Result<OriginCommit<V>> {
        let started_at = if self.inner.write_plan.is_inline() {
            let CacheMemory::Builtin(memory) = &self.memory else {
                unreachable!("inline memory plan requires builtin L1")
            };
            OriginVersion::Memory(memory.capture_origin(Arc::clone(key))?)
        } else {
            OriginVersion::Ordered(self.inner.lanes.capture(key, &self.inner.epoch))
        };
        Ok(OriginCommit { guard, started_at })
    }
    fn lease_cleanup_owner(&self, key: &Arc<str>) -> CacheLeaseOwner {
        CacheLeaseOwner {
            tasks: Arc::clone(&self.inner.tasks),
            events: self.inner.events.clone(),
            key: Arc::clone(key),
        }
    }
    fn lease_owner(&self, key: &Arc<str>) -> Arc<dyn LeaseTaskOwner> {
        Arc::new(self.lease_cleanup_owner(key))
    }
    fn cluster_participation(
        &self,
        key: &Arc<str>,
        lease: Option<DistributedLease>,
        policy: LeasePolicy,
    ) -> ClusterParticipation {
        ClusterParticipation::new(lease, policy, || self.lease_cleanup_owner(key))
    }
    fn flight_guard(
        &self,
        key: &Arc<str>,
        local: LocalParticipation,
        lease: Option<DistributedLease>,
        policy: LeasePolicy,
    ) -> FlightGuard {
        FlightGuard {
            local,
            lease: self.cluster_participation(key, lease, policy),
            _reclamation: self.memory.fence(),
        }
    }
    fn full_key(&self, key: &str) -> Arc<str> {
        match &self.inner.key_prefix {
            Some(prefix) => Arc::from(format!("{prefix}{key}")),
            None => Arc::from(key),
        }
    }
    fn resolve_options(&self, key: &str, options: Option<EntryOptions>) -> Result<EntryOptions> {
        let opts = options
            .or_else(|| {
                self.inner
                    .default_options_provider
                    .as_ref()
                    .and_then(|provider| {
                        provider.options_for_with_defaults(key, &self.inner.default_options)
                    })
            })
            .unwrap_or_else(|| self.inner.default_options.clone());
        self.validate_options(&opts)?;
        Ok(opts)
    }
    fn validate_options(&self, opts: &EntryOptions) -> Result<()> {
        self.inner.validate_options(opts)
    }
    fn validate_marker_options(&self, opts: &EntryOptions) -> Result<()> {
        self.inner.validate_marker_options(opts)
    }
    fn validate_execution_options(&self, opts: &EntryOptions, target: OptionsTarget) -> Result<()> {
        self.inner.validate_execution_options(opts, target)
    }
    fn copy(&self, value: &V, opts: &EntryOptions) -> Result<V> {
        self.inner.copy(value, opts)
    }
    fn stale_info(&self, entry: &Entry<V>, opts: &EntryOptions) -> Result<StaleInfo<V>> {
        Ok(StaleInfo {
            value: self.copy(entry.value(), opts)?,
            etag: entry.meta().etag().map(str::to_owned),
            last_modified: entry.meta().last_modified(),
            tags: entry.meta().tags().into(),
        })
    }
    fn fresh_entry(
        &self,
        value: V,
        opts: &EntryOptions,
        snapshot: Timestamp,
        tags: Box<[Tag]>,
        etag: Option<String>,
        last_modified: Option<Timestamp>,
    ) -> Result<Entry<V>> {
        let now = self.inner.clock.now();
        let sample = self.jitter_sample(opts)?;
        Entry::try_fresh_with_jitter(
            value,
            opts,
            snapshot,
            now,
            sample,
            tags,
            etag,
            last_modified,
        )
    }
    fn jitter_sample(&self, options: &EntryOptions) -> Result<JitterSample> {
        Ok(self.inner.jitter.sample(options.jitter_max())?)
    }
    fn emit(&self, event: CacheEvent) {
        if let CacheEvent::CircuitBreakerChange { component, closed } = &event {
            self.memory.emit_layer_lazy(|| match component {
                CircuitComponent::Distributed => {
                    LayerEvent::Distributed(DistributedEvent::CircuitBreakerChange {
                        closed: *closed,
                    })
                }
                CircuitComponent::Backplane => {
                    LayerEvent::Backplane(BackplaneEvent::CircuitBreakerChange { closed: *closed })
                }
            });
        }
        match &event {
            CacheEvent::SerializationError { key, .. } => self.memory.emit_layer_lazy(|| {
                LayerEvent::Distributed(DistributedEvent::SerializationError {
                    key: Arc::clone(key),
                })
            }),
            CacheEvent::DeserializationError { key, .. } => self.memory.emit_layer_lazy(|| {
                LayerEvent::Distributed(DistributedEvent::DeserializationError {
                    key: Arc::clone(key),
                })
            }),
            _ => {}
        }
        self.memory.emit(event);
    }
    fn tags(&self, entry: &Entry<V>) -> TagVerdict {
        self.inner.tags(entry)
    }
    fn circuit(&self, component: CircuitComponent) -> bool {
        let circuit = match component {
            CircuitComponent::Distributed => &self.inner.circuit_l2,
            CircuitComponent::Backplane => &self.inner.circuit_backplane,
        };
        if circuit.is_closed_without_clock() {
            return true;
        }
        match circuit.check(self.inner.clock.now()) {
            CircuitCheck::Open { .. } => false,
            CircuitCheck::Closed => true,
            CircuitCheck::ClosedAfterCooldown => {
                self.emit(CacheEvent::CircuitBreakerChange {
                    component,
                    closed: true,
                });
                true
            }
        }
    }
    fn close_circuit(&self, component: CircuitComponent) {
        let circuit = match component {
            CircuitComponent::Distributed => &self.inner.circuit_l2,
            CircuitComponent::Backplane => &self.inner.circuit_backplane,
        };
        if circuit.close() {
            self.emit(CacheEvent::CircuitBreakerChange {
                component,
                closed: true,
            });
        }
    }
    fn failure(&self, key: &Arc<str>, error: &Error) {
        match error {
            Error::Serialization(message) => self.emit(CacheEvent::SerializationError {
                key: Arc::clone(key),
                message: message.clone(),
            }),
            Error::Deserialization(message) => self.emit(CacheEvent::DeserializationError {
                key: Arc::clone(key),
                message: message.clone(),
            }),
            Error::Codec(error) => match error {
                crate::CodecError::Serialization { source } => {
                    self.emit(CacheEvent::SerializationError {
                        key: Arc::clone(key),
                        message: source.to_string(),
                    })
                }
                crate::CodecError::Deserialization { source } => {
                    self.emit(CacheEvent::DeserializationError {
                        key: Arc::clone(key),
                        message: source.to_string(),
                    })
                }
            },
            Error::Distributed(_)
            | Error::Lease(LeaseError::Backend { .. })
            | Error::Marker(MarkerError::Backend { .. }) => {
                self.trip_circuit(CircuitComponent::Distributed)
            }
            Error::Backplane(_) => self.trip_circuit(CircuitComponent::Backplane),
            Error::Transport(error) => match error {
                crate::TransportError::Distributed { .. } => {
                    self.trip_circuit(CircuitComponent::Distributed)
                }
                crate::TransportError::Backplane { .. } => {
                    self.trip_circuit(CircuitComponent::Backplane)
                }
            },
            _ => {}
        }
    }
    fn trip_circuit(&self, component: CircuitComponent) {
        let circuit = match component {
            CircuitComponent::Distributed => &self.inner.circuit_l2,
            CircuitComponent::Backplane => &self.inner.circuit_backplane,
        };
        if circuit.trip(self.inner.clock.now()) {
            self.emit(CacheEvent::CircuitBreakerChange {
                component,
                closed: false,
            });
        }
    }
}
enum PreparedData {
    Absent,
    Skipped,
    Ready(DataMutation),
    Failed(Error),
    ColdExpire {
        cause: Error,
        logical_expiration: Timestamp,
    },
}
enum LocalCommit<V> {
    Applied(LocalEffect),
    Store(Entry<V>),
}
struct DataCommit<V> {
    lane_guard: crate::memory::ReclamationGuard<crate::commit::LaneGuard>,
    key: Arc<str>,
    data: PreparedData,
    command: Option<BackplaneCommand>,
    opts: EntryOptions,
    fence: Arc<Fence>,
    flight: Option<FlightGuard>,
    local: LocalCommit<V>,
}
async fn lease_lost(state: &mut watch::Receiver<LeaseState>) {
    loop {
        if *state.borrow_and_update() != LeaseState::Held {
            return;
        }
        if state.changed().await.is_err() {
            return;
        }
    }
}
async fn health_changed(state: &mut Option<watch::Receiver<BackplaneState>>) {
    match state {
        Some(state) => {
            if state.changed().await.is_err() {
                std::future::pending::<()>().await;
            }
        }
        None => std::future::pending::<()>().await,
    }
}
fn recovery_action(action: BackplaneAction) -> RecoveryAction {
    match action {
        BackplaneAction::Set => RecoveryAction::Set,
        BackplaneAction::Remove => RecoveryAction::Remove,
        BackplaneAction::Expire => RecoveryAction::Expire,
    }
}
fn newer_of<V: Clone>(existing: Option<Entry<V>>, candidate: Entry<V>) -> Entry<V> {
    match existing {
        Some(existing) if existing.meta().created() >= candidate.meta().created() => existing,
        _ => candidate,
    }
}

impl<V: Clone + Send + Sync + 'static> CacheInner<V> {
    fn l2_key<'a>(&self, key: &'a str) -> distributed_key::PhysicalKey<'a> {
        self.distributed_key.physical(key)
    }
    fn logical_key(&self, physical: &str) -> Option<Arc<str>> {
        let key = self.distributed_key.logical(physical)?;
        if self
            .key_prefix
            .as_ref()
            .is_some_and(|prefix| !key.starts_with(&**prefix))
        {
            return None;
        }
        Some(Arc::from(key))
    }
}
