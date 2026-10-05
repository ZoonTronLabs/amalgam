//! Multi-level orchestration with fallible boundaries and owned work.
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
    CacheEvent, CacheLevel, CacheOperation, CircuitComponent, Events, OperationOutcome,
};
use crate::execution::{
    CancellationSource, Execution, FactoryCancellation, InlinePermit, LinkMode, Scopes, lock,
};
use crate::factory::{FactoryContext, FactoryProduct, ProductOrigin, StaleInfo};
use crate::lifecycle::Tasks;
use crate::locking::{KeyGuard, KeyedLock};
use crate::marker_reads::{
    MarkerLifecycleAccess, MarkerObservation, MarkerObservations, MarkerPresence,
    MarkerReadFailure, MarkerReadOutcome, MarkerReadPolicy, MarkerReads,
};
use crate::marker_snapshots::{
    MarkerLifecyclePolicy, MarkerSnapshot, MarkerSnapshotCache, MarkerSnapshotRead,
    MarkerSnapshotRenewal,
};
use crate::maybe::MaybeValue;
use crate::memory::{MemoryAdmission, MemoryExpiry, MemoryLimits, MemoryStore};
use crate::observability::{OperationObservation, component_span};
use crate::options::{
    EntryOptions, JitterSample, JitterSource, KeyModifierMode, RandomJitterSource,
    RemoveByTagBehavior,
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
    /// Preserve the physically live L2 snapshot for fail-safe. Existing Rust default.
    #[default]
    RetainStale,
    /// Physically remove L2 while expiring L1, matching FusionCache 2.9.
    Remove,
}
/// Explicit cluster-lock compatibility contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeasePolicy {
    /// Require owned acquisition and atomic backend fencing.
    Fenced,
    /// Deliberate cooperative legacy integration; cannot promise partition fencing.
    CooperativeLegacy,
}
/// Durable reconciliation contract when notification continuity is unavailable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconciliationPolicy {
    /// No external storage or notifications exist; all invalidation is local.
    LocalOnly,
    /// Periodically discard L1 so subsequent reads reconcile durable markers.
    Periodic(Duration),
    /// Trust an acknowledged continuous native backplane; gaps still discard L1.
    BackplaneContinuity,
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
    Acknowledged(crate::ContinuityEpoch),
    /// Legacy provider has no health/ACK facet; periodic reconciliation applies.
    BestEffort,
}

/// A typed value cache. Public clones share lifecycle; workers never own public handles.
pub struct Cache<V: Clone + Send + Sync + 'static> {
    inner: Arc<CacheInner<V>>,
    lifetime: Arc<PublicLifetime<V>>,
}
struct PublicLifetime<V: Clone + Send + Sync + 'static> {
    inner: Weak<CacheInner<V>>,
}
impl<V: Clone + Send + Sync + 'static> Drop for PublicLifetime<V> {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.upgrade() {
            inner.close();
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
struct Worker<V: Clone + Send + Sync + 'static> {
    inner: Arc<CacheInner<V>>,
}
impl<V: Clone + Send + Sync + 'static> Clone for Worker<V> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
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
    memory: MemoryStore<V>,
    locks: KeyedLock,
    lanes: Lanes,
    tags: TagRegistry,
    events: Events,
    clock: Arc<dyn Clock>,
    default_options: EntryOptions,
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
    distributed_wire_version: Arc<str>,
    distributed_key_modifier_mode: KeyModifierMode,
    disable_tagging: bool,
    wait_for_initial_backplane_subscribe: bool,
    cloner: Option<Arc<dyn ValueCloner<V>>>,
    jitter: Arc<dyn JitterSource>,
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
enum MarkerFetch {
    Observed(MarkerPresence),
    Snapshot(MarkerSnapshotRead),
    Deadline(MarkerReadFailure),
}
enum MarkerLookup {
    Ready(MarkerReadOutcome),
    Refresh(Option<Entry<MarkerObservation>>),
}
#[derive(Clone, Copy)]
enum MarkerFactoryStale<'a> {
    Memory(&'a Entry<MarkerObservation>),
    Distributed(MarkerSnapshot),
    Absent,
}
enum MarkerProviderFault {
    Backend,
    Protocol,
}
impl MarkerProviderFault {
    fn read_failure(self) -> MarkerReadFailure {
        match self {
            Self::Backend => MarkerReadFailure::Backend,
            Self::Protocol => MarkerReadFailure::Protocol,
        }
    }
    fn write_outcome(self) -> crate::MarkerSnapshotWriteOutcome {
        match self {
            Self::Backend => crate::MarkerSnapshotWriteOutcome::BackendFailure,
            Self::Protocol => crate::MarkerSnapshotWriteOutcome::ProtocolFailure,
        }
    }
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum L2ReadPolicy {
    PreserveFailure,
    FactoryFallback,
}
impl L2ReadPolicy {
    fn rethrow(self, options: &EntryOptions, error: &Error) -> bool {
        match self {
            Self::PreserveFailure => true,
            Self::FactoryFallback => match error {
                Error::Serialization(_) | Error::Deserialization(_) | Error::Codec(_) => {
                    options.rethrow_serialization_exceptions()
                }
                Error::Config(_)
                | Error::Clone(_)
                | Error::Tag(_)
                | Error::OperationCancelled { .. }
                | Error::FactoryCancelled { .. } => true,
                _ => options.rethrow_distributed_exceptions(),
            },
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
}
struct ReadyValue<'key, V> {
    value: V,
    key: Cow<'key, str>,
}
type LookupKey = crate::factory::FactoryKeys;
// Ready operations cannot park. Their counted permit covers all synchronous
// user code, including unused factory/fallback destructors and event callbacks.
struct ReadyLookup<'a, V> {
    result: Result<ReadyValue<'a, V>>,
    observation: OperationObservation,
    permit: InlinePermit<'a>,
}
enum LookupStart<'a, V> {
    Ready(ReadyLookup<'a, V>),
    Owned {
        observation: OperationObservation,
        key: Cow<'a, str>,
        permit: InlinePermit<'a>,
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
            Ok(ready) => Ok(ReadyValue {
                value: complete(ready.value),
                key: ready.key,
            }),
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

impl<V: Clone + Send + Sync + 'static> Cache<V> {
    /// Starts validated construction.
    pub fn builder() -> CacheBuilder<V> {
        CacheBuilder::new()
    }
    /// Creates the always-valid memory-only defaults without a runtime.
    pub fn new() -> Self {
        CacheBuilder::new().build()
    }
    /// Diagnostic name.
    pub fn name(&self) -> &str {
        &self.inner.name
    }
    /// This instance's diagnostic and backplane sender identity.
    pub fn instance_id(&self) -> &str {
        &self.inner.instance_id
    }
    /// The configured distributed byte store, when L2 is enabled.
    pub fn distributed_cache(&self) -> Option<&Arc<dyn DistributedCache>> {
        match &self.inner.storage {
            Storage::MemoryOnly => None,
            Storage::Hybrid { backend, .. } => Some(backend),
        }
    }
    /// The configured backplane, when attached.
    pub fn backplane(&self) -> Option<&Arc<dyn Backplane>> {
        self.inner.backplane.as_ref()
    }
    /// The configured distributed locker, when attached.
    pub fn distributed_locker(&self) -> Option<&Arc<dyn DistributedLocker>> {
        self.inner.distributed_locker.as_ref()
    }
    /// Unified event route, including eviction and plugins.
    pub fn events(&self) -> &Events {
        &self.inner.events
    }
    /// Starts a dynamic plugin session owned by its registration and this cache.
    /// Dropping or stopping the registration detaches it; shutdown waits callbacks.
    pub fn register_plugin(&self, plugin: Arc<dyn Plugin>) -> Result<crate::PluginRegistration> {
        let permit = self.inner.scopes.inline();
        permit.admit()?;
        let registration = self.inner.plugins.register(plugin)?;
        permit.status(None)?;
        Ok(registration)
    }
    /// Static default options; per-key providers may override them.
    pub fn entry_options(&self) -> EntryOptions {
        self.inner.default_options.clone()
    }
    /// Independent tag/clear operation defaults; key providers do not affect them.
    pub fn tags_entry_options(&self) -> EntryOptions {
        self.inner.tags_default_options.clone()
    }

    /// The selected secondary control-read contract.
    pub fn marker_read_policy(&self) -> MarkerReadPolicy {
        self.inner.marker_reads.policy()
    }
    /// The independent marker snapshot lifetime/repair contract.
    pub fn marker_lifecycle_policy(&self) -> MarkerLifecyclePolicy {
        self.inner.marker_reads.lifecycle_policy()
    }
    /// Configured subscription readiness policy.
    pub fn wait_for_initial_backplane_subscribe(&self) -> bool {
        self.inner.wait_for_initial_backplane_subscribe
    }
    /// Waits native subscription admission. Close cancels a parked readiness wait.
    pub async fn ready(&self) -> Result<BackplaneReadiness> {
        if self.inner.scopes.is_closed() {
            return Err(Error::CacheClosed);
        }
        let worker = self.worker();
        self.inner
            .scopes
            .execution(
                async move { worker.await_readiness().await },
                CancellationSource::new(),
            )
            .await
    }
    /// Number of exact queued/in-flight recovery operations (zero when disabled).
    pub fn pending_recovery(&self) -> usize {
        self.inner
            .recovery
            .as_ref()
            .map_or(0, |recovery| recovery.len())
    }
    /// Immutable diagnostic ticket for a data key; it pins the exact commit lane.
    pub fn recovery_ticket(&self, key: impl AsRef<str>) -> Option<ReplayTicket> {
        let key = self.worker().full_key(key.as_ref());
        self.inner
            .recovery
            .as_ref()
            .and_then(|recovery| recovery.snapshot(&key))
    }
    fn worker(&self) -> Worker<V> {
        Worker {
            inner: Arc::clone(&self.inner),
        }
    }
    fn lookup_key(&self, raw: &str, full: Arc<str>) -> LookupKey {
        let raw = if self.inner.key_prefix.as_deref().is_none_or(str::is_empty) {
            Arc::clone(&full)
        } else {
            Arc::from(raw)
        };
        LookupKey { raw, full }
    }
    fn observed<T: Send + 'static>(
        &self,
        operation: CacheOperation,
        key: Option<Arc<str>>,
        token: Option<FactoryCancellation>,
        work: impl Future<Output = Result<Observed<T>>> + Send + 'static,
    ) -> impl Future<Output = Result<T>> + Send + '_ {
        // The scope will own this work on the heap. Erase it before composing
        // the observer futures, so callers do not embed the complete mutation
        // state machine in every async adapter and tracing layer.
        self.observed_using(
            operation,
            key,
            token,
            CancellationSource::new(),
            Box::pin(work),
        )
    }
    async fn observed_using<T: Send + 'static>(
        &self,
        operation: CacheOperation,
        key: Option<Arc<str>>,
        token: Option<FactoryCancellation>,
        source: CancellationSource,
        work: Pin<Box<dyn Future<Output = Result<Observed<T>>> + Send>>,
    ) -> Result<T> {
        let observation = OperationObservation::new(
            self.inner.events.clone(),
            &self.inner.name,
            &self.inner.instance_id,
            operation,
            key.as_deref(),
        );
        self.execute_observed(observation, token, source, ObservationAdmission::New, work)
            .await
    }
    async fn execute_observed<T: Send + 'static>(
        &self,
        mut observation: OperationObservation,
        token: Option<FactoryCancellation>,
        source: CancellationSource,
        admission: ObservationAdmission<'_>,
        work: impl Future<Output = Result<Observed<T>>> + Send + 'static,
    ) -> Result<T> {
        let span = observation.span();
        if self.inner.scopes.is_closed() {
            drop(work);
            observation.finish(OperationOutcome::from_error(&Error::CacheClosed));
            return Err(Error::CacheClosed);
        }
        self.worker().start_maintenance();
        let worker = self.worker();
        let completion_token = source.token();
        // The scope owns the observer too: a parked caller can be cancelled and
        // finish its logical observation without polling its future again.
        let execution = self.inner.scopes.execution(
            async move {
                if worker.inner.wait_for_initial_backplane_subscribe
                    && !worker.inner.subscription_admitted.load(Ordering::Acquire)
                {
                    if let Err(error) = worker.await_readiness().await {
                        observation.finish(OperationOutcome::from_error(&error));
                        return Err(error);
                    }
                    worker
                        .inner
                        .subscription_admitted
                        .store(true, Ordering::Release);
                }
                let result = work.await;
                // Synchronous completion/destruction can close or explicitly
                // cancel this scope while it is polling. Attribute that reason
                // before finishing its single logical observation.
                let result = match completion_token.reason() {
                    Some(reason) => Err(Error::OperationCancelled { reason }),
                    None => result,
                };
                match result {
                    Ok(result) => {
                        if let Some(level) = result.level {
                            observation.set_level(level);
                        }
                        observation.finish(result.outcome);
                        Ok(result.value)
                    }
                    Err(error) => {
                        observation.finish(OperationOutcome::from_error(&error));
                        Err(error)
                    }
                }
            }
            .instrument(span),
            source,
        );
        // Registration owns the observer and all caller work before the ready
        // path's permit is released, closing the transfer gap against shutdown.
        match admission {
            ObservationAdmission::New => {}
            ObservationAdmission::Inline(permit) => drop(permit),
        }
        drive(execution, token).await
    }
    fn start_lookup<'a>(
        &'a self,
        raw: &'a str,
        options: Option<&EntryOptions>,
        token: Option<&FactoryCancellation>,
        operation: CacheOperation,
        mode: LookupMode,
    ) -> LookupStart<'a, V> {
        let permit = self.inner.scopes.inline();
        let key = match &self.inner.key_prefix {
            Some(prefix) => Cow::Owned(format!("{prefix}{raw}")),
            None => Cow::Borrowed(raw),
        };
        let observation = OperationObservation::new(
            self.inner.events.clone(),
            &self.inner.name,
            &self.inner.instance_id,
            operation,
            Some(&key),
        );
        let span = observation.span();
        let _entered = span.enter();
        let result = permit
            .admit()
            .and_then(|()| self.ready_value(&key, options, token, mode, &permit));
        match result {
            Ok(None) => LookupStart::Owned {
                observation,
                key,
                permit,
            },
            Ok(Some(value)) => LookupStart::Ready(ReadyLookup {
                result: Ok(ReadyValue { value, key }),
                observation,
                permit,
            }),
            Err(error) => LookupStart::Ready(ReadyLookup {
                result: Err(error),
                observation,
                permit,
            }),
        }
    }
    fn ready_value(
        &self,
        key: &str,
        options: Option<&EntryOptions>,
        token: Option<&FactoryCancellation>,
        mode: LookupMode,
        permit: &InlinePermit<'_>,
    ) -> Result<Option<V>> {
        permit.status(token)?;
        let worker = self.worker();
        worker.start_maintenance();
        if self.inner.wait_for_initial_backplane_subscribe
            && !self.inner.subscription_admitted.load(Ordering::Acquire)
            || options.is_none() && self.inner.default_options_provider.is_some()
        {
            return Ok(None);
        }
        let opts = options.unwrap_or(&self.inner.default_options);
        worker.validate_options(opts)?;
        if opts.skip_memory_read() {
            return Ok(None);
        }
        worker.ensure_health();
        permit.status(token)?;
        let now = self.inner.clock.now();
        permit.status(token)?;
        let entry = self.inner.memory.ready_at(key, now);
        permit.status(token)?;
        let Some(entry) = entry else {
            return Ok(None);
        };
        if worker.tags(&entry) != TagVerdict::Valid || !entry.freshness(now).is_fresh() {
            return Ok(None);
        }
        if !worker.marker_reads_ready(entry.meta().tags(), now) {
            return Ok(None);
        }
        if matches!(mode, LookupMode::GetOrSet) {
            let eager = entry.should_eager_refresh(self.inner.clock.now());
            permit.status(token)?;
            if eager {
                return Ok(None);
            }
        }
        let value = worker.copy(entry.value(), opts);
        permit.status(token)?;
        let value = value?;
        worker.marker_ready_events(entry.meta().tags(), now);
        permit.status(token)?;
        Ok(Some(value))
    }
    /// Returns a value or produces it. Background write policy is observable;
    /// use get_or_set_full_with_commit when its actual completion is required.
    pub async fn get_or_set<F, Fut>(&self, key: impl AsRef<str>, factory: F) -> Result<V>
    where
        F: FnOnce(FactoryContext<V>) -> Fut + Send + 'static,
        Fut: Future<Output = std::result::Result<FactoryProduct<V>, FactoryError>> + Send + 'static,
    {
        self.get_or_set_full(key, factory, None, Box::from([]), MaybeValue::none())
            .await
    }
    /// Retrieval with explicit options.
    pub async fn get_or_set_with<F, Fut>(
        &self,
        key: impl AsRef<str>,
        factory: F,
        options: EntryOptions,
    ) -> Result<V>
    where
        F: FnOnce(FactoryContext<V>) -> Fut + Send + 'static,
        Fut: Future<Output = std::result::Result<FactoryProduct<V>, FactoryError>> + Send + 'static,
    {
        self.get_or_set_full(
            key,
            factory,
            Some(options),
            Box::from([]),
            MaybeValue::none(),
        )
        .await
    }
    /// Constant-value retrieval through the same coordination pipeline.
    pub async fn get_or_set_value(
        &self,
        key: impl AsRef<str>,
        value: V,
        options: Option<EntryOptions>,
    ) -> Result<V> {
        self.get_or_set_full(
            key,
            move |ctx| async move { Ok(ctx.value(value)) },
            options,
            Box::from([]),
            MaybeValue::none(),
        )
        .await
    }
    /// Full retrieval. Ordinary timeout may activate fail-safe; explicit
    /// cancellation/shutdown/lease loss never masquerades as an origin failure.
    pub async fn get_or_set_full<F, Fut>(
        &self,
        key: impl AsRef<str>,
        factory: F,
        options: Option<EntryOptions>,
        tags: Box<[Tag]>,
        fail_safe_default: MaybeValue<V>,
    ) -> Result<V>
    where
        F: FnOnce(FactoryContext<V>) -> Fut + Send + 'static,
        Fut: Future<Output = std::result::Result<FactoryProduct<V>, FactoryError>> + Send + 'static,
    {
        Ok(self
            .get_or_set_impl(
                key.as_ref(),
                factory,
                options,
                tags,
                fail_safe_default,
                None,
            )
            .await?
            .value)
    }
    /// Full retrieval plus an actual factory-commit receipt.
    pub async fn get_or_set_full_with_commit<F, Fut>(
        &self,
        key: impl AsRef<str>,
        factory: F,
        options: Option<EntryOptions>,
        tags: Box<[Tag]>,
        fail_safe_default: MaybeValue<V>,
    ) -> Result<CacheValue<V>>
    where
        F: FnOnce(FactoryContext<V>) -> Fut + Send + 'static,
        Fut: Future<Output = std::result::Result<FactoryProduct<V>, FactoryError>> + Send + 'static,
    {
        self.get_or_set_impl(
            key.as_ref(),
            factory,
            options,
            tags,
            fail_safe_default,
            None,
        )
        .await
    }
    /// Retrieval with an explicit caller cancellation request.
    pub async fn get_or_set_cancellable<F, Fut>(
        &self,
        key: impl AsRef<str>,
        factory: F,
        cancellation: FactoryCancellation,
    ) -> Result<V>
    where
        F: FnOnce(FactoryContext<V>) -> Fut + Send + 'static,
        Fut: Future<Output = std::result::Result<FactoryProduct<V>, FactoryError>> + Send + 'static,
    {
        self.get_or_set_full_cancellable(
            key,
            factory,
            None,
            Box::from([]),
            MaybeValue::none(),
            cancellation,
        )
        .await
    }
    /// Full retrieval with explicit cancellation until deliberate background handoff.
    pub async fn get_or_set_full_cancellable<F, Fut>(
        &self,
        key: impl AsRef<str>,
        factory: F,
        options: Option<EntryOptions>,
        tags: Box<[Tag]>,
        fail_safe_default: MaybeValue<V>,
        cancellation: FactoryCancellation,
    ) -> Result<V>
    where
        F: FnOnce(FactoryContext<V>) -> Fut + Send + 'static,
        Fut: Future<Output = std::result::Result<FactoryProduct<V>, FactoryError>> + Send + 'static,
    {
        Ok(self
            .get_or_set_impl(
                key.as_ref(),
                factory,
                options,
                tags,
                fail_safe_default,
                Some(cancellation),
            )
            .await?
            .value)
    }
    async fn get_or_set_impl<F, Fut>(
        &self,
        key: &str,
        factory: F,
        options: Option<EntryOptions>,
        tags: Box<[Tag]>,
        fallback: MaybeValue<V>,
        cancellation: Option<FactoryCancellation>,
    ) -> Result<CacheValue<V>>
    where
        F: FnOnce(FactoryContext<V>) -> Fut + Send + 'static,
        Fut: Future<Output = std::result::Result<FactoryProduct<V>, FactoryError>> + Send + 'static,
    {
        let (observation, full, permit) = match self.start_lookup(
            key,
            options.as_ref(),
            cancellation.as_ref(),
            CacheOperation::GetOrSet,
            LookupMode::GetOrSet,
        ) {
            LookupStart::Ready(ready) => {
                // These captures can execute user Drop code; keep them within
                // the same counted operation before its final cancellation check.
                let span = ready.observation.span();
                let _entered = span.enter();
                drop((factory, tags, fallback, options));
                return ready
                    .finish(
                        cancellation.as_ref(),
                        &self.inner.events,
                        std::convert::identity,
                    )
                    .map(|value| CacheValue {
                        value,
                        commit: CommitReceipt::Unchanged,
                    });
            }
            LookupStart::Owned {
                observation,
                key,
                permit,
            } => (observation, Arc::<str>::from(key.as_ref()), permit),
        };
        let worker = self.worker();
        let key = self.lookup_key(key, full);
        let source = CancellationSource::new();
        let caller = source.token();
        self.execute_observed(
            observation,
            cancellation,
            source,
            ObservationAdmission::Inline(permit),
            async move {
                worker
                    .get_or_set(key, factory, options, tags, fallback, caller)
                    .await
            },
        )
        .await
    }
    /// Canonical read-only L1/L2 lookup. Expected failures retain their typed channel.
    pub async fn read(
        &self,
        key: impl AsRef<str>,
        options: Option<EntryOptions>,
    ) -> Result<MaybeValue<V>> {
        self.read_impl(key.as_ref(), options, None, CacheOperation::TryGet)
            .await
    }
    /// Canonical read with explicit cancellation.
    pub async fn read_cancellable(
        &self,
        key: impl AsRef<str>,
        options: Option<EntryOptions>,
        cancellation: FactoryCancellation,
    ) -> Result<MaybeValue<V>> {
        self.read_impl(
            key.as_ref(),
            options,
            Some(cancellation),
            CacheOperation::TryGet,
        )
        .await
    }
    async fn read_impl(
        &self,
        key: &str,
        options: Option<EntryOptions>,
        token: Option<FactoryCancellation>,
        operation: CacheOperation,
    ) -> Result<MaybeValue<V>> {
        self.read_complete(
            key,
            options,
            token,
            operation,
            L2ReadPolicy::PreserveFailure,
            std::convert::identity,
        )
        .await
    }
    async fn read_complete<T: Send + 'static>(
        &self,
        key: &str,
        options: Option<EntryOptions>,
        token: Option<FactoryCancellation>,
        operation: CacheOperation,
        policy: L2ReadPolicy,
        complete: impl FnOnce(MaybeValue<V>) -> T + Send + 'static,
    ) -> Result<T> {
        let (observation, full, permit) = match self.start_lookup(
            key,
            options.as_ref(),
            token.as_ref(),
            operation,
            LookupMode::Read,
        ) {
            LookupStart::Ready(ready) => {
                return ready.finish(token.as_ref(), &self.inner.events, move |value| {
                    complete(MaybeValue::from_value(value))
                });
            }
            LookupStart::Owned {
                observation,
                key,
                permit,
            } => (observation, Arc::<str>::from(key.as_ref()), permit),
        };
        let worker = self.worker();
        let key = self.lookup_key(key, full);
        let source = CancellationSource::new();
        let cancellation = source.token();
        self.execute_observed(
            observation,
            token,
            source,
            ObservationAdmission::Inline(permit),
            async move {
                let result = worker.read(key, options, policy, &cancellation).await?;
                Ok(Observed::new(
                    complete(result.value),
                    result.outcome,
                    result.level,
                ))
            },
        )
        .await
    }
    /// Returns default only after a successful miss; errors remain errors.
    pub async fn read_or_default(
        &self,
        key: impl AsRef<str>,
        default: V,
        options: Option<EntryOptions>,
    ) -> Result<V> {
        self.read_complete(
            key.as_ref(),
            options,
            None,
            CacheOperation::GetOrDefault,
            L2ReadPolicy::PreserveFailure,
            move |value| value.value_or(default),
        )
        .await
    }
    /// Default-on-successful-miss with explicit cancellation.
    pub async fn read_or_default_cancellable(
        &self,
        key: impl AsRef<str>,
        default: V,
        options: Option<EntryOptions>,
        cancellation: FactoryCancellation,
    ) -> Result<V> {
        self.read_complete(
            key.as_ref(),
            options,
            Some(cancellation),
            CacheOperation::GetOrDefault,
            L2ReadPolicy::PreserveFailure,
            move |value| value.value_or(default),
        )
        .await
    }
    /// Legacy lookup adapter. A diagnosed error maps to absent value because this
    /// historical signature cannot carry failures. Prefer read().
    pub async fn try_get(
        &self,
        key: impl AsRef<str>,
        options: Option<EntryOptions>,
    ) -> MaybeValue<V> {
        match self
            .read_complete(
                key.as_ref(),
                options,
                None,
                CacheOperation::TryGet,
                L2ReadPolicy::FactoryFallback,
                std::convert::identity,
            )
            .await
        {
            Ok(value) => value,
            Err(error) => {
                self.legacy_error(&error);
                MaybeValue::none()
            }
        }
    }
    /// Legacy default adapter; prefer read_or_default for error preservation.
    pub async fn get_or_default(
        &self,
        key: impl AsRef<str>,
        default: V,
        options: Option<EntryOptions>,
    ) -> V {
        let scopes = Arc::clone(&self.inner.scopes);
        match self
            .read_complete(
                key.as_ref(),
                options,
                None,
                CacheOperation::GetOrDefault,
                L2ReadPolicy::FactoryFallback,
                move |value| (value, scopes.inline_owned()),
            )
            .await
        {
            Ok((value, permit)) => {
                let value = value.value_or(default);
                if let Err(error) = permit.status(None) {
                    self.legacy_error(&error);
                }
                value
            }
            Err(error) => {
                self.legacy_error(&error);
                default
            }
        }
    }
    /// Canonical set using resolved per-key options.
    pub async fn try_set(&self, key: impl AsRef<str>, value: V) -> Result<MutationReceipt> {
        self.try_set_full(key, value, None, Box::from([])).await
    }
    /// Canonical set with tags/options.
    pub async fn try_set_full(
        &self,
        key: impl AsRef<str>,
        value: V,
        options: Option<EntryOptions>,
        tags: Box<[Tag]>,
    ) -> Result<MutationReceipt> {
        self.set_impl(key.as_ref(), value, options, tags, None)
            .await
    }
    /// Canonical set with explicit cancellation until scheduled ownership transfer.
    pub async fn try_set_full_cancellable(
        &self,
        key: impl AsRef<str>,
        value: V,
        options: Option<EntryOptions>,
        tags: Box<[Tag]>,
        token: FactoryCancellation,
    ) -> Result<MutationReceipt> {
        self.set_impl(key.as_ref(), value, options, tags, Some(token))
            .await
    }
    async fn set_impl(
        &self,
        key: &str,
        value: V,
        options: Option<EntryOptions>,
        tags: Box<[Tag]>,
        token: Option<FactoryCancellation>,
    ) -> Result<MutationReceipt> {
        let worker = self.worker();
        let raw: Arc<str> = Arc::from(key);
        let full = worker.full_key(key);
        let source = CancellationSource::new();
        let cancellation = source.token();
        self.observed_using(
            CacheOperation::Set,
            Some(full),
            token,
            source,
            Box::pin(async move { worker.set(raw, value, options, tags, &cancellation).await }),
        )
        .await
    }
    /// Legacy unit adapter. Prefer try_set to inspect actual completion.
    pub async fn set(&self, key: impl AsRef<str>, value: V) {
        if let Err(error) = self.try_set(key, value).await {
            self.legacy_error(&error);
        }
    }
    /// Legacy unit adapter with options and tags.
    pub async fn set_full(
        &self,
        key: impl AsRef<str>,
        value: V,
        options: Option<EntryOptions>,
        tags: Box<[Tag]>,
    ) {
        if let Err(error) = self.try_set_full(key, value, options, tags).await {
            self.legacy_error(&error);
        }
    }
    /// Canonical remove.
    pub async fn try_remove(&self, key: impl AsRef<str>) -> Result<MutationReceipt> {
        self.try_remove_with(key, None).await
    }
    /// Remove with per-key dynamic/explicit options.
    pub async fn try_remove_with(
        &self,
        key: impl AsRef<str>,
        options: Option<EntryOptions>,
    ) -> Result<MutationReceipt> {
        self.key_mutation(key.as_ref(), options, KeyMutation::Remove, None)
            .await
    }
    /// Cancellable remove.
    pub async fn try_remove_with_cancellable(
        &self,
        key: impl AsRef<str>,
        options: Option<EntryOptions>,
        token: FactoryCancellation,
    ) -> Result<MutationReceipt> {
        self.key_mutation(key.as_ref(), options, KeyMutation::Remove, Some(token))
            .await
    }
    /// Canonical logical expiration, including a cold L2 key.
    pub async fn try_expire(&self, key: impl AsRef<str>) -> Result<MutationReceipt> {
        self.try_expire_with(key, None).await
    }
    /// Expiration with per-key options.
    pub async fn try_expire_with(
        &self,
        key: impl AsRef<str>,
        options: Option<EntryOptions>,
    ) -> Result<MutationReceipt> {
        self.key_mutation(
            key.as_ref(),
            options,
            KeyMutation::Expire(DistributedExpirePolicy::RetainStale),
            None,
        )
        .await
    }
    /// Cancellable expiration.
    pub async fn try_expire_with_cancellable(
        &self,
        key: impl AsRef<str>,
        options: Option<EntryOptions>,
        token: FactoryCancellation,
    ) -> Result<MutationReceipt> {
        self.key_mutation(
            key.as_ref(),
            options,
            KeyMutation::Expire(DistributedExpirePolicy::RetainStale),
            Some(token),
        )
        .await
    }
    /// Expires L1 and selects the distributed retention/removal contract explicitly.
    pub async fn try_expire_with_policy(
        &self,
        key: impl AsRef<str>,
        options: Option<EntryOptions>,
        policy: DistributedExpirePolicy,
    ) -> Result<MutationReceipt> {
        self.key_mutation(key.as_ref(), options, KeyMutation::Expire(policy), None)
            .await
    }
    /// Explicit expiration policy with caller cancellation through ownership transfer.
    pub async fn try_expire_with_policy_cancellable(
        &self,
        key: impl AsRef<str>,
        options: Option<EntryOptions>,
        policy: DistributedExpirePolicy,
        token: FactoryCancellation,
    ) -> Result<MutationReceipt> {
        self.key_mutation(
            key.as_ref(),
            options,
            KeyMutation::Expire(policy),
            Some(token),
        )
        .await
    }
    async fn key_mutation(
        &self,
        key: &str,
        options: Option<EntryOptions>,
        mutation: KeyMutation,
        token: Option<FactoryCancellation>,
    ) -> Result<MutationReceipt> {
        let worker = self.worker();
        let raw: Arc<str> = Arc::from(key);
        let full = worker.full_key(key);
        let operation = match mutation {
            KeyMutation::Remove => CacheOperation::Remove,
            KeyMutation::Expire(_) => CacheOperation::Expire,
        };
        let source = CancellationSource::new();
        let cancellation = source.token();
        self.observed_using(
            operation,
            Some(full),
            token,
            source,
            Box::pin(async move {
                worker
                    .key_mutation(raw, options, mutation, &cancellation)
                    .await
            }),
        )
        .await
    }
    /// Legacy unit remove adapter.
    pub async fn remove(&self, key: impl AsRef<str>) {
        if let Err(error) = self.try_remove(key).await {
            self.legacy_error(&error);
        }
    }
    /// Legacy unit expiration adapter.
    pub async fn expire(&self, key: impl AsRef<str>) {
        if let Err(error) = self.try_expire(key).await {
            self.legacy_error(&error);
        }
    }
    /// Canonical tag invalidation with invariant-preserving Tag.
    pub async fn try_remove_by_tag(&self, tag: Tag) -> Result<MutationReceipt> {
        self.try_remove_by_tag_with(tag, None).await
    }
    /// Tag invalidation with explicit options.
    pub async fn try_remove_by_tag_with(
        &self,
        tag: Tag,
        options: Option<EntryOptions>,
    ) -> Result<MutationReceipt> {
        self.markers_impl(
            vec![MarkerKind::Tag(tag)],
            options,
            CacheOperation::RemoveByTag,
            None,
        )
        .await
    }
    /// Cancellable tag invalidation.
    pub async fn try_remove_by_tag_with_cancellable(
        &self,
        tag: Tag,
        options: Option<EntryOptions>,
        token: FactoryCancellation,
    ) -> Result<MutationReceipt> {
        self.markers_impl(
            vec![MarkerKind::Tag(tag)],
            options,
            CacheOperation::RemoveByTag,
            Some(token),
        )
        .await
    }
    /// Canonical batch tag invalidation.
    pub async fn try_remove_by_tags(
        &self,
        tags: impl IntoIterator<Item = Tag>,
    ) -> Result<MutationReceipt> {
        self.try_remove_by_tags_with(tags, None).await
    }
    /// Batch invalidation with explicit options.
    pub async fn try_remove_by_tags_with(
        &self,
        tags: impl IntoIterator<Item = Tag>,
        options: Option<EntryOptions>,
    ) -> Result<MutationReceipt> {
        self.markers_impl(
            tags.into_iter().map(MarkerKind::Tag).collect(),
            options,
            CacheOperation::RemoveByTags,
            None,
        )
        .await
    }
    /// Cancellable batch invalidation.
    pub async fn try_remove_by_tags_with_cancellable(
        &self,
        tags: impl IntoIterator<Item = Tag>,
        options: Option<EntryOptions>,
        token: FactoryCancellation,
    ) -> Result<MutationReceipt> {
        self.markers_impl(
            tags.into_iter().map(MarkerKind::Tag).collect(),
            options,
            CacheOperation::RemoveByTags,
            Some(token),
        )
        .await
    }
    /// Canonical scoped clear.
    pub async fn try_clear(&self, mode: ClearMode) -> Result<MutationReceipt> {
        self.try_clear_with(mode, None).await
    }
    /// Clear with explicit options.
    pub async fn try_clear_with(
        &self,
        mode: ClearMode,
        options: Option<EntryOptions>,
    ) -> Result<MutationReceipt> {
        self.markers_impl(
            vec![match mode {
                ClearMode::Expire => MarkerKind::ClearExpire,
                ClearMode::Remove => MarkerKind::ClearRemove,
            }],
            options,
            CacheOperation::Clear,
            None,
        )
        .await
    }
    /// Cancellable clear.
    pub async fn try_clear_with_cancellable(
        &self,
        mode: ClearMode,
        options: Option<EntryOptions>,
        token: FactoryCancellation,
    ) -> Result<MutationReceipt> {
        self.markers_impl(
            vec![match mode {
                ClearMode::Expire => MarkerKind::ClearExpire,
                ClearMode::Remove => MarkerKind::ClearRemove,
            }],
            options,
            CacheOperation::Clear,
            Some(token),
        )
        .await
    }
    async fn markers_impl(
        &self,
        kinds: Vec<MarkerKind>,
        options: Option<EntryOptions>,
        operation: CacheOperation,
        token: Option<FactoryCancellation>,
    ) -> Result<MutationReceipt> {
        let worker = self.worker();
        self.observed(operation, None, token, async move {
            worker.mutate_markers(kinds, options).await
        })
        .await
    }
    /// Legacy raw-tag/unit adapter. Invalid requests are diagnosed.
    pub async fn remove_by_tag(&self, tag: impl AsRef<str>) {
        match Tag::new(tag) {
            Ok(tag) => {
                if let Err(error) = self.try_remove_by_tag(tag).await {
                    self.legacy_error(&error);
                }
            }
            Err(error) => self.legacy_error(&error.into()),
        }
    }
    /// Legacy raw batch adapter; any invalid tag rejects the batch.
    pub async fn remove_by_tags<I, S>(&self, tags: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        match crate::tags::try_collect_tags(tags) {
            Ok(tags) => {
                if let Err(error) = self.try_remove_by_tags(Vec::from(tags)).await {
                    self.legacy_error(&error);
                }
            }
            Err(error) => self.legacy_error(&error.into()),
        }
    }
    /// Legacy boolean/unit clear adapter.
    pub async fn clear(&self, allow_fail_safe: bool) {
        if let Err(error) = self
            .try_clear(if allow_fail_safe {
                ClearMode::Expire
            } else {
                ClearMode::Remove
            })
            .await
        {
            self.legacy_error(&error);
        }
    }
    fn legacy_error(&self, error: &Error) {
        tracing::warn!(cache=%self.inner.name,%error,"legacy cache adapter discarded a typed failure; use the canonical fallible API");
    }
    /// Initiates close, cancellation and plugin teardown once.
    pub fn close(&self) -> CloseOutcome {
        self.inner.close()
    }
    /// Waits execution scopes, supervised effects, cleanup, recovery and plugins.
    pub async fn shutdown(&self) -> Result<ShutdownReport> {
        self.inner.shutdown().await
    }
    /// Runs explicit memory maintenance; does not claim background commit completion.
    pub async fn run_pending_tasks(&self) {
        self.inner.memory.run_pending_tasks().await;
        self.inner.locks.clean_idle(256);
        self.inner.lanes.clean(256);
    }
    /// Waits currently scheduled effects and their cleanup without closing the cache.
    pub async fn flush_pending(&self) -> Result<()> {
        self.inner.tasks.flush().await;
        Ok(())
    }
}
impl<V: Clone + Send + Sync + 'static> Default for Cache<V> {
    fn default() -> Self {
        Self::new()
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
fn validate_budget(timeout: Timeout) -> Result<()> {
    if let Timeout::After(duration) = timeout
        && Instant::now().checked_add(duration).is_none()
    {
        return Err(ConfigError::DeadlineOutOfRange.into());
    }
    Ok(())
}
async fn bounded<T>(timeout: Timeout, work: impl Future<Output = T>) -> Result<Option<T>> {
    validate_budget(timeout)?;
    match timeout {
        Timeout::Infinite => Ok(Some(work.await)),
        Timeout::After(duration) if duration.is_zero() => Ok(None),
        Timeout::After(duration) => Ok(tokio::time::timeout(duration, work).await.ok()),
    }
}

struct FlightGuard {
    local: LocalParticipation,
    lease: Option<DistributedLease>,
    tasks: Arc<Tasks>,
    events: Events,
    key: Arc<str>,
    policy: LeasePolicy,
}
enum LocalParticipation {
    Held(KeyGuard),
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
impl FlightGuard {
    fn proof(&self) -> Result<Option<crate::LeaseProof>> {
        match &self.lease {
            Some(lease) if self.policy == LeasePolicy::Fenced => Ok(Some(lease.proof()?)),
            Some(lease) => {
                if *lease.state().borrow() == LeaseState::Lost {
                    Err(LeaseError::Lost.into())
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }
}
impl Drop for FlightGuard {
    fn drop(&mut self) {
        if let Some(lease) = self.lease.take() {
            self.tasks
                .cleanup(Arc::clone(&self.key), self.events.clone(), async move {
                    lease.release().await.map_err(Error::from)
                });
        }
        std::mem::replace(&mut self.local, LocalParticipation::ReplayOnly).release();
    }
}
enum LockOutcome<V> {
    Acquired(FlightGuard),
    UnlockedAfterTimeout(FlightGuard),
    Served(V),
}
impl<V: Clone + Send + Sync + 'static> Worker<V> {
    fn lease_owner(&self, key: &Arc<str>) -> Arc<dyn LeaseTaskOwner> {
        Arc::new(CacheLeaseOwner {
            tasks: Arc::clone(&self.inner.tasks),
            events: self.inner.events.clone(),
            key: Arc::clone(key),
        })
    }
    async fn await_readiness(&self) -> Result<BackplaneReadiness> {
        let Some(backplane) = &self.inner.backplane else {
            return Ok(BackplaneReadiness::NotConfigured);
        };
        let Some(mut state) = backplane.connection_state() else {
            return Ok(BackplaneReadiness::BestEffort);
        };
        loop {
            let current = *state.borrow_and_update();
            match current {
                BackplaneState::Connected { epoch } => {
                    self.ensure_health();
                    return Ok(BackplaneReadiness::Acknowledged(epoch));
                }
                BackplaneState::Disconnected { .. } => {
                    self.ensure_health();
                }
                BackplaneState::Stopped => {
                    return Err(Error::Backplane("backplane provider stopped".into()));
                }
            }
            if state.changed().await.is_err() {
                return Err(Error::Backplane("backplane health stream closed".into()));
            }
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
        opts.validate_with_cloner(self.inner.cloner.as_deref())?;
        self.validate_execution_options(opts, OptionsTarget::Value)
    }
    fn validate_marker_options(&self, opts: &EntryOptions) -> Result<()> {
        opts.validate()?;
        self.validate_execution_options(opts, OptionsTarget::Marker)
    }
    fn validate_execution_options(&self, opts: &EntryOptions, target: OptionsTarget) -> Result<()> {
        for timeout in [
            opts.memory_lock_timeout(),
            opts.distributed_lock_timeout(),
            opts.factory_soft_timeout(),
            opts.factory_hard_timeout(),
            opts.distributed_soft_timeout(),
            opts.distributed_hard_timeout(),
        ] {
            validate_budget(timeout)?;
        }
        if self.options_require_runtime(opts, target)
            && tokio::runtime::Handle::try_current().is_err()
        {
            return Err(ConfigError::MissingRuntime {
                component: RuntimeComponent::Execution,
            }
            .into());
        }
        Ok(())
    }
    fn options_require_runtime(&self, opts: &EntryOptions, target: OptionsTarget) -> bool {
        let timers = [
            opts.memory_lock_timeout(),
            opts.factory_soft_timeout(),
            opts.factory_hard_timeout(),
            opts.distributed_soft_timeout(),
            opts.distributed_hard_timeout(),
        ]
        .into_iter()
        .any(|timeout| matches!(timeout,Timeout::After(duration) if !duration.is_zero()));
        let distributed = match target {
            OptionsTarget::Value => matches!(self.inner.storage, Storage::Hybrid { .. }),
            OptionsTarget::Marker => matches!(self.inner.markers, MarkerAccess::Durable(_)),
        };
        let background = opts.eager_refresh_threshold().is_some()
            || opts.allow_background_distributed_operations() && distributed
            || opts.allow_background_backplane_operations() && self.inner.backplane.is_some();
        timers
            || background
            || !opts.skip_distributed_locker() && self.inner.distributed_locker.is_some()
    }
    fn copy(&self, value: &V, opts: &EntryOptions) -> Result<V> {
        crate::serializers::copy_value(value, opts, self.inner.cloner.as_deref())
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
        Ok(JitterSample::new(
            self.inner.jitter.sample(options.jitter_max()),
            options.jitter_max(),
        )?)
    }
    fn emit(&self, event: CacheEvent) {
        self.inner.events.emit(event);
    }
    fn tags(&self, entry: &Entry<V>) -> TagVerdict {
        if self.inner.disable_tagging {
            TagVerdict::Valid
        } else {
            self.inner.tags.evaluate(
                entry.meta().created(),
                entry.meta().tags(),
                self.inner.remove_by_tag_behavior,
            )
        }
    }
    async fn read_l1(&self, key: &str, cancellation: &FactoryCancellation) -> Result<L1Read<V>> {
        let captured = self.inner.epoch.load(Ordering::Acquire);
        let now = self.inner.clock.now();
        let Some(entry) = self.inner.memory.get_at(key, now).await else {
            return Ok(L1Read::Miss);
        };
        self.reconcile_controlled_markers(&entry, cancellation)
            .await?;
        self.ensure_health();
        cancellation.check()?;
        let now = self.inner.clock.now();
        if self.inner.epoch.load(Ordering::Acquire) != captured
            || !entry.is_read_eligible()
            || entry.is_physically_expired(now)
        {
            return Ok(L1Read::Miss);
        }
        Ok(match self.tags(&entry) {
            TagVerdict::Remove => {
                self.inner.memory.remove_if_same(key, &entry).await;
                L1Read::Miss
            }
            TagVerdict::Expire => L1Read::Stale(entry),
            TagVerdict::Valid => {
                if entry.freshness(now).is_fresh() {
                    L1Read::Fresh(entry)
                } else {
                    L1Read::Stale(entry)
                }
            }
        })
    }

    fn marker_clear_shortcut(&self) -> bool {
        matches!(self.inner.storage, Storage::Hybrid { .. }) && self.inner.backplane.is_some()
    }

    fn marker_reads_ready(&self, tags: &[Tag], now: Timestamp) -> bool {
        if self.inner.disable_tagging
            || self.inner.marker_reads.policy() == MarkerReadPolicy::DurableRequired
            || matches!(self.inner.markers, MarkerAccess::Local)
        {
            return true;
        }
        if matches!(self.inner.markers, MarkerAccess::Unavailable) {
            return false;
        }
        let MarkerReads::OptionsControlled(observations) = &self.inner.marker_reads else {
            return true;
        };
        let options = &self.inner.tags_default_options;
        let epoch = self.inner.epoch.load(Ordering::Acquire);
        Self::secondary_marker_kinds(tags).all(|kind| {
            observations.ready(&kind, options, now, epoch, self.marker_clear_shortcut())
        })
    }

    fn marker_ready_events(&self, tags: &[Tag], now: Timestamp) {
        if self.inner.disable_tagging {
            return;
        }
        let MarkerReads::OptionsControlled(observations) = &self.inner.marker_reads else {
            return;
        };
        let epoch = self.inner.epoch.load(Ordering::Acquire);
        for kind in Self::secondary_marker_kinds(tags) {
            if let Some(outcome) = observations.ready_outcome(
                &kind,
                &self.inner.tags_default_options,
                now,
                epoch,
                self.marker_clear_shortcut(),
            ) {
                self.marker_event(&kind, outcome);
            }
        }
    }

    async fn reconcile_controlled_markers(
        &self,
        entry: &Entry<V>,
        cancellation: &FactoryCancellation,
    ) -> Result<()> {
        if self.inner.disable_tagging
            || self.inner.marker_reads.policy() == MarkerReadPolicy::DurableRequired
        {
            return Ok(());
        }
        let MarkerReads::OptionsControlled(observations) = &self.inner.marker_reads else {
            return Ok(());
        };
        let store = match &self.inner.markers {
            MarkerAccess::Local => return Ok(()),
            MarkerAccess::Unavailable => return Err(MarkerError::Unsupported.into()),
            MarkerAccess::Durable(store) => Arc::clone(store),
        };
        // Keep the optional controller's large future out of ordinary value
        // flights. Only participating controlled reads allocate this branch.
        Box::pin(self.check_observed_markers(entry, cancellation, observations, store)).await
    }

    async fn check_observed_markers(
        &self,
        entry: &Entry<V>,
        cancellation: &FactoryCancellation,
        observations: &MarkerObservations,
        store: Arc<dyn InvalidationStore>,
    ) -> Result<()> {
        // Independent deadlines are per marker, as in the released reference.
        for kind in Self::secondary_marker_kinds(entry.meta().tags()) {
            cancellation.check()?;
            self.read_control_marker(observations, Arc::clone(&store), kind.clone(), cancellation)
                .await?;
            if self.marker_invalidates_snapshot(&kind, entry.meta().created()) {
                break;
            }
        }
        cancellation.check()
    }

    fn secondary_marker_kinds(tags: &[Tag]) -> impl Iterator<Item = MarkerKind> + '_ {
        std::iter::once(MarkerKind::ClearRemove)
            .chain(tags.iter().cloned().map(MarkerKind::Tag))
            .chain(std::iter::once(MarkerKind::ClearExpire))
    }

    fn marker_invalidates_snapshot(&self, kind: &MarkerKind, created: Timestamp) -> bool {
        [kind, &MarkerKind::ClearRemove].into_iter().any(|kind| {
            self.inner
                .tags
                .marker_version(kind)
                .is_some_and(|version| created <= version.timestamp())
        })
    }

    async fn marker_cached(
        &self,
        observations: &MarkerObservations,
        kind: &MarkerKind,
    ) -> Option<Entry<MarkerObservation>> {
        if self.inner.tags_default_options.skip_memory_read() {
            None
        } else {
            observations
                .memory
                .get_at(&MarkerObservations::key(kind), self.inner.clock.now())
                .await
        }
    }

    fn marker_event(&self, kind: &MarkerKind, outcome: MarkerReadOutcome) {
        self.inner.events.emit_lazy(|| CacheEvent::MarkerRead {
            kind: kind.clone(),
            outcome,
        });
    }

    async fn read_control_marker(
        &self,
        observations: &MarkerObservations,
        store: Arc<dyn InvalidationStore>,
        kind: MarkerKind,
        cancellation: &FactoryCancellation,
    ) -> Result<()> {
        self.validate_execution_options(&self.inner.tags_default_options, OptionsTarget::Marker)?;
        let captured = self.inner.epoch.load(Ordering::Acquire);
        let outcome = match self.marker_lookup(observations, &kind, captured).await {
            MarkerLookup::Ready(outcome) => outcome,
            MarkerLookup::Refresh(cached) => {
                self.refresh_control_marker(
                    observations,
                    store,
                    &kind,
                    cached.as_ref(),
                    captured,
                    cancellation,
                )
                .await?
            }
        };
        cancellation.check()?;
        if self.marker_clear_shortcut() && self.inner.epoch.load(Ordering::Acquire) == captured {
            observations.initialize_clear(&kind, captured, outcome);
        }
        self.marker_event(&kind, outcome);
        Ok(())
    }

    async fn marker_lookup(
        &self,
        observations: &MarkerObservations,
        kind: &MarkerKind,
        captured: u64,
    ) -> MarkerLookup {
        if self.marker_clear_shortcut()
            && let Some(outcome) = observations.clear_status(kind, captured)
        {
            return MarkerLookup::Ready(outcome);
        }
        let cached = self.marker_cached(observations, kind).await;
        if let Some(entry) = &cached
            && entry.freshness(self.inner.clock.now()).is_fresh()
        {
            return MarkerLookup::Ready(entry.value().outcome());
        }
        MarkerLookup::Refresh(cached)
    }

    async fn refresh_control_marker(
        &self,
        observations: &MarkerObservations,
        store: Arc<dyn InvalidationStore>,
        kind: &MarkerKind,
        before_lock: Option<&Entry<MarkerObservation>>,
        captured: u64,
        cancellation: &FactoryCancellation,
    ) -> Result<MarkerReadOutcome> {
        let key = MarkerObservations::key(kind);
        let guard = bounded(
            self.marker_lock_timeout(before_lock),
            observations.locks.lock(&key),
        )
        .await?;
        cancellation.check()?;
        if guard.is_none() && self.marker_fallback_eligible(before_lock) {
            // The current owner will refresh this marker: a contending reader
            // uses its retained fact without extending or replacing its TTL.
            return Ok(MarkerReadOutcome::StaleFallback(
                MarkerReadFailure::LockTimeout,
            ));
        }
        self.resolve_control_read(observations, store, kind, captured, cancellation)
            .await
    }

    async fn resolve_control_read(
        &self,
        observations: &MarkerObservations,
        store: Arc<dyn InvalidationStore>,
        kind: &MarkerKind,
        captured: u64,
        cancellation: &FactoryCancellation,
    ) -> Result<MarkerReadOutcome> {
        let cached = self.marker_cached(observations, kind).await;
        let options = &self.inner.tags_default_options;
        if let Some(entry) = &cached
            && entry.freshness(self.inner.clock.now()).is_fresh()
        {
            return Ok(entry.value().outcome());
        }
        if options.skip_distributed_read()
            || cached.is_some() && options.skip_distributed_read_when_stale()
        {
            return Ok(MarkerReadOutcome::Skipped);
        }
        self.marker_remote(
            observations,
            store,
            kind,
            cached.as_ref(),
            captured,
            cancellation,
        )
        .await
    }

    fn marker_fallback_eligible(&self, cached: Option<&Entry<MarkerObservation>>) -> bool {
        self.inner.tags_default_options.is_fail_safe_enabled()
            && cached.is_some_and(|entry| {
                entry.is_read_eligible() && !entry.is_physically_expired(self.inner.clock.now())
            })
    }

    fn marker_lock_timeout(&self, cached: Option<&Entry<MarkerObservation>>) -> Timeout {
        let options = &self.inner.tags_default_options;
        if options.memory_lock_timeout().is_infinite() && self.marker_fallback_eligible(cached) {
            options.factory_soft_timeout()
        } else {
            options.memory_lock_timeout()
        }
    }

    async fn marker_remote(
        &self,
        observations: &MarkerObservations,
        store: Arc<dyn InvalidationStore>,
        kind: &MarkerKind,
        cached: Option<&Entry<MarkerObservation>>,
        captured: u64,
        cancellation: &FactoryCancellation,
    ) -> Result<MarkerReadOutcome> {
        let fetched = match &observations.lifecycle {
            MarkerLifecycleAccess::DurableOnly => {
                self.fetch_control_marker(
                    Arc::clone(&store),
                    kind.clone(),
                    cached.is_some(),
                    cancellation,
                )
                .await
            }
            MarkerLifecycleAccess::CachedSnapshots(cache) => {
                self.fetch_control_snapshot(
                    Arc::clone(cache),
                    kind.clone(),
                    cached.is_some(),
                    cancellation,
                )
                .await
            }
        };
        match fetched {
            Ok(MarkerFetch::Snapshot(snapshot)) => {
                self.resolve_control_snapshot(
                    observations,
                    kind,
                    cached,
                    snapshot,
                    captured,
                    cancellation,
                )
                .await
            }
            Ok(MarkerFetch::Observed(presence)) => {
                self.record_control_observation(observations, kind, presence, captured)
                    .await
            }
            Ok(MarkerFetch::Deadline(failure)) => {
                self.marker_degraded(observations, kind, cached, failure)
                    .await
            }
            Err(Error::Marker(error)) => {
                let failure = self.suppressed_marker_fault(kind, error)?.read_failure();
                self.marker_degraded(observations, kind, cached, failure)
                    .await
            }
            Err(error) => Err(error),
        }
    }

    async fn fetch_control_snapshot(
        &self,
        cache: Arc<dyn MarkerSnapshotCache>,
        kind: MarkerKind,
        has_fallback: bool,
        cancellation: &FactoryCancellation,
    ) -> Result<MarkerFetch> {
        let options = &self.inner.tags_default_options;
        let timeout = options.appropriate_distributed_timeout(has_fallback);
        let soft = options.is_fail_safe_enabled()
            && has_fallback
            && timeout == options.distributed_soft_timeout()
            && timeout != options.distributed_hard_timeout();
        let source = CancellationSource::new();
        let token = source.token();
        let scope = self.inner.scope.clone();
        let clock = Arc::clone(&self.inner.clock);
        let mut execution = self.inner.scopes.execution(
            async move {
                cache
                    .read_snapshot(&scope, &kind, clock.now(), token)
                    .await
                    .map(MarkerFetch::Snapshot)
                    .map_err(Error::from)
            },
            source,
        );
        execution.link(cancellation, LinkMode::Explicit);
        match bounded(timeout, &mut execution).await? {
            Some(result) => result,
            None => {
                execution.cancel(if soft {
                    Reason::SoftTimeout
                } else {
                    Reason::HardTimeout
                });
                Ok(MarkerFetch::Deadline(if soft {
                    MarkerReadFailure::SoftTimeout
                } else {
                    MarkerReadFailure::HardTimeout
                }))
            }
        }
    }

    async fn resolve_control_snapshot(
        &self,
        observations: &MarkerObservations,
        kind: &MarkerKind,
        cached: Option<&Entry<MarkerObservation>>,
        read: MarkerSnapshotRead,
        captured: u64,
        cancellation: &FactoryCancellation,
    ) -> Result<MarkerReadOutcome> {
        let (observation, stale) = match read {
            MarkerSnapshotRead::Snapshot(snapshot) => {
                self.apply_marker(StoredMarker::new(kind.clone(), snapshot.version()));
                if snapshot.is_fresh(self.inner.clock.now()) {
                    return self
                        .record_control_snapshot(observations, kind, snapshot, captured)
                        .await;
                }
                (
                    MarkerObservation::KnownMaximum(snapshot.version()),
                    MarkerFactoryStale::Distributed(snapshot),
                )
            }
            MarkerSnapshotRead::Missing { maximum } => {
                if let Some(version) = maximum {
                    self.apply_marker(StoredMarker::new(kind.clone(), version));
                }
                let presence = maximum.map_or(MarkerPresence::Absent, MarkerPresence::Present);
                (
                    MarkerObservation::Confirmed(presence),
                    cached.map_or(MarkerFactoryStale::Absent, MarkerFactoryStale::Memory),
                )
            }
        };
        let observation = observation.reconcile_maximum(self.inner.tags.marker_version(kind));
        self.complete_control_marker_factory(
            observations,
            kind,
            observation,
            stale,
            captured,
            cancellation,
        )
        .await
    }

    async fn record_control_snapshot(
        &self,
        observations: &MarkerObservations,
        kind: &MarkerKind,
        snapshot: MarkerSnapshot,
        captured: u64,
    ) -> Result<MarkerReadOutcome> {
        let observation = MarkerObservation::Confirmed(MarkerPresence::Present(snapshot.version()))
            .reconcile_maximum(self.inner.tags.marker_version(kind));
        let observation = observations
            .store_snapshot(
                kind,
                observation,
                snapshot,
                &self.inner.tags_default_options,
                self.inner.clock.now(),
                ContinuityStamp::new(Arc::clone(&self.inner.epoch), captured),
            )
            .await?;
        Ok(observation.observed_outcome())
    }

    async fn complete_control_marker_factory(
        &self,
        observations: &MarkerObservations,
        kind: &MarkerKind,
        observation: MarkerObservation,
        stale: MarkerFactoryStale<'_>,
        captured: u64,
        cancellation: &FactoryCancellation,
    ) -> Result<MarkerReadOutcome> {
        cancellation.check()?;
        let options = &self.inner.tags_default_options;
        if let Some(failure) = self.excluded_marker_factory(stale)? {
            return self
                .retain_excluded_marker_factory(
                    observations,
                    kind,
                    observation.presence(),
                    stale,
                    captured,
                    failure,
                )
                .await;
        }
        // The shared factory is an immediate selection, not remote I/O. Positive
        // factory budgets cannot time out this total, allocation-free decision.
        let observation = observation.reconcile_maximum(self.inner.tags.marker_version(kind));
        let observation = observations
            .store(
                kind,
                observation,
                options,
                self.inner.clock.now(),
                self.jitter_sample(options)?,
                ContinuityStamp::new(Arc::clone(&self.inner.epoch), captured),
            )
            .await?;
        if let MarkerPresence::Present(version) = observation.presence()
            && !options.skip_distributed_write()
        {
            self.write_control_snapshot(
                kind.clone(),
                MarkerSnapshot::fresh(version, options, self.inner.clock.now()),
                captured,
                cancellation,
            )
            .await?;
        }
        cancellation.check()?;
        Ok(observation
            .reconcile_maximum(self.inner.tags.marker_version(kind))
            .observed_outcome())
    }

    fn marker_factory_fallback_eligible(&self, stale: MarkerFactoryStale<'_>) -> bool {
        self.inner.tags_default_options.is_fail_safe_enabled()
            && match stale {
                MarkerFactoryStale::Memory(cached) => self.marker_fallback_eligible(Some(cached)),
                MarkerFactoryStale::Distributed(snapshot) => {
                    !snapshot.is_physically_expired(self.inner.clock.now())
                }
                MarkerFactoryStale::Absent => false,
            }
    }

    fn excluded_marker_factory(
        &self,
        stale: MarkerFactoryStale<'_>,
    ) -> Result<Option<MarkerReadFailure>> {
        let options = &self.inner.tags_default_options;
        let has_fallback = self.marker_factory_fallback_eligible(stale);
        let timeout = options.appropriate_factory_timeout(has_fallback);
        if !timeout
            .as_duration()
            .is_some_and(|duration| duration.is_zero())
        {
            return Ok(None);
        }
        if !options.is_fail_safe_enabled() {
            return Err(Error::FactoryTimeout {
                elapsed: Duration::ZERO,
            });
        }
        Ok(Some(
            if has_fallback
                && timeout == options.factory_soft_timeout()
                && timeout != options.factory_hard_timeout()
            {
                MarkerReadFailure::FactorySoftTimeout
            } else {
                MarkerReadFailure::FactoryHardTimeout
            },
        ))
    }

    async fn retain_excluded_marker_factory(
        &self,
        observations: &MarkerObservations,
        kind: &MarkerKind,
        presence: MarkerPresence,
        stale: MarkerFactoryStale<'_>,
        captured: u64,
        failure: MarkerReadFailure,
    ) -> Result<MarkerReadOutcome> {
        match stale {
            MarkerFactoryStale::Distributed(snapshot)
                if self.marker_factory_fallback_eligible(stale) =>
            {
                observations
                    .retain_snapshot_fallback(
                        kind,
                        snapshot,
                        presence,
                        &self.inner.tags_default_options,
                        self.inner.clock.now(),
                        failure,
                        ContinuityStamp::new(Arc::clone(&self.inner.epoch), captured),
                    )
                    .await?;
                Ok(MarkerReadOutcome::StaleFallback(failure))
            }
            MarkerFactoryStale::Memory(cached) => {
                self.marker_degraded(observations, kind, Some(cached), failure)
                    .await
            }
            MarkerFactoryStale::Distributed(_) | MarkerFactoryStale::Absent => {
                Ok(MarkerReadOutcome::Unavailable(failure))
            }
        }
    }

    async fn write_control_snapshot(
        &self,
        kind: MarkerKind,
        snapshot: MarkerSnapshot,
        captured: u64,
        cancellation: &FactoryCancellation,
    ) -> Result<()> {
        let MarkerReads::OptionsControlled(observations) = &self.inner.marker_reads else {
            return Ok(());
        };
        let MarkerLifecycleAccess::CachedSnapshots(cache) = &observations.lifecycle else {
            return Ok(());
        };
        let source = CancellationSource::new();
        let token = source.token();
        let worker = self.clone();
        let cache = Arc::clone(cache);
        let key = MarkerObservations::key(&kind);
        let execution = self.inner.scopes.execution(
            async move {
                worker
                    .commit_control_snapshot(cache, kind, snapshot, captured, token)
                    .await
            },
            source,
        );
        let background = self
            .inner
            .tags_default_options
            .allow_background_distributed_operations();
        execution.link(
            cancellation,
            if background {
                LinkMode::CallerScope
            } else {
                LinkMode::Explicit
            },
        );
        if background {
            let _completion = self.inner.tasks.spawn(
                ShutdownTask::Distributed,
                key,
                self.inner.events.clone(),
                execution,
            );
            Ok(())
        } else {
            execution.await
        }
    }

    async fn commit_control_snapshot(
        &self,
        cache: Arc<dyn MarkerSnapshotCache>,
        kind: MarkerKind,
        snapshot: MarkerSnapshot,
        captured: u64,
        cancellation: FactoryCancellation,
    ) -> Result<()> {
        cancellation.check()?;
        if self.inner.epoch.load(Ordering::Acquire) != captured {
            return Ok(());
        }
        // Like the reference Set, this owned write has no distributed-read
        // timeout. Provider I/O bounds and explicit cancellation still apply.
        let result = cache
            .renew_snapshot(
                &self.inner.scope,
                &kind,
                snapshot,
                self.inner.clock.now(),
                cancellation.clone(),
            )
            .await
            .map_err(Error::from);
        cancellation.check()?;
        let outcome = match result {
            Ok(renewal) => {
                let (snapshot, outcome) = match renewal {
                    MarkerSnapshotRenewal::Stored(snapshot) => {
                        (Some(snapshot), crate::MarkerSnapshotWriteOutcome::Stored)
                    }
                    MarkerSnapshotRenewal::KeptNewer(snapshot) => {
                        (Some(snapshot), crate::MarkerSnapshotWriteOutcome::KeptNewer)
                    }
                    MarkerSnapshotRenewal::Expired => {
                        (None, crate::MarkerSnapshotWriteOutcome::Expired)
                    }
                };
                if let Some(snapshot) = snapshot {
                    self.apply_marker(StoredMarker::new(kind.clone(), snapshot.version()));
                    if !self.inner.tags_default_options.skip_memory_write()
                        && let MarkerReads::OptionsControlled(observations) =
                            &self.inner.marker_reads
                    {
                        // Set does not hydrate/shorten the just-created L1
                        // lifetime. A higher returned fact only raises its value.
                        observations
                            .merge_maximum(
                                &kind,
                                snapshot.version(),
                                self.inner.clock.now(),
                                ContinuityStamp::new(Arc::clone(&self.inner.epoch), captured),
                            )
                            .await;
                    }
                }
                outcome
            }
            Err(Error::Marker(error)) => {
                self.suppressed_marker_fault(&kind, error)?.write_outcome()
            }
            Err(error) => return Err(error),
        };
        self.inner
            .events
            .emit_lazy(|| CacheEvent::MarkerSnapshotWrite { kind, outcome });
        Ok(())
    }

    async fn record_control_observation(
        &self,
        observations: &MarkerObservations,
        kind: &MarkerKind,
        presence: MarkerPresence,
        captured: u64,
    ) -> Result<MarkerReadOutcome> {
        if let MarkerPresence::Present(version) = presence {
            self.apply_marker(StoredMarker::new(kind.clone(), version));
        }
        let options = &self.inner.tags_default_options;
        let observation = observations
            .store(
                kind,
                MarkerObservation::Confirmed(presence)
                    .reconcile_maximum(self.inner.tags.marker_version(kind)),
                options,
                self.inner.clock.now(),
                self.jitter_sample(options)?,
                ContinuityStamp::new(Arc::clone(&self.inner.epoch), captured),
            )
            .await?;
        Ok(observation.observed_outcome())
    }

    fn suppressed_marker_fault(
        &self,
        kind: &MarkerKind,
        error: MarkerError,
    ) -> Result<MarkerProviderFault> {
        let options = &self.inner.tags_default_options;
        let failure = match &error {
            MarkerError::Backend { .. } if !options.rethrow_distributed_exceptions() => {
                MarkerProviderFault::Backend
            }
            MarkerError::Protocol { .. } | MarkerError::ProtocolWithSource { .. }
                if !options.rethrow_serialization_exceptions() =>
            {
                MarkerProviderFault::Protocol
            }
            MarkerError::Unsupported
            | MarkerError::BlankWireVersion
            | MarkerError::ZeroCapacity
            | MarkerError::ScopeCapacity { .. }
            | MarkerError::Backend { .. }
            | MarkerError::Protocol { .. }
            | MarkerError::ProtocolWithSource { .. } => return Err(error.into()),
        };
        tracing::warn!(%error, ?kind, "marker provider uses explicitly selected degradation policy");
        Ok(failure)
    }

    async fn marker_degraded(
        &self,
        observations: &MarkerObservations,
        kind: &MarkerKind,
        cached: Option<&Entry<MarkerObservation>>,
        failure: MarkerReadFailure,
    ) -> Result<MarkerReadOutcome> {
        if self.inner.tags_default_options.is_fail_safe_enabled()
            && let Some(source) = cached
            && source.is_read_eligible()
            && !source.is_physically_expired(self.inner.clock.now())
        {
            observations
                .retain_fallback(
                    kind,
                    source,
                    &self.inner.tags_default_options,
                    self.inner.clock.now(),
                    failure,
                )
                .await?;
            Ok(MarkerReadOutcome::StaleFallback(failure))
        } else {
            Ok(MarkerReadOutcome::Unavailable(failure))
        }
    }

    async fn fetch_control_marker(
        &self,
        store: Arc<dyn InvalidationStore>,
        kind: MarkerKind,
        has_fallback: bool,
        cancellation: &FactoryCancellation,
    ) -> Result<MarkerFetch> {
        let options = &self.inner.tags_default_options;
        let timeout = options.appropriate_distributed_timeout(has_fallback);
        let soft = options.is_fail_safe_enabled()
            && has_fallback
            && timeout == options.distributed_soft_timeout()
            && timeout != options.distributed_hard_timeout();
        let source = CancellationSource::new();
        let phase = source.token();
        let scope = self.inner.scope.clone();
        let mut execution = self.inner.scopes.execution(
            async move {
                store
                    .read_with_cancellation(&scope, &kind, phase)
                    .await
                    .map_err(Error::from)
            },
            source,
        );
        execution.link(cancellation, LinkMode::Explicit);
        match bounded(timeout, &mut execution).await? {
            Some(result) => result.map(|version| {
                MarkerFetch::Observed(match version {
                    Some(version) => MarkerPresence::Present(version),
                    None => MarkerPresence::Absent,
                })
            }),
            None => {
                execution.cancel(if soft {
                    Reason::SoftTimeout
                } else {
                    Reason::HardTimeout
                });
                Ok(MarkerFetch::Deadline(if soft {
                    MarkerReadFailure::SoftTimeout
                } else {
                    MarkerReadFailure::HardTimeout
                }))
            }
        }
    }
    async fn reconcile_markers(&self, tags: &[Tag]) -> Result<()> {
        if self.inner.disable_tagging {
            return Ok(());
        }
        if let MarkerAccess::Durable(store) = &self.inner.markers {
            let mut kinds = Vec::with_capacity(tags.len() + 2);
            kinds.push(MarkerKind::ClearRemove);
            kinds.push(MarkerKind::ClearExpire);
            kinds.extend(tags.iter().cloned().map(MarkerKind::Tag));
            for marker in store.read_many(&self.inner.scope, &kinds).await? {
                self.apply_marker(marker);
            }
        }
        Ok(())
    }
    fn apply_marker(&self, marker: StoredMarker) {
        // A delayed durable/control marker cannot evict a snapshot created after
        // that marker. Reads apply the registry verdict to each exact snapshot.
        self.inner
            .tags
            .advance(marker.kind().clone(), marker.version());
    }

    async fn seed_marker(&self, marker: &StoredMarker, options: &EntryOptions) -> Result<()> {
        let MarkerReads::OptionsControlled(observations) = &self.inner.marker_reads else {
            return Ok(());
        };
        let captured = self.inner.epoch.load(Ordering::Acquire);
        let observation = observations
            .store(
                marker.kind(),
                MarkerObservation::Confirmed(MarkerPresence::Present(marker.version()))
                    .reconcile_maximum(self.inner.tags.marker_version(marker.kind())),
                options,
                self.inner.clock.now(),
                self.jitter_sample(options)?,
                ContinuityStamp::new(Arc::clone(&self.inner.epoch), captured),
            )
            .await?;
        if self.marker_clear_shortcut() && self.inner.epoch.load(Ordering::Acquire) == captured {
            observations.initialize_clear(marker.kind(), captured, observation.outcome());
        }
        Ok(())
    }
    fn circuit(&self, component: CircuitComponent) -> bool {
        let circuit = match component {
            CircuitComponent::Distributed => &self.inner.circuit_l2,
            CircuitComponent::Backplane => &self.inner.circuit_backplane,
        };
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
    async fn read_l2(
        &self,
        key: &Arc<str>,
        opts: &EntryOptions,
        fallback: FallbackAvailability,
        policy: L2ReadPolicy,
        cancellation: &FactoryCancellation,
    ) -> Result<Option<DistributedLookup<V>>> {
        if matches!(self.inner.storage, Storage::MemoryOnly) {
            return Ok(None);
        }
        if opts.skip_distributed_read() {
            return Ok(None);
        }
        if !self.circuit(CircuitComponent::Distributed) {
            let error = Error::CircuitOpen {
                component: CircuitComponent::Distributed,
            };
            return match policy {
                L2ReadPolicy::PreserveFailure => Err(error),
                L2ReadPolicy::FactoryFallback => {
                    tracing::warn!(%error,key=%key,"legacy/origin lookup skipped an open circuit");
                    Ok(None)
                }
            };
        }
        let timeout = opts
            .appropriate_distributed_timeout(matches!(fallback, FallbackAvailability::Available));
        let span = component_span(&self.inner.name, CacheLevel::Distributed, "read", Some(key));
        let source = CancellationSource::new();
        let phase_cancellation = source.token();
        let worker = self.clone();
        let phase_key = Arc::clone(key);
        let mut execution = self.inner.scopes.execution(
            async move { worker.fetch_l2(&phase_key, &phase_cancellation).await }.instrument(span),
            source,
        );
        execution.link(cancellation, LinkMode::Explicit);
        let soft = opts.is_fail_safe_enabled()
            && matches!(fallback, FallbackAvailability::Available)
            && timeout == opts.distributed_soft_timeout()
            && timeout != opts.distributed_hard_timeout();
        // Value retrieval, decoding and the required durable marker lookup are
        // one read. A marker provider cannot escape the caller's chosen budget.
        // Borrow execution through the deadline wrapper so we publish the exact
        // phase reason before dropping its codec, independently of the caller.
        let result = match bounded(timeout, &mut execution).await {
            Ok(Some(result)) => result,
            Ok(None) => {
                execution.cancel(if soft {
                    Reason::SoftTimeout
                } else {
                    Reason::HardTimeout
                });
                if policy == L2ReadPolicy::FactoryFallback
                    && opts.is_fail_safe_enabled()
                    && matches!(fallback, FallbackAvailability::Available)
                    && timeout == opts.distributed_soft_timeout()
                {
                    Ok(None)
                } else {
                    Err(Error::Distributed(
                        "distributed read deadline elapsed".into(),
                    ))
                }
            }
            Err(error) => Err(error),
        };
        match result {
            Ok(value) => {
                if let Some(entry) = &value {
                    self.reconcile_controlled_markers(&entry.entry, cancellation)
                        .await?;
                }
                Ok(value)
            }
            Err(error) => {
                self.failure(key, &error);
                if policy.rethrow(opts, &error) {
                    Err(error)
                } else {
                    tracing::warn!(%error,key=%key,"distributed read degraded to miss");
                    Ok(None)
                }
            }
        }
    }
    async fn fetch_l2(
        &self,
        key: &Arc<str>,
        cancellation: &FactoryCancellation,
    ) -> Result<Option<DistributedLookup<V>>> {
        let Storage::Hybrid {
            backend,
            serializer,
        } = &self.inner.storage
        else {
            return Ok(None);
        };
        cancellation.check()?;
        let hydration = self.hydration_fence(key).await;
        let bytes = backend.get(&self.inner.l2_key(key)).await?;
        self.close_circuit(CircuitComponent::Distributed);
        let Some(bytes) = bytes else { return Ok(None) };
        let snapshot = serializer
            .decode(&bytes, self.inner.serialization_mode, cancellation)
            .await?;
        let source = snapshot.try_into_entry(self.inner.clock.now())?;
        if source.is_physically_expired(self.inner.clock.now()) {
            return Ok(None);
        }
        if self.inner.marker_reads.policy() == MarkerReadPolicy::DurableRequired {
            self.reconcile_markers(source.meta().tags()).await?;
        }
        Ok(Some(DistributedLookup {
            entry: source,
            hydration,
        }))
    }
    async fn hydration_fence(&self, key: &str) -> HydrationFence<V> {
        let lane = self.inner.lanes.get(key);
        let fence = {
            let Ok(_guard) = Arc::clone(&lane.lock).try_lock_owned() else {
                // A read overlapping an already started commit may return its
                // snapshot, but cannot install it after that commit completes.
                return HydrationFence::ConcurrentMutation;
            };
            lane.snapshot(&self.inner.epoch)
        };
        let observed = self.inner.memory.get_at(key, self.inner.clock.now()).await;
        HydrationFence::Stable { fence, observed }
    }
    async fn hydrate(
        &self,
        key: &Arc<str>,
        source: &DistributedLookup<V>,
        opts: &EntryOptions,
    ) -> Result<HydrationOutcome> {
        if opts.skip_memory_write() {
            return Ok(HydrationOutcome::Skipped(SkipReason::Policy));
        }
        let HydrationFence::Stable { fence, observed } = &source.hydration else {
            return Ok(HydrationOutcome::Skipped(SkipReason::Superseded));
        };
        let Ok(_guard) = Arc::clone(&fence.lane.lock).try_lock_owned() else {
            // Hydration is optional. A newer mutation already owning this lane
            // must not delay the read or install an older snapshot afterward.
            return Ok(HydrationOutcome::Skipped(SkipReason::Superseded));
        };
        let verdict = self.tags(&source.entry);
        if !fence.passive_is_current()
            || verdict == TagVerdict::Remove
            || observed.as_ref().is_some_and(|entry| {
                entry.meta().created() > source.entry.meta().created()
                    || (entry.meta().created() == source.entry.meta().created()
                        && (verdict == TagVerdict::Expire
                            || source.entry.is_logically_expired(self.inner.clock.now())))
            })
        {
            return Ok(HydrationOutcome::Skipped(SkipReason::Superseded));
        }
        let Some(local) = source
            .entry
            .for_memory_hydration(opts, self.inner.clock.now())?
        else {
            return Ok(HydrationOutcome::Skipped(SkipReason::PhysicallyExpired));
        };
        let local =
            local.with_hydrated_value(self.copy(local.value(), opts)?, fence.continuity_stamp());
        if !fence.passive_is_current() {
            return Ok(HydrationOutcome::Skipped(SkipReason::Superseded));
        }
        Ok(HydrationOutcome::Evaluated(
            self.inner
                .memory
                .insert_if_unchanged(
                    Arc::clone(key),
                    observed.as_ref(),
                    local,
                    self.inner.clock.now(),
                )
                .await,
        ))
    }
    async fn read(
        &self,
        key: LookupKey,
        options: Option<EntryOptions>,
        policy: L2ReadPolicy,
        cancellation: &FactoryCancellation,
    ) -> Result<Observed<MaybeValue<V>>> {
        let opts = self.resolve_options(&key.raw, options)?;
        let key = key.full;
        self.ensure_health();
        let mut stale = None;
        if !opts.skip_memory_read() {
            match self.read_l1(&key, cancellation).await? {
                L1Read::Fresh(entry) => {
                    return self.read_hit(key, entry, &opts, HitKind::Fresh, CacheLevel::Memory);
                }
                L1Read::Stale(entry) => stale = Some(ReadStale::Memory(entry)),
                L1Read::Miss => {}
            }
        }
        if !(stale.is_some() && opts.skip_distributed_read_when_stale())
            && let Some(entry) = self
                .read_l2(
                    &key,
                    &opts,
                    if stale.is_some() {
                        FallbackAvailability::Available
                    } else {
                        FallbackAvailability::Unavailable
                    },
                    policy,
                    cancellation,
                )
                .await?
        {
            match self.tags(&entry.entry) {
                TagVerdict::Remove => {}
                verdict => {
                    self.hydrate(&key, &entry, &opts).await?.observe();
                    if verdict == TagVerdict::Valid
                        && entry.entry.freshness(self.inner.clock.now()).is_fresh()
                    {
                        return self.read_hit(
                            key,
                            entry.entry,
                            &opts,
                            HitKind::Fresh,
                            CacheLevel::Distributed,
                        );
                    }
                    stale = Some(match stale {
                        Some(previous)
                            if previous.entry().meta().created()
                                >= entry.entry.meta().created() =>
                        {
                            previous
                        }
                        Some(_) | None => ReadStale::Distributed(entry.entry),
                    });
                }
            }
        }
        if opts.allow_stale_on_read_only()
            && let Some(stale) = stale
            && (policy == L2ReadPolicy::FactoryFallback
                || (!stale.entry().is_physically_expired(self.inner.clock.now())
                    && self.tags(stale.entry()) != TagVerdict::Remove))
        {
            let (entry, level) = stale.into_parts();
            return self.read_hit(key, entry, &opts, HitKind::Stale, level);
        }
        self.emit(CacheEvent::Miss { key });
        Ok(Observed::new(
            MaybeValue::none(),
            OperationOutcome::Miss,
            None,
        ))
    }
    fn read_hit(
        &self,
        key: Arc<str>,
        entry: Entry<V>,
        opts: &EntryOptions,
        kind: HitKind,
        level: CacheLevel,
    ) -> Result<Observed<MaybeValue<V>>> {
        let value = self.copy(entry.value(), opts)?;
        self.emit(CacheEvent::Hit {
            key,
            stale: kind.is_stale(),
        });
        Ok(Observed::new(
            MaybeValue::from_value(value),
            kind.outcome(),
            Some(level),
        ))
    }
    fn served(
        &self,
        key: Arc<str>,
        entry: &Entry<V>,
        opts: &EntryOptions,
        level: CacheLevel,
    ) -> Result<Observed<CacheValue<V>>> {
        let value = self.copy(entry.value(), opts)?;
        self.emit(CacheEvent::Hit { key, stale: false });
        Ok(Observed::new(
            CacheValue {
                value,
                commit: CommitReceipt::Unchanged,
            },
            OperationOutcome::Hit,
            Some(level),
        ))
    }
    async fn acquire_lock(
        &self,
        key: &Arc<str>,
        opts: &EntryOptions,
        stale: Option<&Entry<V>>,
    ) -> Result<LockOutcome<V>> {
        let timeout = if opts.memory_lock_timeout().is_infinite()
            && opts.is_fail_safe_enabled()
            && stale.is_some()
        {
            opts.factory_soft_timeout()
        } else {
            opts.memory_lock_timeout()
        };
        let local = match bounded(timeout, self.inner.locks.lock(key)).await? {
            Some(local) => LocalParticipation::Held(local),
            None => {
                if opts.is_fail_safe_enabled()
                    && let Some(stale) = stale
                {
                    self.emit(CacheEvent::Hit {
                        key: Arc::clone(key),
                        stale: true,
                    });
                    return Ok(LockOutcome::Served(self.copy(stale.value(), opts)?));
                }
                LocalParticipation::UnlockedAfterTimeout
            }
        };
        let lease = if opts.skip_distributed_locker() {
            None
        } else if let Some(locker) = &self.inner.distributed_locker {
            let policy = match self.inner.lease_policy {
                LeasePolicy::Fenced => AcquisitionPolicy::TokenOwned,
                LeasePolicy::CooperativeLegacy => AcquisitionPolicy::LegacyBackendContract,
            };
            let lock_key: Arc<str> = Arc::from(format!("amalgam:lock:{}", self.inner.l2_key(key)));
            match acquire_owned_supervised(
                Arc::clone(locker),
                lock_key,
                self.inner.lease_ttl,
                opts.distributed_lock_timeout(),
                policy,
                self.lease_owner(key),
            )
            .await
            {
                Ok(Some(lease)) => Some(lease),
                Ok(None) => {
                    return Err(Error::LockTimeout {
                        elapsed: opts
                            .distributed_lock_timeout()
                            .as_duration()
                            .unwrap_or(Duration::ZERO),
                    });
                }
                Err(error)
                    if opts.rethrow_distributed_locker_exceptions()
                        || self.inner.lease_policy == LeasePolicy::Fenced =>
                {
                    return Err(error.into());
                }
                Err(error) => {
                    tracing::warn!(%error,"explicit cooperative locker degradation");
                    None
                }
            }
        } else {
            None
        };
        let unlocked = matches!(local, LocalParticipation::UnlockedAfterTimeout);
        let guard = FlightGuard {
            local,
            lease,
            tasks: Arc::clone(&self.inner.tasks),
            events: self.inner.events.clone(),
            key: Arc::clone(key),
            policy: self.inner.lease_policy,
        };
        Ok(if unlocked {
            LockOutcome::UnlockedAfterTimeout(guard)
        } else {
            LockOutcome::Acquired(guard)
        })
    }
    async fn get_or_set<F, Fut>(
        &self,
        key: LookupKey,
        factory: F,
        options: Option<EntryOptions>,
        tags: Box<[Tag]>,
        default: MaybeValue<V>,
        caller: FactoryCancellation,
    ) -> Result<Observed<CacheValue<V>>>
    where
        F: FnOnce(FactoryContext<V>) -> Fut + Send + 'static,
        Fut: Future<Output = std::result::Result<FactoryProduct<V>, FactoryError>> + Send + 'static,
    {
        let opts = self.resolve_options(&key.raw, options)?;
        let raw_key = key.raw;
        let key = key.full;
        self.ensure_health();
        let mut stale = None;
        if !opts.skip_memory_read() {
            match self.read_l1(&key, &caller).await? {
                L1Read::Fresh(entry) => {
                    if entry.should_eager_refresh(self.inner.clock.now()) {
                        self.eager(
                            LookupKey {
                                raw: raw_key,
                                full: Arc::clone(&key),
                            },
                            opts.clone(),
                            entry.clone(),
                            tags,
                            factory,
                        );
                    }
                    return self.served(key, &entry, &opts, CacheLevel::Memory);
                }
                L1Read::Stale(entry) => stale = Some(entry),
                L1Read::Miss => {}
            }
        }
        self.emit(CacheEvent::Miss {
            key: Arc::clone(&key),
        });
        let guard = match self.acquire_lock(&key, &opts, stale.as_ref()).await? {
            LockOutcome::Acquired(guard) | LockOutcome::UnlockedAfterTimeout(guard) => guard,
            LockOutcome::Served(value) => {
                return Ok(Observed::new(
                    CacheValue {
                        value,
                        commit: CommitReceipt::Unchanged,
                    },
                    OperationOutcome::StaleHit,
                    Some(CacheLevel::Memory),
                ));
            }
        };
        if !opts.skip_memory_read() {
            match self.read_l1(&key, &caller).await? {
                L1Read::Fresh(entry) => {
                    return self.served(key, &entry, &opts, CacheLevel::Memory);
                }
                L1Read::Stale(entry) => stale = Some(entry),
                L1Read::Miss => {}
            }
        }
        if !(stale.is_some() && opts.skip_distributed_read_when_stale())
            && let Some(entry) = self
                .read_l2(
                    &key,
                    &opts,
                    if stale.is_some() || default.has_value() {
                        FallbackAvailability::Available
                    } else {
                        FallbackAvailability::Unavailable
                    },
                    L2ReadPolicy::FactoryFallback,
                    &caller,
                )
                .await?
        {
            let verdict = self.tags(&entry.entry);
            if verdict != TagVerdict::Remove {
                self.hydrate(&key, &entry, &opts).await?.observe();
                if verdict == TagVerdict::Valid
                    && entry.entry.freshness(self.inner.clock.now()).is_fresh()
                {
                    return self.served(key, &entry.entry, &opts, CacheLevel::Distributed);
                }
                stale = Some(newer_of(stale, entry.entry));
            }
        }
        let timeout = opts.appropriate_factory_timeout(stale.is_some() || default.has_value());
        let soft = opts.is_fail_safe_enabled()
            && (stale.is_some() || default.has_value())
            && timeout == opts.factory_soft_timeout()
            && timeout != opts.factory_hard_timeout();
        if matches!(timeout,Timeout::After(duration) if duration.is_zero()) {
            drop(guard);
            return self
                .timeout_fallback(&key, &opts, stale.as_ref(), &default, timeout)
                .await;
        }
        let source = CancellationSource::new();
        let origin_cancellation = source.token();
        let ctx = FactoryContext::with_cancellation(
            LookupKey {
                raw: raw_key,
                full: Arc::clone(&key),
            },
            opts.clone(),
            tags,
            stale
                .as_ref()
                .map(|entry| self.stale_info(entry, &opts))
                .transpose()?,
            origin_cancellation.clone(),
        );
        let started = self.inner.clock.now();
        let worker = self.clone();
        let flight_key = Arc::clone(&key);
        let cancelled = source.clone();
        let lease_state = guard.lease.as_ref().map(DistributedLease::state);
        let mut execution=self.inner.scopes.execution(async move {
            let origin=async move {factory(ctx).await};
            let product=if let Some(mut state)=lease_state {tokio::select! {biased; ()=lease_lost(&mut state)=>{cancelled.cancel_with(Reason::LeaseLost);return Err(Error::FactoryCancelled {reason:Reason::LeaseLost});},product=origin=>product}}else{origin.await};
            let product=product.map_err(Error::from)?;
            let result=worker.store_product(flight_key,product,started,guard,&origin_cancellation).await;
            if matches!(&result,Err(Error::Lease(LeaseError::Lost))){cancelled.cancel_with(Reason::LeaseLost);}result
        }.instrument(component_span(&self.inner.name,CacheLevel::Origin,"factory",Some(&key))),source);
        execution.link(&caller, LinkMode::CallerScope);
        let result = match timeout {
            Timeout::Infinite => Some(execution.await),
            Timeout::After(duration) => {
                tokio::select! {biased;result=&mut execution=>Some(result),()=tokio::time::sleep(duration)=>{
                    self.emit(CacheEvent::FactorySyntheticTimeout {key:Arc::clone(&key)});
                    if opts.allow_timed_out_factory_background_completion(){self.background_origin(Arc::clone(&key),execution);}else{execution.cancel(if soft {Reason::SoftTimeout}else{Reason::HardTimeout});}
                    None
                }}
            }
        };
        match result {
            Some(Ok(value)) => Ok(Observed::new(
                value,
                OperationOutcome::Stored,
                Some(CacheLevel::Origin),
            )),
            Some(Err(error))
                if matches!(
                    &error,
                    Error::Factory { .. } | Error::FactoryWithSource { .. }
                ) =>
            {
                self.emit(CacheEvent::FactoryError {
                    key: Arc::clone(&key),
                    message: error.to_string(),
                });
                if let Some(value) = self.fallback(&key, &opts, stale.as_ref(), &default).await? {
                    Ok(Observed::new(
                        CacheValue {
                            value,
                            commit: CommitReceipt::Unchanged,
                        },
                        OperationOutcome::StaleHit,
                        Some(CacheLevel::Memory),
                    ))
                } else {
                    Err(error)
                }
            }
            Some(Err(error)) => Err(error),
            None => {
                self.timeout_fallback(&key, &opts, stale.as_ref(), &default, timeout)
                    .await
            }
        }
    }
    async fn timeout_fallback(
        &self,
        key: &Arc<str>,
        opts: &EntryOptions,
        stale: Option<&Entry<V>>,
        default: &MaybeValue<V>,
        timeout: Timeout,
    ) -> Result<Observed<CacheValue<V>>> {
        if let Some(value) = self.fallback(key, opts, stale, default).await? {
            Ok(Observed::new(
                CacheValue {
                    value,
                    commit: CommitReceipt::Unchanged,
                },
                OperationOutcome::StaleHit,
                Some(CacheLevel::Memory),
            ))
        } else {
            Err(Error::FactoryTimeout {
                elapsed: timeout.as_duration().unwrap_or(Duration::ZERO),
            })
        }
    }
    async fn fallback(
        &self,
        key: &Arc<str>,
        opts: &EntryOptions,
        stale: Option<&Entry<V>>,
        default: &MaybeValue<V>,
    ) -> Result<Option<V>> {
        if !opts.is_fail_safe_enabled() {
            return Ok(None);
        }
        let now = self.inner.clock.now();
        let entry = if let Some(stale) = stale {
            Entry::try_throttled(stale, opts, now)?
        } else {
            None
        };
        let entry = match entry {
            Some(entry) => Some(entry),
            None => default
                .value()
                .map(|value| {
                    self.copy(value, opts)
                        .and_then(|value| Entry::try_from_fail_safe_default(value, opts, now))
                })
                .transpose()?,
        };
        let Some(entry) = entry else {
            return Ok(None);
        };
        let value = self.copy(entry.value(), opts)?;
        if !opts.skip_memory_write() {
            self.inner
                .memory
                .insert_at(Arc::clone(key), entry, now)
                .await;
        }
        self.emit(CacheEvent::FailSafeActivate {
            key: Arc::clone(key),
        });
        self.emit(CacheEvent::Hit {
            key: Arc::clone(key),
            stale: true,
        });
        Ok(Some(value))
    }
    fn background_origin(&self, key: Arc<str>, execution: Execution<CacheValue<V>>) {
        let worker = self.clone();
        let success = Arc::clone(&key);
        let _receiver = self.inner.tasks.spawn(
            ShutdownTask::Factory,
            key,
            self.inner.events.clone(),
            async move {
                let result = execution.await?;
                worker.emit(CacheEvent::BackgroundFactorySuccess { key: success });
                drop(result);
                Ok(())
            },
        );
    }
    fn eager<F, Fut>(
        &self,
        keys: LookupKey,
        opts: EntryOptions,
        current: Entry<V>,
        tags: Box<[Tag]>,
        factory: F,
    ) where
        F: FnOnce(FactoryContext<V>) -> Fut + Send + 'static,
        Fut: Future<Output = std::result::Result<FactoryProduct<V>, FactoryError>> + Send + 'static,
    {
        let key = Arc::clone(&keys.full);
        let Some(local) = self.inner.locks.try_lock(&key) else {
            return;
        };
        self.emit(CacheEvent::EagerRefresh {
            key: Arc::clone(&key),
        });
        let worker = self.clone();
        let background_key = Arc::clone(&key);
        let source = CancellationSource::new();
        let token = source.token();
        let cancelled = source.clone();
        let execution=self.inner.scopes.execution(async move {
            let mut guard=FlightGuard {local:LocalParticipation::Held(local),lease:None,tasks:Arc::clone(&worker.inner.tasks),events:worker.inner.events.clone(),key:Arc::clone(&key),policy:worker.inner.lease_policy};
            if !opts.skip_distributed_locker()&&let Some(locker)=&worker.inner.distributed_locker {
                guard.lease=acquire_owned_supervised(Arc::clone(locker),Arc::from(format!("amalgam:lock:{}",worker.inner.l2_key(&key))),worker.inner.lease_ttl,opts.distributed_lock_timeout(),match worker.inner.lease_policy {LeasePolicy::Fenced=>AcquisitionPolicy::TokenOwned,LeasePolicy::CooperativeLegacy=>AcquisitionPolicy::LegacyBackendContract},worker.lease_owner(&key)).await?;
                if guard.lease.is_none(){return Err(Error::LockTimeout {elapsed:opts.distributed_lock_timeout().as_duration().unwrap_or(Duration::ZERO)});}
            }
            if let Some(entry)=worker.read_l2(&key,&opts,FallbackAvailability::Available,L2ReadPolicy::FactoryFallback,&token).await?&&entry.entry.meta().created()>current.meta().created()&&worker.tags(&entry.entry)==TagVerdict::Valid&&entry.entry.freshness(worker.inner.clock.now()).is_fresh(){worker.hydrate(&key,&entry,&opts).await?.observe();return Ok(CacheValue {value:worker.copy(entry.entry.value(),&opts)?,commit:CommitReceipt::Unchanged});}
            let ctx=FactoryContext::with_cancellation(keys,opts.clone(),tags,Some(worker.stale_info(&current,&opts)?),token.clone());
            let started=worker.inner.clock.now();
            let origin=factory(ctx);
            let product=if let Some(mut state)=guard.lease.as_ref().map(DistributedLease::state) {
                tokio::select! {biased; ()=lease_lost(&mut state)=>return Err(Error::FactoryCancelled {reason:Reason::LeaseLost}),product=origin=>product}
            }else{origin.await};
            let product=product.map_err(Error::from)?;let result=worker.store_product(key,product,started,guard,&token).await;
            if matches!(&result,Err(Error::Lease(LeaseError::Lost))){cancelled.cancel_with(Reason::LeaseLost);}result
        },source);
        self.background_origin(background_key, execution);
    }
    async fn store_product(
        &self,
        key: Arc<str>,
        product: FactoryProduct<V>,
        started: Timestamp,
        guard: FlightGuard,
        cancellation: &FactoryCancellation,
    ) -> Result<CacheValue<V>> {
        let product = product.into_payload()?;
        self.validate_options(&product.options)?;
        guard.proof()?;
        let value = self.copy(&product.value, &product.options)?;
        let stored = self.copy(&product.value, &product.options)?;
        let entry = self.fresh_entry(
            stored,
            &product.options,
            started,
            product.tags,
            product.etag,
            product.last_modified,
        )?;
        let receipt = self
            .write_entry(
                Arc::clone(&key),
                entry,
                product.options,
                Some(guard),
                cancellation,
            )
            .await?;
        match product.origin {
            ProductOrigin::Modified => self.emit(CacheEvent::FactorySuccess {
                key: Arc::clone(&key),
            }),
            ProductOrigin::NotModified => {}
        }
        self.emit(CacheEvent::Set { key });
        Ok(CacheValue {
            value,
            commit: CommitReceipt::Mutation(receipt),
        })
    }
    async fn set(
        &self,
        raw: Arc<str>,
        value: V,
        options: Option<EntryOptions>,
        tags: Box<[Tag]>,
        cancellation: &FactoryCancellation,
    ) -> Result<Observed<MutationReceipt>> {
        let opts = self.resolve_options(&raw, options)?;
        let key = self.full_key(&raw);
        let now = self.inner.clock.now();
        let entry = self.fresh_entry(self.copy(&value, &opts)?, &opts, now, tags, None, None)?;
        let receipt = self
            .write_entry(Arc::clone(&key), entry, opts, None, cancellation)
            .await?;
        self.emit(CacheEvent::Set { key });
        Ok(Observed::new(receipt, OperationOutcome::Stored, None))
    }
    async fn write_entry(
        &self,
        key: Arc<str>,
        entry: Entry<V>,
        opts: EntryOptions,
        flight: Option<FlightGuard>,
        cancellation: &FactoryCancellation,
    ) -> Result<MutationReceipt> {
        let data = match &self.inner.storage {
            Storage::MemoryOnly => PreparedData::Absent,
            Storage::Hybrid { serializer, .. } if !opts.skip_distributed_write() => {
                let snapshot = DistributedSnapshot::from_entry_with_options(
                    &entry,
                    &opts,
                    entry.meta().inserted_at(),
                )?;
                match serializer
                    .encode(&snapshot, self.inner.serialization_mode, cancellation)
                    .await
                {
                    Ok(bytes) => PreparedData::Ready(DataMutation::Set {
                        bytes: bytes.into(),
                        physical_expiration: Timestamp::from_ticks(
                            snapshot.entry().physical_expiration_ticks,
                        ),
                    }),
                    Err(error) => {
                        self.failure(&key, &error);
                        if matches!(
                            error,
                            Error::OperationCancelled { .. } | Error::FactoryCancelled { .. }
                        ) || opts.rethrow_serialization_exceptions()
                        {
                            return Err(error);
                        }
                        PreparedData::Failed(error)
                    }
                }
            }
            Storage::Hybrid { .. } => PreparedData::Skipped,
        };
        let lane = self.inner.lanes.get(&key);
        let lane_guard = Arc::clone(&lane.lock).lock_owned().await;
        if let Some(flight) = &flight {
            flight.proof()?;
        }
        let fence = lane.advance(entry.meta().created(), &self.inner.epoch)?;
        let local = if opts.skip_memory_write() {
            LocalCommit::Applied(LocalEffect::Skipped)
        } else if flight
            .as_ref()
            .map(FlightGuard::proof)
            .transpose()?
            .flatten()
            .is_some()
            && matches!(&data, PreparedData::Ready(_))
        {
            // Atomic fenced backend admission must succeed before the value is
            // visible in L1; a proof observed before an await is insufficient.
            LocalCommit::Store(entry.clone())
        } else {
            LocalCommit::Applied(LocalEffect::Stored(
                self.inner
                    .memory
                    .insert_at(Arc::clone(&key), entry.clone(), self.inner.clock.now())
                    .await,
            ))
        };
        let command = self.data_command(BackplaneAction::Set, &key, entry.meta().created(), &opts);
        let worker = self.clone();
        let task_key = Arc::clone(&key);
        let mode = if opts.allow_background_distributed_operations()
            && matches!(self.inner.storage, Storage::Hybrid { .. })
        {
            CommitMode::Background
        } else {
            CommitMode::Foreground
        };
        let work = async move {
            worker
                .commit_data(DataCommit {
                    key: task_key,
                    data,
                    command,
                    opts,
                    fence,
                    flight,
                    local,
                    lane_guard,
                })
                .await
        };
        self.pipeline_receipt(key, mode, work).await
    }
    fn data_command(
        &self,
        action: BackplaneAction,
        key: &Arc<str>,
        timestamp: Timestamp,
        opts: &EntryOptions,
    ) -> Option<BackplaneCommand> {
        if opts.skip_backplane_notifications() || self.inner.backplane.is_none() {
            None
        } else {
            Some(BackplaneCommand::Data(BackplaneMessage {
                source_id: Arc::clone(&self.inner.instance_id),
                timestamp,
                action,
                key: Arc::from(self.inner.l2_key(key)),
            }))
        }
    }
    async fn commit_receipt(
        &self,
        key: Arc<str>,
        mode: CommitMode,
        work: impl Future<Output = Result<CommitReport>> + Send + 'static,
    ) -> Result<MutationReceipt> {
        match mode {
            CommitMode::Background => {
                let execution = self.inner.scopes.execution(work, CancellationSource::new());
                let receiver = self.inner.tasks.spawn(
                    ShutdownTask::Distributed,
                    key,
                    self.inner.events.clone(),
                    execution,
                );
                Ok(MutationReceipt::Scheduled(CommitCompletion { receiver }))
            }
            CommitMode::Foreground => Ok(MutationReceipt::Completed(work.await?)),
        }
    }
    async fn write_data(
        &self,
        key: &str,
        data: &DataMutation,
        flight: Option<&FlightGuard>,
    ) -> Result<EffectOutcome> {
        let Storage::Hybrid { backend, .. } = &self.inner.storage else {
            return Ok(EffectOutcome::NotConfigured);
        };
        let ttl = data.remaining_ttl(self.inner.clock.now());
        if ttl.is_some_and(|ttl| ttl.is_zero()) {
            return Ok(EffectOutcome::Skipped(SkipReason::PhysicallyExpired));
        }
        if let Some(proof) = flight.map(FlightGuard::proof).transpose()?.flatten() {
            let mutation = match data {
                DataMutation::Set { bytes, .. } | DataMutation::Expire { bytes, .. } => {
                    LeasedMutation::Set {
                        bytes: bytes.to_vec(),
                        ttl,
                    }
                }
                DataMutation::Remove => LeasedMutation::Remove,
            };
            match backend
                .write_with_lease(&self.inner.l2_key(key), mutation, &proof)
                .await?
            {
                LeasedWriteOutcome::Committed => {}
                LeasedWriteOutcome::LeaseLost => return Err(LeaseError::Lost.into()),
            }
        } else {
            match data {
                DataMutation::Set { bytes, .. } | DataMutation::Expire { bytes, .. } => {
                    backend
                        .set(&self.inner.l2_key(key), bytes.to_vec(), ttl)
                        .await?
                }
                DataMutation::Remove => backend.remove(&self.inner.l2_key(key)).await?,
            }
        }
        self.close_circuit(CircuitComponent::Distributed);
        Ok(EffectOutcome::Applied)
    }
    fn enqueue_data(
        &self,
        key: &Arc<str>,
        data: PendingMutation,
        action: RecoveryAction,
        at: Timestamp,
        fence: Arc<Fence>,
    ) -> Result<bool> {
        let Some(recovery) = &self.inner.recovery else {
            return Ok(false);
        };
        let expiry = match &data {
            PendingMutation::Commit {
                mutation:
                    DataMutation::Set {
                        physical_expiration,
                        ..
                    }
                    | DataMutation::Expire {
                        physical_expiration,
                        ..
                    },
                ..
            }
            | PendingMutation::FencedCommit {
                mutation:
                    DataMutation::Set {
                        physical_expiration,
                        ..
                    }
                    | DataMutation::Expire {
                        physical_expiration,
                        ..
                    },
                ..
            } => *physical_expiration,
            _ => at.saturating_add(Duration::from_secs(600)),
        };
        let outcome = recovery.enqueue_versioned(
            RecoveryWork::Data {
                item: RecoveryItem {
                    key: Arc::clone(key),
                    action,
                    timestamp: at,
                    expires_at: expiry,
                    remaining_retries: None,
                },
                mutation: data,
            },
            fence,
        )?;
        Ok(matches!(
            outcome,
            EnqueueOutcome::Queued(_) | EnqueueOutcome::Replaced(_)
        ))
    }
    async fn commit_data(&self, work: DataCommit<V>) -> Result<MutationReceipt> {
        let DataCommit {
            key,
            data,
            command,
            opts,
            fence,
            flight,
            local,
            lane_guard,
        } = work;
        let at = lock(&fence.lane.timestamp).unwrap_or(self.inner.clock.now());
        let action = command
            .as_ref()
            .and_then(|command| match command {
                BackplaneCommand::Data(message) => Some(recovery_action(message.action)),
                BackplaneCommand::Marker(_) => None,
            })
            .unwrap_or(RecoveryAction::Set);
        let distributed = match data {
            PreparedData::Absent => EffectOutcome::NotConfigured,
            PreparedData::Skipped => EffectOutcome::Skipped(SkipReason::Policy),
            PreparedData::Failed(error) => {
                return Ok(MutationReceipt::Completed(CommitReport {
                    local: self.commit_local(&key, local, flight.as_ref()).await?,
                    distributed: EffectOutcome::FailedSuppressed { cause: error },
                    backplane: EffectOutcome::Skipped(SkipReason::Policy),
                }));
            }
            PreparedData::ColdExpire {
                cause,
                logical_expiration,
            } => {
                let queued = self.enqueue_data(
                    &key,
                    PendingMutation::ColdExpire {
                        logical_expiration,
                        notification: command,
                    },
                    RecoveryAction::Expire,
                    at,
                    Arc::clone(&fence),
                )?;
                if opts.rethrow_distributed_exceptions() {
                    return Err(cause);
                }
                return Ok(MutationReceipt::Completed(CommitReport {
                    local: self.commit_local(&key, local, flight.as_ref()).await?,
                    distributed: if queued {
                        EffectOutcome::RecoveryQueued { cause }
                    } else {
                        EffectOutcome::FailedSuppressed { cause }
                    },
                    backplane: EffectOutcome::Skipped(SkipReason::Policy),
                }));
            }
            PreparedData::Ready(data) => {
                let strict = flight.as_ref().is_some_and(|flight| {
                    flight.policy == LeasePolicy::Fenced && flight.lease.is_some()
                });
                let result = if self.circuit(CircuitComponent::Distributed) {
                    self.write_data(&key, &data, flight.as_ref())
                        .instrument(component_span(
                            &self.inner.name,
                            CacheLevel::Distributed,
                            "commit",
                            Some(&key),
                        ))
                        .await
                } else {
                    Err(Error::CircuitOpen {
                        component: CircuitComponent::Distributed,
                    })
                };
                match result {
                    Ok(outcome) => outcome,
                    Err(error) => {
                        self.failure(&key, &error);
                        if matches!(&error,Error::Lease(error) if !matches!(error,LeaseError::Backend {..}))
                        {
                            return Err(error);
                        }
                        let pending = if strict {
                            PendingMutation::FencedCommit {
                                mutation: data,
                                notification: command.clone(),
                            }
                        } else {
                            PendingMutation::Commit {
                                mutation: data,
                                notification: command.clone(),
                            }
                        };
                        let queued =
                            self.enqueue_data(&key, pending, action, at, Arc::clone(&fence))?;
                        if opts.rethrow_distributed_exceptions() {
                            return Err(error);
                        }
                        return Ok(MutationReceipt::Completed(CommitReport {
                            local: self.commit_local(&key, local, flight.as_ref()).await?,
                            distributed: if queued {
                                EffectOutcome::RecoveryQueued { cause: error }
                            } else {
                                EffectOutcome::FailedSuppressed { cause: error }
                            },
                            backplane: EffectOutcome::Skipped(SkipReason::Policy),
                        }));
                    }
                }
            }
        };
        let local = self.commit_local(&key, local, flight.as_ref()).await?;
        if let Some(recovery) = &self.inner.recovery {
            recovery.supersede_through(&key, fence.generation());
        }
        let mode = if opts.allow_background_backplane_operations() && command.is_some() {
            CommitMode::Background
        } else {
            CommitMode::Foreground
        };
        let worker = self.clone();
        let task_key = Arc::clone(&key);
        let work = async move {
            let _lane_guard = lane_guard;
            let backplane = if let Some(command) = command {
                match worker.publish(command.clone()).await {
                    Ok(()) => EffectOutcome::Applied,
                    Err(error) => {
                        worker.failure(&task_key, &error);
                        let queued = worker.enqueue_data(
                            &task_key,
                            PendingMutation::Notify(command),
                            action,
                            at,
                            fence,
                        )?;
                        if opts.rethrow_backplane_exceptions() {
                            return Err(error);
                        }
                        if queued {
                            EffectOutcome::RecoveryQueued { cause: error }
                        } else {
                            EffectOutcome::FailedSuppressed { cause: error }
                        }
                    }
                }
            } else if worker.inner.backplane.is_none() {
                EffectOutcome::NotConfigured
            } else {
                EffectOutcome::Skipped(SkipReason::Policy)
            };
            drop(flight);
            Ok(CommitReport {
                local,
                distributed,
                backplane,
            })
        };
        self.commit_receipt(key, mode, work).await
    }
    async fn commit_local(
        &self,
        key: &Arc<str>,
        local: LocalCommit<V>,
        flight: Option<&FlightGuard>,
    ) -> Result<LocalEffect> {
        match local {
            LocalCommit::Applied(outcome) => Ok(outcome),
            LocalCommit::Store(entry) => {
                if let Some(flight) = flight {
                    flight.proof()?;
                }
                Ok(LocalEffect::Stored(
                    self.inner
                        .memory
                        .insert_at(Arc::clone(key), entry, self.inner.clock.now())
                        .await,
                ))
            }
        }
    }
    async fn pipeline_receipt(
        &self,
        key: Arc<str>,
        mode: CommitMode,
        work: impl Future<Output = Result<MutationReceipt>> + Send + 'static,
    ) -> Result<MutationReceipt> {
        match mode {
            CommitMode::Background => {
                self.commit_receipt(key, CommitMode::Background, async move {
                    work.await?.wait().await
                })
                .await
            }
            CommitMode::Foreground => work.await,
        }
    }
    async fn publish(&self, command: BackplaneCommand) -> Result<()> {
        let Some(backplane) = &self.inner.backplane else {
            return Ok(());
        };
        if !self.circuit(CircuitComponent::Backplane) {
            return Err(Error::CircuitOpen {
                component: CircuitComponent::Backplane,
            });
        }
        let key = match &command {
            BackplaneCommand::Data(message) => Arc::clone(&message.key),
            BackplaneCommand::Marker(command) => {
                Arc::from(format!("marker:{:?}", command.marker().kind()))
            }
        };
        backplane
            .publish_command(command)
            .instrument(component_span(
                &self.inner.name,
                CacheLevel::Backplane,
                "publish",
                Some(&key),
            ))
            .await?;
        self.close_circuit(CircuitComponent::Backplane);
        self.emit(CacheEvent::MessagePublished { key });
        Ok(())
    }
    async fn key_mutation(
        &self,
        raw: Arc<str>,
        options: Option<EntryOptions>,
        mutation: KeyMutation,
        cancellation: &FactoryCancellation,
    ) -> Result<Observed<MutationReceipt>> {
        let opts = self.resolve_options(&raw, options)?;
        let key = self.full_key(&raw);
        let lane = self.inner.lanes.get(&key);
        let lane_guard = Arc::clone(&lane.lock).lock_owned().await;
        let now = self.inner.clock.now();
        let fence = lane.advance(now, &self.inner.epoch)?;
        let local = if opts.skip_memory_write() {
            LocalEffect::Skipped
        } else {
            match mutation {
                KeyMutation::Remove => {
                    self.inner.memory.remove(&key).await;
                    LocalEffect::Removed
                }
                KeyMutation::Expire(_) => {
                    if let Some(entry) = self.inner.memory.get_at(&key, now).await {
                        let expired = entry.with_logical_expiration(now);
                        self.inner
                            .memory
                            .insert_at(Arc::clone(&key), expired, now)
                            .await;
                    }
                    LocalEffect::Expired
                }
            }
        };
        let data = if opts.skip_distributed_write() {
            PreparedData::Skipped
        } else {
            match (&self.inner.storage, mutation) {
                (Storage::MemoryOnly, _) => PreparedData::Absent,
                (
                    Storage::Hybrid { .. },
                    KeyMutation::Remove | KeyMutation::Expire(DistributedExpirePolicy::Remove),
                ) => PreparedData::Ready(DataMutation::Remove),
                (
                    Storage::Hybrid {
                        backend,
                        serializer,
                    },
                    KeyMutation::Expire(DistributedExpirePolicy::RetainStale),
                ) => match backend.get(&self.inner.l2_key(&key)).await {
                    Ok(Some(bytes)) => match serializer
                        .decode(&bytes, self.inner.serialization_mode, cancellation)
                        .await
                    {
                        Ok(snapshot) => {
                            let mut payload = snapshot.entry().clone();
                            payload.logical_expiration_ticks =
                                payload.logical_expiration_ticks.min(now.ticks());
                            let expired = DistributedSnapshot::new(
                                payload,
                                snapshot.inserted_at(),
                                snapshot.retention(),
                            )?;
                            match serializer
                                .encode(&expired, self.inner.serialization_mode, cancellation)
                                .await
                            {
                                Ok(bytes) => PreparedData::Ready(DataMutation::Expire {
                                    bytes: bytes.into(),
                                    physical_expiration: Timestamp::from_ticks(
                                        expired.entry().physical_expiration_ticks,
                                    ),
                                }),
                                Err(error) => {
                                    self.failure(&key, &error);
                                    if matches!(
                                        error,
                                        Error::OperationCancelled { .. }
                                            | Error::FactoryCancelled { .. }
                                    ) || opts.rethrow_serialization_exceptions()
                                    {
                                        return Err(error);
                                    }
                                    PreparedData::Failed(error)
                                }
                            }
                        }
                        Err(error) => {
                            self.failure(&key, &error);
                            if matches!(
                                error,
                                Error::OperationCancelled { .. } | Error::FactoryCancelled { .. }
                            ) || opts.rethrow_serialization_exceptions()
                            {
                                return Err(error);
                            }
                            PreparedData::Failed(error)
                        }
                    },
                    Ok(None) => PreparedData::Ready(DataMutation::Remove),
                    Err(error) => {
                        self.failure(&key, &error);
                        PreparedData::ColdExpire {
                            cause: error,
                            logical_expiration: now,
                        }
                    }
                },
            }
        };
        let action = match mutation {
            KeyMutation::Remove => BackplaneAction::Remove,
            KeyMutation::Expire(_) => BackplaneAction::Expire,
        };
        let command = self.data_command(action, &key, now, &opts);
        let worker = self.clone();
        let task_key = Arc::clone(&key);
        let mode = if opts.allow_background_distributed_operations()
            && matches!(self.inner.storage, Storage::Hybrid { .. })
        {
            CommitMode::Background
        } else {
            CommitMode::Foreground
        };
        let work = async move {
            worker
                .commit_data(DataCommit {
                    key: task_key,
                    data,
                    command,
                    opts,
                    fence,
                    flight: None,
                    local: LocalCommit::Applied(local),
                    lane_guard,
                })
                .await
        };
        let receipt = self.pipeline_receipt(Arc::clone(&key), mode, work).await?;
        self.emit(match mutation {
            KeyMutation::Remove => CacheEvent::Remove { key },
            KeyMutation::Expire(_) => CacheEvent::Expire { key },
        });
        Ok(Observed::new(
            receipt,
            match mutation {
                KeyMutation::Remove => OperationOutcome::Removed,
                KeyMutation::Expire(_) => OperationOutcome::Expired,
            },
            None,
        ))
    }
    async fn mutate_markers(
        &self,
        kinds: Vec<MarkerKind>,
        options: Option<EntryOptions>,
    ) -> Result<Observed<MutationReceipt>> {
        let opts = options.unwrap_or_else(|| self.inner.tags_default_options.clone());
        self.validate_marker_options(&opts)?;
        if self.inner.disable_tagging || matches!(self.inner.markers, MarkerAccess::Unavailable) {
            return Err(MarkerError::Unsupported.into());
        }
        let now = self.inner.clock.now();
        let mut commands = Vec::with_capacity(kinds.len());
        for kind in kinds {
            let outcome = self
                .inner
                .tags
                .advance(kind.clone(), MarkerVersion::new(now));
            let marker = outcome.marker().clone();
            self.seed_marker(&marker, &opts).await?;
            if matches!(kind, MarkerKind::ClearRemove) {
                self.inner.memory.invalidate_all();
            }
            match &kind {
                MarkerKind::Tag(tag) => self.emit(CacheEvent::RemoveByTag {
                    tag: tag.as_str().to_owned(),
                }),
                MarkerKind::ClearExpire | MarkerKind::ClearRemove => self.emit(CacheEvent::Clear),
            }
            commands.push(MarkerCommand::new(
                Arc::clone(&self.inner.instance_id),
                self.inner.scope.clone(),
                marker,
            )?);
            if let MarkerAdvanceOutcome::Compacted { clear_remove, .. } = outcome {
                self.inner.memory.invalidate_all();
                self.seed_marker(
                    &StoredMarker::new(MarkerKind::ClearRemove, clear_remove),
                    &opts,
                )
                .await?;
                commands.push(MarkerCommand::new(
                    Arc::clone(&self.inner.instance_id),
                    self.inner.scope.clone(),
                    StoredMarker::new(MarkerKind::ClearRemove, clear_remove),
                )?);
            }
        }
        let worker = self.clone();
        let mode = if opts.allow_background_distributed_operations()
            && !opts.skip_distributed_write()
            && matches!(self.inner.markers, MarkerAccess::Durable(_))
        {
            CommitMode::Background
        } else {
            CommitMode::Foreground
        };
        let work = async move {
            let mut reports = Vec::with_capacity(commands.len());
            for command in commands {
                reports.push(worker.commit_marker(command, opts.clone()).await?);
            }
            if reports
                .iter()
                .any(|receipt| matches!(receipt, MutationReceipt::Scheduled(_)))
            {
                let key: Arc<str> = Arc::from("invalidation");
                worker
                    .commit_receipt(key, CommitMode::Background, async move {
                        let mut completed = Vec::with_capacity(reports.len());
                        for receipt in reports {
                            completed.push(receipt.wait().await?);
                        }
                        Ok(crate::commit::reports(completed))
                    })
                    .await
            } else {
                let mut completed = Vec::with_capacity(reports.len());
                for receipt in reports {
                    completed.push(receipt.wait().await?);
                }
                Ok(MutationReceipt::Completed(crate::commit::reports(
                    completed,
                )))
            }
        };
        let receipt = self
            .pipeline_receipt(Arc::from("invalidation"), mode, work)
            .await?;
        Ok(Observed::new(receipt, OperationOutcome::Invalidated, None))
    }
    async fn commit_marker(
        &self,
        mut command: MarkerCommand,
        opts: EntryOptions,
    ) -> Result<MutationReceipt> {
        let lane_guard = Arc::clone(&self.inner.marker_lane).lock_owned().await;
        let mut notifications = Vec::with_capacity(2);
        let distributed = match &self.inner.markers {
            MarkerAccess::Local => EffectOutcome::NotConfigured,
            MarkerAccess::Unavailable => return Err(MarkerError::Unsupported.into()),
            MarkerAccess::Durable(_) if opts.skip_distributed_write() => {
                EffectOutcome::Skipped(SkipReason::Policy)
            }
            MarkerAccess::Durable(store) => {
                match store
                    .advance(
                        command.scope(),
                        command.marker().kind().clone(),
                        command.marker().version(),
                    )
                    .await
                {
                    Ok(outcome) => {
                        self.apply_marker(outcome.marker().clone());
                        self.seed_marker(outcome.marker(), &opts).await?;
                        command = MarkerCommand::new(
                            Arc::clone(&self.inner.instance_id),
                            self.inner.scope.clone(),
                            outcome.marker().clone(),
                        )?;
                        if let MarkerAdvanceOutcome::Compacted { clear_remove, .. } = outcome {
                            let clear = MarkerCommand::new(
                                Arc::clone(&self.inner.instance_id),
                                self.inner.scope.clone(),
                                StoredMarker::new(MarkerKind::ClearRemove, clear_remove),
                            )?;
                            self.apply_marker(clear.marker().clone());
                            self.seed_marker(clear.marker(), &opts).await?;
                            notifications.push(clear);
                        }
                        EffectOutcome::Applied
                    }
                    Err(error) => {
                        let queued = self.queue_marker(
                            command.clone(),
                            if opts.skip_backplane_notifications() {
                                MarkerReplay::AdvanceOnly
                            } else {
                                MarkerReplay::AdvanceAndNotify
                            },
                        )?;
                        let error = Error::from(error);
                        self.failure(&Arc::from("invalidation"), &error);
                        if opts.rethrow_distributed_exceptions() {
                            return Err(error);
                        }
                        return Ok(MutationReceipt::Completed(CommitReport {
                            local: LocalEffect::Invalidated,
                            distributed: if queued {
                                EffectOutcome::RecoveryQueued { cause: error }
                            } else {
                                EffectOutcome::FailedSuppressed { cause: error }
                            },
                            backplane: EffectOutcome::Skipped(SkipReason::Policy),
                        }));
                    }
                }
            }
        };
        notifications.push(command);
        let worker = self.clone();
        let mode = if opts.allow_background_backplane_operations()
            && !opts.skip_backplane_notifications()
            && self.inner.backplane.is_some()
        {
            CommitMode::Background
        } else {
            CommitMode::Foreground
        };
        self.commit_receipt(Arc::from("invalidation"), mode, async move {
            let _lane_guard = lane_guard;
            let backplane = if opts.skip_backplane_notifications() {
                EffectOutcome::Skipped(SkipReason::Policy)
            } else if worker.inner.backplane.is_none() {
                EffectOutcome::NotConfigured
            } else {
                let mut outcomes = Vec::with_capacity(notifications.len());
                for command in notifications {
                    if let Err(error) = worker
                        .publish(BackplaneCommand::Marker(command.clone()))
                        .await
                    {
                        worker.failure(&Arc::from("invalidation"), &error);
                        let queued = worker.queue_marker(command, MarkerReplay::NotifyOnly)?;
                        if opts.rethrow_backplane_exceptions() {
                            return Err(error);
                        }
                        outcomes.push(if queued {
                            EffectOutcome::RecoveryQueued { cause: error }
                        } else {
                            EffectOutcome::FailedSuppressed { cause: error }
                        });
                    } else {
                        outcomes.push(EffectOutcome::Applied);
                    }
                }
                crate::commit::effects(outcomes)
            };
            Ok(CommitReport {
                local: LocalEffect::Invalidated,
                distributed,
                backplane,
            })
        })
        .await
    }
    fn queue_marker(&self, command: MarkerCommand, stage: MarkerReplay) -> Result<bool> {
        match &self.inner.recovery {
            Some(recovery) => Ok(matches!(
                recovery.enqueue_marker(command, stage)?,
                EnqueueOutcome::Queued(_) | EnqueueOutcome::Replaced(_)
            )),
            None => Ok(false),
        }
    }
    fn replay_admitted(&self) -> bool {
        self.ensure_health();
        self.inner
            .backplane
            .as_ref()
            .and_then(|backplane| backplane.connection_state())
            .is_none_or(|state| matches!(*state.borrow(), BackplaneState::Connected { .. }))
    }
    async fn replay_legacy(
        &self,
        item: &RecoveryItem,
        cancellation: &FactoryCancellation,
    ) -> Result<()> {
        // Deliberately explicit old executor adapter. Canonical enqueue captures
        // bytes/stages and routes through replay_ticket instead.
        let Storage::Hybrid {
            backend,
            serializer,
        } = &self.inner.storage
        else {
            return Ok(());
        };
        match item.action {
            RecoveryAction::Set => {
                if let Some(entry) = self
                    .inner
                    .memory
                    .get_at(&item.key, self.inner.clock.now())
                    .await
                {
                    let snapshot = DistributedSnapshot::from_entry_with_options(
                        &entry,
                        &self.inner.default_options,
                        entry.meta().inserted_at(),
                    )?;
                    backend
                        .set(
                            &self.inner.l2_key(&item.key),
                            serializer
                                .encode(&snapshot, self.inner.serialization_mode, cancellation)
                                .await?,
                            Some(snapshot.backend_ttl_at(self.inner.clock.now())),
                        )
                        .await?;
                }
            }
            RecoveryAction::Remove => backend.remove(&self.inner.l2_key(&item.key)).await?,
            RecoveryAction::Expire => {}
        }
        Ok(())
    }
    async fn reconcile_replay(
        &self,
        item: &RecoveryItem,
        mutation: &PendingMutation,
        cancellation: &FactoryCancellation,
    ) -> Result<ReplayOutcome> {
        // Recovery bypasses an open circuit, but never treats unavailable current
        // state as safe absence after a missed notification or restart.
        let Storage::Hybrid {
            backend,
            serializer,
        } = &self.inner.storage
        else {
            return Ok(ReplayOutcome::Applied);
        };
        if let Some(current) = backend.get(&self.inner.l2_key(&item.key)).await? {
            let snapshot = serializer
                .decode(&current, self.inner.serialization_mode, cancellation)
                .await?;
            if snapshot.entry().created_ticks > item.timestamp.ticks() {
                return Ok(ReplayOutcome::Superseded);
            }
        }
        let source = match mutation {
            PendingMutation::Commit { mutation, .. }
            | PendingMutation::FencedCommit { mutation, .. } => match mutation {
                DataMutation::Set { bytes, .. } | DataMutation::Expire { bytes, .. } => Some(
                    serializer
                        .decode(bytes, self.inner.serialization_mode, cancellation)
                        .await?
                        .try_into_entry(self.inner.clock.now())?,
                ),
                DataMutation::Remove => None,
            },
            PendingMutation::Legacy
            | PendingMutation::Notify(_)
            | PendingMutation::ColdExpire { .. } => None,
        };
        self.reconcile_markers(source.as_ref().map_or(&[], |entry| entry.meta().tags()))
            .await?;
        if source
            .as_ref()
            .is_some_and(|source| self.tags(source) != TagVerdict::Valid)
        {
            return Ok(ReplayOutcome::Superseded);
        }
        Ok(ReplayOutcome::Applied)
    }
    async fn expire_current(
        &self,
        key: &str,
        logical_expiration: Timestamp,
        cancellation: &FactoryCancellation,
    ) -> Result<ReplayOutcome> {
        let Storage::Hybrid {
            backend,
            serializer,
        } = &self.inner.storage
        else {
            return Ok(ReplayOutcome::Applied);
        };
        let Some(bytes) = backend.get(&self.inner.l2_key(key)).await? else {
            return Ok(ReplayOutcome::Applied);
        };
        let snapshot = serializer
            .decode(&bytes, self.inner.serialization_mode, cancellation)
            .await?;
        // A remote newer write can arrive between reconciliation and this read.
        if snapshot.entry().created_ticks > logical_expiration.ticks() {
            return Ok(ReplayOutcome::Superseded);
        }
        let mut entry = snapshot.entry().clone();
        entry.logical_expiration_ticks = entry
            .logical_expiration_ticks
            .min(logical_expiration.ticks());
        let expired =
            DistributedSnapshot::new(entry, snapshot.inserted_at(), snapshot.retention())?;
        let data = DataMutation::Expire {
            bytes: serializer
                .encode(&expired, self.inner.serialization_mode, cancellation)
                .await?
                .into(),
            physical_expiration: Timestamp::from_ticks(expired.entry().physical_expiration_ticks),
        };
        self.write_data(key, &data, None).await?;
        Ok(ReplayOutcome::Applied)
    }
    async fn replay_owned(
        &self,
        ticket: ReplayTicket,
        cancellation: &FactoryCancellation,
    ) -> Result<ReplayOutcome> {
        let Some(recovery) = &self.inner.recovery else {
            return Ok(ReplayOutcome::Superseded);
        };
        if !self.replay_admitted() {
            return Ok(ReplayOutcome::Paused);
        }
        match ticket.work() {
            RecoveryWork::Data { item, mutation } => {
                // Acquire cluster participation before the commit lane: an origin
                // holding that lease must remain free to finish its own commit.
                let flight = if matches!(mutation, PendingMutation::FencedCommit { .. }) {
                    let Some(locker) = &self.inner.distributed_locker else {
                        return Err(LeaseError::UnsupportedFencing.into());
                    };
                    let lease = acquire_owned_supervised(
                        Arc::clone(locker),
                        Arc::from(format!("amalgam:lock:{}", self.inner.l2_key(&item.key))),
                        self.inner.lease_ttl,
                        self.inner.default_options.distributed_lock_timeout(),
                        AcquisitionPolicy::TokenOwned,
                        self.lease_owner(&item.key),
                    )
                    .await?
                    .ok_or(LeaseError::AcquisitionTimeout)?;
                    Some(FlightGuard {
                        local: LocalParticipation::ReplayOnly,
                        lease: Some(lease),
                        tasks: Arc::clone(&self.inner.tasks),
                        events: self.inner.events.clone(),
                        key: Arc::clone(&item.key),
                        policy: LeasePolicy::Fenced,
                    })
                } else {
                    None
                };
                let lane = self.inner.lanes.get(&item.key);
                let _lane = Arc::clone(&lane.lock).lock_owned().await;
                if !recovery.is_current(&ticket) {
                    return Ok(ReplayOutcome::Superseded);
                }
                if !self.replay_admitted() {
                    return Ok(ReplayOutcome::Paused);
                }
                let reconciled = self.reconcile_replay(item, mutation, cancellation).await?;
                if reconciled != ReplayOutcome::Applied {
                    return Ok(reconciled);
                }
                if !recovery.is_current(&ticket) {
                    return Ok(ReplayOutcome::Superseded);
                }
                if !self.replay_admitted() {
                    return Ok(ReplayOutcome::Paused);
                }
                let command = match mutation {
                    PendingMutation::Legacy => {
                        self.replay_legacy(item, cancellation).await?;
                        None
                    }
                    PendingMutation::Notify(command) => Some(command),
                    PendingMutation::ColdExpire {
                        logical_expiration,
                        notification,
                    } => {
                        let outcome = self
                            .expire_current(&item.key, *logical_expiration, cancellation)
                            .await?;
                        if outcome != ReplayOutcome::Applied {
                            return Ok(outcome);
                        }
                        if notification.is_some() {
                            recovery.notification_stage(&ticket);
                        }
                        notification.as_ref()
                    }
                    PendingMutation::Commit {
                        mutation,
                        notification,
                    }
                    | PendingMutation::FencedCommit {
                        mutation,
                        notification,
                    } => {
                        if mutation
                            .remaining_ttl(self.inner.clock.now())
                            .is_some_and(|ttl| ttl.is_zero())
                        {
                            return Ok(ReplayOutcome::Expired);
                        }
                        self.write_data(&item.key, mutation, flight.as_ref())
                            .await?;
                        if notification.is_some() {
                            recovery.notification_stage(&ticket);
                        }
                        notification.as_ref()
                    }
                };
                if !recovery.is_current(&ticket) {
                    return Ok(ReplayOutcome::Superseded);
                }
                if !self.replay_admitted() {
                    return Ok(ReplayOutcome::Paused);
                }
                if let Some(command) = command
                    && let Some(backplane) = &self.inner.backplane
                {
                    backplane.publish_command(command.clone()).await?;
                    self.close_circuit(CircuitComponent::Backplane);
                }
                Ok(ReplayOutcome::Applied)
            }
            RecoveryWork::Marker { command, stage } => {
                let _lane_guard = Arc::clone(&self.inner.marker_lane).lock_owned().await;
                if !recovery.is_current(&ticket) {
                    return Ok(ReplayOutcome::Superseded);
                }
                if !self.replay_admitted() {
                    return Ok(ReplayOutcome::Paused);
                }
                if matches!(
                    stage,
                    MarkerReplay::AdvanceAndNotify | MarkerReplay::AdvanceOnly
                ) {
                    match &self.inner.markers {
                        MarkerAccess::Durable(store) => {
                            let outcome = store
                                .advance(
                                    command.scope(),
                                    command.marker().kind().clone(),
                                    command.marker().version(),
                                )
                                .await?;
                            self.apply_marker(outcome.marker().clone());
                            if *stage == MarkerReplay::AdvanceAndNotify {
                                recovery.notification_stage(&ticket);
                            }
                            if let MarkerAdvanceOutcome::Compacted { clear_remove, .. } = outcome {
                                let clear = MarkerCommand::new(
                                    Arc::clone(&self.inner.instance_id),
                                    self.inner.scope.clone(),
                                    StoredMarker::new(MarkerKind::ClearRemove, clear_remove),
                                )?;
                                self.apply_marker(clear.marker().clone());
                                if *stage != MarkerReplay::AdvanceOnly {
                                    self.queue_marker(clear, MarkerReplay::NotifyOnly)?;
                                }
                            }
                        }
                        MarkerAccess::Local => {}
                        MarkerAccess::Unavailable => return Err(MarkerError::Unsupported.into()),
                    }
                }
                if !recovery.is_current(&ticket) {
                    return Ok(ReplayOutcome::Superseded);
                }
                if !self.replay_admitted() {
                    return Ok(ReplayOutcome::Paused);
                }
                if *stage != MarkerReplay::AdvanceOnly
                    && let Some(backplane) = &self.inner.backplane
                {
                    backplane
                        .publish_command(BackplaneCommand::Marker(command.clone()))
                        .await?;
                    self.close_circuit(CircuitComponent::Backplane);
                }
                Ok(ReplayOutcome::Applied)
            }
        }
    }
    fn continuity_gap(&self) {
        #[allow(
            deprecated,
            reason = "Atomic::try_update is unavailable on the supported Rust 1.88"
        )]
        if self
            .inner
            .epoch
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |epoch| {
                epoch.checked_add(1)
            })
            .is_err()
        {
            tracing::error!("cache continuity generation exhausted");
            self.inner.close();
        }
        self.inner.memory.invalidate_all();
        self.inner.marker_reads.invalidate();
        if let Some(recovery) = &self.inner.recovery {
            recovery.suspend();
        }
    }
    fn ensure_health(&self) {
        let Some(health) = self
            .inner
            .backplane
            .as_ref()
            .and_then(|backplane| backplane.connection_state())
        else {
            return;
        };
        let current = *health.borrow();
        let mut seen = lock(&self.inner.health_seen);
        if seen.as_ref() == Some(&current) {
            return;
        }
        let previous = seen.replace(current);
        // Serialize barrier transitions with the observed state. Otherwise a
        // delayed disconnected caller can suspend replay after a newer ACK.
        let gap = !matches!(current, BackplaneState::Connected { .. }) || previous.is_some();
        #[allow(
            deprecated,
            reason = "Atomic::try_update is unavailable on the supported Rust 1.88"
        )]
        let exhausted = gap
            && self
                .inner
                .epoch
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |epoch| {
                    epoch.checked_add(1)
                })
                .is_err();
        match current {
            BackplaneState::Disconnected { .. } | BackplaneState::Stopped => {
                if let Some(recovery) = &self.inner.recovery {
                    recovery.suspend();
                }
            }
            BackplaneState::Connected { .. } => {
                self.inner
                    .subscription_admitted
                    .store(true, Ordering::Release);
                if let Some(recovery) = &self.inner.recovery
                    && let Err(error) = recovery.pause_after_reconnect()
                {
                    tracing::warn!(%error,"recovery reconnect delay rejected");
                }
            }
        }
        drop(seen);
        if gap {
            self.inner.memory.invalidate_all();
            self.inner.marker_reads.invalidate();
        }
        if exhausted {
            tracing::error!("cache continuity generation exhausted");
            self.inner.close();
        }
    }
    fn start_maintenance(&self) {
        if tokio::runtime::Handle::try_current().is_err() {
            return;
        }
        if self.inner.maintenance.swap(true, Ordering::AcqRel) {
            return;
        }
        let weak = Arc::downgrade(&self.inner);
        let source = CancellationSource::new();
        let interval = match self.inner.reconciliation {
            ReconciliationPolicy::Periodic(interval) => interval.min(Duration::from_secs(1)),
            ReconciliationPolicy::LocalOnly | ReconciliationPolicy::BackplaneContinuity => {
                Duration::from_secs(1)
            }
        };
        let execution = self.inner.scopes.execution(
            async move {
                loop {
                    tokio::time::sleep(interval).await;
                    let Some(inner) = weak.upgrade() else {
                        return Ok(());
                    };
                    inner.memory.run_pending_tasks().await;
                    if let MarkerReads::OptionsControlled(observations) = &inner.marker_reads {
                        observations.memory.run_pending_tasks().await;
                        observations.locks.clean_idle(64);
                    }
                    inner.locks.clean_idle(256);
                    inner.lanes.clean(256);
                    if matches!(inner.reconciliation, ReconciliationPolicy::Periodic(_)) {
                        let interval = match inner.reconciliation {
                            ReconciliationPolicy::Periodic(interval) => interval,
                            ReconciliationPolicy::LocalOnly
                            | ReconciliationPolicy::BackplaneContinuity => Duration::ZERO,
                        };
                        let reconcile = {
                            let mut last = lock(&inner.last_reconcile);
                            if last.elapsed() >= interval {
                                *last = Instant::now();
                                true
                            } else {
                                false
                            }
                        };
                        if reconcile {
                            inner.memory.invalidate_all();
                        }
                    }
                }
            },
            source,
        );
        let _receiver = self.inner.tasks.spawn(
            ShutdownTask::Maintenance,
            Arc::from("maintenance"),
            self.inner.events.clone(),
            execution,
        );
    }
    fn start_listener(&self) {
        let Some(backplane) = &self.inner.backplane else {
            return;
        };
        let mut messages = backplane.subscribe();
        let mut health = backplane.connection_state();
        let weak = Arc::downgrade(&self.inner);
        let source = CancellationSource::new();
        let execution=self.inner.scopes.execution(async move {
            loop {
                tokio::select! {
                    result=messages.recv()=>{
                        let Some(inner)=weak.upgrade()else{return Ok(());};let worker=Worker {inner};
                        match result {
                            Ok(message)=>worker.apply_backplane(message).await?,
                            Err(broadcast::error::RecvError::Lagged(_))=>{worker.continuity_gap();worker.ensure_health();if let Some(recovery)=&worker.inner.recovery {recovery.pause_after_reconnect()?;}},
                            Err(broadcast::error::RecvError::Closed)=>{worker.continuity_gap();return Ok(());}
                        }
                    },
                    ()=health_changed(&mut health)=>{
                        if let Some(inner)=weak.upgrade(){Worker {inner}.ensure_health();}else{return Ok(());}
                    }
                }
            }
        },source);
        let _receiver = self.inner.tasks.spawn(
            ShutdownTask::Backplane,
            Arc::from("listener"),
            self.inner.events.clone(),
            execution,
        );
        self.ensure_health();
    }
    async fn apply_backplane(&self, message: BackplaneMessage) -> Result<()> {
        if self.inner.ignore_incoming_backplane {
            return Ok(());
        }
        let command = match BackplaneCommand::from_message(message) {
            Ok(command) => command,
            Err(error) => {
                self.continuity_gap();
                // A rejected inner frame loses history without disconnecting
                // the transport. Re-enter reconciliation even at the same ACK.
                if self.replay_admitted()
                    && let Some(recovery) = &self.inner.recovery
                {
                    recovery.pause_after_reconnect()?;
                }
                tracing::warn!(%error,"invalid backplane frame triggered reconciliation");
                return Ok(());
            }
        };
        if command.source_id() == &*self.inner.instance_id {
            return Ok(());
        }
        self.close_circuit(CircuitComponent::Backplane);
        match command {
            BackplaneCommand::Marker(command) => {
                if command.scope() == &self.inner.scope {
                    self.apply_marker(command.marker().clone());
                    self.seed_marker(command.marker(), &self.inner.tags_default_options)
                        .await?;
                    self.emit(CacheEvent::MarkerReceived { command });
                }
                Ok(())
            }
            BackplaneCommand::Data(message) => {
                let Some(key) = self.inner.logical_key(&message.key) else {
                    return Ok(());
                };
                self.emit(CacheEvent::MessageReceived {
                    key: Arc::clone(&key),
                });
                let lane = self.inner.lanes.get(&key);
                let lane_guard = Arc::clone(&lane.lock).lock_owned().await;
                if lock(&lane.timestamp).is_some_and(|at| at > message.timestamp) {
                    return Ok(());
                }
                let existing = self.inner.memory.get_at(&key, self.inner.clock.now()).await;
                if existing
                    .as_ref()
                    .is_some_and(|entry| entry.meta().created() > message.timestamp)
                {
                    return Ok(());
                }
                let fence = lane.advance(message.timestamp, &self.inner.epoch)?;
                if let Some(recovery) = &self.inner.recovery {
                    recovery.cancel_through(&key, message.timestamp);
                }
                match message.action {
                    BackplaneAction::Remove => {
                        self.inner.memory.remove(&key).await;
                    }
                    BackplaneAction::Expire => {
                        if let Some(entry) = existing {
                            let expired = entry.with_logical_expiration(message.timestamp);
                            self.inner
                                .memory
                                .insert_if_unchanged(
                                    Arc::clone(&key),
                                    Some(&entry),
                                    expired,
                                    self.inner.clock.now(),
                                )
                                .await;
                        }
                    }
                    BackplaneAction::Set => {
                        if let Some(existing) = existing {
                            drop(lane_guard);
                            self.passive(key, existing, fence);
                            return Ok(());
                        }
                    }
                }
                Ok(())
            }
        }
    }
    fn passive(&self, key: Arc<str>, expected: Entry<V>, fence: Arc<Fence>) {
        let worker = self.clone();
        let task_key = Arc::clone(&key);
        let source = CancellationSource::new();
        let cancellation = source.token();
        let execution = self.inner.scopes.execution(
            async move {
                let raw = worker
                    .inner
                    .key_prefix
                    .as_ref()
                    .and_then(|prefix| key.strip_prefix(&**prefix))
                    .unwrap_or(&key);
                let opts = worker.resolve_options(raw, None)?;
                let read = worker
                    .read_l2(
                        &key,
                        &opts,
                        FallbackAvailability::Unavailable,
                        L2ReadPolicy::FactoryFallback,
                        &cancellation,
                    )
                    .await;
                let _lane = Arc::clone(&fence.lane.lock).lock_owned().await;
                if !fence.passive_is_current() {
                    return Ok(());
                }
                match read {
                    Ok(Some(entry)) if worker.tags(&entry.entry) != TagVerdict::Remove => {
                        if !opts.skip_memory_write()
                            && let Some(local) = entry
                                .entry
                                .for_memory_hydration(&opts, worker.inner.clock.now())?
                        {
                            let local = local.with_hydrated_value(
                                worker.copy(local.value(), &opts)?,
                                fence.continuity_stamp(),
                            );
                            if !fence.passive_is_current() {
                                return Ok(());
                            }
                            worker
                                .inner
                                .memory
                                .insert_if_unchanged(
                                    key,
                                    Some(&expected),
                                    local,
                                    worker.inner.clock.now(),
                                )
                                .await;
                        }
                    }
                    Err(
                        error @ (Error::OperationCancelled { .. } | Error::FactoryCancelled { .. }),
                    ) => {
                        return Err(error);
                    }
                    Ok(Some(_)) | Ok(None) | Err(_) => {
                        worker.inner.memory.remove_if_same(&key, &expected).await;
                    }
                }
                Ok(())
            },
            source,
        );
        let _receiver = self.inner.tasks.spawn(
            ShutdownTask::Distributed,
            task_key,
            self.inner.events.clone(),
            execution,
        );
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
    key: Arc<str>,
    data: PreparedData,
    command: Option<BackplaneCommand>,
    opts: EntryOptions,
    fence: Arc<Fence>,
    flight: Option<FlightGuard>,
    local: LocalCommit<V>,
    lane_guard: tokio::sync::OwnedMutexGuard<()>,
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
    fn l2_key(&self, key: &str) -> String {
        match self.distributed_key_modifier_mode {
            KeyModifierMode::Prefix => format!("{}:{key}", self.distributed_wire_version),
            KeyModifierMode::Suffix => format!("{key}:{}", self.distributed_wire_version),
            KeyModifierMode::None => key.to_owned(),
        }
    }
    fn logical_key(&self, physical: &str) -> Option<Arc<str>> {
        let key = match self.distributed_key_modifier_mode {
            KeyModifierMode::Prefix => {
                physical.strip_prefix(&format!("{}:", self.distributed_wire_version))?
            }
            KeyModifierMode::Suffix => {
                physical.strip_suffix(&format!(":{}", self.distributed_wire_version))?
            }
            KeyModifierMode::None => physical,
        };
        if self
            .key_prefix
            .as_ref()
            .is_some_and(|prefix| !key.starts_with(&**prefix))
        {
            return None;
        }
        Some(Arc::from(key))
    }
    fn close(&self) -> CloseOutcome {
        let outcome = {
            let mut lifecycle = lock(&self.lifecycle);
            match &*lifecycle {
                Lifecycle::Running => {
                    *lifecycle = Lifecycle::Closing;
                    CloseOutcome::Started
                }
                Lifecycle::Closing => CloseOutcome::AlreadyClosing,
                Lifecycle::Closed(_) => CloseOutcome::AlreadyClosed,
            }
        };
        // Another close can be preempted after publishing Closing but before
        // its scope barrier. Every closer establishes that barrier before an
        // awaitable shutdown may report drainage.
        self.scopes.close();
        if outcome == CloseOutcome::Started {
            if let Some(recovery) = &self.recovery {
                recovery.stop();
            }
            for error in self.plugins.stop_all() {
                tracing::warn!(%error,"plugin close failed");
            }
        }
        outcome
    }
    async fn shutdown(&self) -> Result<ShutdownReport> {
        let _shutdown = self.shutdown_gate.lock().await;
        if let Lifecycle::Closed(result) = &*lock(&self.lifecycle) {
            return result.clone().map_err(Error::from);
        }
        self.close();
        self.scopes.drained().await;
        self.tasks.drain().await;
        let mut failures = self.tasks.take_failures();
        if let Some(recovery) = &self.recovery
            && let Err(error) = recovery.shutdown().await
        {
            failures.push(ShutdownFailure::Work(error.into()));
        }
        failures.extend(
            self.plugins
                .shutdown()
                .await
                .into_iter()
                .map(ShutdownFailure::Plugin),
        );
        let result = if failures.is_empty() {
            Ok(ShutdownReport)
        } else {
            let mut failures = failures.into_iter();
            match failures.next() {
                Some(first) => Err(ShutdownError::new(first, failures)),
                None => unreachable!("nonempty failure collection"),
            }
        };
        *lock(&self.lifecycle) = Lifecycle::Closed(result.clone());
        result.map_err(Error::from)
    }
}
#[async_trait]
impl<V: Clone + Send + Sync + 'static> RecoveryExecutor for CacheInner<V> {
    async fn replay(&self, item: &RecoveryItem) -> Result<()> {
        if self.scopes.is_closed() {
            return Err(Error::CacheClosed);
        }
        let worker = Worker {
            inner: self.owner.upgrade().ok_or(Error::CacheClosed)?,
        };
        let item = item.clone();
        let source = CancellationSource::new();
        let cancellation = source.token();
        self.scopes
            .execution(
                async move { worker.replay_legacy(&item, &cancellation).await },
                source,
            )
            .await
    }
    async fn replay_ticket(&self, ticket: &ReplayTicket) -> Result<ReplayOutcome> {
        if self.scopes.is_closed() {
            return Err(Error::CacheClosed);
        }
        let worker = Worker {
            inner: self.owner.upgrade().ok_or(Error::CacheClosed)?,
        };
        let ticket = ticket.clone();
        let source = CancellationSource::new();
        let cancellation = source.token();
        self.scopes
            .execution(
                async move { worker.replay_owned(ticket, &cancellation).await },
                source,
            )
            .await
    }
}

/// Builder for a [`Cache`].
///
/// ```
/// use amalgam::{Cache, EntryOptions};
/// use std::time::Duration;
///
/// let cache: Cache<String> = Cache::builder()
///     .name("users")
///     .key_prefix("u:")
///     .default_options(EntryOptions::new(Duration::from_secs(60)))
///     .build();
/// ```
#[must_use = "a builder does nothing until `.build()` is called"]
pub struct CacheBuilder<V> {
    name: Option<Arc<str>>,
    instance_id: Option<Arc<str>>,
    key_prefix: Option<Arc<str>>,
    default_options: EntryOptions,
    tags_default_options: EntryOptions,
    marker_read_policy: MarkerReadPolicy,
    marker_lifecycle_policy: MarkerLifecyclePolicy,
    marker_read_limits: MemoryLimits,
    clock: Option<Arc<dyn Clock>>,
    max_capacity: Option<u64>,
    max_weighted_capacity: Option<u64>,
    value_cloner: Option<Arc<dyn ValueCloner<V>>>,
    jitter: Arc<dyn JitterSource>,
    invalidation_store: Option<Arc<dyn InvalidationStore>>,
    lease_policy: LeasePolicy,
    lease_ttl: Duration,
    reconciliation: Option<ReconciliationPolicy>,
    lock_shards: usize,
    remove_by_tag_behavior: RemoveByTagBehavior,
    events_capacity: usize,
    distributed: Option<Arc<dyn DistributedCache>>,
    serializer: Option<crate::distributed::Serializer<V>>,
    serialization_mode: crate::distributed::SerializationMode,
    backplane: Option<Arc<dyn Backplane>>,
    distributed_locker: Option<Arc<dyn DistributedLocker>>,
    plugins: Vec<Arc<dyn Plugin>>,
    distributed_circuit_breaker: Duration,
    backplane_circuit_breaker: Duration,
    recovery_config: RecoveryConfig,
    default_options_provider: Option<Arc<dyn DefaultEntryOptionsProvider>>,
    ignore_incoming_backplane: bool,
    distributed_wire_version: Arc<str>,
    distributed_key_modifier_mode: KeyModifierMode,
    disable_tagging: bool,
    wait_for_initial_backplane_subscribe: bool,
    _marker: std::marker::PhantomData<fn() -> V>,
}

impl<V> CacheBuilder<V> {
    /// Creates a builder with default settings.
    pub fn new() -> Self {
        Self {
            name: None,
            instance_id: None,
            key_prefix: None,
            default_options: EntryOptions::default(),
            tags_default_options: EntryOptions::tag_defaults(),
            marker_read_policy: MarkerReadPolicy::default(),
            marker_lifecycle_policy: MarkerLifecyclePolicy::default(),
            marker_read_limits: MemoryLimits::new(Some(4096), None),
            clock: None,
            max_capacity: None,
            max_weighted_capacity: None,
            value_cloner: None,
            jitter: Arc::new(RandomJitterSource),
            invalidation_store: None,
            lease_policy: LeasePolicy::Fenced,
            lease_ttl: Duration::from_secs(30),
            reconciliation: None,
            lock_shards: 1024,
            remove_by_tag_behavior: RemoveByTagBehavior::default(),
            events_capacity: 256,
            distributed: None,
            serializer: None,
            serialization_mode: crate::distributed::SerializationMode::default(),
            backplane: None,
            distributed_locker: None,
            plugins: Vec::new(),
            distributed_circuit_breaker: Duration::ZERO,
            backplane_circuit_breaker: Duration::ZERO,
            recovery_config: RecoveryConfig::default(),
            default_options_provider: None,
            ignore_incoming_backplane: false,
            distributed_wire_version: Arc::from("v2"),
            distributed_key_modifier_mode: KeyModifierMode::default(),
            disable_tagging: false,
            wait_for_initial_backplane_subscribe: true,
            _marker: std::marker::PhantomData,
        }
    }

    /// Sets this instance's id (used to ignore its own backplane messages). A
    /// random id is generated if not set.
    pub fn instance_id(mut self, id: impl AsRef<str>) -> Self {
        self.instance_id = Some(Arc::from(id.as_ref()));
        self
    }

    /// Attaches an L2 distributed cache backend. Pair with
    /// [`serializer`](Self::serializer).
    pub fn distributed(mut self, distributed: Arc<dyn DistributedCache>) -> Self {
        self.distributed = Some(distributed);
        self
    }

    /// Sets the serializer used for the L2 wire format (e.g.
    /// [`JsonSerializer`](crate::JsonSerializer)).
    pub fn serializer(mut self, serializer: Arc<dyn DistributedSerializer<V>>) -> Self {
        self.serializer = Some(crate::distributed::Serializer::Sync(serializer));
        self
    }
    /// Sets an asynchronous full-snapshot codec, optionally offering a sync counterpart.
    pub fn async_serializer(
        mut self,
        serializer: Arc<dyn crate::distributed::AsyncDistributedSerializer<V>>,
    ) -> Self {
        self.serializer = Some(crate::distributed::Serializer::Async(serializer));
        self
    }
    /// Chooses the preferred available codec model. SyncPreferred preserves legacy behavior.
    pub fn serialization_mode(mut self, mode: crate::distributed::SerializationMode) -> Self {
        self.serialization_mode = mode;
        self
    }

    /// Attaches a backplane for multi-node L1 invalidation.
    ///
    /// Note: when a backplane is configured, [`build`](Self::build) spawns a
    /// listener task and so must be called from within a tokio runtime.
    pub fn backplane(mut self, backplane: Arc<dyn Backplane>) -> Self {
        self.backplane = Some(backplane);
        self
    }

    /// Sets the cache's name (used in events/diagnostics).
    pub fn name(mut self, name: impl AsRef<str>) -> Self {
        self.name = Some(Arc::from(name.as_ref()));
        self
    }

    /// Sets a prefix prepended to every key.
    pub fn key_prefix(mut self, prefix: impl AsRef<str>) -> Self {
        self.key_prefix = Some(Arc::from(prefix.as_ref()));
        self
    }

    /// Sets the default entry options merged into every operation that does not
    /// supply its own.
    pub fn default_options(mut self, options: EntryOptions) -> Self {
        self.default_options = options;
        self
    }
    /// Sets independent defaults for tag invalidation and cache-wide clear.
    /// Explicit mutation options take precedence; per-key providers are skipped.
    /// OptionsControlled secondary reads always use these cache-wide defaults.
    pub fn tags_default_options(mut self, options: EntryOptions) -> Self {
        self.tags_default_options = options;
        self
    }

    /// Selects independent secondary marker reads while preserving the existing
    /// durable contract by default. Skips/suppressed failures are explicit choices.
    pub fn marker_read_policy(mut self, policy: MarkerReadPolicy) -> Self {
        self.marker_read_policy = policy;
        self
    }

    /// Opts into independent expiring snapshots and nonzero read-miss repair.
    /// Existing durable providers remain supported by the default DurableOnly.
    pub fn marker_lifecycle_policy(mut self, policy: MarkerLifecyclePolicy) -> Self {
        self.marker_lifecycle_policy = policy;
        self
    }

    /// Limits the separate observation cache; never expires durable tombstones
    /// or discards known local invalidations. Tag size and priority select admission.
    pub fn marker_read_limits(mut self, limits: MemoryLimits) -> Self {
        self.marker_read_limits = limits;
        self
    }

    /// Injects a custom [`Clock`] (e.g. [`ManualClock`](crate::ManualClock) in
    /// tests).
    pub fn clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = Some(clock);
        self
    }

    /// Caps the number of entries held in L1 (`None` = unbounded).
    pub fn max_capacity(mut self, capacity: u64) -> Self {
        self.max_capacity = Some(capacity);
        self
    }

    /// Caps total admitted L1 weight, independently from the entry count.
    pub fn max_weighted_capacity(mut self, capacity: u64) -> Self {
        self.max_weighted_capacity = Some(capacity);
        self
    }
    /// Supplies an explicit deep-copy algorithm for auto-clone options.
    pub fn value_cloner(mut self, cloner: Arc<dyn ValueCloner<V>>) -> Self {
        self.value_cloner = Some(cloner);
        self
    }

    /// Uses the compiler-checked ordinary clone for auto-clone values.
    /// This bypasses codec copying without admitting shared mutable state.
    /// Later `value_cloner` calls may explicitly replace this strategy.
    pub fn immutable_values(mut self) -> Self
    where
        V: crate::serializers::ImmutableValue,
    {
        self.value_cloner = Some(Arc::new(crate::serializers::ImmutableCloner));
        self
    }
    /// Supplies expiration jitter outside pure entry construction.
    pub fn jitter_source(mut self, jitter: Arc<dyn JitterSource>) -> Self {
        self.jitter = jitter;
        self
    }
    /// Supplies genuine atomic invalidation storage for a custom byte backend.
    pub fn invalidation_store(mut self, store: Arc<dyn InvalidationStore>) -> Self {
        self.invalidation_store = Some(store);
        self
    }
    /// Chooses strict native fencing or explicitly cooperative legacy ownership.
    pub fn lease_policy(mut self, policy: LeasePolicy) -> Self {
        self.lease_policy = policy;
        self
    }
    /// Sets ownership lease duration independently from entry retention.
    pub fn lease_ttl(mut self, ttl: Duration) -> Self {
        self.lease_ttl = ttl;
        self
    }
    /// Declares notification/durable-marker reconciliation behavior.
    pub fn reconciliation_policy(mut self, policy: ReconciliationPolicy) -> Self {
        self.reconciliation = Some(policy);
        self
    }

    /// Retains the legacy shard setting for source compatibility.
    ///
    /// Flights are now per-key; this setting does not serialize distinct keys
    /// or select the internal lookup map's sharding.
    pub fn lock_shards(mut self, shards: usize) -> Self {
        self.lock_shards = shards;
        self
    }

    /// Sets what `remove_by_tag` does to matched entries.
    pub fn remove_by_tag_behavior(mut self, behavior: RemoveByTagBehavior) -> Self {
        self.remove_by_tag_behavior = behavior;
        self
    }

    /// Sets the event channel's buffer capacity.
    pub fn events_capacity(mut self, capacity: usize) -> Self {
        self.events_capacity = capacity;
        self
    }

    /// Attaches a cross-node distributed locker (e.g. Redis-backed) for
    /// cluster-wide single-flight.
    pub fn distributed_locker(mut self, locker: Arc<dyn DistributedLocker>) -> Self {
        self.distributed_locker = Some(locker);
        self
    }

    /// Registers a [`Plugin`] to observe events and lifecycle.
    pub fn plugin(mut self, plugin: Arc<dyn Plugin>) -> Self {
        self.plugins.push(plugin);
        self
    }

    /// Opens the L2 circuit breaker for `duration` after an L2 failure
    /// (`Duration::ZERO` disables it — the default).
    pub fn distributed_circuit_breaker(mut self, duration: Duration) -> Self {
        self.distributed_circuit_breaker = duration;
        self
    }

    /// Opens the backplane circuit breaker for `duration` after a backplane
    /// failure (`Duration::ZERO` disables it — the default).
    pub fn backplane_circuit_breaker(mut self, duration: Duration) -> Self {
        self.backplane_circuit_breaker = duration;
        self
    }

    /// Configures (or disables) auto-recovery of failed L2 / backplane operations.
    pub fn auto_recovery(mut self, config: RecoveryConfig) -> Self {
        self.recovery_config = config;
        self
    }

    /// Sets a provider of dynamic, per-key default options (consulted when a call
    /// supplies no explicit options).
    pub fn default_options_provider(
        mut self,
        provider: Arc<dyn DefaultEntryOptionsProvider>,
    ) -> Self {
        self.default_options_provider = Some(provider);
        self
    }

    /// Drops all incoming backplane notifications (dangerous; for testing).
    pub fn ignore_incoming_backplane(mut self, ignore: bool) -> Self {
        self.ignore_incoming_backplane = ignore;
        self
    }

    /// Sets the L2 wire-format version combined with distributed keys.
    pub fn distributed_wire_version(mut self, version: impl AsRef<str>) -> Self {
        self.distributed_wire_version = Arc::from(version.as_ref());
        self
    }

    /// Sets how the wire-format version is combined with the L2 key (prefix,
    /// suffix, or none).
    pub fn distributed_key_modifier_mode(mut self, mode: KeyModifierMode) -> Self {
        self.distributed_key_modifier_mode = mode;
        self
    }

    /// Disables tagging cache-wide (FusionCache `DisableTagging`).
    ///
    /// When enabled, the per-read tag/clear-marker check is skipped entirely (a
    /// small read-path saving for caches that never tag), and
    /// [`remove_by_tag`](Cache::remove_by_tag) /
    /// [`remove_by_tags`](Cache::remove_by_tags) / [`clear`](Cache::clear) are
    /// ignored and logged at `warn` (never silently) — the Rust-idiomatic
    /// counterpart of FusionCache throwing on tag use when tagging is disabled.
    /// Off by default.
    pub fn disable_tagging(mut self, disable: bool) -> Self {
        self.disable_tagging = disable;
        self
    }

    /// Gates operations on native subscription acknowledgement (default true).
    /// Construction installs the local receiver synchronously; use
    /// `try_build_ready` to await admission before returning the cache. Native
    /// Redis connect already awaits its first ACK. Healthless custom adapters
    /// report BestEffort and use bounded periodic durable reconciliation.
    pub fn wait_for_initial_backplane_subscribe(mut self, wait: bool) -> Self {
        self.wait_for_initial_backplane_subscribe = wait;
        self
    }
}

impl<V> Default for CacheBuilder<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V: Clone + Send + Sync + 'static> CacheBuilder<V> {
    /// Async construction that also waits native backplane acknowledgement.
    pub async fn try_build_ready(self) -> Result<Cache<V>> {
        let cache = self.try_build()?;
        cache.ready().await?;
        Ok(cache)
    }
    /// Diagnostic legacy construction adapter. Prefer try_build for expected
    /// configuration/plugin/runtime rejection.
    pub fn build(self) -> Cache<V> {
        match self.try_build() {
            Ok(cache) => cache,
            Err(error) => panic!("invalid legacy cache construction: {error}"),
        }
    }
    /// Constructs a fully valid cache before starting its services.
    pub fn try_build(self) -> Result<Cache<V>> {
        let name = self.name.unwrap_or_else(|| Arc::from("amalgam"));
        let instance_id = self
            .instance_id
            .unwrap_or_else(|| Arc::from(format!("amalgam-{:016x}", fastrand::u64(..))));
        for (value, field) in [
            (&*name, IdentityField::CacheName),
            (&*instance_id, IdentityField::InstanceId),
            (&*self.distributed_wire_version, IdentityField::WireVersion),
        ] {
            if value.trim().is_empty() {
                return Err(ConfigError::BlankIdentity { field }.into());
            }
        }
        if self.distributed.is_some() && self.serializer.is_none() {
            return Err(ConfigError::DistributedWithoutSerializer.into());
        }
        let cloner = self.value_cloner.or_else(|| {
            self.serializer
                .as_ref()
                .and_then(|serializer| serializer.value_cloner())
        });
        self.default_options
            .validate_with_cloner(cloner.as_deref())?;
        self.tags_default_options.validate()?;
        for options in [&self.default_options, &self.tags_default_options] {
            for timeout in [
                options.memory_lock_timeout(),
                options.distributed_lock_timeout(),
                options.factory_soft_timeout(),
                options.factory_hard_timeout(),
                options.distributed_soft_timeout(),
                options.distributed_hard_timeout(),
            ] {
                validate_budget(timeout)?;
            }
        }
        let recovery_enabled = self.recovery_config.enabled
            && (self.distributed.is_some() || self.backplane.is_some());
        if self.backplane.is_some() && tokio::runtime::Handle::try_current().is_err() {
            return Err(ConfigError::MissingRuntime {
                component: RuntimeComponent::Backplane,
            }
            .into());
        }
        if recovery_enabled && tokio::runtime::Handle::try_current().is_err() {
            return Err(ConfigError::MissingRuntime {
                component: RuntimeComponent::Recovery,
            }
            .into());
        }
        let lease_ttl = LeaseTtl::new(self.lease_ttl)?;
        let reconciliation = self.reconciliation.unwrap_or_else(|| {
            if self.backplane.is_none() && self.distributed.is_none() {
                ReconciliationPolicy::LocalOnly
            } else if self
                .backplane
                .as_ref()
                .and_then(|backplane| backplane.connection_state())
                .is_some()
            {
                ReconciliationPolicy::BackplaneContinuity
            } else {
                ReconciliationPolicy::Periodic(Duration::from_secs(1))
            }
        });
        match reconciliation {
            ReconciliationPolicy::LocalOnly => {
                if self.backplane.is_some() || self.distributed.is_some() {
                    return Err(ConfigError::LocalReconciliationWithExternalStorage.into());
                }
            }
            ReconciliationPolicy::Periodic(interval) => {
                if interval.is_zero() {
                    return Err(ConfigError::ZeroReconciliationInterval.into());
                }
                validate_budget(Timeout::After(interval))?;
            }
            ReconciliationPolicy::BackplaneContinuity => {
                if self
                    .backplane
                    .as_ref()
                    .and_then(|backplane| backplane.connection_state())
                    .is_none()
                {
                    return Err(ConfigError::UnavailableBackplaneContinuity.into());
                }
            }
        }
        let scope = CacheScope::new(
            self.key_prefix.clone().unwrap_or_else(|| Arc::from("")),
            Arc::clone(&self.distributed_wire_version),
            self.distributed_key_modifier_mode,
        )?;
        let markers = match self.invalidation_store.or_else(|| {
            self.distributed
                .as_ref()
                .and_then(|backend| backend.invalidation_store())
        }) {
            Some(store) => MarkerAccess::Durable(store),
            None if self.distributed.is_some() => MarkerAccess::Unavailable,
            None => MarkerAccess::Local,
        };
        let marker_lifecycle = match self.marker_lifecycle_policy {
            MarkerLifecyclePolicy::DurableOnly => MarkerLifecycleAccess::DurableOnly,
            MarkerLifecyclePolicy::CachedSnapshots => {
                if self.marker_read_policy != MarkerReadPolicy::OptionsControlled {
                    return Err(ConfigError::MarkerLifecycleRequiresControlledReads.into());
                }
                let cache = match &markers {
                    MarkerAccess::Durable(store) => store.snapshot_cache(),
                    MarkerAccess::Local | MarkerAccess::Unavailable => None,
                }
                .ok_or(ConfigError::MarkerSnapshotCapabilityUnavailable)?;
                MarkerLifecycleAccess::CachedSnapshots(cache)
            }
        };
        let storage = match (self.distributed, self.serializer) {
            (Some(backend), Some(serializer)) => Storage::Hybrid {
                backend,
                serializer,
            },
            (None, _) => Storage::MemoryOnly,
            (Some(_), None) => return Err(ConfigError::DistributedWithoutSerializer.into()),
        };
        let clock: Arc<dyn Clock> = self.clock.unwrap_or_else(|| Arc::new(SystemClock));
        let recovery = if recovery_enabled {
            Some(AutoRecoveryService::try_new(
                self.recovery_config,
                Arc::clone(&clock),
            )?)
        } else {
            None
        };
        let events = Events::with_capacity(self.events_capacity);
        let plugins = PluginHost::try_new(
            PluginContext::new(&*name, &*instance_id, events.clone())?,
            self.plugins,
        )?;
        let expiry = match clock.timing_model() {
            ClockTiming::RealTime => MemoryExpiry::RealTime,
            ClockTiming::Controlled => MemoryExpiry::ClockDriven,
        };
        let marker_reads = match self.marker_read_policy {
            MarkerReadPolicy::DurableRequired => MarkerReads::DurableRequired,
            MarkerReadPolicy::OptionsControlled => {
                MarkerReads::OptionsControlled(Box::new(MarkerObservations::new(
                    self.marker_read_limits,
                    Arc::clone(&clock),
                    expiry,
                    marker_lifecycle,
                )))
            }
        };
        let inner = Arc::new_cyclic(|owner| CacheInner {
            owner: owner.clone(),
            name,
            instance_id,
            memory: MemoryStore::with_clock_and_expiry(
                MemoryLimits::new(self.max_capacity, self.max_weighted_capacity),
                events.clone(),
                Arc::clone(&clock),
                expiry,
            ),
            locks: KeyedLock::new(self.lock_shards),
            lanes: Lanes::new(),
            tags: TagRegistry::new(),
            events,
            clock,
            default_options: self.default_options,
            tags_default_options: self.tags_default_options,
            marker_reads,
            key_prefix: self.key_prefix,
            remove_by_tag_behavior: self.remove_by_tag_behavior,
            storage,
            serialization_mode: self.serialization_mode,
            markers,
            scope,
            backplane: self.backplane,
            distributed_locker: self.distributed_locker,
            lease_policy: self.lease_policy,
            lease_ttl,
            circuit_l2: CircuitBreaker::new(self.distributed_circuit_breaker),
            circuit_backplane: CircuitBreaker::new(self.backplane_circuit_breaker),
            plugins,
            recovery: recovery.clone(),
            default_options_provider: self.default_options_provider,
            ignore_incoming_backplane: self.ignore_incoming_backplane,
            distributed_wire_version: self.distributed_wire_version,
            distributed_key_modifier_mode: self.distributed_key_modifier_mode,
            disable_tagging: self.disable_tagging,
            wait_for_initial_backplane_subscribe: self.wait_for_initial_backplane_subscribe,
            cloner,
            jitter: self.jitter,
            scopes: Scopes::new(),
            tasks: Tasks::new(),
            epoch: Arc::new(AtomicU64::new(0)),
            maintenance: AtomicBool::new(false),
            subscription_admitted: AtomicBool::new(false),
            reconciliation,
            lifecycle: std::sync::Mutex::new(Lifecycle::Running),
            shutdown_gate: tokio::sync::Mutex::new(()),
            marker_lane: Arc::new(tokio::sync::Mutex::new(())),
            health_seen: std::sync::Mutex::new(None),
            last_reconcile: std::sync::Mutex::new(Instant::now()),
        });
        if let Some(recovery) = &recovery {
            let executor: Arc<dyn RecoveryExecutor> = inner.clone();
            recovery.try_set_executor(Arc::downgrade(&executor))?;
            recovery.try_spawn()?;
        }
        if self.lease_policy == LeasePolicy::CooperativeLegacy && inner.distributed_locker.is_some()
        {
            tracing::warn!(cache=%inner.name,"explicit cooperative legacy lease policy: partition fencing is unavailable");
        }
        let cache = Cache {
            lifetime: Arc::new(PublicLifetime {
                inner: Arc::downgrade(&inner),
            }),
            inner,
        };
        cache.worker().start_listener();
        Ok(cache)
    }
}
