//! Exact-identity recovery, with strong generation fences and typed marker work.
//!
//! A successful old replay never removes, decrements, or evicts a replacement.
//! The executor additionally holds the cache's same-key commit lane before any
//! side effect: queue CAS alone cannot order an already-started backend mutation.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::watch;
use tokio::time::Instant;

use crate::backplane::{BackplaneCommand, MarkerCommand};
use crate::error::Result;
use crate::tags::{CacheScope, MarkerKind};
use crate::time::{Clock, Timestamp};

mod marker_snapshots;
pub use marker_snapshots::{
    MarkerMutationRecovery, MarkerMutationStage, MarkerSnapshotParticipation, MarkerSnapshotReplay,
};

/// The compatible legacy data replay discriminants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryAction {
    /// Re-write a value and announce it.
    Set,
    /// Remove a value and announce it.
    Remove,
    /// Expire a value and announce it.
    Expire,
}

/// Compatible legacy replay DTO; no required fields have been added.
#[derive(Debug, Clone)]
pub struct RecoveryItem {
    /// The prefixed data key.
    pub key: Arc<str>,
    /// The data mutation.
    pub action: RecoveryAction,
    /// Original ordering revision.
    pub timestamp: Timestamp,
    /// Physical retry deadline.
    pub expires_at: Timestamp,
    /// Additional retries after the initial attempt; absent uses configured budget.
    pub remaining_retries: Option<u32>,
}

/// Recovery configuration. `None` remains an explicit unlimited budget.
#[derive(Debug, Clone)]
pub struct RecoveryConfig {
    /// Enables the queue and worker.
    pub enabled: bool,
    /// Positive retry interval and post-reconnect barrier.
    pub delay: Duration,
    /// Maximum retained items, or explicitly unlimited.
    pub max_items: Option<usize>,
    /// Additional retries after the initial attempt, or unlimited.
    pub max_retries: Option<u32>,
}

impl Default for RecoveryConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            delay: Duration::from_secs(2),
            max_items: Some(1024),
            max_retries: None,
        }
    }
}

/// Failures of protocol construction or owned recovery lifecycle.
#[derive(Debug, thiserror::Error)]
pub enum RecoveryError {
    /// Ordering identifiers must never wrap and become current again.
    #[error("recovery generation sequence exhausted")]
    GenerationExhausted,
    /// Queue identities must never be reused.
    #[error("recovery identity sequence exhausted")]
    IdentityExhausted,
    /// Invalid recovery interval.
    #[error("enabled recovery requires a positive delay")]
    ZeroDelay,
    /// Recovery startup requires a Tokio runtime.
    #[error("recovery requires a Tokio runtime")]
    MissingRuntime,
    /// An executor was already installed.
    #[error("recovery executor was already configured")]
    ExecutorAlreadyConfigured,
    /// A stopped service cannot be restarted.
    #[error("recovery service has stopped")]
    Stopped,
    /// The old executor only knows the unchanged RecoveryItem data API.
    #[error("legacy recovery executor cannot replay typed marker work")]
    UnsupportedMarkerExecutor,
    /// A repair cannot override an explicit exclusion of distributed writes.
    #[error("marker repair cannot capture skipped snapshot writes")]
    SnapshotWritesSkipped,
    /// A stage transition changed the namespace, kind or committed revision.
    #[error("marker recovery identity changed")]
    MarkerIdentityChanged,
    /// A requested transition does not follow the captured protocol stage.
    #[error("invalid marker recovery stage transition")]
    InvalidMarkerStage,
    /// A reconnect barrier cannot be represented safely.
    #[error("recovery barrier is outside the monotonic clock range")]
    InvalidBarrier,
    /// An owned worker unexpectedly failed.
    #[error("recovery task failed: {source}")]
    Task {
        /// Original task failure.
        #[source]
        source: tokio::task::JoinError,
    },
}

/// Per-key operation order, independent from possibly equal wall-clock revisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OperationGeneration(u64);

impl OperationGeneration {
    /// Creates an existing operation generation; zero is the initial state.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// The diagnostic sequence number.
    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }

    /// Advances without wraparound.
    pub fn next(self) -> std::result::Result<Self, RecoveryError> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or(RecoveryError::GenerationExhausted)
    }
}

/// Strong opaque pin to the exact per-key commit lane retained by pending work.
/// Implementations return the captured generation, not a changing live counter.
pub trait RecoveryFence: Send + Sync {
    /// Immutable generation captured when this work was admitted.
    fn generation(&self) -> OperationGeneration;
    /// Whether that captured generation is still allowed to replay.
    fn is_current(&self) -> bool;
}

/// The exact original value-side effect, captured before it fails.
#[derive(Debug, Clone)]
pub enum DataMutation {
    /// Write immutable bytes with their original absolute physical expiry.
    Set {
        /// The original serialized frame, never a later L1 value.
        bytes: Arc<[u8]>,
        /// Absolute source physical deadline.
        physical_expiration: Timestamp,
    },
    /// Remove the value.
    Remove,
    /// Persist an already logically expired frame without renewing its lifetime.
    Expire {
        /// Serialized expired snapshot.
        bytes: Arc<[u8]>,
        /// Original physical deadline.
        physical_expiration: Timestamp,
    },
}

impl DataMutation {
    /// Remaining lifetime at the actual retry, if this mutation stores a value.
    #[must_use]
    pub fn remaining_ttl(&self, now: Timestamp) -> Option<Duration> {
        match self {
            Self::Set {
                physical_expiration,
                ..
            }
            | Self::Expire {
                physical_expiration,
                ..
            } => Some(physical_expiration.saturating_duration_since(now)),
            Self::Remove => None,
        }
    }
}

/// The failed stage. A notification-only retry cannot re-write an old value.
#[derive(Debug, Clone)]
pub enum PendingMutation {
    /// Explicit adapter for the old executor's RecoveryItem contract.
    Legacy,
    /// Storage must succeed before its optional notification is published.
    Commit {
        /// Captured storage side effect.
        mutation: DataMutation,
        /// A real notification, absent when notifications were skipped.
        notification: Option<BackplaneCommand>,
    },
    /// An origin commit must reacquire token-owned participation before retry.
    FencedCommit {
        /// Captured immutable value mutation and original deadline.
        mutation: DataMutation,
        /// Optional publication only after the fenced write succeeds.
        notification: Option<BackplaneCommand>,
    },
    /// A cold expiration could not read its snapshot. Resolve its frame under
    /// the same commit lane, preserving physical expiry and refusing to expire
    /// a snapshot created after the original operation.
    ColdExpire {
        /// Original logical expiration boundary.
        logical_expiration: Timestamp,
        /// Publish only after expiration has actually been persisted.
        notification: Option<BackplaneCommand>,
    },
    /// Storage already succeeded; retry only the failed notification.
    Notify(BackplaneCommand),
}

/// The failed marker stage, separate from ordinary data replay actions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarkerReplay {
    /// Persist the durable maximum; the caller explicitly skipped notification.
    AdvanceOnly,
    /// Durable atomic max must complete before publication.
    AdvanceAndNotify,
    /// Durable max already succeeded; only publication failed.
    NotifyOnly,
}

/// Exact-identity advancement of a pending operation's remaining stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryStageTransition {
    /// The current ticket now retains only its pending notification.
    Advanced,
    /// The exact ticket already has no storage stage to advance.
    Unchanged,
    /// A replacement ticket has superseded this identity.
    Superseded,
}

/// Closed replay work without changing public RecoveryAction exhaustiveness.
#[derive(Debug, Clone)]
pub enum RecoveryWork {
    /// An ordinary value mutation or its notification.
    Data {
        /// Unchanged public data identity and retry lifetime.
        item: RecoveryItem,
        /// Immutable payload and exact failed stage.
        mutation: PendingMutation,
    },
    /// A namespaced durable invalidation, never a synthetic data key.
    Marker {
        /// Validated source, scope and marker revision.
        command: MarkerCommand,
        /// The exact remaining protocol stage.
        stage: MarkerReplay,
    },
    /// Captured mutation policy and exact durable/population/publication stage.
    MarkerMutation(Arc<MarkerMutationRecovery>),
    /// Expendable observation repair with its original absolute lifetime.
    MarkerSnapshot(Arc<MarkerSnapshotReplay>),
}

impl RecoveryWork {
    /// The legacy data DTO, absent by meaning for marker work.
    #[must_use]
    pub fn item(&self) -> Option<&RecoveryItem> {
        match self {
            Self::Data { item, .. } => Some(item),
            Self::Marker { .. } | Self::MarkerMutation(_) | Self::MarkerSnapshot(_) => None,
        }
    }

    fn key(&self) -> RecoveryKey {
        match self {
            Self::Data { item, .. } => RecoveryKey::Data(item.key.clone()),
            Self::Marker { command, .. } => {
                RecoveryKey::Marker(command.scope().clone(), command.marker().kind().clone())
            }
            Self::MarkerMutation(work) => RecoveryKey::Marker(
                work.command().scope().clone(),
                work.command().marker().kind().clone(),
            ),
            Self::MarkerSnapshot(work) => {
                RecoveryKey::MarkerSnapshot(work.scope().clone(), work.kind().clone())
            }
        }
    }

    fn timestamp(&self) -> Timestamp {
        match self {
            Self::Data { item, .. } => item.timestamp,
            Self::Marker { command, .. } => command.marker().version().timestamp(),
            Self::MarkerMutation(work) => work.command().marker().version().timestamp(),
            Self::MarkerSnapshot(work) => work.snapshot().version().timestamp(),
        }
    }

    fn expires_at(&self) -> Option<Timestamp> {
        match self {
            Self::Data { item, .. } => Some(item.expires_at),
            Self::Marker { .. } | Self::MarkerMutation(_) => None,
            Self::MarkerSnapshot(work) => Some(work.snapshot().physical_expiration()),
        }
    }

    fn supersedes(&self, previous: &Self) -> bool {
        match self {
            Self::MarkerSnapshot(work) => match previous {
                Self::MarkerSnapshot(previous) => work.snapshot().supersedes(previous.snapshot()),
                Self::Data { .. } | Self::Marker { .. } | Self::MarkerMutation(_) => false,
            },
            Self::Data { .. } | Self::Marker { .. } | Self::MarkerMutation(_) => {
                self.timestamp() > previous.timestamp()
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum RecoveryKey {
    Data(Arc<str>),
    Marker(CacheScope, MarkerKind),
    MarkerSnapshot(CacheScope, MarkerKind),
}

/// An exact queue identity, never reused even when the same key is replaced.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RecoveryId(u64);

/// Immutable replay ticket which strongly retains its operation fence/lane.
#[derive(Clone)]
pub struct ReplayTicket {
    id: RecoveryId,
    work: RecoveryWork,
    generation: OperationGeneration,
    fence: Option<Arc<dyn RecoveryFence>>,
}

impl std::fmt::Debug for ReplayTicket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplayTicket")
            .field("id", &self.id)
            .field("work", &self.work)
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}

impl ReplayTicket {
    /// This exact queue operation's immutable payload.
    #[must_use]
    pub fn work(&self) -> &RecoveryWork {
        &self.work
    }

    /// Captured operation generation.
    #[must_use]
    pub const fn generation(&self) -> OperationGeneration {
        self.generation
    }

    /// Strong opaque commit-lane pin, absent only for explicit legacy work.
    #[must_use]
    pub fn fence(&self) -> Option<&Arc<dyn RecoveryFence>> {
        self.fence.as_ref()
    }

    /// Whether the captured operation is still current in its original lane.
    #[must_use]
    pub fn fence_is_current(&self) -> bool {
        self.fence
            .as_ref()
            .is_none_or(|fence| fence.generation() == self.generation && fence.is_current())
    }

    /// Stable identity useful for diagnostics without exposing a reusable constructor.
    #[must_use]
    pub const fn identity(&self) -> RecoveryId {
        self.id
    }
}

/// Successful replay or intentional abandonment, none of which consumes retries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayOutcome {
    /// Dependency continuity is not acknowledged; retain this exact ticket and budget.
    Paused,
    /// The intended side effects succeeded.
    Applied,
    /// A newer mutation made this ticket obsolete.
    Superseded,
    /// Its source physical lifetime elapsed.
    Expired,
}

/// Queue admission is an expected closed outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnqueueOutcome {
    /// A new queue identity was installed.
    Queued(RecoveryId),
    /// A newer operation replaced an existing identity.
    Replaced(RecoveryId),
    /// The original generation/revision is already obsolete.
    Superseded,
    /// Recovery was explicitly disabled.
    Disabled,
    /// Safe capacity eviction could not admit this item.
    CapacityRejected,
    /// Physical lifetime was already over.
    Expired,
    /// No replay attempt was configured.
    BudgetExhausted,
}

/// Explicit successful supersession result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SupersedeOutcome {
    /// Older or equal work was removed.
    Removed,
    /// Nothing eligible was pending.
    Unchanged,
}

/// Owned worker startup outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryStart {
    /// This caller started the worker.
    Started,
    /// A worker already owns the service.
    AlreadyRunning,
    /// Recovery is disabled.
    Disabled,
}

/// Extensible replay behavior; old data executors compile unchanged.
#[allow(
    clippy::double_must_use,
    reason = "async-trait 0.1.89 emits must_use on boxed futures"
)]
#[async_trait]
pub trait RecoveryExecutor: Send + Sync {
    /// The compatible legacy data replay operation.
    async fn replay(&self, item: &RecoveryItem) -> Result<()>;

    /// Canonical typed replay. Core overrides this to check the ticket and fence
    /// while holding the exact pinned commit lane before side effects.
    async fn replay_ticket(&self, ticket: &ReplayTicket) -> Result<ReplayOutcome> {
        if !ticket.fence_is_current() {
            return Ok(ReplayOutcome::Superseded);
        }
        match ticket.work() {
            RecoveryWork::Data { item, .. } => {
                self.replay(item).await.map(|()| ReplayOutcome::Applied)
            }
            RecoveryWork::Marker { .. }
            | RecoveryWork::MarkerMutation(_)
            | RecoveryWork::MarkerSnapshot(_) => {
                Err(RecoveryError::UnsupportedMarkerExecutor.into())
            }
        }
    }
}

#[derive(Clone, Copy)]
enum AttemptState {
    Pending,
    InFlight,
}

struct Queued {
    ticket: ReplayTicket,
    remaining: Option<u64>,
    state: AttemptState,
}
struct QueueState {
    entries: HashMap<RecoveryKey, Queued>,
    next_id: Option<u64>,
}

enum RecoveryBarrier {
    Ready,
    Disconnected,
    After(Instant),
}

struct ReplayClaim<'a> {
    service: &'a AutoRecoveryService,
    ticket: &'a ReplayTicket,
    finished: bool,
}
impl Drop for ReplayClaim<'_> {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let mut queue = self
            .service
            .queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(queued) = queue
            .entries
            .get_mut(&self.ticket.work.key())
            .filter(|queued| queued.ticket.id == self.ticket.id)
        {
            queued.state = AttemptState::Pending;
        }
    }
}

/// Exact-identity bounded recovery and its explicitly owned background worker.
pub struct AutoRecoveryService {
    config: RecoveryConfig,
    clock: Arc<dyn Clock>,
    queue: Mutex<QueueState>,
    executor: OnceLock<Weak<dyn RecoveryExecutor>>,
    worker: Mutex<Option<tokio::task::JoinHandle<()>>>,
    stop: watch::Sender<bool>,
    barrier: Mutex<RecoveryBarrier>,
    shutdown_gate: tokio::sync::Mutex<()>,
}

impl AutoRecoveryService {
    /// Compatible construction; canonical callers validate with `try_new` first.
    #[must_use]
    pub fn new(config: RecoveryConfig, clock: Arc<dyn Clock>) -> Arc<Self> {
        match Self::try_new(config, clock) {
            Ok(service) => service,
            Err(error) => panic!("invalid recovery configuration: {error}"),
        }
    }

    /// Validates construction without starting tasks.
    pub fn try_new(
        config: RecoveryConfig,
        clock: Arc<dyn Clock>,
    ) -> std::result::Result<Arc<Self>, RecoveryError> {
        if config.enabled && config.delay.is_zero() {
            return Err(RecoveryError::ZeroDelay);
        }
        if config.enabled && Instant::now().checked_add(config.delay).is_none() {
            return Err(RecoveryError::InvalidBarrier);
        }
        let (stop, _) = watch::channel(false);
        Ok(Arc::new(Self {
            config,
            clock,
            queue: Mutex::new(QueueState {
                entries: HashMap::new(),
                next_id: Some(1),
            }),
            executor: OnceLock::new(),
            worker: Mutex::new(None),
            stop,
            barrier: Mutex::new(RecoveryBarrier::Ready),
            shutdown_gate: tokio::sync::Mutex::new(()),
        }))
    }

    /// Wires an executor once, returning a typed duplicate-configuration failure.
    pub fn try_set_executor(
        &self,
        executor: Weak<dyn RecoveryExecutor>,
    ) -> std::result::Result<(), RecoveryError> {
        self.executor
            .set(executor)
            .map_err(|_| RecoveryError::ExecutorAlreadyConfigured)
    }

    /// Legacy configuration adapter which reports rather than hides rejection.
    pub fn set_executor(&self, executor: Weak<dyn RecoveryExecutor>) {
        if let Err(error) = self.try_set_executor(executor) {
            tracing::warn!(%error, "recovery executor rejected");
        }
    }

    /// Number of pending or in-flight exact operations.
    #[must_use]
    pub fn len(&self) -> usize {
        self.queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entries
            .len()
    }

    /// Whether no exact work is retained.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Legacy timestamp-ordered data adapter, intentionally unversioned.
    pub fn enqueue(&self, item: RecoveryItem) {
        let work = RecoveryWork::Data {
            item,
            mutation: PendingMutation::Legacy,
        };
        if let Err(error) = self.enqueue_work(work, None) {
            tracing::warn!(%error, "recovery admission failed");
        }
    }

    /// Admits immutable canonical work and strongly pins its original commit lane.
    pub fn enqueue_versioned(
        &self,
        work: RecoveryWork,
        fence: Arc<dyn RecoveryFence>,
    ) -> std::result::Result<EnqueueOutcome, RecoveryError> {
        self.enqueue_work(work, Some(fence))
    }

    /// Marker work may use its atomic maximum without a local data-key lane.
    pub fn enqueue_marker(
        &self,
        command: MarkerCommand,
        stage: MarkerReplay,
    ) -> std::result::Result<EnqueueOutcome, RecoveryError> {
        self.enqueue_work(RecoveryWork::Marker { command, stage }, None)
    }

    pub(crate) fn enqueue_marker_mutation(
        &self,
        work: MarkerMutationRecovery,
    ) -> std::result::Result<EnqueueOutcome, RecoveryError> {
        self.enqueue_work(RecoveryWork::MarkerMutation(Arc::new(work)), None)
    }

    pub(crate) fn enqueue_marker_snapshot(
        &self,
        work: MarkerSnapshotReplay,
    ) -> std::result::Result<EnqueueOutcome, RecoveryError> {
        self.enqueue_work(RecoveryWork::MarkerSnapshot(Arc::new(work)), None)
    }

    fn enqueue_work(
        &self,
        mut work: RecoveryWork,
        fence: Option<Arc<dyn RecoveryFence>>,
    ) -> std::result::Result<EnqueueOutcome, RecoveryError> {
        if !self.config.enabled {
            return Ok(EnqueueOutcome::Disabled);
        }
        if *self.stop.borrow() {
            return Err(RecoveryError::Stopped);
        }
        if work
            .expires_at()
            .is_some_and(|expiry| self.clock.now() >= expiry)
        {
            return Ok(EnqueueOutcome::Expired);
        }
        if fence.as_ref().is_some_and(|fence| !fence.is_current()) {
            return Ok(EnqueueOutcome::Superseded);
        }
        let remaining = work
            .item()
            .and_then(|item| item.remaining_retries)
            .or(self.config.max_retries)
            .map(|retries| u64::from(retries) + 1);
        let key = work.key();
        // Open fence behavior and destruction must never execute under the
        // queue mutex; a valid implementation may inspect this service.
        let generation = fence.as_ref().map(|fence| fence.generation());
        let mut retired = Vec::with_capacity(2);
        let mut queue = self
            .queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *self.stop.borrow() {
            return Err(RecoveryError::Stopped);
        }
        if let Some(existing) = queue.entries.get(&key) {
            let older = match generation {
                Some(generation) if existing.ticket.fence.is_some() => {
                    generation <= existing.ticket.generation
                }
                Some(_) | None => !work.supersedes(&existing.ticket.work),
            };
            if older {
                return Ok(EnqueueOutcome::Superseded);
            }
            if let RecoveryWork::MarkerMutation(previous) = &existing.ticket.work {
                match &mut work {
                    RecoveryWork::MarkerMutation(next) => {
                        *next = Arc::new(next.inherit_compaction(previous)?);
                    }
                    RecoveryWork::Marker {
                        command,
                        stage: MarkerReplay::NotifyOnly,
                    } => {
                        let next = MarkerMutationRecovery::committed_notification(command.clone())
                            .inherit_compaction(previous)?;
                        if next.pending_compaction().is_some() {
                            work = RecoveryWork::MarkerMutation(Arc::new(next));
                        }
                    }
                    RecoveryWork::Data { .. }
                    | RecoveryWork::Marker {
                        stage: MarkerReplay::AdvanceOnly | MarkerReplay::AdvanceAndNotify,
                        ..
                    }
                    | RecoveryWork::MarkerSnapshot(_) => {}
                }
            }
        }
        let replaced = queue.entries.contains_key(&key);
        if !replaced
            && let Some(limit) = self.config.max_items
            && queue.entries.len() >= limit
        {
            let bound = work.expires_at().unwrap_or(Timestamp::MAX);
            let victim = queue
                .entries
                .iter()
                .filter_map(|(key, queued)| {
                    queued
                        .ticket
                        .work
                        .expires_at()
                        .map(|expiry| (key.clone(), expiry))
                })
                .min_by_key(|(_, expiry)| *expiry);
            if let Some((victim, expiry)) = victim
                && expiry < bound
            {
                retired.extend(queue.entries.remove(&victim));
            }
            if queue.entries.len() >= limit {
                return Ok(EnqueueOutcome::CapacityRejected);
            }
        }
        let id = queue.next_id.ok_or(RecoveryError::IdentityExhausted)?;
        queue.next_id = id.checked_add(1);
        let id = RecoveryId(id);
        let ticket = ReplayTicket {
            id,
            work,
            generation: generation.unwrap_or(OperationGeneration::new(id.0)),
            fence,
        };
        retired.extend(queue.entries.insert(
            key,
            Queued {
                ticket,
                remaining,
                state: AttemptState::Pending,
            },
        ));
        Ok(if replaced {
            EnqueueOutcome::Replaced(id)
        } else {
            EnqueueOutcome::Queued(id)
        })
    }

    /// Removes only older/equal data work after a successful canonical mutation.
    pub fn supersede_through(
        &self,
        key: &str,
        generation: OperationGeneration,
    ) -> SupersedeOutcome {
        let mut queue = self
            .queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let key = RecoveryKey::Data(Arc::from(key));
        if queue
            .entries
            .get(&key)
            .is_some_and(|queued| queued.ticket.generation <= generation)
        {
            let retired = queue.entries.remove(&key);
            drop(queue);
            drop(retired);
            SupersedeOutcome::Removed
        } else {
            SupersedeOutcome::Unchanged
        }
    }

    /// Legacy success adapter using the original timestamp ordering boundary.
    pub fn cancel_through(&self, key: &str, at: Timestamp) -> SupersedeOutcome {
        let mut queue = self
            .queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let key = RecoveryKey::Data(Arc::from(key));
        if queue
            .entries
            .get(&key)
            .is_some_and(|queued| queued.ticket.work.timestamp() <= at)
        {
            let retired = queue.entries.remove(&key);
            drop(queue);
            drop(retired);
            SupersedeOutcome::Removed
        } else {
            SupersedeOutcome::Unchanged
        }
    }

    /// Exact queue identity and operation fence check, used inside core's lane.
    #[must_use]
    pub fn is_current(&self, ticket: &ReplayTicket) -> bool {
        ticket.fence_is_current()
            && self
                .queue
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .entries
                .get(&ticket.work.key())
                .is_some_and(|queued| queued.ticket.id == ticket.id)
    }

    /// Advances only this in-flight operation to its remaining notification stage.
    /// The stable identity/fence prevents an old completion changing newer work.
    pub fn notification_stage(&self, ticket: &ReplayTicket) -> RecoveryStageTransition {
        let mut queue = self
            .queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(queued) = queue
            .entries
            .get_mut(&ticket.work.key())
            .filter(|queued| queued.ticket.id == ticket.id)
        else {
            return RecoveryStageTransition::Superseded;
        };
        match &mut queued.ticket.work {
            RecoveryWork::Data { mutation, .. } => match mutation {
                PendingMutation::Commit {
                    notification: Some(command),
                    ..
                }
                | PendingMutation::FencedCommit {
                    notification: Some(command),
                    ..
                }
                | PendingMutation::ColdExpire {
                    notification: Some(command),
                    ..
                } => {
                    *mutation = PendingMutation::Notify(command.clone());
                }
                PendingMutation::Legacy
                | PendingMutation::Notify(_)
                | PendingMutation::Commit {
                    notification: None, ..
                }
                | PendingMutation::FencedCommit {
                    notification: None, ..
                }
                | PendingMutation::ColdExpire {
                    notification: None, ..
                } => return RecoveryStageTransition::Unchanged,
            },
            RecoveryWork::Marker { stage, .. } => match stage {
                MarkerReplay::AdvanceAndNotify => *stage = MarkerReplay::NotifyOnly,
                MarkerReplay::AdvanceOnly | MarkerReplay::NotifyOnly => {
                    return RecoveryStageTransition::Unchanged;
                }
            },
            RecoveryWork::MarkerMutation(work) => match work.stage() {
                MarkerMutationStage::Populate { options, .. }
                    if !options.skip_backplane_notifications() =>
                {
                    let Ok(next) = work.notification() else {
                        return RecoveryStageTransition::Unchanged;
                    };
                    *work = Arc::new(next);
                }
                MarkerMutationStage::Advance { .. }
                | MarkerMutationStage::Populate { .. }
                | MarkerMutationStage::Notify { .. } => return RecoveryStageTransition::Unchanged,
            },
            RecoveryWork::MarkerSnapshot(_) => return RecoveryStageTransition::Unchanged,
        }
        RecoveryStageTransition::Advanced
    }

    pub(crate) fn marker_population_stage(
        &self,
        ticket: &ReplayTicket,
        work: MarkerMutationRecovery,
    ) -> RecoveryStageTransition {
        let mut queue = self
            .queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(queued) = queue
            .entries
            .get_mut(&ticket.work.key())
            .filter(|queued| queued.ticket.id == ticket.id)
        else {
            return RecoveryStageTransition::Superseded;
        };
        queued.ticket.work = RecoveryWork::MarkerMutation(Arc::new(work));
        RecoveryStageTransition::Advanced
    }

    /// Owned diagnostic snapshot of pending durable/population/publication work.
    #[must_use]
    pub fn marker_work(&self, scope: &CacheScope, kind: &MarkerKind) -> Option<ReplayTicket> {
        self.queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entries
            .get(&RecoveryKey::Marker(scope.clone(), kind.clone()))
            .map(|queued| queued.ticket.clone())
    }

    /// Owned diagnostic snapshot of a finite observation repair, separate from durable work.
    #[must_use]
    pub fn marker_snapshot_work(
        &self,
        scope: &CacheScope,
        kind: &MarkerKind,
    ) -> Option<ReplayTicket> {
        self.queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entries
            .get(&RecoveryKey::MarkerSnapshot(scope.clone(), kind.clone()))
            .map(|queued| queued.ticket.clone())
    }

    /// Owned snapshot strongly retains the original lane across asynchronous work.
    #[must_use]
    pub fn snapshot(&self, key: &str) -> Option<ReplayTicket> {
        self.queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entries
            .get(&RecoveryKey::Data(Arc::from(key)))
            .map(|queued| queued.ticket.clone())
    }

    /// Compare-removes exactly this ticket, leaving a newer replacement untouched.
    pub fn complete(&self, ticket: &ReplayTicket) -> SupersedeOutcome {
        let mut queue = self
            .queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let key = ticket.work.key();
        if queue
            .entries
            .get(&key)
            .is_some_and(|queued| queued.ticket.id == ticket.id)
        {
            let retired = queue.entries.remove(&key);
            drop(queue);
            drop(retired);
            SupersedeOutcome::Removed
        } else {
            SupersedeOutcome::Unchanged
        }
    }

    fn record_failure(&self, ticket: &ReplayTicket) {
        let mut queue = self
            .queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let key = ticket.work.key();
        let Some(queued) = queue
            .entries
            .get_mut(&key)
            .filter(|queued| queued.ticket.id == ticket.id)
        else {
            return;
        };
        match queued.remaining {
            Some(0 | 1) => {
                let retired = queue.entries.remove(&key);
                drop(queue);
                drop(retired);
            }
            Some(remaining) => {
                queued.remaining = Some(remaining - 1);
                queued.state = AttemptState::Pending;
            }
            None => {
                queued.state = AttemptState::Pending;
            }
        }
    }

    fn claim(&self, key: &RecoveryKey) -> Option<ReplayTicket> {
        let mut queue = self
            .queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let queued = queue.entries.get_mut(key)?;
        if matches!(queued.state, AttemptState::InFlight) {
            return None;
        }
        queued.state = AttemptState::InFlight;
        Some(queued.ticket.clone())
    }

    /// Replays a claimed snapshot once; replacements remain independently pending.
    pub async fn drain_once(&self, executor: &dyn RecoveryExecutor) {
        let paused = match *self
            .barrier
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
        {
            RecoveryBarrier::Ready => false,
            RecoveryBarrier::Disconnected => true,
            RecoveryBarrier::After(until) => Instant::now() < until,
        };
        if paused {
            return;
        }
        let keys: Vec<_> = self
            .queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entries
            .keys()
            .cloned()
            .collect();
        for key in keys {
            let Some(ticket) = self.claim(&key) else {
                continue;
            };
            if ticket
                .work
                .expires_at()
                .is_some_and(|expiry| self.clock.now() >= expiry)
                || !ticket.fence_is_current()
            {
                self.complete(&ticket);
                continue;
            }
            let mut claim = ReplayClaim {
                service: self,
                ticket: &ticket,
                finished: false,
            };
            match executor.replay_ticket(&ticket).await {
                Ok(ReplayOutcome::Paused) => continue,
                Ok(ReplayOutcome::Applied | ReplayOutcome::Superseded | ReplayOutcome::Expired) => {
                    self.complete(&ticket);
                }
                Err(error) => {
                    tracing::debug!(%error, "recovery attempt failed");
                    self.record_failure(&ticket);
                }
            }
            claim.finished = true;
        }
    }

    /// Pauses replay throughout a continuity gap until a subscription ACK arrives.
    pub fn suspend(&self) {
        *self
            .barrier
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = RecoveryBarrier::Disconnected;
    }

    /// Starts the post-ACK barrier using monotonic time, independently of domain clocks.
    pub fn pause_after_reconnect(&self) -> std::result::Result<(), RecoveryError> {
        let until = Instant::now()
            .checked_add(self.config.delay)
            .ok_or(RecoveryError::InvalidBarrier)?;
        *self
            .barrier
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = RecoveryBarrier::After(until);
        Ok(())
    }

    /// Idempotently starts one explicitly owned worker.
    pub fn try_spawn(self: &Arc<Self>) -> std::result::Result<RecoveryStart, RecoveryError> {
        if !self.config.enabled {
            return Ok(RecoveryStart::Disabled);
        }
        if *self.stop.borrow() {
            return Err(RecoveryError::Stopped);
        }
        let runtime =
            tokio::runtime::Handle::try_current().map_err(|_| RecoveryError::MissingRuntime)?;
        let mut worker = self
            .worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *self.stop.borrow() {
            return Err(RecoveryError::Stopped);
        }
        if worker.is_some() {
            return Ok(RecoveryStart::AlreadyRunning);
        }
        let weak = Arc::downgrade(self);
        let delay = self.config.delay;
        let mut stop = self.stop.subscribe();
        *worker = Some(runtime.spawn(async move {
            let mut ticker = tokio::time::interval(delay);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                if *stop.borrow() {
                    return;
                }
                tokio::select! { biased;
                    _ = stop.changed() => return,
                    _ = ticker.tick() => {}
                }
                let Some(service) = weak.upgrade() else {
                    return;
                };
                let Some(executor) = service.executor.get().and_then(Weak::upgrade) else {
                    if service.executor.get().is_some() {
                        return;
                    }
                    continue;
                };
                tokio::select! { biased;
                    _ = stop.changed() => return,
                    _ = service.drain_once(executor.as_ref()) => {}
                }
            }
        }));
        Ok(RecoveryStart::Started)
    }

    /// Legacy start adapter reports configuration/runtime failures explicitly.
    pub fn spawn(self: &Arc<Self>) {
        if let Err(error) = self.try_spawn() {
            tracing::warn!(%error, "recovery worker could not start");
        }
    }

    /// Begins owned cancellation without keeping the cache alive.
    pub fn stop(&self) {
        self.stop.send_replace(true);
        let retired = {
            let mut queue = self
                .queue
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            std::mem::take(&mut queue.entries)
        };
        drop(retired);
    }

    /// Joins worker cancellation deterministically; repeated calls are harmless.
    pub async fn shutdown(&self) -> std::result::Result<(), RecoveryError> {
        self.stop();
        let _shutdown = self.shutdown_gate.lock().await;
        let worker = self
            .worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(worker) = worker
            && let Err(source) = worker.await
            && !source.is_cancelled()
        {
            return Err(RecoveryError::Task { source });
        }
        Ok(())
    }
}

impl Drop for AutoRecoveryService {
    fn drop(&mut self) {
        self.stop.send_replace(true);
        if let Some(worker) = self
            .worker
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            worker.abort();
        }
    }
}
