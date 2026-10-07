//! Typed commit receipts and pinned per-key mutation ordering.
use crate::error::{Error, Result};
use crate::execution::lock;
use crate::locking::{CoordinationPlan, WeakSlots};
use crate::memory::MemoryAdmission;
use crate::recovery::{OperationGeneration, RecoveryError, RecoveryFence};
use crate::time::Timestamp;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

mod ordered;
pub(crate) use ordered::LaneGuard;
use ordered::{Admission, QueueMap, QueueRef};

/// Reason an optional storage/notification stage was omitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkipReason {
    /// The caller explicitly disabled this stage.
    Policy,
    /// The absolute physical lifetime elapsed before the stage ran.
    PhysicallyExpired,
    /// A newer local mutation superseded deferred work.
    Superseded,
}
/// An actual stage result, including policy-suppressed failure evidence.
#[derive(Debug)]
pub enum EffectOutcome {
    /// All stage outcomes of a batch, retaining every suppressed failure.
    Batch(EffectBatch),
    /// No provider was configured.
    NotConfigured,
    /// Policy or lifetime omitted this stage.
    Skipped(SkipReason),
    /// The effect was acknowledged.
    Applied,
    /// Exact immutable work was admitted to recovery after this failure.
    RecoveryQueued {
        /// Original failure.
        cause: Error,
    },
    /// The configured rethrow policy suppressed an observed failure.
    FailedSuppressed {
        /// Original failure.
        cause: Error,
    },
}
/// A nonempty collection of acknowledged/degraded stage outcomes.
#[derive(Debug)]
pub struct EffectBatch {
    stages: Box<[EffectOutcome]>,
}
impl EffectBatch {
    /// Creates a batch with an explicit mandatory first result.
    pub fn new(first: EffectOutcome, remaining: impl IntoIterator<Item = EffectOutcome>) -> Self {
        let mut stages = vec![first];
        stages.extend(remaining);
        Self {
            stages: stages.into_boxed_slice(),
        }
    }
    /// Every original stage result, in execution order.
    pub fn stages(&self) -> &[EffectOutcome] {
        &self.stages
    }
}
pub(crate) fn effects(outcomes: Vec<EffectOutcome>) -> EffectOutcome {
    let mut outcomes = outcomes.into_iter();
    match outcomes.next() {
        None => EffectOutcome::NotConfigured,
        Some(first) => match outcomes.next() {
            None => first,
            Some(second) => EffectOutcome::Batch(EffectBatch::new(
                first,
                std::iter::once(second).chain(outcomes),
            )),
        },
    }
}
pub(crate) fn reports(reports: Vec<CommitReport>) -> CommitReport {
    let mut distributed = Vec::with_capacity(reports.len());
    let mut backplane = Vec::with_capacity(reports.len());
    for report in reports {
        distributed.push(report.distributed);
        backplane.push(report.backplane);
    }
    CommitReport {
        local: LocalEffect::Invalidated,
        distributed: effects(distributed),
        backplane: effects(backplane),
    }
}
/// The in-process effect of a mutation.
#[derive(Debug, Clone, Copy)]
pub enum LocalEffect {
    /// The memory store evaluated admission.
    Stored(MemoryAdmission),
    /// The key was removed.
    Removed,
    /// The key was logically expired.
    Expired,
    /// Local invalidation markers advanced.
    Invalidated,
    /// The caller disabled memory writes.
    Skipped,
}
/// A completed mutation's acknowledged/degraded stages.
#[derive(Debug)]
pub struct CommitReport {
    /// In-process admission or invalidation.
    pub local: LocalEffect,
    /// Distributed persistence outcome.
    pub distributed: EffectOutcome,
    /// Ordered peer-publication outcome.
    pub backplane: EffectOutcome,
}
/// Foreground completion or deliberately transferred cache-owned work.
#[derive(Debug)]
pub enum MutationReceipt {
    /// Every requested stage has completed.
    Completed(CommitReport),
    /// Remaining work is owned and supervised by the cache.
    Scheduled(CommitCompletion),
}
impl MutationReceipt {
    /// Waits for the actual commit, preserving an original typed failure.
    pub async fn wait(self) -> Result<CommitReport> {
        match self {
            Self::Completed(report) => Ok(report),
            Self::Scheduled(completion) => completion.wait().await,
        }
    }
}
pub(crate) enum TaskResult<T> {
    Completed(Result<T>),
    Panicked(Arc<tokio::task::JoinError>),
}
/// Completion of a supervised cache-owned commit. Dropping it does not detach
/// guards or remove error observation.
pub struct CommitCompletion {
    pub(crate) receiver: oneshot::Receiver<TaskResult<CommitReport>>,
}
impl std::fmt::Debug for CommitCompletion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommitCompletion").finish_non_exhaustive()
    }
}
impl CommitCompletion {
    /// Waits for the actual ordered storage/publication pipeline.
    pub async fn wait(self) -> Result<CommitReport> {
        match self.receiver.await {
            Ok(TaskResult::Completed(result)) => result,
            Ok(TaskResult::Panicked(error)) => panic!("background cache commit panicked: {error}"),
            Err(_) => Err(Error::CacheClosed),
        }
    }
}
/// Whether retrieval initiated a cache commit.
#[derive(Debug)]
pub enum CommitReceipt {
    /// Retrieval served an existing value without starting distributed work.
    Unchanged,
    /// Retrieval produced a cache mutation.
    Mutation(MutationReceipt),
}
/// Retrieved value with optional actual commit completion.
#[derive(Debug)]
pub struct CacheValue<V> {
    /// Isolated caller value.
    pub value: V,
    /// Actual mutation receipt, when a factory committed a product.
    pub commit: CommitReceipt,
}

pub(crate) struct KeyLane {
    admission: Admission,
    pub(crate) generation: AtomicU64,
    pub(crate) timestamp: Mutex<Option<Timestamp>>,
}
impl KeyLane {
    fn new(queue: QueueRef) -> Self {
        Self {
            admission: Admission::new(queue),
            generation: AtomicU64::new(0),
            timestamp: Mutex::new(None),
        }
    }
    pub(crate) fn snapshot(self: Arc<Self>, epoch: &Arc<AtomicU64>) -> Fence {
        let generation = OperationGeneration::new(self.generation.load(Ordering::Acquire));
        Fence {
            lane: self,
            generation,
            epoch: Arc::clone(epoch),
            captured_epoch: epoch.load(Ordering::Acquire),
        }
    }
    fn advance_revision(&self) -> Result<OperationGeneration> {
        #[allow(deprecated, reason = "Atomic::try_update is unavailable on Rust 1.88")]
        let generation = self
            .generation
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |old| {
                old.checked_add(1)
            })
            .map_err(|_| RecoveryError::GenerationExhausted)?
            + 1;
        Ok(OperationGeneration::new(generation))
    }
    pub(crate) fn advance(
        self: &Arc<Self>,
        at: Timestamp,
        epoch: &Arc<AtomicU64>,
    ) -> Result<Arc<Fence>> {
        let generation = self.advance_revision()?.value();
        let mut stamp = lock(&self.timestamp);
        *stamp = Some(stamp.map_or(at, |old| old.max(at)));
        drop(stamp);
        Ok(Arc::new(Fence {
            lane: Arc::clone(self),
            generation: OperationGeneration::new(generation),
            epoch: Arc::clone(epoch),
            captured_epoch: epoch.load(Ordering::Acquire),
        }))
    }
}
pub(crate) struct Fence {
    pub(crate) lane: Arc<KeyLane>,
    generation: OperationGeneration,
    epoch: Arc<AtomicU64>,
    captured_epoch: u64,
}
impl RecoveryFence for Fence {
    fn generation(&self) -> OperationGeneration {
        self.generation
    }
    fn is_current(&self) -> bool {
        self.lane.generation.load(Ordering::Acquire) == self.generation.value()
    }
}
impl Fence {
    pub(crate) fn passive_is_current(&self) -> bool {
        self.is_current() && self.epoch.load(Ordering::Acquire) == self.captured_epoch
    }

    pub(crate) fn continuity_stamp(&self) -> crate::entry::ContinuityStamp {
        crate::entry::ContinuityStamp::new(Arc::clone(&self.epoch), self.captured_epoch)
    }
}
pub(crate) struct Lanes {
    slots: WeakSlots<KeyLane>,
    queues: QueueMap,
}
impl Lanes {
    pub(crate) fn new(plan: CoordinationPlan) -> Self {
        Self {
            slots: plan.slots(),
            queues: QueueMap::new(),
        }
    }
    pub(crate) fn get(&self, key: &str) -> Arc<KeyLane> {
        self.slots.get(key, || KeyLane::new(self.queues.bind(key)))
    }
    pub(crate) fn capture(&self, key: &str, epoch: &Arc<AtomicU64>) -> Fence {
        self.slots.get_with(
            key,
            || KeyLane::new(self.queues.bind(key)),
            |lane| lane.snapshot(epoch),
        )
    }
    pub(crate) fn clean(&self, budget: usize) {
        self.slots.clean(budget);
    }
}
