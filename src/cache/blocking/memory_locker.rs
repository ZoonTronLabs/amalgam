//! Explicit native provider dispatch without an ambient mode selector.
use super::{BlockingRuntime, runtime};
use crate::cache::{Cache, PublicLifetime, WorkAdmission};
use crate::commit::TaskResult;
use crate::execution::{CancellationSource, LinkMode, Scopes};
use crate::lifecycle::Tasks;
use crate::{
    Error, Events, FactoryCancellationReason as Reason, advanced::ShutdownTask,
    provider::BlockingMemoryLocker, provider::MemoryLock, provider::MemoryLockGuard,
    provider::MemoryLockOutcome, provider::MemoryLockRequest, provider::MemoryLockerError,
};
use std::sync::Arc;

pub(in crate::cache) struct NativeMemoryView<V: Clone + Send + Sync + 'static> {
    source: Cache<V>,
    runtime: BlockingRuntime,
}
impl<V: Clone + Send + Sync + 'static> NativeMemoryView<V> {
    pub(in crate::cache) fn bind(source: Cache<V>, runtime: &BlockingRuntime) -> Cache<V> {
        if !source.inner.locks.has_blocking_acquirer() {
            return source;
        }
        Cache {
            inner: Arc::clone(&source.inner),
            lifetime: Arc::new(PublicLifetime::NativeMemory(Self {
                source,
                runtime: runtime.clone(),
            })),
        }
    }
    pub(in crate::cache) fn source(&self) -> &Cache<V> {
        &self.source
    }
    #[cold]
    pub(in crate::cache) fn work(&self) -> WorkAdmission {
        WorkAdmission::native(NativeMemoryWork {
            scopes: self.source.operation_scopes(),
            tasks: Arc::clone(&self.source.inner.tasks),
            events: self.source.inner.events.clone(),
            runtime: self.runtime.clone(),
            lineage: self.runtime.lineage(),
        })
    }
}

pub(crate) struct NativeMemoryWork {
    scopes: Arc<Scopes>,
    tasks: Arc<Tasks>,
    events: Events,
    runtime: BlockingRuntime,
    lineage: runtime::FactoryLineage,
}
pub(crate) enum MemoryAcquireRoute<'work> {
    Asynchronous,
    Native(&'work NativeMemoryWork),
}
impl NativeMemoryWork {
    pub(in crate::cache) fn scopes(&self) -> &Arc<Scopes> {
        &self.scopes
    }
    pub(crate) async fn acquire(
        &self,
        provider: Arc<dyn BlockingMemoryLocker>,
        request: MemoryLockRequest,
    ) -> AcquisitionResult {
        let key: Arc<str> = Arc::from(request.key());
        let parent = request.cancellation().clone();
        let execution = self.execution(provider, request, &parent);
        let receiver = self.dispatch(key, parent, execution)?;
        acquisition_result(receiver.await)
    }
    fn execution(
        &self,
        provider: Arc<dyn BlockingMemoryLocker>,
        request: MemoryLockRequest,
        parent: &crate::FactoryCancellation,
    ) -> AcquisitionExecution {
        let token = parent.clone();
        let scopes = Arc::clone(&self.scopes);
        let work = async move {
            let _callback = runtime::callback_scope(&scopes);
            token.check()?;
            let outcome = provider
                .acquire(request)
                .map(|value| protect_release(value, &scopes));
            token.check()?;
            Ok(outcome)
        };
        let execution = self.scopes.execution(work, CancellationSource::new());
        execution.link(parent, LinkMode::Explicit);
        execution
    }
    fn dispatch(
        &self,
        key: Arc<str>,
        parent: crate::FactoryCancellation,
        execution: AcquisitionExecution,
    ) -> std::result::Result<AcquisitionReceiver, MemoryLockerError> {
        let executor = self
            .runtime
            .memory_lock_executor(&self.lineage)
            .map_err(MemoryLockerError::from_source)?;
        let launch = launch(executor, self.runtime.clone(), execution);
        Ok(self.tasks.spawn_blocking(
            ShutdownTask::MemoryLockerAcquisition,
            key,
            self.events.clone(),
            parent,
            launch,
        ))
    }
}
type AcquisitionResult = std::result::Result<MemoryLockOutcome, MemoryLockerError>;
type AcquisitionExecution = crate::execution::Execution<AcquisitionResult>;
type AcquisitionReceiver = tokio::sync::oneshot::Receiver<TaskResult<AcquisitionResult>>;
async fn launch(
    executor: runtime::FactoryExecutor,
    runtime: BlockingRuntime,
    execution: AcquisitionExecution,
) -> crate::Result<tokio::task::JoinHandle<crate::Result<AcquisitionResult>>> {
    let permit = executor
        .permit()
        .await
        .map_err(MemoryLockerError::from_source)?;
    Ok(executor.handle().spawn_blocking(move || {
        let _permit = permit;
        let _lineage = executor.enter();
        runtime.run(execution)
    }))
}
fn acquisition_result(
    result: std::result::Result<
        TaskResult<AcquisitionResult>,
        tokio::sync::oneshot::error::RecvError,
    >,
) -> AcquisitionResult {
    match result {
        Ok(TaskResult::Completed(Ok(outcome))) => outcome,
        Ok(TaskResult::Completed(Err(Error::OperationCancelled { reason }))) => {
            Err(MemoryLockerError::Cancelled { reason })
        }
        Ok(TaskResult::Completed(Err(Error::CacheClosed))) | Err(_) => {
            Err(MemoryLockerError::Cancelled {
                reason: Reason::CacheShutdown,
            })
        }
        Ok(TaskResult::Completed(Err(error))) => Err(MemoryLockerError::from_source(error)),
        Ok(TaskResult::Panicked(source)) => Err(MemoryLockerError::Provider { source }),
    }
}
fn protect_release(outcome: MemoryLockOutcome, scopes: &Arc<Scopes>) -> MemoryLockOutcome {
    match outcome {
        MemoryLockOutcome::Acquired(lock) => {
            MemoryLockOutcome::Acquired(MemoryLock::new(NativeRelease {
                lock,
                scopes: Arc::clone(scopes),
            }))
        }
        MemoryLockOutcome::Unavailable => MemoryLockOutcome::Unavailable,
    }
}
struct NativeRelease {
    lock: MemoryLock,
    scopes: Arc<Scopes>,
}
impl MemoryLockGuard for NativeRelease {
    fn release(self: Box<Self>) -> std::result::Result<(), MemoryLockerError> {
        let _callback = runtime::callback_scope(&self.scopes);
        self.lock.release()
    }
}
