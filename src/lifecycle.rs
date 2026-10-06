//! Supervised cache tasks, preserving panic/cleanup evidence without public
//! Cache handles in worker futures.
use crate::commit::TaskResult;
use crate::error::{Error, FactoryCancellationReason, Result, ShutdownFailure, ShutdownTask};
use crate::events::{CacheEvent, Events};
use crate::execution::lock;
use std::collections::VecDeque;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::{oneshot, watch};

pub(crate) struct Tasks {
    handles: std::sync::Mutex<VecDeque<TrackedTask>>,
    failures: std::sync::Mutex<Vec<ShutdownFailure>>,
    join: tokio::sync::Mutex<()>,
    runtime: std::sync::OnceLock<tokio::runtime::Handle>,
}
struct TrackedTask {
    task: ShutdownTask,
    join: tokio::task::JoinHandle<()>,
    finished: watch::Receiver<bool>,
}
impl Tasks {
    pub(crate) fn new() -> Arc<Self> {
        let runtime = std::sync::OnceLock::new();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let _ = runtime.set(handle);
        }
        Arc::new(Self {
            handles: std::sync::Mutex::new(VecDeque::new()),
            failures: std::sync::Mutex::new(Vec::new()),
            join: tokio::sync::Mutex::new(()),
            runtime,
        })
    }
    pub(crate) fn spawn<T: Send + 'static>(
        self: &Arc<Self>,
        task: ShutdownTask,
        key: Arc<str>,
        events: Events,
        work: impl Future<Output = Result<T>> + Send + 'static,
    ) -> oneshot::Receiver<TaskResult<T>> {
        match self.executor() {
            Ok(runtime) => {
                let worker = runtime.spawn(work);
                self.supervise(runtime, worker, task, key, events)
            }
            Err(error) => Self::rejected(error),
        }
    }
    pub(crate) fn spawn_blocking<T: Send + 'static>(
        self: &Arc<Self>,
        task: ShutdownTask,
        key: Arc<str>,
        events: Events,
        cancellation: crate::FactoryCancellation,
        launch: impl Future<Output = Result<tokio::task::JoinHandle<Result<T>>>> + Send + 'static,
    ) -> oneshot::Receiver<TaskResult<T>> {
        match self.executor() {
            Ok(runtime) => self.supervise(
                runtime,
                wait_blocking(launch, cancellation),
                task,
                key,
                events,
            ),
            Err(error) => Self::rejected(error),
        }
    }
    #[cfg(test)]
    pub(crate) fn tracked_count(&self) -> usize {
        lock(&self.handles).len()
    }
    fn executor(&self) -> Result<tokio::runtime::Handle> {
        let runtime = self
            .runtime
            .get()
            .cloned()
            .or_else(|| tokio::runtime::Handle::try_current().ok())
            .ok_or(crate::ConfigError::MissingRuntime {
                component: crate::RuntimeComponent::Execution,
            })?;
        let _ = self.runtime.set(runtime.clone());
        Ok(runtime)
    }
    fn rejected<T>(error: Error) -> oneshot::Receiver<TaskResult<T>> {
        let (sender, receiver) = oneshot::channel();
        let _ = sender.send(TaskResult::Completed(Err(error)));
        receiver
    }
    fn supervise<T: Send + 'static>(
        self: &Arc<Self>,
        runtime: tokio::runtime::Handle,
        worker: impl Future<Output = std::result::Result<Result<T>, tokio::task::JoinError>>
        + Send
        + 'static,
        task: ShutdownTask,
        key: Arc<str>,
        events: Events,
    ) -> oneshot::Receiver<TaskResult<T>> {
        let (sender, receiver) = oneshot::channel();
        let tasks = Arc::clone(self);
        let (finished, completion) = watch::channel(false);
        let supervisor = runtime.spawn(async move {
            let result = match worker.await {
                Ok(result) => {
                    if let Err(error) = &result
                        && !matches!(
                            error,
                            Error::OperationCancelled {
                                reason: FactoryCancellationReason::CacheShutdown
                            } | Error::CacheClosed
                        )
                    {
                        tracing::warn!(%error,key=%key,"cache-owned background operation failed");
                        events.emit(if task == ShutdownTask::Factory {
                            CacheEvent::BackgroundFactoryError {
                                key: Arc::clone(&key),
                                message: error.to_string(),
                            }
                        } else {
                            CacheEvent::BackgroundCommitError {
                                key: Arc::clone(&key),
                                message: error.to_string(),
                            }
                        });
                    }
                    TaskResult::Completed(result)
                }
                Err(error) => {
                    let error = Arc::new(error);
                    tracing::error!(%error,key=%key,"cache-owned task panicked");
                    events.emit(if task == ShutdownTask::Factory {
                        CacheEvent::BackgroundFactoryError {
                            key,
                            message: error.to_string(),
                        }
                    } else {
                        CacheEvent::BackgroundCommitError {
                            key,
                            message: error.to_string(),
                        }
                    });
                    lock(&tasks.failures).push(ShutdownFailure::BackgroundTask {
                        task,
                        source: Arc::clone(&error),
                    });
                    TaskResult::Panicked(error)
                }
            };
            let _ = sender.send(result);
            finished.send_replace(true);
        });
        let mut handles = lock(&self.handles);
        let count = handles.len().min(4);
        for _ in 0..count {
            if let Some(old) = handles.pop_front()
                && !old.join.is_finished()
            {
                handles.push_back(old);
            }
        }
        handles.push_back(TrackedTask {
            task,
            join: supervisor,
            finished: completion,
        });
        receiver
    }
    pub(crate) async fn flush(&self) {
        loop {
            let pending: Vec<_> = lock(&self.handles)
                .iter()
                .filter(|task| {
                    matches!(
                        task.task,
                        ShutdownTask::Factory
                            | ShutdownTask::Distributed
                            | ShutdownTask::LeaseRelease
                            | ShutdownTask::MemoryLockerAcquisition
                    ) && !*task.finished.borrow()
                })
                .map(|task| task.finished.clone())
                .collect();
            if pending.is_empty() {
                return;
            }
            for mut completion in pending {
                while !*completion.borrow_and_update() {
                    if completion.changed().await.is_err() {
                        break;
                    }
                }
            }
        }
    }
    pub(crate) fn cleanup(
        self: &Arc<Self>,
        key: Arc<str>,
        events: Events,
        work: impl Future<Output = Result<()>> + Send + 'static,
    ) {
        self.cleanup_task(ShutdownTask::LeaseRelease, key, events, work);
    }
    pub(crate) fn cleanup_task(
        self: &Arc<Self>,
        task: ShutdownTask,
        key: Arc<str>,
        events: Events,
        work: impl Future<Output = Result<()>> + Send + 'static,
    ) {
        if let Err(error) = self.executor() {
            lock(&self.failures).push(ShutdownFailure::Work(error));
            return;
        }
        let tasks = Arc::clone(self);
        let _receiver = self.spawn(task, Arc::clone(&key), events.clone(), async move {
            if let Err(error) = work.await {
                tracing::warn!(%error,key=%key,"owned cleanup failed");
                events.emit(CacheEvent::BackgroundCommitError {
                    key,
                    message: error.to_string(),
                });
                lock(&tasks.failures).push(ShutdownFailure::Work(error));
            }
            Ok(())
        });
    }
    pub(crate) async fn drain(&self) {
        let _join = self.join.lock().await;
        loop {
            let handle = lock(&self.handles).pop_front();
            match handle {
                Some(handle) => {
                    if let Err(error) = handle.join.await {
                        lock(&self.failures).push(ShutdownFailure::BackgroundTask {
                            task: handle.task,
                            source: Arc::new(error),
                        });
                    }
                }
                None => return,
            }
        }
    }
    pub(crate) fn take_failures(&self) -> Vec<ShutdownFailure> {
        std::mem::take(&mut *lock(&self.failures))
    }
}

async fn wait_blocking<T: Send + 'static>(
    launch: impl Future<Output = Result<tokio::task::JoinHandle<Result<T>>>>,
    cancellation: crate::FactoryCancellation,
) -> std::result::Result<Result<T>, tokio::task::JoinError> {
    let mut launch = std::pin::pin!(launch);
    let mut worker = tokio::select! {
        biased;
        reason = cancellation.cancelled() => return Ok(Err(Error::OperationCancelled { reason })),
        result = &mut launch => match result {
            Ok(worker) => worker,
            Err(error) => return Ok(Err(error)),
        }
    };
    tokio::select! {
        biased;
        result = &mut worker => result,
        reason = cancellation.cancelled() => {
            // Dispatch has its own permit; unrelated work cannot indefinitely
            // occupy this callback's pool slot. A started callback must join.
            worker.abort();
            match worker.await {
                Err(error) if error.is_cancelled() => Ok(Err(Error::OperationCancelled { reason })),
                result => result,
            }
        }
    }
}
