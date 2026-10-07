//! Native caller-thread operations over the shared cache contracts.
//!
//! The executor stays driven while callers block. Timed, cancellable and eager
//! factories run on its blocking pool and retain counted execution and join
//! supervision until their actual callback, captures and result have finished.
use super::*;
use crate::commit::TaskResult;
use crate::factory::FactoryInvocation;
use std::thread::{self, ThreadId};
mod api;
mod requests;
pub use requests::{BlockingReceiptRequest, BlockingRequest};
mod memory_locker;
mod runtime;
pub(super) use memory_locker::NativeMemoryView;
pub(crate) use memory_locker::{MemoryAcquireRoute, NativeMemoryWork};
use runtime::FactoryLineage;
pub use runtime::{
    BlockingDispatchError, BlockingRuntime, BlockingRuntimeError, BlockingThreadPool,
};

pub(super) fn check_drain(owner: &Arc<Scopes>, operation: crate::DrainOperation) -> Result<()> {
    runtime::check_drain(owner, operation)
}

/// Fallible executor creation and validated cache construction remain distinct.
#[derive(Debug, thiserror::Error)]
pub enum BlockingCacheBuildError {
    /// The operating system could not start the executor.
    #[error("cannot create cache executor: {0}")]
    Executor(#[source] BlockingRuntimeError),
    /// The existing cache builder rejected its configuration.
    #[error("invalid cache configuration: {0}")]
    Cache(#[source] Error),
}
/// Synchronous operations sharing the async engine's entries and coordination.
/// Dropping the final clone requests close and owns asynchronous drainage;
/// shutdown waits for all actual work and reports its failures synchronously.
pub struct BlockingCache<V: Clone + Send + Sync + 'static> {
    cache: Cache<V>,
    runtime: BlockingRuntime,
}
impl<V: Clone + Send + Sync + 'static> Clone for BlockingCache<V> {
    fn clone(&self) -> Self {
        Self {
            cache: self.cache.clone(),
            runtime: self.runtime.clone(),
        }
    }
}

/// Actual synchronous commit completion, with stage-specific payloads.
#[derive(Debug)]
pub enum BlockingMutationReceipt {
    /// Foreground effects already completed.
    Completed(CommitReport),
    /// The owning cache still has scheduled effects.
    Scheduled(BlockingCommitCompletion),
}
/// A scheduled commit keeps its executor alive while being waited on.
pub struct BlockingCommitCompletion {
    completion: CommitCompletion,
    runtime: BlockingRuntime,
}
impl std::fmt::Debug for BlockingCommitCompletion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockingCommitCompletion")
            .finish_non_exhaustive()
    }
}
impl BlockingCommitCompletion {
    /// Waits for ordered storage/publication and their owned cleanup.
    pub fn wait(self) -> Result<CommitReport> {
        self.runtime.run(self.completion.wait())
    }
}
impl BlockingMutationReceipt {
    /// Waits the actual effects, rather than treating scheduling as completion.
    pub fn wait(self) -> Result<CommitReport> {
        match self {
            Self::Completed(report) => Ok(report),
            Self::Scheduled(work) => work.wait(),
        }
    }
}
/// Whether retrieval produced a mutation requiring completion observation.
#[derive(Debug)]
pub enum BlockingCommitReceipt {
    /// An existing value was served.
    Unchanged,
    /// A factory produced a mutation.
    Mutation(BlockingMutationReceipt),
}
/// Retrieved value and its actual synchronous commit receipt.
#[derive(Debug)]
pub struct BlockingCacheValue<V> {
    /// Isolated caller value.
    pub value: V,
    /// Exact mutation ownership.
    pub commit: BlockingCommitReceipt,
}

#[derive(Clone, Copy)]
enum CallerCancellation {
    Absent,
    Explicit,
}
struct FactoryDispatch {
    caller: ThreadId,
    cancellation: CallerCancellation,
    default_present: bool,
    lineage: FactoryLineage,
}
impl FactoryDispatch {
    fn inline<V>(&self, context: &FactoryContext<V>) -> bool {
        match context.invocation() {
            FactoryInvocation::EagerRefresh => false,
            FactoryInvocation::Foreground => {
                matches!(self.cancellation, CallerCancellation::Absent)
                    && self.caller == thread::current().id()
                    && context.options().appropriate_factory_timeout(
                        context.has_stale_value() || self.default_present,
                    ) == Timeout::Infinite
            }
        }
    }
}

async fn run_factory<V, F>(
    worker: Worker<V>,
    runtime: BlockingRuntime,
    dispatch: FactoryDispatch,
    factory: F,
    context: FactoryContext<V>,
) -> std::result::Result<V, FactoryError>
where
    V: Clone + Send + Sync + 'static,
    F: FnOnce(FactoryContext<V>) -> std::result::Result<V, FactoryError> + Send + 'static,
{
    if dispatch.inline(&context) {
        let _callback = runtime::callback_scope(worker.scopes());
        return factory(context);
    }
    let key: Arc<str> = Arc::from(context.key());
    let context_invocation = context.invocation();
    let parent = context.cancellation().clone();
    // Registration precedes dispatch, including cancellation before pool start.
    let scopes = Arc::clone(worker.scopes());
    let execution = worker.scopes().execution(
        async move {
            let _callback = runtime::callback_scope(&scopes);
            Ok(factory(context))
        },
        CancellationSource::new(),
    );
    execution.link(&parent, LinkMode::Explicit);
    let independent;
    let lineage = match context_invocation {
        FactoryInvocation::Foreground => &dispatch.lineage,
        FactoryInvocation::EagerRefresh => {
            independent = FactoryLineage::independent();
            &independent
        }
    };
    let executor = runtime
        .factory_executor(lineage)
        .map_err(FactoryError::from_source)?;
    let launch = async move {
        let permit = executor.permit().await.map_err(FactoryError::from_source)?;
        Ok(executor.handle().spawn_blocking(move || {
            let _permit = permit;
            let _lineage = executor.enter();
            runtime.run(execution)
        }))
    };
    let receiver = worker.inner.tasks.spawn_blocking(
        ShutdownTask::Factory,
        key,
        worker.inner.events.clone(),
        parent,
        launch,
    );
    match receiver.await {
        Ok(TaskResult::Completed(Ok(result))) => result,
        Ok(TaskResult::Completed(Err(
            Error::OperationCancelled { reason } | Error::FactoryCancelled { reason },
        ))) => Err(FactoryError::cancelled(reason)),
        Ok(TaskResult::Completed(Err(Error::CacheClosed))) | Err(_) => {
            Err(FactoryError::cancelled(Reason::CacheShutdown))
        }
        Ok(TaskResult::Completed(Err(error))) => Err(FactoryError::from_source(error)),
        Ok(TaskResult::Panicked(error)) => Err(FactoryError::from_source(error)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroUsize;
    use std::sync::Barrier;
    enum PublicHandle {
        Native(BlockingCache<u64>),
        Async(Cache<u64>),
    }

    #[test]
    fn concurrent_final_clones_complete_the_owned_shutdown() {
        let runtime = BlockingRuntime::with_workers(
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(2).unwrap(),
        )
        .unwrap();
        for _ in 0..32 {
            let cache =
                BlockingCache::<u64>::on_runtime(Cache::builder(), runtime.clone()).unwrap();
            let inner = Arc::clone(&cache.cache.inner);
            let barrier = Arc::new(Barrier::new(8));
            let clones: Vec<_> = (0..8)
                .map(|index| {
                    if index % 2 == 0 {
                        PublicHandle::Native(cache.clone())
                    } else {
                        PublicHandle::Async(cache.as_async().clone())
                    }
                })
                .collect();
            drop(cache);
            let threads: Vec<_> = clones
                .into_iter()
                .map(|cache| {
                    let barrier = Arc::clone(&barrier);
                    thread::spawn(move || {
                        barrier.wait();
                        match cache {
                            PublicHandle::Native(handle) => drop(handle),
                            PublicHandle::Async(handle) => drop(handle),
                        }
                    })
                })
                .collect();
            for thread in threads {
                thread.join().unwrap();
            }
            runtime.run(async {
                tokio::time::timeout(Duration::from_secs(2), async {
                    loop {
                        let closed = match &*lock(&inner.lifecycle) {
                            Lifecycle::Closed(result) => Some(result.clone()),
                            Lifecycle::Running | Lifecycle::Closing => None,
                        };
                        if let Some(result) = closed {
                            result.unwrap();
                            return;
                        }
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                })
                .await
                .expect("final public clones must complete shutdown");
            });
        }
    }

    #[test]
    fn queued_cancel_shutdown_does_not_wait_for_another_cache_callback() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::{Condvar, Mutex, mpsc};
        #[derive(Clone, Default)]
        struct Gate(Arc<(Mutex<bool>, Condvar)>);
        impl Gate {
            fn wait(&self) {
                let (state, wake) = &*self.0;
                let mut open = state.lock().unwrap();
                while !*open {
                    open = wake.wait(open).unwrap();
                }
            }
            fn open(&self) {
                let (state, wake) = &*self.0;
                *state.lock().unwrap() = true;
                wake.notify_all();
            }
        }
        struct Release(Gate);
        impl Drop for Release {
            fn drop(&mut self) {
                self.0.open();
            }
        }
        struct Capture(Arc<AtomicUsize>);
        impl Drop for Capture {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let runtime = BlockingRuntime::with_workers(
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        )
        .unwrap();
        let busy = BlockingCache::<u64>::on_runtime(Cache::builder(), runtime.clone()).unwrap();
        let queued = BlockingCache::<u64>::on_runtime(Cache::builder(), runtime.clone()).unwrap();
        let release = Release(Gate::default());
        let gate = release.0.clone();
        let (started_tx, started_rx) = mpsc::channel();
        let occupied = busy.clone();
        let occupier = thread::spawn(move || {
            occupied.get_or_set_cancellable(
                "busy",
                move |ctx| {
                    started_tx.send(()).unwrap();
                    gate.wait();
                    Ok::<_, crate::FactoryError>(ctx.value(1))
                },
                CancellationSource::new().token(),
            )
        });
        started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let captured = Capture(Arc::clone(&drops));
        let invoked = Arc::clone(&calls);
        let source = CancellationSource::new();
        let token = source.token();
        let request = queued.clone();
        let caller = thread::spawn(move || {
            request.get_or_set_cancellable(
                "queued",
                move |ctx| {
                    let _held = &captured;
                    invoked.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, crate::FactoryError>(ctx.value(99))
                },
                token,
            )
        });
        runtime.run(async {
            tokio::time::timeout(Duration::from_secs(2), async {
                while queued.cache.inner.tasks.tracked_count() == 0 {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .unwrap();
        });
        source.cancel();
        let answer = caller.join().unwrap();
        let closing = queued.clone();
        let (drained_tx, drained_rx) = mpsc::channel();
        let closer = thread::spawn(move || {
            drained_tx.send(closing.shutdown()).unwrap();
        });
        let before_release = drained_rx.recv_timeout(Duration::from_millis(150));
        release.0.open();
        occupier.join().unwrap().unwrap();
        closer.join().unwrap();
        queued.shutdown().unwrap();
        busy.shutdown().unwrap();
        assert!(matches!(
            answer,
            Err(Error::OperationCancelled {
                reason: Reason::CallerCancelled
            }) | Err(Error::FactoryCancelled {
                reason: Reason::CallerCancelled
            })
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        before_release
            .expect("cancelled queued work must drain while the other cache remains blocked")
            .unwrap();
    }
}
