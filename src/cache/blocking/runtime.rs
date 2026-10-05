//! Driven I/O and bounded lineage-separated blocking callback pools.
use crate::execution::{Scopes, lock};
use std::cell::{Cell, RefCell};
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, Thread};
use tokio::runtime::{Builder, Handle, Runtime, RuntimeFlavor};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const MAX_CALLBACK_NESTING: usize = 32;
const MAX_IO_WORKERS: usize = 64;
const MAX_ROOT_CALLBACKS: usize = 512;

/// Independently bounded executor thread pools.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockingThreadPool {
    /// Workers driving asynchronous I/O and timers.
    IoWorkers,
    /// Concurrent root synchronous callbacks; nested pools are separate.
    RootCallbacks,
}
/// Configuration rejection and operating-system failure remain distinct.
#[derive(Debug, thiserror::Error)]
pub enum BlockingRuntimeError {
    /// The requested positive thread bound exceeds the explicit resource limit.
    #[error("{pool:?} count {requested} exceeds {maximum}")]
    ThreadLimit {
        /// The independently bounded pool.
        pool: BlockingThreadPool,
        /// The rejected positive request.
        requested: NonZeroUsize,
        /// Maximum supported threads for this pool.
        maximum: usize,
    },
    /// The operating system could not create the I/O executor.
    #[error("cannot create I/O executor: {0}")]
    Executor(#[source] std::io::Error),
}
fn validate_threads(
    pool: BlockingThreadPool,
    requested: NonZeroUsize,
    maximum: usize,
) -> Result<(), BlockingRuntimeError> {
    if requested.get() > maximum {
        return Err(BlockingRuntimeError::ThreadLimit {
            pool,
            requested,
            maximum,
        });
    }
    Ok(())
}

/// Expected rejection while dispatching a synchronous factory.
#[derive(Debug, thiserror::Error)]
pub enum BlockingDispatchError {
    /// Nested callback dispatch exceeded the explicit resource bound.
    #[error("synchronous factory nesting exceeds {limit} callbacks")]
    NestingLimit {
        /// Maximum number of offloaded callback ancestors, including this call.
        limit: usize,
    },
    /// The operating system could not create a nested callback executor.
    #[error("cannot create nested factory executor: {0}")]
    Executor(#[source] std::io::Error),
    /// A callback queue no longer accepts work.
    #[error("synchronous callback queue is closed: {0}")]
    QueueClosed(#[source] tokio::sync::AcquireError),
}

/// Driven executor for synchronous callers, independently of caller Tokio.
/// A clone shares I/O and callback pools. Root callback concurrency is bounded;
/// nested calls use up to 31 additional single-thread pools, created lazily.
#[derive(Clone)]
pub struct BlockingRuntime {
    driver: Arc<Driver>,
}
struct Driver {
    handle: Handle,
    runtime: Option<Runtime>,
    root_callbacks: NonZeroUsize,
    pools: Mutex<Vec<CallbackPool>>,
}
struct CallbackPool {
    runtime: Runtime,
    permits: Arc<Semaphore>,
}
impl Drop for Driver {
    fn drop(&mut self) {
        // Callback work and final-cache drainage retain a strong owner.
        // Shutdown is also valid when the last owner is on a foreign worker.
        let pools = self
            .pools
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for pool in pools.drain(..) {
            pool.runtime.shutdown_background();
        }
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}
#[derive(Clone, Copy)]
struct CallbackDepth(usize);
#[derive(Clone, Copy)]
struct PoolDepth(usize);
impl CallbackDepth {
    fn descend(self) -> Result<(PoolDepth, Self), BlockingDispatchError> {
        if self.0 >= MAX_CALLBACK_NESTING {
            return Err(BlockingDispatchError::NestingLimit {
                limit: MAX_CALLBACK_NESTING,
            });
        }
        Ok((PoolDepth(self.0), Self(self.0 + 1)))
    }
}
#[derive(Clone)]
pub(super) struct FactoryLineage {
    depth: CallbackDepth,
    owners: Vec<Weak<Scopes>>,
}
pub(super) struct FactoryExecutor {
    handle: Handle,
    permits: Arc<Semaphore>,
    lineage: FactoryLineage,
}
pub(super) struct FactoryThreadGuard(FactoryLineage);
pub(super) struct CallbackScope(usize);
impl Drop for CallbackScope {
    fn drop(&mut self) {
        FACTORY_OWNERS.with(|owners| owners.borrow_mut().truncate(self.0));
    }
}
pub(super) fn callback_scope(owner: &Arc<Scopes>) -> CallbackScope {
    FACTORY_OWNERS.with(|owners| {
        let mut owners = owners.borrow_mut();
        let previous = owners.len();
        owners.push(Arc::downgrade(owner));
        CallbackScope(previous)
    })
}
pub(super) fn check_drain(
    owner: &Arc<Scopes>,
    operation: crate::DrainOperation,
) -> crate::Result<()> {
    let identity = Arc::downgrade(owner);
    let own_factory = FACTORY_OWNERS.with(|owners| {
        owners
            .borrow()
            .iter()
            .any(|candidate| candidate.ptr_eq(&identity))
    });
    if own_factory {
        return Err(crate::Error::ReentrantDrain { operation });
    }
    Ok(())
}
impl Drop for FactoryThreadGuard {
    fn drop(&mut self) {
        CALLBACK_DEPTH.with(|depth| depth.set(self.0.depth));
        FACTORY_OWNERS.with(|owners| {
            *owners.borrow_mut() = std::mem::take(&mut self.0.owners);
        });
    }
}
impl FactoryLineage {
    pub(super) fn independent() -> Self {
        Self {
            depth: CallbackDepth(0),
            owners: Vec::new(),
        }
    }
}
impl FactoryExecutor {
    pub(super) fn handle(&self) -> Handle {
        self.handle.clone()
    }
    pub(super) async fn permit(&self) -> Result<OwnedSemaphorePermit, BlockingDispatchError> {
        Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .map_err(BlockingDispatchError::QueueClosed)
    }
    pub(super) fn enter(self) -> FactoryThreadGuard {
        let depth = CALLBACK_DEPTH.with(|depth| depth.replace(self.lineage.depth));
        let owners = FACTORY_OWNERS
            .with(|owners| std::mem::replace(&mut *owners.borrow_mut(), self.lineage.owners));
        FactoryThreadGuard(FactoryLineage { depth, owners })
    }
}
struct ThreadWake(Thread);
impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}
thread_local! {
    static CALLER_WAKER: Waker = Waker::from(Arc::new(ThreadWake(thread::current())));
    static CALLBACK_DEPTH: Cell<CallbackDepth> = const { Cell::new(CallbackDepth(0)) };
    static FACTORY_OWNERS: RefCell<Vec<Weak<Scopes>>> = const { RefCell::new(Vec::new()) };
}
impl BlockingRuntime {
    /// Creates a driven executor with two I/O workers and 32 root callback threads.
    pub fn new() -> Result<Self, BlockingRuntimeError> {
        Self::with_workers(
            NonZeroUsize::MIN.saturating_add(1),
            NonZeroUsize::MIN.saturating_add(31),
        )
    }
    fn from_runtime(runtime: Runtime, root_callbacks: NonZeroUsize) -> Self {
        let handle = runtime.handle().clone();
        Self {
            driver: Arc::new(Driver {
                handle,
                runtime: Some(runtime),
                root_callbacks,
                pools: Mutex::new(Vec::new()),
            }),
        }
    }
    /// Positive I/O-worker and root-callback bounds are enforced by the types.
    /// I/O has its own 32-thread blocking bound, separate from callbacks.
    /// Nested callbacks use separate lazy single-thread pools: at most 31 per
    /// executor and 32 offloaded ancestors per call. Exceeding that depth returns
    /// a typed factory error; it does not park behind its own ancestor.
    /// Requests above 64 I/O workers or 512 root callbacks return a typed
    /// configuration error before any threads or queues are created.
    pub fn with_workers(
        workers: NonZeroUsize,
        blocking: NonZeroUsize,
    ) -> Result<Self, BlockingRuntimeError> {
        validate_threads(BlockingThreadPool::IoWorkers, workers, MAX_IO_WORKERS)?;
        validate_threads(
            BlockingThreadPool::RootCallbacks,
            blocking,
            MAX_ROOT_CALLBACKS,
        )?;
        let runtime = Builder::new_multi_thread()
            .worker_threads(workers.get())
            .max_blocking_threads(32)
            .thread_name("amalgam-sync")
            .enable_all()
            .build()
            .map_err(BlockingRuntimeError::Executor)?;
        Ok(Self::from_runtime(runtime, blocking))
    }
    /// Drives a future on its caller while I/O and timers remain driven.
    /// It works outside Tokio and on foreign workers without nested block_on.
    /// The synchronous caller remains occupied until its operation completes.
    pub fn run<F: Future>(&self, future: F) -> F::Output {
        match Handle::try_current() {
            Ok(handle) if handle.runtime_flavor() == RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(|| self.run_entered(future))
            }
            Ok(_) | Err(_) => self.run_entered(future),
        }
    }
    fn run_entered<F: Future>(&self, future: F) -> F::Output {
        let _entered = self.driver.handle.enter();
        let mut future = std::pin::pin!(future);
        CALLER_WAKER.with(|waker| {
            let mut context = Context::from_waker(waker);
            loop {
                match future.as_mut().poll(&mut context) {
                    Poll::Ready(value) => return value,
                    Poll::Pending => thread::park(),
                }
            }
        })
    }
    pub(in crate::cache) fn handle(&self) -> &Handle {
        &self.driver.handle
    }
    pub(super) fn lineage(&self) -> FactoryLineage {
        FactoryLineage {
            depth: CALLBACK_DEPTH.with(Cell::get),
            owners: FACTORY_OWNERS.with(|owners| owners.borrow().clone()),
        }
    }
    pub(super) fn factory_executor(
        &self,
        parents: &FactoryLineage,
    ) -> Result<FactoryExecutor, BlockingDispatchError> {
        // Every wait goes to a strictly deeper pool, even across drivers.
        // Opposite A -> B and B -> A roots cannot wait on occupied root pools.
        let (pool, depth) = parents.depth.descend()?;
        let (handle, permits) = self.pool(pool).map_err(BlockingDispatchError::Executor)?;
        Ok(FactoryExecutor {
            handle,
            permits,
            lineage: FactoryLineage {
                depth,
                owners: parents.owners.clone(),
            },
        })
    }
    fn pool(&self, depth: PoolDepth) -> std::io::Result<(Handle, Arc<Semaphore>)> {
        let mut pools = lock(&self.driver.pools);
        while pools.len() <= depth.0 {
            let slots = if pools.is_empty() {
                self.driver.root_callbacks.get()
            } else {
                1
            };
            let runtime = Builder::new_current_thread()
                .max_blocking_threads(slots)
                .thread_name("amalgam-callback")
                .build()?;
            pools.push(CallbackPool {
                runtime,
                permits: Arc::new(Semaphore::new(slots)),
            });
        }
        let pool = &pools[depth.0];
        Ok((pool.runtime.handle().clone(), Arc::clone(&pool.permits)))
    }
}
