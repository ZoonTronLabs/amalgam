//! A child phase borrows its cache-owned parent instead of owning a second work
//! future. Every production caller runs inside Execution: that parent owns and
//! drains the pinned future. A first-poll parent uses admission; a suspended
//! parent is registered for cancellation without another caller poll.
use super::CacheBinding;
use super::{
    CancelWork, CancellationRequest, CancellationSource, FactoryCancellation, Reason, Request,
    RequestOwner, Scopes, lock,
};
use crate::error::{Error, Result};
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Waker};

#[derive(Debug)]
struct Control {
    request: Request,
    binding: CacheBinding,
    checkpoint: AtomicBool,
    waker: std::sync::Mutex<Option<Waker>>,
}
impl RequestOwner for Control {
    fn request(&self) -> &Request {
        &self.request
    }
    fn cache_binding(&self) -> Option<&CacheBinding> {
        Some(&self.binding)
    }
    fn track_shutdown(self: Arc<Self>) {
        let erased: Arc<dyn CancelWork> = self.clone();
        self.binding.subscribe(erased);
    }
}
impl CancelWork for Control {
    fn cancel(&self, reason: Reason) {
        if RequestOwner::cancel_with(self, reason) == CancellationRequest::Cancelled {
            let waker = lock(&self.waker).take();
            if let Some(waker) = waker {
                waker.wake();
            }
        }
    }
    fn finished(&self) -> bool {
        self.request.state.load(Ordering::Acquire) != 0
    }
}

/// Only a cache-owned parent may poll this phase. The borrow cannot be detached
/// from that operation. Its parent is already counted through polling/Drop and
/// subscribed for shutdown after suspension. An inherited cache-bound token
/// reports shutdown during first-poll callbacks without a second task token.
pub(crate) struct BorrowedPhase<'a> {
    registry: &'a Scopes,
    parent: &'a FactoryCancellation,
    control: Arc<Control>,
}
impl<'a> BorrowedPhase<'a> {
    pub(crate) fn new(registry: &'a Arc<Scopes>, parent: &'a FactoryCancellation) -> Self {
        let control = Arc::new(Control {
            request: Request::new(),
            binding: CacheBinding::new(Arc::clone(registry)),
            checkpoint: AtomicBool::new(false),
            waker: std::sync::Mutex::new(None),
        });
        let target: Arc<dyn CancelWork> = control.clone();
        parent.link_phase(target);
        Self {
            registry,
            parent,
            control,
        }
    }
    pub(crate) fn token(&self) -> FactoryCancellation {
        CancellationSource::from_owner(self.control.clone()).token()
    }
    pub(crate) fn checkpoint<T>(&self) -> ExecutionCheckpoint<'_, T> {
        ExecutionCheckpoint {
            checkpoint: &self.control.checkpoint,
            _value: PhantomData,
        }
    }
    pub(crate) fn checkpoint_reached(&self) -> bool {
        self.control.checkpoint.load(Ordering::Acquire)
    }
    pub(crate) fn cancel(&self, reason: Reason) {
        self.control.cancel(reason);
    }
    /// Declare this after the pinned work so its cause is published before
    /// that future is destroyed, even when polling unwinds.
    pub(crate) fn retirement(&self) -> PhaseRetirement<'_> {
        PhaseRetirement { phase: self }
    }
    fn cancellation_reason(&self) -> Option<Reason> {
        super::CancellationState::load(&self.control.request.state)
            .reason()
            .or_else(|| self.parent.reason())
            .or_else(|| self.registry.is_closed().then_some(Reason::CacheShutdown))
    }
    fn abandon(&self) {
        if !self.control.finished() {
            self.cancel(self.cancellation_reason().unwrap_or(Reason::CallerDropped));
        }
    }
    pub(crate) fn poll<T, F: Future<Output = Result<T>>>(
        &self,
        cx: &mut Context<'_>,
        work: Pin<&mut F>,
    ) -> Poll<Result<T>> {
        if let Some(reason) = self.cancellation_reason() {
            self.cancel(reason);
            return Poll::Ready(Err(Error::OperationCancelled { reason }));
        }
        let waker = cx.waker().clone();
        let previous = lock(&self.control.waker).replace(waker);
        drop(previous);
        let mut lease = PollRetirement {
            phase: self,
            finished: false,
        };
        let result = work.poll(cx);
        if let Some(reason) = self.cancellation_reason() {
            self.cancel(reason);
        }
        let completed = result.is_ready()
            && self.control.request.cancel_with(Reason::ScopeFinished)
                == CancellationRequest::Cancelled;
        lease.finished = true;
        if completed {
            return result;
        }
        match super::CancellationState::load(&self.control.request.state) {
            super::CancellationState::Active => result,
            super::CancellationState::Cancelled(reason) => {
                Poll::Ready(Err(Error::OperationCancelled { reason }))
            }
        }
    }
}
impl Drop for BorrowedPhase<'_> {
    fn drop(&mut self) {
        self.abandon();
    }
}
pub(crate) struct PhaseRetirement<'a> {
    phase: &'a BorrowedPhase<'a>,
}
impl Drop for PhaseRetirement<'_> {
    fn drop(&mut self) {
        self.phase.abandon();
    }
}
struct PollRetirement<'a> {
    phase: &'a BorrowedPhase<'a>,
    finished: bool,
}
impl Drop for PollRetirement<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.phase.abandon();
        }
    }
}
/// A checkpoint borrows the phase allocation and never clones its ownership.
pub(crate) struct ExecutionCheckpoint<'a, T> {
    checkpoint: &'a AtomicBool,
    _value: PhantomData<fn() -> T>,
}
impl<T> ExecutionCheckpoint<'_, T> {
    pub(crate) fn record(&self) {
        self.checkpoint.store(true, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, mpsc};
    use std::time::Duration;

    struct ObserveDrop {
        token: FactoryCancellation,
        observed: mpsc::Sender<Option<Reason>>,
    }
    impl Drop for ObserveDrop {
        fn drop(&mut self) {
            self.observed.send(self.token.reason()).unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn close_drains_ready_child_callback_through_its_parent() {
        let scopes = Scopes::new();
        let worker_scopes = scopes.clone();
        let source = CancellationSource::new();
        let parent = source.token();
        let (entered, entry) = tokio::sync::oneshot::channel();
        let (release, released) = mpsc::channel();
        let child_token = Arc::new(Mutex::new(None));
        let retained = child_token.clone();
        let parent = scopes.execution(
            async move {
                let phase = BorrowedPhase::new(&worker_scopes, &parent);
                *lock(&retained) = Some(phase.token());
                let mut work = std::pin::pin!(async move {
                    entered.send(()).unwrap();
                    released.recv_timeout(Duration::from_secs(5)).unwrap();
                    Ok(7_u64)
                });
                let _retirement = phase.retirement();
                std::future::poll_fn(|cx| phase.poll(cx, work.as_mut())).await
            },
            source,
        );
        let result = tokio::spawn(parent);
        entry.await.unwrap();
        scopes.close();
        assert_eq!(
            lock(&child_token).as_ref().unwrap().reason(),
            Some(Reason::CacheShutdown)
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(10), scopes.drained())
                .await
                .is_err()
        );
        release.send(()).unwrap();
        assert!(matches!(
            result.await.unwrap(),
            Err(Error::OperationCancelled {
                reason: Reason::CacheShutdown
            })
        ));
        tokio::time::timeout(Duration::from_secs(1), scopes.drained())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn pending_child_has_one_parent_task_and_retires_before_drain_without_repoll() {
        let scopes = Scopes::new();
        let worker_scopes = scopes.clone();
        let source = CancellationSource::new();
        let parent = source.token();
        let (observed, observation) = mpsc::channel();
        let mut execution = scopes.execution(
            async move {
                let phase = BorrowedPhase::new(&worker_scopes, &parent);
                let owned = ObserveDrop {
                    token: phase.token(),
                    observed,
                };
                let mut work = std::pin::pin!(async move {
                    let _owned = owned;
                    std::future::pending::<Result<()>>().await
                });
                let _retirement = phase.retirement();
                std::future::poll_fn(|cx| phase.poll(cx, work.as_mut())).await
            },
            source,
        );
        let initial =
            std::future::poll_fn(|cx| Poll::Ready(Pin::new(&mut execution).poll(cx))).await;
        assert!(initial.is_pending());
        assert_eq!(
            scopes.tasks.len(),
            1,
            "the child owns no second task registration"
        );
        scopes.close();
        assert_eq!(
            observation.recv_timeout(Duration::from_secs(1)).unwrap(),
            Some(Reason::CacheShutdown)
        );
        tokio::time::timeout(Duration::from_secs(1), scopes.drained())
            .await
            .unwrap();
        assert!(matches!(
            execution.await,
            Err(Error::OperationCancelled {
                reason: Reason::CacheShutdown
            })
        ));
    }

    #[tokio::test]
    async fn close_before_child_poll_prevents_user_work_and_publishes_drop_cause() {
        let scopes = Scopes::new();
        let worker_scopes = scopes.clone();
        let source = CancellationSource::new();
        let parent = source.token();
        let (observed, observation) = mpsc::channel();
        let result = scopes
            .execution(
                async move {
                    let phase = BorrowedPhase::new(&worker_scopes, &parent);
                    let owned = ObserveDrop {
                        token: phase.token(),
                        observed,
                    };
                    let mut work = std::pin::pin!(async move {
                        let _owned = owned;
                        panic!("closed child must not run");
                        #[allow(unreachable_code)]
                        Ok(7_u64)
                    });
                    let _retirement = phase.retirement();
                    worker_scopes.close();
                    std::future::poll_fn(|cx| phase.poll(cx, work.as_mut())).await
                },
                source,
            )
            .await;
        assert!(matches!(
            result,
            Err(Error::OperationCancelled {
                reason: Reason::CacheShutdown
            })
        ));
        assert_eq!(
            observation.recv_timeout(Duration::from_secs(1)).unwrap(),
            Some(Reason::CacheShutdown)
        );
    }

    // The probe stays in the pinned future owned by the cache. A local moved
    // into a panicking user function would unwind before control returns to any
    // executor, and is not cache-controlled retirement.
    struct PanicOnPoll {
        _owned: ObserveDrop,
    }
    impl Future for PanicOnPoll {
        type Output = Result<u64>;
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            panic!("child panic sentinel");
        }
    }

    #[tokio::test]
    async fn panic_publishes_child_drop_cause_before_retiring_pinned_user_work() {
        let scopes = Scopes::new();
        let worker_scopes = scopes.clone();
        let source = CancellationSource::new();
        let parent = source.token();
        let (observed, observation) = mpsc::channel();
        let execution = scopes.execution(
            async move {
                let phase = BorrowedPhase::new(&worker_scopes, &parent);
                let owned = ObserveDrop {
                    token: phase.token(),
                    observed,
                };
                let mut work = std::pin::pin!(PanicOnPoll { _owned: owned });
                let _retirement = phase.retirement();
                std::future::poll_fn(|cx| phase.poll(cx, work.as_mut())).await
            },
            source,
        );
        assert!(tokio::spawn(execution).await.unwrap_err().is_panic());
        assert_eq!(
            observation.recv_timeout(Duration::from_secs(1)).unwrap(),
            Some(Reason::CallerDropped)
        );
        tokio::time::timeout(Duration::from_secs(1), scopes.drained())
            .await
            .unwrap();
    }
}
