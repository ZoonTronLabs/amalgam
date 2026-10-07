use super::{CancellationSource, Execution, FactoryCancellation, LinkMode, Scopes};
use crate::error::{Error, FactoryCancellationReason as Reason, Result};
use std::future::Future;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::task::{Context, Poll};
use std::time::Duration;

struct Retirement {
    token: FactoryCancellation,
    reasons: Arc<Mutex<Vec<Reason>>>,
}
impl Drop for Retirement {
    fn drop(&mut self) {
        self.reasons
            .lock()
            .unwrap()
            .push(self.token.reason().unwrap());
    }
}
struct Ready {
    _retirement: Retirement,
}
impl Future for Ready {
    type Output = Result<u64>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(Ok(7))
    }
}
struct Waiting {
    _retirement: Retirement,
}
impl Future for Waiting {
    type Output = Result<u64>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}
fn retirement(source: &CancellationSource, reasons: &Arc<Mutex<Vec<Reason>>>) -> Retirement {
    Retirement {
        token: source.token(),
        reasons: Arc::clone(reasons),
    }
}
fn poll_once<T: Send + 'static>(work: &mut Execution<T>) -> Poll<Result<T>> {
    Pin::new(work).poll(&mut Context::from_waker(std::task::Waker::noop()))
}

#[test]
fn a_ready_first_poll_uses_admission_without_any_task_subscription_or_runtime() {
    let scopes = Scopes::new();
    let source = CancellationSource::for_cache(Arc::clone(&scopes));
    let reasons = Arc::new(Mutex::new(Vec::new()));
    let mut execution = scopes.ready_execution(
        Ready {
            _retirement: retirement(&source, &reasons),
        },
        source,
    );
    assert!(scopes.tasks.is_empty());
    assert!(!scopes.idle());
    assert!(matches!(poll_once(&mut execution), Poll::Ready(Ok(7))));
    assert!(scopes.tasks.is_empty());
    assert!(scopes.idle());
    assert_eq!(*reasons.lock().unwrap(), vec![Reason::ScopeFinished]);
}

#[tokio::test]
async fn suspension_subscribes_before_admission_ends_and_shutdown_needs_no_repoll() {
    let scopes = Scopes::new();
    let source = CancellationSource::for_cache(Arc::clone(&scopes));
    let reasons = Arc::new(Mutex::new(Vec::new()));
    let mut execution = scopes.ready_execution(
        Waiting {
            _retirement: retirement(&source, &reasons),
        },
        source,
    );
    assert!(scopes.tasks.is_empty());
    assert!(poll_once(&mut execution).is_pending());
    assert_eq!(scopes.tasks.len(), 1);
    scopes.close();
    tokio::time::timeout(Duration::from_secs(1), scopes.drained())
        .await
        .unwrap();
    assert_eq!(*reasons.lock().unwrap(), vec![Reason::CacheShutdown]);
    assert!(matches!(
        execution.await,
        Err(Error::OperationCancelled {
            reason: Reason::CacheShutdown
        })
    ));
}

struct BlockingReady {
    token: FactoryCancellation,
    entered: mpsc::Sender<()>,
    resume: mpsc::Receiver<()>,
    _retirement: Retirement,
}
impl Future for BlockingReady {
    type Output = Result<u64>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        self.entered.send(()).unwrap();
        self.resume.recv().unwrap();
        assert_eq!(self.token.reason(), Some(Reason::CacheShutdown));
        Poll::Ready(Ok(7))
    }
}
#[tokio::test]
async fn shutdown_is_visible_inside_a_blocked_first_poll_and_drains_its_retirement() {
    let scopes = Scopes::new();
    let source = CancellationSource::for_cache(Arc::clone(&scopes));
    let reasons = Arc::new(Mutex::new(Vec::new()));
    let (entered, started) = mpsc::channel();
    let (resume, released) = mpsc::channel();
    let worker_scopes = Arc::clone(&scopes);
    let worker_reasons = Arc::clone(&reasons);
    let worker = std::thread::spawn(move || {
        let work = BlockingReady {
            token: source.token(),
            entered,
            resume: released,
            _retirement: retirement(&source, &worker_reasons),
        };
        let mut execution = worker_scopes.ready_execution(work, source);
        poll_once(&mut execution)
    });
    started.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(scopes.tasks.is_empty());
    scopes.close();
    assert!(
        tokio::time::timeout(Duration::from_millis(10), scopes.drained())
            .await
            .is_err()
    );
    resume.send(()).unwrap();
    assert!(matches!(
        worker.join().unwrap(),
        Poll::Ready(Err(Error::OperationCancelled {
            reason: Reason::CacheShutdown
        }))
    ));
    tokio::time::timeout(Duration::from_secs(1), scopes.drained())
        .await
        .unwrap();
    assert_eq!(*reasons.lock().unwrap(), vec![Reason::CacheShutdown]);
}

struct Immovable {
    address: Arc<AtomicUsize>,
    polls: Arc<AtomicUsize>,
    _retirement: Retirement,
    _pin: PhantomPinned,
}
impl Future for Immovable {
    type Output = Result<u64>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        let work = self.as_ref().get_ref();
        let address = std::ptr::from_ref(work) as usize;
        if work.polls.fetch_add(1, Ordering::SeqCst) == 0 {
            work.address.store(address, Ordering::SeqCst);
            Poll::Pending
        } else {
            assert_eq!(work.address.load(Ordering::SeqCst), address);
            Poll::Ready(Ok(7))
        }
    }
}
#[tokio::test]
async fn promotion_keeps_a_non_unpin_future_at_its_first_poll_address() {
    let scopes = Scopes::new();
    let source = CancellationSource::for_cache(Arc::clone(&scopes));
    let reasons = Arc::new(Mutex::new(Vec::new()));
    let work = Immovable {
        address: Arc::new(AtomicUsize::new(0)),
        polls: Arc::new(AtomicUsize::new(0)),
        _retirement: retirement(&source, &reasons),
        _pin: PhantomPinned,
    };
    let mut execution = scopes.ready_execution(work, source);
    assert!(poll_once(&mut execution).is_pending());
    assert!(matches!(poll_once(&mut execution), Poll::Ready(Ok(7))));
    assert_eq!(*reasons.lock().unwrap(), vec![Reason::ScopeFinished]);
    scopes.close();
    scopes.drained().await;
}

#[tokio::test]
async fn an_exported_first_poll_token_cancels_foreign_work_without_foreign_repoll() {
    let a = Scopes::new();
    let b = Scopes::new();
    let owner = CancellationSource::for_cache(Arc::clone(&a));
    let source = CancellationSource::new();
    let reasons = Arc::new(Mutex::new(Vec::new()));
    let execution = b.execution(
        Waiting {
            _retirement: retirement(&source, &reasons),
        },
        source,
    );
    execution.link(&owner.token(), LinkMode::Explicit);
    a.close();
    tokio::time::timeout(Duration::from_secs(1), a.drained())
        .await
        .unwrap();
    assert_eq!(*reasons.lock().unwrap(), vec![Reason::CacheShutdown]);
    assert!(matches!(
        execution.await,
        Err(Error::OperationCancelled {
            reason: Reason::CacheShutdown
        })
    ));
    b.close();
    b.drained().await;
}

#[tokio::test]
async fn an_exported_child_token_also_preserves_cross_cache_shutdown_delivery() {
    let a = Scopes::new();
    let b = Scopes::new();
    let parent = CancellationSource::for_cache(Arc::clone(&a)).token();
    let phase = super::BorrowedPhase::new(&a, &parent);
    let source = CancellationSource::new();
    let reasons = Arc::new(Mutex::new(Vec::new()));
    let execution = b.execution(
        Waiting {
            _retirement: retirement(&source, &reasons),
        },
        source,
    );
    execution.link(&phase.token(), LinkMode::Explicit);
    a.close();
    tokio::time::timeout(Duration::from_secs(1), a.drained())
        .await
        .unwrap();
    assert_eq!(*reasons.lock().unwrap(), vec![Reason::CacheShutdown]);
    assert!(matches!(
        execution.await,
        Err(Error::OperationCancelled {
            reason: Reason::CacheShutdown
        })
    ));
    b.close();
    b.drained().await;
}

struct Panicking {
    _retirement: Retirement,
}
impl Future for Panicking {
    type Output = Result<u64>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        panic!("probe");
    }
}
#[test]
fn a_first_poll_panic_publishes_cause_before_cache_controlled_work_destruction() {
    let scopes = Scopes::new();
    let source = CancellationSource::for_cache(Arc::clone(&scopes));
    let reasons = Arc::new(Mutex::new(Vec::new()));
    let mut execution = scopes.ready_execution(
        Panicking {
            _retirement: retirement(&source, &reasons),
        },
        source,
    );
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| poll_once(&mut execution)))
            .is_err()
    );
    assert_eq!(*reasons.lock().unwrap(), vec![Reason::CallerDropped]);
    assert!(scopes.idle());
    assert!(scopes.tasks.is_empty());
}

#[test]
fn a_promoted_scope_owns_completion_before_source_notification() {
    let scopes = Scopes::new();
    let source = CancellationSource::for_cache(Arc::clone(&scopes));
    let token = source.token();
    let execution = scopes.execution(std::future::pending::<Result<u64>>(), source);
    *super::lock(&execution.owned_scope().state) = super::State::Completed;
    scopes.close();
    assert_eq!(token.reason(), Some(Reason::ScopeFinished));
}
