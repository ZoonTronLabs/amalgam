//! Cache-owned cancellable execution scopes. User code is never polled or dropped
//! while a registry/state lock is held.
use crate::error::{Error, FactoryCancellationReason as Reason, Result};
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, Waker};
use tokio::sync::Notify;

type Work<T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'static>>;

/// Read-only cancellation state carried into owned cache work and origin factories.
#[derive(Clone, Debug)]
pub struct FactoryCancellation {
    request: Arc<Request>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CancellationState {
    Active,
    Cancelled(Reason),
}

impl CancellationState {
    fn load(state: &AtomicU8) -> Self {
        match state.load(Ordering::Acquire) {
            0 => Self::Active,
            1 => Self::Cancelled(Reason::CallerCancelled),
            2 => Self::Cancelled(Reason::CallerDropped),
            3 => Self::Cancelled(Reason::SoftTimeout),
            4 => Self::Cancelled(Reason::HardTimeout),
            5 => Self::Cancelled(Reason::CacheShutdown),
            6 => Self::Cancelled(Reason::LeaseLost),
            7 => Self::Cancelled(Reason::ScopeFinished),
            _ => unreachable!("only closed cancellation states are stored"),
        }
    }

    fn cancelled_code(reason: Reason) -> u8 {
        match reason {
            Reason::CallerCancelled => 1,
            Reason::CallerDropped => 2,
            Reason::SoftTimeout => 3,
            Reason::HardTimeout => 4,
            Reason::CacheShutdown => 5,
            Reason::LeaseLost => 6,
            Reason::ScopeFinished => 7,
        }
    }
}

impl FactoryCancellation {
    pub(crate) fn reason(&self) -> Option<Reason> {
        match CancellationState::load(&self.request.state) {
            CancellationState::Active => None,
            CancellationState::Cancelled(reason) => Some(reason),
        }
    }
    /// Whether this execution scope has ended or was cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.reason().is_some()
    }
    /// Checks the owning scope without waiting, preserving its terminal reason.
    ///
    /// Successful scope completion also ends the token. A background continuation
    /// receives its own scope token, which remains active until that work ends.
    pub fn check(&self) -> Result<()> {
        match self.reason() {
            Some(reason) => Err(Error::OperationCancelled { reason }),
            None => Ok(()),
        }
    }
    /// Waits for a precise execution-scope reason.
    pub async fn cancelled(&self) -> Reason {
        loop {
            // Subscribe before checking terminal state so cancellation cannot
            // fall between the state read and notification registration.
            let changed = self.request.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(reason) = self.reason() {
                return reason;
            }
            changed.await;
        }
    }
}

/// Result of an explicit cancellation request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancellationRequest {
    /// This request cancelled the active scope.
    Cancelled,
    /// Its first terminal reason was already recorded.
    AlreadyCancelled,
}

/// Explicit caller cancellation. Clones address the same request.
#[derive(Clone, Debug)]
pub struct CancellationSource {
    request: Arc<Request>,
}
#[derive(Debug)]
struct Request {
    state: AtomicU8,
    changed: Notify,
    listeners: Mutex<VecDeque<Listener>>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LinkMode {
    Explicit,
    CallerScope,
}
struct Listener {
    target: Weak<dyn CancelWork>,
    mode: LinkMode,
}
impl std::fmt::Debug for Listener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CancellationListener")
    }
}
impl CancellationSource {
    /// Creates an active request without needing a runtime.
    pub fn new() -> Self {
        Self {
            request: Arc::new(Request {
                state: AtomicU8::new(0),
                changed: Notify::new(),
                listeners: Mutex::new(VecDeque::new()),
            }),
        }
    }
    /// Obtains a read-only token.
    pub fn token(&self) -> FactoryCancellation {
        FactoryCancellation {
            request: Arc::clone(&self.request),
        }
    }
    /// Requests caller cancellation once.
    pub fn cancel(&self) -> CancellationRequest {
        self.cancel_with(Reason::CallerCancelled)
    }
    pub(crate) fn cancel_with(&self, reason: Reason) -> CancellationRequest {
        let changed = self
            .request
            .state
            .compare_exchange(
                0,
                CancellationState::cancelled_code(reason),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok();
        if changed {
            self.request.changed.notify_waiters();
            let listeners: Vec<_> = lock(&self.request.listeners)
                .iter()
                .filter_map(|listener| {
                    listener
                        .target
                        .upgrade()
                        .map(|target| (target, listener.mode))
                })
                .collect();
            for (target, mode) in listeners {
                if mode == LinkMode::Explicit || reason != Reason::ScopeFinished {
                    target.cancel(reason);
                }
            }
            CancellationRequest::Cancelled
        } else {
            CancellationRequest::AlreadyCancelled
        }
    }
}
impl Default for CancellationSource {
    fn default() -> Self {
        Self::new()
    }
}

trait CancelWork: Send + Sync {
    fn cancel(&self, reason: Reason);
    fn finished(&self) -> bool;
}

pub(crate) struct Scopes {
    // Admission increments active before reading closing; shutdown sets closing
    // before reading active. A single sequentially consistent order prevents
    // both sides observing the other transition as absent on weak-memory CPUs.
    closing: AtomicBool,
    scopes: Mutex<VecDeque<Weak<dyn CancelWork>>>,
    changed: Notify,
    active: AtomicUsize,
}
impl Scopes {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            closing: AtomicBool::new(false),
            scopes: Mutex::new(VecDeque::new()),
            changed: Notify::new(),
            active: AtomicUsize::new(0),
        })
    }
    pub(crate) fn is_closed(&self) -> bool {
        self.closing.load(Ordering::SeqCst)
    }
    fn activity(&self) -> Activity<'_> {
        self.active.fetch_add(1, Ordering::SeqCst);
        Activity::Borrowed(self)
    }
    /// A synchronous operation has no parked future to own. Count it through
    /// every user callback so close rejects its result and shutdown drains it.
    pub(crate) fn inline(&self) -> InlinePermit<'_> {
        let activity = self.activity();
        InlinePermit { activity }
    }
    /// Transfers synchronous completion ownership through an internal result.
    /// It retains only the scope counter, never the public cache lifetime.
    pub(crate) fn inline_owned(self: &Arc<Self>) -> InlinePermit<'static> {
        self.active.fetch_add(1, Ordering::SeqCst);
        InlinePermit {
            activity: Activity::Owned(Arc::clone(self)),
        }
    }
    pub(crate) fn execution<T: Send + 'static>(
        self: &Arc<Self>,
        work: impl Future<Output = Result<T>> + Send + 'static,
        source: CancellationSource,
    ) -> Execution<T> {
        let scope = Arc::new(Scope {
            state: Mutex::new(State::Pending(Box::pin(work))),
            waker: Mutex::new(None),
            source,
            registry: Arc::clone(self),
        });
        let erased: Arc<dyn CancelWork> = scope.clone();
        let closed = {
            let mut scopes = lock(&self.scopes);
            let count = scopes.len().min(4);
            for _ in 0..count {
                if let Some(old) = scopes.pop_front()
                    && old.strong_count() != 0
                {
                    scopes.push_back(old);
                }
            }
            scopes.push_back(Arc::downgrade(&erased));
            self.is_closed()
        };
        if closed {
            scope.cancel(Reason::CacheShutdown);
        }
        Execution { scope }
    }
    pub(crate) fn close(&self) -> bool {
        let _activity = self.activity();
        let started = !self.closing.swap(true, Ordering::SeqCst);
        let scopes: Vec<_> = lock(&self.scopes)
            .iter()
            .filter_map(Weak::upgrade)
            .collect();
        for scope in scopes {
            scope.cancel(Reason::CacheShutdown);
        }
        started
    }
    pub(crate) async fn drained(&self) {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.active.load(Ordering::SeqCst) == 0
                && lock(&self.scopes)
                    .iter()
                    .filter_map(Weak::upgrade)
                    .all(|scope| scope.finished())
                // A concurrent cancel/poll can mark a scope terminal while its
                // user future destructor is still running. Recheck activity
                // after the terminal-state snapshot, before claiming drainage.
                && self.active.load(Ordering::SeqCst) == 0
            {
                return;
            }
            notified.await;
        }
    }
}

enum Activity<'a> {
    Borrowed(&'a Scopes),
    Owned(Arc<Scopes>),
}
impl Activity<'_> {
    fn registry(&self) -> &Scopes {
        match self {
            Self::Borrowed(registry) => registry,
            Self::Owned(registry) => registry,
        }
    }
}
impl Drop for Activity<'_> {
    fn drop(&mut self) {
        let registry = self.registry();
        if registry.active.fetch_sub(1, Ordering::SeqCst) == 1 && registry.is_closed() {
            registry.changed.notify_waiters();
        }
    }
}
pub(crate) struct InlinePermit<'a> {
    activity: Activity<'a>,
}
impl InlinePermit<'_> {
    pub(crate) fn admit(&self) -> Result<()> {
        if self.activity.registry().is_closed() {
            Err(Error::CacheClosed)
        } else {
            Ok(())
        }
    }
    /// Cancellation has precedence over an inline value/clone result. Successful
    /// completion linearizes at this check; callback drainage lasts until Drop.
    pub(crate) fn status(&self, token: Option<&FactoryCancellation>) -> Result<()> {
        if let Some(reason) = token.and_then(FactoryCancellation::reason) {
            return Err(Error::OperationCancelled { reason });
        }
        if self.activity.registry().is_closed() {
            return Err(Error::OperationCancelled {
                reason: Reason::CacheShutdown,
            });
        }
        Ok(())
    }
}

enum State<T> {
    Pending(Work<T>),
    Polling { cancellation: Option<Reason> },
    Cancelled(Reason),
    Completed,
}
struct Scope<T> {
    state: Mutex<State<T>>,
    waker: Mutex<Option<Waker>>,
    source: CancellationSource,
    registry: Arc<Scopes>,
}
impl<T: Send + 'static> CancelWork for Scope<T> {
    fn cancel(&self, reason: Reason) {
        let _activity = self.registry.activity();
        let pending = {
            let mut state = lock(&self.state);
            match &mut *state {
                State::Pending(_) => match std::mem::replace(&mut *state, State::Cancelled(reason))
                {
                    State::Pending(work) => Some(work),
                    _ => unreachable!("pending state was matched"),
                },
                State::Polling { cancellation } => {
                    if cancellation.is_none() {
                        *cancellation = Some(reason);
                    }
                    None
                }
                State::Cancelled(_) | State::Completed => None,
            }
        };
        self.source.cancel_with(reason);
        drop(pending);
        let waker = lock(&self.waker).take();
        if let Some(waker) = waker {
            waker.wake();
        }
        self.registry.changed.notify_waiters();
    }
    fn finished(&self) -> bool {
        matches!(*lock(&self.state), State::Cancelled(_) | State::Completed)
    }
}

pub(crate) struct Execution<T: Send + 'static> {
    scope: Arc<Scope<T>>,
}
impl<T: Send + 'static> Execution<T> {
    pub(crate) fn cancel(&self, reason: Reason) {
        self.scope.cancel(reason);
    }
    pub(crate) fn link(&self, token: &FactoryCancellation, mode: LinkMode) {
        let erased: Arc<dyn CancelWork> = self.scope.clone();
        {
            let mut listeners = lock(&token.request.listeners);
            let count = listeners.len().min(4);
            for _ in 0..count {
                if let Some(old) = listeners.pop_front()
                    && old.target.strong_count() != 0
                {
                    listeners.push_back(old);
                }
            }
            listeners.push_back(Listener {
                target: Arc::downgrade(&erased),
                mode,
            });
        }
        let reason = match CancellationState::load(&token.request.state) {
            CancellationState::Active => None,
            CancellationState::Cancelled(reason) => Some(reason),
        };
        if let Some(reason) = reason
            && (mode == LinkMode::Explicit || reason != Reason::ScopeFinished)
        {
            self.cancel(reason);
        }
    }
}
struct PollLease<T: Send + 'static>(Arc<Scope<T>>);
impl<T: Send + 'static> Drop for PollLease<T> {
    fn drop(&mut self) {
        if matches!(*lock(&self.0.state), State::Polling { .. }) {
            *lock(&self.0.state) = State::Cancelled(Reason::CallerDropped);
            self.0.source.cancel_with(Reason::CallerDropped);
            self.0.registry.changed.notify_waiters();
        }
    }
}
impl<T: Send + 'static> Future for Execution<T> {
    type Output = Result<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let _activity = self.scope.registry.activity();
        *lock(&self.scope.waker) = Some(cx.waker().clone());
        let mut work = {
            let mut state = lock(&self.scope.state);
            match &*state {
                State::Cancelled(reason) => {
                    return Poll::Ready(Err(Error::OperationCancelled { reason: *reason }));
                }
                State::Completed => panic!("completed cache execution was polled again"),
                State::Polling { .. } => panic!("cache execution was polled concurrently"),
                State::Pending(_) => {
                    match std::mem::replace(&mut *state, State::Polling { cancellation: None }) {
                        State::Pending(work) => work,
                        _ => unreachable!("pending state was matched"),
                    }
                }
            }
        };
        let _lease = PollLease(Arc::clone(&self.scope));
        let result = work.as_mut().poll(cx);
        let cancellation = {
            let mut state = lock(&self.scope.state);
            let cancellation = match &*state {
                State::Polling { cancellation } => *cancellation,
                _ => unreachable!("poll lease owns the state"),
            };
            *state = match (&result, cancellation) {
                (_, Some(reason)) => State::Cancelled(reason),
                (Poll::Ready(_), None) => State::Completed,
                (Poll::Pending, None) => State::Polling { cancellation: None },
            };
            if result.is_pending() && cancellation.is_none() {
                *state = State::Pending(work);
            } else {
                drop(state);
                drop(work);
            }
            cancellation
        };
        if let Some(reason) = cancellation {
            self.scope.registry.changed.notify_waiters();
            return Poll::Ready(Err(Error::OperationCancelled { reason }));
        }
        if result.is_ready() {
            self.scope.source.cancel_with(Reason::ScopeFinished);
            self.scope.registry.changed.notify_waiters();
        }
        result
    }
}
impl<T: Send + 'static> Drop for Execution<T> {
    fn drop(&mut self) {
        self.scope.cancel(Reason::CallerDropped);
    }
}

pub(crate) fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
