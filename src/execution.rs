//! Cache-owned cancellable execution scopes. User code is never polled or dropped
//! while a registry/state lock is held.
use crate::error::{Error, FactoryCancellationReason as Reason, Result};
use std::cell::Cell;
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

const ACTIVE_STRIPES: usize = 64;
#[repr(align(128))]
struct ActiveStripe {
    owner: AtomicUsize,
    local: AtomicUsize,
    shared: AtomicUsize,
}
#[derive(Clone, Copy)]
struct StripeIndex {
    position: usize,
    owner: std::num::NonZeroUsize,
}
static NEXT_STRIPE: AtomicUsize = AtomicUsize::new(1);
thread_local! {
    // Const TLS has neither lazy runtime initialization nor a destructor.
    // The shared allocator is touched once per thread, never once per lookup.
    static ACTIVE_STRIPE: Cell<Option<StripeIndex>> = const { Cell::new(None) };
}
fn current_stripe() -> StripeIndex {
    ACTIVE_STRIPE.with(|cached| match cached.get() {
        Some(index) => index,
        None => {
            let Some(owner) =
                std::num::NonZeroUsize::new(NEXT_STRIPE.fetch_add(1, Ordering::Relaxed))
            else {
                unreachable!("the process cannot create usize::MAX threads");
            };
            let index = StripeIndex {
                position: owner.get() % ACTIVE_STRIPES,
                owner,
            };
            cached.set(Some(index));
            index
        }
    })
}

pub(crate) struct Scopes {
    // Admission publishes its stripe before reading closing; shutdown publishes
    // closing before scanning every stripe. SeqCst preserves the same Dekker
    // argument on weak-memory CPUs. Thread-bound guards use a single-writer
    // count; transferable work and thread-index collisions use a separate RMW
    // count. A non-Send guard prevents moving a local reservation to a writer
    // thread, so releasing local activity needs only a Release store.
    closing: AtomicBool,
    scopes: Mutex<VecDeque<Weak<dyn CancelWork>>>,
    changed: Notify,
    active: [ActiveStripe; ACTIVE_STRIPES],
}
impl Scopes {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            closing: AtomicBool::new(false),
            scopes: Mutex::new(VecDeque::new()),
            changed: Notify::new(),
            active: std::array::from_fn(|_| ActiveStripe {
                owner: AtomicUsize::new(0),
                local: AtomicUsize::new(0),
                shared: AtomicUsize::new(0),
            }),
        })
    }
    pub(crate) fn is_closed(&self) -> bool {
        self.closing.load(Ordering::SeqCst)
    }
    fn activity(&self) -> ThreadActivity<'_> {
        let stripe = current_stripe();
        let counter = &self.active[stripe.position];
        let owner = counter.owner.load(Ordering::Acquire);
        let local = owner == stripe.owner.get()
            || owner == 0
                && counter
                    .owner
                    .compare_exchange(0, stripe.owner.get(), Ordering::AcqRel, Ordering::Acquire)
                    .is_ok();
        let activity = if local {
            let count = counter.local.load(Ordering::Relaxed);
            counter.local.store(count + 1, Ordering::SeqCst);
            Activity::Local(self, stripe)
        } else {
            counter.shared.fetch_add(1, Ordering::SeqCst);
            Activity::Shared(self, stripe)
        };
        ThreadActivity {
            activity,
            _thread: std::marker::PhantomData,
        }
    }
    fn idle(&self) -> bool {
        // Check zero directly: a summed count could overflow, and drainage
        // needs only the absence of work, not a globally coherent total.
        self.active.iter().all(|stripe| {
            stripe.local.load(Ordering::SeqCst) == 0 && stripe.shared.load(Ordering::SeqCst) == 0
        })
    }
    /// A synchronous operation has no parked future to own. Count it through
    /// every user callback so close rejects its result and shutdown drains it.
    pub(crate) fn inline(&self) -> InlinePermit<'_> {
        let activity = self.activity();
        InlinePermit {
            activity: InlineActivity::Thread(activity),
        }
    }
    /// Transfers synchronous completion ownership through an internal result.
    /// It retains only the scope counter, never the public cache lifetime.
    pub(crate) fn inline_owned(self: &Arc<Self>) -> OwnedInlinePermit {
        let stripe = current_stripe();
        self.active[stripe.position]
            .shared
            .fetch_add(1, Ordering::SeqCst);
        OwnedInlinePermit {
            registry: Arc::clone(self),
            stripe,
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
            checkpoint: AtomicBool::new(false),
        });
        self.register_scope(scope)
    }
    /// A weak progress signal shares the already owned execution allocation.
    /// It records one internal checkpoint independently of terminal cancellation.
    pub(crate) fn execution_with_checkpoint<T: Send + 'static, F>(
        self: &Arc<Self>,
        work: impl FnOnce(ExecutionCheckpoint<T>) -> F,
        source: CancellationSource,
    ) -> Execution<T>
    where
        F: Future<Output = Result<T>> + Send + 'static,
    {
        let scope = Arc::new_cyclic(|scope| Scope {
            state: Mutex::new(State::Pending(Box::pin(work(ExecutionCheckpoint {
                scope: scope.clone(),
            })))),
            waker: Mutex::new(None),
            source,
            registry: Arc::clone(self),
            checkpoint: AtomicBool::new(false),
        });
        self.register_scope(scope)
    }
    fn register_scope<T: Send + 'static>(self: &Arc<Self>, scope: Arc<Scope<T>>) -> Execution<T> {
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
            if self.idle()
                && lock(&self.scopes)
                    .iter()
                    .filter_map(Weak::upgrade)
                    .all(|scope| scope.finished())
                // A concurrent cancel/poll can mark a scope terminal while its
                // user future destructor is still running. Recheck activity
                // after the terminal-state snapshot, before claiming drainage.
                && self.idle()
            {
                return;
            }
            notified.await;
        }
    }
}

enum Activity<'a> {
    Local(&'a Scopes, StripeIndex),
    Shared(&'a Scopes, StripeIndex),
}
impl Activity<'_> {
    fn registry(&self) -> &Scopes {
        match self {
            Self::Local(registry, _) | Self::Shared(registry, _) => registry,
        }
    }
}
struct ThreadActivity<'a> {
    activity: Activity<'a>,
    _thread: std::marker::PhantomData<std::rc::Rc<()>>,
}
impl Drop for ThreadActivity<'_> {
    fn drop(&mut self) {
        let idle = match self.activity {
            Activity::Local(registry, stripe) => {
                // Only this thread modifies local; nested callbacks are counted.
                let counter = &registry.active[stripe.position].local;
                let count = counter.load(Ordering::Relaxed);
                counter.store(count - 1, Ordering::Release);
                count == 1
            }
            Activity::Shared(registry, stripe) => {
                registry.active[stripe.position]
                    .shared
                    .fetch_sub(1, Ordering::SeqCst)
                    == 1
            }
        };
        if idle && self.activity.registry().is_closed() {
            self.activity.registry().changed.notify_waiters();
        }
    }
}
pub(crate) struct OwnedInlinePermit {
    registry: Arc<Scopes>,
    stripe: StripeIndex,
}
impl OwnedInlinePermit {
    pub(crate) fn admit(&self) -> Result<()> {
        if self.registry.is_closed() {
            Err(Error::CacheClosed)
        } else {
            Ok(())
        }
    }
    pub(crate) fn status(&self, token: Option<&FactoryCancellation>) -> Result<()> {
        status(&self.registry, token)
    }
}
impl Drop for OwnedInlinePermit {
    fn drop(&mut self) {
        // A transferred reservation always releases the original shared stripe.
        if self.registry.active[self.stripe.position]
            .shared
            .fetch_sub(1, Ordering::SeqCst)
            == 1
            && self.registry.is_closed()
        {
            self.registry.changed.notify_waiters();
        }
    }
}
enum InlineActivity<'a> {
    Thread(ThreadActivity<'a>),
    Owned(OwnedInlinePermit),
}
impl InlineActivity<'_> {
    fn registry(&self) -> &Scopes {
        match self {
            Self::Thread(activity) => activity.activity.registry(),
            Self::Owned(permit) => &permit.registry,
        }
    }
}
pub(crate) struct InlinePermit<'a> {
    activity: InlineActivity<'a>,
}
impl InlinePermit<'_> {
    pub(crate) fn from_owned(permit: OwnedInlinePermit) -> Self {
        Self {
            activity: InlineActivity::Owned(permit),
        }
    }
    pub(crate) fn admit(&self) -> Result<()> {
        if self.activity.registry().is_closed() {
            Err(Error::CacheClosed)
        } else {
            Ok(())
        }
    }
    /// Cancellation wins over a clone/completion result. Borrowed reservations
    /// are thread-bound and released before handing a miss to owned execution.
    pub(crate) fn status(&self, token: Option<&FactoryCancellation>) -> Result<()> {
        status(self.activity.registry(), token)
    }
}
fn status(registry: &Scopes, token: Option<&FactoryCancellation>) -> Result<()> {
    if let Some(reason) = token.and_then(FactoryCancellation::reason) {
        return Err(Error::OperationCancelled { reason });
    }
    if registry.is_closed() {
        return Err(Error::OperationCancelled {
            reason: Reason::CacheShutdown,
        });
    }
    Ok(())
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
    checkpoint: AtomicBool,
}
/// Work observes progress without owning its parent or creating a strong cycle.
pub(crate) struct ExecutionCheckpoint<T> {
    scope: Weak<Scope<T>>,
}
impl<T> ExecutionCheckpoint<T> {
    pub(crate) fn record(&self) {
        if let Some(scope) = self.scope.upgrade() {
            scope.checkpoint.store(true, Ordering::Release);
        }
    }
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
    pub(crate) fn checkpoint_reached(&self) -> bool {
        self.scope.checkpoint.load(Ordering::Acquire)
    }
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

#[cfg(test)]
mod tests {
    use super::Scopes;
    use std::sync::mpsc;
    use std::time::Duration;

    #[tokio::test]
    async fn nested_local_and_colliding_work_must_all_exit_before_drain() {
        let scopes = Scopes::new();
        let original = super::current_stripe();
        let outer = scopes.inline();
        let inner = scopes.inline();
        let other = super::StripeIndex {
            position: original.position,
            owner: std::num::NonZeroUsize::new(original.owner.get() + super::ACTIVE_STRIPES)
                .unwrap(),
        };
        // Simulate the identifier of another thread colliding with this stripe.
        // It must use shared accounting even though local activity is live.
        super::ACTIVE_STRIPE.with(|cached| cached.set(Some(other)));
        let collision = scopes.inline();
        super::ACTIVE_STRIPE.with(|cached| cached.set(Some(original)));
        scopes.close();
        drop(outer);
        let mut drain = std::pin::pin!(scopes.drained());
        assert!(
            tokio::time::timeout(Duration::from_millis(5), &mut drain)
                .await
                .is_err()
        );
        drop(inner);
        assert!(
            tokio::time::timeout(Duration::from_millis(5), &mut drain)
                .await
                .is_err()
        );
        drop(collision);
        tokio::time::timeout(Duration::from_secs(1), drain)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn transferred_inline_work_drains_only_after_its_destination_releases_it() {
        let scopes = Scopes::new();
        let permit = scopes.inline_owned();
        let (arrived, arrival) = mpsc::channel();
        let (release, released) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            arrived.send(()).unwrap();
            released.recv().unwrap();
            drop(permit);
        });
        arrival.recv().unwrap();
        scopes.close();
        let mut drain = std::pin::pin!(scopes.drained());
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut drain)
                .await
                .is_err()
        );
        release.send(()).unwrap();
        thread.join().unwrap();
        tokio::time::timeout(Duration::from_secs(1), drain)
            .await
            .unwrap();
    }
}
