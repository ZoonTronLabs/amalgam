//! Cache-owned cancellable execution scopes. User code is never polled or dropped
//! while a registry/state lock is held.
use crate::error::{Error, FactoryCancellationReason as Reason, Result};
use std::cell::Cell;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, Wake, Waker};
use tokio::sync::Notify;
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};
use tokio_util::task::TaskTracker;
use tokio_util::task::task_tracker::TaskTrackerToken;

type Work<T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'static>>;

mod phase;
pub(crate) use phase::{BorrowedPhase, ExecutionCheckpoint};

/// Read-only cancellation state carried into owned cache work and origin factories.
#[derive(Clone, Debug)]
pub struct FactoryCancellation {
    request: Arc<dyn RequestOwner>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CancellationState {
    Active,
    Cancelled(Reason),
}

impl CancellationState {
    fn reason(self) -> Option<Reason> {
        match self {
            Self::Active => None,
            Self::Cancelled(reason) => Some(reason),
        }
    }
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
    pub(crate) fn link_work(&self, erased: Arc<dyn CancelWork>, mode: LinkMode) {
        {
            let mut listeners = self.request.request().listeners.lock();
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
        let reason = match CancellationState::load(&self.request.request().state) {
            CancellationState::Active => None,
            CancellationState::Cancelled(reason) => Some(reason),
        };
        if let Some(reason) = reason
            && mode.accepts(reason)
        {
            erased.cancel(reason);
        }
    }
    pub(crate) fn reason(&self) -> Option<Reason> {
        match CancellationState::load(&self.request.request().state) {
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
            let changed = self.request.request().changed.notified();
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
    request: Arc<dyn RequestOwner>,
}
#[derive(Debug)]
pub(crate) struct Request {
    state: AtomicU8,
    changed: Notify,
    listeners: parking_lot::Mutex<VecDeque<Listener>>,
}
/// Cancellation can share the allocation of its owning flight.
pub(crate) trait RequestOwner: std::fmt::Debug + Send + Sync + 'static {
    fn request(&self) -> &Request;
}
impl RequestOwner for Request {
    fn request(&self) -> &Request {
        self
    }
}
impl Request {
    pub(crate) fn cancel_with(&self, reason: Reason) -> CancellationRequest {
        let changed = self
            .state
            .compare_exchange(
                0,
                CancellationState::cancelled_code(reason),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok();
        if changed {
            self.changed.notify_waiters();
            let listeners: Vec<_> = self
                .listeners
                .lock()
                .iter()
                .filter_map(|listener| {
                    listener
                        .target
                        .upgrade()
                        .map(|target| (target, listener.mode))
                })
                .collect();
            for (target, mode) in listeners {
                if mode.accepts(reason) {
                    target.cancel(reason);
                }
            }
            CancellationRequest::Cancelled
        } else {
            CancellationRequest::AlreadyCancelled
        }
    }
    pub(crate) fn new() -> Self {
        Self {
            state: AtomicU8::new(0),
            changed: Notify::new(),
            listeners: parking_lot::Mutex::new(VecDeque::new()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LinkMode {
    Explicit,
    CallerScope,
    OriginCaller,
}
impl LinkMode {
    fn accepts(self, reason: Reason) -> bool {
        match self {
            Self::Explicit => true,
            Self::CallerScope => reason != Reason::ScopeFinished,
            Self::OriginCaller => !matches!(reason, Reason::ScopeFinished | Reason::CallerDropped),
        }
    }
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
            request: Arc::new(Request::new()),
        }
    }
    pub(crate) fn from_owner(request: Arc<dyn RequestOwner>) -> Self {
        Self { request }
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
        self.request.request().cancel_with(reason)
    }
}
impl Default for CancellationSource {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) trait CancelWork: Send + Sync {
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
    shutdown: CancellationToken,
    tasks: TaskTracker,
    cancelled: parking_lot::Mutex<VecDeque<Arc<dyn CancelWork>>>,
    changed: Notify,
    active: [ActiveStripe; ACTIVE_STRIPES],
}
impl Scopes {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            closing: AtomicBool::new(false),
            shutdown: CancellationToken::new(),
            tasks: TaskTracker::new(),
            cancelled: parking_lot::Mutex::new(VecDeque::new()),
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
        self.reserve_activity(Ordering::SeqCst)
    }
    fn reserve_activity(&self, publication: Ordering) -> ThreadActivity<'_> {
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
            counter.local.store(count + 1, publication);
            Activity::Local(self, stripe)
        } else {
            counter.shared.fetch_add(1, publication);
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
    /// Reserve before storage admission. The storage fence must publish this
    /// count before checking close or invoking any user code.
    #[cfg(target_arch = "x86_64")]
    pub(crate) fn defer_inline(&self) -> DeferredInlinePermit<'_> {
        DeferredInlinePermit {
            activity: self.reserve_activity(Ordering::Release),
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
            tracking: parking_lot::Mutex::new(None),
        });
        self.register_scope(scope)
    }
    fn register_scope<T: Send + 'static>(self: &Arc<Self>, scope: Arc<Scope<T>>) -> Execution<T> {
        let erased: Arc<dyn CancelWork> = scope.clone();
        self.register_work(erased, &scope.tracking);
        Execution { scope }
    }
    /// Registration is local to suspended work; normal completion removes it.
    pub(crate) fn register_work(
        self: &Arc<Self>,
        erased: Arc<dyn CancelWork>,
        tracking: &parking_lot::Mutex<Option<TrackedCancellation>>,
    ) {
        let _activity = self.activity();
        let closed = {
            let mut slot = tracking.lock();
            if slot.is_some() {
                return;
            }
            let mut registration = TrackedCancellation {
                waiting: Box::pin(self.shutdown.clone().cancelled_owned()),
                _task: self.tasks.token(),
            };
            let waker = Waker::from(Arc::new(CancelWake {
                owner: Arc::downgrade(self),
                target: Arc::downgrade(&erased),
            }));
            let closed = registration
                .waiting
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_ready();
            *slot = Some(registration);
            closed
        };
        if closed || self.is_closed() {
            erased.cancel(Reason::CacheShutdown);
        }
        // Completion can race subscription setup. Activity accounting covers
        // any destructor still running after terminal state was published.
        if erased.finished() {
            finish_tracking(tracking);
            self.changed.notify_waiters();
        }
    }
    pub(crate) fn close(&self) -> bool {
        let _activity = self.activity();
        let started = !self.closing.swap(true, Ordering::SeqCst);
        // Pairs with both complete admission and the shared storage fence.
        std::sync::atomic::fence(Ordering::SeqCst);
        self.tasks.close();
        self.shutdown.cancel();
        let mut panic = None;
        loop {
            let next = self.cancelled.lock().pop_front();
            let Some(work) = next else { break };
            // Token notification only queues work. User Drop runs here, after
            // both token and queue coordination have been released.
            if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                work.cancel(Reason::CacheShutdown);
            })) && panic.is_none()
            {
                panic = Some(payload);
            }
        }
        if let Some(payload) = panic {
            std::panic::resume_unwind(payload);
        }
        started
    }
    pub(crate) async fn drained(&self) {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.idle() && self.tasks.is_empty() && self.idle() {
                return;
            }
            if self.tasks.is_empty() {
                notified.await;
            } else {
                tokio::select! {
                    _ = self.tasks.wait() => {},
                    _ = &mut notified => {},
                }
            }
        }
    }
}

/// Kept until user work has actually been destroyed, not just marked terminal.
pub(crate) struct TrackedCancellation {
    waiting: Pin<Box<WaitForCancellationFutureOwned>>,
    _task: TaskTrackerToken,
}
struct CancelWake {
    owner: Weak<Scopes>,
    target: Weak<dyn CancelWork>,
}
impl Wake for CancelWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let Some(owner) = self.owner.upgrade() else {
            return;
        };
        let Some(target) = self.target.upgrade() else {
            return;
        };
        owner.cancelled.lock().push_back(target);
    }
}
pub(crate) fn finish_tracking(tracking: &parking_lot::Mutex<Option<TrackedCancellation>>) {
    let retired = tracking.lock().take();
    drop(retired);
}
/// Field order also preserves drainage when a user destructor unwinds.
pub(crate) struct Retirement<T> {
    pub(crate) _work: T,
    pub(crate) _tracking: Option<TrackedCancellation>,
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
/// A reservation cannot be checked or used for callbacks until a storage
/// reader guard, acquired AFTER this reservation, supplies its SeqCst fence.
#[cfg(target_arch = "x86_64")]
pub(crate) struct DeferredInlinePermit<'a> {
    activity: ThreadActivity<'a>,
}
#[cfg(target_arch = "x86_64")]
impl<'a> DeferredInlinePermit<'a> {
    pub(crate) fn after_reader<T>(
        self,
        _guard: &crate::reader_slots::ReadGuard<'_, T>,
    ) -> InlinePermit<'a> {
        InlinePermit {
            activity: InlineActivity::Thread(self.activity),
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
    tracking: parking_lot::Mutex<Option<TrackedCancellation>>,
}
impl<T> Scope<T> {
    fn drop_reason(&self) -> Reason {
        match CancellationState::load(&self.source.request.request().state) {
            CancellationState::Cancelled(reason) => reason,
            CancellationState::Active if self.registry.is_closed() => Reason::CacheShutdown,
            CancellationState::Active => Reason::CallerDropped,
        }
    }
}
impl<T: Send + 'static> CancelWork for Scope<T> {
    fn cancel(&self, reason: Reason) {
        let _activity = self.registry.activity();
        let (pending, terminal, recorded) = {
            let mut state = lock(&self.state);
            match &mut *state {
                State::Pending(_) => match std::mem::replace(&mut *state, State::Cancelled(reason))
                {
                    State::Pending(work) => (Some(work), true, reason),
                    _ => unreachable!("pending state was matched"),
                },
                State::Polling { cancellation } => {
                    let recorded = *cancellation.get_or_insert(reason);
                    (None, false, recorded)
                }
                State::Cancelled(recorded) => (None, true, *recorded),
                State::Completed => (None, true, Reason::ScopeFinished),
            }
        };
        let retirement = Retirement {
            _work: pending,
            _tracking: if terminal {
                self.tracking.lock().take()
            } else {
                None
            },
        };
        self.source.cancel_with(recorded);
        drop(retirement);
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
        token.link_work(erased, mode);
    }
}
struct PollLease<T: Send + 'static>(Arc<Scope<T>>);
impl<T: Send + 'static> Drop for PollLease<T> {
    fn drop(&mut self) {
        let reason = {
            let mut state = lock(&self.0.state);
            match &*state {
                State::Polling { cancellation } => {
                    let reason = cancellation.unwrap_or_else(|| self.0.drop_reason());
                    *state = State::Cancelled(reason);
                    Some(reason)
                }
                State::Pending(_) | State::Cancelled(_) | State::Completed => None,
            }
        };
        if let Some(reason) = reason {
            self.0.source.cancel_with(reason);
            self.0.registry.changed.notify_waiters();
        }
        if self.0.finished() {
            finish_tracking(&self.0.tracking);
        }
    }
}
impl<T: Send + 'static> Execution<T> {
    fn poll_work(
        &mut self,
        cx: &mut Context<'_>,
        admit: impl FnOnce(&Arc<Scope<T>>),
        suspended: impl FnOnce(&Arc<Scope<T>>),
    ) -> Poll<Result<T>> {
        let _activity = self.scope.registry.activity();
        admit(&self.scope);
        *lock(&self.scope.waker) = Some(cx.waker().clone());
        let _lease = PollLease(Arc::clone(&self.scope));
        let mut work = {
            let mut state = lock(&self.scope.state);
            match &*state {
                State::Cancelled(reason) => {
                    let reason = *reason;
                    drop(state);
                    self.scope.source.cancel_with(reason);
                    return Poll::Ready(Err(Error::OperationCancelled { reason }));
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
        let result = work.as_mut().poll(cx);
        let cancellation = {
            let mut state = lock(&self.scope.state);
            let cancellation = match &*state {
                State::Polling { cancellation } => *cancellation,
                _ => unreachable!("poll lease owns the state"),
            }
            .or_else(|| {
                self.scope
                    .registry
                    .is_closed()
                    .then_some(Reason::CacheShutdown)
            });
            *state = match (&result, cancellation) {
                (_, Some(reason)) => State::Cancelled(reason),
                (Poll::Ready(_), None) => State::Completed,
                (Poll::Pending, None) => State::Polling { cancellation: None },
            };
            if result.is_pending() && cancellation.is_none() {
                *state = State::Pending(work);
            } else {
                drop(state);
                let retirement = Retirement {
                    _work: work,
                    _tracking: self.scope.tracking.lock().take(),
                };
                // The terminal scope state owns the reason. Publish it before
                // user destruction or a result can expose completion to callers.
                self.scope
                    .source
                    .cancel_with(cancellation.unwrap_or(Reason::ScopeFinished));
                drop(retirement);
            }
            cancellation
        };
        if let Some(reason) = cancellation {
            self.scope.registry.changed.notify_waiters();
            return Poll::Ready(Err(Error::OperationCancelled { reason }));
        }
        if result.is_ready() {
            self.scope.registry.changed.notify_waiters();
        } else {
            // Retain the polling activity until suspended work is subscribed.
            // Shutdown cannot observe an idle gap between these two states.
            suspended(&self.scope);
        }
        result
    }
}
impl<T: Send + 'static> Future for Execution<T> {
    type Output = Result<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().poll_work(cx, |_| {}, |_| {})
    }
}
impl<T: Send + 'static> Drop for Execution<T> {
    fn drop(&mut self) {
        self.scope.cancel(self.scope.drop_reason());
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

#[cfg(test)]
mod close_drop_cause_tests {
    use super::*;
    use std::sync::mpsc;
    use std::time::Duration;

    struct ParkedDrop {
        entered: mpsc::Sender<()>,
        release: mpsc::Receiver<()>,
    }
    impl Drop for ParkedDrop {
        fn drop(&mut self) {
            self.entered.send(()).unwrap();
            self.release.recv_timeout(Duration::from_secs(5)).unwrap();
        }
    }

    #[test]
    fn caller_drop_during_close_keeps_shutdown_cause_before_registry_reaches_it() {
        let scopes = Scopes::new();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let parked = ParkedDrop {
            entered: entered_tx,
            release: release_rx,
        };
        let blocker = scopes.execution(
            async move {
                let _parked = parked;
                std::future::pending::<Result<()>>().await
            },
            CancellationSource::new(),
        );
        let source = CancellationSource::new();
        let token = source.token();
        let target = scopes.execution(std::future::pending::<Result<()>>(), source);
        let registry = scopes.clone();
        let closer = std::thread::spawn(move || registry.close());
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(scopes.is_closed());
        drop(target);
        let observed = token.reason();
        // Always unblock/join before asserting, including on the old failing code.
        release_tx.send(()).unwrap();
        assert!(closer.join().unwrap());
        drop(blocker);
        assert_eq!(observed, Some(Reason::CacheShutdown));
    }

    enum Finish {
        Value,
        Closed,
    }
    struct CloseThenReady {
        scopes: Arc<Scopes>,
        token: FactoryCancellation,
        finish: Finish,
        dropped: Arc<Mutex<Vec<Option<Reason>>>>,
    }
    impl Future for CloseThenReady {
        type Output = Result<u8>;
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            // Reach close's publication-to-notification window deterministically.
            // No shutdown notification has reached this polling scope yet.
            self.scopes.closing.store(true, Ordering::SeqCst);
            Poll::Ready(match self.finish {
                Finish::Value => Ok(7),
                Finish::Closed => Err(Error::CacheClosed),
            })
        }
    }
    impl Drop for CloseThenReady {
        fn drop(&mut self) {
            lock(&self.dropped).push(self.token.reason());
        }
    }
    #[test]
    fn ready_during_close_publishes_shutdown_before_retiring_the_future() {
        for finish in [Finish::Value, Finish::Closed] {
            let scopes = Scopes::new();
            let source = CancellationSource::new();
            let token = source.token();
            let dropped = Arc::new(Mutex::new(Vec::new()));
            let execution = scopes.execution(
                CloseThenReady {
                    scopes: scopes.clone(),
                    token: token.clone(),
                    finish,
                    dropped: dropped.clone(),
                },
                source,
            );
            let mut execution = std::pin::pin!(execution);
            let result = execution
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()));
            assert!(matches!(
                result,
                Poll::Ready(Err(Error::OperationCancelled {
                    reason: Reason::CacheShutdown
                }))
            ));
            assert_eq!(token.reason(), Some(Reason::CacheShutdown));
            assert_eq!(*lock(&dropped), [Some(Reason::CacheShutdown)]);
            assert!(!scopes.close());
            let mut drain = std::pin::pin!(scopes.drained());
            assert!(
                drain
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_ready()
            );
        }
    }
    #[test]
    fn terminal_state_owns_the_reason_before_token_notification() {
        for (state, expected) in [
            (
                State::Cancelled(Reason::CallerCancelled),
                Reason::CallerCancelled,
            ),
            (State::Completed, Reason::ScopeFinished),
        ] {
            let scopes = Scopes::new();
            let source = CancellationSource::new();
            let token = source.token();
            let execution = scopes.execution(std::future::pending::<Result<()>>(), source);
            // Reach the interval after committing the terminal scope state but
            // before publishing its token. A later notification must preserve it.
            let pending = {
                let mut current = lock(&execution.scope.state);
                std::mem::replace(&mut *current, state)
            };
            drop(pending);
            assert_eq!(token.reason(), None);
            execution.cancel(Reason::CacheShutdown);
            assert_eq!(token.reason(), Some(expected));
            drop(execution);
            assert!(scopes.close());
        }
    }

    #[test]
    fn panic_during_a_closed_poll_preserves_the_published_shutdown_cause() {
        let scopes = Scopes::new();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let execution = scopes.execution(
            async move {
                entered_tx.send(()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                panic!("original factory panic");
                #[allow(unreachable_code)]
                Ok::<(), Error>(())
            },
            CancellationSource::new(),
        );
        let scope = execution.scope.clone();
        let poller = std::thread::spawn(move || {
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let mut execution = std::pin::pin!(execution);
                let mut context = Context::from_waker(Waker::noop());
                execution.as_mut().poll(&mut context)
            }))
        });
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(scopes.close());
        release_tx.send(()).unwrap();
        let panic = poller.join().unwrap().unwrap_err();
        assert_eq!(
            panic.downcast_ref::<&str>(),
            Some(&"original factory panic")
        );
        assert!(matches!(
            *lock(&scope.state),
            State::Cancelled(Reason::CacheShutdown)
        ));
    }
}

#[cfg(test)]
mod tracker_contract_tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::mpsc;
    use std::time::Duration;

    struct ReenterDrop {
        scopes: Weak<Scopes>,
        drops: Arc<AtomicUsize>,
        reenter: bool,
    }
    impl Drop for ReenterDrop {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
            if self.reenter {
                let scopes = self.scopes.upgrade().unwrap();
                let second = Self {
                    scopes: Arc::downgrade(&scopes),
                    drops: self.drops.clone(),
                    reenter: false,
                };
                let work = scopes.execution(
                    async move {
                        let _second = second;
                        std::future::pending::<Result<()>>().await
                    },
                    CancellationSource::new(),
                );
                drop(work);
                assert!(!scopes.close());
            }
        }
    }
    #[test]
    fn closing_unpolled_work_without_runtime_allows_drop_to_reenter() {
        let scopes = Scopes::new();
        let drops = Arc::new(AtomicUsize::new(0));
        let capture = ReenterDrop {
            scopes: Arc::downgrade(&scopes),
            drops: drops.clone(),
            reenter: true,
        };
        let source = CancellationSource::new();
        let token = source.token();
        let work = scopes.execution(
            async move {
                let _capture = capture;
                std::future::pending::<Result<()>>().await
            },
            source,
        );
        assert!(scopes.close());
        assert_eq!(drops.load(Ordering::SeqCst), 2);
        assert_eq!(token.reason(), Some(Reason::CacheShutdown));
        let mut drain = std::pin::pin!(scopes.drained());
        assert!(
            drain
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_ready()
        );
        drop(work);
    }

    struct WaitInDrop {
        entered: mpsc::Sender<()>,
        release: mpsc::Receiver<()>,
    }
    impl Drop for WaitInDrop {
        fn drop(&mut self) {
            self.entered.send(()).unwrap();
            self.release.recv_timeout(Duration::from_secs(5)).unwrap();
        }
    }
    #[tokio::test]
    async fn drainage_waits_for_drop_after_terminal_cancellation_was_published() {
        let scopes = Scopes::new();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let capture = WaitInDrop {
            entered: entered_tx,
            release: release_rx,
        };
        let source = CancellationSource::new();
        let token = source.token();
        let work = scopes.execution(
            async move {
                let _capture = capture;
                std::future::pending::<Result<()>>().await
            },
            source,
        );
        let closer_scopes = scopes.clone();
        let closer = std::thread::spawn(move || closer_scopes.close());
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(token.reason(), Some(Reason::CacheShutdown));
        let mut drain = std::pin::pin!(scopes.drained());
        let waited = tokio::time::timeout(Duration::from_millis(20), &mut drain)
            .await
            .is_err();
        // Release and join even if the assertion fails, so this test cannot
        // strand a user destructor or hide the underlying drainage failure.
        release_tx.send(()).unwrap();
        assert!(closer.join().unwrap());
        assert!(waited);
        tokio::time::timeout(Duration::from_secs(1), drain)
            .await
            .unwrap();
        drop(work);
    }

    struct CountDrop(Arc<AtomicUsize>);
    impl Drop for CountDrop {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct PanicDrop;
    impl Drop for PanicDrop {
        fn drop(&mut self) {
            std::panic::panic_any(73_u32);
        }
    }
    #[test]
    fn a_panicking_destructor_does_not_leave_other_shutdown_work_alive() {
        let scopes = Scopes::new();
        let panic = PanicDrop;
        let first = scopes.execution(
            async move {
                let _panic = panic;
                std::future::pending::<Result<()>>().await
            },
            CancellationSource::new(),
        );
        let drops = Arc::new(AtomicUsize::new(0));
        let count = CountDrop(drops.clone());
        let second = scopes.execution(
            async move {
                let _count = count;
                std::future::pending::<Result<()>>().await
            },
            CancellationSource::new(),
        );
        let panic =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| scopes.close())).unwrap_err();
        assert_eq!(panic.downcast_ref::<u32>(), Some(&73));
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        let mut drain = std::pin::pin!(scopes.drained());
        assert!(
            drain
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_ready()
        );
        drop((first, second));
    }
}
