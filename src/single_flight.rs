//! Active computations, not per-key asynchronous lock lanes. A caller owns only
//! a subscription; the table retains suspended work until completion or close.
use crate::error::{Error, FactoryCancellationReason as Reason, Result};
use crate::execution::{CancelWork, CancellationSource, Request, RequestOwner, Scopes};
use ahash::RandomState;
use hashbrown::HashMap;
use parking_lot::Mutex;
use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::task::{Context, Poll, Wake, Waker};

type Work<T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'static>>;
type Panic = Box<dyn Any + Send + 'static>;
type FlightShard<T> = Mutex<HashMap<Arc<str>, Arc<Flight<T>>, RandomState>>;
const SHARDS: usize = 64;

pub(crate) struct Flights<T: Send + Sync + 'static> {
    hash: RandomState,
    shards: Box<[FlightShard<T>]>,
}
impl<T: Send + Sync + 'static> Flights<T> {
    pub(crate) fn new() -> Arc<Self> {
        let hash = RandomState::new();
        Arc::new(Self {
            shards: (0..SHARDS)
                .map(|_| Mutex::new(HashMap::with_hasher(hash.clone())))
                .collect(),
            hash,
        })
    }
    pub(crate) fn acquire(self: &Arc<Self>, key: Arc<str>, scopes: Arc<Scopes>) -> Claim<T> {
        let shard = self.hash.hash_one(key.as_ref()) as usize & (SHARDS - 1);
        let mut entries = self.shards[shard].lock();
        let (flight, leader) = match entries.get(key.as_ref()) {
            Some(flight) => {
                flight.cell.lock().subscribers += 1;
                (Arc::clone(flight), false)
            }
            None => {
                let flight = Arc::new(Flight {
                    owner: Arc::downgrade(self),
                    key: Arc::clone(&key),
                    shard,
                    scopes,
                    request: Request::new(),
                    revision: AtomicU64::new(1),
                    cell: Mutex::new(Cell {
                        subscribers: 1,
                        state: State::Starting,
                    }),
                    runnable: AtomicBool::new(true),
                    wakers: Mutex::new(Vec::new()),
                });
                entries.insert(key, Arc::clone(&flight));
                (flight, true)
            }
        };
        Claim {
            subscription: Subscription {
                flight,
                leader,
                consumed: false,
                polled: false,
            },
            leader,
        }
    }
}

pub(crate) struct Claim<T: Send + Sync + 'static> {
    pub(crate) subscription: Subscription<T>,
    pub(crate) leader: bool,
}
pub(crate) struct Subscription<T: Send + Sync + 'static> {
    pub(crate) flight: Arc<Flight<T>>,
    leader: bool,
    consumed: bool,
    polled: bool,
}
pub(crate) enum Completion<T> {
    Owned(Result<T>),
    Shared(Arc<Result<T>>),
    Panicked(Panic),
    FollowerPanicked,
}
enum Terminal<T> {
    Owned(Result<T>),
    Shared(Arc<Result<T>>),
    Panicked(Option<Panic>),
    Cancelled(Reason),
}
enum State<T> {
    Starting,
    Pending(Work<T>),
    Polling,
    Completed(Terminal<T>),
    Finished,
}
struct Cell<T> {
    subscribers: usize,
    state: State<T>,
}
pub(crate) struct Flight<T: Send + Sync + 'static> {
    owner: Weak<Flights<T>>,
    key: Arc<str>,
    shard: usize,
    scopes: Arc<Scopes>,
    request: Request,
    revision: AtomicU64,
    cell: Mutex<Cell<T>>,
    runnable: AtomicBool,
    wakers: Mutex<Vec<Waker>>,
}
impl<T: Send + Sync + 'static> std::fmt::Debug for Flight<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Flight")
            .field("key", &self.key)
            .finish_non_exhaustive()
    }
}
impl<T: Send + Sync + 'static> crate::memory::RevisionSource for Flight<T> {
    fn current(&self) -> u64 {
        self.revision.load(Ordering::Acquire)
    }
    fn advance(&self) {
        self.revision
            .store(self.current().saturating_add(1), Ordering::Release);
    }
}
impl<T: Send + Sync + 'static> RequestOwner for Flight<T> {
    fn request(&self) -> &Request {
        &self.request
    }
}
impl<T: Send + Sync + 'static> Flight<T> {
    pub(crate) fn key(&self) -> &Arc<str> {
        &self.key
    }
    pub(crate) fn source(self: &Arc<Self>) -> CancellationSource {
        CancellationSource::from_owner(self.clone())
    }
    pub(crate) fn install(self: &Arc<Self>, work: Work<T>) {
        let mut work = Some(work);
        {
            let mut cell = self.cell.lock();
            if matches!(cell.state, State::Starting) {
                cell.state = State::Pending(work.take().unwrap());
            }
        }
        drop(work);
    }
    /// First poll uses the same pinned allocation as every later driver.
    /// This method never polls or destroys user work inside coordination.
    pub(crate) fn poll_work(self: &Arc<Self>) {
        let _activity = self.scopes.inline();
        let work = {
            let mut cell = self.cell.lock();
            if !matches!(cell.state, State::Pending(_))
                || !self.runnable.swap(false, Ordering::AcqRel)
            {
                return;
            }
            match std::mem::replace(&mut cell.state, State::Polling) {
                State::Pending(work) => work,
                State::Starting | State::Polling | State::Completed(_) | State::Finished => {
                    unreachable!()
                }
            }
        };
        let waker = Waker::from(Arc::clone(self));
        let mut context = Context::from_waker(&waker);
        // User creation, poll and completed-future Drop are one unwind boundary.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let mut work = work;
            match work.as_mut().poll(&mut context) {
                Poll::Ready(result) => {
                    drop(work);
                    (Some(result), None)
                }
                Poll::Pending => (None, Some(work)),
            }
        }));
        match result {
            Ok((Some(result), None)) => self.complete(Terminal::Owned(result)),
            Ok((None, Some(work))) => {
                if let Some(reason) = self.source().token().reason() {
                    drop(work);
                    self.complete(Terminal::Cancelled(reason));
                } else {
                    let mut cell = self.cell.lock();
                    cell.state = State::Pending(work);
                    drop(cell);
                    // A cancel can race the transition back to Pending.
                    if let Some(reason) = self.source().token().reason() {
                        self.cancel(reason);
                    }
                }
            }
            Err(panic) => self.complete(Terminal::Panicked(Some(panic))),
            Ok((None, None) | (Some(_), Some(_))) => unreachable!("poll has one ownership outcome"),
        }
    }
    fn complete(self: &Arc<Self>, terminal: Terminal<T>) {
        let source = self.source();
        // Always remove the table entry before publishing terminal state. A new
        // caller then rechecks L1; existing subscriptions keep this computation.
        let retired = self.owner.upgrade().and_then(|owner| {
            let mut entries = owner.shards[self.shard].lock();
            if entries
                .get(self.key.as_ref())
                .is_some_and(|flight| Arc::ptr_eq(flight, self))
            {
                entries.remove(self.key.as_ref())
            } else {
                None
            }
        });
        drop(retired);
        let reason = source.token().reason();
        let terminal = match reason {
            Some(reason) => {
                drop(terminal);
                Terminal::Cancelled(reason)
            }
            None => terminal,
        };
        let discarded = {
            let mut cell = self.cell.lock();
            let terminal = match terminal {
                Terminal::Owned(result) if cell.subscribers > 1 => {
                    Terminal::Shared(Arc::new(result))
                }
                other => other,
            };
            if cell.subscribers == 0 {
                cell.state = State::Finished;
                Some(terminal)
            } else {
                cell.state = State::Completed(terminal);
                None
            }
        };
        drop(discarded);
        source.cancel_with(Reason::ScopeFinished);
        self.notify();
    }
    fn abandon_start(&self) {
        let Some(owner) = self.owner.upgrade() else {
            return;
        };
        let retired = {
            // Same order as acquire: map, then cell. A subscriber arriving
            // after the tentative zero count prevents retirement here.
            let mut entries = owner.shards[self.shard].lock();
            let mut cell = self.cell.lock();
            if cell.subscribers == 0 && matches!(cell.state, State::Starting) {
                cell.state = State::Finished;
                entries.remove(self.key.as_ref())
            } else {
                None
            }
        };
        if retired.is_some() {
            drop(retired);
            self.request.cancel_with(Reason::ScopeFinished);
            self.notify();
        }
    }
    pub(crate) fn register_pending(self: &Arc<Self>) {
        self.scopes.register_work(self.clone());
    }
    pub(crate) fn is_finished(&self) -> bool {
        matches!(
            self.cell.lock().state,
            State::Completed(_) | State::Finished
        )
    }
    fn register_waker(&self, waker: &Waker) {
        let mut wakers = self.wakers.lock();
        if !wakers.iter().any(|old| old.will_wake(waker)) {
            wakers.push(waker.clone());
        }
    }
    fn notify(&self) {
        let wakers = std::mem::take(&mut *self.wakers.lock());
        for waker in wakers {
            waker.wake();
        }
    }
    pub(crate) fn driver(self: Arc<Self>) -> impl Future<Output = Result<()>> + Send {
        std::future::poll_fn(move |cx| {
            self.register_waker(cx.waker());
            self.poll_work();
            if self.is_finished() {
                Poll::Ready(Ok(()))
            } else {
                Poll::Pending
            }
        })
    }
}
impl<T: Send + Sync + 'static> Wake for Flight<T> {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.runnable.store(true, Ordering::Release);
        self.notify();
    }
}
impl<T: Send + Sync + 'static> CancelWork for Flight<T> {
    fn cancel(&self, reason: Reason) {
        let _activity = self.scopes.inline();
        // Use the request itself: cancellation must not require an Arc upgrade.
        self.request.cancel_with(reason);
        let work = {
            let mut cell = self.cell.lock();
            match cell.state {
                State::Starting | State::Pending(_) => Some(std::mem::replace(
                    &mut cell.state,
                    State::Completed(Terminal::Cancelled(reason)),
                )),
                State::Polling | State::Completed(_) | State::Finished => None,
            }
        };
        // Both the last future and any captures are destroyed outside the guard.
        drop(work);
        let retired = self.owner.upgrade().and_then(|owner| {
            let mut entries = owner.shards[self.shard].lock();
            if entries
                .get(self.key.as_ref())
                .is_some_and(|flight| std::ptr::eq(flight.as_ref(), self))
            {
                entries.remove(self.key.as_ref())
            } else {
                None
            }
        });
        drop(retired);
        self.notify();
    }
    fn finished(&self) -> bool {
        self.is_finished()
    }
}
impl<T: Send + Sync + 'static> Subscription<T> {
    pub(crate) fn try_complete(&mut self) -> Option<Completion<T>> {
        let mut cell = self.flight.cell.lock();
        let result = match &mut cell.state {
            State::Completed(Terminal::Owned(_)) => {
                match std::mem::replace(&mut cell.state, State::Finished) {
                    State::Completed(Terminal::Owned(result)) => Completion::Owned(result),
                    _ => unreachable!(),
                }
            }
            State::Completed(Terminal::Shared(result)) => Completion::Shared(Arc::clone(result)),
            State::Completed(Terminal::Panicked(panic)) => {
                if self.leader {
                    panic
                        .take()
                        .map_or(Completion::FollowerPanicked, Completion::Panicked)
                } else {
                    Completion::FollowerPanicked
                }
            }
            State::Completed(Terminal::Cancelled(reason)) => {
                Completion::Owned(Err(Error::OperationCancelled { reason: *reason }))
            }
            State::Starting | State::Pending(_) | State::Polling => return None,
            State::Finished => panic!("completed subscription polled again"),
        };
        self.consumed = true;
        Some(result)
    }
}
impl<T: Send + Sync + 'static> Future for Subscription<T> {
    type Output = Completion<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        assert!(!this.consumed, "completed subscription polled again");
        this.flight.register_waker(cx.waker());
        if !this.polled {
            this.polled = true;
            this.flight.runnable.store(true, Ordering::Release);
        }
        this.flight.poll_work();
        this.try_complete().map_or(Poll::Pending, Poll::Ready)
    }
}
impl<T: Send + Sync + 'static> Drop for Subscription<T> {
    fn drop(&mut self) {
        let _activity = self.flight.scopes.inline();
        let (retired, abandoned) = {
            let mut cell = self.flight.cell.lock();
            cell.subscribers -= 1;
            let abandoned = cell.subscribers == 0 && matches!(cell.state, State::Starting);
            let retired = if cell.subscribers == 0 && matches!(cell.state, State::Completed(_)) {
                Some(std::mem::replace(&mut cell.state, State::Finished))
            } else {
                None
            };
            (retired, abandoned)
        };
        drop(retired);
        if abandoned {
            self.flight.abandon_start();
        }
        self.flight.runnable.store(true, Ordering::Release);
        self.flight.notify();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn abandoned_preparation_does_not_leave_a_key_waiting_forever() {
        let table = Flights::<u64>::new();
        let scopes = Scopes::new();
        let first = table.acquire(Arc::from("abandoned"), scopes.clone());
        let first_signal = first.subscription.flight.source().token();
        assert!(first.leader);
        drop(first);
        assert!(first_signal.is_cancelled());
        let retry = table.acquire(Arc::from("abandoned"), scopes);
        assert!(retry.leader);
    }
}
