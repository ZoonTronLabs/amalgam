//! Typed original-value diagnostics with bounded delivery and explicit reclamation.
use super::EventStreamClosed;
use crate::entry::Entry;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use tokio::sync::Notify;

/// The actual physical retirement of a stored representation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryEvictionReason {
    /// Explicit removal or loss of local read eligibility.
    Removed,
    /// A new value replaced the representation.
    Replaced,
    /// Its absolute or selected monotonic physical deadline elapsed.
    Expired,
    /// It was a capacity victim.
    Capacity,
}

/// Selects when an entry becomes eligible for original-value diagnostics.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum EvictionCapture {
    /// A value/component subscriber must exist when the entry is inserted.
    #[default]
    AtInsertion,
    /// Current subscribers also observe retirement of previously inserted values.
    AtRetirement,
}

/// The original stored value and metadata. Cloning only clones entry handles.
#[derive(Debug)]
pub struct MemoryEviction<V> {
    key: Arc<str>,
    reason: MemoryEvictionReason,
    entry: Entry<V>,
}
impl<V> Clone for MemoryEviction<V> {
    fn clone(&self) -> Self {
        Self {
            key: Arc::clone(&self.key),
            reason: self.reason,
            entry: self.entry.clone(),
        }
    }
}
impl<V> MemoryEviction<V> {
    /// The processed logical key.
    pub fn key(&self) -> &str {
        &self.key
    }
    /// The cause of physical retirement.
    pub fn reason(&self) -> MemoryEvictionReason {
        self.reason
    }
    /// Borrows the exact stored value; no user clone was made for diagnostics.
    pub fn value(&self) -> &V {
        self.entry.value()
    }
    /// The actual immutable stored representation, including metadata.
    pub fn entry(&self) -> &Entry<V> {
        &self.entry
    }
}

/// A non-owning-cache, lazily allocated original-value event producer.
pub struct MemoryEvictions<V> {
    producer: Arc<Producer<V>>,
}
impl<V> Clone for MemoryEvictions<V> {
    fn clone(&self) -> Self {
        Self {
            producer: Arc::clone(&self.producer),
        }
    }
}
struct Producer<V> {
    stream: OnceLock<Arc<Hub<V>>>,
    capacity: usize,
}
struct Hub<V> {
    state: Mutex<State<V>>,
    receivers: AtomicUsize,
    changed: Notify,
}
#[derive(Clone, Copy)]
enum Phase {
    Open,
    Closed,
}
struct State<V> {
    phase: Phase,
    next: u128,
    frames: VecDeque<Frame<V>>,
}
struct Frame<V> {
    sequence: u128,
    event: MemoryEviction<V>,
}

/// An immediately attempted original-value receive has no value or is closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum EvictionReceiveError {
    /// The live stream currently has no new record.
    #[error("no new memory eviction")]
    Empty,
    /// No future records can arrive.
    #[error("memory eviction stream closed")]
    Closed,
}

/// An independent cursor with automatic lag recovery and cumulative loss.
pub struct MemoryEvictionSubscription<V> {
    hub: Arc<Hub<V>>,
    next: u128,
    lost: u64,
}

impl<V> MemoryEvictions<V> {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            producer: Arc::new(Producer {
                stream: OnceLock::new(),
                capacity: capacity.max(1),
            }),
        }
    }
    /// Subscribes to future eligible retirements without retaining the cache.
    #[must_use]
    pub fn subscribe(&self) -> MemoryEvictionSubscription<V> {
        let hub = self.producer.stream.get_or_init(|| {
            Arc::new(Hub {
                state: Mutex::new(State {
                    phase: Phase::Open,
                    next: 0,
                    frames: VecDeque::new(),
                }),
                receivers: AtomicUsize::new(0),
                changed: Notify::new(),
            })
        });
        let state = lock(&hub.state);
        hub.receivers.fetch_add(1, Ordering::AcqRel);
        MemoryEvictionSubscription {
            hub: Arc::clone(hub),
            next: state.next,
            lost: 0,
        }
    }
    pub(crate) fn has_receivers(&self) -> bool {
        self.producer
            .stream
            .get()
            .is_some_and(|hub| hub.receivers.load(Ordering::Acquire) != 0)
    }
    pub(crate) fn emit(
        &self,
        key: &Arc<str>,
        reason: MemoryEvictionReason,
        entry: &Entry<V>,
    ) -> Option<Entry<V>> {
        let hub = self.producer.stream.get()?;
        if hub.receivers.load(Ordering::Acquire) == 0 {
            return None;
        }
        let event = MemoryEviction {
            key: Arc::clone(key),
            reason,
            entry: entry.clone(),
        };
        let retired = {
            let mut state = lock(&hub.state);
            if matches!(state.phase, Phase::Closed) || hub.receivers.load(Ordering::Acquire) == 0 {
                return None;
            }
            let Some(next) = state.next.checked_add(1) else {
                state.phase = Phase::Closed;
                drop(state);
                hub.changed.notify_waiters();
                return None;
            };
            let sequence = state.next;
            state.next = next;
            state.frames.push_back(Frame { sequence, event });
            if state.frames.len() > self.producer.capacity {
                state.frames.pop_front()
            } else {
                None
            }
        };
        hub.changed.notify_waiters();
        // The caller retains a displaced slot through its outer cache guards.
        retired.map(|frame| frame.event.entry)
    }
}
impl<V> MemoryEvictionSubscription<V> {
    /// Receives the next record, accounting for overwritten records automatically.
    pub async fn recv(&mut self) -> Result<MemoryEviction<V>, EventStreamClosed> {
        loop {
            let hub = Arc::clone(&self.hub);
            let notified = hub.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            match self.try_recv() {
                Ok(event) => return Ok(event),
                Err(EvictionReceiveError::Closed) => return Err(EventStreamClosed),
                Err(EvictionReceiveError::Empty) => notified.await,
            }
        }
    }
    /// Synchronously reads an available record; suitable for native cache users.
    pub fn try_recv(&mut self) -> Result<MemoryEviction<V>, EvictionReceiveError> {
        let state = lock(&self.hub.state);
        let oldest = state
            .frames
            .front()
            .map_or(state.next, |frame| frame.sequence);
        if self.next < oldest {
            self.lost = self
                .lost
                .saturating_add(u64::try_from(oldest - self.next).unwrap_or(u64::MAX));
            self.next = oldest;
        }
        if self.next < state.next {
            // The retained distance is bounded by capacity (a usize).
            let index = (self.next - oldest) as usize;
            let event = state.frames[index].event.clone();
            self.next += 1;
            return Ok(event);
        }
        match state.phase {
            Phase::Open => Err(EvictionReceiveError::Empty),
            Phase::Closed => Err(EvictionReceiveError::Closed),
        }
    }
    /// Total overwritten records for this cursor (saturating).
    pub fn lost_events(&self) -> u64 {
        self.lost
    }
}
impl<V> Drop for MemoryEvictionSubscription<V> {
    fn drop(&mut self) {
        let retired = {
            let mut state = lock(&self.hub.state);
            if self.hub.receivers.fetch_sub(1, Ordering::AcqRel) == 1 {
                std::mem::take(&mut state.frames)
            } else {
                VecDeque::new()
            }
        };
        drop(retired);
    }
}
impl<V> Drop for Producer<V> {
    fn drop(&mut self) {
        if let Some(hub) = self.stream.get() {
            lock(&hub.state).phase = Phase::Closed;
            hub.changed.notify_waiters();
        }
    }
}
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
impl<V> std::fmt::Debug for MemoryEvictions<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryEvictions")
            .field("capacity", &self.producer.capacity)
            .finish_non_exhaustive()
    }
}
impl<V> std::fmt::Debug for MemoryEvictionSubscription<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryEvictionSubscription")
            .field("lost", &self.lost)
            .finish_non_exhaustive()
    }
}
