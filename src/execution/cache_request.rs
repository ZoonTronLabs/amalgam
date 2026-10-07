//! A cache-bound cancellation view without an owned work scope. Reading a
//! terminal reason never invokes listeners or destroys user work. Only an
//! actual outgoing link subscribes this view for independent shutdown delivery.
use super::{
    CancelWork, CancellationState, Reason, Request, RequestOwner, Scopes, TrackedCancellation,
    finish_tracking,
};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

pub(crate) struct CacheBinding {
    pub(super) registry: Arc<Scopes>,
    tracking: parking_lot::Mutex<Option<TrackedCancellation>>,
}
impl CacheBinding {
    pub(super) fn new(registry: Arc<Scopes>) -> Self {
        Self {
            registry,
            tracking: parking_lot::Mutex::new(None),
        }
    }
    pub(super) fn subscribe(&self, owner: Arc<dyn CancelWork>) {
        self.registry.register_work(owner, &self.tracking);
    }
    pub(super) fn finish(&self) {
        finish_tracking(&self.tracking);
    }
    pub(super) fn reason(&self) -> Option<Reason> {
        self.registry.is_closed().then_some(Reason::CacheShutdown)
    }
}
impl fmt::Debug for CacheBinding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CacheBinding").finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum Delivery {
    FirstPoll = 0,
    Owned = 1,
}

#[derive(Debug)]
pub(super) struct CacheRequest {
    request: Request,
    binding: CacheBinding,
    delivery: AtomicU8,
}
impl CacheRequest {
    pub(super) fn new(registry: Arc<Scopes>) -> Self {
        Self {
            request: Request::new(),
            binding: CacheBinding::new(registry),
            delivery: AtomicU8::new(Delivery::FirstPoll as u8),
        }
    }
}
impl CacheRequest {
    fn delivery(&self) -> Delivery {
        match self.delivery.load(Ordering::Acquire) {
            0 => Delivery::FirstPoll,
            1 => Delivery::Owned,
            _ => unreachable!("only closed delivery states are stored"),
        }
    }
}
impl RequestOwner for CacheRequest {
    fn request(&self) -> &Request {
        &self.request
    }
    fn cache_binding(&self) -> Option<&CacheBinding> {
        match self.delivery() {
            Delivery::FirstPoll => Some(&self.binding),
            Delivery::Owned => None,
        }
    }
    fn inline_root(&self, registry: &Arc<Scopes>) -> bool {
        self.delivery() == Delivery::FirstPoll && Arc::ptr_eq(&self.binding.registry, registry)
    }
    fn track_shutdown(self: Arc<Self>) {
        if self.delivery() == Delivery::FirstPoll {
            let erased: Arc<dyn CancelWork> = self.clone();
            self.binding.subscribe(erased);
        }
    }
    fn promote(&self) {
        // Once pinned work has a scope, that scope owns terminal ordering and
        // shutdown delivery. An old queued view notification must not replace
        // completion which the scope has already committed.
        self.delivery
            .store(Delivery::Owned as u8, Ordering::Release);
        self.binding.finish();
    }
}
impl CancelWork for CacheRequest {
    fn cancel(&self, reason: Reason) {
        let _activity = self.binding.registry.inline();
        match self.delivery() {
            Delivery::FirstPoll => {
                RequestOwner::cancel_with(self, reason);
            }
            Delivery::Owned => {
                self.binding.finish();
            }
        }
    }
    fn finished(&self) -> bool {
        self.delivery() == Delivery::Owned
            || CancellationState::load(&self.request.state)
                .reason()
                .is_some()
    }
}
