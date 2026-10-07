//! First-poll ownership: count and pin work, promote only a real suspension.
//! The transferable admission remains until work retirement or subscribed scope
//! ownership. No thread-bound guard is stored in a future.
use super::{CancellationSource, OwnedInlinePermit, Reason, Scope, Scopes, Work};
use crate::error::{Error, Result};
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll};

enum Ownership<T> {
    Held(Work<T>),
    Transferred,
}
pub(super) struct InlineFrame<T> {
    work: Ownership<T>,
    source: CancellationSource,
    permit: OwnedInlinePermit,
}
pub(super) enum FirstPoll<T> {
    Ready(Result<T>),
    Suspended(Arc<Scope<T>>),
}
impl<T: Send + 'static> InlineFrame<T> {
    pub(super) fn new(
        registry: &Arc<Scopes>,
        work: impl Future<Output = Result<T>> + Send + 'static,
        source: CancellationSource,
    ) -> Self {
        let permit = registry.inline_owned();
        Self {
            work: Ownership::Held(Box::pin(work)),
            source,
            permit,
        }
    }
    pub(super) fn cancel(&self, reason: Reason) {
        self.source.cancel_with(reason);
    }
    pub(super) fn first_poll(mut self, cx: &mut Context<'_>) -> FirstPoll<T> {
        if let Some(reason) = self.source.token().reason() {
            return FirstPoll::Ready(Err(Error::OperationCancelled { reason }));
        }
        let Ownership::Held(work) = &mut self.work else {
            unreachable!("first-poll work has one owner");
        };
        let result = work.as_mut().poll(cx);
        if let Some(reason) = self.source.token().reason() {
            return FirstPoll::Ready(Err(Error::OperationCancelled { reason }));
        }
        match result {
            Poll::Ready(result) => self.complete(result),
            Poll::Pending => self.promote(cx),
        }
    }
    fn complete(&self, result: Result<T>) -> FirstPoll<T> {
        self.source.cancel_with(Reason::ScopeFinished);
        match self.source.token().reason() {
            Some(Reason::ScopeFinished) => FirstPoll::Ready(result),
            Some(reason) => FirstPoll::Ready(Err(Error::OperationCancelled { reason })),
            None => unreachable!("completion publishes a terminal reason"),
        }
    }
    fn promote(&mut self, cx: &mut Context<'_>) -> FirstPoll<T> {
        let Ownership::Held(work) = std::mem::replace(&mut self.work, Ownership::Transferred)
        else {
            unreachable!("suspended work has one owner");
        };
        let scope = self.permit.registry.subscribe_pinned(
            work,
            self.source.clone(),
            Some(cx.waker().clone()),
        );
        FirstPoll::Suspended(scope)
    }
}
impl<T> Drop for InlineFrame<T> {
    fn drop(&mut self) {
        let Ownership::Held(work) = std::mem::replace(&mut self.work, Ownership::Transferred)
        else {
            return;
        };
        // Keep admission until destruction actually ends, including unwinding.
        let reason = self
            .source
            .token()
            .reason()
            .unwrap_or(Reason::CallerDropped);
        self.source.cancel_with(reason);
        drop(work);
    }
}
