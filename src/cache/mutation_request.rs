//! Mutation input is lazy; only an actual asynchronous write owns a future.
use super::memory_inline::MutationStart;
use super::{Cache, EntryOptions, FactoryCancellation, MutationReceipt, Result, Tag};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

struct Input<K, V> {
    key: K,
    value: V,
    options: Option<Box<EntryOptions>>,
    tags: Box<[Tag]>,
    token: Option<FactoryCancellation>,
}
enum State<'a, K, V> {
    Start(Input<K, V>),
    Pending(MutationStart<'a>),
    Finished,
}
pub(super) struct MutationRequest<'a, K, V: Clone + Send + Sync + 'static> {
    cache: &'a Cache<V>,
    state: State<'a, K, V>,
}
// Inputs are never structurally pinned or exposed as Pin. Asynchronous work
// owns its separate stable pinned pointer, including when K/V themselves are !Unpin.
impl<K, V: Clone + Send + Sync + 'static> Unpin for MutationRequest<'_, K, V> {}
impl<'a, K, V: Clone + Send + Sync + 'static> MutationRequest<'a, K, V> {
    pub(super) fn new(
        cache: &'a Cache<V>,
        key: K,
        value: V,
        options: Option<EntryOptions>,
        tags: Box<[Tag]>,
        token: Option<FactoryCancellation>,
    ) -> Self {
        Self {
            cache,
            state: State::Start(Input {
                key,
                value,
                options: options.map(Box::new),
                tags,
                token,
            }),
        }
    }
}
impl<K: AsRef<str>, V: Clone + Send + Sync + 'static> Future for MutationRequest<'_, K, V> {
    type Output = Result<MutationReceipt>;
    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if matches!(this.state, State::Start(_)) {
            let State::Start(input) = std::mem::replace(&mut this.state, State::Finished) else {
                unreachable!("start state was matched");
            };
            this.state = State::Pending(this.cache.set_impl(
                input.key.as_ref(),
                input.value,
                input.options,
                input.tags,
                input.token,
            ));
        }
        let State::Pending(work) = &mut this.state else {
            panic!("completed cache mutation was polled again");
        };
        let result = Pin::new(work).poll(context);
        if result.is_ready() {
            this.state = State::Finished;
        }
        result
    }
}
