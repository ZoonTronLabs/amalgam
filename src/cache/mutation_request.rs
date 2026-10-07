//! Mutation input is lazy; only an actual asynchronous write owns a future.
use super::memory_inline::MutationStart;
use super::{Cache, EntryOptions, FactoryCancellation, MutationReceipt, Result, Tag};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

pub(super) struct Input<K, V> {
    pub(super) key: K,
    pub(super) value: V,
    pub(super) options: Option<Box<EntryOptions>>,
    pub(super) tags: std::result::Result<Box<[Tag]>, crate::TagError>,
    pub(super) token: Option<FactoryCancellation>,
}
pub(super) enum State<'a, K, V, T> {
    Start(Input<K, V>),
    Pending(MutationStart<'a, T>),
    Finished,
}
pub(super) struct MutationRequest<'a, K, V: Clone + Send + Sync + 'static, T = MutationReceipt> {
    pub(super) cache: &'a Cache<V>,
    pub(super) state: State<'a, K, V, T>,
}
// Inputs are never structurally pinned or exposed as Pin. Asynchronous work
// owns its separate stable pinned pointer, including when K/V themselves are !Unpin.
impl<K, V: Clone + Send + Sync + 'static, T> Unpin for MutationRequest<'_, K, V, T> {}

impl<K: AsRef<str>, V: Clone + Send + Sync + 'static, T: MutationOutput> Future
    for MutationRequest<'_, K, V, T>
{
    type Output = Result<T>;
    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if matches!(this.state, State::Start(_)) {
            let State::Start(input) = std::mem::replace(&mut this.state, State::Finished) else {
                unreachable!("start state was matched");
            };
            this.state = State::Pending(this.cache.set_impl::<T>(
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

pub(super) trait MutationOutput: Send + Unpin + 'static {
    fn local(effect: super::LocalEffect) -> Self;
    fn pipeline(receipt: MutationReceipt) -> Self;
}
impl MutationOutput for () {
    fn local(_effect: super::LocalEffect) {}
    fn pipeline(receipt: MutationReceipt) {
        drop(receipt);
    }
}
impl MutationOutput for MutationReceipt {
    fn local(effect: super::LocalEffect) -> Self {
        super::memory_inline::receipt(effect)
    }
    fn pipeline(receipt: MutationReceipt) -> Self {
        receipt
    }
}
