//! A lazy read owns asynchronous work only after a real miss.
use super::{Cache, EntryOptions, FactoryCancellation, MaybeValue, Result};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

type Pending<V> = Pin<Box<dyn Future<Output = Result<MaybeValue<V>>> + Send + 'static>>;
pub(super) enum ReadStart<V> {
    Ready(Result<MaybeValue<V>>),
    Pending(Pending<V>),
}
struct Input<K> {
    key: K,
    options: Option<Box<EntryOptions>>,
    token: Option<FactoryCancellation>,
}
enum State<K, V> {
    Start(Input<K>),
    Pending(Pending<V>),
    Finished,
}
pub(super) struct ReadRequest<'a, K, V: Clone + Send + Sync + 'static> {
    cache: &'a Cache<V>,
    state: State<K, V>,
}
// The key is never structurally pinned or projected as Pin. It is an input,
// not a future; the only pinned field is already behind its stable heap pointer.
impl<K, V: Clone + Send + Sync + 'static> Unpin for ReadRequest<'_, K, V> {}
impl<'a, K, V: Clone + Send + Sync + 'static> ReadRequest<'a, K, V> {
    pub(super) fn new(
        cache: &'a Cache<V>,
        key: K,
        options: Option<EntryOptions>,
        token: Option<FactoryCancellation>,
    ) -> Self {
        Self {
            cache,
            state: State::Start(Input {
                key,
                options: options.map(Box::new),
                token,
            }),
        }
    }
}
impl<K: AsRef<str>, V: Clone + Send + Sync + 'static> Future for ReadRequest<'_, K, V> {
    type Output = Result<MaybeValue<V>>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if matches!(this.state, State::Start(_)) {
            let State::Start(input) = std::mem::replace(&mut this.state, State::Finished) else {
                unreachable!("start state was matched");
            };
            match this
                .cache
                .begin_read(input.key.as_ref(), input.options, input.token)
            {
                ReadStart::Ready(result) => return Poll::Ready(result),
                ReadStart::Pending(work) => this.state = State::Pending(work),
            }
        }
        let State::Pending(work) = &mut this.state else {
            panic!("completed cache read was polled again");
        };
        let result = work.as_mut().poll(cx);
        if result.is_ready() {
            this.state = State::Finished;
        }
        result
    }
}
