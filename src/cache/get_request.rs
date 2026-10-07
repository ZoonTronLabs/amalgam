//! Lazy origin requests. Input choices are separate from pinned execution.
use super::inline_cold::Start;
use super::observed_execution::ObservedExecution;
use super::{
    Cache, CacheValue, EntryOptions, FactoryCancellation, FactoryContext, FactoryError,
    FactoryOrigin, FactoryProduct, MaybeValue, Result, Tag,
};
use std::future::{Future, IntoFuture};
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

struct Input<K, F, V> {
    key: K,
    factory: F,
    options: Option<Box<EntryOptions>>,
    tags: std::result::Result<Box<[Tag]>, crate::TagError>,
    fallback: MaybeValue<V>,
    token: Option<FactoryCancellation>,
}

/// A lazy factory request returning its cached or computed value when awaited.
#[must_use = "a cache request only executes when awaited"]
pub struct GetOrSetRequest<'a, K, F, V: Clone + Send + Sync + 'static> {
    cache: &'a Cache<V>,
    input: Input<K, F, V>,
}

/// A lazy factory request returning the value and its actual commit receipt.
#[must_use = "a cache request only executes when awaited"]
pub struct ReceiptGetOrSetRequest<'a, K, F, V: Clone + Send + Sync + 'static> {
    cache: &'a Cache<V>,
    input: Input<K, F, V>,
}

impl<'a, K, F, V: Clone + Send + Sync + 'static> GetOrSetRequest<'a, K, F, V> {
    pub(super) fn new(cache: &'a Cache<V>, key: K, factory: F) -> Self {
        Self {
            cache,
            input: Input {
                key,
                factory,
                options: None,
                tags: Ok(Box::from([])),
                fallback: MaybeValue::none(),
                token: None,
            },
        }
    }

    /// Requests the mutation report instead of returning only the value.
    pub fn with_receipt(self) -> ReceiptGetOrSetRequest<'a, K, F, V> {
        ReceiptGetOrSetRequest {
            cache: self.cache,
            input: self.input,
        }
    }
}

macro_rules! settings {
    ($request:ident) => {
        impl<K, F, V: Clone + Send + Sync + 'static> $request<'_, K, F, V> {
            /// Edits a copy of cache defaults, retaining untouched settings.
            pub fn options(mut self, update: impl FnOnce(EntryOptions) -> EntryOptions) -> Self {
                let options = self
                    .input
                    .options
                    .take()
                    .map(|options| *options)
                    .unwrap_or_else(|| self.cache.entry_options());
                self.input.options = Some(Box::new(update(options)));
                self
            }

            /// Validates raw string tags; an invalid tag rejects the awaited request.
            pub fn tags<I, S>(mut self, tags: I) -> Self
            where
                I: IntoIterator<Item = S>,
                S: AsRef<str>,
            {
                self.input.tags = crate::tags::try_collect_tags(tags);
                self
            }

            /// Sets the optional fail-safe value for this caller.
            pub fn fail_safe_default(mut self, value: V) -> Self {
                self.input.fallback = MaybeValue::from_value(value);
                self
            }

            /// Uses an explicit caller cancellation signal.
            pub fn cancellation(mut self, token: FactoryCancellation) -> Self {
                self.input.token = Some(token);
                self
            }
        }
    };
}
settings!(GetOrSetRequest);
settings!(ReceiptGetOrSetRequest);

enum State<K, F, V: Send + 'static> {
    Start(Input<K, F, V>),
    Pending(ObservedExecution<CacheValue<V>>),
    Finished,
}

/// The pinned execution of a factory request, created by `IntoFuture`.
pub struct GetOrSetFuture<'a, K, F, V: Clone + Send + Sync + 'static, T = V> {
    cache: &'a Cache<V>,
    state: State<K, F, V>,
    output: PhantomData<fn() -> T>,
}
// K/F/V are unpinned input captures, consumed before user work is created. The
// movable observer retains work in its separate stable pinned allocation.
impl<K, F, V: Clone + Send + Sync + 'static, T> Unpin for GetOrSetFuture<'_, K, F, V, T> {}

trait Output<V> {
    fn complete(value: CacheValue<V>) -> Self;
}
impl<V> Output<V> for V {
    fn complete(value: CacheValue<V>) -> Self {
        value.value
    }
}
impl<V> Output<V> for CacheValue<V> {
    fn complete(value: CacheValue<V>) -> Self {
        value
    }
}

macro_rules! execution {
    ($request:ident, $output:ty) => {
        impl<'a, K, F, Fut, V> IntoFuture for $request<'a, K, F, V>
        where
            K: AsRef<str>,
            V: Clone + Send + Sync + 'static,
            F: FnOnce(FactoryContext<V>) -> Fut + Send + 'static,
            Fut: Future<Output = std::result::Result<FactoryProduct<V>, FactoryError>>
                + Send
                + 'static,
        {
            type Output = Result<$output>;
            type IntoFuture = GetOrSetFuture<'a, K, F, V, $output>;
            fn into_future(self) -> Self::IntoFuture {
                GetOrSetFuture {
                    cache: self.cache,
                    state: State::Start(self.input),
                    output: PhantomData,
                }
            }
        }
    };
}
execution!(GetOrSetRequest, V);
execution!(ReceiptGetOrSetRequest, CacheValue<V>);

impl<K, F, Fut, V, T> Future for GetOrSetFuture<'_, K, F, V, T>
where
    K: AsRef<str>,
    V: Clone + Send + Sync + 'static,
    F: FnOnce(FactoryContext<V>) -> Fut + Send + 'static,
    Fut: Future<Output = std::result::Result<FactoryProduct<V>, FactoryError>> + Send + 'static,
    T: Output<V>,
{
    type Output = Result<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if matches!(this.state, State::Start(_)) {
            let State::Start(input) = std::mem::replace(&mut this.state, State::Finished) else {
                unreachable!("start state was matched");
            };
            match this.cache.begin_origin_request(
                input.key.as_ref(),
                FactoryOrigin::new(input.factory),
                input.options,
                input.tags,
                input.fallback,
                input.token,
            ) {
                Start::Ready(result) => return Poll::Ready(result.map(T::complete)),
                Start::Pending(work) => this.state = State::Pending(work),
            }
        }
        let State::Pending(work) = &mut this.state else {
            panic!("completed cache origin was polled again");
        };
        let result = Pin::new(work).poll(cx);
        if result.is_ready() {
            this.state = State::Finished;
        }
        result.map(|result| result.map(T::complete))
    }
}
