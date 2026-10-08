//! Lazy read requests keep input separate from admitted execution.
use super::observed_execution::ObservedExecution;
use super::{Cache, EntryOptions, FactoryCancellation, Result};
use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::task::{Context, Poll};

pub(super) enum ReadStart<T: Send + 'static> {
    Ready(Result<T>),
    Pending(ObservedExecution<T>),
}
struct Input<K> {
    key: K,
    options: Option<Box<EntryOptions>>,
    token: Option<FactoryCancellation>,
}
/// A lazy read-only lookup returning a value or an explicit miss.
#[must_use = "a cache request only executes when awaited"]
pub struct TryGetRequest<'a, K, V: Clone + Send + Sync + 'static> {
    cache: &'a Cache<V>,
    input: Input<K>,
}
/// A lazy read-only lookup returning the supplied default on a successful miss.
#[must_use = "a cache request only executes when awaited"]
pub struct GetOrDefaultRequest<'a, K, V: Clone + Send + Sync + 'static> {
    cache: &'a Cache<V>,
    input: Input<K>,
    default: V,
}
impl<'a, K, V: Clone + Send + Sync + 'static> TryGetRequest<'a, K, V> {
    pub(super) fn new(cache: &'a Cache<V>, key: K) -> Self {
        Self {
            cache,
            input: Input {
                key,
                options: None,
                token: None,
            },
        }
    }
}
impl<'a, K, V: Clone + Send + Sync + 'static> GetOrDefaultRequest<'a, K, V> {
    pub(super) fn new(cache: &'a Cache<V>, key: K, default: V) -> Self {
        Self {
            cache,
            input: Input {
                key,
                options: None,
                token: None,
            },
            default,
        }
    }
}
macro_rules! settings {
    ($request:ident) => {
        impl<K, V: Clone + Send + Sync + 'static> $request<'_, K, V> {
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
            /// Links an explicit caller cancellation signal during execution.
            pub fn cancellation(mut self, token: FactoryCancellation) -> Self {
                self.input.token = Some(token);
                self
            }
        }
    };
}
settings!(TryGetRequest);
settings!(GetOrDefaultRequest);

enum State<K, T: Send + 'static> {
    Start(Input<K>),
    Pending(ObservedExecution<T>),
    Finished,
}
/// The execution of a read-only request, created by IntoFuture.
pub struct TryGetFuture<'a, K, V: Clone + Send + Sync + 'static> {
    cache: &'a Cache<V>,
    state: State<K, Option<V>>,
}
// Input keys are never structurally pinned. Owned work remains in its stable allocation.
impl<K, V: Clone + Send + Sync + 'static> Unpin for TryGetFuture<'_, K, V> {}
impl<'a, K, V: Clone + Send + Sync + 'static> TryGetFuture<'a, K, V> {
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
impl<'a, K: AsRef<str>, V: Clone + Send + Sync + 'static> IntoFuture for TryGetRequest<'a, K, V> {
    type Output = Result<Option<V>>;
    type IntoFuture = TryGetFuture<'a, K, V>;
    fn into_future(self) -> Self::IntoFuture {
        TryGetFuture {
            cache: self.cache,
            state: State::Start(self.input),
        }
    }
}
impl<K: AsRef<str>, V: Clone + Send + Sync + 'static> Future for TryGetFuture<'_, K, V> {
    type Output = Result<Option<V>>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match &mut this.state {
            State::Start(_) => {
                let State::Start(input) = std::mem::replace(&mut this.state, State::Finished)
                else {
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
            State::Pending(_) => {}
            State::Finished => panic!("completed cache read was polled again"),
        }
        let State::Pending(work) = &mut this.state else {
            panic!("completed cache read was polled again");
        };
        let result = Pin::new(work).poll(cx);
        if result.is_ready() {
            this.state = State::Finished;
        }
        result
    }
}

enum DefaultState<K, V: Send + 'static> {
    Start { input: Input<K>, default: V },
    Pending(ObservedExecution<V>),
    Finished,
}
/// The execution of a read-only default request, created by IntoFuture.
pub struct GetOrDefaultFuture<'a, K, V: Clone + Send + Sync + 'static> {
    cache: &'a Cache<V>,
    state: DefaultState<K, V>,
}
impl<K, V: Clone + Send + Sync + 'static> Unpin for GetOrDefaultFuture<'_, K, V> {}
impl<'a, K: AsRef<str>, V: Clone + Send + Sync + 'static> IntoFuture
    for GetOrDefaultRequest<'a, K, V>
{
    type Output = Result<V>;
    type IntoFuture = GetOrDefaultFuture<'a, K, V>;
    fn into_future(self) -> Self::IntoFuture {
        GetOrDefaultFuture {
            cache: self.cache,
            state: DefaultState::Start {
                input: self.input,
                default: self.default,
            },
        }
    }
}
impl<K: AsRef<str>, V: Clone + Send + Sync + 'static> Future for GetOrDefaultFuture<'_, K, V> {
    type Output = Result<V>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match &mut this.state {
            DefaultState::Start { .. } => {
                let DefaultState::Start { input, default } =
                    std::mem::replace(&mut this.state, DefaultState::Finished)
                else {
                    unreachable!("start state was matched");
                };
                match this.cache.begin_default_read(
                    input.key.as_ref(),
                    input.options.map(|options| *options),
                    input.token,
                    default,
                ) {
                    ReadStart::Ready(result) => return Poll::Ready(result),
                    ReadStart::Pending(work) => this.state = DefaultState::Pending(work),
                }
            }
            DefaultState::Pending(_) => {}
            DefaultState::Finished => panic!("completed cache default read was polled again"),
        }
        let DefaultState::Pending(work) = &mut this.state else {
            panic!("completed cache default read was polled again");
        };
        let result = Pin::new(work).poll(cx);
        if result.is_ready() {
            this.state = DefaultState::Finished;
        }
        result
    }
}

/// A lazy native lookup sharing asynchronous admission and its ready L1 path.
#[must_use = "a native cache request only executes through execute()"]
pub struct BlockingTryGetRequest<'a, K, V: Clone + Send + Sync + 'static> {
    cache: &'a super::BlockingCache<V>,
    input: Input<K>,
}
impl<'a, K, V: Clone + Send + Sync + 'static> BlockingTryGetRequest<'a, K, V> {
    pub(super) fn new(cache: &'a super::BlockingCache<V>, key: K) -> Self {
        Self {
            cache,
            input: Input {
                key,
                options: None,
                token: None,
            },
        }
    }
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
    /// Links an explicit caller cancellation signal during execution.
    pub fn cancellation(mut self, token: FactoryCancellation) -> Self {
        self.input.token = Some(token);
        self
    }
}
impl<K: AsRef<str>, V: Clone + Send + Sync + 'static> BlockingTryGetRequest<'_, K, V> {
    /// Runs the native lookup and returns a typed value, miss or error.
    pub fn execute(self) -> Result<Option<V>> {
        self.cache.execute_read(
            self.input.key.as_ref(),
            self.input.options.map(|options| *options),
            self.input.token,
        )
    }
}
