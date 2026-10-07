//! Lazy key and marker invalidation over the shared owned execution engine.
use super::observed_execution::ObservedExecution;
use super::{
    Cache, CacheOperation, ClearMode, DistributedExpirePolicy, EntryOptions, FactoryCancellation,
    KeyMutation, MarkerKind, MutationReceipt, Result, Tag,
};
use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::task::{Context, Poll};

pub(super) enum Target<K> {
    Key {
        key: K,
        mutation: KeyMutation,
    },
    Markers {
        kinds: std::result::Result<Vec<MarkerKind>, crate::TagError>,
        operation: CacheOperation,
    },
}
struct Input<'a, K, V: Clone + Send + Sync + 'static> {
    cache: &'a Cache<V>,
    target: Target<K>,
    options: Option<Box<EntryOptions>>,
    token: Option<FactoryCancellation>,
}
impl<'a, K, V: Clone + Send + Sync + 'static> Input<'a, K, V> {
    fn new(cache: &'a Cache<V>, target: Target<K>) -> Self {
        Self {
            cache,
            target,
            options: None,
            token: None,
        }
    }
    fn options(mut self, update: impl FnOnce(EntryOptions) -> EntryOptions) -> Self {
        let base = self
            .options
            .take()
            .map(|options| *options)
            .unwrap_or_else(|| match &self.target {
                Target::Key { .. } => self.cache.entry_options(),
                Target::Markers { .. } => self.cache.tags_entry_options(),
            });
        self.options = Some(Box::new(update(base)));
        self
    }
}

/// Lazy physical key removal returning `Result<()>`.
#[must_use = "a cache request only executes when awaited"]
pub struct RemoveRequest<'a, K, V: Clone + Send + Sync + 'static> {
    input: Input<'a, K, V>,
}
/// Lazy logical expiration returning `Result<()>`.
#[must_use = "a cache request only executes when awaited"]
pub struct ExpireRequest<'a, K, V: Clone + Send + Sync + 'static> {
    input: Input<'a, K, V>,
}
/// Lazy invalidation of one or more raw string tags.
#[must_use = "a cache request only executes when awaited"]
pub struct TagInvalidationRequest<'a, V: Clone + Send + Sync + 'static> {
    input: Input<'a, &'static str, V>,
}
/// Lazy cache-wide invalidation with an explicit expire/remove mode.
#[must_use = "a cache request only executes when awaited"]
pub struct ClearRequest<'a, V: Clone + Send + Sync + 'static> {
    input: Input<'a, &'static str, V>,
}
/// Lazy invalidation with explicit commit-stage evidence.
#[must_use = "a cache request only executes when awaited"]
pub struct ReceiptInvalidationRequest<'a, K, V: Clone + Send + Sync + 'static> {
    input: Input<'a, K, V>,
}
impl<'a, K, V: Clone + Send + Sync + 'static> RemoveRequest<'a, K, V> {
    pub(super) fn new(cache: &'a Cache<V>, key: K) -> Self {
        Self {
            input: Input::new(
                cache,
                Target::Key {
                    key,
                    mutation: KeyMutation::Remove,
                },
            ),
        }
    }
    /// Requests a report of local and distributed commit stages.
    pub fn with_receipt(self) -> ReceiptInvalidationRequest<'a, K, V> {
        ReceiptInvalidationRequest { input: self.input }
    }
}
impl<'a, K, V: Clone + Send + Sync + 'static> ExpireRequest<'a, K, V> {
    pub(super) fn new(cache: &'a Cache<V>, key: K) -> Self {
        Self {
            input: Input::new(
                cache,
                Target::Key {
                    key,
                    mutation: KeyMutation::Expire(DistributedExpirePolicy::default()),
                },
            ),
        }
    }
    /// Selects explicit advanced L2 retention; ordinary expiration removes L2.
    pub fn distributed_policy(mut self, policy: DistributedExpirePolicy) -> Self {
        let Target::Key {
            mutation: KeyMutation::Expire(current),
            ..
        } = &mut self.input.target
        else {
            unreachable!("expiration requests are constructed only for expiration")
        };
        *current = policy;
        self
    }
    /// Requests a report of local and distributed commit stages.
    pub fn with_receipt(self) -> ReceiptInvalidationRequest<'a, K, V> {
        ReceiptInvalidationRequest { input: self.input }
    }
}
impl<'a, V: Clone + Send + Sync + 'static> TagInvalidationRequest<'a, V> {
    pub(super) fn new(cache: &'a Cache<V>, tag: impl AsRef<str>) -> Self {
        Self {
            input: Input::new(
                cache,
                Target::Markers {
                    kinds: Tag::new(tag).map(|tag| vec![MarkerKind::Tag(tag)]),
                    operation: CacheOperation::RemoveByTag,
                },
            ),
        }
    }
    /// Adds tags to the same request. Any invalid tag rejects the entire batch.
    pub fn and_tags<I, S>(mut self, tags: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let Target::Markers { kinds, operation } = &mut self.input.target else {
            unreachable!("tag requests are constructed only for markers")
        };
        match (kinds.as_mut(), crate::tags::try_collect_tags(tags)) {
            (Ok(kinds), Ok(tags)) => {
                *operation = CacheOperation::RemoveByTags;
                kinds.extend(Vec::from(tags).into_iter().map(MarkerKind::Tag));
            }
            (Ok(_), Err(error)) => *kinds = Err(error),
            // A prior tag rejection is terminal; awaiting returns that same error.
            (Err(_), Ok(_) | Err(_)) => {}
        }
        self
    }
    /// Requests a report of local and distributed commit stages.
    pub fn with_receipt(self) -> ReceiptInvalidationRequest<'a, &'static str, V> {
        ReceiptInvalidationRequest { input: self.input }
    }
}
impl<'a, V: Clone + Send + Sync + 'static> ClearRequest<'a, V> {
    pub(super) fn new(cache: &'a Cache<V>, mode: ClearMode) -> Self {
        let kind = match mode {
            ClearMode::Expire => MarkerKind::ClearExpire,
            ClearMode::Remove => MarkerKind::ClearRemove,
        };
        Self {
            input: Input::new(
                cache,
                Target::Markers {
                    kinds: Ok(vec![kind]),
                    operation: CacheOperation::Clear,
                },
            ),
        }
    }
    /// Requests a report of local and distributed commit stages.
    pub fn with_receipt(self) -> ReceiptInvalidationRequest<'a, &'static str, V> {
        ReceiptInvalidationRequest { input: self.input }
    }
}

macro_rules! settings {
    ($request:ident $(, $key:ident)?) => {
        impl<$($key,)? V: Clone + Send + Sync + 'static> $request<'_, $($key,)? V> {
            /// Edits this operation's defaults, preserving untouched settings.
            pub fn options(mut self, update: impl FnOnce(EntryOptions) -> EntryOptions) -> Self {
                self.input = self.input.options(update);
                self
            }
            /// Links explicit caller cancellation when the request is polled.
            pub fn cancellation(mut self, token: FactoryCancellation) -> Self {
                self.input.token = Some(token);
                self
            }
        }
    };
}
settings!(RemoveRequest, K);
settings!(ExpireRequest, K);
settings!(TagInvalidationRequest);
settings!(ClearRequest);
settings!(ReceiptInvalidationRequest, K);

enum State<'a, K, V: Clone + Send + Sync + 'static> {
    Input(Input<'a, K, V>),
    Pending(ObservedExecution<MutationReceipt>),
    Finished,
}
struct Driver<'a, K, V: Clone + Send + Sync + 'static> {
    state: State<'a, K, V>,
}
// Only owned execution is structurally pinned; input keys are moved before polling it.
impl<K, V: Clone + Send + Sync + 'static> Unpin for Driver<'_, K, V> {}
impl<K: AsRef<str>, V: Clone + Send + Sync + 'static> Future for Driver<'_, K, V> {
    type Output = Result<MutationReceipt>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if matches!(this.state, State::Input(_)) {
            let State::Input(input) = std::mem::replace(&mut this.state, State::Finished) else {
                unreachable!("input state was matched")
            };
            let options = input.options.map(|options| *options);
            let execution = match input.target {
                Target::Key { key, mutation } => {
                    input
                        .cache
                        .begin_key_mutation(key.as_ref(), options, mutation, input.token)
                }
                Target::Markers { kinds, operation } => match kinds {
                    Ok(kinds) => input
                        .cache
                        .begin_markers(kinds, options, operation, input.token),
                    Err(error) => return Poll::Ready(Err(error.into())),
                },
            };
            this.state = State::Pending(execution);
        }
        let State::Pending(work) = &mut this.state else {
            panic!("completed cache invalidation was polled again")
        };
        let result = Pin::new(work).poll(cx);
        if result.is_ready() {
            this.state = State::Finished;
        }
        result
    }
}
/// Execution future of an ordinary invalidation.
pub struct InvalidationFuture<'a, K, V: Clone + Send + Sync + 'static> {
    work: Driver<'a, K, V>,
}
/// Execution future of an invalidation with commit evidence.
pub struct ReceiptInvalidationFuture<'a, K, V: Clone + Send + Sync + 'static> {
    work: Driver<'a, K, V>,
}
macro_rules! execute_unit {
    ($request:ident $(, $key:ident)?) => {
        impl<'a, $($key: AsRef<str>,)? V: Clone + Send + Sync + 'static> IntoFuture
            for $request<'a, $($key,)? V> {
            type Output = Result<()>;
            type IntoFuture = InvalidationFuture<'a, $($key,)? V>;
            fn into_future(self) -> Self::IntoFuture {
                InvalidationFuture { work: Driver { state: State::Input(self.input) } }
            }
        }
    };
}
execute_unit!(RemoveRequest, K);
execute_unit!(ExpireRequest, K);
impl<'a, V: Clone + Send + Sync + 'static> IntoFuture for TagInvalidationRequest<'a, V> {
    type Output = Result<()>;
    type IntoFuture = InvalidationFuture<'a, &'static str, V>;
    fn into_future(self) -> Self::IntoFuture {
        InvalidationFuture {
            work: Driver {
                state: State::Input(self.input),
            },
        }
    }
}
impl<'a, V: Clone + Send + Sync + 'static> IntoFuture for ClearRequest<'a, V> {
    type Output = Result<()>;
    type IntoFuture = InvalidationFuture<'a, &'static str, V>;
    fn into_future(self) -> Self::IntoFuture {
        InvalidationFuture {
            work: Driver {
                state: State::Input(self.input),
            },
        }
    }
}
impl<'a, K: AsRef<str>, V: Clone + Send + Sync + 'static> IntoFuture
    for ReceiptInvalidationRequest<'a, K, V>
{
    type Output = Result<MutationReceipt>;
    type IntoFuture = ReceiptInvalidationFuture<'a, K, V>;
    fn into_future(self) -> Self::IntoFuture {
        ReceiptInvalidationFuture {
            work: Driver {
                state: State::Input(self.input),
            },
        }
    }
}
impl<K: AsRef<str>, V: Clone + Send + Sync + 'static> Future for InvalidationFuture<'_, K, V> {
    type Output = Result<()>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().work)
            .poll(cx)
            .map(|result| result.map(drop))
    }
}
impl<K: AsRef<str>, V: Clone + Send + Sync + 'static> Future
    for ReceiptInvalidationFuture<'_, K, V>
{
    type Output = Result<MutationReceipt>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().work).poll(cx)
    }
}
