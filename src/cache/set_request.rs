//! Lazy canonical writes; completion reports are an explicit input choice.
use super::mutation_request::{Input, MutationRequest};
use super::{Cache, EntryOptions, FactoryCancellation, MutationReceipt, Result};
use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::task::{Context, Poll};

/// A lazy value write returning `Result<()>` when awaited.
#[must_use = "a cache request only executes when awaited"]
pub struct SetRequest<'a, K, V: Clone + Send + Sync + 'static> {
    cache: &'a Cache<V>,
    input: Input<K, V>,
}
/// A lazy write returning its actual commit receipt when awaited.
#[must_use = "a cache request only executes when awaited"]
pub struct ReceiptSetRequest<'a, K, V: Clone + Send + Sync + 'static> {
    cache: &'a Cache<V>,
    input: Input<K, V>,
}
impl<'a, K, V: Clone + Send + Sync + 'static> SetRequest<'a, K, V> {
    pub(super) fn new(cache: &'a Cache<V>, key: K, value: V) -> Self {
        Self {
            cache,
            input: Input {
                key,
                value,
                options: None,
                tags: Ok(Box::from([])),
                token: None,
            },
        }
    }
    /// Requests storage/publication stage evidence and scheduled completion.
    pub fn with_receipt(self) -> ReceiptSetRequest<'a, K, V> {
        ReceiptSetRequest {
            cache: self.cache,
            input: self.input,
        }
    }
}
macro_rules! settings {
    ($request:ident) => {
        impl<K, V: Clone + Send + Sync + 'static> $request<'_, K, V> {
            /// Edits a copy of this cache's defaults, preserving untouched settings.
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
            /// Assigns raw string tags; a blank tag rejects the awaited write.
            pub fn tags<I, S>(mut self, tags: I) -> Self
            where
                I: IntoIterator<Item = S>,
                S: AsRef<str>,
            {
                self.input.tags = crate::tags::try_collect_tags(tags);
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
settings!(SetRequest);
settings!(ReceiptSetRequest);

/// The execution future of an ordinary write. Input is not structurally pinned.
pub struct SetFuture<'a, K, V: Clone + Send + Sync + 'static> {
    work: MutationRequest<'a, K, V, ()>,
}
/// The execution future of a write that requested a receipt.
pub struct ReceiptSetFuture<'a, K, V: Clone + Send + Sync + 'static> {
    work: MutationRequest<'a, K, V>,
}
impl<'a, K: AsRef<str>, V: Clone + Send + Sync + 'static> IntoFuture for SetRequest<'a, K, V> {
    type Output = Result<()>;
    type IntoFuture = SetFuture<'a, K, V>;
    fn into_future(self) -> Self::IntoFuture {
        SetFuture {
            work: MutationRequest {
                cache: self.cache,
                state: super::mutation_request::State::Start(self.input),
            },
        }
    }
}
impl<'a, K: AsRef<str>, V: Clone + Send + Sync + 'static> IntoFuture
    for ReceiptSetRequest<'a, K, V>
{
    type Output = Result<MutationReceipt>;
    type IntoFuture = ReceiptSetFuture<'a, K, V>;
    fn into_future(self) -> Self::IntoFuture {
        ReceiptSetFuture {
            work: MutationRequest {
                cache: self.cache,
                state: super::mutation_request::State::Start(self.input),
            },
        }
    }
}
impl<K: AsRef<str>, V: Clone + Send + Sync + 'static> Future for SetFuture<'_, K, V> {
    type Output = Result<()>;
    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().work).poll(context)
    }
}
impl<K: AsRef<str>, V: Clone + Send + Sync + 'static> Future for ReceiptSetFuture<'_, K, V> {
    type Output = Result<MutationReceipt>;
    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().work).poll(context)
    }
}
