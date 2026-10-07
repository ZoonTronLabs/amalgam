//! Native origin inputs retain the caller-thread factory path.
use super::{BlockingCache, BlockingCacheValue};
use crate::cache::inline_cold::Start;
use crate::cache::origin::{CacheOrigin, OriginKind};
use crate::{
    CacheValue, EntryOptions, FactoryCancellation, FactoryContext, FactoryError, FactoryProduct,
    MaybeValue, Result, Tag,
};
use std::future::{Future, ready};

#[doc(hidden)]
pub trait NativeOrigin<V>: Send + 'static {
    const KIND: OriginKind;
    type AsyncOrigin: CacheOrigin<V>;
    fn invoke(self, context: FactoryContext<V>) -> std::result::Result<V, FactoryError>;
    fn into_async(self) -> Self::AsyncOrigin;
}
#[doc(hidden)]
pub struct NativeCallback<F>(F);
impl<V, F, E> NativeOrigin<V> for F
where
    V: Clone + Send + Sync + 'static,
    F: FnOnce(FactoryContext<V>) -> std::result::Result<V, E> + Send + 'static,
    E: std::error::Error + Send + Sync + 'static,
{
    const KIND: OriginKind = OriginKind::Factory;
    type AsyncOrigin = NativeCallback<F>;
    fn invoke(self, context: FactoryContext<V>) -> std::result::Result<V, FactoryError> {
        self(context).map_err(FactoryError::from_boundary)
    }
    fn into_async(self) -> Self::AsyncOrigin {
        NativeCallback(self)
    }
}
impl<V: Clone + Send + Sync + 'static> NativeOrigin<V> for crate::source::Value<V> {
    const KIND: OriginKind = OriginKind::Constant;
    type AsyncOrigin = Self;
    fn invoke(self, _: FactoryContext<V>) -> std::result::Result<V, FactoryError> {
        Ok(self.0)
    }
    fn into_async(self) -> Self::AsyncOrigin {
        self
    }
}
impl<V, F, E> CacheOrigin<V> for NativeCallback<F>
where
    V: Clone + Send + Sync + 'static,
    F: FnOnce(FactoryContext<V>) -> std::result::Result<V, E> + Send + 'static,
    E: std::error::Error + Send + Sync + 'static,
{
    const KIND: OriginKind = OriginKind::Factory;
    fn invoke(
        self,
        context: FactoryContext<V>,
    ) -> impl Future<Output = std::result::Result<FactoryProduct<V>, FactoryError>> + Send + 'static
    {
        let completion = context.completion();
        ready(NativeOrigin::invoke(self.0, context).map(|value| completion.complete(value)))
    }
}
struct Input<K, S, V> {
    key: K,
    source: S,
    options: Option<Box<EntryOptions>>,
    tags: std::result::Result<Box<[Tag]>, crate::TagError>,
    fallback: MaybeValue<V>,
    token: Option<FactoryCancellation>,
}
/// A lazy native retrieval. Configure it before explicit `execute()`.
#[must_use = "a native origin request executes only through execute()"]
pub struct BlockingGetOrSetRequest<'a, K, S, V: Clone + Send + Sync + 'static> {
    cache: &'a BlockingCache<V>,
    input: Input<K, S, V>,
}
/// A lazy native retrieval returning its actual commit evidence.
#[must_use = "a native origin request executes only through execute()"]
pub struct BlockingReceiptGetOrSetRequest<'a, K, S, V: Clone + Send + Sync + 'static> {
    cache: &'a BlockingCache<V>,
    input: Input<K, S, V>,
}
impl<'a, K, S, V: Clone + Send + Sync + 'static> BlockingGetOrSetRequest<'a, K, S, V> {
    pub(super) fn new(cache: &'a BlockingCache<V>, key: K, source: S) -> Self {
        Self {
            cache,
            input: Input {
                key,
                source,
                options: None,
                tags: Ok(Box::from([])),
                fallback: MaybeValue::none(),
                token: None,
            },
        }
    }
    /// Requests the value and actual synchronous commit evidence.
    pub fn with_receipt(self) -> BlockingReceiptGetOrSetRequest<'a, K, S, V> {
        BlockingReceiptGetOrSetRequest {
            cache: self.cache,
            input: self.input,
        }
    }
}
macro_rules! settings {
    ($request:ident) => {
        impl<K, S, V: Clone + Send + Sync + 'static> $request<'_, K, S, V> {
            /// Edits a copy of entry defaults, retaining untouched settings.
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
            /// Validates raw tags before any source or cache work.
            pub fn tags<I, T>(mut self, tags: I) -> Self
            where
                I: IntoIterator<Item = T>,
                T: AsRef<str>,
            {
                self.input.tags = crate::tags::try_collect_tags(tags);
                self
            }
            /// Configures the optional fail-safe value for this request.
            pub fn fail_safe_default(mut self, value: Option<V>) -> Self {
                self.input.fallback = value.map_or_else(MaybeValue::none, MaybeValue::from_value);
                self
            }
            /// Links explicit caller cancellation.
            pub fn cancellation(mut self, token: FactoryCancellation) -> Self {
                self.input.token = Some(token);
                self
            }
        }
    };
}
settings!(BlockingGetOrSetRequest);
settings!(BlockingReceiptGetOrSetRequest);
impl<K: AsRef<str>, S: crate::source::BlockingSource<V>, V: Clone + Send + Sync + 'static>
    BlockingGetOrSetRequest<'_, K, S, V>
{
    /// Runs the caller-thread retrieval and returns its cached or supplied value.
    pub fn execute(self) -> Result<V> {
        complete(self.cache, self.input).map(|value| value.value)
    }
}
impl<K: AsRef<str>, S: crate::source::BlockingSource<V>, V: Clone + Send + Sync + 'static>
    BlockingReceiptGetOrSetRequest<'_, K, S, V>
{
    /// Runs retrieval and preserves scheduled completion on the same executor.
    pub fn execute(self) -> Result<BlockingCacheValue<V>> {
        complete(self.cache, self.input).map(|value| self.cache.wrap_value(value))
    }
}
fn complete<
    K: AsRef<str>,
    S: crate::source::BlockingSource<V>,
    V: Clone + Send + Sync + 'static,
>(
    cache: &BlockingCache<V>,
    input: Input<K, S, V>,
) -> Result<CacheValue<V>> {
    let Input {
        key,
        source,
        options,
        tags,
        fallback,
        token,
    } = input;
    match tags {
        Ok(tags) if matches!(S::KIND, OriginKind::Factory) => cache.retrieve(
            key.as_ref(),
            move |context| NativeOrigin::invoke(source, context),
            options.map(|options| *options),
            tags,
            fallback,
            token,
        ),
        tags => cache.runtime.run(async {
            match cache.cache.begin_origin_request(
                key.as_ref(),
                source.into_async(),
                options,
                tags,
                fallback,
                token,
            ) {
                Start::Ready(result) => result,
                Start::Pending(work) => work.await,
            }
        }),
    }
}
