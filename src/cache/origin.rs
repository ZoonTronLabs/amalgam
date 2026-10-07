//! Statically dispatched factory and supplied-value origins.
//! The distinct capture types avoid allocating or reserving a V-sized constant
//! alternative in every user-factory future.
use crate::{FactoryContext, FactoryError, FactoryProduct, advanced::CacheValue};

/// A completed origin distinguishes actual factory work from supplied values
/// and eager L2 reuse. The coordinator selects foreground/background once.
pub(super) enum OriginCompletion<V> {
    Factory(CacheValue<V>),
    Constant(CacheValue<V>),
    Distributed(CacheValue<V>),
}
use std::future::{Future, ready};

#[derive(Clone, Copy)]
#[doc(hidden)]
pub enum OriginKind {
    Factory,
    Constant,
}
/// Sealed engine invocation contract; not an extensible provider interface.
#[doc(hidden)]
pub trait CacheOrigin<V>: Send + 'static {
    const KIND: OriginKind;
    fn invoke(
        self,
        context: FactoryContext<V>,
    ) -> impl Future<Output = Result<FactoryProduct<V>, FactoryError>> + Send + 'static;
}
pub(super) struct FactoryOrigin<F>(F);
impl<F> FactoryOrigin<F> {
    pub(super) fn new(factory: F) -> Self {
        Self(factory)
    }
}
impl<V, F, Fut, E> CacheOrigin<V> for F
where
    V: Clone + Send + Sync + 'static,
    F: FnOnce(FactoryContext<V>) -> Fut + Send + 'static,
    Fut: Future<Output = Result<V, E>> + Send + 'static,
    E: std::error::Error + Send + Sync + 'static,
{
    const KIND: OriginKind = OriginKind::Factory;
    fn invoke(
        self,
        context: FactoryContext<V>,
    ) -> impl Future<Output = Result<FactoryProduct<V>, FactoryError>> + Send + 'static {
        let completion = context.completion();
        let work = self(context);
        async move {
            work.await
                .map(|value| completion.complete(value))
                .map_err(FactoryError::from_boundary)
        }
    }
}
impl<V, F: CacheOrigin<V>> CacheOrigin<V> for FactoryOrigin<F> {
    const KIND: OriginKind = F::KIND;
    fn invoke(
        self,
        context: FactoryContext<V>,
    ) -> impl Future<Output = Result<FactoryProduct<V>, FactoryError>> + Send + 'static {
        self.0.invoke(context)
    }
}
impl<V: Clone + Send + Sync + 'static> CacheOrigin<V> for crate::source::Value<V> {
    const KIND: OriginKind = OriginKind::Constant;
    fn invoke(
        self,
        context: FactoryContext<V>,
    ) -> impl Future<Output = Result<FactoryProduct<V>, FactoryError>> + Send + 'static {
        ready(Ok(context.constant(self.0)))
    }
}
/// The observer scope and an optional explicit signal have distinct lifetimes.
pub(super) struct OriginCaller {
    pub(super) operation: crate::FactoryCancellation,
    pub(super) explicit: Option<crate::FactoryCancellation>,
}
