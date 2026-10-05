//! Statically dispatched factory and supplied-value origins.
//! The distinct capture types avoid allocating or reserving a V-sized constant
//! alternative in every user-factory future.
use crate::{FactoryContext, FactoryError, FactoryProduct};
use std::future::{Future, Ready, ready};

#[derive(Clone, Copy)]
pub(super) enum OriginKind {
    Factory,
    Constant,
}
pub(super) trait CacheOrigin<V>: Send + 'static {
    type Output: Future<Output = Result<FactoryProduct<V>, FactoryError>> + Send + 'static;
    const KIND: OriginKind;
    fn invoke(self, context: FactoryContext<V>) -> Self::Output;
}
pub(super) struct FactoryOrigin<F>(F);
impl<F> FactoryOrigin<F> {
    pub(super) fn new(factory: F) -> Self {
        Self(factory)
    }
}
impl<V, F, Fut> CacheOrigin<V> for FactoryOrigin<F>
where
    V: Clone + Send + Sync + 'static,
    F: FnOnce(FactoryContext<V>) -> Fut + Send + 'static,
    Fut: Future<Output = Result<FactoryProduct<V>, FactoryError>> + Send + 'static,
{
    type Output = Fut;
    const KIND: OriginKind = OriginKind::Factory;
    fn invoke(self, context: FactoryContext<V>) -> Fut {
        (self.0)(context)
    }
}
pub(super) struct ConstantOrigin<V>(V);
impl<V> ConstantOrigin<V> {
    pub(super) fn new(value: V) -> Self {
        Self(value)
    }
}
impl<V: Clone + Send + Sync + 'static> CacheOrigin<V> for ConstantOrigin<V> {
    type Output = Ready<Result<FactoryProduct<V>, FactoryError>>;
    const KIND: OriginKind = OriginKind::Constant;
    fn invoke(self, context: FactoryContext<V>) -> Self::Output {
        ready(Ok(context.constant(self.0)))
    }
}
