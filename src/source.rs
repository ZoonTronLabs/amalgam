//! Factory and supplied-value inputs to the same `get_or_set` operation.
//!
//! `factory` supplies the context type to Rust's closure inference and works
//! with asynchronous and native callbacks. The helper returns the callback
//! unchanged. `value` uses the supplied-value contract without factory timeouts,
//! success events or eager refresh.
use crate::FactoryContext;

mod private {
    pub trait NativeSealed<V>: crate::cache::blocking::origin_request::NativeOrigin<V> {}
    impl<V, T: crate::cache::blocking::origin_request::NativeOrigin<V>> NativeSealed<V> for T {}
    pub trait Sealed<V>: crate::cache::origin::CacheOrigin<V> {}
    impl<V, T: crate::cache::origin::CacheOrigin<V>> Sealed<V> for T {}
}

/// An asynchronous factory or a supplied value for `Cache::get_or_set`.
///
/// This input family is sealed. Factories return ordinary `Result<V, E>` with
/// thread-safe source errors; supplied values use `value`.
pub trait Source<V>: private::Sealed<V> {}
impl<V, T: private::Sealed<V>> Source<V> for T {}

/// A supplied value; its factory-only policies are disabled by its input type.
#[derive(Debug, Clone)]
pub struct Value<V>(pub(crate) V);

/// Supplies a value to `get_or_set` without invoking a user factory.
pub fn value<V>(value: V) -> Value<V> {
    Value(value)
}

/// Gives an async or native callback its inferred `FactoryContext<V>` type.
/// The callback and its ordinary result/error are retained unchanged.
///
/// ```
/// use amalgam::{Cache, source};
/// # async fn example() -> amalgam::Result<()> {
/// let cache = Cache::<u64>::new();
/// let value = cache.get_or_set("factory", source::factory(|mut ctx| async move {
///     ctx.try_set_tags(["profiles"]).map_err(amalgam::FactoryError::from_source)?;
///     Ok::<_, amalgam::FactoryError>(7)
/// })).await?;
/// assert_eq!(value, 7);
/// assert_eq!(cache.get_or_set("supplied", source::value(9)).await?, 9);
/// cache.shutdown().await?;
/// # Ok(()) }
/// ```
pub fn factory<V, F, R>(callback: F) -> F
where
    F: FnOnce(FactoryContext<V>) -> R,
{
    callback
}

/// A native factory or supplied value for `BlockingCache::get_or_set`.
/// This sealed input preserves direct caller-thread execution for ordinary factories.
pub trait BlockingSource<V>: private::NativeSealed<V> {}
impl<V, T: private::NativeSealed<V>> BlockingSource<V> for T {}
