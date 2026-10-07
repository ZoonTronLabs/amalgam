//! Copy capabilities are selected once, before invoking any value strategy.
use super::ValueCloner;
use crate::error::{ConfigError, Result};
use crate::options::EntryOptions;
use std::sync::Arc;

enum Route<'a, V> {
    Ordinary,
    Strategy(&'a dyn ValueCloner<V>),
}
/// Opaque proof that the chosen value-copy policy was validated.
pub(crate) struct ValueCopy<'a, V> {
    route: Route<'a, V>,
}
impl<'a, V: Clone> ValueCopy<'a, V> {
    pub(crate) fn validated(
        options: &EntryOptions,
        cloner: Option<&'a dyn ValueCloner<V>>,
    ) -> Result<Self> {
        options.validate_with_cloner(cloner)?;
        let route = if options.enable_auto_clone() {
            Route::Strategy(cloner.ok_or(ConfigError::AutoCloneWithoutCloner)?)
        } else {
            Route::Ordinary
        };
        Ok(Self { route })
    }
    pub(crate) fn copy(&self, value: &V) -> Result<V> {
        match self.route {
            Route::Ordinary => Ok(value.clone()),
            Route::Strategy(cloner) => cloner.clone_value(value).map_err(Into::into),
        }
    }
}
enum Defaults<V> {
    Ordinary,
    Strategy(Arc<dyn ValueCloner<V>>),
}
pub(crate) struct DefaultValueCopy<V> {
    route: Defaults<V>,
}
impl<V: Clone> DefaultValueCopy<V> {
    pub(crate) fn validated(
        options: &EntryOptions,
        cloner: Option<&Arc<dyn ValueCloner<V>>>,
    ) -> Result<Self> {
        options.validate_with_cloner(cloner.map(Arc::as_ref))?;
        let route = if options.enable_auto_clone() {
            Defaults::Strategy(Arc::clone(
                cloner.ok_or(ConfigError::AutoCloneWithoutCloner)?,
            ))
        } else {
            Defaults::Ordinary
        };
        Ok(Self { route })
    }
    pub(crate) fn borrowed(&self) -> ValueCopy<'_, V> {
        ValueCopy {
            route: match &self.route {
                Defaults::Ordinary => Route::Ordinary,
                Defaults::Strategy(cloner) => Route::Strategy(cloner.as_ref()),
            },
        }
    }
}
