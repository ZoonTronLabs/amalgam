//! The [`MaybeValue`] type: "a value, or explicitly no value".

/// A value that may or may not be present.
///
/// This is the Rust counterpart of FusionCache's `MaybeValue<T>`. It is a thin,
/// purpose-named wrapper around [`Option`] so the public API reads the same as
/// FusionCache (`has_value`, `value_or_default`, `from_value`, …) while still
/// interoperating freely with idiomatic `Option` via [`From`].
///
/// It appears in two places:
/// * the return type of [`Cache::try_get`](crate::Cache::try_get) — a miss is
///   [`MaybeValue::none`], distinct from "present but `None`/null";
/// * the `fail_safe_default` argument of
///   [`Cache::get_or_set`](crate::Cache::get_or_set) — a last-resort value used
///   only when the factory fails and no stale entry exists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaybeValue<V>(Option<V>);

impl<V> MaybeValue<V> {
    /// A present value.
    #[must_use]
    pub fn from_value(value: V) -> Self {
        Self(Some(value))
    }

    /// The absence of a value.
    #[must_use]
    pub fn none() -> Self {
        Self(None)
    }

    /// `true` if a value is present.
    #[must_use]
    pub fn has_value(&self) -> bool {
        self.0.is_some()
    }

    /// Borrows the contained value, if any.
    #[must_use]
    pub fn value(&self) -> Option<&V> {
        self.0.as_ref()
    }

    /// Consumes the wrapper, returning the contained value, if any.
    #[must_use]
    pub fn into_value(self) -> Option<V> {
        self.0
    }

    /// Returns the contained value or `default` when absent.
    #[must_use]
    pub fn value_or(self, default: V) -> V {
        self.0.unwrap_or(default)
    }

    /// Maps the contained value, preserving absence.
    #[must_use]
    pub fn map<U, F: FnOnce(V) -> U>(self, f: F) -> MaybeValue<U> {
        MaybeValue(self.0.map(f))
    }
}

impl<V: Clone> MaybeValue<V> {
    /// Returns a clone of the contained value or `default` when absent.
    #[must_use]
    pub fn value_or_clone(&self, default: V) -> V {
        self.0.clone().unwrap_or(default)
    }
}

impl<V: Default> MaybeValue<V> {
    /// Returns the contained value or `V::default()` when absent.
    #[must_use]
    pub fn value_or_default(self) -> V {
        self.0.unwrap_or_default()
    }
}

impl<V> Default for MaybeValue<V> {
    /// The default is [`MaybeValue::none`], matching `default(MaybeValue<T>)`.
    fn default() -> Self {
        Self(None)
    }
}

impl<V> From<V> for MaybeValue<V> {
    fn from(value: V) -> Self {
        Self::from_value(value)
    }
}

impl<V> From<Option<V>> for MaybeValue<V> {
    fn from(option: Option<V>) -> Self {
        Self(option)
    }
}

impl<V> From<MaybeValue<V>> for Option<V> {
    fn from(maybe: MaybeValue<V>) -> Self {
        maybe.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn none_has_no_value() {
        let m: MaybeValue<i32> = MaybeValue::none();
        assert!(!m.has_value());
        assert_eq!(m.value(), None);
        assert_eq!(MaybeValue::<i32>::default(), MaybeValue::none());
    }

    #[test]
    fn from_value_round_trips() {
        let m = MaybeValue::from_value(42);
        assert!(m.has_value());
        assert_eq!(m.value(), Some(&42));
        assert_eq!(Option::from(m), Some(42));
    }

    #[test]
    fn value_or_default_falls_back() {
        assert_eq!(MaybeValue::<i32>::none().value_or_default(), 0);
        assert_eq!(MaybeValue::from_value(7).value_or_default(), 7);
    }
}
