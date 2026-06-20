//! Error types.
//!
//! Following the project guideline that libraries expose typed errors via
//! `thiserror`, every fallible boundary returns [`Error`]. Business outcomes
//! that are *not* failures (a cache miss, a factory choosing to reuse a stale
//! value) are modelled in the return *type*, never as errors.

use std::time::Duration;

/// The crate-wide result alias.
pub type Result<T> = std::result::Result<T, Error>;

/// An error surfaced from a cache operation.
///
/// Note what is deliberately *absent*: a "cache miss" is not an error (it is a
/// `None`/`MaybeValue::none`), and a factory that fails while fail-safe rescues
/// a stale value never produces an `Error` at all.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The factory failed and no stale value or fail-safe default was available
    /// to fall back to. Carries the message reported by the factory.
    #[error("factory failed: {message}")]
    Factory {
        /// The failure message reported by the factory.
        message: String,
    },

    /// The factory exceeded its hard timeout and no fallback value existed.
    #[error("factory timed out after {elapsed:?}")]
    FactoryTimeout {
        /// How long the factory was allowed to run before timing out.
        elapsed: Duration,
    },

    /// Acquiring the per-key single-flight lock exceeded its timeout and no
    /// fallback value existed.
    #[error("lock acquisition timed out after {elapsed:?}")]
    LockTimeout {
        /// How long the caller waited for the lock.
        elapsed: Duration,
    },

    /// A value could not be serialized for the distributed (L2) cache.
    #[error("serialization failed: {0}")]
    Serialization(String),

    /// A value could not be deserialized from the distributed (L2) cache.
    #[error("deserialization failed: {0}")]
    Deserialization(String),

    /// The distributed (L2) cache backend returned an error.
    #[error("distributed cache error: {0}")]
    Distributed(String),

    /// The backplane backend returned an error.
    #[error("backplane error: {0}")]
    Backplane(String),
}

/// The error a user-supplied factory returns to signal failure.
///
/// Returning this (or calling [`FactoryContext::fail`](crate::FactoryContext::fail))
/// triggers the fail-safe path: a stale value or the `fail_safe_default` is
/// served if available, otherwise the failure is surfaced as [`Error::Factory`].
///
/// It can wrap an arbitrary source error so the original cause is preserved in
/// the error chain.
#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct FactoryError {
    message: String,
    #[source]
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl FactoryError {
    /// Creates a factory error with a human-readable message.
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: into_nonblank(message.into()),
            source: None,
        }
    }

    /// Creates a factory error from an arbitrary source error, preserving it in
    /// the error chain.
    #[must_use]
    pub fn from_source<E>(source: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self {
            message: source.to_string(),
            source: Some(Box::new(source)),
        }
    }

    /// The failure message.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

fn into_nonblank(message: String) -> String {
    if message.trim().is_empty() {
        // Mirrors FusionCache's default factory-failure message.
        "an error occurred while running the factory".to_owned()
    } else {
        message
    }
}

impl From<FactoryError> for Error {
    fn from(err: FactoryError) -> Self {
        Error::Factory {
            message: err.message,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blank_factory_message_is_defaulted() {
        assert_eq!(
            FactoryError::new("   ").message(),
            "an error occurred while running the factory"
        );
    }

    #[test]
    fn factory_error_preserves_source() {
        let io = std::io::Error::other("boom");
        let err = FactoryError::from_source(io);
        assert_eq!(err.message(), "boom");
        assert!(std::error::Error::source(&err).is_some());
    }
}
