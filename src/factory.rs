//! The factory execution context and its product.
//!
//! When `get_or_set` must produce a value, it hands the factory a
//! [`FactoryContext`]. Through it the factory can:
//!
//! * read the previously-cached **stale value** and its `ETag`/`LastModified`
//!   (conditional refresh);
//! * **adapt** the entry options for the value it is about to produce
//!   (adaptive caching) via [`FactoryContext::options_mut`];
//! * signal one of three outcomes: a new value ([`FactoryContext::value`] /
//!   [`FactoryContext::modified`]), "nothing changed, reuse the stale value"
//!   ([`FactoryContext::not_modified`]), or failure
//!   ([`FactoryContext::fail`]).
//!
//! The factory returns `Result<FactoryProduct<V>, FactoryError>`; an `Err`
//! drives the fail-safe path.

use crate::error::FactoryError;
use crate::options::EntryOptions;
use crate::tags::Tag;
use crate::time::Timestamp;

/// The value (and metadata) a factory produced, ready to be cached.
///
/// Construct it through the [`FactoryContext`] methods rather than directly, so
/// the (possibly adapted) options and tags are carried along correctly.
#[derive(Debug)]
pub struct FactoryProduct<V> {
    pub(crate) value: V,
    pub(crate) options: EntryOptions,
    pub(crate) etag: Option<String>,
    pub(crate) last_modified: Option<Timestamp>,
    pub(crate) tags: Box<[Tag]>,
    /// `true` when this product is the *reused stale value* (a `NotModified`
    /// conditional-refresh result) rather than a freshly-produced value.
    pub(crate) reused_stale: bool,
}

impl<V> FactoryProduct<V> {
    /// Borrows the produced value.
    #[must_use]
    pub fn value(&self) -> &V {
        &self.value
    }
}

/// Context passed to a factory, carrying stale-value information and the mutable
/// options for the value being produced.
#[derive(Debug)]
pub struct FactoryContext<V> {
    key: std::sync::Arc<str>,
    options: EntryOptions,
    call_tags: Box<[Tag]>,
    adaptive_tags: Option<Box<[Tag]>>,
    stale_value: Option<V>,
    stale_etag: Option<String>,
    stale_last_modified: Option<Timestamp>,
    stale_tags: Box<[Tag]>,
}

impl<V> FactoryContext<V> {
    pub(crate) fn new(
        key: std::sync::Arc<str>,
        options: EntryOptions,
        call_tags: Box<[Tag]>,
        stale: Option<StaleInfo<V>>,
    ) -> Self {
        let (stale_value, stale_etag, stale_last_modified, stale_tags) = match stale {
            Some(s) => (Some(s.value), s.etag, s.last_modified, s.tags),
            None => (None, None, None, Box::from([])),
        };
        Self {
            key,
            options,
            call_tags,
            adaptive_tags: None,
            stale_value,
            stale_etag,
            stale_last_modified,
            stale_tags,
        }
    }

    /// The (prefixed) cache key being produced.
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    /// The options for the value being produced. Mutate these to **adapt** the
    /// caching of this specific value (e.g. shorten the duration for an empty
    /// result). This is *adaptive caching*.
    #[must_use]
    pub fn options_mut(&mut self) -> &mut EntryOptions {
        &mut self.options
    }

    /// The options for the value being produced.
    #[must_use]
    pub fn options(&self) -> &EntryOptions {
        &self.options
    }

    /// Adapts the options for the value being produced using the chainable
    /// builder methods — the ergonomic way to do *adaptive caching*:
    ///
    /// ```ignore
    /// ctx.adapt(|o| o.with_duration(Duration::from_secs(5)));
    /// ```
    pub fn adapt<F>(&mut self, f: F)
    where
        F: FnOnce(EntryOptions) -> EntryOptions,
    {
        let current = self.options.clone();
        self.options = f(current);
    }

    /// `true` if a previously-cached (now stale) value is available.
    #[must_use]
    pub fn has_stale_value(&self) -> bool {
        self.stale_value.is_some()
    }

    /// The previously-cached (now stale) value, if any.
    #[must_use]
    pub fn stale_value(&self) -> Option<&V> {
        self.stale_value.as_ref()
    }

    /// The `ETag` of the stale value, for issuing a conditional request.
    #[must_use]
    pub fn stale_etag(&self) -> Option<&str> {
        self.stale_etag.as_deref()
    }

    /// The `LastModified` of the stale value, for issuing a conditional request.
    #[must_use]
    pub fn stale_last_modified(&self) -> Option<Timestamp> {
        self.stale_last_modified
    }

    /// Sets the tags for the value being produced (adaptive tagging). Overrides
    /// the tags passed to the `get_or_set` call.
    pub fn set_tags<I, S>(&mut self, tags: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.adaptive_tags = Some(crate::tags::collect_tags(tags));
    }

    fn effective_tags(&self) -> Box<[Tag]> {
        self.adaptive_tags
            .clone()
            .unwrap_or_else(|| self.call_tags.clone())
    }

    /// Produces a new value, cached with the current (possibly adapted) options.
    #[must_use]
    pub fn value(self, value: V) -> FactoryProduct<V> {
        let tags = self.effective_tags();
        FactoryProduct {
            value,
            options: self.options,
            etag: None,
            last_modified: None,
            tags,
            reused_stale: false,
        }
    }

    /// Begins producing a *modified* value, allowing an `ETag`/`LastModified`
    /// (and tags) to be attached for future conditional refreshes.
    pub fn modified(self, value: V) -> ModifiedBuilder<V> {
        ModifiedBuilder {
            ctx: self,
            value,
            etag: None,
            last_modified: None,
            tags: None,
        }
    }

    /// Signals a factory failure, returning a [`FactoryError`] to return as
    /// `Err`. Triggers the fail-safe path.
    #[must_use]
    pub fn fail(&self, message: impl Into<String>) -> FactoryError {
        FactoryError::new(message)
    }
}

impl<V: Clone> FactoryContext<V> {
    /// Signals that the resource has **not changed** (e.g. an HTTP `304`): the
    /// stale value is reused as the fresh result and its expiration is bumped
    /// using the current options. Fails if there is no stale value to reuse.
    pub fn not_modified(self) -> Result<FactoryProduct<V>, FactoryError> {
        match &self.stale_value {
            Some(value) => Ok(FactoryProduct {
                value: value.clone(),
                options: self.options.clone(),
                etag: self.stale_etag.clone(),
                last_modified: self.stale_last_modified,
                tags: self.stale_tags.clone(),
                reused_stale: true,
            }),
            None => Err(FactoryError::new(
                "not_modified() was called but no stale value is available to reuse",
            )),
        }
    }
}

/// Carries the stale value and its conditional-refresh metadata into a
/// [`FactoryContext`].
pub(crate) struct StaleInfo<V> {
    pub(crate) value: V,
    pub(crate) etag: Option<String>,
    pub(crate) last_modified: Option<Timestamp>,
    pub(crate) tags: Box<[Tag]>,
}

/// Builder for a modified factory result with conditional-refresh metadata.
#[derive(Debug)]
#[must_use = "call `.done()` to produce the FactoryProduct"]
pub struct ModifiedBuilder<V> {
    ctx: FactoryContext<V>,
    value: V,
    etag: Option<String>,
    last_modified: Option<Timestamp>,
    tags: Option<Box<[Tag]>>,
}

impl<V> ModifiedBuilder<V> {
    /// Attaches an `ETag` to the produced value.
    pub fn etag(mut self, etag: impl Into<String>) -> Self {
        self.etag = Some(etag.into());
        self
    }

    /// Attaches a `LastModified` timestamp to the produced value.
    pub fn last_modified(mut self, last_modified: Timestamp) -> Self {
        self.last_modified = Some(last_modified);
        self
    }

    /// Sets the tags for the produced value (overriding call/adaptive tags).
    pub fn tags<I, S>(mut self, tags: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.tags = Some(crate::tags::collect_tags(tags));
        self
    }

    /// Finishes building the product.
    #[must_use]
    pub fn done(self) -> FactoryProduct<V> {
        let tags = self.tags.unwrap_or_else(|| self.ctx.effective_tags());
        FactoryProduct {
            value: self.value,
            options: self.ctx.options,
            etag: self.etag,
            last_modified: self.last_modified,
            tags,
            reused_stale: false,
        }
    }
}
