//! Origin factory context, validated tag requests and closed product variants.
use crate::error::FactoryError;
use crate::execution::FactoryCancellation;
use crate::options::EntryOptions;
use crate::tags::{Tag, TagError, try_collect_tags};
use crate::time::Timestamp;
use std::sync::Arc;

#[derive(Debug)]
enum TagRequest {
    Inherited,
    Valid(Box<[Tag]>),
    Rejected(TagError),
}
impl TagRequest {
    fn resolve(self, fallback: Box<[Tag]>) -> Result<Box<[Tag]>, TagError> {
        match self {
            Self::Inherited => Ok(fallback),
            Self::Valid(tags) => Ok(tags),
            Self::Rejected(error) => Err(error),
        }
    }
}
#[derive(Debug)]
enum FactoryOutput<V> {
    Modified {
        value: V,
        etag: Option<String>,
        last_modified: Option<Timestamp>,
        tags: Result<Box<[Tag]>, TagError>,
    },
    NotModified {
        stale: StaleInfo<V>,
        tags: Result<Box<[Tag]>, TagError>,
    },
}
/// A factory's modified or conditional result, constructed through its context.
#[derive(Debug)]
pub struct FactoryProduct<V> {
    output: FactoryOutput<V>,
    options: EntryOptions,
}
impl<V> FactoryProduct<V> {
    /// Borrows the produced value.
    pub fn value(&self) -> &V {
        match &self.output {
            FactoryOutput::Modified { value, .. } => value,
            FactoryOutput::NotModified { stale, .. } => &stale.value,
        }
    }
    pub(crate) fn into_payload(self) -> crate::Result<FactoryPayload<V>> {
        match self.output {
            FactoryOutput::Modified {
                value,
                etag,
                last_modified,
                tags,
            } => Ok(FactoryPayload {
                value,
                options: self.options,
                etag,
                last_modified,
                tags: tags?,
                origin: ProductOrigin::Modified,
            }),
            FactoryOutput::NotModified { stale, tags } => Ok(FactoryPayload {
                value: stale.value,
                options: self.options,
                etag: stale.etag,
                last_modified: stale.last_modified,
                tags: tags?,
                origin: ProductOrigin::NotModified,
            }),
        }
    }
}
pub(crate) enum ProductOrigin {
    Modified,
    NotModified,
}
pub(crate) struct FactoryPayload<V> {
    pub(crate) value: V,
    pub(crate) options: EntryOptions,
    pub(crate) etag: Option<String>,
    pub(crate) last_modified: Option<Timestamp>,
    pub(crate) tags: Box<[Tag]>,
    pub(crate) origin: ProductOrigin,
}
/// Origin context. Optional stale data forms one complete snapshot.
#[derive(Debug)]
pub struct FactoryContext<V> {
    key: Arc<str>,
    options: EntryOptions,
    call_tags: Box<[Tag]>,
    adaptive_tags: TagRequest,
    stale: Option<StaleInfo<V>>,
    cancellation: FactoryCancellation,
}
impl<V> FactoryContext<V> {
    pub(crate) fn with_cancellation(
        key: Arc<str>,
        options: EntryOptions,
        call_tags: Box<[Tag]>,
        stale: Option<StaleInfo<V>>,
        cancellation: FactoryCancellation,
    ) -> Self {
        Self {
            key,
            options,
            call_tags,
            adaptive_tags: TagRequest::Inherited,
            stale,
            cancellation,
        }
    }
    /// The prefixed data key.
    pub fn key(&self) -> &str {
        &self.key
    }
    /// Adapts produced-entry options; the cache validates them before effects.
    pub fn options_mut(&mut self) -> &mut EntryOptions {
        &mut self.options
    }
    /// Current options.
    pub fn options(&self) -> &EntryOptions {
        &self.options
    }
    /// Applies a chainable option transformation.
    pub fn adapt(&mut self, adapt: impl FnOnce(EntryOptions) -> EntryOptions) {
        self.options = adapt(self.options.clone());
    }
    /// Whether a stale snapshot exists.
    pub fn has_stale_value(&self) -> bool {
        self.stale.is_some()
    }
    /// Isolated stale value.
    pub fn stale_value(&self) -> Option<&V> {
        self.stale.as_ref().map(|stale| &stale.value)
    }
    /// Optional stale validator.
    pub fn stale_etag(&self) -> Option<&str> {
        self.stale.as_ref().and_then(|stale| stale.etag.as_deref())
    }
    /// Optional stale modification timestamp.
    pub fn stale_last_modified(&self) -> Option<Timestamp> {
        self.stale.as_ref().and_then(|stale| stale.last_modified)
    }
    /// Read-only origin execution cancellation.
    pub fn cancellation(&self) -> &FactoryCancellation {
        &self.cancellation
    }
    /// Replaces adaptive tags using already validated values.
    pub fn set_validated_tags(&mut self, tags: Box<[Tag]>) {
        self.adaptive_tags = TagRequest::Valid(tags);
    }
    /// Validates all raw tags, rejecting the entire boundary on invalid input.
    pub fn try_set_tags<I, S>(&mut self, tags: I) -> Result<(), TagError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.adaptive_tags = TagRequest::Valid(try_collect_tags(tags)?);
        Ok(())
    }
    /// Legacy adapter. A rejected request is diagnosed and its product is rejected
    /// by the fallible cache pipeline; invalid tags never silently disappear.
    pub fn set_tags<I, S>(&mut self, tags: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.adaptive_tags = match try_collect_tags(tags) {
            Ok(tags) => TagRequest::Valid(tags),
            Err(error) => {
                tracing::warn!(%error,"invalid legacy factory tags");
                TagRequest::Rejected(error)
            }
        };
    }
    /// Produces a modified value using current options/tags.
    pub fn value(self, value: V) -> FactoryProduct<V> {
        FactoryProduct {
            output: FactoryOutput::Modified {
                value,
                etag: None,
                last_modified: None,
                tags: self.adaptive_tags.resolve(self.call_tags),
            },
            options: self.options,
        }
    }
    /// Begins a modified result with conditional metadata.
    pub fn modified(self, value: V) -> ModifiedBuilder<V> {
        ModifiedBuilder {
            ctx: self,
            value,
            etag: None,
            last_modified: None,
            tags: TagRequest::Inherited,
        }
    }
    /// Creates an expected origin failure with a diagnostic message.
    pub fn fail(&self, message: impl Into<String>) -> FactoryError {
        FactoryError::new(message)
    }
    /// Reuses the complete isolated stale snapshot. Absence is typed rejection.
    pub fn not_modified(self) -> Result<FactoryProduct<V>, FactoryError> {
        match self.stale {
            Some(mut stale) => {
                let tags = self.adaptive_tags.resolve(std::mem::take(&mut stale.tags));
                Ok(FactoryProduct {
                    output: FactoryOutput::NotModified { stale, tags },
                    options: self.options,
                })
            }
            None => Err(FactoryError::new(
                "not_modified() requires a stale snapshot",
            )),
        }
    }
}
#[derive(Debug)]
pub(crate) struct StaleInfo<V> {
    pub(crate) value: V,
    pub(crate) etag: Option<String>,
    pub(crate) last_modified: Option<Timestamp>,
    pub(crate) tags: Box<[Tag]>,
}
/// Builder for modified-value validators and tags.
#[derive(Debug)]
#[must_use = "call done() to return a factory product"]
pub struct ModifiedBuilder<V> {
    ctx: FactoryContext<V>,
    value: V,
    etag: Option<String>,
    last_modified: Option<Timestamp>,
    tags: TagRequest,
}
impl<V> ModifiedBuilder<V> {
    /// Attaches an ETag.
    pub fn etag(mut self, etag: impl Into<String>) -> Self {
        self.etag = Some(etag.into());
        self
    }
    /// Attaches a modification timestamp.
    pub fn last_modified(mut self, at: Timestamp) -> Self {
        self.last_modified = Some(at);
        self
    }
    /// Attaches validated tags.
    pub fn validated_tags(mut self, tags: Box<[Tag]>) -> Self {
        self.tags = TagRequest::Valid(tags);
        self
    }
    /// Validates all raw tags before accepting the builder.
    pub fn try_tags<I, S>(mut self, tags: I) -> Result<Self, TagError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.tags = TagRequest::Valid(try_collect_tags(tags)?);
        Ok(self)
    }
    /// Diagnostic legacy raw-tag adapter. Invalid products fail at cache commit.
    pub fn tags<I, S>(mut self, tags: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.tags = match try_collect_tags(tags) {
            Ok(tags) => TagRequest::Valid(tags),
            Err(error) => {
                tracing::warn!(%error,"invalid legacy modified tags");
                TagRequest::Rejected(error)
            }
        };
        self
    }
    /// Produces the result with current adapted options.
    pub fn done(self) -> FactoryProduct<V> {
        let inherited = self.ctx.adaptive_tags.resolve(self.ctx.call_tags);
        let tags = match self.tags {
            TagRequest::Inherited => inherited,
            TagRequest::Valid(tags) => Ok(tags),
            TagRequest::Rejected(error) => Err(error),
        };
        FactoryProduct {
            output: FactoryOutput::Modified {
                value: self.value,
                etag: self.etag,
                last_modified: self.last_modified,
                tags,
            },
            options: self.ctx.options,
        }
    }
}
