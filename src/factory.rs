//! Origin factory context, validated tag requests and closed product variants.
use crate::error::FactoryError;
use crate::execution::FactoryCancellation;
use crate::options::EntryOptions;
use crate::tags::{Tag, TagError, try_collect_tags};
use crate::time::Timestamp;
use std::sync::Arc;
mod feedback;
pub(crate) use feedback::{
    FactoryAdaptation, FactoryCompletion, FactoryFeedback, ProducedMetadata,
};

#[derive(Debug, Clone)]
pub(crate) struct FactoryKeys {
    pub(crate) raw: Arc<str>,
    pub(crate) full: Arc<str>,
}

#[derive(Debug, Clone)]
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
    Constant {
        value: V,
        tags: Result<Box<[Tag]>, TagError>,
    },
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
pub(crate) struct FactoryProduct<V> {
    output: FactoryOutput<V>,
    options: EntryOptions,
}
impl<V> FactoryProduct<V> {
    pub(crate) fn into_payload(self) -> crate::Result<FactoryPayload<V>> {
        match self.output {
            FactoryOutput::Constant { value, tags } => Ok(FactoryPayload {
                value,
                options: self.options,
                tags: tags?,
                etag: None,
                last_modified: None,
                origin: ProductOrigin::Constant,
            }),
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
    Constant,
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
/// Why the cache initiated this origin callback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FactoryInvocation {
    /// A caller initiated ordinary retrieval; it may later hand off timed work.
    Foreground,
    /// An existing value initiated background eager refresh.
    EagerRefresh,
}
/// Origin context. Optional stale data forms one complete snapshot.
#[derive(Debug)]
pub struct FactoryContext<V> {
    invocation: FactoryInvocation,
    keys: FactoryKeys,
    options: EntryOptions,
    call_tags: Box<[Tag]>,
    adaptive_tags: TagRequest,
    stale: Option<StaleInfo<V>>,
    cancellation: FactoryCancellation,
}
impl<V> FactoryContext<V> {
    pub(crate) fn with_cancellation(
        keys: FactoryKeys,
        options: EntryOptions,
        call_tags: Box<[Tag]>,
        stale: Option<StaleInfo<V>>,
        cancellation: FactoryCancellation,
    ) -> Self {
        Self {
            invocation: FactoryInvocation::Foreground,
            keys,
            options,
            call_tags,
            adaptive_tags: TagRequest::Inherited,
            stale,
            cancellation,
        }
    }
    pub(crate) fn with_invocation(mut self, invocation: FactoryInvocation) -> Self {
        self.invocation = invocation;
        self
    }
    /// Original foreground/eager cause, independent of the polling thread.
    pub fn invocation(&self) -> FactoryInvocation {
        self.invocation
    }
    /// The prefixed data key.
    pub fn key(&self) -> &str {
        &self.keys.full
    }
    /// The caller's key before the cache prefix was applied.
    pub fn original_key(&self) -> &str {
        &self.keys.raw
    }
    /// Current call or adaptive tags. A rejected legacy tag request is explicit.
    pub fn tags(&self) -> Result<&[Tag], TagError> {
        match &self.adaptive_tags {
            TagRequest::Inherited => Ok(&self.call_tags),
            TagRequest::Valid(tags) => Ok(tags),
            TagRequest::Rejected(error) => Err(*error),
        }
    }
    /// Tags in the complete stale snapshot, when one exists.
    pub fn stale_tags(&self) -> Option<&[Tag]> {
        self.stale.as_ref().map(|stale| stale.tags.as_ref())
    }
    /// Adapts produced-entry options; the cache validates them before effects.
    pub fn options_mut(&mut self) -> FactoryOptionsMut<'_, V> {
        FactoryOptionsMut { context: self }
    }
    /// Current options.
    pub fn options(&self) -> &EntryOptions {
        &self.options
    }
    /// Applies a chainable option transformation.
    pub fn adapt(&mut self, adapt: impl FnOnce(EntryOptions) -> EntryOptions) {
        self.options = adapt(self.options.clone());
        self.publish_adaptation();
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
        self.publish_adaptation();
    }
    /// Validates all raw tags, rejecting the entire boundary on invalid input.
    pub fn try_set_tags<I, S>(&mut self, tags: I) -> Result<(), TagError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.adaptive_tags = TagRequest::Valid(try_collect_tags(tags)?);
        self.publish_adaptation();
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
        self.publish_adaptation();
    }
    fn publish_adaptation(&self) {
        self.cancellation
            .publish_factory_adaptation(FactoryAdaptation {
                options: self.options.clone(),
                tags: self.adaptive_tags.clone().resolve(self.call_tags.clone()),
                metadata: ProducedMetadata::Modified {
                    etag: None,
                    last_modified: None,
                },
            });
    }
    pub(crate) fn completion(&self) -> FactoryCompletion {
        FactoryCompletion::new(
            self.options.clone(),
            self.call_tags.clone(),
            self.cancellation.clone(),
        )
    }
    pub(crate) fn constant(self, value: V) -> FactoryProduct<V> {
        FactoryProduct {
            output: FactoryOutput::Constant {
                value,
                tags: self.adaptive_tags.resolve(self.call_tags),
            },
            options: self.options,
        }
    }
    /// Returns a plain value; adapted options and tags remain in this execution.
    pub fn value(self, value: V) -> V {
        value
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
    pub fn not_modified(self) -> Result<V, FactoryError> {
        let Some(mut stale) = self.stale else {
            return Err(FactoryError::new(
                "not_modified() requires a stale snapshot",
            ));
        };
        let tags = self.adaptive_tags.resolve(std::mem::take(&mut stale.tags));
        self.cancellation
            .publish_factory_adaptation(FactoryAdaptation {
                options: self.options,
                tags,
                metadata: ProducedMetadata::NotModified {
                    etag: stale.etag,
                    last_modified: stale.last_modified,
                },
            });
        Ok(stale.value)
    }
    /// Starts an explicit conditional refresh. Tags default to the stale snapshot,
    /// matching FusionCache; validators can be retained, replaced or cleared.
    /// The existing `not_modified` adapter keeps its adaptive-tag contract.
    pub fn not_modified_builder(self) -> Result<NotModifiedBuilder<V>, ConditionalRefreshError> {
        let stale = self.stale.ok_or(ConditionalRefreshError::NoStaleSnapshot)?;
        let tags = match self.adaptive_tags {
            TagRequest::Inherited | TagRequest::Valid(_) => TagRequest::Inherited,
            TagRequest::Rejected(error) => TagRequest::Rejected(error),
        };
        Ok(NotModifiedBuilder {
            stale,
            options: self.options,
            tags,
            cancellation: self.cancellation,
            etag: ValidatorUpdate::Retain,
            last_modified: ValidatorUpdate::Retain,
        })
    }
}

/// Expected rejection when constructing a conditional unchanged product.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ConditionalRefreshError {
    /// An unchanged result requires a complete existing stale snapshot.
    #[error("NotModified requires a stale snapshot")]
    NoStaleSnapshot,
}
impl From<ConditionalRefreshError> for FactoryError {
    fn from(error: ConditionalRefreshError) -> Self {
        Self::from_source(error)
    }
}

/// An explicit update to optional conditional-refresh metadata.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ValidatorUpdate<T> {
    /// Preserve the stale snapshot's validator.
    #[default]
    Retain,
    /// Replace the validator with this value.
    Replace(T),
    /// Remove the validator from the refreshed snapshot.
    Clear,
}
impl<T> ValidatorUpdate<T> {
    fn apply(self, previous: Option<T>) -> Option<T> {
        match self {
            Self::Retain => previous,
            Self::Replace(value) => Some(value),
            Self::Clear => None,
        }
    }
}

/// A conditional product that always owns a complete existing stale snapshot.
#[derive(Debug)]
#[must_use = "call done() to return the conditional factory product"]
pub struct NotModifiedBuilder<V> {
    stale: StaleInfo<V>,
    options: EntryOptions,
    tags: TagRequest,
    cancellation: FactoryCancellation,
    etag: ValidatorUpdate<String>,
    last_modified: ValidatorUpdate<Timestamp>,
}
impl<V> NotModifiedBuilder<V> {
    /// Selects whether to retain, replace or clear the ETag.
    pub fn etag(mut self, update: ValidatorUpdate<String>) -> Self {
        self.etag = update;
        self
    }
    /// Selects whether to retain, replace or clear the modification timestamp.
    pub fn last_modified(mut self, update: ValidatorUpdate<Timestamp>) -> Self {
        self.last_modified = update;
        self
    }
    /// Explicitly replaces stale tags with validated values.
    pub fn validated_tags(mut self, tags: Box<[Tag]>) -> Self {
        self.tags = TagRequest::Valid(tags);
        self
    }
    /// Returns the unchanged value with selected metadata and adapted options.
    pub fn done(mut self) -> V {
        self.stale.etag = self.etag.apply(self.stale.etag);
        self.stale.last_modified = self.last_modified.apply(self.stale.last_modified);
        let tags = self.tags.resolve(std::mem::take(&mut self.stale.tags));
        self.cancellation
            .publish_factory_adaptation(FactoryAdaptation {
                options: self.options,
                tags,
                metadata: ProducedMetadata::NotModified {
                    etag: self.stale.etag,
                    last_modified: self.stale.last_modified,
                },
            });
        self.stale.value
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
    pub fn done(self) -> V {
        let inherited = self.ctx.adaptive_tags.resolve(self.ctx.call_tags);
        let tags = match self.tags {
            TagRequest::Inherited => inherited,
            TagRequest::Valid(tags) => Ok(tags),
            TagRequest::Rejected(error) => Err(error),
        };
        self.ctx
            .cancellation
            .publish_factory_adaptation(FactoryAdaptation {
                options: self.ctx.options,
                tags,
                metadata: ProducedMetadata::Modified {
                    etag: self.etag,
                    last_modified: self.last_modified,
                },
            });
        self.value
    }
}

/// Publishes adaptive options when the temporary mutable view is released.
#[derive(Debug)]
pub struct FactoryOptionsMut<'a, V> {
    context: &'a mut FactoryContext<V>,
}
impl<V> std::ops::Deref for FactoryOptionsMut<'_, V> {
    type Target = EntryOptions;
    fn deref(&self) -> &EntryOptions {
        &self.context.options
    }
}
impl<V> std::ops::DerefMut for FactoryOptionsMut<'_, V> {
    fn deref_mut(&mut self) -> &mut EntryOptions {
        &mut self.context.options
    }
}
impl<V> Drop for FactoryOptionsMut<'_, V> {
    fn drop(&mut self) {
        self.context.publish_adaptation();
    }
}
