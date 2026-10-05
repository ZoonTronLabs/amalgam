//! Validated tags, cache namespaces, and monotonic invalidation markers.
//!
//! The inclusive comparison matches FusionCache 2.9: an entry whose revision is
//! equal to an invalidation marker is invalidated. Markers use their own typed
//! domain; an ordinary cache key never becomes an invalidation command.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::options::{KeyModifierMode, RemoveByTagBehavior};
use crate::time::Timestamp;

/// Rejection at the raw-string tag boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TagError {
    /// Tags cannot be empty or consist only of whitespace.
    #[error("cache tag must not be blank")]
    Blank,
}

/// A nonblank cache tag. Deserialization uses the same validating factory.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Tag(Arc<str>);

impl Tag {
    /// Validates a raw tag without changing its spelling.
    pub fn new(tag: impl AsRef<str>) -> Result<Self, TagError> {
        let tag = tag.as_ref();
        if tag.trim().is_empty() {
            Err(TagError::Blank)
        } else {
            Ok(Self(Arc::from(tag)))
        }
    }

    /// The original nonblank spelling.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for Tag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Serialize for Tag {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for Tag {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// Validates every raw tag; a blank tag rejects the entire boundary operation.
pub fn try_collect_tags<I, S>(tags: I) -> Result<Box<[Tag]>, TagError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    tags.into_iter().map(Tag::new).collect()
}

/// Explicit legacy raw-string adapter. Rejected blank tags are reported; new
/// operation boundaries use `try_collect_tags` and propagate their typed failure.
#[must_use]
pub fn collect_tags<I, S>(tags: I) -> Box<[Tag]>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    tags.into_iter()
        .filter_map(|tag| match Tag::new(tag) {
            Ok(tag) => Some(tag),
            Err(error) => {
                tracing::warn!(%error, "legacy raw tag was rejected");
                None
            }
        })
        .collect()
}

/// Failures in the invalidation protocol, separate from ordinary value I/O.
#[derive(Debug, thiserror::Error)]
pub enum MarkerError {
    /// A marker capability was requested from a legacy byte store.
    #[error("the configured backend does not supply atomic invalidation storage")]
    Unsupported,
    /// The wire namespace is blank.
    #[error("invalidation wire namespace must not be blank")]
    BlankWireVersion,
    /// Capacity limits must admit at least one tag and one scope.
    #[error("invalidation capacities must be positive")]
    ZeroCapacity,
    /// A backend cannot admit another independent durable scope safely.
    #[error("invalidation scope capacity {limit} is exhausted")]
    ScopeCapacity {
        /// The configured bound.
        limit: usize,
    },
    /// External durable storage failed; the original cause is retained.
    #[error("invalidation storage failed: {source}")]
    Backend {
        /// Original backend failure.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// A marker response or control frame violated the wire protocol.
    #[error("invalid invalidation protocol: {detail}")]
    Protocol {
        /// A diagnostic explanation.
        detail: String,
    },
    /// Decoding a control frame failed with its original parse/validation cause.
    #[error("invalid invalidation protocol: {source}")]
    ProtocolWithSource {
        /// Original parser or boundary failure.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

impl MarkerError {
    /// Preserves a control-protocol parser or validation failure.
    pub fn protocol(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::ProtocolWithSource {
            source: Box::new(source),
        }
    }

    /// Preserves an external storage cause.
    pub fn backend(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Backend {
            source: Box::new(source),
        }
    }
}

/// A collision-free marker namespace independent of diagnostic cache names.
#[derive(Debug, Clone)]
pub struct CacheScope {
    prefix: Arc<str>,
    version: Arc<str>,
    modifier: KeyModifierMode,
    physical_prefix: Arc<str>,
    physical_suffix: Arc<str>,
}

impl PartialEq for CacheScope {
    fn eq(&self, other: &Self) -> bool {
        self.physical_prefix == other.physical_prefix
            && self.physical_suffix == other.physical_suffix
    }
}
impl Eq for CacheScope {}
impl std::hash::Hash for CacheScope {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.physical_prefix.hash(state);
        self.physical_suffix.hash(state);
    }
}

impl CacheScope {
    /// Validates the namespace. Prefix may be empty; version may not be blank.
    pub fn new(
        prefix: impl AsRef<str>,
        version: impl AsRef<str>,
        modifier: KeyModifierMode,
    ) -> Result<Self, MarkerError> {
        if version.as_ref().trim().is_empty() {
            return Err(MarkerError::BlankWireVersion);
        }
        let (physical_prefix, physical_suffix) = match modifier {
            KeyModifierMode::Prefix => (
                format!("{}:{}", version.as_ref(), prefix.as_ref()),
                String::new(),
            ),
            KeyModifierMode::Suffix => {
                (prefix.as_ref().to_owned(), format!(":{}", version.as_ref()))
            }
            KeyModifierMode::None => (prefix.as_ref().to_owned(), String::new()),
        };
        Ok(Self {
            prefix: Arc::from(prefix.as_ref()),
            version: Arc::from(version.as_ref()),
            modifier,
            physical_prefix: physical_prefix.into(),
            physical_suffix: physical_suffix.into(),
        })
    }

    /// The cache's configured key prefix.
    #[must_use]
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// The configured wire namespace.
    #[must_use]
    pub fn wire_version(&self) -> &str {
        &self.version
    }

    /// How value keys incorporate their wire namespace.
    #[must_use]
    pub fn modifier(&self) -> KeyModifierMode {
        self.modifier
    }

    /// A length-delimited identifier; separators inside either string are safe.
    #[must_use]
    pub fn storage_id(&self) -> String {
        format!(
            "{}:{}{}:{}",
            self.physical_prefix.len(),
            self.physical_prefix,
            self.physical_suffix.len(),
            self.physical_suffix
        )
    }
}

/// Independent marker categories. Data keys are never parsed into this enum.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MarkerKind {
    /// Lazy invalidation of entries carrying this validated tag.
    Tag(Tag),
    /// Logical invalidation of the whole scope.
    ClearExpire,
    /// Physical invalidation of the whole scope.
    ClearRemove,
}

/// An ordered invalidation revision, distinct from lease time and retry identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MarkerVersion(Timestamp);

impl MarkerVersion {
    /// Wraps an injected operation timestamp.
    #[must_use]
    pub const fn new(at: Timestamp) -> Self {
        Self(at)
    }

    /// The revision used for inclusive entry comparisons.
    #[must_use]
    pub const fn timestamp(self) -> Timestamp {
        self.0
    }

    /// An order-preserving representation exact even outside Lua's integer range.
    #[must_use]
    pub fn ordered_hex(self) -> String {
        format!("{:016x}", (self.0.ticks() as u64) ^ (1_u64 << 63))
    }

    /// Parses the exact storage representation.
    pub fn from_ordered_hex(encoded: &str) -> Result<Self, MarkerError> {
        if encoded.len() != 16 || !encoded.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(MarkerError::Protocol {
                detail: "invalid marker revision".into(),
            });
        }
        let biased = u64::from_str_radix(encoded, 16).map_err(|error| MarkerError::Protocol {
            detail: error.to_string(),
        })?;
        Ok(Self(Timestamp::from_ticks((biased ^ (1_u64 << 63)) as i64)))
    }
}

/// One marker observed from durable storage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredMarker {
    kind: MarkerKind,
    version: MarkerVersion,
}

impl StoredMarker {
    /// Pairs a validated marker identity with its revision.
    #[must_use]
    pub fn new(kind: MarkerKind, version: MarkerVersion) -> Self {
        Self { kind, version }
    }

    /// The marker category.
    #[must_use]
    pub fn kind(&self) -> &MarkerKind {
        &self.kind
    }

    /// The durable maximum.
    #[must_use]
    pub fn version(&self) -> MarkerVersion {
        self.version
    }
}

/// Successful atomic advancement, including any conservative capacity eviction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarkerAdvanceOutcome {
    /// The candidate was merged with the existing maximum.
    Advanced(StoredMarker),
    /// Tag tombstones were compacted after promoting a durable clear-remove fence.
    Compacted {
        /// The requested marker's effective revision.
        marker: StoredMarker,
        /// Every entry through this revision is now hard-invalidated.
        clear_remove: MarkerVersion,
    },
}

impl MarkerAdvanceOutcome {
    /// The effective maximum for the requested operation.
    #[must_use]
    pub fn marker(&self) -> &StoredMarker {
        match self {
            Self::Advanced(marker) | Self::Compacted { marker, .. } => marker,
        }
    }
}

/// Positive resource bounds for the in-memory marker provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarkerStoreLimits {
    max_tags: usize,
    max_scopes: usize,
}

impl MarkerStoreLimits {
    /// Rejects zero capacities before work starts.
    pub fn new(max_tags: usize, max_scopes: usize) -> Result<Self, MarkerError> {
        if max_tags == 0 || max_scopes == 0 {
            return Err(MarkerError::ZeroCapacity);
        }
        Ok(Self {
            max_tags,
            max_scopes,
        })
    }

    /// Tag markers admitted in each namespace before conservative compaction.
    #[must_use]
    pub const fn max_tags(self) -> usize {
        self.max_tags
    }

    /// Independent durable namespaces admitted by the reference provider.
    #[must_use]
    pub const fn max_scopes(self) -> usize {
        self.max_scopes
    }
}

impl Default for MarkerStoreLimits {
    fn default() -> Self {
        Self {
            max_tags: 4096,
            max_scopes: 1024,
        }
    }
}

/// The strongest invalidation applying to an entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TagVerdict {
    /// No marker invalidates the entry.
    Valid,
    /// Treat the value as logically stale, retaining fail-safe eligibility.
    Expire,
    /// The entry must be physically discarded.
    Remove,
}

#[derive(Debug, Default)]
pub(crate) struct MarkerState {
    tags: HashMap<Tag, MarkerVersion>,
    clear_expire: Option<MarkerVersion>,
    clear_remove: Option<MarkerVersion>,
}

impl MarkerState {
    pub(crate) fn read(&self, kind: &MarkerKind) -> Option<MarkerVersion> {
        match kind {
            MarkerKind::Tag(tag) => self.tags.get(tag).copied(),
            MarkerKind::ClearExpire => self.clear_expire,
            MarkerKind::ClearRemove => self.clear_remove,
        }
    }

    pub(crate) fn advance(
        &mut self,
        kind: MarkerKind,
        candidate: MarkerVersion,
        capacity: usize,
    ) -> MarkerAdvanceOutcome {
        let version = self.read(&kind).map_or(candidate, |old| old.max(candidate));
        match &kind {
            MarkerKind::Tag(tag) => {
                self.tags.insert(tag.clone(), version);
            }
            MarkerKind::ClearExpire => {
                self.clear_expire = Some(version);
            }
            MarkerKind::ClearRemove => {
                self.clear_remove = Some(version);
            }
        }
        let marker = StoredMarker::new(kind, version);
        if self.tags.len() <= capacity {
            return MarkerAdvanceOutcome::Advanced(marker);
        }
        // A durable global tombstone replaces tag tombstones before they disappear.
        let through = self.tags.values().copied().max().unwrap_or(version);
        let clear_remove = self.clear_remove.map_or(through, |old| old.max(through));
        self.clear_remove = Some(clear_remove);
        self.tags.retain(|_, at| *at > clear_remove);
        MarkerAdvanceOutcome::Compacted {
            marker,
            clear_remove,
        }
    }

    pub(crate) fn tag_count(&self) -> usize {
        self.tags.len()
    }
}

/// Local marker maxima, bounded without forgetting an invalidation.
#[derive(Debug)]
pub struct TagRegistry {
    state: Mutex<MarkerState>,
    capacity: usize,
}

impl Default for TagRegistry {
    fn default() -> Self {
        Self {
            state: Mutex::new(MarkerState::default()),
            capacity: 4096,
        }
    }
}

impl TagRegistry {
    /// Creates an empty, bounded local registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Uses an explicit positive bound.
    pub fn with_capacity(capacity: usize) -> Result<Self, MarkerError> {
        if capacity == 0 {
            return Err(MarkerError::ZeroCapacity);
        }
        Ok(Self {
            state: Mutex::new(MarkerState::default()),
            capacity,
        })
    }

    /// Merges an observed marker, returning any safe compaction fence.
    pub fn advance(&self, kind: MarkerKind, at: MarkerVersion) -> MarkerAdvanceOutcome {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .advance(kind, at, self.capacity)
    }

    /// Legacy local invalidation adapter.
    pub fn mark_tag(&self, tag: Tag, at: Timestamp) {
        self.advance(MarkerKind::Tag(tag), MarkerVersion::new(at));
    }

    /// Reads one locally known tag maximum.
    #[must_use]
    pub fn tag_marker(&self, tag: &Tag) -> Option<Timestamp> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .read(&MarkerKind::Tag(tag.clone()))
            .map(MarkerVersion::timestamp)
    }

    /// Reads a confirmed local maximum without changing observation freshness.
    #[must_use]
    pub fn marker_version(&self, kind: &MarkerKind) -> Option<MarkerVersion> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .read(kind)
    }

    /// Merges a scope-wide logical invalidation.
    pub fn mark_clear_expire(&self, at: Timestamp) {
        self.advance(MarkerKind::ClearExpire, MarkerVersion::new(at));
    }

    /// Merges a scope-wide physical invalidation.
    pub fn mark_clear_remove(&self, at: Timestamp) {
        self.advance(MarkerKind::ClearRemove, MarkerVersion::new(at));
    }

    /// Applies the strongest known marker using inclusive snapshot comparisons.
    #[must_use]
    pub fn evaluate(
        &self,
        created: Timestamp,
        tags: &[Tag],
        behavior: RemoveByTagBehavior,
    ) -> TagVerdict {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state
            .clear_remove
            .is_some_and(|at| created <= at.timestamp())
        {
            return TagVerdict::Remove;
        }
        if tags.iter().any(|tag| {
            state
                .tags
                .get(tag)
                .is_some_and(|at| created <= at.timestamp())
        }) {
            return match behavior {
                RemoveByTagBehavior::Expire => TagVerdict::Expire,
                RemoveByTagBehavior::Remove => TagVerdict::Remove,
            };
        }
        if state
            .clear_expire
            .is_some_and(|at| created <= at.timestamp())
        {
            return TagVerdict::Expire;
        }
        TagVerdict::Valid
    }

    /// Number of retained per-tag maxima.
    #[must_use]
    pub fn len(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .tag_count()
    }

    /// Whether no per-tag maximum is retained; global fences may still exist.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tag_construction_and_deserialization_reject_blank() {
        assert_eq!(Tag::new(" "), Err(TagError::Blank));
        assert!(serde_json::from_str::<Tag>("\" \"").is_err());
        assert!(try_collect_tags(["ok", " "]).is_err());
    }

    #[test]
    fn inclusive_markers_and_compaction_never_forget_invalidation() {
        let registry = TagRegistry::with_capacity(1).unwrap();
        let at = Timestamp::from_ticks(42);
        registry.mark_tag(Tag::new("a").unwrap(), at);
        assert_eq!(
            registry.evaluate(at, &[Tag::new("a").unwrap()], RemoveByTagBehavior::Expire),
            TagVerdict::Expire
        );
        registry.mark_tag(Tag::new("b").unwrap(), Timestamp::from_ticks(43));
        assert_eq!(registry.len(), 0);
        assert_eq!(
            registry.evaluate(at, &[Tag::new("a").unwrap()], RemoveByTagBehavior::Expire),
            TagVerdict::Remove
        );
    }

    #[test]
    fn storage_order_covers_full_signed_range() {
        let values = [i64::MIN, -1, 0, 1, (1_i64 << 54) + 1, i64::MAX];
        let encodings: Vec<_> = values
            .into_iter()
            .map(|ticks| MarkerVersion::new(Timestamp::from_ticks(ticks)).ordered_hex())
            .collect();
        assert!(encodings.windows(2).all(|pair| pair[0] < pair[1]));
        for (ticks, encoded) in values.into_iter().zip(encodings) {
            assert_eq!(
                MarkerVersion::from_ordered_hex(&encoded)
                    .unwrap()
                    .timestamp()
                    .ticks(),
                ticks
            );
        }
    }
}
