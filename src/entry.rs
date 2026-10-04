//! The internal cache entry envelope: value plus the metadata that drives
//! freshness, fail-safe and eager-refresh decisions.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::error::{ConfigError, Result};
use crate::options::{
    EntryOptions, EntryWeight, JitterSample, JitterSource, Priority, RandomJitterSource,
};
use crate::serializers::ValueCloner;
use crate::tags::Tag;
use crate::time::Timestamp;

/// Whether an entry is still within its logical freshness window.
///
/// Storage checks physical expiration separately before serving an entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Freshness {
    /// `now` is before the logical expiration — safe to return directly.
    Fresh,
    /// `now` is at or after the logical expiration — only usable as a fail-safe
    /// fallback.
    Stale,
}

impl Freshness {
    /// `true` if the entry is fresh.
    #[must_use]
    pub fn is_fresh(self) -> bool {
        matches!(self, Freshness::Fresh)
    }
}

/// Metadata stored alongside a cached value.
#[derive(Debug, Clone)]
pub struct Metadata {
    /// When the underlying value was produced (used for tag-marker comparison).
    created: Timestamp,
    /// When this representation was inserted, independently of value ordering.
    inserted_at: Timestamp,
    /// The freshness boundary.
    logical_expiration: Timestamp,
    /// The physical boundary (the value is gone after this).
    physical_expiration: Timestamp,
    /// The TTL handed to the backend at insert time (`physical_expiration` minus
    /// the insertion instant). Stored so the backend's expiry policy can read it
    /// back without consulting the wall clock.
    backend_ttl: Duration,
    /// `true` if this entry's value was itself produced by a fail-safe
    /// activation (it must not be jittered or eagerly refreshed again).
    origin: EntryOrigin,
    /// HTTP-style entity tag, for conditional refresh.
    etag: Option<String>,
    /// HTTP-style last-modified time, for conditional refresh.
    last_modified: Option<Timestamp>,
    /// The tags attached to this entry.
    tags: Box<[Tag]>,
    retention: RetentionMetadata,
}

#[derive(Debug, Clone)]
enum EntryOrigin {
    Fresh { eager_refresh_at: Option<Timestamp> },
    FailSafe,
}

#[derive(Debug, Clone, Copy)]
enum RetentionMetadata {
    Unspecified,
    Specified {
        size: Option<EntryWeight>,
        priority: Priority,
    },
}

impl Metadata {
    /// The creation timestamp.
    #[must_use]
    pub fn created(&self) -> Timestamp {
        self.created
    }

    /// The insertion/hydration instant used for TTL calculations.
    #[must_use]
    pub fn inserted_at(&self) -> Timestamp {
        self.inserted_at
    }

    /// The configured entry weight; absence counts as one unit in memory.
    #[must_use]
    pub fn size(&self) -> Option<EntryWeight> {
        match self.retention {
            RetentionMetadata::Unspecified => None,
            RetentionMetadata::Specified { size, .. } => size,
        }
    }

    /// The capacity-eviction priority.
    #[must_use]
    pub fn priority(&self) -> Priority {
        match self.retention {
            RetentionMetadata::Unspecified => Priority::Normal,
            RetentionMetadata::Specified { priority, .. } => priority,
        }
    }

    /// An explicitly persisted priority; legacy/plain metadata may omit it and
    /// permit the caller's configured priority during hydration.
    #[must_use]
    pub fn stored_priority(&self) -> Option<Priority> {
        match self.retention {
            RetentionMetadata::Unspecified => None,
            RetentionMetadata::Specified { priority, .. } => Some(priority),
        }
    }

    /// The eager-refresh boundary for this representation.
    #[must_use]
    pub fn eager_refresh_at(&self) -> Option<Timestamp> {
        match self.origin {
            EntryOrigin::Fresh { eager_refresh_at } => eager_refresh_at,
            EntryOrigin::FailSafe => None,
        }
    }

    /// The logical-expiration (freshness) boundary.
    #[must_use]
    pub fn logical_expiration(&self) -> Timestamp {
        self.logical_expiration
    }

    /// The physical-expiration (fail-safe) boundary.
    #[must_use]
    pub fn physical_expiration(&self) -> Timestamp {
        self.physical_expiration
    }

    /// The tags attached to this entry.
    #[must_use]
    pub fn tags(&self) -> &[Tag] {
        &self.tags
    }

    /// The entity tag, if any.
    #[must_use]
    pub fn etag(&self) -> Option<&str> {
        self.etag.as_deref()
    }

    /// The last-modified timestamp, if any.
    #[must_use]
    pub fn last_modified(&self) -> Option<Timestamp> {
        self.last_modified
    }

    /// `true` if this value came from a fail-safe activation.
    #[must_use]
    pub fn is_from_fail_safe(&self) -> bool {
        matches!(self.origin, EntryOrigin::FailSafe)
    }
}

/// A cheaply-cloneable handle to a cached value and its metadata.
///
/// Entries are immutable; "mutating" an entry (throttling a stale value,
/// refreshing it) produces a new `Entry` that replaces the old one. This avoids
/// interior mutability and makes every stored value safe to share across tasks.
#[derive(Debug)]
pub struct Entry<V> {
    inner: Arc<EntryInner<V>>,
}

#[derive(Debug)]
struct EntryInner<V> {
    value: V,
    meta: Metadata,
    eligibility: Eligibility,
}

#[derive(Debug, Clone)]
enum Eligibility {
    Local,
    Hydrated(ContinuityStamp),
}

/// A private local read fence; it is deliberately absent from wire metadata.
#[derive(Debug, Clone)]
pub(crate) struct ContinuityStamp {
    epoch: Arc<AtomicU64>,
    captured: u64,
}

impl ContinuityStamp {
    pub(crate) fn new(epoch: Arc<AtomicU64>, captured: u64) -> Self {
        Self { epoch, captured }
    }

    fn is_current(&self) -> bool {
        self.epoch.load(Ordering::Acquire) == self.captured
    }
}

impl<V> Clone for Entry<V> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<V> Entry<V> {
    /// Borrows the cached value.
    #[must_use]
    pub fn value(&self) -> &V {
        &self.inner.value
    }

    /// The entry's metadata.
    #[must_use]
    pub fn meta(&self) -> &Metadata {
        &self.inner.meta
    }

    /// The TTL to hand the backend's expiry policy.
    #[must_use]
    pub fn backend_ttl(&self) -> Duration {
        self.inner.meta.backend_ttl
    }

    /// Remaining physical TTL at an actual backend insertion or retry instant.
    #[must_use]
    pub fn backend_ttl_at(&self, now: Timestamp) -> Duration {
        self.meta()
            .physical_expiration
            .saturating_duration_since(now)
    }

    /// Whether two handles refer to the identical immutable representation.
    #[must_use]
    pub fn is_same_instance(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    pub(crate) fn is_read_eligible(&self) -> bool {
        match &self.inner.eligibility {
            Eligibility::Local => true,
            Eligibility::Hydrated(stamp) => stamp.is_current(),
        }
    }

    /// Recomputes remaining backend TTL without changing absolute deadlines.
    #[must_use]
    pub fn at_insertion(&self, inserted_at: Timestamp) -> Self
    where
        V: Clone,
    {
        if inserted_at == self.meta().inserted_at {
            return self.clone();
        }
        let mut meta = self.meta().clone();
        meta.inserted_at = inserted_at;
        meta.backend_ttl = self.backend_ttl_at(inserted_at);
        Self {
            inner: Arc::new(EntryInner {
                value: self.value().clone(),
                meta,
                eligibility: self.inner.eligibility.clone(),
            }),
        }
    }

    /// Preserves wire metadata while binding an L2 hydration to local continuity.
    pub(crate) fn with_hydrated_value(&self, value: V, stamp: ContinuityStamp) -> Self {
        Self {
            inner: Arc::new(EntryInner {
                value,
                meta: self.meta().clone(),
                eligibility: Eligibility::Hydrated(stamp),
            }),
        }
    }

    /// Computes freshness relative to `now`.
    #[must_use]
    pub fn freshness(&self, now: Timestamp) -> Freshness {
        if now.is_before(self.inner.meta.logical_expiration) {
            Freshness::Fresh
        } else {
            Freshness::Stale
        }
    }

    /// `true` if the entry is logically expired at `now`.
    #[must_use]
    pub fn is_logically_expired(&self, now: Timestamp) -> bool {
        !now.is_before(self.inner.meta.logical_expiration)
    }

    /// `true` if `now` is at/after the physical boundary (the entry is dead and
    /// no longer usable even as a fail-safe fallback).
    #[must_use]
    pub fn is_physically_expired(&self, now: Timestamp) -> bool {
        !now.is_before(self.inner.meta.physical_expiration)
    }

    /// `true` if a proactive background refresh should start now.
    #[must_use]
    pub fn should_eager_refresh(&self, now: Timestamp) -> bool {
        match self.inner.meta.eager_refresh_at() {
            Some(at) => !now.is_before(at) && now.is_before(self.inner.meta.logical_expiration),
            None => false,
        }
    }
}

impl<V: Clone> Entry<V> {
    /// Clones out the cached value.
    #[must_use]
    pub fn value_cloned(&self) -> V {
        self.inner.value.clone()
    }

    /// Copies the value according to the requested isolation policy.
    pub fn value_with_options(
        &self,
        options: &EntryOptions,
        cloner: Option<&dyn ValueCloner<V>>,
    ) -> Result<V> {
        crate::serializers::copy_value(self.value(), options, cloner)
    }
}

impl<V> Entry<V> {
    /// Builds a fresh entry from a freshly-produced value.
    #[must_use]
    pub fn fresh(
        value: V,
        options: &EntryOptions,
        created: Timestamp,
        tags: Box<[Tag]>,
        etag: Option<String>,
        last_modified: Option<Timestamp>,
    ) -> Self {
        Self::fresh_at(value, options, created, created, tags, etag, last_modified)
    }

    /// Legacy infallible adapter for separate snapshot and insertion timestamps.
    ///
    /// # Panics
    /// Panics for invalid developer configuration. Cache operations use
    /// [`try_fresh_at`](Self::try_fresh_at) to propagate typed rejection.
    #[must_use]
    pub fn fresh_at(
        value: V,
        options: &EntryOptions,
        snapshot_created: Timestamp,
        inserted_at: Timestamp,
        tags: Box<[Tag]>,
        etag: Option<String>,
        last_modified: Option<Timestamp>,
    ) -> Self {
        match Self::try_fresh_at(
            value,
            options,
            snapshot_created,
            inserted_at,
            tags,
            etag,
            last_modified,
        ) {
            Ok(entry) => entry,
            Err(error) => panic!("invalid entry configuration: {error}"),
        }
    }

    /// Builds a valid fresh representation. TTL starts at insertion; snapshot
    /// ordering remains anchored at when the origin lookup started.
    pub fn try_fresh_at(
        value: V,
        options: &EntryOptions,
        snapshot_created: Timestamp,
        inserted_at: Timestamp,
        tags: Box<[Tag]>,
        etag: Option<String>,
        last_modified: Option<Timestamp>,
    ) -> Result<Self> {
        let jitter = JitterSample::new(
            RandomJitterSource.sample(options.jitter_max()),
            options.jitter_max(),
        )?;
        Self::try_fresh_with_jitter(
            value,
            options,
            snapshot_created,
            inserted_at,
            jitter,
            tags,
            etag,
            last_modified,
        )
    }

    /// Canonical pure construction with explicit insertion time and sampled
    /// jitter. No clock or random source is consulted inside this factory.
    #[allow(clippy::too_many_arguments)]
    pub fn try_fresh_with_jitter(
        value: V,
        options: &EntryOptions,
        snapshot_created: Timestamp,
        inserted_at: Timestamp,
        jitter: JitterSample,
        tags: Box<[Tag]>,
        etag: Option<String>,
        last_modified: Option<Timestamp>,
    ) -> Result<Self> {
        let size = options.size()?;
        let logical_expiration = options.logical_expiration_with_jitter(inserted_at, jitter)?;
        let physical_expiration = inserted_at
            .saturating_add(options.physical_ttl())
            .max(logical_expiration);
        let retention = if size.is_some()
            || options.priority() != Priority::Normal
            || options.is_fail_safe_enabled()
            || options.eager_refresh_threshold().is_some()
            || etag.is_some()
            || last_modified.is_some()
        {
            RetentionMetadata::Specified {
                size,
                priority: options.priority(),
            }
        } else {
            RetentionMetadata::Unspecified
        };
        let meta = Metadata {
            created: snapshot_created,
            inserted_at,
            logical_expiration,
            physical_expiration,
            backend_ttl: physical_expiration.saturating_duration_since(inserted_at),
            origin: EntryOrigin::Fresh {
                eager_refresh_at: options
                    .eager_refresh_at_expiration(inserted_at, logical_expiration),
            },
            etag,
            last_modified,
            tags,
            retention,
        };
        Ok(Self {
            inner: Arc::new(EntryInner {
                value,
                meta,
                eligibility: Eligibility::Local,
            }),
        })
    }

    /// Builds a throttled fail-safe entry that re-serves an existing value for
    /// the throttle window, keeping the original physical boundary.
    ///
    /// Returns `None` when the source value is already physically expired and so
    /// cannot be reused.
    #[must_use]
    pub fn throttled(source: &Entry<V>, options: &EntryOptions, now: Timestamp) -> Option<Self>
    where
        V: Clone,
    {
        match Self::try_throttled(source, options, now) {
            Ok(entry) => entry,
            Err(error) => panic!("invalid fail-safe entry configuration: {error}"),
        }
    }

    /// Validates operation options before reusing a stale representation.
    pub fn try_throttled(
        source: &Entry<V>,
        options: &EntryOptions,
        now: Timestamp,
    ) -> Result<Option<Self>>
    where
        V: Clone,
    {
        options.validate()?;
        if !source.is_read_eligible() || source.is_physically_expired(now) {
            return Ok(None);
        }
        let physical_expiration = source.inner.meta.physical_expiration;
        let backend_ttl = physical_expiration.saturating_duration_since(now);
        let logical_expiration = now
            .saturating_add(options.fail_safe_throttle_duration())
            .min(physical_expiration);
        let meta = Metadata {
            created: source.inner.meta.created,
            inserted_at: now,
            logical_expiration,
            physical_expiration,
            backend_ttl,
            origin: EntryOrigin::FailSafe,
            etag: source.inner.meta.etag.clone(),
            last_modified: source.inner.meta.last_modified,
            tags: source.inner.meta.tags.clone(),
            retention: source.inner.meta.retention,
        };
        Ok(Some(Self {
            inner: Arc::new(EntryInner {
                value: source.inner.value.clone(),
                meta,
                eligibility: source.inner.eligibility.clone(),
            }),
        }))
    }

    /// Produces a copy of this entry that is logically expired as of `at`, while
    /// keeping the original physical boundary so fail-safe can still serve it.
    /// Used by [`Cache::expire`](crate::Cache::expire).
    #[must_use]
    pub fn with_logical_expiration(&self, at: Timestamp) -> Self
    where
        V: Clone,
    {
        self.with_logical_expiration_at(at, at)
    }

    /// Expires this representation using the actual insertion instant for TTL.
    #[must_use]
    pub fn with_logical_expiration_at(&self, at: Timestamp, inserted_at: Timestamp) -> Self
    where
        V: Clone,
    {
        let mut meta = self.inner.meta.clone();
        meta.logical_expiration = at.min(meta.physical_expiration);
        meta.inserted_at = inserted_at;
        meta.origin = match meta.origin {
            EntryOrigin::Fresh { .. } => EntryOrigin::Fresh {
                eager_refresh_at: None,
            },
            EntryOrigin::FailSafe => EntryOrigin::FailSafe,
        };
        meta.backend_ttl = meta
            .physical_expiration
            .saturating_duration_since(inserted_at);
        Self {
            inner: Arc::new(EntryInner {
                value: self.inner.value.clone(),
                meta,
                eligibility: self.inner.eligibility.clone(),
            }),
        }
    }

    /// Rebuilds an in-memory entry from data read out of the L2 distributed
    /// cache, recomputing the backend TTL from the (absolute) physical boundary.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn rehydrate(
        value: V,
        created: Timestamp,
        logical_expiration: Timestamp,
        physical_expiration: Timestamp,
        is_from_fail_safe: bool,
        etag: Option<String>,
        last_modified: Option<Timestamp>,
        tags: Box<[Tag]>,
        now: Timestamp,
    ) -> Self {
        match Self::try_rehydrate(
            value,
            created,
            logical_expiration,
            physical_expiration,
            is_from_fail_safe,
            etag,
            last_modified,
            tags,
            now,
        ) {
            Ok(entry) => entry,
            Err(error) => panic!("invalid entry metadata: {error}"),
        }
    }

    /// Validates absolute deadlines while rebuilding a distributed snapshot.
    #[allow(clippy::too_many_arguments)]
    pub fn try_rehydrate(
        value: V,
        created: Timestamp,
        logical_expiration: Timestamp,
        physical_expiration: Timestamp,
        is_from_fail_safe: bool,
        etag: Option<String>,
        last_modified: Option<Timestamp>,
        tags: Box<[Tag]>,
        now: Timestamp,
    ) -> Result<Self> {
        if logical_expiration > physical_expiration {
            return Err(ConfigError::InvalidEntryDeadlines.into());
        }
        let meta = Metadata {
            created,
            inserted_at: now,
            logical_expiration,
            physical_expiration,
            backend_ttl: physical_expiration.saturating_duration_since(now),
            origin: if is_from_fail_safe {
                EntryOrigin::FailSafe
            } else {
                EntryOrigin::Fresh {
                    eager_refresh_at: None,
                }
            },
            etag,
            last_modified,
            tags,
            retention: RetentionMetadata::Unspecified,
        };
        Ok(Self {
            inner: Arc::new(EntryInner {
                value,
                meta,
                eligibility: Eligibility::Local,
            }),
        })
    }

    /// Adds already-validated persisted retention metadata.
    #[must_use]
    pub fn with_retention(&self, size: Option<EntryWeight>, priority: Priority) -> Self
    where
        V: Clone,
    {
        let mut meta = self.meta().clone();
        meta.retention = RetentionMetadata::Specified { size, priority };
        Self {
            inner: Arc::new(EntryInner {
                value: self.value().clone(),
                meta,
                eligibility: self.inner.eligibility.clone(),
            }),
        }
    }

    /// Creates the local representation of an L2 entry without renewing its
    /// source deadlines. Physically dead snapshots are rejected as absent.
    pub fn for_memory_hydration(
        &self,
        options: &EntryOptions,
        now: Timestamp,
    ) -> Result<Option<Self>>
    where
        V: Clone,
    {
        let requested_size = options.size()?;
        if !self.is_read_eligible() || self.is_physically_expired(now) {
            return Ok(None);
        }
        let mut meta = self.meta().clone();
        meta.inserted_at = now;
        meta.logical_expiration = meta
            .logical_expiration
            .min(now.saturating_add(options.resolved_memory_duration()));
        meta.physical_expiration = meta
            .physical_expiration
            .min(now.saturating_add(options.physical_ttl()))
            .max(meta.logical_expiration);
        meta.backend_ttl = meta.physical_expiration.saturating_duration_since(now);
        meta.retention = match meta.retention {
            RetentionMetadata::Specified { size, priority } => RetentionMetadata::Specified {
                size: size.or(requested_size),
                priority,
            },
            RetentionMetadata::Unspecified => RetentionMetadata::Specified {
                size: requested_size,
                priority: options.priority(),
            },
        };
        meta.origin = match meta.origin {
            EntryOrigin::FailSafe => EntryOrigin::FailSafe,
            EntryOrigin::Fresh { .. } => EntryOrigin::Fresh {
                eager_refresh_at: options
                    .eager_refresh_threshold()
                    .filter(|_| now < meta.logical_expiration)
                    .map(|threshold| {
                        now.saturating_add(
                            meta.logical_expiration
                                .saturating_duration_since(now)
                                .mul_f32(threshold.fraction()),
                        )
                    }),
            },
        };
        Ok(Some(Self {
            inner: Arc::new(EntryInner {
                value: self.value().clone(),
                meta,
                eligibility: self.inner.eligibility.clone(),
            }),
        }))
    }

    /// Builds a fail-safe entry from a default value (the `fail_safe_default`),
    /// throttled like [`throttled`](Self::throttled).
    #[must_use]
    pub fn from_fail_safe_default(value: V, options: &EntryOptions, now: Timestamp) -> Self {
        match Self::try_from_fail_safe_default(value, options, now) {
            Ok(entry) => entry,
            Err(error) => panic!("invalid fail-safe entry configuration: {error}"),
        }
    }

    /// Builds a valid fail-safe default with typed configuration rejection.
    pub fn try_from_fail_safe_default(
        value: V,
        options: &EntryOptions,
        now: Timestamp,
    ) -> Result<Self> {
        let size = options.size()?;
        let throttle = options.fail_safe_throttle_duration();
        let physical_expiration = now.saturating_add(options.physical_ttl());
        let meta = Metadata {
            created: now,
            inserted_at: now,
            logical_expiration: now.saturating_add(throttle).min(physical_expiration),
            physical_expiration,
            backend_ttl: options.physical_ttl(),
            origin: EntryOrigin::FailSafe,
            etag: None,
            last_modified: None,
            tags: Box::from([]),
            retention: RetentionMetadata::Specified {
                size,
                priority: options.priority(),
            },
        };
        Ok(Self {
            inner: Arc::new(EntryInner {
                value,
                meta,
                eligibility: Eligibility::Local,
            }),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::time::Timestamp;

    fn opts() -> EntryOptions {
        EntryOptions::new(Duration::from_secs(10)).with_fail_safe(
            true,
            Some(Duration::from_secs(100)),
            Some(Duration::from_secs(5)),
        )
    }

    #[test]
    fn fresh_entry_is_fresh_then_stale() {
        let created = Timestamp::from_ticks(0);
        let e = Entry::fresh(7i32, &opts(), created, Box::from([]), None, None);
        let before = created.saturating_add(Duration::from_secs(5));
        let after = created.saturating_add(Duration::from_secs(15));
        assert_eq!(e.freshness(before), Freshness::Fresh);
        assert_eq!(e.freshness(after), Freshness::Stale);
        // physical boundary = max(10, 100) = 100s
        assert!(!e.is_physically_expired(after));
        assert!(e.is_physically_expired(created.saturating_add(Duration::from_secs(101))));
    }

    #[test]
    fn throttled_keeps_physical_boundary_and_resets_logical() {
        let created = Timestamp::from_ticks(0);
        let e = Entry::fresh(7i32, &opts(), created, Box::from([]), None, None);
        let now = created.saturating_add(Duration::from_secs(20)); // stale, still physical
        let t = Entry::throttled(&e, &opts(), now).expect("still physically alive");
        assert!(t.meta().is_from_fail_safe());
        // logical = now + throttle(5s); fresh again for 5s.
        assert_eq!(
            t.freshness(now.saturating_add(Duration::from_secs(2))),
            Freshness::Fresh
        );
        assert_eq!(
            t.freshness(now.saturating_add(Duration::from_secs(6))),
            Freshness::Stale
        );
    }

    #[test]
    fn throttled_none_when_physically_dead() {
        let created = Timestamp::from_ticks(0);
        let e = Entry::fresh(7i32, &opts(), created, Box::from([]), None, None);
        let now = created.saturating_add(Duration::from_secs(200));
        assert!(Entry::throttled(&e, &opts(), now).is_none());
    }
}
