//! The internal cache entry envelope: value plus the metadata that drives
//! freshness, fail-safe and eager-refresh decisions.

use std::sync::Arc;
use std::time::Duration;

use crate::options::EntryOptions;
use crate::tags::Tag;
use crate::time::Timestamp;

/// Whether an entry is still within its logical freshness window.
///
/// An entry physically present in the cache is always one of these two states:
/// beyond the *physical* expiration the backend has already evicted it, so a
/// third "gone" state is unrepresentable here.
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
    is_from_fail_safe: bool,
    /// When to start a proactive background refresh, if eager refresh is enabled.
    eager_refresh_at: Option<Timestamp>,
    /// HTTP-style entity tag, for conditional refresh.
    etag: Option<String>,
    /// HTTP-style last-modified time, for conditional refresh.
    last_modified: Option<Timestamp>,
    /// The tags attached to this entry.
    tags: Box<[Tag]>,
}

impl Metadata {
    /// The creation timestamp.
    #[must_use]
    pub fn created(&self) -> Timestamp {
        self.created
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
        self.is_from_fail_safe
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
        if self.inner.meta.is_from_fail_safe {
            return false;
        }
        match self.inner.meta.eager_refresh_at {
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
        let physical_ttl = options.physical_ttl();
        let physical_expiration = created.saturating_add(physical_ttl);
        let meta = Metadata {
            created,
            logical_expiration: options.logical_expiration(created),
            physical_expiration,
            backend_ttl: physical_ttl,
            is_from_fail_safe: false,
            eager_refresh_at: options.eager_refresh_at(created),
            etag,
            last_modified,
            tags,
        };
        Self {
            inner: Arc::new(EntryInner { value, meta }),
        }
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
        if source.is_physically_expired(now) {
            return None;
        }
        let physical_expiration = source.inner.meta.physical_expiration;
        let backend_ttl = physical_expiration.saturating_duration_since(now);
        let logical_expiration = now.saturating_add(options.fail_safe_throttle_duration());
        let meta = Metadata {
            created: source.inner.meta.created,
            logical_expiration,
            physical_expiration,
            backend_ttl,
            is_from_fail_safe: true,
            eager_refresh_at: None,
            etag: source.inner.meta.etag.clone(),
            last_modified: source.inner.meta.last_modified,
            tags: source.inner.meta.tags.clone(),
        };
        Some(Self {
            inner: Arc::new(EntryInner {
                value: source.inner.value.clone(),
                meta,
            }),
        })
    }

    /// Produces a copy of this entry that is logically expired as of `at`, while
    /// keeping the original physical boundary so fail-safe can still serve it.
    /// Used by [`Cache::expire`](crate::Cache::expire).
    #[must_use]
    pub fn with_logical_expiration(&self, at: Timestamp) -> Self
    where
        V: Clone,
    {
        let mut meta = self.inner.meta.clone();
        meta.logical_expiration = at;
        meta.eager_refresh_at = None;
        meta.backend_ttl = meta.physical_expiration.saturating_duration_since(at);
        Self {
            inner: Arc::new(EntryInner {
                value: self.inner.value.clone(),
                meta,
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
        let meta = Metadata {
            created,
            logical_expiration,
            physical_expiration,
            backend_ttl: physical_expiration.saturating_duration_since(now),
            is_from_fail_safe,
            eager_refresh_at: None,
            etag,
            last_modified,
            tags,
        };
        Self {
            inner: Arc::new(EntryInner { value, meta }),
        }
    }

    /// Builds a fail-safe entry from a default value (the `fail_safe_default`),
    /// throttled like [`throttled`](Self::throttled).
    #[must_use]
    pub fn from_fail_safe_default(value: V, options: &EntryOptions, now: Timestamp) -> Self {
        let throttle = options.fail_safe_throttle_duration();
        let physical_expiration = now.saturating_add(options.physical_ttl());
        let meta = Metadata {
            created: now,
            logical_expiration: now.saturating_add(throttle),
            physical_expiration,
            backend_ttl: options.physical_ttl(),
            is_from_fail_safe: true,
            eager_refresh_at: None,
            etag: None,
            last_modified: None,
            tags: Box::from([]),
        };
        Self {
            inner: Arc::new(EntryInner { value, meta }),
        }
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
