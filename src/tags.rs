//! Tagging: [`Tag`] values and the lazy [`TagRegistry`] that powers
//! [`Cache::remove_by_tag`](crate::Cache::remove_by_tag) and
//! [`Cache::clear`](crate::Cache::clear).
//!
//! FusionCache implements "remove by tag" *lazily*: `RemoveByTag` does not scan
//! and touch matching entries. Instead it records a per-tag timestamp marker;
//! when an entry is later read, it is considered invalid if it was created at or
//! before the marker for any of its tags. This crate keeps the same semantics
//! but stores the markers in a dedicated, strongly-typed structure rather than
//! smuggling them through the value cache as magic `__fc:t:*` keys.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use dashmap::DashMap;

use crate::options::RemoveByTagBehavior;
use crate::time::Timestamp;

/// A non-blank cache tag.
///
/// Tags are validated at construction (FusionCache rejects blank tags), so an
/// invalid tag is unrepresentable downstream.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Tag(Arc<str>);

impl Tag {
    /// Creates a tag, or `None` if the input is empty or only whitespace.
    #[must_use]
    pub fn new(tag: impl AsRef<str>) -> Option<Self> {
        let s = tag.as_ref();
        if s.trim().is_empty() {
            None
        } else {
            Some(Self(Arc::from(s)))
        }
    }

    /// The tag as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for Tag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Collects valid tags from string-like inputs, silently dropping blanks.
#[must_use]
pub fn collect_tags<I, S>(tags: I) -> Box<[Tag]>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    tags.into_iter().filter_map(Tag::new).collect()
}

/// The verdict of a tag/clear-marker check against a single entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TagVerdict {
    /// No marker invalidates the entry.
    Valid,
    /// The entry should be treated as logically expired (fail-safe may serve it).
    Expire,
    /// The entry should be hard-removed.
    Remove,
}

const NEVER: i64 = i64::MIN;

/// A lazy registry of tag and clear markers, shared across the cache.
#[derive(Debug)]
pub struct TagRegistry {
    /// tag → the tick at which entries with this tag became invalid.
    markers: DashMap<Tag, i64>,
    /// "expire everything created at/before this tick".
    clear_expire: AtomicI64,
    /// "remove everything created at/before this tick".
    clear_remove: AtomicI64,
}

impl Default for TagRegistry {
    fn default() -> Self {
        Self {
            markers: DashMap::new(),
            clear_expire: AtomicI64::new(NEVER),
            clear_remove: AtomicI64::new(NEVER),
        }
    }
}

impl TagRegistry {
    /// Creates an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records that the given tag was invalidated at `at` (keeping the latest of
    /// any existing marker).
    pub fn mark_tag(&self, tag: Tag, at: Timestamp) {
        self.markers
            .entry(tag)
            .and_modify(|existing| *existing = (*existing).max(at.ticks()))
            .or_insert_with(|| at.ticks());
    }

    /// Returns the marker timestamp for a tag, if any.
    #[must_use]
    pub fn tag_marker(&self, tag: &Tag) -> Option<Timestamp> {
        self.markers.get(tag).map(|v| Timestamp::from_ticks(*v))
    }

    /// Records a "clear (expire)" at `at`: every entry created at/before this
    /// point becomes logically expired.
    pub fn mark_clear_expire(&self, at: Timestamp) {
        bump_max(&self.clear_expire, at.ticks());
    }

    /// Records a "clear (remove)" at `at`: every entry created at/before this
    /// point is hard-removed.
    pub fn mark_clear_remove(&self, at: Timestamp) {
        bump_max(&self.clear_remove, at.ticks());
    }

    /// Evaluates an entry (by its creation timestamp and tags) against all
    /// markers, returning the strongest applicable verdict.
    ///
    /// Order mirrors FusionCache: clear-remove first (hardest), then per-tag
    /// markers (honouring `behavior`), then clear-expire. The comparison is
    /// inclusive (`entry_created <= marker`).
    #[must_use]
    pub fn evaluate(
        &self,
        entry_created: Timestamp,
        tags: &[Tag],
        behavior: RemoveByTagBehavior,
    ) -> TagVerdict {
        let created = entry_created.ticks();

        if created <= self.clear_remove.load(Ordering::Relaxed) {
            return TagVerdict::Remove;
        }

        for tag in tags {
            if let Some(marker) = self.markers.get(tag).map(|m| *m)
                && created <= marker
            {
                return match behavior {
                    RemoveByTagBehavior::Expire => TagVerdict::Expire,
                    RemoveByTagBehavior::Remove => TagVerdict::Remove,
                };
            }
        }

        if created <= self.clear_expire.load(Ordering::Relaxed) {
            return TagVerdict::Expire;
        }

        TagVerdict::Valid
    }
}

fn bump_max(slot: &AtomicI64, value: i64) {
    let mut current = slot.load(Ordering::Relaxed);
    while value > current {
        match slot.compare_exchange_weak(current, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(observed) => current = observed,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tag(s: &str) -> Tag {
        Tag::new(s).unwrap()
    }

    #[test]
    fn blank_tags_rejected() {
        assert!(Tag::new("").is_none());
        assert!(Tag::new("   ").is_none());
        assert!(Tag::new("x").is_some());
    }

    #[test]
    fn entry_before_marker_is_invalid() {
        let reg = TagRegistry::new();
        let created = Timestamp::from_ticks(100);
        reg.mark_tag(tag("a"), Timestamp::from_ticks(200));
        assert_eq!(
            reg.evaluate(created, &[tag("a")], RemoveByTagBehavior::Expire),
            TagVerdict::Expire
        );
        // An entry created after the marker is unaffected.
        let newer = Timestamp::from_ticks(300);
        assert_eq!(
            reg.evaluate(newer, &[tag("a")], RemoveByTagBehavior::Expire),
            TagVerdict::Valid
        );
    }

    #[test]
    fn remove_behavior_dominates() {
        let reg = TagRegistry::new();
        reg.mark_tag(tag("a"), Timestamp::from_ticks(200));
        assert_eq!(
            reg.evaluate(
                Timestamp::from_ticks(100),
                &[tag("a")],
                RemoveByTagBehavior::Remove
            ),
            TagVerdict::Remove
        );
    }

    #[test]
    fn clear_remove_outranks_clear_expire() {
        let reg = TagRegistry::new();
        reg.mark_clear_expire(Timestamp::from_ticks(200));
        reg.mark_clear_remove(Timestamp::from_ticks(200));
        assert_eq!(
            reg.evaluate(Timestamp::from_ticks(100), &[], RemoveByTagBehavior::Expire),
            TagVerdict::Remove
        );
    }
}
