//! Default writes carry only their required lifetime facts through commit.
use std::sync::Arc;

use super::{
    Eligibility, Entry, EntryInner, EntryOrigin, FreshPlan, FreshValue, Metadata, RetentionMetadata,
};
use crate::{EntryOptions, Result, Tag, Timestamp};

pub(crate) enum DefaultFreshPlan {
    Plain(PlainPlan),
    Prepared(FreshPlan),
    General,
}
impl DefaultFreshPlan {
    pub(crate) fn for_options(options: &EntryOptions) -> Result<Self> {
        Ok(match FreshPlan::for_options(options)? {
            Some(plan)
                if matches!(plan.retention, RetentionMetadata::Unspecified)
                    && plan.logical == plan.physical =>
            {
                Self::Plain(PlainPlan {
                    lifetime: plan.logical,
                })
            }
            Some(plan) => Self::Prepared(plan),
            None => Self::General,
        })
    }
}
pub(crate) struct PlainPlan {
    lifetime: crate::time::LifetimeSpan,
}
impl PlainPlan {
    pub(crate) fn metadata(&self, now: Timestamp) -> PlainMetadata {
        PlainMetadata {
            created: now,
            expires: self.lifetime.after(now),
        }
    }
    pub(crate) fn prepare<V>(&self, value: V, now: Timestamp, tags: Box<[Tag]>) -> FreshValue<V> {
        self.metadata(now).into_fresh(value, tags)
    }
}
/// This plan is selected only for identical logical and physical lifetimes.
#[derive(Clone, Copy)]
pub(crate) struct PlainMetadata {
    created: Timestamp,
    expires: Timestamp,
}
impl PlainMetadata {
    pub(crate) fn logical(&self) -> Timestamp {
        self.expires
    }
    pub(crate) fn physical(&self) -> Timestamp {
        self.expires
    }
    #[inline]
    fn install(self, meta: &mut Metadata) {
        meta.created = self.created;
        meta.inserted_at = self.created;
        meta.logical_expiration = self.expires;
        meta.physical_expiration = self.expires;
        meta.backend_ttl = self.expires.saturating_duration_since(self.created);
        meta.origin = EntryOrigin::Fresh {
            eager_refresh_at: None,
        };
        meta.etag = None;
        meta.last_modified = None;
        meta.tags = Box::new([]);
        meta.retention = RetentionMetadata::Unspecified;
    }
    pub(crate) fn into_fresh<V>(self, value: V, tags: Box<[Tag]>) -> FreshValue<V> {
        FreshValue(EntryInner {
            value,
            meta: Metadata {
                created: self.created,
                inserted_at: self.created,
                logical_expiration: self.expires,
                physical_expiration: self.expires,
                backend_ttl: self.expires.saturating_duration_since(self.created),
                origin: EntryOrigin::Fresh {
                    eager_refresh_at: None,
                },
                etag: None,
                last_modified: None,
                tags,
                retention: RetentionMetadata::Unspecified,
            },
            eligibility: Eligibility::Local,
        })
    }
}
pub(crate) enum PlainReplacement<V> {
    Replaced { retired: V },
    Shared { incoming: V },
}
impl<V> Entry<V> {
    pub(crate) fn replace_plain(
        &mut self,
        incoming: V,
        meta: PlainMetadata,
    ) -> PlainReplacement<V> {
        let Some(inner) = Arc::get_mut(&mut self.inner) else {
            return PlainReplacement::Shared { incoming };
        };
        let retired = std::mem::replace(&mut inner.value, incoming);
        meta.install(&mut inner.meta);
        inner.eligibility = Eligibility::Local;
        PlainReplacement::Replaced { retired }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::{EagerThreshold, Priority};

    fn rich_source() -> Entry<u64> {
        let options = EntryOptions::new(Duration::from_secs(10))
            .with_fail_safe(true, Some(Duration::from_secs(100)), None)
            .with_eager_refresh(EagerThreshold::new(0.5))
            .with_priority(Priority::High)
            .with_size(7);
        Entry::try_fresh_at(
            7,
            &options,
            Timestamp::from_ticks(0),
            Timestamp::from_ticks(0),
            Box::new([Tag::new("previous").unwrap()]),
            Some("original-etag".to_owned()),
            Some(Timestamp::from_ticks(0)),
        )
        .unwrap()
    }
    fn next_metadata() -> PlainMetadata {
        let DefaultFreshPlan::Plain(plan) =
            DefaultFreshPlan::for_options(&EntryOptions::new(Duration::from_secs(20))).unwrap()
        else {
            panic!("plain defaults should be specialized")
        };
        plan.metadata(Timestamp::from_ticks(30_000_000))
    }
    #[test]
    fn plain_replacement_resets_all_facts_and_preserves_an_owned_metadata_snapshot() {
        let mut entry = rich_source();
        let original = entry.meta().clone();
        assert!(
            matches!(
                entry.replace_plain(9, next_metadata()),
                PlainReplacement::Replaced { retired: 7 }
            ),
            "contract: plain replacement resets all facts and preserves an owned metadata snapshot"
        );
        assert_eq!(
            entry.value(),
            &9,
            "contract: plain replacement resets all facts and preserves an owned metadata snapshot"
        );
        assert_eq!(
            entry.meta().created(),
            Timestamp::from_ticks(30_000_000),
            "contract: plain replacement resets all facts and preserves an owned metadata snapshot"
        );
        assert_eq!(
            entry.meta().logical_expiration(),
            Timestamp::from_ticks(230_000_000),
            "contract: plain replacement resets all facts and preserves an owned metadata snapshot"
        );
        assert_eq!(
            entry.meta().physical_expiration(),
            Timestamp::from_ticks(230_000_000),
            "contract: plain replacement resets all facts and preserves an owned metadata snapshot"
        );
        assert_eq!(
            entry.backend_ttl(),
            Duration::from_secs(20),
            "contract: plain replacement resets all facts and preserves an owned metadata snapshot"
        );
        assert!(
            entry.meta().tags().is_empty(),
            "contract: plain replacement resets all facts and preserves an owned metadata snapshot"
        );
        assert!(
            entry.meta().etag().is_none(),
            "contract: plain replacement resets all facts and preserves an owned metadata snapshot"
        );
        assert!(
            entry.meta().last_modified().is_none(),
            "contract: plain replacement resets all facts and preserves an owned metadata snapshot"
        );
        assert!(
            entry.meta().eager_refresh_at().is_none(),
            "contract: plain replacement resets all facts and preserves an owned metadata snapshot"
        );
        assert_eq!(
            entry.meta().priority(),
            Priority::Normal,
            "contract: plain replacement resets all facts and preserves an owned metadata snapshot"
        );
        assert!(
            entry.meta().stored_priority().is_none(),
            "contract: plain replacement resets all facts and preserves an owned metadata snapshot"
        );
        assert!(
            entry.meta().size().is_none(),
            "contract: plain replacement resets all facts and preserves an owned metadata snapshot"
        );
        assert_eq!(
            original.etag(),
            Some("original-etag"),
            "contract: plain replacement resets all facts and preserves an owned metadata snapshot"
        );
        assert_eq!(
            original.tags()[0].as_str(),
            "previous",
            "contract: plain replacement resets all facts and preserves an owned metadata snapshot"
        );
        assert_eq!(
            original.priority(),
            Priority::High,
            "contract: plain replacement resets all facts and preserves an owned metadata snapshot"
        );
        assert_eq!(
            original.size().unwrap().units(),
            7,
            "contract: plain replacement resets all facts and preserves an owned metadata snapshot"
        );
        assert_eq!(
            original.created(),
            Timestamp::from_ticks(0),
            "contract: plain replacement resets all facts and preserves an owned metadata snapshot"
        );
        assert!(
            original.eager_refresh_at().is_some(),
            "contract: plain replacement resets all facts and preserves an owned metadata snapshot"
        );
    }
    #[test]
    fn shared_representation_refuses_reuse_without_consuming_or_changing_either_value() {
        let mut entry = rich_source();
        let retained = entry.clone();
        assert!(
            matches!(
                entry.replace_plain(9, next_metadata()),
                PlainReplacement::Shared { incoming: 9 }
            ),
            "contract: shared representation refuses reuse without consuming or changing either value"
        );
        assert!(
            entry.is_same_instance(&retained),
            "contract: shared representation refuses reuse without consuming or changing either value"
        );
        assert_eq!(
            entry.value(),
            &7,
            "contract: shared representation refuses reuse without consuming or changing either value"
        );
        assert_eq!(
            retained.meta().etag(),
            Some("original-etag"),
            "contract: shared representation refuses reuse without consuming or changing either value"
        );
        assert_eq!(
            entry.meta().tags(),
            retained.meta().tags(),
            "contract: shared representation refuses reuse without consuming or changing either value"
        );
        assert_eq!(
            retained.meta().created(),
            Timestamp::from_ticks(0),
            "contract: shared representation refuses reuse without consuming or changing either value"
        );
    }
}
