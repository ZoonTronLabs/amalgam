#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

use std::future::Future;
use std::result::Result as StdResult;
use std::sync::Arc;
use std::time::Duration;

use amalgam::provider::{Clock, InMemoryDistributedCache, JsonSerializer, ManualClock};
use amalgam::{Cache, EagerThreshold, EntryOptions, FactoryContext, FactoryError, Tag, TagError};

fn tags(values: &[&str]) -> Box<[Tag]> {
    values
        .iter()
        .map(|value| Tag::new(value).unwrap())
        .collect()
}

#[tokio::test]
async fn factory_exposes_raw_key_and_current_tags_without_decoding_the_prefix() {
    let cache = Cache::<u64>::builder()
        .key_prefix("scope:")
        .try_build()
        .unwrap();
    cache
        .get_or_set(
            "scope:raw",
            typed_factory(|mut ctx| async move {
                assert_eq!(
                    ctx.original_key(),
                    "scope:raw",
                    "contract: factory exposes raw key and current tags without decoding the prefix"
                );
                assert_eq!(
                    ctx.key(),
                    "scope:scope:raw",
                    "contract: factory exposes raw key and current tags without decoding the prefix"
                );
                assert_eq!(
                    ctx.tags().unwrap(),
                    &*tags(&["call"]),
                    "contract: factory exposes raw key and current tags without decoding the prefix"
                );
                assert_eq!(
                    ctx.stale_tags(),
                    None,
                    "contract: factory exposes raw key and current tags without decoding the prefix"
                );
                ctx.try_set_tags(["adapted"]).unwrap();
                assert_eq!(
                    ctx.tags().unwrap(),
                    &*tags(&["adapted"]),
                    "contract: factory exposes raw key and current tags without decoding the prefix"
                );
                Ok::<_, FactoryError>(ctx.value(7))
            }),
        )
        .tags(tags(&["call"]))
        .await
        .unwrap();
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn hydrated_stale_tags_remain_distinct_from_current_call_tags() {
    let clock = Arc::new(ManualClock::default());
    let l2 = Arc::new(InMemoryDistributedCache::new(clock.clone()));
    let options = EntryOptions::new(Duration::from_secs(1)).with_fail_safe(
        true,
        Some(Duration::from_secs(60)),
        None,
    );
    let build = || {
        Cache::<u64>::builder()
            .clock(clock.clone())
            .key_prefix("scope:")
            .distributed(l2.clone())
            .serializer(Arc::new(JsonSerializer))
            .default_options(options.clone())
            .try_build()
            .unwrap()
    };
    let first = build();
    first
        .get_or_set(
            "raw",
            typed_factory(|ctx| async move { Ok::<_, FactoryError>(ctx.value(7)) }),
        )
        .tags(tags(&["stored"]))
        .await
        .unwrap();
    clock.advance(Duration::from_secs(2));
    let second = build();
    second
        .get_or_set(
            "raw",
            typed_factory(|ctx| async move {
                assert_eq!(
                    ctx.original_key(),
                    "raw",
                    "contract: hydrated stale tags remain distinct from current call tags"
                );
                assert_eq!(
                    ctx.key(),
                    "scope:raw",
                    "contract: hydrated stale tags remain distinct from current call tags"
                );
                assert_eq!(
                    ctx.tags().unwrap(),
                    &*tags(&["new-call"]),
                    "contract: hydrated stale tags remain distinct from current call tags"
                );
                assert_eq!(
                    ctx.stale_tags().unwrap(),
                    &*tags(&["stored"]),
                    "contract: hydrated stale tags remain distinct from current call tags"
                );
                assert_eq!(
                    ctx.stale_value(),
                    Some(&7),
                    "contract: hydrated stale tags remain distinct from current call tags"
                );
                ctx.not_modified()
            }),
        )
        .tags(tags(&["new-call"]))
        .await
        .unwrap();
    // NotModified without an explicit adaptation retains the stale tags.
    clock.advance(Duration::from_millis(1));
    let _ = second
        .remove_by_tag(Tag::new("stored").unwrap())
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(
        second.try_get("raw").await.unwrap().is_none(),
        "contract: hydrated stale tags remain distinct from current call tags"
    );
    first.shutdown().await.unwrap();
    second.shutdown().await.unwrap();
}

#[tokio::test]
async fn invalid_legacy_tags_are_visible_as_a_typed_error_and_cannot_be_committed() {
    let cache = Cache::<u64>::new();
    let result = cache
        .get_or_set(
            "raw",
            amalgam::source::factory(|mut ctx| async move {
                ctx.set_tags([" "]);
                assert_eq!(ctx.tags(), Err(TagError::Blank), "contract: invalid legacy tags are visible as a typed error and cannot be committed");
                Ok::<_, FactoryError>(ctx.value(1))
            }),
        )
        .await;
    assert!(
        matches!(result, Err(amalgam::Error::Tag(TagError::Blank))),
        "contract: invalid legacy tags are visible as a typed error and cannot be committed"
    );
    assert!(
        cache.try_get("raw").await.unwrap().is_none(),
        "contract: invalid legacy tags are visible as a typed error and cannot be committed"
    );
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn eager_factory_preserves_the_original_prefixed_key_and_snapshot_tags() {
    let clock = Arc::new(ManualClock::default());
    let dyn_clock: Arc<dyn Clock> = clock.clone();
    let cache = Cache::<u64>::builder()
        .clock(dyn_clock)
        .key_prefix("scope:")
        .default_options(
            EntryOptions::new(Duration::from_secs(10))
                .with_eager_refresh(Some(EagerThreshold::new(0.5).unwrap())),
        )
        .try_build()
        .unwrap();
    cache
        .get_or_set(
            "scope:raw",
            typed_factory(|ctx| async move { Ok::<_, FactoryError>(ctx.value(7)) }),
        )
        .tags(tags(&["stored"]))
        .await
        .unwrap();
    clock.advance(Duration::from_secs(6));
    let (done, finished) = tokio::sync::oneshot::channel();
    assert_eq!(
        cache
            .get_or_set(
                "scope:raw",
                typed_factory(|ctx| async move {
                    assert_eq!(ctx.original_key(), "scope:raw", "contract: eager factory preserves the original prefixed key and snapshot tags");
                    assert_eq!(ctx.key(), "scope:scope:raw", "contract: eager factory preserves the original prefixed key and snapshot tags");
                    assert_eq!(ctx.stale_tags().unwrap(), &*tags(&["stored"]), "contract: eager factory preserves the original prefixed key and snapshot tags");
                    assert_eq!(ctx.tags().unwrap(), &*tags(&["new-call"]), "contract: eager factory preserves the original prefixed key and snapshot tags");
                    done.send(()).unwrap();
                    Ok::<_, FactoryError>(ctx.value(8))
                })
            )
            .tags(tags(&["new-call"]))
            .await
            .unwrap(),
        7, "contract: eager factory preserves the original prefixed key and snapshot tags"
    );
    tokio::time::timeout(Duration::from_secs(2), finished)
        .await
        .unwrap()
        .unwrap();
    cache.flush_pending().await.unwrap();
    assert_eq!(
        cache.try_get("scope:raw").await.unwrap().as_ref(),
        Some(&8),
        "contract: eager factory preserves the original prefixed key and snapshot tags"
    );
    cache.shutdown().await.unwrap();
}

fn typed_factory<V, F, Fut>(factory: F) -> F
where
    F: FnOnce(FactoryContext<V>) -> Fut,
    Fut: Future<Output = StdResult<V, FactoryError>>,
{
    factory
}
