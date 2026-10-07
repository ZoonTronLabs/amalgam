use amalgam::{
    Cache, Clock, EntryOptions, InMemoryDistributedCache, JsonSerializer, ManualClock, MaybeValue,
    Tag, TagError,
};
use std::sync::Arc;
use std::time::Duration;

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
        .get_or_set_full(
            "scope:raw",
            |mut ctx| async move {
                assert_eq!(ctx.original_key(), "scope:raw");
                assert_eq!(ctx.key(), "scope:scope:raw");
                assert_eq!(ctx.tags().unwrap(), &*tags(&["call"]));
                assert_eq!(ctx.stale_tags(), None);
                ctx.try_set_tags(["adapted"]).unwrap();
                assert_eq!(ctx.tags().unwrap(), &*tags(&["adapted"]));
                Ok::<_, amalgam::FactoryError>(ctx.value(7))
            },
            None,
            tags(&["call"]),
            MaybeValue::none(),
        )
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
        .get_or_set_full(
            "raw",
            |ctx| async move { Ok::<_, amalgam::FactoryError>(ctx.value(7)) },
            None,
            tags(&["stored"]),
            MaybeValue::none(),
        )
        .await
        .unwrap();
    clock.advance(Duration::from_secs(2));
    let second = build();
    second
        .get_or_set_full(
            "raw",
            |ctx| async move {
                assert_eq!(ctx.original_key(), "raw");
                assert_eq!(ctx.key(), "scope:raw");
                assert_eq!(ctx.tags().unwrap(), &*tags(&["new-call"]));
                assert_eq!(ctx.stale_tags().unwrap(), &*tags(&["stored"]));
                assert_eq!(ctx.stale_value(), Some(&7));
                ctx.not_modified()
            },
            None,
            tags(&["new-call"]),
            MaybeValue::none(),
        )
        .await
        .unwrap();
    // NotModified without an explicit adaptation retains the stale tags.
    clock.advance(Duration::from_millis(1));
    second
        .try_remove_by_tag(Tag::new("stored").unwrap())
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(!second.read("raw", None).await.unwrap().has_value());
    first.shutdown().await.unwrap();
    second.shutdown().await.unwrap();
}

#[tokio::test]
async fn invalid_legacy_tags_are_visible_as_a_typed_error_and_cannot_be_committed() {
    let cache = Cache::<u64>::new();
    let result = cache
        .get_or_set("raw", |mut ctx| async move {
            ctx.set_tags([" "]);
            assert_eq!(ctx.tags(), Err(TagError::Blank));
            Ok::<_, amalgam::FactoryError>(ctx.value(1))
        })
        .await;
    assert!(matches!(result, Err(amalgam::Error::Tag(TagError::Blank))));
    assert!(!cache.read("raw", None).await.unwrap().has_value());
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
                .with_eager_refresh(Some(amalgam::EagerThreshold::new(0.5).unwrap())),
        )
        .try_build()
        .unwrap();
    cache
        .get_or_set_full(
            "scope:raw",
            |ctx| async move { Ok::<_, amalgam::FactoryError>(ctx.value(7)) },
            None,
            tags(&["stored"]),
            MaybeValue::none(),
        )
        .await
        .unwrap();
    clock.advance(Duration::from_secs(6));
    let (done, finished) = tokio::sync::oneshot::channel();
    assert_eq!(
        cache
            .get_or_set_full(
                "scope:raw",
                |ctx| async move {
                    assert_eq!(ctx.original_key(), "scope:raw");
                    assert_eq!(ctx.key(), "scope:scope:raw");
                    assert_eq!(ctx.stale_tags().unwrap(), &*tags(&["stored"]));
                    assert_eq!(ctx.tags().unwrap(), &*tags(&["new-call"]));
                    done.send(()).unwrap();
                    Ok::<_, amalgam::FactoryError>(ctx.value(8))
                },
                None,
                tags(&["new-call"]),
                MaybeValue::none()
            )
            .await
            .unwrap(),
        7
    );
    tokio::time::timeout(Duration::from_secs(2), finished)
        .await
        .unwrap()
        .unwrap();
    cache.flush_pending().await.unwrap();
    assert_eq!(
        cache.read("scope:raw", None).await.unwrap().value(),
        Some(&8)
    );
    cache.shutdown().await.unwrap();
}
