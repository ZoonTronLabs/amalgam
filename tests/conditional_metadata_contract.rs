use amalgam::*;
use std::sync::Arc;
use std::time::Duration;

fn options() -> EntryOptions {
    EntryOptions::new(Duration::from_secs(1)).with_fail_safe(
        true,
        Some(Duration::from_secs(60)),
        None,
    )
}
fn tags(value: &str) -> Box<[Tag]> {
    Box::from([Tag::new(value).unwrap()])
}

#[tokio::test]
async fn conditional_builder_preserves_the_value_and_saved_tags_but_replaces_or_clears_validators()
{
    for clear in [false, true] {
        let clock = Arc::new(ManualClock::default());
        let backend = Arc::new(InMemoryDistributedCache::new(clock.clone()));
        let build = || {
            Cache::<u64>::builder()
                .clock(clock.clone())
                .distributed(backend.clone())
                .serializer(Arc::new(JsonSerializer))
                .default_options(options())
                .try_build()
                .unwrap()
        };
        let first = build();
        first
            .get_or_set_full(
                "key",
                |ctx| async move {
                    Ok::<_, amalgam::FactoryError>(
                        ctx.modified(7)
                            .etag("old")
                            .last_modified(Timestamp::from_ticks(1))
                            .done(),
                    )
                },
                None,
                tags("saved"),
                MaybeValue::none(),
            )
            .await
            .unwrap();
        clock.advance(Duration::from_secs(2));
        let second = build();
        second
            .get_or_set_full(
                "key",
                move |mut ctx| async move {
                    assert_eq!(ctx.tags().unwrap(), &*tags("request"));
                    assert_eq!(ctx.stale_tags().unwrap(), &*tags("saved"));
                    ctx.try_set_tags(["attempted"]).unwrap();
                    let changed = ctx.not_modified_builder()?;
                    Ok(if clear {
                        changed
                            .etag(ValidatorUpdate::Clear)
                            .last_modified(ValidatorUpdate::Clear)
                            .done()
                    } else {
                        changed
                            .etag(ValidatorUpdate::Replace("new".to_owned()))
                            .last_modified(ValidatorUpdate::Replace(Timestamp::from_ticks(2)))
                            .done()
                    })
                },
                None,
                tags("request"),
                MaybeValue::none(),
            )
            .await
            .unwrap();
        clock.advance(Duration::from_secs(2));
        let inspect = build();
        inspect
            .get_or_set("key", move |ctx| async move {
                assert_eq!(ctx.stale_value(), Some(&7));
                assert_eq!(ctx.stale_tags().unwrap(), &*tags("saved"));
                assert_eq!(ctx.stale_etag(), if clear { None } else { Some("new") });
                assert_eq!(
                    ctx.stale_last_modified(),
                    if clear {
                        None
                    } else {
                        Some(Timestamp::from_ticks(2))
                    }
                );
                Ok::<_, amalgam::FactoryError>(ctx.not_modified_builder()?.done())
            })
            .await
            .unwrap();
        first.shutdown().await.unwrap();
        second.shutdown().await.unwrap();
        inspect.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn explicit_conditional_tags_replace_the_saved_tags_after_a_new_revision() {
    let clock = Arc::new(ManualClock::default());
    let cache = Cache::<u64>::builder()
        .clock(clock.clone())
        .default_options(options())
        .try_build()
        .unwrap();
    cache
        .get_or_set_full(
            "key",
            |ctx| async move { Ok::<_, amalgam::FactoryError>(ctx.value(7)) },
            None,
            tags("old"),
            MaybeValue::none(),
        )
        .await
        .unwrap();
    clock.advance(Duration::from_secs(2));
    cache
        .get_or_set("key", |ctx| async move {
            Ok::<_, amalgam::FactoryError>(
                ctx.not_modified_builder()?
                    .validated_tags(tags("new"))
                    .done(),
            )
        })
        .await
        .unwrap();
    clock.advance(Duration::from_millis(1));
    cache
        .try_remove_by_tag(Tag::new("old").unwrap())
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert_eq!(cache.read("key", None).await.unwrap().value(), Some(&7));
    cache
        .try_remove_by_tag(Tag::new("new").unwrap())
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(!cache.read("key", None).await.unwrap().has_value());
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn absent_snapshot_is_typed_and_bad_legacy_tags_cannot_escape_through_the_builder() {
    let clock = Arc::new(ManualClock::default());
    let cache = Cache::<u64>::builder()
        .clock(clock.clone())
        .default_options(options())
        .try_build()
        .unwrap();
    let no_source = cache
        .get_or_set("absent", |ctx| async move {
            let error = ctx.not_modified_builder().unwrap_err();
            assert_eq!(error, ConditionalRefreshError::NoStaleSnapshot);
            Err::<_, amalgam::FactoryError>(error.into())
        })
        .await
        .unwrap_err();
    assert!(
        matches!(no_source, Error::FactoryWithSource { .. }),
        "{no_source:?}"
    );
    cache
        .get_or_set("key", |ctx| async move {
            Ok::<_, amalgam::FactoryError>(ctx.value(7))
        })
        .await
        .unwrap();
    clock.advance(Duration::from_secs(2));
    let bad_tags = cache
        .get_or_set("key", |mut ctx| async move {
            ctx.set_tags([" "]);
            Ok::<_, amalgam::FactoryError>(ctx.not_modified_builder()?.done())
        })
        .await;
    assert!(matches!(bad_tags, Err(Error::Tag(TagError::Blank))));
    cache.shutdown().await.unwrap();
}
