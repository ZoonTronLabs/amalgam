use amalgam::*;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

#[tokio::test]
async fn cached_none_is_a_hit_in_memory_and_l2_with_auto_clone_enabled() {
    let clock = Arc::new(ManualClock::default());
    let backend = Arc::new(InMemoryDistributedCache::new(clock.clone()));
    let build = || {
        Cache::<Option<String>>::builder()
            .clock(clock.clone())
            .distributed(backend.clone())
            .serializer(Arc::new(JsonSerializer))
            .default_options(
                EntryOptions::new(Duration::from_secs(60)).with_enable_auto_clone(true),
            )
            .try_build()
            .unwrap()
    };
    let first = build();
    first
        .try_set("null", None)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert_eq!(first.read("null", None).await.unwrap().value(), Some(&None));
    assert!(!first.read("missing", None).await.unwrap().has_value());
    let second = build();
    let calls = Arc::new(AtomicUsize::new(0));
    let factory_calls = calls.clone();
    assert_eq!(
        second
            .get_or_set("null", move |ctx| async move {
                factory_calls.fetch_add(1, Ordering::SeqCst);
                Ok::<_, amalgam::FactoryError>(ctx.value(Some("origin".to_owned())))
            })
            .await
            .unwrap(),
        None
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        second.read("null", None).await.unwrap().value(),
        Some(&None)
    );
    first.shutdown().await.unwrap();
    second.shutdown().await.unwrap();
}

#[tokio::test]
async fn null_stale_snapshot_supports_not_modified_and_fail_safe() {
    let clock = Arc::new(ManualClock::default());
    let cache = Cache::<Option<String>>::builder()
        .clock(clock.clone())
        .default_options(EntryOptions::new(Duration::from_secs(1)).with_fail_safe(
            true,
            Some(Duration::from_secs(60)),
            Some(Duration::from_secs(1)),
        ))
        .try_build()
        .unwrap();
    cache
        .get_or_set("null", |ctx| async move {
            Ok::<_, amalgam::FactoryError>(ctx.modified(None).etag("null-etag").done())
        })
        .await
        .unwrap();
    clock.advance(Duration::from_secs(2));
    assert_eq!(
        cache
            .get_or_set("null", |ctx| async move {
                assert!(ctx.has_stale_value());
                assert_eq!(ctx.stale_value(), Some(&None));
                assert_eq!(ctx.stale_etag(), Some("null-etag"));
                ctx.not_modified()
            })
            .await
            .unwrap(),
        None
    );
    clock.advance(Duration::from_secs(2));
    assert_eq!(
        cache
            .get_or_set(
                "null",
                |ctx| async move { Err(ctx.fail("expected failure")) }
            )
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        cache
            .read(
                "null",
                Some(EntryOptions::default().with_allow_stale_on_read_only(true))
            )
            .await
            .unwrap()
            .value(),
        Some(&None)
    );
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn present_null_fallback_is_distinct_from_absent_fallback() {
    let cache = Cache::<Option<String>>::new();
    let options = EntryOptions::default().with_fail_safe(true, None, None);
    assert_eq!(
        cache
            .get_or_set_full(
                "null",
                |ctx| async move { Err(ctx.fail("expected failure")) },
                Some(options.clone()),
                Box::from([]),
                MaybeValue::from_value(None)
            )
            .await
            .unwrap(),
        None
    );
    assert!(matches!(
        cache
            .get_or_set_full(
                "absent",
                |ctx| async move { Err(ctx.fail("expected failure")) },
                Some(options),
                Box::from([]),
                MaybeValue::none()
            )
            .await,
        Err(Error::Factory { .. })
    ));
    cache.shutdown().await.unwrap();
}
