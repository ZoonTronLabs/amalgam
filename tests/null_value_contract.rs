#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

use std::future::Future;
use std::result::Result as StdResult;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use amalgam::provider::{InMemoryDistributedCache, JsonSerializer, ManualClock};
use amalgam::{Cache, EntryOptions, Error, FactoryContext, FactoryError};

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
    let _ = first
        .set("null", None)
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert_eq!(
        first.try_get("null").await.unwrap().as_ref(),
        Some(&None),
        "contract: cached none is a hit in memory and l2 with auto clone enabled"
    );
    assert!(
        first.try_get("missing").await.unwrap().is_none(),
        "contract: cached none is a hit in memory and l2 with auto clone enabled"
    );
    let second = build();
    let calls = Arc::new(AtomicUsize::new(0));
    let factory_calls = calls.clone();
    assert_eq!(
        second
            .get_or_set(
                "null",
                amalgam::source::factory(move |ctx| async move {
                    factory_calls.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, FactoryError>(ctx.value(Some("origin".to_owned())))
                })
            )
            .await
            .unwrap(),
        None,
        "contract: cached none is a hit in memory and l2 with auto clone enabled"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "contract: cached none is a hit in memory and l2 with auto clone enabled"
    );
    assert_eq!(
        second.try_get("null").await.unwrap().as_ref(),
        Some(&None),
        "contract: cached none is a hit in memory and l2 with auto clone enabled"
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
        .get_or_set(
            "null",
            amalgam::source::factory(|ctx| async move {
                Ok::<_, FactoryError>(ctx.modified(None).etag("null-etag").done())
            }),
        )
        .await
        .unwrap();
    clock.advance(Duration::from_secs(2));
    assert_eq!(
        cache
            .get_or_set(
                "null",
                amalgam::source::factory(|ctx| async move {
                    assert!(
                        ctx.has_stale_value(),
                        "contract: null stale snapshot supports not modified and fail safe"
                    );
                    assert_eq!(
                        ctx.stale_value(),
                        Some(&None),
                        "contract: null stale snapshot supports not modified and fail safe"
                    );
                    assert_eq!(
                        ctx.stale_etag(),
                        Some("null-etag"),
                        "contract: null stale snapshot supports not modified and fail safe"
                    );
                    ctx.not_modified()
                })
            )
            .await
            .unwrap(),
        None,
        "contract: null stale snapshot supports not modified and fail safe"
    );
    clock.advance(Duration::from_secs(2));
    assert_eq!(
        cache
            .get_or_set(
                "null",
                amalgam::source::factory(|ctx| async move { Err(ctx.fail("expected failure")) })
            )
            .await
            .unwrap(),
        None,
        "contract: null stale snapshot supports not modified and fail safe"
    );
    assert_eq!(
        cache
            .try_get("null")
            .options(|_| EntryOptions::default().with_allow_stale_on_read_only(true))
            .await
            .unwrap()
            .as_ref(),
        Some(&None),
        "contract: null stale snapshot supports not modified and fail safe"
    );
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn present_null_fallback_is_distinct_from_absent_fallback() {
    let cache = Cache::<Option<String>>::new();
    let options = EntryOptions::default().with_fail_safe(true, None, None);
    assert_eq!(
        cache
            .get_or_set(
                "null",
                typed_factory(|ctx| async move { Err(ctx.fail("expected failure")) })
            )
            .options(|_| options.clone())
            .fail_safe_default(Some(None))
            .await
            .unwrap(),
        None,
        "contract: present null fallback is distinct from absent fallback"
    );
    assert!(
        matches!(
            cache
                .get_or_set(
                    "absent",
                    typed_factory(|ctx| async move { Err(ctx.fail("expected failure")) })
                )
                .options(|_| options)
                .await,
            Err(Error::Factory { .. })
        ),
        "contract: present null fallback is distinct from absent fallback"
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
