//! Standalone elapsed deadlines retain fail-safe and replacement semantics.
#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

use std::time::Duration;

use amalgam::{BlockingCache, Cache, EntryOptions, FactoryError};

#[tokio::test]
async fn logical_expire_retains_stale_for_the_factory_but_remove_does_not() {
    let cache = Cache::builder()
        .default_options(EntryOptions::new(Duration::from_secs(60)).with_fail_safe(
            true,
            Some(Duration::from_secs(120)),
            None,
        ))
        .try_build()
        .unwrap();
    cache.set("k", 7_u64).await.unwrap();
    let _ = cache
        .expire("k")
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    let stale = cache
        .get_or_set(
            "k",
            amalgam::source::factory(|ctx| async move {
                assert_eq!(
                    ctx.stale_value(),
                    Some(&7),
                    "contract: logical expire retains stale for the factory but remove does not"
                );
                Err(ctx.fail("source unavailable"))
            }),
        )
        .await
        .unwrap();
    assert_eq!(
        stale, 7,
        "contract: logical expire retains stale for the factory but remove does not"
    );
    let _ = cache
        .remove("k")
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    let value = cache
        .get_or_set(
            "k",
            amalgam::source::factory(|ctx| async move {
                assert_eq!(
                    ctx.stale_value(),
                    None,
                    "contract: logical expire retains stale for the factory but remove does not"
                );
                Ok::<_, FactoryError>(ctx.value(9))
            }),
        )
        .await
        .unwrap();
    assert_eq!(
        value, 9,
        "contract: logical expire retains stale for the factory but remove does not"
    );
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn zero_duration_and_full_range_duration_have_the_same_boundary_in_both_views() {
    let cache = BlockingCache::<u64>::new().unwrap();
    let asynchronous = cache.as_async();
    for duration in [Duration::MAX, Duration::ZERO, Duration::MAX] {
        let _ = asynchronous
            .remove("k")
            .with_receipt()
            .await
            .unwrap()
            .wait()
            .await
            .unwrap();
        asynchronous
            .set("k", 3)
            .options(|_| EntryOptions::new(duration))
            .await
            .unwrap();
        let expected = (!duration.is_zero()).then_some(3);
        assert_eq!(
            asynchronous.try_get("k").await.unwrap(),
            expected,
            "contract: zero duration and full range duration have the same boundary in both views"
        );
        assert_eq!(
            cache.try_get("k").execute().unwrap(),
            expected,
            "contract: zero duration and full range duration have the same boundary in both views"
        );
    }
    asynchronous.shutdown().await.unwrap();
}

#[tokio::test]
async fn physically_expired_stale_is_neither_returned_nor_given_to_the_factory() {
    let cache = Cache::builder()
        .default_options(EntryOptions::new(Duration::from_millis(1)).with_fail_safe(
            true,
            Some(Duration::from_millis(5)),
            None,
        ))
        .try_build()
        .unwrap();
    cache.set("k", 7_u64).await.unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(
        cache.try_get("k").await.unwrap(),
        None,
        "contract: physically expired stale is neither returned nor given to the factory"
    );
    assert!(
        cache
            .get_or_set(
                "k",
                amalgam::source::factory(|ctx| async move {
                    assert_eq!(ctx.stale_value(), None, "contract: physically expired stale is neither returned nor given to the factory");
                    Err(ctx.fail("source unavailable"))
                })
            )
            .await
            .is_err(), "contract: physically expired stale is neither returned nor given to the factory"
    );
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn pinned_eviction_snapshot_does_not_keep_the_replaced_deadline() {
    let cache = Cache::<u64>::new();
    let mut evictions = cache.memory_evictions().subscribe();
    cache
        .set("k", 1)
        .options(|_| EntryOptions::new(Duration::from_secs(60)))
        .await
        .unwrap();
    cache
        .set("k", 2)
        .options(|_| EntryOptions::new(Duration::MAX))
        .await
        .unwrap();
    let old = evictions.try_recv().unwrap();
    assert_eq!(
        *old.value(),
        1,
        "contract: pinned eviction snapshot does not keep the replaced deadline"
    );
    assert_eq!(
        cache.try_get("k").await.unwrap(),
        Some(2),
        "contract: pinned eviction snapshot does not keep the replaced deadline"
    );
    cache
        .set("k", 3)
        .options(|_| EntryOptions::new(Duration::from_millis(5)))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(
        cache.try_get("k").await.unwrap(),
        None,
        "contract: pinned eviction snapshot does not keep the replaced deadline"
    );
    assert_eq!(
        *old.value(),
        1,
        "contract: pinned eviction snapshot does not keep the replaced deadline"
    );
    cache.shutdown().await.unwrap();
}
