//! Standalone elapsed deadlines retain fail-safe and replacement semantics.
use amalgam::{BlockingCache, Cache, EntryOptions};
use std::time::Duration;

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
    cache.try_expire("k").await.unwrap().wait().await.unwrap();
    let stale = cache
        .get_or_set("k", |ctx| async move {
            assert_eq!(ctx.stale_value(), Some(&7));
            Err(ctx.fail("source unavailable"))
        })
        .await
        .unwrap();
    assert_eq!(stale, 7);
    cache.try_remove("k").await.unwrap().wait().await.unwrap();
    let value = cache
        .get_or_set("k", |ctx| async move {
            assert_eq!(ctx.stale_value(), None);
            Ok(ctx.value(9))
        })
        .await
        .unwrap();
    assert_eq!(value, 9);
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn zero_duration_and_full_range_duration_have_the_same_boundary_in_both_views() {
    let cache = BlockingCache::<u64>::new().unwrap();
    let asynchronous = cache.as_async();
    for duration in [Duration::MAX, Duration::ZERO, Duration::MAX] {
        asynchronous
            .try_remove("k")
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
            asynchronous.read("k", None).await.unwrap().into_value(),
            expected
        );
        assert_eq!(cache.read("k", None).unwrap().into_value(), expected);
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
    assert_eq!(cache.read("k", None).await.unwrap().into_value(), None);
    assert!(
        cache
            .get_or_set("k", |ctx| async move {
                assert_eq!(ctx.stale_value(), None);
                Err(ctx.fail("source unavailable"))
            })
            .await
            .is_err()
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
    assert_eq!(*old.value(), 1);
    assert_eq!(cache.read("k", None).await.unwrap().into_value(), Some(2));
    cache
        .set("k", 3)
        .options(|_| EntryOptions::new(Duration::from_millis(5)))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(cache.read("k", None).await.unwrap().into_value(), None);
    assert_eq!(*old.value(), 1);
    cache.shutdown().await.unwrap();
}
