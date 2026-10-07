//! API 0.4 invalidation requests remain lazy, typed and atomic on bad tag input.
use amalgam::{
    BlockingCache, Cache, CancellationSource, ClearMode, EntryOptions, Error,
    FactoryCancellationReason, ManualClock, MutationReceipt,
};
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn unpolled_mutations_do_nothing_and_receipts_are_explicit() {
    let cache = Cache::<u64>::new();
    cache.set("key", 7).await.unwrap();
    let request = cache.remove("key");
    assert_eq!(cache.read("key", None).await.unwrap().value(), Some(&7));
    drop(request);
    assert_eq!(cache.read("key", None).await.unwrap().value(), Some(&7));
    let receipt = cache.remove("key").with_receipt().await.unwrap();
    assert!(matches!(receipt, MutationReceipt::Completed(_)));
    receipt.wait().await.unwrap();
    assert!(!cache.read("key", None).await.unwrap().has_value());
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn invalid_string_tag_rejects_the_entire_batch_before_invalidation() {
    let cache = Cache::<u64>::new();
    cache.set("first", 1).tags(["first-tag"]).await.unwrap();
    cache.set("second", 2).tags(["second-tag"]).await.unwrap();
    assert!(matches!(
        cache
            .remove_by_tag("first-tag")
            .and_tags(["second-tag", " "])
            .await,
        Err(Error::Tag(_))
    ));
    assert_eq!(cache.read("first", None).await.unwrap().value(), Some(&1));
    assert_eq!(cache.read("second", None).await.unwrap().value(), Some(&2));
    cache
        .remove_by_tag("first-tag")
        .and_tags(["second-tag"])
        .await
        .unwrap();
    assert!(!cache.read("first", None).await.unwrap().has_value());
    assert!(!cache.read("second", None).await.unwrap().has_value());
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn expiration_options_overlay_preserves_default_fail_safe() {
    let clock = Arc::new(ManualClock::default());
    let cache = Cache::<u64>::builder()
        .clock(clock.clone())
        .default_options(EntryOptions::new(Duration::from_secs(60)).with_fail_safe(
            true,
            Some(Duration::from_secs(600)),
            None,
        ))
        .try_build()
        .unwrap();
    cache.set("key", 7).await.unwrap();
    clock.advance(Duration::from_secs(1));
    cache
        .expire("key")
        .options(|options| options.with_duration(Duration::from_secs(30)))
        .await
        .unwrap();
    assert_eq!(
        cache
            .get_or_set(
                "key",
                amalgam::source::factory(|ctx| async move {
                    Err::<u64, _>(ctx.fail("origin unavailable"))
                })
            )
            .await
            .unwrap(),
        7
    );
    cache.clear(ClearMode::Remove).await.unwrap();
    assert!(!cache.read("key", None).await.unwrap().has_value());
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancelled_removal_keeps_the_value_and_returns_the_reason() {
    let cache = Cache::<u64>::new();
    cache.set("key", 7).await.unwrap();
    let source = CancellationSource::new();
    source.cancel();
    assert!(matches!(
        cache.remove("key").cancellation(source.token()).await,
        Err(Error::OperationCancelled {
            reason: FactoryCancellationReason::CallerCancelled
        })
    ));
    assert_eq!(cache.read("key", None).await.unwrap().value(), Some(&7));
    cache.shutdown().await.unwrap();
}

#[test]
fn native_mutation_executes_only_when_requested_and_keeps_typed_failures() {
    let cache = BlockingCache::<u64>::new().unwrap();
    cache.set("key", 7).tags(["group"]).execute().unwrap();
    let request = cache.remove("key");
    assert_eq!(cache.read("key", None).unwrap().value(), Some(&7));
    drop(request);
    assert!(matches!(
        cache.remove_by_tag(" ").execute(),
        Err(Error::Tag(_))
    ));
    assert_eq!(cache.read("key", None).unwrap().value(), Some(&7));
    cache
        .remove_by_tag("group")
        .with_receipt()
        .execute()
        .unwrap()
        .wait()
        .unwrap();
    assert!(!cache.read("key", None).unwrap().has_value());
    cache.shutdown().unwrap();
}
