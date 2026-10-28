//! API 0.4 invalidation requests remain lazy, typed and atomic on bad tag input.
#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

use std::sync::Arc;
use std::time::Duration;

use amalgam::advanced::MutationReceipt;
use amalgam::provider::ManualClock;
use amalgam::{
    BlockingCache, Cache, CancellationSource, ClearMode, EntryOptions, Error,
    FactoryCancellationReason,
};

#[tokio::test]
async fn unpolled_mutations_do_nothing_and_receipts_are_explicit() {
    let cache = Cache::<u64>::new();
    cache.set("key", 7).await.unwrap();
    let request = cache.remove("key");
    assert_eq!(
        cache.try_get("key").await.unwrap().as_ref(),
        Some(&7),
        "contract: unpolled mutations do nothing and receipts are explicit"
    );
    std::mem::drop(request);
    assert_eq!(
        cache.try_get("key").await.unwrap().as_ref(),
        Some(&7),
        "contract: unpolled mutations do nothing and receipts are explicit"
    );
    let receipt = cache.remove("key").with_receipt().await.unwrap();
    assert!(
        matches!(receipt, MutationReceipt::Completed(_)),
        "contract: unpolled mutations do nothing and receipts are explicit"
    );
    let _ = receipt.wait().await.unwrap();
    assert!(
        cache.try_get("key").await.unwrap().is_none(),
        "contract: unpolled mutations do nothing and receipts are explicit"
    );
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn invalid_string_tag_rejects_the_entire_batch_before_invalidation() {
    let cache = Cache::<u64>::new();
    cache.set("first", 1).tags(["first-tag"]).await.unwrap();
    cache.set("second", 2).tags(["second-tag"]).await.unwrap();
    assert!(
        matches!(
            cache
                .remove_by_tag("first-tag")
                .and_tags(["second-tag", " "])
                .await,
            Err(Error::Tag(_))
        ),
        "contract: invalid string tag rejects the entire batch before invalidation"
    );
    assert_eq!(
        cache.try_get("first").await.unwrap().as_ref(),
        Some(&1),
        "contract: invalid string tag rejects the entire batch before invalidation"
    );
    assert_eq!(
        cache.try_get("second").await.unwrap().as_ref(),
        Some(&2),
        "contract: invalid string tag rejects the entire batch before invalidation"
    );
    cache
        .remove_by_tag("first-tag")
        .and_tags(["second-tag"])
        .await
        .unwrap();
    assert!(
        cache.try_get("first").await.unwrap().is_none(),
        "contract: invalid string tag rejects the entire batch before invalidation"
    );
    assert!(
        cache.try_get("second").await.unwrap().is_none(),
        "contract: invalid string tag rejects the entire batch before invalidation"
    );
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
        7,
        "contract: expiration options overlay preserves default fail safe"
    );
    cache.clear(ClearMode::Remove).await.unwrap();
    assert!(
        cache.try_get("key").await.unwrap().is_none(),
        "contract: expiration options overlay preserves default fail safe"
    );
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancelled_removal_keeps_the_value_and_returns_the_reason() {
    let cache = Cache::<u64>::new();
    cache.set("key", 7).await.unwrap();
    let source = CancellationSource::new();
    source.cancel();
    assert!(
        matches!(
            cache.remove("key").cancellation(source.token()).await,
            Err(Error::OperationCancelled {
                reason: FactoryCancellationReason::CallerCancelled
            })
        ),
        "contract: cancelled removal keeps the value and returns the reason"
    );
    assert_eq!(
        cache.try_get("key").await.unwrap().as_ref(),
        Some(&7),
        "contract: cancelled removal keeps the value and returns the reason"
    );
    cache.shutdown().await.unwrap();
}

#[test]
fn native_mutation_executes_only_when_requested_and_keeps_typed_failures() {
    let cache = BlockingCache::<u64>::new().unwrap();
    cache.set("key", 7).tags(["group"]).execute().unwrap();
    let request = cache.remove("key");
    assert_eq!(
        cache.try_get("key").execute().unwrap().as_ref(),
        Some(&7),
        "contract: native mutation executes only when requested and keeps typed failures"
    );
    std::mem::drop(request);
    assert!(
        matches!(cache.remove_by_tag(" ").execute(), Err(Error::Tag(_))),
        "contract: native mutation executes only when requested and keeps typed failures"
    );
    assert_eq!(
        cache.try_get("key").execute().unwrap().as_ref(),
        Some(&7),
        "contract: native mutation executes only when requested and keeps typed failures"
    );
    let _ = cache
        .remove_by_tag("group")
        .with_receipt()
        .execute()
        .unwrap()
        .wait()
        .unwrap();
    assert!(
        cache.try_get("key").execute().unwrap().is_none(),
        "contract: native mutation executes only when requested and keeps typed failures"
    );
    cache.shutdown().unwrap();
}
