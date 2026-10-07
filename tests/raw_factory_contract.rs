//! API 0.4 factories return values and retain typed errors/adaptive metadata.
use amalgam::{BlockingCache, Cache, EntryOptions, ManualClock, Tag};
use std::convert::Infallible;
use std::error::Error as _;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, thiserror::Error)]
#[error("original raw factory failure")]
struct OriginError(Arc<()>);

#[tokio::test]
async fn plain_result_and_custom_error_keep_value_and_original_source() {
    let cache = Cache::<u64>::new();
    assert_eq!(
        cache
            .get_or_set("plain", |_| async { Ok::<_, Infallible>(7) })
            .await
            .unwrap(),
        7
    );
    let id = Arc::new(());
    let original = id.clone();
    let error = cache
        .get_or_set("error", move |_| async move {
            Err::<u64, _>(OriginError(original))
        })
        .await
        .unwrap_err();
    assert!(Arc::ptr_eq(
        &id,
        &error
            .source()
            .unwrap()
            .downcast_ref::<amalgam::FactoryError>()
            .unwrap()
            .source()
            .unwrap()
            .downcast_ref::<OriginError>()
            .unwrap()
            .0
    ));
    assert!(!cache.read("error", None).await.unwrap().has_value());
    cache.shutdown().await.unwrap();
}

#[test]
fn blocking_factory_accepts_the_same_plain_value_and_custom_error() {
    let cache = BlockingCache::<u64>::new().unwrap();
    assert_eq!(
        cache
            .get_or_set("plain", |_| Ok::<_, Infallible>(7))
            .unwrap(),
        7
    );
    let id = Arc::new(());
    let original = id.clone();
    let error = cache
        .get_or_set("error", move |_| Err::<u64, _>(OriginError(original)))
        .unwrap_err();
    assert!(Arc::ptr_eq(
        &id,
        &error
            .source()
            .unwrap()
            .downcast_ref::<amalgam::FactoryError>()
            .unwrap()
            .source()
            .unwrap()
            .downcast_ref::<OriginError>()
            .unwrap()
            .0
    ));
    cache.shutdown().unwrap();
}

#[tokio::test]
async fn raw_pending_factory_retains_mutated_options_and_tags() {
    let clock = Arc::new(ManualClock::default());
    let cache = Cache::<u64>::builder()
        .clock(clock.clone())
        .default_options(EntryOptions::new(Duration::from_millis(1)))
        .try_build()
        .unwrap();
    let value = cache
        .get_or_set("adaptive", |mut ctx| async move {
            let options = ctx.options().clone().with_duration(Duration::from_secs(10));
            *ctx.options_mut() = options;
            ctx.try_set_tags(["adapted"]).unwrap();
            tokio::task::yield_now().await;
            Ok::<_, Infallible>(9)
        })
        .await
        .unwrap();
    assert_eq!(value, 9);
    clock.advance(Duration::from_secs(1));
    assert_eq!(
        cache.read("adaptive", None).await.unwrap().value(),
        Some(&9)
    );
    cache
        .remove_by_tag(Tag::new("adapted").unwrap())
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(!cache.read("adaptive", None).await.unwrap().has_value());
    cache.shutdown().await.unwrap();
}
