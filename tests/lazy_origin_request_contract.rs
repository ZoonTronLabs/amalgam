use amalgam::{
    Cache, CancellationSource, CommitReceipt, EntryOptions, Error, FactoryCancellationReason,
    ManualClock, MutationReceipt,
};
use std::future::{Future, IntoFuture};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

fn ready<F: IntoFuture>(request: F) -> F::Output {
    let mut future = std::pin::pin!(request.into_future());
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("ready L1 work must finish without a runtime"),
    }
}

#[test]
fn an_abandoned_request_releases_its_capture_without_invoking_the_factory() {
    let cache = Cache::<u64>::new();
    let capture = Arc::new(7);
    let weak = Arc::downgrade(&capture);
    let request = cache.get_or_set(
        "abandoned",
        move |ctx| async move { Ok(ctx.value(*capture)) },
    );
    assert!(weak.upgrade().is_some());
    assert!(!ready(cache.read("abandoned", None)).unwrap().has_value());
    drop(request);
    assert!(weak.upgrade().is_none());
    assert!(!ready(cache.read("abandoned", None)).unwrap().has_value());
}

#[test]
fn receipts_are_completed_for_inline_factory_and_unchanged_for_a_ready_hit() {
    let cache = Cache::new();
    let created = ready(
        cache
            .get_or_set("receipt", |ctx| async move { Ok(ctx.value(7)) })
            .with_receipt(),
    )
    .unwrap();
    assert_eq!(created.value, 7);
    assert!(matches!(
        created.commit,
        CommitReceipt::Mutation(MutationReceipt::Completed(_))
    ));
    let existing = ready(
        cache
            .get_or_set("receipt", |_| async { panic!("hot factory must not run") })
            .with_receipt(),
    )
    .unwrap();
    assert_eq!(existing.value, 7);
    assert!(matches!(existing.commit, CommitReceipt::Unchanged));
}

#[tokio::test]
async fn an_option_overlay_preserves_cache_fail_safe_and_keeps_its_defaults_immutable() {
    let clock = Arc::new(ManualClock::default());
    let cache = Cache::builder()
        .clock(clock.clone())
        .default_options(EntryOptions::new(Duration::from_secs(60)).with_fail_safe(
            true,
            Some(Duration::from_secs(3600)),
            Some(Duration::from_secs(1)),
        ))
        .try_build()
        .unwrap();
    let value = cache
        .get_or_set("overlay", |ctx| async move { Ok(ctx.value(7)) })
        .options(|options| options.with_duration(Duration::from_millis(100)))
        .tags(["profile"])
        .await
        .unwrap();
    assert_eq!(value, 7);
    clock.advance(Duration::from_millis(101));
    let stale = cache
        .get_or_set("overlay", |ctx| async move { Err(ctx.fail("source down")) })
        .fail_safe_default(99)
        .await
        .unwrap();
    assert_eq!(stale, 7, "the overlay must preserve stale retention");
    assert_eq!(cache.entry_options().duration(), Duration::from_secs(60));
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn invalid_tags_and_explicit_cancellation_reject_before_the_factory_runs() {
    let cache = Cache::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let factory_calls = calls.clone();
    let invalid = cache
        .get_or_set("invalid", move |ctx| async move {
            factory_calls.fetch_add(1, Ordering::SeqCst);
            Ok(ctx.value(7))
        })
        .tags(["valid", " "])
        .await;
    assert!(invalid.is_err());
    assert!(!cache.read("invalid", None).await.unwrap().has_value());
    let cancellation = CancellationSource::new();
    cancellation.cancel();
    let factory_calls = calls.clone();
    let cancelled = cache
        .get_or_set("cancelled", move |ctx| async move {
            factory_calls.fetch_add(1, Ordering::SeqCst);
            Ok(ctx.value(7))
        })
        .cancellation(cancellation.token())
        .await;
    assert!(matches!(
        cancelled,
        Err(Error::OperationCancelled {
            reason: FactoryCancellationReason::CallerCancelled
        })
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(!cache.read("cancelled", None).await.unwrap().has_value());
    cache.shutdown().await.unwrap();
}
