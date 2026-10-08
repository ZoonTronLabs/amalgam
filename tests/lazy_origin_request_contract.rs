use amalgam::{
    Cache, CancellationSource, EntryOptions, Error, FactoryCancellationReason,
    advanced::CommitReceipt, advanced::MutationReceipt, provider::ManualClock,
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
        amalgam::source::factory(move |ctx| async move {
            Ok::<_, amalgam::FactoryError>(ctx.value(*capture))
        }),
    );
    assert!(weak.upgrade().is_some());
    assert!(ready(cache.try_get("abandoned")).unwrap().is_none());
    drop(request);
    assert!(weak.upgrade().is_none());
    assert!(ready(cache.try_get("abandoned")).unwrap().is_none());
}

#[test]
fn receipts_are_completed_for_inline_factory_and_unchanged_for_a_ready_hit() {
    let cache = Cache::new();
    let created = ready(
        cache
            .get_or_set(
                "receipt",
                amalgam::source::factory(|ctx| async move {
                    Ok::<_, amalgam::FactoryError>(ctx.value(7))
                }),
            )
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
            .get_or_set::<_, _>(
                "receipt",
                typed_factory(|_| async { panic!("hot factory must not run") }),
            )
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
        .get_or_set(
            "overlay",
            amalgam::source::factory(
                |ctx| async move { Ok::<_, amalgam::FactoryError>(ctx.value(7)) },
            ),
        )
        .options(|options| options.with_duration(Duration::from_millis(100)))
        .tags(["profile"])
        .await
        .unwrap();
    assert_eq!(value, 7);
    clock.advance(Duration::from_millis(101));
    let stale = cache
        .get_or_set(
            "overlay",
            amalgam::source::factory(|ctx| async move { Err(ctx.fail("source down")) }),
        )
        .fail_safe_default(Some(99))
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
        .get_or_set(
            "invalid",
            amalgam::source::factory(move |ctx| async move {
                factory_calls.fetch_add(1, Ordering::SeqCst);
                Ok::<_, amalgam::FactoryError>(ctx.value(7))
            }),
        )
        .tags(["valid", " "])
        .await;
    assert!(invalid.is_err());
    assert!(cache.try_get("invalid").await.unwrap().is_none());
    let cancellation = CancellationSource::new();
    cancellation.cancel();
    let factory_calls = calls.clone();
    let cancelled = cache
        .get_or_set(
            "cancelled",
            amalgam::source::factory(move |ctx| async move {
                factory_calls.fetch_add(1, Ordering::SeqCst);
                Ok::<_, amalgam::FactoryError>(ctx.value(7))
            }),
        )
        .cancellation(cancellation.token())
        .await;
    assert!(matches!(
        cancelled,
        Err(Error::OperationCancelled {
            reason: FactoryCancellationReason::CallerCancelled
        })
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(cache.try_get("cancelled").await.unwrap().is_none());
    cache.shutdown().await.unwrap();
}

fn typed_factory<V, F, Fut>(factory: F) -> F
where
    F: FnOnce(amalgam::FactoryContext<V>) -> Fut,
    Fut: std::future::Future<Output = std::result::Result<V, amalgam::FactoryError>>,
{
    factory
}

#[test]
fn native_request_keeps_captures_lazy_and_runs_the_factory_on_the_caller() {
    let cache = amalgam::BlockingCache::<u64>::new().unwrap();
    let capture = Arc::new(7);
    let weak = Arc::downgrade(&capture);
    let calls = Arc::new(AtomicUsize::new(0));
    let origin_calls = calls.clone();
    let request = cache.get_or_set(
        "abandoned",
        amalgam::source::factory(move |_| {
            origin_calls.fetch_add(1, Ordering::SeqCst);
            Ok::<_, std::convert::Infallible>(*capture)
        }),
    );
    assert!(weak.upgrade().is_some());
    drop(request);
    assert!(weak.upgrade().is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(cache.try_get("abandoned").execute().unwrap().is_none());

    let caller = std::thread::current().id();
    let created = cache
        .get_or_set(
            "executed",
            amalgam::source::factory(move |_| {
                assert_eq!(std::thread::current().id(), caller);
                Ok::<_, std::convert::Infallible>(9)
            }),
        )
        .with_receipt()
        .execute()
        .unwrap();
    assert_eq!(created.value, 9);
    let amalgam::advanced::BlockingCommitReceipt::Mutation(receipt) = created.commit else {
        panic!("a newly computed value must retain its actual mutation receipt");
    };
    receipt.wait().unwrap();
    cache.shutdown().unwrap();
}

#[test]
fn native_supplied_value_rejects_invalid_tags_and_cancellation_without_writing() {
    let cache = amalgam::BlockingCache::<Option<u64>>::new().unwrap();
    let invalid = cache
        .get_or_set("invalid", amalgam::source::value(None))
        .tags(["valid", " "])
        .execute();
    assert!(matches!(invalid, Err(Error::Tag(_))));
    assert!(cache.try_get("invalid").execute().unwrap().is_none());
    let cancellation = CancellationSource::new();
    cancellation.cancel();
    let cancelled = cache
        .get_or_set("cancelled", amalgam::source::value(None))
        .cancellation(cancellation.token())
        .execute();
    assert!(matches!(
        cancelled,
        Err(Error::OperationCancelled {
            reason: FactoryCancellationReason::CallerCancelled
        })
    ));
    assert!(cache.try_get("cancelled").execute().unwrap().is_none());
    cache.shutdown().unwrap();
}
