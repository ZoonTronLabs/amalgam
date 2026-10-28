#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

use std::future::{Future, IntoFuture};
use std::result::Result as StdResult;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use amalgam::advanced::{CommitReceipt, MutationReceipt};
use amalgam::provider::ManualClock;
use amalgam::{
    Cache, CancellationSource, EntryOptions, Error, FactoryCancellationReason, FactoryContext,
    FactoryError,
};

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
        amalgam::source::factory(
            move |ctx| async move { Ok::<_, FactoryError>(ctx.value(*capture)) },
        ),
    );
    assert!(
        weak.upgrade().is_some(),
        "contract: an abandoned request releases its capture without invoking the factory"
    );
    assert!(
        ready(cache.try_get("abandoned")).unwrap().is_none(),
        "contract: an abandoned request releases its capture without invoking the factory"
    );
    std::mem::drop(request);
    assert!(
        weak.upgrade().is_none(),
        "contract: an abandoned request releases its capture without invoking the factory"
    );
    assert!(
        ready(cache.try_get("abandoned")).unwrap().is_none(),
        "contract: an abandoned request releases its capture without invoking the factory"
    );
}

#[test]
fn receipts_are_completed_for_inline_factory_and_unchanged_for_a_ready_hit() {
    let cache = Cache::new();
    let created = ready(
        cache
            .get_or_set(
                "receipt",
                amalgam::source::factory(|ctx| async move { Ok::<_, FactoryError>(ctx.value(7)) }),
            )
            .with_receipt(),
    )
    .unwrap();
    assert_eq!(
        created.value, 7,
        "contract: receipts are completed for inline factory and unchanged for a ready hit"
    );
    assert!(
        matches!(
            created.commit,
            CommitReceipt::Mutation(MutationReceipt::Completed(_))
        ),
        "contract: receipts are completed for inline factory and unchanged for a ready hit"
    );
    let existing = ready(
        cache
            .get_or_set::<_, _>(
                "receipt",
                typed_factory(|_| async { panic!("hot factory must not run") }),
            )
            .with_receipt(),
    )
    .unwrap();
    assert_eq!(
        existing.value, 7,
        "contract: receipts are completed for inline factory and unchanged for a ready hit"
    );
    assert!(
        matches!(existing.commit, CommitReceipt::Unchanged),
        "contract: receipts are completed for inline factory and unchanged for a ready hit"
    );
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
            amalgam::source::factory(|ctx| async move { Ok::<_, FactoryError>(ctx.value(7)) }),
        )
        .options(|options| options.with_duration(Duration::from_millis(100)))
        .tags(["profile"])
        .await
        .unwrap();
    assert_eq!(
        value, 7,
        "contract: an option overlay preserves cache fail safe and keeps its defaults immutable"
    );
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
    assert_eq!(
        cache.entry_options().duration(),
        Duration::from_secs(60),
        "contract: an option overlay preserves cache fail safe and keeps its defaults immutable"
    );
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
                Ok::<_, FactoryError>(ctx.value(7))
            }),
        )
        .tags(["valid", " "])
        .await;
    assert!(
        invalid.is_err(),
        "contract: invalid tags and explicit cancellation reject before the factory runs"
    );
    assert!(
        cache.try_get("invalid").await.unwrap().is_none(),
        "contract: invalid tags and explicit cancellation reject before the factory runs"
    );
    let cancellation = CancellationSource::new();
    cancellation.cancel();
    let factory_calls = calls.clone();
    let cancelled = cache
        .get_or_set(
            "cancelled",
            amalgam::source::factory(move |ctx| async move {
                factory_calls.fetch_add(1, Ordering::SeqCst);
                Ok::<_, FactoryError>(ctx.value(7))
            }),
        )
        .cancellation(cancellation.token())
        .await;
    assert!(
        matches!(
            cancelled,
            Err(Error::OperationCancelled {
                reason: FactoryCancellationReason::CallerCancelled
            })
        ),
        "contract: invalid tags and explicit cancellation reject before the factory runs"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "contract: invalid tags and explicit cancellation reject before the factory runs"
    );
    assert!(
        cache.try_get("cancelled").await.unwrap().is_none(),
        "contract: invalid tags and explicit cancellation reject before the factory runs"
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
    assert!(
        weak.upgrade().is_some(),
        "contract: native request keeps captures lazy and runs the factory on the caller"
    );
    std::mem::drop(request);
    assert!(
        weak.upgrade().is_none(),
        "contract: native request keeps captures lazy and runs the factory on the caller"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "contract: native request keeps captures lazy and runs the factory on the caller"
    );
    assert!(
        cache.try_get("abandoned").execute().unwrap().is_none(),
        "contract: native request keeps captures lazy and runs the factory on the caller"
    );

    let caller = std::thread::current().id();
    let created = cache
        .get_or_set(
            "executed",
            amalgam::source::factory(move |_| {
                assert_eq!(std::thread::current().id(), caller, "contract: native request keeps captures lazy and runs the factory on the caller");
                Ok::<_, std::convert::Infallible>(9)
            }),
        )
        .with_receipt()
        .execute()
        .unwrap();
    assert_eq!(
        created.value, 9,
        "contract: native request keeps captures lazy and runs the factory on the caller"
    );
    let amalgam::advanced::BlockingCommitReceipt::Mutation(receipt) = created.commit else {
        panic!("a newly computed value must retain its actual mutation receipt");
    };
    let _ = receipt.wait().unwrap();
    cache.shutdown().unwrap();
}

#[test]
fn native_supplied_value_rejects_invalid_tags_and_cancellation_without_writing() {
    let cache = amalgam::BlockingCache::<Option<u64>>::new().unwrap();
    let invalid = cache
        .get_or_set("invalid", amalgam::source::value(None))
        .tags(["valid", " "])
        .execute();
    assert!(
        matches!(invalid, Err(Error::Tag(_))),
        "contract: native supplied value rejects invalid tags and cancellation without writing"
    );
    assert!(
        cache.try_get("invalid").execute().unwrap().is_none(),
        "contract: native supplied value rejects invalid tags and cancellation without writing"
    );
    let cancellation = CancellationSource::new();
    cancellation.cancel();
    let cancelled = cache
        .get_or_set("cancelled", amalgam::source::value(None))
        .cancellation(cancellation.token())
        .execute();
    assert!(
        matches!(
            cancelled,
            Err(Error::OperationCancelled {
                reason: FactoryCancellationReason::CallerCancelled
            })
        ),
        "contract: native supplied value rejects invalid tags and cancellation without writing"
    );
    assert!(
        cache.try_get("cancelled").execute().unwrap().is_none(),
        "contract: native supplied value rejects invalid tags and cancellation without writing"
    );
    cache.shutdown().unwrap();
}
