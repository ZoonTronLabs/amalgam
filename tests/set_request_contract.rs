//! Canonical set is lazy, fallible and keeps the cache's unedited defaults.
use amalgam::{Cache, EntryOptions, Error, ManualClock, MutationReceipt, TagError};
use std::future::{Future, IntoFuture};
use std::pin::pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

fn ready<T>(future: impl IntoFuture<Output = T>) -> T {
    let mut future = pin!(future.into_future());
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("a ready standalone write needs no runtime"),
    }
}

#[test]
fn request_and_its_unpolled_future_retain_input_without_writing() {
    let cache = Cache::<Arc<u64>>::new();
    for convert in [false, true] {
        let value = Arc::new(42);
        let weak = Arc::downgrade(&value);
        let request = cache.set("lazy", value);
        assert!(weak.upgrade().is_some());
        assert!(!ready(cache.read("lazy", None)).unwrap().has_value());
        if convert {
            let future = request.into_future();
            assert!(weak.upgrade().is_some());
            drop(future);
        } else {
            drop(request);
        }
        assert!(weak.upgrade().is_none());
        assert!(!ready(cache.read("lazy", None)).unwrap().has_value());
    }
}

#[test]
fn plain_write_is_inline_and_receipt_is_an_explicit_completed_choice() {
    let cache = Cache::<u64>::new();
    ready(cache.set("value", 17)).unwrap();
    assert_eq!(ready(cache.read("value", None)).unwrap().value(), Some(&17));
    let receipt = ready(cache.set("value", 19).with_receipt()).unwrap();
    assert!(matches!(receipt, MutationReceipt::Completed(_)));
    assert_eq!(ready(cache.read("value", None)).unwrap().value(), Some(&19));
    cache.close();
    assert!(matches!(
        ready(cache.set("value", 21)),
        Err(Error::CacheClosed)
    ));
}

#[test]
fn a_blank_tag_returns_a_typed_error_and_keeps_the_previous_value() {
    let cache = Cache::<u64>::new();
    ready(cache.set("value", 17)).unwrap();
    assert!(matches!(
        ready(cache.set("value", 19).tags(["valid", " "])),
        Err(Error::Tag(TagError::Blank))
    ));
    assert_eq!(ready(cache.read("value", None)).unwrap().value(), Some(&17));
}

#[test]
fn duration_overlay_preserves_fail_safe_from_cache_defaults() {
    let clock = Arc::new(ManualClock::default());
    let defaults = EntryOptions::new(Duration::from_secs(60)).with_fail_safe(
        true,
        Some(Duration::from_secs(100)),
        Some(Duration::from_secs(5)),
    );
    let cache = Cache::<u64>::builder()
        .clock(clock.clone())
        .default_options(defaults)
        .try_build()
        .unwrap();
    ready(
        cache
            .set("value", 17)
            .options(|options| options.with_duration(Duration::from_secs(1))),
    )
    .unwrap();
    clock.advance(Duration::from_secs(2));
    assert!(!ready(cache.read("value", None)).unwrap().has_value());
    let fallback = ready(cache.get_or_set("value", |context| {
        std::future::ready(Err(context.fail("source unavailable")))
    }))
    .unwrap();
    assert_eq!(
        fallback, 17,
        "changing duration must not reset fail-safe retention"
    );
}
