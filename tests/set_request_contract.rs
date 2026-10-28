//! Canonical set is lazy, fallible and keeps the cache's unedited defaults.
#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

use std::future::{Future, IntoFuture};
use std::pin::pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use amalgam::advanced::MutationReceipt;
use amalgam::provider::ManualClock;
use amalgam::{Cache, EntryOptions, Error, TagError};

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
        assert!(
            weak.upgrade().is_some(),
            "contract: request and its unpolled future retain input without writing"
        );
        assert!(
            ready(cache.try_get("lazy")).unwrap().is_none(),
            "contract: request and its unpolled future retain input without writing"
        );
        if convert {
            let future = request.into_future();
            assert!(
                weak.upgrade().is_some(),
                "contract: request and its unpolled future retain input without writing"
            );
            std::mem::drop(future);
        } else {
            std::mem::drop(request);
        }
        assert!(
            weak.upgrade().is_none(),
            "contract: request and its unpolled future retain input without writing"
        );
        assert!(
            ready(cache.try_get("lazy")).unwrap().is_none(),
            "contract: request and its unpolled future retain input without writing"
        );
    }
}

#[test]
fn plain_write_is_inline_and_receipt_is_an_explicit_completed_choice() {
    let cache = Cache::<u64>::new();
    ready(cache.set("value", 17)).unwrap();
    assert_eq!(
        ready(cache.try_get("value")).unwrap().as_ref(),
        Some(&17),
        "contract: plain write is inline and receipt is an explicit completed choice"
    );
    let receipt = ready(cache.set("value", 19).with_receipt()).unwrap();
    assert!(
        matches!(receipt, MutationReceipt::Completed(_)),
        "contract: plain write is inline and receipt is an explicit completed choice"
    );
    assert_eq!(
        ready(cache.try_get("value")).unwrap().as_ref(),
        Some(&19),
        "contract: plain write is inline and receipt is an explicit completed choice"
    );
    cache.close();
    assert!(
        matches!(ready(cache.set("value", 21)), Err(Error::CacheClosed)),
        "contract: plain write is inline and receipt is an explicit completed choice"
    );
}

#[test]
fn a_blank_tag_returns_a_typed_error_and_keeps_the_previous_value() {
    let cache = Cache::<u64>::new();
    ready(cache.set("value", 17)).unwrap();
    assert!(
        matches!(
            ready(cache.set("value", 19).tags(["valid", " "])),
            Err(Error::Tag(TagError::Blank))
        ),
        "contract: a blank tag returns a typed error and keeps the previous value"
    );
    assert_eq!(
        ready(cache.try_get("value")).unwrap().as_ref(),
        Some(&17),
        "contract: a blank tag returns a typed error and keeps the previous value"
    );
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
    assert!(
        ready(cache.try_get("value")).unwrap().is_none(),
        "contract: duration overlay preserves fail safe from cache defaults"
    );
    let fallback = ready(cache.get_or_set(
        "value",
        amalgam::source::factory(|context| {
            std::future::ready(Err(context.fail("source unavailable")))
        }),
    ))
    .unwrap();
    assert_eq!(
        fallback, 17,
        "changing duration must not reset fail-safe retention"
    );
}
