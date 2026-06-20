//! `failsafe` — serve a *stale* value when the factory fails.
//!
//! Fail-safe is the headline resiliency feature: once a value has been cached,
//! a later factory *failure* does not surface as an error — the last known-good
//! (now logically stale, but still physically retained) value is served instead.
//! This keeps an app up when a backend has a hiccup.
//!
//! We use real (short) wall-clock durations plus `tokio::time::sleep` so the
//! example reads like a real timeline:
//!
//! 1. prime the cache with a 200ms freshness window,
//! 2. sleep past it so the entry goes stale,
//! 3. call again with a *failing* factory — fail-safe returns the stale value.
//!
//! We also subscribe to `cache.events()` and print the `FailSafeActivate` event.
//!
//! Run it with:
//!
//! ```text
//! cargo run --example failsafe
//! ```

use std::time::Duration;

use amalgam::{Cache, CacheEvent, EntryOptions};

#[tokio::main]
async fn main() {
    let cache: Cache<String> = Cache::new();

    // Subscribe to the event stream BEFORE doing any work, so we don't miss
    // the FailSafeActivate event. Events are a broadcast channel.
    let mut events = cache.events().subscribe();

    // Fresh for 200ms, but fail-safe keeps the value physically alive for up to
    // 1 hour so it can be reused as a stale fallback. Throttle: after a fail-safe
    // activation, keep serving stale for 1s before retrying the factory.
    let opts = EntryOptions::new(Duration::from_millis(200)).with_fail_safe(
        true,
        Some(Duration::from_secs(60 * 60)),
        Some(Duration::from_secs(1)),
    );

    // 1. Prime the cache with a good value.
    let primed = cache
        .get_or_set_with(
            "report",
            |ctx| async move { Ok(ctx.value("GOOD report data".to_owned())) },
            opts.clone(),
        )
        .await
        .expect("priming succeeds");
    println!("primed value          => {primed:?}");

    // 2. Let the freshness window elapse: the entry is now logically stale.
    tokio::time::sleep(Duration::from_millis(250)).await;
    println!("(slept 250ms — the entry is now stale)");

    // 3. The backend is now down: the factory returns Err(ctx.fail(..)).
    //    Because fail-safe is enabled and a stale value exists, the cache serves
    //    the stale value instead of propagating the error.
    let served = cache
        .get_or_set_with(
            "report",
            |ctx| async move {
                println!("  [factory] backend is down — returning an error");
                Err(ctx.fail("backend timeout"))
            },
            opts,
        )
        .await
        .expect("fail-safe serves stale instead of erroring");
    println!("served after failure  => {served:?}  (stale value reused)");

    assert_eq!(
        served, "GOOD report data",
        "fail-safe must serve the stale value"
    );

    // Drain the buffered events and report the fail-safe activation.
    let mut saw_fail_safe = false;
    while let Ok(event) = events.try_recv() {
        if let CacheEvent::FailSafeActivate { key } = event {
            println!("event: FailSafeActivate {{ key: {key:?} }}");
            saw_fail_safe = true;
        }
    }
    assert!(
        saw_fail_safe,
        "a FailSafeActivate event should have been emitted"
    );
    println!("OK: stale value served and FailSafeActivate observed.");
}
