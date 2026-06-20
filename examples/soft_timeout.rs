//! `soft_timeout` — don't let a slow factory block the caller.
//!
//! A *soft timeout* (which only applies when fail-safe is on AND a stale value
//! exists) caps how long `get_or_set` waits for the factory. If the factory is
//! slower than the soft timeout, the cache returns the **stale value immediately**
//! and lets the factory keep running in the **background**; when it eventually
//! finishes, it updates the cache for next time. Latency stays low, the value
//! still gets refreshed.
//!
//! Timeline (real wall-clock time so the example reads naturally):
//!
//! 1. prime a value (200ms freshness),
//! 2. sleep past it → stale,
//! 3. call again with a *slow* (300ms) factory but a 50ms soft timeout:
//!    the call returns the stale value in ~50ms,
//! 4. wait for the background factory to finish and see the cache hold the new value.
//!
//! Run it with:
//!
//! ```text
//! cargo run --example soft_timeout
//! ```

use std::time::{Duration, Instant};

use amalgam::{Cache, EntryOptions, Timeout};

#[tokio::main]
async fn main() {
    let cache: Cache<String> = Cache::new();

    // Fail-safe ON (required for soft timeouts) + a 50ms soft timeout.
    // `Timeout::Infinite` hard timeout means we never *abort* the factory — we
    // only stop *waiting* for it on the hot path. `true` lets a timed-out factory
    // finish in the background and update the cache.
    let opts = EntryOptions::new(Duration::from_millis(200))
        .with_fail_safe(
            true,
            Some(Duration::from_secs(60 * 60)),
            Some(Duration::from_secs(1)),
        )
        .with_factory_timeouts(
            Timeout::After(Duration::from_millis(50)), // soft
            Timeout::Infinite,                         // hard (never abort)
            true,                                      // allow background completion
        );

    // 1. Prime with v1.
    cache
        .get_or_set_with(
            "feed",
            |ctx| async move { Ok(ctx.value("v1".to_owned())) },
            opts.clone(),
        )
        .await
        .expect("priming succeeds");
    println!("primed value => \"v1\"");

    // 2. Go stale.
    tokio::time::sleep(Duration::from_millis(250)).await;
    println!("(slept 250ms — \"feed\" is now stale)");

    // 3. Slow factory (300ms) vs 50ms soft timeout: returns stale "v1" fast.
    let started = Instant::now();
    let served = cache
        .get_or_set_with(
            "feed",
            |ctx| async move {
                println!("  [factory] started (will take 300ms)…");
                tokio::time::sleep(Duration::from_millis(300)).await;
                println!("  [factory] finished in the background, producing \"v2\"");
                Ok(ctx.value("v2".to_owned()))
            },
            opts,
        )
        .await
        .expect("soft timeout returns the stale value");
    let waited = started.elapsed();
    println!(
        "served after ~{}ms => {served:?}  (stale value returned immediately)",
        waited.as_millis()
    );
    assert_eq!(served, "v1", "soft timeout returns the stale value");
    assert!(
        waited < Duration::from_millis(250),
        "the caller should not have waited for the full 300ms factory"
    );

    // 4. Give the background factory time to finish, then read the fresh value.
    tokio::time::sleep(Duration::from_millis(400)).await;
    let now_cached = cache.try_get("feed", None).await;
    println!(
        "cache now holds      => {:?}  (updated by the background factory)",
        now_cached.value()
    );
    assert_eq!(
        now_cached.value(),
        Some(&"v2".to_owned()),
        "the background completion should have updated the cache"
    );
    println!("OK: fast stale response, then background refresh to \"v2\".");
}
