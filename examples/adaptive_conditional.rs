//! `adaptive_conditional` — the factory shapes its own caching.
//!
//! Two related features, both driven from inside the factory via the
//! `FactoryContext`:
//!
//! * **Adaptive caching** — the factory inspects what it produced and *adapts*
//!   this entry's options per call, e.g. cache an "empty"/cheap result for a much
//!   shorter time than a real one. Done with `ctx.adapt(|o| o.with_duration(..))`.
//!
//! * **Conditional refresh** — HTTP-style revalidation. The stale value carries an
//!   `ETag`; on refresh the factory either says "nothing changed"
//!   (`ctx.not_modified()` → the stale value is reused and its expiration bumped)
//!   or returns a new value with a new ETag (`ctx.modified(v).etag(..).done()`).
//!
//! We use a short fail-safe duration so a stale-but-retained value exists for the
//! conditional-refresh step.
//!
//! Run it with:
//!
//! ```text
//! cargo run --example adaptive_conditional
//! ```

use std::time::Duration;

use amalgam::{Cache, EntryOptions, FactoryContext};

#[tokio::main]
async fn main() {
    adaptive_caching().await;
    println!();
    conditional_refresh().await;
}

/// The factory adapts the entry's duration based on the value it produced.
async fn adaptive_caching() {
    println!("== adaptive caching ==");
    let cache: Cache<String> = Cache::new();

    // Baseline duration is generous (10s)…
    let base = EntryOptions::new(Duration::from_secs(10));

    // …but the factory decides this particular (empty) result is cheap and should
    // only live for 100ms, so a real value is fetched again soon.
    let value = cache
        .get_or_set_with(
            "search:zzz",
            |mut ctx: FactoryContext<String>| async move {
                let result = String::new(); // pretend the search returned nothing
                if result.is_empty() {
                    // Adapt: shorten THIS entry's lifetime to 100ms.
                    ctx.adapt(|o| o.with_duration(Duration::from_millis(100)));
                    println!("  [factory] empty result → adapting duration down to 100ms");
                }
                Ok(ctx.value(result))
            },
            base.clone(),
        )
        .await
        .expect("factory runs");
    println!("produced (empty) value, cached for only 100ms: {value:?}");

    // Within 100ms it is still cached (factory not re-run).
    let cached = cache
        .get_or_set_with(
            "search:zzz",
            |ctx| async move {
                println!("  [factory] (should NOT run yet)");
                Ok(ctx.value("late".to_owned()))
            },
            base.clone(),
        )
        .await
        .expect("served from cache");
    println!("immediately after  => served from cache: {cached:?}");

    // After the adapted 100ms window the entry expires and the factory re-runs.
    tokio::time::sleep(Duration::from_millis(150)).await;
    let refreshed = cache
        .get_or_set_with(
            "search:zzz",
            |ctx| async move {
                println!("  [factory] adapted window elapsed → re-running");
                Ok(ctx.value("RESULTS NOW".to_owned()))
            },
            base,
        )
        .await
        .expect("factory re-runs after the adapted window");
    println!("after 150ms        => factory re-ran: {refreshed:?}");
    assert_eq!(refreshed, "RESULTS NOW");
}

/// HTTP-style conditional refresh using ETags.
async fn conditional_refresh() {
    println!("== conditional refresh (ETag) ==");
    let cache: Cache<String> = Cache::new();

    // Short freshness (150ms) but fail-safe retains the stale value so the
    // refresh factory can revalidate it.
    let opts = EntryOptions::new(Duration::from_millis(150)).with_fail_safe(
        true,
        Some(Duration::from_secs(60 * 60)),
        Some(Duration::from_secs(1)),
    );

    // 1. Prime with a value carrying an ETag.
    let primed = cache
        .get_or_set_with(
            "doc",
            |ctx| async move {
                Ok(ctx
                    .modified("document v1".to_owned())
                    .etag("etag-v1")
                    .done())
            },
            opts.clone(),
        )
        .await
        .expect("priming succeeds");
    println!("primed with etag-v1 => {primed:?}");

    // 2. Let it go stale so the next call revalidates.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // 3a. Server says "Not Modified": reuse the stale value, bump its expiration.
    let reused = cache
        .get_or_set_with(
            "doc",
            |ctx: FactoryContext<String>| async move {
                // The stale ETag is available to issue a conditional request.
                println!(
                    "  [factory] revalidating with stale etag = {:?}",
                    ctx.stale_etag()
                );
                println!("  [factory] server replied 304 Not Modified");
                ctx.not_modified() // already a Result<FactoryProduct, _>
            },
            opts.clone(),
        )
        .await
        .expect("not_modified reuses the stale value");
    println!("after 304 NotModified => {reused:?}  (stale value reused, expiration bumped)");
    assert_eq!(reused, "document v1");

    // 3b. Let it go stale again, then the server returns a genuinely new version.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let updated = cache
        .get_or_set_with(
            "doc",
            |ctx: FactoryContext<String>| async move {
                println!(
                    "  [factory] revalidating with stale etag = {:?}",
                    ctx.stale_etag()
                );
                println!("  [factory] server replied 200 with a new body");
                Ok(ctx
                    .modified("document v2".to_owned())
                    .etag("etag-v2")
                    .done())
            },
            opts,
        )
        .await
        .expect("modified replaces the value");
    println!("after 200 Modified    => {updated:?}  (new value + new etag cached)");
    assert_eq!(updated, "document v2");
    println!("OK: adaptive duration + conditional (304/200) refresh both demonstrated.");
}
