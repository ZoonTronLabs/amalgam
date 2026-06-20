//! `basic` — the smallest useful `amalgam` program.
//!
//! Shows the core promise of `get_or_set`: a value is produced by a *factory*
//! the first time it is asked for, then served straight from the in-memory (L1)
//! cache on every subsequent call. We prove the factory ran only once with an
//! atomic counter.
//!
//! Run it with:
//!
//! ```text
//! cargo run --example basic
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use amalgam::Cache;

#[tokio::main]
async fn main() {
    // A cache of String values, all defaults (L1 only, no fail-safe).
    let cache: Cache<String> = Cache::new();

    // Counts how many times the (expensive) factory actually runs.
    let factory_runs = Arc::new(AtomicUsize::new(0));

    // First call: the key is missing, so the factory runs and its result is cached.
    let first = {
        let runs = factory_runs.clone();
        cache
            .get_or_set("greeting", move |ctx| async move {
                runs.fetch_add(1, Ordering::SeqCst);
                println!("  [factory] running — computing the value the slow way…");
                // Imagine an HTTP call or a DB query here.
                Ok(ctx.value("Hello from the factory".to_owned()))
            })
            .await
            .expect("factory succeeded")
    };
    println!("1st get_or_set => {first:?}");

    // Second call for the *same* key: a cache HIT. The factory does NOT run again.
    let second = {
        let runs = factory_runs.clone();
        cache
            .get_or_set("greeting", move |ctx| async move {
                // This body should never execute — the value is already cached.
                runs.fetch_add(1, Ordering::SeqCst);
                Ok(ctx.value("(this should not appear)".to_owned()))
            })
            .await
            .expect("served from cache")
    };
    println!("2nd get_or_set => {second:?}  (served from L1, no factory call)");

    let runs = factory_runs.load(Ordering::SeqCst);
    println!("\nfactory ran {runs} time(s) — expected exactly 1");
    assert_eq!(runs, 1, "the second call must be a cache hit");
    assert_eq!(first, second);
    println!("OK: second call was a cache hit.");
}
