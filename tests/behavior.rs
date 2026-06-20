//! End-to-end behavioural tests — the parity oracle.
//!
//! Each test pins one FusionCache behaviour and asserts `amalgam` reproduces it.
//! Expiration is driven by an injected [`ManualClock`] for determinism; factory
//! timeouts use the real tokio timer (they are wall-clock by nature).

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use amalgam::{
    Cache, CacheEvent, Clock, EagerThreshold, EntryOptions, Error, FactoryContext, ManualClock,
    MaybeValue, Tag, Timeout,
};

fn build<V: Clone + Send + Sync + 'static>() -> (Cache<V>, Arc<ManualClock>) {
    let clock = Arc::new(ManualClock::default());
    let dyn_clock: Arc<dyn Clock> = clock.clone();
    (Cache::builder().clock(dyn_clock).build(), clock)
}

fn fail_safe_opts() -> EntryOptions {
    EntryOptions::new(Duration::from_secs(10)).with_fail_safe(
        true,
        Some(Duration::from_secs(100)),
        Some(Duration::from_secs(30)),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stampede_runs_factory_once() {
    let cache: Cache<i32> = Cache::new();
    let calls = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();
    for _ in 0..32 {
        let cache = cache.clone();
        let calls = calls.clone();
        handles.push(tokio::spawn(async move {
            cache
                .get_or_set("hot", move |ctx| async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(30)).await;
                    Ok(ctx.value(42))
                })
                .await
        }));
    }
    for h in handles {
        assert_eq!(h.await.unwrap().unwrap(), 42);
    }
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "factory must run exactly once"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fail_safe_serves_stale_on_factory_error() {
    let (cache, clock) = build::<String>();
    let opts = fail_safe_opts();

    let primed = cache
        .get_or_set_with(
            "k",
            |ctx| async move { Ok(ctx.value("fresh".to_owned())) },
            opts.clone(),
        )
        .await
        .unwrap();
    assert_eq!(primed, "fresh");

    clock.advance(Duration::from_secs(20)); // logically stale, physically alive

    let served = cache
        .get_or_set_with("k", |ctx| async move { Err(ctx.fail("boom")) }, opts)
        .await
        .unwrap();
    assert_eq!(served, "fresh", "stale value reused as fail-safe fallback");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fail_safe_default_used_when_no_stale() {
    let cache: Cache<String> = Cache::new();
    let opts = EntryOptions::new(Duration::from_secs(10)).with_fail_safe(true, None, None);

    let served = cache
        .get_or_set_full(
            "k",
            |ctx| async move { Err(ctx.fail("boom")) },
            Some(opts),
            Box::from([]),
            MaybeValue::from_value("default".to_owned()),
        )
        .await
        .unwrap();
    assert_eq!(served, "default");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn factory_error_propagates_without_fail_safe() {
    let cache: Cache<String> = Cache::new();
    let result = cache
        .get_or_set("k", |ctx| async move { Err(ctx.fail("boom")) })
        .await;
    assert!(matches!(result, Err(Error::Factory { .. })));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn soft_timeout_returns_stale_then_completes_in_background() {
    let (cache, clock) = build::<String>();
    let opts = EntryOptions::new(Duration::from_secs(10))
        .with_fail_safe(
            true,
            Some(Duration::from_secs(100)),
            Some(Duration::from_secs(1)),
        )
        .with_factory_timeouts(
            Timeout::After(Duration::from_millis(50)),
            Timeout::Infinite,
            true,
        );

    cache
        .get_or_set_with(
            "k",
            |ctx| async move { Ok(ctx.value("v1".to_owned())) },
            opts.clone(),
        )
        .await
        .unwrap();
    clock.advance(Duration::from_secs(20)); // stale

    let served = cache
        .get_or_set_with(
            "k",
            |ctx| async move {
                tokio::time::sleep(Duration::from_millis(300)).await;
                Ok(ctx.value("v2".to_owned()))
            },
            opts,
        )
        .await
        .unwrap();
    assert_eq!(served, "v1", "soft timeout returns stale immediately");

    // Background factory completes and replaces the value.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let after = cache.try_get("k", None).await;
    assert_eq!(
        after.value(),
        Some(&"v2".to_owned()),
        "background completion updates cache"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hard_timeout_without_fallback_errors() {
    let cache: Cache<String> = Cache::new();
    let opts = EntryOptions::new(Duration::from_secs(10)).with_factory_timeouts(
        Timeout::Infinite,
        Timeout::After(Duration::from_millis(50)),
        false,
    );
    let result = cache
        .get_or_set_with(
            "k",
            |ctx| async move {
                tokio::time::sleep(Duration::from_millis(300)).await;
                Ok(ctx.value("v".to_owned()))
            },
            opts,
        )
        .await;
    assert!(matches!(result, Err(Error::FactoryTimeout { .. })));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn eager_refresh_triggers_background_update() {
    let (cache, clock) = build::<i32>();
    let threshold = EagerThreshold::new(0.5).unwrap();
    let opts = EntryOptions::new(Duration::from_secs(10)).with_eager_refresh(Some(threshold));
    let calls = Arc::new(AtomicUsize::new(0));

    {
        let calls = calls.clone();
        cache
            .get_or_set_with(
                "k",
                move |ctx| async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(ctx.value(1))
                },
                opts.clone(),
            )
            .await
            .unwrap();
    }

    clock.advance(Duration::from_secs(6)); // past 50% threshold, still fresh

    {
        let calls = calls.clone();
        let served = cache
            .get_or_set_with(
                "k",
                move |ctx| async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(ctx.value(2))
                },
                opts,
            )
            .await
            .unwrap();
        assert_eq!(
            served, 1,
            "still returns the fresh value while refreshing in background"
        );
    }

    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "eager refresh ran the factory in background"
    );
    assert_eq!(cache.try_get("k", None).await.value(), Some(&2));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expired_entry_without_fail_safe_reruns_factory() {
    let (cache, clock) = build::<i32>();
    let opts = EntryOptions::new(Duration::from_secs(10)); // no fail-safe
    let calls = Arc::new(AtomicUsize::new(0));

    {
        let calls = calls.clone();
        cache
            .get_or_set_with(
                "k",
                move |ctx| async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(ctx.value(1))
                },
                opts.clone(),
            )
            .await
            .unwrap();
    }
    clock.advance(Duration::from_secs(11)); // logically expired

    let v = {
        let calls = calls.clone();
        cache
            .get_or_set_with(
                "k",
                move |ctx| async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(ctx.value(2))
                },
                opts,
            )
            .await
            .unwrap()
    };
    assert_eq!(v, 2);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn adaptive_caching_overrides_duration() {
    let (cache, clock) = build::<i32>();
    let long = || EntryOptions::new(Duration::from_secs(100));

    // The factory adapts the produced entry's duration down to 2s.
    cache
        .get_or_set_with(
            "k",
            |mut ctx: FactoryContext<i32>| async move {
                ctx.adapt(|o| o.with_duration(Duration::from_secs(2)));
                Ok(ctx.value(42))
            },
            long(),
        )
        .await
        .unwrap();

    clock.advance(Duration::from_secs(1)); // within adapted 2s ⇒ cached
    let cached = cache
        .get_or_set_with("k", |ctx| async move { Ok(ctx.value(999)) }, long())
        .await
        .unwrap();
    assert_eq!(cached, 42, "adapted duration still fresh");

    clock.advance(Duration::from_secs(2)); // now past adapted 2s ⇒ re-run
    let refreshed = cache
        .get_or_set_with("k", |ctx| async move { Ok(ctx.value(7)) }, long())
        .await
        .unwrap();
    assert_eq!(
        refreshed, 7,
        "adapted duration governs expiration, not the 100s call duration"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conditional_refresh_not_modified_reuses_stale_and_bumps_expiration() {
    let (cache, clock) = build::<String>();
    let opts = fail_safe_opts();

    cache
        .get_or_set_with(
            "k",
            |ctx| async move { Ok(ctx.modified("data-v1".to_owned()).etag("abc").done()) },
            opts.clone(),
        )
        .await
        .unwrap();
    clock.advance(Duration::from_secs(20)); // stale

    let calls = Arc::new(AtomicUsize::new(0));
    let served = {
        let calls = calls.clone();
        cache
            .get_or_set_with(
                "k",
                move |ctx: FactoryContext<String>| async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(
                        ctx.stale_etag(),
                        Some("abc"),
                        "stale ETag is exposed to factory"
                    );
                    ctx.not_modified()
                },
                opts,
            )
            .await
            .unwrap()
    };
    assert_eq!(served, "data-v1", "NotModified reuses the stale value");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    // Expiration was bumped ⇒ now fresh again at the current clock.
    assert_eq!(
        cache.try_get("k", None).await.value(),
        Some(&"data-v1".to_owned())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conditional_refresh_modified_replaces_value() {
    let (cache, clock) = build::<String>();
    let opts = fail_safe_opts();

    cache
        .get_or_set_with(
            "k",
            |ctx| async move { Ok(ctx.modified("data-v1".to_owned()).etag("abc").done()) },
            opts.clone(),
        )
        .await
        .unwrap();
    clock.advance(Duration::from_secs(20));

    let served = cache
        .get_or_set_with(
            "k",
            |ctx: FactoryContext<String>| async move {
                Ok(ctx.modified("data-v2".to_owned()).etag("def").done())
            },
            opts,
        )
        .await
        .unwrap();
    assert_eq!(served, "data-v2");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remove_by_tag_invalidates_matching_entries() {
    let (cache, clock) = build::<i32>();
    let long = || EntryOptions::new(Duration::from_secs(100));
    let calls = Arc::new(AtomicUsize::new(0));
    let tagged = || -> Box<[Tag]> { Box::from([Tag::new("group").unwrap()]) };

    {
        let calls = calls.clone();
        cache
            .get_or_set_full(
                "k",
                move |ctx| async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(ctx.value(1))
                },
                Some(long()),
                tagged(),
                MaybeValue::none(),
            )
            .await
            .unwrap();
    }

    clock.advance(Duration::from_secs(1));
    cache.remove_by_tag("group").await;
    clock.advance(Duration::from_secs(1)); // so the new entry is created after the marker

    let v = {
        let calls = calls.clone();
        cache
            .get_or_set_full(
                "k",
                move |ctx| async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(ctx.value(2))
                },
                Some(long()),
                tagged(),
                MaybeValue::none(),
            )
            .await
            .unwrap()
    };
    assert_eq!(v, 2, "tagged entry was invalidated and the factory re-ran");
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clear_removes_everything() {
    let (cache, _clock) = build::<i32>();
    cache.set("a", 1).await;
    cache.set("b", 2).await;
    assert_eq!(cache.try_get("a", None).await.value(), Some(&1));

    cache.clear(false).await; // hard remove
    cache.run_pending_tasks().await;
    assert!(!cache.try_get("a", None).await.has_value());
    assert!(!cache.try_get("b", None).await.has_value());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn try_get_and_get_or_default() {
    let cache: Cache<i32> = Cache::new();
    assert!(!cache.try_get("k", None).await.has_value());
    assert_eq!(cache.get_or_default("k", -1, None).await, -1);

    cache.set("k", 5).await;
    assert_eq!(cache.try_get("k", None).await.value(), Some(&5));
    assert_eq!(cache.get_or_default("k", -1, None).await, 5);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn allow_stale_on_read_only_serves_stale() {
    let (cache, clock) = build::<i32>();
    let opts = fail_safe_opts();
    cache
        .get_or_set_with("k", |ctx| async move { Ok(ctx.value(9)) }, opts)
        .await
        .unwrap();
    clock.advance(Duration::from_secs(20)); // stale

    // Default read-only: stale hidden.
    assert!(!cache.try_get("k", None).await.has_value());
    // With allow_stale_on_read_only: stale visible.
    let stale_opts = EntryOptions::new(Duration::from_secs(10)).with_allow_stale_on_read_only(true);
    assert_eq!(cache.try_get("k", Some(stale_opts)).await.value(), Some(&9));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn events_are_emitted() {
    let cache: Cache<i32> = Cache::new();
    let mut rx = cache.events().subscribe();

    cache.set("k", 1).await;
    let _ = cache
        .get_or_set("k", |ctx| async move { Ok(ctx.value(1)) })
        .await
        .unwrap();

    let mut saw_set = false;
    let mut saw_fresh_hit = false;
    // Drain what is buffered.
    for _ in 0..8 {
        match rx.try_recv() {
            Ok(CacheEvent::Set { .. }) => saw_set = true,
            Ok(CacheEvent::Hit { stale: false, .. }) => saw_fresh_hit = true,
            Ok(_) => {}
            Err(_) => break,
        }
    }
    assert!(saw_set, "a Set event was emitted");
    assert!(saw_fresh_hit, "a fresh Hit event was emitted");
}
