//! Integration tests for the advanced, newly-wired cache features.
//!
//! Each test exercises one feature end-to-end through the public API only:
//! plugins, the cross-node distributed locker, the L2 circuit breaker paired
//! with auto-recovery, multi-node tag propagation over the backplane, the named
//! `CacheRegistry`, and the dynamic `DefaultEntryOptionsProvider`.
//!
//! Convention (mirrors `tests/behavior.rs` / `tests/multilevel.rs`): cache
//! *expiration* is driven by an injected [`ManualClock`] for determinism, while
//! genuinely time-based machinery (the auto-recovery background loop, distributed
//! lock polling, backplane fan-out) uses the real tokio timer with generous
//! margins. Non-obvious timing is explained inline at each call site.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;

use amalgam::{
    Backplane, Cache, CacheEvent, CacheRegistry, CircuitComponent, Clock,
    DefaultEntryOptionsProvider, DistributedCache, DistributedLocker, DistributedSerializer,
    EntryOptions, InMemoryDistributedCache, InMemoryDistributedLocker, InProcessBackplane,
    JsonSerializer, ManualClock, MaybeValue, Plugin, RecoveryConfig, Result, Tag,
};

// ---------------------------------------------------------------------------
// 1. Plugins: a plugin receives Set and Hit events.
// ---------------------------------------------------------------------------

/// A plugin that tallies the `Set` and fresh-`Hit` events it observes.
struct CountingPlugin {
    sets: Arc<AtomicUsize>,
    hits: Arc<AtomicUsize>,
}

impl Plugin for CountingPlugin {
    fn name(&self) -> &str {
        "counting"
    }

    fn on_event(&self, event: &CacheEvent) {
        match event {
            CacheEvent::Set { .. } => {
                self.sets.fetch_add(1, Ordering::SeqCst);
            }
            CacheEvent::Hit { stale: false, .. } => {
                self.hits.fetch_add(1, Ordering::SeqCst);
            }
            _ => {}
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plugin_receives_set_and_hit_events() {
    let sets = Arc::new(AtomicUsize::new(0));
    let hits = Arc::new(AtomicUsize::new(0));
    let plugin = Arc::new(CountingPlugin {
        sets: sets.clone(),
        hits: hits.clone(),
    });

    let clock = Arc::new(ManualClock::default());
    let dyn_clock: Arc<dyn Clock> = clock.clone();
    let cache: Cache<i32> = Cache::builder().clock(dyn_clock).plugin(plugin).build();

    // `set` emits a `Set`. Plugins are notified *synchronously* on the emit
    // path, so the counters are up to date the moment `.await` returns — no
    // sleep is needed here.
    cache.set("k", 1).await;

    // The key is now fresh in L1, so this resolves from the hot path and emits a
    // fresh `Hit` (the factory never runs).
    let v = cache
        .get_or_set("k", |ctx| async move { Ok(ctx.value(999)) })
        .await
        .unwrap();
    assert_eq!(v, 1, "fresh L1 value short-circuits the factory");

    assert!(
        sets.load(Ordering::SeqCst) >= 1,
        "plugin observed at least one Set event"
    );
    assert!(
        hits.load(Ordering::SeqCst) >= 1,
        "plugin observed at least one fresh Hit event"
    );
}

// ---------------------------------------------------------------------------
// 2. Distributed locker → cross-instance single-flight.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn distributed_locker_enforces_cross_instance_single_flight() {
    let clock = Arc::new(ManualClock::default());
    let dyn_clock: Arc<dyn Clock> = clock.clone();

    // One shared L2 and one shared cross-node locker behind both caches.
    let l2: Arc<dyn DistributedCache> = Arc::new(InMemoryDistributedCache::new(dyn_clock.clone()));
    let locker: Arc<dyn DistributedLocker> =
        Arc::new(InMemoryDistributedLocker::new(dyn_clock.clone()));
    let serializer: Arc<dyn DistributedSerializer<String>> = Arc::new(JsonSerializer);
    let opts = EntryOptions::new(Duration::from_secs(60));

    let build = || -> Cache<String> {
        Cache::builder()
            .clock(dyn_clock.clone())
            .distributed(l2.clone())
            .serializer(serializer.clone())
            .distributed_locker(locker.clone())
            .default_options(opts.clone())
            .build()
    };
    let cache1 = build();
    let cache2 = build();

    // A single shared counter incremented inside the factory: it must end at 1.
    let calls = Arc::new(AtomicUsize::new(0));

    let slow_factory = |calls: Arc<AtomicUsize>| {
        move |ctx: amalgam::FactoryContext<String>| async move {
            calls.fetch_add(1, Ordering::SeqCst);
            // Hold the single-flight long enough that the second instance is
            // guaranteed to be waiting on the *distributed* lock when we finish.
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok(ctx.value("from-factory".to_owned()))
        }
    };

    // Both instances race `get_or_set` on the same key concurrently. Each has its
    // own L1 and its own local lock, so the *only* thing serialising them is the
    // shared distributed locker. The winner runs the factory and writes L2 before
    // its guard drops (which releases the distributed lock); the loser then
    // acquires the lock, finds nothing in its own L1, and reads the winner's
    // value back from the shared L2 — so the factory runs exactly once.
    let h1 = {
        let cache = cache1.clone();
        let f = slow_factory(calls.clone());
        tokio::spawn(async move { cache.get_or_set("k", f).await })
    };
    let h2 = {
        let cache = cache2.clone();
        let f = slow_factory(calls.clone());
        tokio::spawn(async move { cache.get_or_set("k", f).await })
    };

    let v1 = h1.await.unwrap().unwrap();
    let v2 = h2.await.unwrap().unwrap();

    assert_eq!(v1, "from-factory");
    assert_eq!(v2, "from-factory", "loser served the winner's L2 value");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "the distributed lock collapsed both flights into a single factory run"
    );
}

// ---------------------------------------------------------------------------
// 3. Circuit breaker + auto-recovery.
// ---------------------------------------------------------------------------

/// An L2 wrapper that fails `get`/`set` while a `down` flag is set, otherwise
/// delegates to an inner [`InMemoryDistributedCache`]. Models a transiently
/// unavailable distributed backend.
struct FlakyL2 {
    inner: Arc<InMemoryDistributedCache>,
    down: Arc<AtomicBool>,
}

#[async_trait]
impl DistributedCache for FlakyL2 {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        if self.down.load(Ordering::SeqCst) {
            return Err(amalgam::Error::Distributed("down".into()));
        }
        self.inner.get(key).await
    }

    async fn set(&self, key: &str, value: Vec<u8>, ttl: Option<Duration>) -> Result<()> {
        if self.down.load(Ordering::SeqCst) {
            return Err(amalgam::Error::Distributed("down".into()));
        }
        self.inner.set(key, value, ttl).await
    }

    async fn remove(&self, key: &str) -> Result<()> {
        // `remove` is not part of the failure surface under test; always delegate.
        self.inner.remove(key).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn circuit_breaker_opens_then_auto_recovery_replays_write() {
    let clock = Arc::new(ManualClock::default());
    let dyn_clock: Arc<dyn Clock> = clock.clone();

    let inner_l2 = Arc::new(InMemoryDistributedCache::new(dyn_clock.clone()));
    let down = Arc::new(AtomicBool::new(true)); // start with L2 unavailable
    let flaky: Arc<dyn DistributedCache> = Arc::new(FlakyL2 {
        inner: inner_l2.clone(),
        down: down.clone(),
    });
    let serializer: Arc<dyn DistributedSerializer<String>> = Arc::new(JsonSerializer);

    let cache: Cache<String> = Cache::builder()
        .clock(dyn_clock.clone())
        .distributed(flaky)
        .serializer(serializer)
        .distributed_circuit_breaker(Duration::from_secs(30))
        .auto_recovery(RecoveryConfig {
            enabled: true,
            delay: Duration::from_millis(100),
            max_items: None,
            max_retries: None,
        })
        .build();

    let mut events = cache.events().subscribe();

    // (a) With L2 down, a write fails: the L2 entry is never stored, the breaker
    // trips open (firing exactly one CircuitBreakerChange{closed:false}), and the
    // operation is queued for auto-recovery.
    cache.set("k", "v1".to_owned()).await;

    // The L2 layer prefixes stored keys with the wire-format version ("v1" by
    // default) and there is no key prefix, so the backend key is "v1:k".
    const L2_KEY: &str = "v1:k";
    assert!(
        inner_l2.get(L2_KEY).await.unwrap().is_none(),
        "the failed write left nothing in L2"
    );

    let mut saw_open = false;
    for _ in 0..16 {
        match events.try_recv() {
            Ok(CacheEvent::CircuitBreakerChange {
                component: CircuitComponent::Distributed,
                closed: false,
            }) => {
                saw_open = true;
                break;
            }
            Ok(_) => {}
            Err(_) => break,
        }
    }
    assert!(
        saw_open,
        "tripping the L2 breaker emitted CircuitBreakerChange{{ closed: false }}"
    );

    // (b) Recover the dependency. The background auto-recovery loop ticks on the
    // real timer (delay = 100ms) and replays the queued Set directly to L2
    // (bypassing the still-open breaker), so the value eventually lands in the
    // inner L2. We wait well past several tick intervals to stay non-flaky; the
    // ManualClock never advances, so the recovery item never times out (its TTL
    // is 600s) and remains eligible for replay.
    down.store(false, Ordering::SeqCst);

    let mut recovered = false;
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(50)).await; // up to ~1s total
        if inner_l2.get(L2_KEY).await.unwrap().is_some() {
            recovered = true;
            break;
        }
    }
    assert!(
        recovered,
        "auto-recovery replayed the write to the inner L2 once it came back up"
    );

    // Keep the cache alive until the end: the recovery loop holds only a Weak to
    // it, and would stop the moment the cache were dropped.
    drop(cache);
}

// ---------------------------------------------------------------------------
// 4. Multi-node tag propagation over the backplane.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remove_by_tag_propagates_across_nodes() {
    let clock = Arc::new(ManualClock::default());
    let dyn_clock: Arc<dyn Clock> = clock.clone();
    let backplane: Arc<dyn Backplane> = Arc::new(InProcessBackplane::default());
    let long = || EntryOptions::new(Duration::from_secs(100));
    let tagged = || -> Box<[Tag]> { Box::from([Tag::new("group").unwrap()]) };

    let build = |id: &str| -> Cache<String> {
        Cache::builder()
            .clock(dyn_clock.clone())
            .backplane(backplane.clone())
            .instance_id(id)
            .build()
    };
    let node_a = build("node-a");
    let node_b = build("node-b");

    let calls = Arc::new(AtomicUsize::new(0));

    // Node A caches "k" tagged ["group"], created at the clock's start (T0).
    {
        let calls = calls.clone();
        node_a
            .get_or_set_full(
                "k",
                move |ctx| async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(ctx.value("v1".to_owned()))
                },
                Some(long()),
                tagged(),
                MaybeValue::none(),
            )
            .await
            .unwrap();
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    // Advance so the tag marker B is about to publish is strictly newer than A's
    // entry (the tag check is inclusive: entry_created <= marker ⇒ invalid).
    clock.advance(Duration::from_secs(1)); // now T1

    // Node B invalidates the tag; the marker rides the backplane to node A.
    node_b.remove_by_tag("group").await;
    tokio::time::sleep(Duration::from_millis(80)).await; // let the marker propagate

    // Advance again so A's *next* entry is created strictly after the marker (T2),
    // otherwise the freshly-produced entry would itself be caught by the inclusive
    // comparison — the exact gotcha pinned in behavior.rs.
    clock.advance(Duration::from_secs(1)); // now T2

    // A's previous entry (created at T0 <= marker T1) is now invalidated, so the
    // factory must re-run.
    let v = {
        let calls = calls.clone();
        node_a
            .get_or_set_full(
                "k",
                move |ctx| async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(ctx.value("v2".to_owned()))
                },
                Some(long()),
                tagged(),
                MaybeValue::none(),
            )
            .await
            .unwrap()
    };
    assert_eq!(
        v, "v2",
        "the propagated tag marker invalidated node A's entry"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "node A re-ran the factory after the cross-node tag invalidation"
    );
}

// ---------------------------------------------------------------------------
// 5. Registry: named caches.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registry_resolves_named_caches_independently() {
    let clock = Arc::new(ManualClock::default());
    let dyn_clock: Arc<dyn Clock> = clock.clone();

    let registry: CacheRegistry<i32> = CacheRegistry::new();
    let make = || -> Cache<i32> { Cache::builder().clock(dyn_clock.clone()).build() };
    registry.register("alpha", make());
    registry.register("beta", make());
    assert_eq!(registry.len(), 2);

    let alpha = registry.get("alpha").expect("alpha is registered");
    let beta = registry.get("beta").expect("beta is registered");
    assert!(registry.get("missing").is_none());

    // The two named caches are independent stores.
    alpha.set("k", 1).await;
    beta.set("k", 2).await;
    assert_eq!(alpha.try_get("k", None).await.value(), Some(&1));
    assert_eq!(beta.try_get("k", None).await.value(), Some(&2));

    // `get_or_create` builds exactly once: a second call returns the existing
    // cache without invoking the builder again.
    let builds = Arc::new(AtomicUsize::new(0));
    let gamma1 = registry.get_or_create("gamma", || {
        builds.fetch_add(1, Ordering::SeqCst);
        make()
    });
    gamma1.set("k", 7).await;
    let gamma2 = registry.get_or_create("gamma", || {
        builds.fetch_add(1, Ordering::SeqCst);
        make()
    });
    assert_eq!(
        builds.load(Ordering::SeqCst),
        1,
        "gamma was built only once"
    );
    assert_eq!(
        gamma2.try_get("k", None).await.value(),
        Some(&7),
        "the second get_or_create returned the same gamma cache"
    );
}

// ---------------------------------------------------------------------------
// 6. DefaultEntryOptionsProvider: per-key dynamic defaults.
// ---------------------------------------------------------------------------

/// Returns a short freshness window for keys starting with `short:`, and falls
/// back to the cache's static default for everything else.
struct ShortPrefixProvider;

impl DefaultEntryOptionsProvider for ShortPrefixProvider {
    fn options_for(&self, key: &str) -> Option<EntryOptions> {
        if key.starts_with("short:") {
            Some(EntryOptions::new(Duration::from_secs(2)))
        } else {
            None
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_options_provider_applies_per_key_duration() {
    let clock = Arc::new(ManualClock::default());
    let dyn_clock: Arc<dyn Clock> = clock.clone();

    let cache: Cache<i32> = Cache::builder()
        .clock(dyn_clock)
        .default_options(EntryOptions::new(Duration::from_secs(100))) // static default
        .default_options_provider(Arc::new(ShortPrefixProvider))
        .build();

    let short_calls = Arc::new(AtomicUsize::new(0));
    let normal_calls = Arc::new(AtomicUsize::new(0));

    let prime = |cache: &Cache<i32>, key: &'static str, counter: Arc<AtomicUsize>, val: i32| {
        let cache = cache.clone();
        async move {
            cache
                .get_or_set(key, move |ctx| async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    Ok(ctx.value(val))
                })
                .await
                .unwrap()
        }
    };

    // Prime both keys. Neither passes explicit options, so the provider is
    // consulted: "short:x" gets the 2s window, "normal" falls back to 100s.
    assert_eq!(prime(&cache, "short:x", short_calls.clone(), 1).await, 1);
    assert_eq!(prime(&cache, "normal", normal_calls.clone(), 1).await, 1);
    assert_eq!(short_calls.load(Ordering::SeqCst), 1);
    assert_eq!(normal_calls.load(Ordering::SeqCst), 1);

    // Advance past the short window but well within the static default.
    clock.advance(Duration::from_secs(3));

    // "short:x" has expired (provider's 2s window) ⇒ the factory re-runs.
    assert_eq!(prime(&cache, "short:x", short_calls.clone(), 2).await, 2);
    assert_eq!(
        short_calls.load(Ordering::SeqCst),
        2,
        "short-prefixed key expired per the provider's 2s duration"
    );

    // "normal" is still fresh (100s default) ⇒ the factory does NOT re-run.
    assert_eq!(prime(&cache, "normal", normal_calls.clone(), 99).await, 1);
    assert_eq!(
        normal_calls.load(Ordering::SeqCst),
        1,
        "normal key used the cache default and is still fresh"
    );
}
