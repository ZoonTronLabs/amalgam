//! Public-API concurrency regressions and explicit FusionCache compatibility observations.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use amalgam::{
    Cache, CacheEvent, Clock, DistributedCache, DistributedLocker, EagerThreshold, EntryOptions,
    InMemoryDistributedCache, InMemoryDistributedLocker, JsonSerializer, ManualClock, MaybeValue,
    RemoveByTagBehavior, Tag, Timeout,
};
use tokio::sync::{Semaphore, oneshot};

struct Running(Arc<AtomicUsize>);

impl Running {
    fn start(counter: Arc<AtomicUsize>) -> Self {
        counter.fetch_add(1, Ordering::SeqCst);
        Self(counter)
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

fn eager_options() -> EntryOptions {
    EntryOptions::new(Duration::from_secs(10))
        .with_eager_refresh(Some(EagerThreshold::new(0.5).expect("valid threshold")))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelling_finite_factory_must_not_detach_it_and_release_singleflight() {
    let cache: Cache<i32> = Cache::new();
    let active = Arc::new(AtomicUsize::new(0));
    let (started_tx, started_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let finite = EntryOptions::new(Duration::from_secs(10)).with_factory_timeouts(
        Timeout::Infinite,
        Timeout::After(Duration::from_secs(10)),
        false,
    );
    let first = {
        let cache = cache.clone();
        let active = active.clone();
        tokio::spawn(async move {
            cache
                .get_or_set_with(
                    "k",
                    move |ctx| async move {
                        let _running = Running::start(active);
                        let _ = started_tx.send(());
                        let _ = release_rx.await;
                        Ok(ctx.value(1))
                    },
                    finite,
                )
                .await
        })
    };
    started_rx.await.expect("first factory started");
    first.abort();
    assert!(first.await.expect_err("caller cancelled").is_cancelled());
    tokio::task::yield_now().await;
    let still_active = active.load(Ordering::SeqCst);
    let observed_active = {
        let active = active.clone();
        cache
            .get_or_set("k", move |ctx| async move {
                Ok(ctx.value(active.load(Ordering::SeqCst) as i32))
            })
            .await
            .expect("second flight finishes")
    };
    let _ = release_tx.send(());
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(
        (still_active, observed_active),
        (0, 0),
        "cancelled factory is still running after its single-flight guard was released"
    );
}

#[tokio::test]
async fn unrelated_nested_cache_keys_must_not_deadlock_when_hash_shards_collide() {
    let shard = |key: &str| {
        let mut h = DefaultHasher::new();
        key.hash(&mut h);
        h.finish() & 1023
    };
    let outer = "audit-outer";
    let inner = (0..100_000)
        .map(|i| format!("audit-inner-{i}"))
        .find(|key| shard(key) == shard(outer))
        .expect("find default-bank collision");
    assert_ne!(inner, outer);
    let cache: Cache<i32> = Cache::new();
    let nested_cache = cache.clone();
    let result = tokio::time::timeout(
        Duration::from_millis(80),
        cache.get_or_set(outer, move |ctx| async move {
            let nested = nested_cache
                .get_or_set(inner, |ctx| async move { Ok(ctx.value(42)) })
                .await
                .map_err(amalgam::FactoryError::from_source)?;
            Ok(ctx.value(nested))
        }),
    )
    .await;
    assert!(
        result.is_ok(),
        "distinct-key dependency graph is acyclic, but collided locks deadlocked it"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distinct_cold_keys_are_not_serialized_by_the_legacy_shard_hint() {
    let cache: Cache<i32> = Cache::builder().lock_shards(1).build();
    let mut tasks = Vec::with_capacity(128);
    for index in 0..128 {
        let cache = cache.clone();
        tasks.push(tokio::spawn(async move {
            cache
                .get_or_set(format!("independent-{index}"), move |ctx| async move {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    Ok(ctx.value(index))
                })
                .await
        }));
    }
    tokio::time::timeout(Duration::from_millis(300), async move {
        for (index, task) in tasks.into_iter().enumerate() {
            assert_eq!(
                task.await.expect("factory task").expect("origin result"),
                index as i32
            );
        }
    })
    .await
    .expect("128 independent 10ms factories execute concurrently");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn foreground_factory_panics_propagate_and_release_the_flight() {
    for hard in [Timeout::Infinite, Timeout::After(Duration::from_secs(1))] {
        let cache: Cache<i32> = Cache::new();
        let caller = {
            let cache = cache.clone();
            tokio::spawn(async move {
                cache
                    .get_or_set_with(
                        "k",
                        |_ctx| async move {
                            panic!("factory contract defect");
                        },
                        EntryOptions::new(Duration::from_secs(10)).with_factory_timeouts(
                            Timeout::Infinite,
                            hard,
                            false,
                        ),
                    )
                    .await
            })
        };
        assert!(
            caller
                .await
                .expect_err("programmer panic propagates")
                .is_panic()
        );
        let next = tokio::time::timeout(
            Duration::from_millis(100),
            cache.get_or_set("k", |ctx| async move { Ok(ctx.value(2)) }),
        )
        .await
        .expect("panic released owned flight")
        .expect("next factory succeeds");
        assert_eq!(next, 2);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn background_factory_panics_are_observed_and_release_the_flight() {
    let clock = Arc::new(ManualClock::default());
    let cache: Cache<i32> = Cache::builder()
        .clock(clock.clone())
        .default_options(eager_options())
        .build();
    cache.set("k", 1).await;
    clock.advance(Duration::from_secs(6));
    let mut events = cache.events().subscribe();
    assert_eq!(
        cache
            .get_or_set("k", |_ctx| async move {
                panic!("background factory contract defect");
            })
            .await
            .expect("fresh eager hit"),
        1
    );
    tokio::time::timeout(Duration::from_millis(100), async {
        loop {
            if let CacheEvent::BackgroundFactoryError { message, .. } =
                events.recv().await.expect("background diagnostics")
            {
                assert!(message.contains("panicked"));
                break;
            }
        }
    })
    .await
    .expect("panic supervisor emitted diagnostic");
    clock.advance(Duration::from_secs(10));
    assert_eq!(
        tokio::time::timeout(
            Duration::from_millis(100),
            cache.get_or_set("k", |ctx| async move { Ok(ctx.value(2)) })
        )
        .await
        .expect("panic released flight")
        .expect("next origin succeeds"),
        2
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_configured_memory_lock_timeout_must_bound_a_cold_waiter() {
    let cache: Cache<i32> = Cache::new();
    let (started_tx, started_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let first = {
        let cache = cache.clone();
        tokio::spawn(async move {
            cache
                .get_or_set("k", move |ctx| async move {
                    let _ = started_tx.send(());
                    let _ = release_rx.await;
                    Ok(ctx.value(1))
                })
                .await
        })
    };
    started_rx.await.expect("first factory holds lock");
    let finite = EntryOptions::new(Duration::from_secs(10))
        .with_memory_lock_timeout(Timeout::After(Duration::from_millis(10)))
        .with_factory_timeouts(
            Timeout::Infinite,
            Timeout::After(Duration::from_millis(10)),
            false,
        );
    let second = tokio::time::timeout(
        Duration::from_millis(60),
        cache.get_or_set_with("k", |ctx| async move { Ok(ctx.value(2)) }, finite),
    )
    .await;
    let _ = release_tx.send(());
    first.await.expect("first joined").expect("first finishes");
    assert_eq!(
        second
            .expect("finite memory-lock wait")
            .expect("best-effort factory"),
        2,
        "finite memory-lock timeout must continue using the second factory like FusionCache"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_factory_soft_timeout_must_also_bound_waiting_for_a_stale_singleflight() {
    let clock = Arc::new(ManualClock::default());
    let options = EntryOptions::new(Duration::from_secs(10))
        .with_fail_safe(
            true,
            Some(Duration::from_secs(100)),
            Some(Duration::from_millis(1)),
        )
        .with_factory_timeouts(
            Timeout::After(Duration::from_millis(10)),
            Timeout::Infinite,
            true,
        );
    let cache: Cache<i32> = Cache::builder()
        .clock(clock.clone())
        .default_options(options)
        .build();
    cache.set("k", 1).await;
    clock.advance(Duration::from_secs(20));
    let (release_tx, release_rx) = oneshot::channel();
    assert_eq!(
        cache
            .get_or_set("k", move |ctx| async move {
                let _ = release_rx.await;
                Ok(ctx.value(2))
            })
            .await
            .expect("first stale fallback"),
        1
    );
    clock.advance(Duration::from_secs(1)); // Throttle expired while background factory still holds lock.
    let second = tokio::time::timeout(
        Duration::from_millis(60),
        cache.get_or_set("k", |ctx| async move { Ok(ctx.value(3)) }),
    )
    .await;
    let _ = release_tx.send(());
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(
        second.is_ok(),
        "10ms factory soft timeout did not protect stale caller waiting behind background factory"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn eager_refresh_must_obtain_the_configured_distributed_locker() {
    let clock = Arc::new(ManualClock::default());
    let dyn_clock: Arc<dyn Clock> = clock.clone();
    let locker: Arc<dyn DistributedLocker> =
        Arc::new(InMemoryDistributedLocker::new(dyn_clock.clone()));
    let options = eager_options();
    let build = || {
        Cache::<i32>::builder()
            .clock(dyn_clock.clone())
            .distributed_locker(locker.clone())
            .default_options(options.clone())
            .build()
    };
    let a = build();
    let b = build();
    a.set("k", 1).await;
    b.set("k", 1).await;
    clock.advance(Duration::from_secs(6));
    let calls = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(Semaphore::new(0));
    for cache in [&a, &b] {
        let calls = calls.clone();
        let gate = gate.clone();
        assert_eq!(
            cache
                .get_or_set("k", move |ctx| async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    gate.acquire().await.expect("gate open").forget();
                    Ok(ctx.value(2))
                })
                .await
                .expect("read returned"),
            1
        );
    }
    tokio::time::sleep(Duration::from_millis(40)).await;
    let concurrent_factories = calls.load(Ordering::SeqCst);
    gate.add_permits(2);
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(
        concurrent_factories, 1,
        "both cache nodes ran eager factories for the same key"
    );
}

#[tokio::test]
async fn distributed_locker_must_not_acquire_a_lease_after_the_wait_deadline() {
    let locker = InMemoryDistributedLocker::new(Arc::new(amalgam::SystemClock));
    let first = locker
        .acquire("k", Duration::from_millis(5), Timeout::Infinite)
        .await
        .expect("initial acquire")
        .expect("initial lease");
    let started = std::time::Instant::now();
    let second = locker
        .acquire(
            "k",
            Duration::from_secs(10),
            Timeout::After(Duration::from_millis(1)),
        )
        .await
        .expect("second acquire");
    let elapsed = started.elapsed();
    if let Some(token) = &second {
        locker.release("k", token).await.expect("release second");
    }
    locker.release("k", &first).await.expect("release first");
    assert!(
        second.is_none(),
        "1ms acquisition deadline returned a new lease after {elapsed:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn eager_refresh_must_prefer_a_newer_l2_entry_before_running_factory() {
    let clock = Arc::new(ManualClock::default());
    let dyn_clock: Arc<dyn Clock> = clock.clone();
    let l2: Arc<dyn DistributedCache> = Arc::new(InMemoryDistributedCache::new(dyn_clock.clone()));
    let options = eager_options();
    let build = || {
        Cache::<i32>::builder()
            .clock(dyn_clock.clone())
            .distributed(l2.clone())
            .serializer(Arc::new(JsonSerializer))
            .default_options(options.clone())
            .build()
    };
    let a = build();
    let b = build();
    a.set("k", 1).await;
    clock.advance(Duration::from_secs(6));
    b.try_set("k", 2)
        .await
        .expect("newer write accepted")
        .wait()
        .await
        .expect("newer L2 write completed");
    let calls = Arc::new(AtomicUsize::new(0));
    let factory_calls = calls.clone();
    assert_eq!(
        a.get_or_set("k", move |ctx| async move {
            factory_calls.fetch_add(1, Ordering::SeqCst);
            Ok(ctx.value(3))
        })
        .await
        .expect("fresh read"),
        1
    );
    tokio::time::timeout(Duration::from_secs(3), a.flush_pending())
        .await
        .expect("eager refresh completed promptly")
        .expect("eager refresh drained");
    let after = a.try_get("k", None).await;
    assert_eq!(
        (calls.load(Ordering::SeqCst), after.value().copied()),
        (0, Some(2)),
        "eager refresh skipped newer L2 and executed an unnecessary origin factory"
    );
}

#[tokio::test]
async fn an_immediate_factory_deadline_must_not_invoke_the_factory() {
    let cache: Cache<i32> = Cache::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let options = EntryOptions::new(Duration::from_secs(10)).with_factory_timeouts(
        Timeout::Infinite,
        Timeout::After(Duration::ZERO),
        false,
    );
    let factory_calls = calls.clone();
    let _ = cache
        .get_or_set_with(
            "k",
            move |ctx| {
                factory_calls.fetch_add(1, Ordering::SeqCst);
                async move { Ok(ctx.value(1)) }
            },
            options,
        )
        .await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "already elapsed deadline still invoked factory"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tag_invalidation_must_cover_a_factory_snapshot_started_before_its_marker() {
    let clock = Arc::new(ManualClock::default());
    let cache: Cache<i32> = Cache::builder()
        .clock(clock.clone())
        .remove_by_tag_behavior(RemoveByTagBehavior::Remove)
        .build();
    let (snapshot_tx, snapshot_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let first = {
        let cache = cache.clone();
        tokio::spawn(async move {
            cache
                .get_or_set_full(
                    "k",
                    move |ctx| async move {
                        let database_snapshot = 1;
                        let _ = snapshot_tx.send(());
                        let _ = release_rx.await;
                        Ok(ctx.value(database_snapshot))
                    },
                    None,
                    vec![Tag::new("group").expect("tag")].into_boxed_slice(),
                    MaybeValue::none(),
                )
                .await
        })
    };
    snapshot_rx.await.expect("origin snapshot captured");
    clock.advance(Duration::from_secs(1));
    cache.remove_by_tag("group").await;
    clock.advance(Duration::from_secs(1));
    let _ = release_tx.send(());
    first
        .await
        .expect("first joined")
        .expect("overlapping call returned its snapshot");
    assert!(
        !cache.try_get("k", None).await.has_value(),
        "factory snapshot from before marker was timestamped at completion and escaped invalidation"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_fc_lock_timeout_can_serve_a_previously_captured_stale_snapshot() {
    let cache: Cache<i32> = Cache::new();
    let options = EntryOptions::new(Duration::from_millis(2))
        .with_fail_safe(
            true,
            Some(Duration::from_millis(60)),
            Some(Duration::from_millis(2)),
        )
        .with_memory_lock_timeout(Timeout::After(Duration::from_millis(90)));
    cache
        .set_full("k", 1, Some(options.clone()), Box::from([]))
        .await;
    tokio::time::sleep(Duration::from_millis(5)).await;
    let (started_tx, started_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let first = {
        let cache = cache.clone();
        let options = options.clone();
        tokio::spawn(async move {
            cache
                .get_or_set_with(
                    "k",
                    move |ctx| async move {
                        let _ = started_tx.send(());
                        let _ = release_rx.await;
                        Ok(ctx.value(2))
                    },
                    options,
                )
                .await
        })
    };
    started_rx.await.expect("first factory holds lock");
    let second = tokio::time::timeout(
        Duration::from_millis(150),
        cache.get_or_set_with("k", |ctx| async move { Ok(ctx.value(3)) }, options),
    )
    .await;
    let _ = release_tx.send(());
    first.await.expect("first joined").expect("first finishes");
    let served = second.ok().and_then(Result::ok);
    assert_eq!(
        served,
        Some(1),
        "FusionCache compatibility: lock timeout returns its captured stale snapshot"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn background_completion_cannot_resurrect_an_awaited_remove() {
    let clock = Arc::new(ManualClock::default());
    let options = EntryOptions::new(Duration::from_secs(10))
        .with_fail_safe(
            true,
            Some(Duration::from_secs(100)),
            Some(Duration::from_secs(1)),
        )
        .with_factory_timeouts(
            Timeout::After(Duration::from_millis(10)),
            Timeout::Infinite,
            true,
        );
    let cache: Cache<i32> = Cache::builder()
        .clock(clock.clone())
        .default_options(options)
        .build();
    cache.set("k", 1).await;
    clock.advance(Duration::from_secs(20));
    let mut events = cache.events().subscribe();
    let (release_tx, release_rx) = oneshot::channel();
    assert_eq!(
        cache
            .get_or_set("k", move |ctx| async move {
                let _ = release_rx.await;
                Ok(ctx.value(2))
            })
            .await
            .expect("fail safe stale"),
        1
    );
    cache.remove("k").await;
    assert!(!cache.try_get("k", None).await.has_value());
    let _ = release_tx.send(());
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if matches!(
                events.recv().await.expect("event stream"),
                CacheEvent::BackgroundFactorySuccess { .. }
            ) {
                break;
            }
        }
    })
    .await
    .expect("background completed");
    assert_eq!(
        cache.try_get("k", None).await.value().copied(),
        None,
        "an awaited remove supersedes a previously started background factory"
    );
}
