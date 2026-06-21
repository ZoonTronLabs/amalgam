//! Multi-level (L1 + L2 + backplane) integration tests.
//!
//! These exercise the "hybrid cache" identity: a shared in-memory L2 backing
//! several `Cache` instances, and an in-process backplane keeping their L1s
//! coherent.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use amalgam::{
    Backplane, Cache, Clock, DistributedCache, DistributedSerializer, EntryOptions,
    InMemoryDistributedCache, InProcessBackplane, JsonSerializer, ManualClock,
};

fn shared_l2(clock: Arc<dyn Clock>) -> Arc<dyn DistributedCache> {
    Arc::new(InMemoryDistributedCache::new(clock))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l2_read_through_across_instances() {
    let clock = Arc::new(ManualClock::default());
    let dyn_clock: Arc<dyn Clock> = clock.clone();
    let l2 = shared_l2(dyn_clock.clone());
    let serializer: Arc<dyn DistributedSerializer<String>> = Arc::new(JsonSerializer);
    let opts = EntryOptions::new(Duration::from_secs(60));
    let calls = Arc::new(AtomicUsize::new(0));

    let cache1: Cache<String> = Cache::builder()
        .clock(dyn_clock.clone())
        .distributed(l2.clone())
        .serializer(serializer.clone())
        .default_options(opts.clone())
        .build();

    {
        let calls = calls.clone();
        cache1
            .get_or_set("k", move |ctx| async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(ctx.value("from-factory".to_owned()))
            })
            .await
            .unwrap();
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    // A second instance with an empty L1 but the same L2.
    let cache2: Cache<String> = Cache::builder()
        .clock(dyn_clock.clone())
        .distributed(l2.clone())
        .serializer(serializer.clone())
        .default_options(opts)
        .build();

    let served = {
        let calls = calls.clone();
        cache2
            .get_or_set("k", move |ctx| async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(ctx.value("should-not-run".to_owned()))
            })
            .await
            .unwrap()
    };
    assert_eq!(served, "from-factory", "value came from L2");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "factory did not run on the second instance"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backplane_remove_invalidates_peer() {
    let clock = Arc::new(ManualClock::default());
    let dyn_clock: Arc<dyn Clock> = clock.clone();
    let l2 = shared_l2(dyn_clock.clone());
    let serializer: Arc<dyn DistributedSerializer<String>> = Arc::new(JsonSerializer);
    let backplane: Arc<dyn Backplane> = Arc::new(InProcessBackplane::default());
    let opts = EntryOptions::new(Duration::from_secs(60));

    let build = |id: &str| -> Cache<String> {
        Cache::builder()
            .clock(dyn_clock.clone())
            .distributed(l2.clone())
            .serializer(serializer.clone())
            .backplane(backplane.clone())
            .default_options(opts.clone())
            .instance_id(id)
            .build()
    };
    let cache1 = build("node-1");
    let cache2 = build("node-2");

    cache1.set("k", "v1".to_owned()).await;
    tokio::time::sleep(Duration::from_millis(60)).await; // let the Set propagate

    let served = cache2
        .get_or_set("k", |ctx| async move { Ok(ctx.value("x".to_owned())) })
        .await
        .unwrap();
    assert_eq!(served, "v1", "peer pulled the value from shared L2");

    cache1.remove("k").await;
    tokio::time::sleep(Duration::from_millis(100)).await; // let the Remove propagate

    assert!(
        !cache2.try_get("k", None).await.has_value(),
        "peer L1 was evicted by the backplane Remove"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backplane_set_makes_peer_repull_new_value() {
    let clock = Arc::new(ManualClock::default());
    let dyn_clock: Arc<dyn Clock> = clock.clone();
    let l2 = shared_l2(dyn_clock.clone());
    let serializer: Arc<dyn DistributedSerializer<String>> = Arc::new(JsonSerializer);
    let backplane: Arc<dyn Backplane> = Arc::new(InProcessBackplane::default());
    let opts = EntryOptions::new(Duration::from_secs(60));

    let build = |id: &str| -> Cache<String> {
        Cache::builder()
            .clock(dyn_clock.clone())
            .distributed(l2.clone())
            .serializer(serializer.clone())
            .backplane(backplane.clone())
            .default_options(opts.clone())
            .instance_id(id)
            .build()
    };
    let cache1 = build("node-1");
    let cache2 = build("node-2");
    let calls = Arc::new(AtomicUsize::new(0));

    cache1.set("k", "v1".to_owned()).await;
    tokio::time::sleep(Duration::from_millis(60)).await;

    // cache2 caches v1 locally.
    {
        let calls = calls.clone();
        let v = cache2
            .get_or_set("k", move |ctx| async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(ctx.value("x".to_owned()))
            })
            .await
            .unwrap();
        assert_eq!(v, "v1");
    }

    // cache1 updates the value → backplane Set → cache2 drops its stale L1 copy.
    cache1.set("k", "v2".to_owned()).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let v = {
        let calls = calls.clone();
        cache2
            .get_or_set("k", move |ctx| async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(ctx.value("x".to_owned()))
            })
            .await
            .unwrap()
    };
    assert_eq!(v, "v2", "peer re-pulled the updated value from L2");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "no factory ran on the peer"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backplane_set_eagerly_refreshes_present_l1() {
    let clock = Arc::new(ManualClock::default());
    let dyn_clock: Arc<dyn Clock> = clock.clone();
    let l2 = shared_l2(dyn_clock.clone());
    let serializer: Arc<dyn DistributedSerializer<String>> = Arc::new(JsonSerializer);
    let backplane: Arc<dyn Backplane> = Arc::new(InProcessBackplane::default());
    let opts = EntryOptions::new(Duration::from_secs(60));

    let build = |id: &str| -> Cache<String> {
        Cache::builder()
            .clock(dyn_clock.clone())
            .distributed(l2.clone())
            .serializer(serializer.clone())
            .backplane(backplane.clone())
            .default_options(opts.clone())
            .instance_id(id)
            .build()
    };
    let cache1 = build("node-1");
    let cache2 = build("node-2");

    cache1.set("k", "v1".to_owned()).await;
    tokio::time::sleep(Duration::from_millis(60)).await;

    // cache2 caches v1 in its own L1.
    let v = cache2
        .get_or_set("k", |ctx| async move { Ok(ctx.value("x".to_owned())) })
        .await
        .unwrap();
    assert_eq!(v, "v1");

    // cache1 updates the value → backplane Set. FusionCache "passive update": a
    // peer that already holds the key eagerly refreshes its L1 from L2 instead of
    // merely evicting and re-pulling on the next read.
    cache1.set("k", "v2".to_owned()).await;
    tokio::time::sleep(Duration::from_millis(150)).await;

    // try_get reads L1 ONLY (no factory, no L2 read): the fresh value is already
    // present because it was refreshed eagerly. An evict-only peer would miss here.
    let got = cache2.try_get("k", None).await;
    assert!(
        got.has_value(),
        "peer L1 was eagerly refreshed from L2, not just evicted"
    );
    assert_eq!(got.value().map(String::as_str), Some("v2"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backplane_expire_marks_peer_stale_keeping_physical() {
    let clock = Arc::new(ManualClock::default());
    let dyn_clock: Arc<dyn Clock> = clock.clone();
    let l2 = shared_l2(dyn_clock.clone());
    let serializer: Arc<dyn DistributedSerializer<String>> = Arc::new(JsonSerializer);
    let backplane: Arc<dyn Backplane> = Arc::new(InProcessBackplane::default());
    let opts = EntryOptions::new(Duration::from_secs(60));

    let build = |id: &str| -> Cache<String> {
        Cache::builder()
            .clock(dyn_clock.clone())
            .distributed(l2.clone())
            .serializer(serializer.clone())
            .backplane(backplane.clone())
            .default_options(opts.clone())
            .instance_id(id)
            .build()
    };
    let cache1 = build("node-1");
    let cache2 = build("node-2");

    cache1.set("k", "v1".to_owned()).await;
    tokio::time::sleep(Duration::from_millis(60)).await;
    // cache2 holds it fresh in L1.
    cache2
        .get_or_set("k", |ctx| async move { Ok(ctx.value("x".to_owned())) })
        .await
        .unwrap();

    cache1.expire("k").await; // logical expire → backplane Expire
    tokio::time::sleep(Duration::from_millis(120)).await;
    clock.advance(Duration::from_secs(1)); // move past the logical-expire instant

    // A plain read hides the now-stale entry...
    assert!(
        !cache2.try_get("k", None).await.has_value(),
        "Expire hides the entry from a plain read on the peer"
    );
    // ...but it is still physically present (Expire, not Remove): a stale-allowed
    // read serves it, proving the peer kept it for fail-safe.
    let stale_opts = cache2.entry_options().with_allow_stale_on_read_only(true);
    let stale = cache2.try_get("k", Some(stale_opts)).await;
    assert_eq!(
        stale.value().map(String::as_str),
        Some("v1"),
        "Expire kept the entry physically; only the logical window elapsed"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backplane_clear_remove_propagates_to_peer() {
    let clock = Arc::new(ManualClock::default());
    let dyn_clock: Arc<dyn Clock> = clock.clone();
    let l2 = shared_l2(dyn_clock.clone());
    let serializer: Arc<dyn DistributedSerializer<String>> = Arc::new(JsonSerializer);
    let backplane: Arc<dyn Backplane> = Arc::new(InProcessBackplane::default());
    let opts = EntryOptions::new(Duration::from_secs(60));

    let build = |id: &str| -> Cache<String> {
        Cache::builder()
            .clock(dyn_clock.clone())
            .distributed(l2.clone())
            .serializer(serializer.clone())
            .backplane(backplane.clone())
            .default_options(opts.clone())
            .instance_id(id)
            .build()
    };
    let cache1 = build("node-1");
    let cache2 = build("node-2");

    cache1.set("k", "v1".to_owned()).await;
    tokio::time::sleep(Duration::from_millis(60)).await;
    cache2
        .get_or_set("k", |ctx| async move { Ok(ctx.value("x".to_owned())) })
        .await
        .unwrap();
    assert!(
        cache2.try_get("k", None).await.has_value(),
        "peer holds the value before the clear"
    );

    cache1.clear(false).await; // remove-all → CLEAR_REMOVE marker over the backplane
    tokio::time::sleep(Duration::from_millis(120)).await;
    cache2.run_pending_tasks().await;

    assert!(
        !cache2.try_get("k", None).await.has_value(),
        "cross-node clear(remove-all) evicted the peer's L1 entry"
    );
}
