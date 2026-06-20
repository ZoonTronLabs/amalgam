//! `multilevel` — two cache instances sharing an L2 + a backplane.
//!
//! This is the "hybrid cache" identity: each node has its own fast in-memory L1,
//! but they all read/write a shared distributed L2 (here an in-process stand-in
//! for Redis), and a *backplane* keeps their L1s coherent across nodes.
//!
//! We demonstrate two things:
//!
//! 1. **L2 read-through** — instance 2 serves instance 1's value out of the shared
//!    L2 without ever running its own factory.
//! 2. **Backplane invalidation** — a `remove` on instance 1 publishes a backplane
//!    message that evicts the stale copy from instance 2's L1.
//!
//! Run it with:
//!
//! ```text
//! cargo run --example multilevel
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use amalgam::{
    Backplane, Cache, Clock, DistributedCache, DistributedSerializer, EntryOptions,
    InMemoryDistributedCache, InProcessBackplane, JsonSerializer, SystemClock,
};

#[tokio::main]
async fn main() {
    // One shared clock, one shared L2, one shared backplane — all three are the
    // pieces every node points at. (We use the real SystemClock here.)
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let l2: Arc<dyn DistributedCache> = Arc::new(InMemoryDistributedCache::new(clock.clone()));
    let serializer: Arc<dyn DistributedSerializer<String>> = Arc::new(JsonSerializer);
    let backplane: Arc<dyn Backplane> = Arc::new(InProcessBackplane::default());
    let opts = EntryOptions::new(Duration::from_secs(60));

    // A small helper so both "nodes" are built identically apart from their id.
    let build = |id: &str| -> Cache<String> {
        Cache::builder()
            .clock(clock.clone())
            .distributed(l2.clone())
            .serializer(serializer.clone())
            .backplane(backplane.clone())
            .default_options(opts.clone())
            .instance_id(id)
            .build()
    };
    let node1 = build("node-1");
    let node2 = build("node-2");

    // Counts factory runs across BOTH nodes; should only ever reach 1.
    let factory_runs = Arc::new(AtomicUsize::new(0));

    // --- 1. node-1 produces the value; it lands in node-1's L1 *and* the shared L2.
    let v1 = {
        let runs = factory_runs.clone();
        node1
            .get_or_set("profile", move |ctx| async move {
                runs.fetch_add(1, Ordering::SeqCst);
                println!("  [node-1 factory] producing the value");
                Ok(ctx.value("Alice".to_owned()))
            })
            .await
            .expect("node-1 produces the value")
    };
    println!("node-1 get_or_set => {v1:?}");

    // --- 2. node-2 has an EMPTY L1, but it reads the value through the shared L2.
    //         Its factory must NOT run.
    let v2 = {
        let runs = factory_runs.clone();
        node2
            .get_or_set("profile", move |ctx| async move {
                runs.fetch_add(1, Ordering::SeqCst);
                println!("  [node-2 factory] (this should NOT run)");
                Ok(ctx.value("should-not-run".to_owned()))
            })
            .await
            .expect("node-2 reads through L2")
    };
    println!("node-2 get_or_set => {v2:?}  (came from shared L2, no factory)");
    assert_eq!(v2, "Alice", "node-2 must serve node-1's value from L2");
    assert_eq!(
        factory_runs.load(Ordering::SeqCst),
        1,
        "only node-1's factory ran"
    );

    // node-2 now has "Alice" cached in its own L1 too.
    assert_eq!(
        node2.try_get("profile", None).await.value(),
        Some(&"Alice".to_owned())
    );

    // --- 3. node-1 removes the key. The backplane broadcasts a Remove, which
    //         evicts the entry from node-2's L1 as well.
    println!("\nnode-1 removes \"profile\" — backplane will invalidate node-2…");
    node1.remove("profile").await;

    // Give the in-process backplane a moment to deliver the message.
    tokio::time::sleep(Duration::from_millis(150)).await;

    let after = node2.try_get("profile", None).await;
    println!(
        "node-2 try_get    => {:?}  (evicted by the backplane)",
        after.value()
    );
    assert!(
        !after.has_value(),
        "node-2's L1 copy must be evicted after node-1's remove propagates"
    );
    println!("OK: L2 read-through worked and the backplane invalidated the peer.");
}
