//! Tests for the FusionCache-parity gap-closes: the constant-value
//! `get_or_set_value` overload and the `Eviction` event.

use std::time::Duration;

use amalgam::{Cache, CacheEvent, EntryOptions};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_or_set_value_sets_then_returns_existing() {
    let cache: Cache<i32> = Cache::new();

    // Absent → stores and returns the constant.
    let first = cache.get_or_set_value("k", 7, None).await.unwrap();
    assert_eq!(first, 7);

    // Present → returns the existing value, ignoring the new constant.
    let second = cache.get_or_set_value("k", 999, None).await.unwrap();
    assert_eq!(second, 7);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn size_eviction_emits_eviction_event() {
    // Capacity 1 ⇒ inserting more entries evicts older ones by size.
    let cache: Cache<i32> = Cache::builder().max_capacity(1).build();
    let mut events = cache.events().subscribe();

    let long = EntryOptions::new(Duration::from_secs(3600));
    for i in 0..20 {
        cache.set_full(format!("k{i}"), i, Some(long.clone()), Box::from([])).await;
    }
    // Force moka to run its eviction maintenance deterministically.
    cache.run_pending_tasks().await;

    let mut saw_eviction = false;
    for _ in 0..64 {
        match events.try_recv() {
            Ok(CacheEvent::Eviction { .. }) => {
                saw_eviction = true;
                break;
            }
            Ok(_) => {}
            Err(_) => break,
        }
    }
    assert!(saw_eviction, "a size-based eviction fired an Eviction event");
}
