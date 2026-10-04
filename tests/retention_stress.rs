//! Admission/expiry invariants under mixed priority, weight and repeated churn.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use amalgam::entry::Entry;
use amalgam::{
    Clock, EntryOptions, EntryWeight, Events, JitterSample, ManualClock, MemoryAdmission,
    MemoryLimits, MemoryStore, Priority, Timestamp,
};

async fn visible(store: &MemoryStore<u64>, now: Timestamp) -> HashMap<String, u64> {
    let mut values = HashMap::with_capacity(32);
    for index in 0..32 {
        let key = format!("slot-{index}");
        if let Some(entry) = store.get_at(&key, now).await {
            values.insert(key, *entry.value());
        }
    }
    values
}

#[tokio::test]
async fn bounded_retention_churn_preserves_rejected_contents_and_releases_expired_weight() {
    let clock = Arc::new(ManualClock::new(Timestamp::from_ticks(10_000)));
    let store = MemoryStore::with_clock(
        MemoryLimits::new(Some(8), Some(32)),
        Events::default(),
        clock.clone(),
    );
    for index in 0..2048 {
        if index % 16 == 0 {
            clock.advance(Duration::from_millis(3));
            store.run_pending_tasks().await;
        }
        let now = clock.now();
        let before = visible(&store, now).await;
        let options = EntryOptions::new(Duration::from_millis((index % 31) as u64))
            .with_entry_weight(EntryWeight::new((index % 41) as u64))
            .with_priority(
                [
                    Priority::Low,
                    Priority::Normal,
                    Priority::High,
                    Priority::NeverRemove,
                ][index % 4],
            );
        let entry = Entry::try_fresh_with_jitter(
            index as u64,
            &options,
            now,
            now,
            JitterSample::new(Duration::ZERO, Duration::ZERO).expect("zero jitter"),
            Box::new([]),
            None,
            None,
        )
        .expect("valid snapshot");
        let admission = store
            .insert_at(format!("slot-{}", index % 32).into(), entry, now)
            .await;
        let after = visible(&store, now).await;
        if matches!(admission, MemoryAdmission::Rejected(_)) {
            assert_eq!(
                before, after,
                "rejection must not evict a valid existing entry"
            );
        }
        let usage = store.usage();
        assert!(usage.entries <= 8, "entry limit exceeded: {usage:?}");
        assert!(usage.weight <= 32, "weighted limit exceeded: {usage:?}");
        assert_eq!(usage.entries as usize, after.len());
    }
    clock.advance(Duration::from_secs(1));
    store.run_pending_tasks().await;
    assert!(visible(&store, clock.now()).await.is_empty());
    assert_eq!(store.usage().entries, 0);
    assert_eq!(store.usage().weight, 0);
}
