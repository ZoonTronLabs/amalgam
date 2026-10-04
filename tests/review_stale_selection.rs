use amalgam::*;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Copy)]
enum NewerSource {
    Memory,
    EqualStampMemory,
    Distributed,
}

async fn selected_stale_source(source: NewerSource) {
    let clock = Arc::new(ManualClock::default());
    let backend = Arc::new(InMemoryDistributedCache::new(clock.clone()));
    let opts = EntryOptions::new(Duration::from_secs(1))
        .with_fail_safe(true, Some(Duration::from_secs(60)), None)
        .with_allow_stale_on_read_only(true);
    let build = || {
        Cache::<u64>::builder()
            .clock(clock.clone())
            .distributed(backend.clone())
            .serializer(Arc::new(JsonSerializer))
            .default_options(opts.clone())
            .auto_recovery(RecoveryConfig {
                enabled: false,
                ..RecoveryConfig::default()
            })
            .try_build()
            .unwrap()
    };
    let cache = build();
    cache.try_set("key", 1).await.unwrap().wait().await.unwrap();
    if !matches!(source, NewerSource::EqualStampMemory) {
        clock.advance(Duration::from_secs(1));
    }
    let writer = build();
    match source {
        NewerSource::Memory | NewerSource::EqualStampMemory => {
            cache
                .try_set_full(
                    "key",
                    2,
                    Some(opts.clone().with_skip_distributed(false, true)),
                    Box::from([]),
                )
                .await
                .unwrap()
                .wait()
                .await
                .unwrap();
        }
        NewerSource::Distributed => {
            writer
                .try_set("key", 2)
                .await
                .unwrap()
                .wait()
                .await
                .unwrap();
        }
    }
    clock.advance(Duration::from_secs(2));
    let mut events = cache.events().subscribe();
    assert_eq!(cache.read("key", None).await.unwrap().value(), Some(&2));
    let level = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if let CacheEvent::OperationCompleted { level, .. } = events.recv().await.unwrap() {
                break level;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(
        level,
        Some(match source {
            NewerSource::Memory | NewerSource::EqualStampMemory => CacheLevel::Memory,
            NewerSource::Distributed => CacheLevel::Distributed,
        })
    );
    assert_eq!(
        cache.read("key", None).await.unwrap().value(),
        Some(&2),
        "selecting newer stale data must not replace it with an older L2 snapshot"
    );
    cache.shutdown().await.unwrap();
    writer.shutdown().await.unwrap();
}

#[tokio::test]
async fn newer_stale_memory_survives_older_l2_hydration() {
    selected_stale_source(NewerSource::Memory).await;
}
#[tokio::test]
async fn newer_stale_l2_keeps_distributed_provenance() {
    selected_stale_source(NewerSource::Distributed).await;
}

#[tokio::test]
async fn equal_stamp_stale_memory_survives_older_l2_hydration() {
    selected_stale_source(NewerSource::EqualStampMemory).await;
}
