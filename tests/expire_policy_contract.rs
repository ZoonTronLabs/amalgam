use amalgam::*;
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn expiration_policy_keeps_l1_stale_and_selects_l2_retention_or_removal() {
    for policy in [
        DistributedExpirePolicy::RetainStale,
        DistributedExpirePolicy::Remove,
    ] {
        let clock = Arc::new(ManualClock::default());
        let backend = Arc::new(InMemoryDistributedCache::new(clock.clone()));
        let options = EntryOptions::new(Duration::from_secs(60)).with_fail_safe(
            true,
            Some(Duration::from_secs(120)),
            None,
        );
        let build = || {
            Cache::<u64>::builder()
                .clock(clock.clone())
                .distributed(backend.clone())
                .serializer(Arc::new(JsonSerializer))
                .default_options(options.clone())
                .try_build()
                .unwrap()
        };
        let first = build();
        first.try_set("key", 7).await.unwrap().wait().await.unwrap();
        clock.advance(Duration::from_millis(1));
        first
            .try_expire_with_policy("key", None, policy)
            .await
            .unwrap()
            .wait()
            .await
            .unwrap();
        let stale_read = options.clone().with_allow_stale_on_read_only(true);
        assert_eq!(
            first
                .read("key", Some(stale_read.clone()))
                .await
                .unwrap()
                .value(),
            Some(&7)
        );
        let second = build();
        let remote = second.read("key", Some(stale_read)).await.unwrap();
        match policy {
            DistributedExpirePolicy::RetainStale => assert_eq!(remote.value(), Some(&7)),
            DistributedExpirePolicy::Remove => assert!(!remote.has_value()),
        }
        first.shutdown().await.unwrap();
        second.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn explicit_skip_and_cancellation_still_control_distributed_removal() {
    let clock = Arc::new(ManualClock::default());
    let backend = Arc::new(InMemoryDistributedCache::new(clock.clone()));
    let cache = Cache::<u64>::builder()
        .clock(clock.clone())
        .distributed(backend.clone())
        .serializer(Arc::new(JsonSerializer))
        .try_build()
        .unwrap();
    cache.try_set("key", 7).await.unwrap().wait().await.unwrap();
    let source = CancellationSource::new();
    source.cancel();
    assert!(matches!(
        cache
            .try_expire_with_policy_cancellable(
                "key",
                None,
                DistributedExpirePolicy::Remove,
                source.token()
            )
            .await,
        Err(Error::OperationCancelled {
            reason: FactoryCancellationReason::CallerCancelled
        })
    ));
    assert_eq!(cache.read("key", None).await.unwrap().value(), Some(&7));
    let report = cache
        .try_expire_with_policy(
            "key",
            Some(EntryOptions::default().with_skip_distributed(false, true)),
            DistributedExpirePolicy::Remove,
        )
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(matches!(
        report.distributed,
        EffectOutcome::Skipped(SkipReason::Policy)
    ));
    assert!(backend.get("v2:key").await.unwrap().is_some());
    cache.shutdown().await.unwrap();
}
