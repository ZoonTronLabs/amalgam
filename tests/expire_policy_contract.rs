#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

use std::sync::Arc;
use std::time::Duration;

use amalgam::advanced::{DistributedExpirePolicy, EffectOutcome, SkipReason};
use amalgam::provider::{DistributedCache, InMemoryDistributedCache, JsonSerializer, ManualClock};
use amalgam::{Cache, CancellationSource, EntryOptions, Error, FactoryCancellationReason};

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
        let _ = first
            .set("key", 7)
            .with_receipt()
            .await
            .unwrap()
            .wait()
            .await
            .unwrap();
        clock.advance(Duration::from_millis(1));
        let _ = first
            .expire("key")
            .distributed_policy(policy)
            .with_receipt()
            .await
            .unwrap()
            .wait()
            .await
            .unwrap();
        let stale_read = options.clone().with_allow_stale_on_read_only(true);
        assert_eq!(
            first
                .try_get("key")
                .options(|_| stale_read.clone())
                .await
                .unwrap()
                .as_ref(),
            Some(&7),
            "contract: expiration policy keeps l1 stale and selects l2 retention or removal"
        );
        let second = build();
        let remote = second.try_get("key").options(|_| stale_read).await.unwrap();
        match policy {
            DistributedExpirePolicy::RetainStale => assert_eq!(
                remote.as_ref(),
                Some(&7),
                "contract: expiration policy keeps l1 stale and selects l2 retention or removal"
            ),
            DistributedExpirePolicy::Remove => assert!(
                remote.is_none(),
                "contract: expiration policy keeps l1 stale and selects l2 retention or removal"
            ),
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
    let _ = cache
        .set("key", 7)
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    let source = CancellationSource::new();
    source.cancel();
    assert!(
        matches!(
            cache
                .expire("key")
                .distributed_policy(DistributedExpirePolicy::Remove)
                .cancellation(source.token())
                .with_receipt()
                .await,
            Err(Error::OperationCancelled {
                reason: FactoryCancellationReason::CallerCancelled
            })
        ),
        "contract: explicit skip and cancellation still control distributed removal"
    );
    assert_eq!(
        cache.try_get("key").await.unwrap().as_ref(),
        Some(&7),
        "contract: explicit skip and cancellation still control distributed removal"
    );
    let report = cache
        .expire("key")
        .options(|_| EntryOptions::default().with_skip_distributed(false, true))
        .distributed_policy(DistributedExpirePolicy::Remove)
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(
        matches!(
            report.distributed,
            EffectOutcome::Skipped(SkipReason::Policy)
        ),
        "contract: explicit skip and cancellation still control distributed removal"
    );
    assert!(
        backend.get("v2:key").await.unwrap().is_some(),
        "contract: explicit skip and cancellation still control distributed removal"
    );
    cache.shutdown().await.unwrap();
}
