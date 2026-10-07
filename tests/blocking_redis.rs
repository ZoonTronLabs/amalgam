#![cfg(feature = "redis")]
//! Native caller-thread contracts on actual Redis/Valkey storage and notifications.
use amalgam::redis_backend::{RedisBackplane, RedisDistributedCache};
use amalgam::*;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};
mod support {
    pub mod redis_fixture;
}

fn driver() -> BlockingRuntime {
    BlockingRuntime::with_workers(NonZeroUsize::MIN, NonZeroUsize::MIN).unwrap()
}
fn node(
    driver: BlockingRuntime,
    backend: Arc<RedisDistributedCache>,
    backplane: Arc<RedisBackplane>,
    prefix: &str,
) -> BlockingCache<Option<u64>> {
    BlockingCache::on_runtime(
        Cache::builder()
            .key_prefix(prefix)
            .distributed(backend)
            .serializer(Arc::new(JsonSerializer))
            .backplane(backplane),
        driver,
    )
    .unwrap()
}
fn miss_after_notification(cache: &BlockingCache<Option<u64>>, key: &str) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while cache.read(key, None).unwrap().has_value() {
        assert!(
            Instant::now() < deadline,
            "acknowledged native peer must observe invalidation"
        );
        std::thread::sleep(Duration::from_millis(2));
    }
}
#[test]
fn native_receipts_null_tags_clear_and_expiration_use_real_storage() {
    let Some(url) = support::redis_fixture::redis_url() else {
        return;
    };
    let first_driver = driver();
    let second_driver = driver();
    let backend = Arc::new(
        first_driver
            .run(RedisDistributedCache::connect(url.clone()))
            .unwrap(),
    );
    let prefix = format!("native-contract-{}:", SystemClock.now().ticks());
    let channel = format!("{prefix}notifications");
    let first_notifications = Arc::new(
        first_driver
            .run(RedisBackplane::connect_with_channel(
                url.clone(),
                channel.clone(),
            ))
            .unwrap(),
    );
    let second_notifications = Arc::new(
        second_driver
            .run(RedisBackplane::connect_with_channel(url, channel))
            .unwrap(),
    );
    let first = node(
        first_driver.clone(),
        backend.clone(),
        first_notifications,
        &prefix,
    );
    let peer = node(
        second_driver.clone(),
        backend.clone(),
        second_notifications,
        &prefix,
    );
    assert!(matches!(
        first.ready().unwrap(),
        BackplaneReadiness::Acknowledged(_)
    ));
    assert!(matches!(
        peer.ready().unwrap(),
        BackplaneReadiness::Acknowledged(_)
    ));
    let tag = Tag::new("native-null").unwrap();
    let constant = first
        .get_or_set("null", amalgam::source::value(None))
        .options(|_| {
            EntryOptions::new(Duration::from_secs(60))
                .with_factory_timeouts(Timeout::Infinite, Timeout::After(Duration::ZERO), false)
                .with_allow_background_distributed_operations(true)
        })
        .tags([tag.clone()])
        .cancellation(CancellationSource::new().token())
        .with_receipt()
        .execute()
        .unwrap();
    assert_eq!(constant.value, None);
    let BlockingCommitReceipt::Mutation(receipt) = constant.commit else {
        panic!("cold supplied value must expose its actual commit");
    };
    assert!(matches!(receipt, BlockingMutationReceipt::Scheduled(_)));
    receipt.wait().unwrap();
    assert_eq!(peer.read("null", None).unwrap().into_value(), Some(None));
    first
        .remove_by_tag(tag)
        .cancellation(CancellationSource::new().token())
        .with_receipt()
        .execute()
        .unwrap()
        .wait()
        .unwrap();
    miss_after_notification(&peer, "null");
    first.try_set("remove-l2", Some(7)).unwrap().wait().unwrap();
    assert_eq!(
        peer.read("remove-l2", None).unwrap().into_value(),
        Some(Some(7))
    );
    peer.expire("remove-l2")
        .distributed_policy(DistributedExpirePolicy::Remove)
        .cancellation(CancellationSource::new().token())
        .with_receipt()
        .execute()
        .unwrap()
        .wait()
        .unwrap();
    let cold = node(
        driver(),
        backend,
        Arc::new(
            first_driver
                .run(RedisBackplane::connect_with_channel(
                    support::redis_fixture::redis_url().unwrap(),
                    format!("{prefix}cold-notifications"),
                ))
                .unwrap(),
        ),
        &prefix,
    );
    cold.ready().unwrap();
    assert!(!cold.read("remove-l2", None).unwrap().has_value());
    first.try_set("clear", Some(9)).unwrap().wait().unwrap();
    assert_eq!(
        peer.read("clear", None).unwrap().into_value(),
        Some(Some(9))
    );
    first
        .clear(ClearMode::Remove)
        .cancellation(CancellationSource::new().token())
        .with_receipt()
        .execute()
        .unwrap()
        .wait()
        .unwrap();
    miss_after_notification(&peer, "clear");
    assert!(!cold.read("clear", None).unwrap().has_value());
    let cancelled = CancellationSource::new();
    cancelled.cancel();
    assert!(matches!(
        first
            .get_or_set("cancelled", amalgam::source::value(Some(99)))
            .cancellation(cancelled.token())
            .execute(),
        Err(Error::OperationCancelled {
            reason: FactoryCancellationReason::CallerCancelled
        })
    ));
    assert!(!cold.read("cancelled", None).unwrap().has_value());
    cold.shutdown().unwrap();
    peer.shutdown().unwrap();
    first.shutdown().unwrap();
}
