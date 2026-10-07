#![cfg(feature = "redis")]
//! Mandatory native acceptance in CI: TTL namespace, atomic maxima and ownership.
use amalgam::advanced::*;
use amalgam::provider::*;
use amalgam::*;
use std::sync::Arc;
use std::time::Duration;
use {
    amalgam::redis_backend::RedisDistributedCache, amalgam::redis_backend::RedisDistributedLocker,
};
mod support {
    pub mod redis_fixture;
}

fn token() -> FactoryCancellation {
    CancellationSource::new().token()
}
fn kind() -> MarkerKind {
    MarkerKind::Tag(Tag::new("snapshot-group").unwrap())
}
fn scope(test: &str) -> CacheScope {
    CacheScope::new(
        format!("snapshot-{test}-{}:", SystemClock.now().ticks()),
        "v2",
        KeyModifierMode::Prefix,
    )
    .unwrap()
}
fn physical_key(scope: &CacheScope) -> String {
    let scope = scope
        .storage_id()
        .bytes()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    let tag = "snapshot-group"
        .bytes()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    format!("\u{1f}amalgam/v2/marker-snapshots/{scope}/t:{tag}")
}

#[tokio::test]
async fn native_snapshot_has_real_ttl_and_expiry_keeps_its_durable_fact() {
    let Some(url) = support::redis_fixture::redis_url() else {
        return;
    };
    let backend = RedisDistributedCache::connect(&url).await.unwrap();
    let journal = backend.invalidation_store().unwrap();
    let snapshots = journal.snapshot_cache().unwrap();
    let scope = scope("ttl");
    let revision = MarkerVersion::new(Timestamp::from_ticks(5));
    journal.advance(&scope, kind(), revision).await.unwrap();
    let options = EntryOptions::new(Duration::from_millis(500));
    let now = SystemClock.now();
    let snapshot = MarkerSnapshot::fresh(revision, &options, now);
    assert_eq!(
        snapshots
            .read_snapshot(&scope, &kind(), now, token())
            .await
            .unwrap(),
        MarkerSnapshotRead::Missing {
            maximum: Some(revision)
        }
    );
    assert!(matches!(
        snapshots
            .renew_snapshot(&scope, &kind(), snapshot, now, token())
            .await
            .unwrap(),
        MarkerSnapshotRenewal::Stored(_)
    ));
    let mut connection = redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let ttl: i64 = redis::cmd("PTTL")
        .arg(physical_key(&scope))
        .query_async(&mut connection)
        .await
        .unwrap();
    assert!(ttl > 0 && ttl <= 500, "actual Redis TTL {ttl}");
    backend
        .set(&physical_key(&scope), vec![99], None)
        .await
        .unwrap();
    assert_eq!(
        snapshots
            .read_snapshot(&scope, &kind(), now, token())
            .await
            .unwrap()
            .snapshot(),
        Some(snapshot),
        "ordinary reserved-area keys must be escaped"
    );
    tokio::time::sleep(Duration::from_millis(700)).await;
    let ttl: i64 = redis::cmd("PTTL")
        .arg(physical_key(&scope))
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(ttl, -2, "the backend record itself must have expired");
    assert_eq!(journal.read(&scope, &kind()).await.unwrap(), Some(revision));
    assert_eq!(
        snapshots
            .read_snapshot(&scope, &kind(), SystemClock.now(), token())
            .await
            .unwrap(),
        MarkerSnapshotRead::Missing {
            maximum: Some(revision)
        }
    );
}

#[tokio::test]
async fn native_snapshot_preserves_exact_large_revisions_and_later_insertions() {
    let Some(url) = support::redis_fixture::redis_url() else {
        return;
    };
    let backend = RedisDistributedCache::connect(&url).await.unwrap();
    let journal = backend.invalidation_store().unwrap();
    let snapshots = journal.snapshot_cache().unwrap();
    let scope = scope("ordering");
    let high = MarkerVersion::new(Timestamp::from_ticks(9_007_199_254_740_993));
    let now = SystemClock.now();
    journal.advance(&scope, kind(), high).await.unwrap();
    let low = MarkerSnapshot::fresh(
        MarkerVersion::new(Timestamp::MIN),
        &EntryOptions::new(Duration::from_secs(30)),
        now,
    );
    let MarkerSnapshotRenewal::Stored(merged) = snapshots
        .renew_snapshot(&scope, &kind(), low, now, token())
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(merged.version(), high);
    let later = MarkerSnapshot::new(
        high,
        now.saturating_add(Duration::from_secs(1)),
        now.saturating_add(Duration::from_secs(20)),
        now.saturating_add(Duration::from_secs(30)),
    )
    .unwrap();
    snapshots
        .renew_snapshot(&scope, &kind(), later, now, token())
        .await
        .unwrap();
    assert_eq!(
        snapshots
            .renew_snapshot(&scope, &kind(), merged, now, token())
            .await
            .unwrap(),
        MarkerSnapshotRenewal::KeptNewer(later)
    );
    assert_eq!(
        snapshots
            .read_snapshot(&scope, &kind(), now, token())
            .await
            .unwrap()
            .snapshot(),
        Some(later)
    );
    let max = MarkerVersion::new(Timestamp::MAX);
    journal.advance(&scope, kind(), max).await.unwrap();
    assert_eq!(
        snapshots
            .read_snapshot(&scope, &kind(), now, token())
            .await
            .unwrap()
            .maximum(),
        Some(max)
    );
}

#[tokio::test]
async fn native_snapshot_protocol_and_cancellation_are_typed_failures() {
    let Some(url) = support::redis_fixture::redis_url() else {
        return;
    };
    let backend = RedisDistributedCache::connect(&url).await.unwrap();
    let journal = backend.invalidation_store().unwrap();
    let snapshots = journal.snapshot_cache().unwrap();
    let scope = scope("protocol");
    let mut connection = redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    redis::cmd("SET")
        .arg(physical_key(&scope))
        .arg("corrupted-frame")
        .query_async::<()>(&mut connection)
        .await
        .unwrap();
    assert!(matches!(
        snapshots
            .read_snapshot(&scope, &kind(), SystemClock.now(), token())
            .await,
        Err(MarkerSnapshotCacheError::Provider(
            MarkerError::Protocol { .. }
        ))
    ));
    let source = CancellationSource::new();
    source.cancel();
    let snapshot = MarkerSnapshot::fresh(
        MarkerVersion::new(Timestamp::MIN),
        &EntryOptions::default(),
        SystemClock.now(),
    );
    assert!(matches!(
        snapshots
            .renew_snapshot(&scope, &kind(), snapshot, SystemClock.now(), source.token())
            .await,
        Err(MarkerSnapshotCacheError::Cancelled {
            reason: FactoryCancellationReason::CallerCancelled
        })
    ));
    let unchanged: String = redis::cmd("GET")
        .arg(physical_key(&scope))
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(unchanged, "corrupted-frame");
}

#[tokio::test]
async fn native_snapshot_renewal_atomically_rejects_a_replaced_lease() {
    let Some(url) = support::redis_fixture::redis_url() else {
        return;
    };
    let backend = RedisDistributedCache::connect(&url).await.unwrap();
    let journal = backend.invalidation_store().unwrap();
    let snapshots = journal.snapshot_cache().unwrap();
    let scope = scope("lease");
    let locker = Arc::new(RedisDistributedLocker::connect(url).await.unwrap());
    let key: Arc<str> = Arc::from(format!("snapshot-lock-{}", scope.storage_id()));
    let lease = acquire_owned(
        locker.clone(),
        key.clone(),
        LeaseTtl::new(Duration::from_secs(5)).unwrap(),
        Timeout::Infinite,
        AcquisitionPolicy::TokenOwned,
    )
    .await
    .unwrap()
    .unwrap();
    let proof = lease.proof().unwrap();
    let now = SystemClock.now();
    let snapshot = MarkerSnapshot::fresh(
        MarkerVersion::new(Timestamp::MIN),
        &EntryOptions::new(Duration::from_secs(30)),
        now,
    );
    assert!(matches!(
        snapshots
            .renew_snapshot_with_lease(&scope, &kind(), snapshot, now, &proof, token())
            .await
            .unwrap(),
        MarkerSnapshotRenewal::Stored(_)
    ));
    lease.release().await.unwrap();
    let replacement = acquire_owned(
        locker,
        key,
        LeaseTtl::new(Duration::from_secs(5)).unwrap(),
        Timeout::Infinite,
        AcquisitionPolicy::TokenOwned,
    )
    .await
    .unwrap()
    .unwrap();
    assert!(matches!(
        snapshots
            .renew_snapshot_with_lease(&scope, &kind(), snapshot, now, &proof, token())
            .await,
        Err(MarkerSnapshotCacheError::Lease(LeaseError::Lost))
    ));
    replacement.release().await.unwrap();
}
