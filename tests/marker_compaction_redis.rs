#![cfg(feature = "redis")]
//! Lower per-node limits must report the actual clear maximum after compaction.
use amalgam::advanced::*;
use amalgam::provider::*;
use amalgam::*;
use std::sync::Arc;
use std::time::Duration;
use {amalgam::redis_backend::RedisDistributedCache, amalgam::redis_backend::RedisIoOptions};
mod support {
    pub mod redis_fixture;
}

fn time(seconds: i64) -> Timestamp {
    Timestamp::from_ticks(seconds * 10_000_000)
}
fn version(seconds: i64) -> MarkerVersion {
    MarkerVersion::new(time(seconds))
}
fn options() -> EntryOptions {
    EntryOptions::tag_defaults()
        .with_distributed_duration(Duration::from_secs(5))
        .with_fail_safe(false, None, None)
        .with_allow_background_distributed_operations(false)
        .with_allow_background_backplane_operations(false)
        .with_skip_backplane_notifications(true)
        .with_rethrow_distributed_exceptions(false)
}
async fn seeded(url: &str, label: &str) -> (CacheScope, Arc<dyn InvalidationStore>) {
    let scope = CacheScope::new(
        format!("compaction-{label}-{}:", SystemClock.now().ticks()),
        "v2",
        KeyModifierMode::Prefix,
    )
    .unwrap();
    let backend = RedisDistributedCache::connect(url).await.unwrap();
    let store = backend.invalidation_store().unwrap();
    for (tag, revision) in [("one", 20), ("two", 21)] {
        store
            .advance(
                &scope,
                MarkerKind::Tag(Tag::new(tag).unwrap()),
                version(revision),
            )
            .await
            .unwrap();
    }
    let narrow = Arc::new(
        RedisDistributedCache::connect_with_options(
            url,
            RedisIoOptions::default(),
            MarkerStoreLimits::new(1, 1024).unwrap(),
        )
        .await
        .unwrap(),
    );
    (scope, narrow.invalidation_store().unwrap())
}

#[tokio::test]
async fn native_clear_compaction_returns_the_final_authoritative_maximum_once() {
    let Some(url) = support::redis_fixture::redis_url() else {
        return;
    };
    for candidate in [10, 30] {
        let (scope, store) = seeded(&url, &format!("maximum-{candidate}")).await;
        let outcome = store
            .advance(&scope, MarkerKind::ClearRemove, version(candidate))
            .await
            .unwrap();
        let actual = store
            .read(&scope, &MarkerKind::ClearRemove)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            outcome.marker().version(),
            actual,
            "returned primary clear must equal its committed maximum, even after lower-limit compaction"
        );
        assert_eq!(actual, version(candidate.max(21)));
        assert!(
            matches!(outcome, MarkerAdvanceOutcome::Advanced(_)),
            "clear-remove compaction has one primary obligation, not a duplicate additional clear"
        );
        assert!(
            store
                .read(&scope, &MarkerKind::Tag(Tag::new("one").unwrap()))
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .read(&scope, &MarkerKind::Tag(Tag::new("two").unwrap()))
                .await
                .unwrap()
                .is_none()
        );
    }
}

fn journal_key(scope: &CacheScope) -> String {
    let encoded: String = scope
        .storage_id()
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    format!("\u{1f}amalgam/v2/markers/{encoded}")
}

#[tokio::test]
async fn native_captured_clear_recovery_survives_lower_limit_compaction() {
    let Some(url) = support::redis_fixture::redis_url() else {
        return;
    };
    let (scope, _) = seeded(&url, "captured").await;
    let backend = Arc::new(
        RedisDistributedCache::connect_with_options(
            &url,
            RedisIoOptions::default(),
            MarkerStoreLimits::new(1, 1024).unwrap(),
        )
        .await
        .unwrap(),
    );
    let store = backend.invalidation_store().unwrap();
    let clock = Arc::new(ManualClock::new(time(10)));
    let cache = Cache::<u64>::builder()
        .clock(clock.clone())
        .key_prefix(scope.prefix())
        .distributed(backend)
        .serializer(Arc::new(JsonSerializer))
        .tags_default_options(options())
        .marker_read_policy(MarkerReadPolicy::OptionsControlled)
        .marker_lifecycle_policy(MarkerLifecyclePolicy::CachedSnapshots)
        .reconciliation_policy(ReconciliationPolicy::Periodic(Duration::from_secs(3600)))
        .auto_recovery(RecoveryConfig {
            delay: Duration::from_millis(200),
            max_retries: Some(0),
            ..RecoveryConfig::default()
        })
        .try_build()
        .unwrap();
    let mut connection = redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let key = journal_key(&scope);
    let stash = format!("{key}:saved-by-test");
    let _: () = redis::cmd("RENAME")
        .arg(&key)
        .arg(&stash)
        .query_async(&mut connection)
        .await
        .unwrap();
    let _: i64 = redis::cmd("LPUSH")
        .arg(&key)
        .arg("actual-wrong-type")
        .query_async(&mut connection)
        .await
        .unwrap();
    let report = cache
        .clear(ClearMode::Remove)
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(matches!(
        report.distributed,
        EffectOutcome::RecoveryQueued { .. }
    ));
    let ticket = cache
        .marker_recovery_ticket(&MarkerKind::ClearRemove)
        .unwrap();
    assert!(matches!(ticket.work(), RecoveryWork::MarkerMutation(work)
        if matches!(work.stage(), MarkerMutationStage::Advance { .. })));
    clock.set(time(12));
    // RENAME replaces the fault atomically. DEL followed by RENAME lets a
    // concurrent replay commit into an empty journal before the saved journal
    // overwrites that commit; the fixture itself would roll back authority.
    let _: () = redis::cmd("RENAME")
        .arg(&stash)
        .arg(&key)
        .query_async(&mut connection)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let read = store
                .snapshot_cache()
                .unwrap()
                .read_snapshot(
                    &scope,
                    &MarkerKind::ClearRemove,
                    time(12),
                    CancellationSource::new().token(),
                )
                .await
                .unwrap();
            if let Some(snapshot) = read.snapshot() {
                assert_eq!(snapshot.version(), version(21));
                assert_eq!(snapshot.created(), time(10));
                assert_eq!(snapshot.logical_expiration(), time(15));
                assert_eq!(snapshot.physical_expiration(), time(15));
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("committed global clear must reach snapshot population after compaction");
    assert_eq!(
        store.read(&scope, &MarkerKind::ClearRemove).await.unwrap(),
        Some(version(21))
    );
    cache.shutdown().await.unwrap();
}
