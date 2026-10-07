#![cfg(feature = "redis")]
//! Recovery through real Redis Lua failures, original lifetime and native TTL.
use amalgam::redis_backend::RedisDistributedCache;
use amalgam::*;
use std::sync::Arc;
use std::time::Duration;
mod support {
    pub mod redis_fixture;
}

fn time(seconds: i64) -> Timestamp {
    Timestamp::from_ticks(seconds * 10_000_000)
}
fn group() -> MarkerKind {
    MarkerKind::Tag(Tag::new("recovery-group").unwrap())
}
fn token() -> FactoryCancellation {
    CancellationSource::new().token()
}
fn options() -> EntryOptions {
    EntryOptions::tag_defaults()
        .with_memory_duration(Duration::from_secs(2))
        .with_distributed_duration(Duration::from_secs(5))
        .with_fail_safe(false, None, None)
        .with_allow_background_distributed_operations(false)
        .with_allow_background_backplane_operations(false)
        .with_skip_backplane_notifications(true)
        .with_rethrow_distributed_exceptions(false)
        .with_rethrow_serialization_exceptions(false)
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
fn journal_key(scope: &CacheScope) -> String {
    format!(
        "\u{1f}amalgam/v2/markers/{}",
        hex(scope.storage_id().as_bytes())
    )
}
fn snapshot_key(scope: &CacheScope, kind: &MarkerKind) -> String {
    let field = match kind {
        MarkerKind::Tag(tag) => format!("t:{}", hex(tag.as_str().as_bytes())),
        MarkerKind::ClearExpire => "e".to_owned(),
        MarkerKind::ClearRemove => "r".to_owned(),
    };
    format!(
        "\u{1f}amalgam/v2/marker-snapshots/{}/{field}",
        hex(scope.storage_id().as_bytes())
    )
}
struct Fixture {
    clock: Arc<ManualClock>,
    cache: Cache<u64>,
    scope: CacheScope,
    journal: Arc<dyn InvalidationStore>,
    connection: redis::aio::MultiplexedConnection,
}
async fn fixture(url: &str, name: &str) -> Fixture {
    let scope = CacheScope::new(
        format!("marker-recovery-{name}-{}:", SystemClock.now().ticks()),
        "v2",
        KeyModifierMode::Prefix,
    )
    .unwrap();
    let clock = Arc::new(ManualClock::new(time(10)));
    let backend = Arc::new(RedisDistributedCache::connect(url).await.unwrap());
    let journal = backend.invalidation_store().unwrap();
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
            delay: Duration::from_millis(30),
            ..RecoveryConfig::default()
        })
        .try_build()
        .unwrap();
    let connection = redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    Fixture {
        clock,
        cache,
        scope,
        journal,
        connection,
    }
}
async fn mutate(
    cache: &Cache<u64>,
    kind: &MarkerKind,
    options: Option<EntryOptions>,
) -> Result<CommitReport> {
    let receipt = match kind {
        MarkerKind::Tag(tag) => {
            cache
                .remove_by_tag(tag.clone())
                .options(|defaults| (options).unwrap_or(defaults))
                .with_receipt()
                .await?
        }
        MarkerKind::ClearExpire => {
            cache
                .clear(ClearMode::Expire)
                .options(|defaults| (options).unwrap_or(defaults))
                .with_receipt()
                .await?
        }
        MarkerKind::ClearRemove => {
            cache
                .clear(ClearMode::Remove)
                .options(|defaults| (options).unwrap_or(defaults))
                .with_receipt()
                .await?
        }
    };
    receipt.wait().await
}
fn backend_queued(effect: &EffectOutcome) -> bool {
    match effect {
        EffectOutcome::RecoveryQueued { cause } => {
            matches!(cause, Error::Marker(MarkerError::Backend { .. }))
        }
        EffectOutcome::Batch(batch) => batch.stages().iter().any(backend_queued),
        EffectOutcome::Applied
        | EffectOutcome::NotConfigured
        | EffectOutcome::Skipped(_)
        | EffectOutcome::FailedSuppressed { .. } => false,
    }
}
async fn finish(f: &mut Fixture, kind: &MarkerKind) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while f.cache.pending_recovery() != 0 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    let snapshot = f
        .journal
        .snapshot_cache()
        .unwrap()
        .read_snapshot(&f.scope, kind, time(12), token())
        .await
        .unwrap()
        .snapshot()
        .unwrap();
    assert_eq!(snapshot.version(), MarkerVersion::new(time(10)));
    assert_eq!(snapshot.created(), time(10));
    assert_eq!(snapshot.logical_expiration(), time(15));
    assert_eq!(snapshot.physical_expiration(), time(15));
    let ttl: i64 = redis::cmd("PTTL")
        .arg(snapshot_key(&f.scope, kind))
        .query_async(&mut f.connection)
        .await
        .unwrap();
    assert!(
        ttl > 0 && ttl <= 3000,
        "retry must use actual remaining three seconds, TTL={ttl}"
    );
    assert_eq!(
        f.journal.read(&f.scope, kind).await.unwrap(),
        Some(MarkerVersion::new(time(10)))
    );
    f.cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn native_snapshot_storage_fault_retries_original_remaining_ttl() {
    let Some(url) = support::redis_fixture::redis_url() else {
        return;
    };
    for (name, kind) in [
        ("tag", group()),
        ("expire", MarkerKind::ClearExpire),
        ("remove", MarkerKind::ClearRemove),
    ] {
        let mut f = fixture(&url, name).await;
        let key = snapshot_key(&f.scope, &kind);
        let _: i64 = redis::cmd("LPUSH")
            .arg(&key)
            .arg("real-wrong-type")
            .query_async(&mut f.connection)
            .await
            .unwrap();
        let report = mutate(
            &f.cache,
            &kind,
            Some(options().with_rethrow_serialization_exceptions(true)),
        )
        .await
        .unwrap();
        assert!(backend_queued(&report.distributed));
        assert_eq!(
            f.journal.read(&f.scope, &kind).await.unwrap(),
            Some(MarkerVersion::new(time(10)))
        );
        let ticket = f.cache.marker_snapshot_recovery_ticket(&kind).unwrap();
        let RecoveryWork::MarkerSnapshot(work) = ticket.work() else {
            panic!("snapshot work required");
        };
        assert_eq!(work.snapshot().created(), time(10));
        f.clock.set(time(12));
        let _: i64 = redis::cmd("DEL")
            .arg(key)
            .query_async(&mut f.connection)
            .await
            .unwrap();
        finish(&mut f, &kind).await;
    }
}

#[tokio::test]
async fn native_snapshot_protocol_fault_has_independent_policy_and_replay() {
    let Some(url) = support::redis_fixture::redis_url() else {
        return;
    };
    let mut f = fixture(&url, "protocol").await;
    let key = snapshot_key(&f.scope, &group());
    let _: () = redis::cmd("SET")
        .arg(&key)
        .arg("invalid-snapshot-frame")
        .query_async(&mut f.connection)
        .await
        .unwrap();
    let report = mutate(
        &f.cache,
        &group(),
        Some(options().with_rethrow_distributed_exceptions(true)),
    )
    .await
    .unwrap();
    let EffectOutcome::Batch(batch) = report.distributed else {
        panic!("durable and snapshot effects required");
    };
    assert!(batch.stages().iter().any(|effect| matches!(
        effect,
        EffectOutcome::RecoveryQueued {
            cause: Error::Marker(
                MarkerError::Protocol { .. } | MarkerError::ProtocolWithSource { .. }
            )
        }
    )));
    assert!(f.cache.marker_snapshot_recovery_ticket(&group()).is_some());
    f.clock.set(time(12));
    let _: i64 = redis::cmd("DEL")
        .arg(key)
        .query_async(&mut f.connection)
        .await
        .unwrap();
    finish(&mut f, &group()).await;
}

#[tokio::test]
async fn native_durable_advance_fault_replays_original_policy_after_real_wrong_type() {
    let Some(url) = support::redis_fixture::redis_url() else {
        return;
    };
    let mut f = fixture(&url, "advance").await;
    let key = journal_key(&f.scope);
    let _: () = redis::cmd("SET")
        .arg(&key)
        .arg("real-wrong-type")
        .query_async(&mut f.connection)
        .await
        .unwrap();
    let report = mutate(&f.cache, &group(), None).await.unwrap();
    assert!(backend_queued(&report.distributed));
    let ticket = f.cache.marker_recovery_ticket(&group()).unwrap();
    let RecoveryWork::MarkerMutation(work) = ticket.work() else {
        panic!("mutation work required");
    };
    assert!(
        matches!(work.stage(), MarkerMutationStage::Advance { snapshot, .. } if snapshot.created() == time(10))
    );
    f.clock.set(time(12));
    let _: i64 = redis::cmd("DEL")
        .arg(key)
        .query_async(&mut f.connection)
        .await
        .unwrap();
    finish(&mut f, &group()).await;
}
