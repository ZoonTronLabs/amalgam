//! Distributed acceptance and separately named FusionCache compatibility observations.

#[cfg(feature = "redis")]
#[path = "support/redis_fixture.rs"]
mod redis_fixture;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use amalgam::{
    Cache, CacheEvent, EntryOptions, Error, FactoryError, RecoveryConfig, Result, Tag, Timeout,
    Timestamp, advanced::AutoRecoveryService, advanced::RecoveryAction, advanced::RecoveryItem,
    provider::Backplane, provider::BackplaneAction, provider::BackplaneMessage, provider::Clock,
    provider::DistributedCache, provider::DistributedEntry, provider::DistributedSerializer,
    provider::InMemoryDistributedCache, provider::InProcessBackplane, provider::JsonSerializer,
    provider::ManualClock, provider::RecoveryExecutor,
};
use async_trait::async_trait;
use tokio::sync::{Notify, Semaphore};

fn opts() -> EntryOptions {
    EntryOptions::new(Duration::from_secs(60)).with_allow_background_backplane_operations(false)
}

fn no_recovery() -> RecoveryConfig {
    RecoveryConfig {
        enabled: false,
        ..RecoveryConfig::default()
    }
}

fn build(clock: Arc<dyn Clock>, l2: Arc<dyn DistributedCache>) -> Cache<i32> {
    Cache::builder()
        .clock(clock)
        .distributed(l2)
        .serializer(Arc::new(JsonSerializer))
        .default_options(opts())
        .auto_recovery(no_recovery())
        .try_build()
        .unwrap()
}

fn message(key: &str, action: BackplaneAction, timestamp: Timestamp) -> BackplaneMessage {
    BackplaneMessage {
        source_id: Arc::from("remote"),
        timestamp,
        action,
        key: Arc::from(format!("v2:{key}")),
    }
}

async fn received(events: &mut tokio::sync::broadcast::Receiver<CacheEvent>, key: &str) {
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if matches!(events.recv().await.unwrap(), CacheEvent::MessageReceived { key: k } if &*k == key) {
                return;
            }
        }
    }).await.unwrap();
    tokio::task::yield_now().await;
}

#[tokio::test]
async fn audit_tag_marker_survives_a_fresh_node_without_backplane() {
    let clock = Arc::new(ManualClock::default());
    let l2 = Arc::new(InMemoryDistributedCache::new(clock.clone()));
    let writer = build(clock.clone(), l2.clone());
    writer
        .set("k", 1)
        .tags([Tag::new("group").unwrap()])
        .await
        .unwrap();
    clock.advance(Duration::from_secs(1));
    writer.remove_by_tag("group").await.unwrap();
    let fresh_node = build(clock, l2);
    let actual = fresh_node
        .get_or_set("k", amalgam::source::value(2))
        .await
        .unwrap();
    assert_eq!(
        actual, 2,
        "the tag invalidation must remain effective in shared L2"
    );
}

#[tokio::test]
async fn audit_clear_marker_survives_a_fresh_node_without_backplane() {
    let clock = Arc::new(ManualClock::default());
    let l2 = Arc::new(InMemoryDistributedCache::new(clock.clone()));
    let writer = build(clock.clone(), l2.clone());
    writer.set("k", 1).await.unwrap();
    clock.advance(Duration::from_secs(1));
    writer.clear(amalgam::ClearMode::Remove).await.unwrap();
    let fresh_node = build(clock, l2);
    let actual = fresh_node
        .get_or_set("k", amalgam::source::value(2))
        .await
        .unwrap();
    assert_eq!(
        actual, 2,
        "clear must prevent a new node reading the uncleared L2 value"
    );
}

#[tokio::test]
async fn audit_expire_from_cold_node_updates_l2() {
    let clock = Arc::new(ManualClock::default());
    let l2 = Arc::new(InMemoryDistributedCache::new(clock.clone()));
    let writer = build(clock.clone(), l2.clone());
    writer.set("k", 1).await.unwrap();
    clock.advance(Duration::from_secs(1));
    let invalidator = build(clock.clone(), l2.clone());
    invalidator.expire("k").await.unwrap();
    let reader = build(clock, l2);
    let actual = reader
        .get_or_set("k", amalgam::source::value(2))
        .await
        .unwrap();
    assert_eq!(
        actual, 2,
        "expire must affect an L2 entry even when local L1 is empty"
    );
}

#[tokio::test]
async fn audit_l2_uses_its_own_logical_duration() {
    let clock = Arc::new(ManualClock::default());
    let l2 = Arc::new(InMemoryDistributedCache::new(clock.clone()));
    let custom = EntryOptions::new(Duration::from_secs(1))
        .with_distributed_duration(Duration::from_secs(10));
    let writer = build(clock.clone(), l2.clone());
    writer
        .set("k", 1)
        .options(|_| custom.clone())
        .await
        .unwrap();
    clock.advance(Duration::from_secs(2));
    let reader = build(clock, l2);
    let actual = reader
        .get_or_set("k", amalgam::source::value(2))
        .options(|_| custom)
        .await
        .unwrap();
    assert_eq!(
        actual, 1,
        "L2 must still be logically fresh during its ten-second duration"
    );
}

struct PausedReplay {
    started: Notify,
    release: Semaphore,
}

#[async_trait]
impl RecoveryExecutor for PausedReplay {
    async fn replay(&self, _item: &RecoveryItem) -> Result<()> {
        self.started.notify_one();
        self.release.acquire().await.unwrap().forget();
        Ok(())
    }
}

fn recovery_item(action: RecoveryAction, ticks: i64) -> RecoveryItem {
    RecoveryItem {
        key: Arc::from("k"),
        action,
        timestamp: Timestamp::from_ticks(ticks),
        expires_at: Timestamp::from_ticks(1_000_000),
        remaining_retries: None,
    }
}

#[tokio::test]
async fn audit_new_recovery_item_survives_older_replay_completion() {
    let queue = AutoRecoveryService::new(
        RecoveryConfig::default(),
        Arc::new(ManualClock::new(Timestamp::from_ticks(0))),
    );
    queue.enqueue(recovery_item(RecoveryAction::Set, 1));
    let executor = Arc::new(PausedReplay {
        started: Notify::new(),
        release: Semaphore::new(0),
    });
    let drain_queue = queue.clone();
    let drain_executor = executor.clone();
    let drain = tokio::spawn(async move { drain_queue.drain_once(drain_executor.as_ref()).await });
    tokio::time::timeout(Duration::from_secs(1), executor.started.notified())
        .await
        .unwrap();
    queue.enqueue(recovery_item(RecoveryAction::Remove, 2));
    executor.release.add_permits(1);
    drain.await.unwrap();
    assert_eq!(
        queue.len(),
        1,
        "completion of Set@1 must preserve the queued Remove@2"
    );
}

struct FailingRemove {
    inner: InMemoryDistributedCache,
    down: AtomicBool,
    removed: Notify,
}

#[async_trait]
impl DistributedCache for FailingRemove {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.inner.get(key).await
    }
    async fn set(&self, key: &str, bytes: Vec<u8>, ttl: Option<Duration>) -> Result<()> {
        self.inner.set(key, bytes, ttl).await
    }
    async fn remove(&self, key: &str) -> Result<()> {
        if self.down.load(Ordering::SeqCst) {
            return Err(Error::Distributed("remove down".into()));
        }
        self.inner.remove(key).await?;
        self.removed.notify_one();
        Ok(())
    }
}

#[tokio::test]
async fn audit_successful_set_supersedes_pending_recovery_remove() {
    let clock = Arc::new(ManualClock::default());
    let l2 = Arc::new(FailingRemove {
        inner: InMemoryDistributedCache::new(clock.clone()),
        down: AtomicBool::new(true),
        removed: Notify::new(),
    });
    let cache: Cache<i32> = Cache::builder()
        .clock(clock.clone())
        .distributed(l2.clone())
        .serializer(Arc::new(JsonSerializer))
        .backplane(Arc::new(InProcessBackplane::default()))
        .default_options(opts())
        .auto_recovery(RecoveryConfig {
            delay: Duration::from_millis(50),
            ..RecoveryConfig::default()
        })
        .try_build()
        .unwrap();
    cache.set("k", 1).await.unwrap();
    clock.advance(Duration::from_secs(1));
    cache.remove("k").await.unwrap();
    clock.advance(Duration::from_secs(1));
    cache.set("k", 2).await.unwrap();
    l2.down.store(false, Ordering::SeqCst);
    assert!(
        tokio::time::timeout(Duration::from_millis(150), l2.removed.notified())
            .await
            .is_err(),
        "superseded remove must not execute"
    );
    assert!(
        l2.get("v2:k").await.unwrap().is_some(),
        "recovery of the older Remove must not erase Set@2"
    );
}

struct PausedRead {
    inner: InMemoryDistributedCache,
    pause_next: AtomicBool,
    started: Notify,
    release: Semaphore,
}

#[async_trait]
impl DistributedCache for PausedRead {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let snapshot = self.inner.get(key).await?;
        if self.pause_next.swap(false, Ordering::SeqCst) {
            self.started.notify_one();
            self.release.acquire().await.unwrap().forget();
        }
        Ok(snapshot)
    }
    async fn set(&self, key: &str, bytes: Vec<u8>, ttl: Option<Duration>) -> Result<()> {
        self.inner.set(key, bytes, ttl).await
    }
    async fn remove(&self, key: &str) -> Result<()> {
        self.inner.remove(key).await
    }
}

#[tokio::test]
async fn audit_passive_refresh_does_not_resurrect_after_remove() {
    let clock = Arc::new(ManualClock::default());
    let l2 = Arc::new(PausedRead {
        inner: InMemoryDistributedCache::new(clock.clone()),
        pause_next: AtomicBool::new(false),
        started: Notify::new(),
        release: Semaphore::new(0),
    });
    let writer = build(clock.clone(), l2.clone());
    writer.set("k", 1).await.unwrap();
    let backplane = Arc::new(InProcessBackplane::default());
    let reader: Cache<i32> = Cache::builder()
        .clock(clock.clone())
        .distributed(l2.clone())
        .serializer(Arc::new(JsonSerializer))
        .backplane(backplane.clone())
        .default_options(opts())
        .auto_recovery(no_recovery())
        .try_build()
        .unwrap();
    reader
        .get_or_set("k", amalgam::source::value(0))
        .await
        .unwrap();
    let mut events = reader.events().subscribe();
    l2.pause_next.store(true, Ordering::SeqCst);
    clock.advance(Duration::from_secs(1));
    backplane
        .publish(message("k", BackplaneAction::Set, clock.now()))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), l2.started.notified())
        .await
        .unwrap();
    clock.advance(Duration::from_secs(1));
    l2.remove("v2:k").await.unwrap();
    backplane
        .publish(message("k", BackplaneAction::Remove, clock.now()))
        .await
        .unwrap();
    received(&mut events, "k").await;
    tokio::time::timeout(Duration::from_secs(1), async {
        while reader.try_get("k", None).await.has_value() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    l2.release.add_permits(1);
    let _ = tokio::time::timeout(Duration::from_millis(100), async {
        while !reader.try_get("k", None).await.has_value() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(
        !reader.try_get("k", None).await.has_value(),
        "the older in-flight read must not recreate the deleted L1 entry"
    );
}

#[tokio::test]
async fn audit_old_backplane_expire_does_not_expire_newer_entry() {
    let clock = Arc::new(ManualClock::default());
    let backplane = Arc::new(InProcessBackplane::default());
    let cache: Cache<i32> = Cache::builder()
        .clock(clock.clone())
        .backplane(backplane.clone())
        .default_options(opts().with_skip_backplane_notifications(true))
        .auto_recovery(no_recovery())
        .try_build()
        .unwrap();
    clock.advance(Duration::from_secs(10));
    cache.set("k", 2).await.unwrap();
    let mut events = cache.events().subscribe();
    backplane
        .publish(message(
            "k",
            BackplaneAction::Expire,
            Timestamp::from_ticks(1),
        ))
        .await
        .unwrap();
    received(&mut events, "k").await;
    assert_eq!(
        cache.try_get("k", None).await.value(),
        Some(&2),
        "Expire@1 must not invalidate the newer local Set"
    );
}

#[tokio::test]
async fn audit_lagged_backplane_invalidates_possibly_stale_l1() {
    let backplane = Arc::new(InProcessBackplane::with_capacity(1));
    let cache: Cache<i32> = Cache::builder()
        .backplane(backplane.clone())
        .reconciliation_policy(amalgam::advanced::ReconciliationPolicy::BackplaneContinuity)
        .default_options(opts().with_skip_backplane_notifications(true))
        .auto_recovery(no_recovery())
        .try_build()
        .unwrap();
    cache.set("k", 1).await.unwrap();
    let mut events = cache.events().subscribe();
    tokio::task::yield_now().await;
    backplane
        .publish(message(
            "k",
            BackplaneAction::Remove,
            Timestamp::from_ticks(1),
        ))
        .await
        .unwrap();
    backplane
        .publish(message(
            "other-a",
            BackplaneAction::Remove,
            Timestamp::from_ticks(2),
        ))
        .await
        .unwrap();
    backplane
        .publish(message(
            "other-b",
            BackplaneAction::Remove,
            Timestamp::from_ticks(3),
        ))
        .await
        .unwrap();
    received(&mut events, "other-b").await;
    assert!(
        !cache.try_get("k", None).await.has_value(),
        "a dropped invalidation must not leave a fresh stale L1 value"
    );
}

struct HangingL2;

#[async_trait]
impl DistributedCache for HangingL2 {
    async fn get(&self, _key: &str) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }
    async fn set(&self, _key: &str, _bytes: Vec<u8>, _ttl: Option<Duration>) -> Result<()> {
        std::future::pending().await
    }
    async fn remove(&self, _key: &str) -> Result<()> {
        std::future::pending().await
    }
}

fn timeout_cache() -> Cache<i32> {
    Cache::builder()
        .distributed(Arc::new(HangingL2))
        .serializer(Arc::new(JsonSerializer))
        .default_options(opts().with_distributed_timeouts(
            Timeout::After(Duration::from_millis(5)),
            Timeout::After(Duration::from_millis(10)),
        ))
        .auto_recovery(no_recovery())
        .try_build()
        .unwrap()
}

#[tokio::test]
async fn shared_fc_distributed_read_timeout_does_not_bound_write() {
    let cache = timeout_cache();
    let completed = tokio::time::timeout(Duration::from_millis(100), cache.set("k", 1)).await;
    assert!(
        completed.is_err(),
        "FusionCache write budget is independent of its read hard timeout"
    );
}

#[tokio::test]
async fn shared_fc_distributed_read_timeout_does_not_bound_remove() {
    let cache = timeout_cache();
    let completed = tokio::time::timeout(Duration::from_millis(100), cache.remove("k")).await;
    assert!(
        completed.is_err(),
        "FusionCache removal budget is independent of its read hard timeout"
    );
}

struct BadSerialize;

impl DistributedSerializer<i32> for BadSerialize {
    fn serialize(&self, _entry: &DistributedEntry<i32>) -> Result<Vec<u8>> {
        Err(Error::Serialization("cannot encode".into()))
    }
    fn deserialize(&self, bytes: &[u8]) -> Result<DistributedEntry<i32>> {
        JsonSerializer.deserialize(bytes)
    }
}

#[tokio::test]
async fn audit_factory_write_honors_rethrow_serialization_flag() {
    let clock = Arc::new(ManualClock::default());
    let cache: Cache<i32> = Cache::builder()
        .clock(clock.clone())
        .distributed(Arc::new(InMemoryDistributedCache::new(clock)))
        .serializer(Arc::new(BadSerialize))
        .default_options(opts())
        .auto_recovery(no_recovery())
        .try_build()
        .unwrap();
    let actual = cache.get_or_set("k", amalgam::source::value(1)).await;
    assert!(
        matches!(actual, Err(Error::Serialization(_))),
        "serialization rethrow is enabled by default"
    );
}

struct FailedWrite;

#[async_trait]
impl DistributedCache for FailedWrite {
    async fn get(&self, _key: &str) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }
    async fn set(&self, _key: &str, _bytes: Vec<u8>, _ttl: Option<Duration>) -> Result<()> {
        Err(Error::Distributed("set down".into()))
    }
    async fn remove(&self, _key: &str) -> Result<()> {
        Err(Error::Distributed("remove down".into()))
    }
}

#[tokio::test]
async fn audit_factory_write_honors_rethrow_distributed_flag() {
    let cache: Cache<i32> = Cache::builder()
        .distributed(Arc::new(FailedWrite))
        .serializer(Arc::new(JsonSerializer))
        .default_options(opts().with_rethrow_distributed_exceptions(true))
        .auto_recovery(no_recovery())
        .try_build()
        .unwrap();
    let actual = cache.get_or_set("k", amalgam::source::value(1)).await;
    assert!(
        matches!(actual, Err(Error::Distributed(_))),
        "awaited L2 writes must propagate opted-in transport errors"
    );
}

#[tokio::test]
async fn audit_tag_marker_survives_node_joining_after_backplane_publication() {
    let clock = Arc::new(ManualClock::default());
    let l2 = Arc::new(InMemoryDistributedCache::new(clock.clone()));
    let bp = Arc::new(InProcessBackplane::default());
    let writer: Cache<i32> = Cache::builder()
        .clock(clock.clone())
        .distributed(l2.clone())
        .serializer(Arc::new(JsonSerializer))
        .backplane(bp.clone())
        .default_options(opts())
        .auto_recovery(no_recovery())
        .try_build()
        .unwrap();
    writer
        .set("k", 1)
        .tags([Tag::new("group").unwrap()])
        .await
        .unwrap();
    clock.advance(Duration::from_secs(1));
    writer.remove_by_tag("group").await.unwrap();
    let reader: Cache<i32> = Cache::builder()
        .clock(clock)
        .distributed(l2)
        .serializer(Arc::new(JsonSerializer))
        .backplane(bp)
        .default_options(opts())
        .auto_recovery(no_recovery())
        .try_build()
        .unwrap();
    assert_eq!(
        reader
            .get_or_set("k", amalgam::source::value(2))
            .await
            .unwrap(),
        2,
        "a node joining after publication must discover the persisted tag marker"
    );
}

#[tokio::test]
async fn audit_clear_respects_cache_key_prefix() {
    let clock = Arc::new(ManualClock::default());
    let bp = Arc::new(InProcessBackplane::default());
    let make = |prefix| -> Cache<i32> {
        Cache::builder()
            .clock(clock.clone())
            .backplane(bp.clone())
            .key_prefix(prefix)
            .default_options(opts())
            .auto_recovery(no_recovery())
            .try_build()
            .unwrap()
    };
    let alpha = make("alpha:");
    let beta = make("beta:");
    beta.set("k", 7).await.unwrap();
    clock.advance(Duration::from_secs(1));
    let mut controls = bp.subscribe();
    alpha
        .clear(amalgam::ClearMode::Remove)
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    let control = tokio::time::timeout(Duration::from_secs(1), controls.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        amalgam::provider::BackplaneCommand::from_message(control).unwrap(),
        amalgam::provider::BackplaneCommand::Marker(_)
    ));
    assert_eq!(
        beta.try_get("k", None).await.value(),
        Some(&7),
        "clearing alpha must preserve beta's independent prefixed entries"
    );
}

#[tokio::test]
async fn audit_ordinary_cache_key_is_not_interpreted_as_a_clear_command() {
    let clock = Arc::new(ManualClock::default());
    let bp = Arc::new(InProcessBackplane::default());
    let make = || -> Cache<i32> {
        Cache::builder()
            .clock(clock.clone())
            .backplane(bp.clone())
            .default_options(opts())
            .auto_recovery(no_recovery())
            .try_build()
            .unwrap()
    };
    let writer = make();
    let peer = make();
    peer.set("k", 7)
        .options(|_| opts().with_skip_backplane_notifications(true))
        .await
        .unwrap();
    clock.advance(Duration::from_secs(1));
    let mut events = peer.events().subscribe();
    writer.set("__amalgam:clear:remove", 1).await.unwrap();
    received(&mut events, "__amalgam:clear:remove").await;
    assert_eq!(
        peer.try_get("k", None).await.value(),
        Some(&7),
        "a publicly accepted data key must not act as a cache-wide clear notification"
    );
}

struct FailingBackplane {
    inner: InProcessBackplane,
    down: AtomicBool,
}

#[async_trait]
impl Backplane for FailingBackplane {
    async fn publish(&self, msg: BackplaneMessage) -> Result<()> {
        if self.down.load(Ordering::SeqCst) {
            return Err(Error::Backplane("down".into()));
        }
        self.inner.publish(msg).await
    }
    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<BackplaneMessage> {
        self.inner.subscribe()
    }
}

#[tokio::test]
async fn audit_failed_tag_marker_is_recovered_after_backplane_returns() {
    let clock = Arc::new(ManualClock::default());
    let bp = Arc::new(FailingBackplane {
        inner: InProcessBackplane::default(),
        down: AtomicBool::new(true),
    });
    let make = || -> Cache<i32> {
        Cache::builder()
            .clock(clock.clone())
            .backplane(bp.clone())
            .default_options(opts().with_skip_backplane_notifications(true))
            .auto_recovery(RecoveryConfig {
                delay: Duration::from_millis(50),
                ..RecoveryConfig::default()
            })
            .try_build()
            .unwrap()
    };
    let sender = make();
    let peer = make();
    peer.set("k", 1)
        .tags([Tag::new("group").unwrap()])
        .await
        .unwrap();
    clock.advance(Duration::from_secs(1));
    sender
        .remove_by_tag(Tag::new("group").unwrap())
        .options(|_| opts())
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    bp.down.store(false, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !peer.try_get("k", None).await.has_value(),
        "tag invalidations must retry like ordinary backplane messages"
    );
}

#[tokio::test]
async fn shared_fc_tag_invalidation_rechecks_origin_despite_failsafe_throttle() {
    let clock = Arc::new(ManualClock::default());
    let custom = opts().with_fail_safe(
        true,
        Some(Duration::from_secs(100)),
        Some(Duration::from_secs(10)),
    );
    let cache: Cache<i32> = Cache::builder()
        .clock(clock.clone())
        .default_options(custom)
        .auto_recovery(no_recovery())
        .try_build()
        .unwrap();
    cache
        .set("k", 1)
        .tags([Tag::new("group").unwrap()])
        .await
        .unwrap();
    clock.advance(Duration::from_secs(1));
    cache.remove_by_tag("group").await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    for _ in 0..2 {
        let calls = calls.clone();
        assert_eq!(
            cache
                .get_or_set(
                    "k",
                    amalgam::source::factory(move |_| async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Err(FactoryError::new("offline"))
                    })
                )
                .await
                .unwrap(),
            1
        );
    }
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "FusionCache invalidated tag is rechecked even during fail-safe throttle"
    );
}

#[tokio::test]
async fn audit_same_tick_tag_marker_invalidates_entry_like_fusioncache() {
    let clock = Arc::new(ManualClock::default());
    let cache: Cache<i32> = Cache::builder()
        .clock(clock)
        .default_options(opts())
        .try_build()
        .unwrap();
    cache
        .set("k", 1)
        .tags([Tag::new("group").unwrap()])
        .await
        .unwrap();
    cache.remove_by_tag("group").await.unwrap();
    assert!(
        !cache.try_get("k", None).await.has_value(),
        "FusionCache v2.9.0 invalidates when created <= tag marker"
    );
}

struct PausedWrite {
    inner: InMemoryDistributedCache,
    pause_next: AtomicBool,
    started: Notify,
    release: Semaphore,
    stored: Notify,
    read: Notify,
}

#[async_trait]
impl DistributedCache for PausedWrite {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let value = self.inner.get(key).await?;
        self.read.notify_one();
        Ok(value)
    }
    async fn set(&self, key: &str, bytes: Vec<u8>, ttl: Option<Duration>) -> Result<()> {
        if self.pause_next.swap(false, Ordering::SeqCst) {
            self.started.notify_one();
            self.release.acquire().await.unwrap().forget();
        }
        self.inner.set(key, bytes, ttl).await?;
        self.stored.notify_one();
        Ok(())
    }
    async fn remove(&self, key: &str) -> Result<()> {
        self.inner.remove(key).await
    }
}

#[tokio::test]
async fn audit_background_l2_write_publishes_only_after_the_value_is_visible() {
    let clock = Arc::new(ManualClock::default());
    let l2 = Arc::new(PausedWrite {
        inner: InMemoryDistributedCache::new(clock.clone()),
        pause_next: AtomicBool::new(false),
        started: Notify::new(),
        release: Semaphore::new(0),
        stored: Notify::new(),
        read: Notify::new(),
    });
    let bp = Arc::new(InProcessBackplane::default());
    let make = || -> Cache<i32> {
        Cache::builder()
            .clock(clock.clone())
            .distributed(l2.clone())
            .serializer(Arc::new(JsonSerializer))
            .backplane(bp.clone())
            .default_options(opts())
            .auto_recovery(no_recovery())
            .try_build()
            .unwrap()
    };
    let writer = make();
    let reader = make();
    let mut events = reader.events().subscribe();
    writer.set("k", 1).await.unwrap();
    received(&mut events, "k").await;
    reader
        .get_or_set("k", amalgam::source::value(0))
        .await
        .unwrap();
    l2.stored.notified().await;
    l2.read.notified().await;
    l2.pause_next.store(true, Ordering::SeqCst);
    clock.advance(Duration::from_secs(1));
    let commit = writer
        .set("k", 2)
        .options(|_| opts().with_allow_background_distributed_operations(true))
        .with_receipt()
        .await
        .unwrap();
    assert!(matches!(
        commit,
        amalgam::advanced::MutationReceipt::Scheduled(_)
    ));
    tokio::time::timeout(Duration::from_secs(1), l2.started.notified())
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(30), received(&mut events, "k"))
            .await
            .is_err(),
        "notification must wait for L2 visibility"
    );
    l2.release.add_permits(1);
    commit.wait().await.unwrap();
    received(&mut events, "k").await;
    tokio::time::timeout(Duration::from_secs(1), l2.read.notified())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), l2.stored.notified())
        .await
        .unwrap();
    for _ in 0..3 {
        tokio::task::yield_now().await;
    }
    let bytes = l2.inner.get("v2:k").await.unwrap().unwrap();
    let stored: amalgam::provider::DistributedSnapshot<i32> =
        JsonSerializer.deserialize_snapshot(&bytes).unwrap();
    assert_eq!(
        stored.entry().value,
        2,
        "the delayed L2 write has now completed"
    );
    assert_eq!(
        reader.try_get("k", None).await.value(),
        Some(&2),
        "the notification must not leave the peer permanently caching the older L2 snapshot"
    );
}

#[cfg(feature = "redis")]
#[tokio::test]
async fn audit_redis_backplane_roundtrips_arbitrary_public_instance_id() {
    let Some(url) = redis_fixture::redis_url() else {
        return;
    };
    let channel = format!("amalgam:audit:source-id:{:016x}", fastrand::u64(..));
    let backplane = amalgam::provider::RedisBackplane::connect_with_channel(url, channel)
        .await
        .unwrap();
    let mut receiver = backplane.subscribe();
    let msg = BackplaneMessage {
        source_id: Arc::from("node|a"),
        timestamp: Timestamp::from_ticks(123),
        action: BackplaneAction::Remove,
        key: Arc::from("k"),
    };
    backplane.publish(msg).await.unwrap();
    let actual = tokio::time::timeout(Duration::from_millis(150), receiver.recv()).await;
    assert!(
        actual.is_ok(),
        "a publicly accepted instance id must survive Redis's wire encoding"
    );
}

#[cfg(feature = "redis")]
#[tokio::test]
async fn audit_redis_lock_does_not_acquire_after_wait_deadline() {
    use amalgam::provider::DistributedLocker;
    let Some(url) = redis_fixture::redis_url() else {
        return;
    };
    let locker = amalgam::provider::RedisDistributedLocker::connect(url)
        .await
        .unwrap();
    let key = format!("amalgam:audit:deadline:{:016x}", fastrand::u64(..));
    let first = locker
        .acquire(&key, Duration::from_millis(15), Timeout::Infinite)
        .await
        .unwrap()
        .unwrap();
    let started = tokio::time::Instant::now();
    let actual = locker
        .acquire(
            &key,
            Duration::from_secs(1),
            Timeout::After(Duration::from_millis(1)),
        )
        .await
        .unwrap();
    let elapsed = started.elapsed();
    if let Some(ref token) = actual {
        locker.release(&key, token).await.unwrap();
    }
    locker.release(&key, &first).await.unwrap();
    assert!(
        actual.is_none(),
        "the one-millisecond deadline passed, but lock acquired after {elapsed:?}"
    );
}
