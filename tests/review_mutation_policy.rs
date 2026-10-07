use amalgam::*;
use async_trait::async_trait;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

struct ToggleBackend {
    inner: InMemoryDistributedCache,
    failed: AtomicBool,
    attempts: AtomicUsize,
}
#[async_trait]
impl DistributedCache for ToggleBackend {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.inner.get(key).await
    }
    async fn set(&self, key: &str, bytes: Vec<u8>, ttl: Option<Duration>) -> Result<()> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        if self.failed.load(Ordering::SeqCst) {
            Err(Error::Distributed("write fault".into()))
        } else {
            self.inner.set(key, bytes, ttl).await
        }
    }
    async fn remove(&self, key: &str) -> Result<()> {
        self.inner.remove(key).await
    }
    fn invalidation_store(&self) -> Option<Arc<dyn InvalidationStore>> {
        self.inner.invalidation_store()
    }
}
#[tokio::test]
async fn skipped_write_does_not_extend_distributed_circuit_cooldown() {
    let clock = Arc::new(ManualClock::default());
    let backend = Arc::new(ToggleBackend {
        inner: InMemoryDistributedCache::new(clock.clone()),
        failed: AtomicBool::new(true),
        attempts: AtomicUsize::new(0),
    });
    let cache: Cache<u64> = Cache::builder()
        .clock(clock.clone())
        .distributed(backend.clone())
        .serializer(Arc::new(JsonSerializer))
        .distributed_circuit_breaker(Duration::from_secs(10))
        .auto_recovery(RecoveryConfig {
            enabled: false,
            ..RecoveryConfig::default()
        })
        .default_options(EntryOptions::default().with_rethrow_distributed_exceptions(true))
        .try_build()
        .unwrap();
    assert!(matches!(
        cache.set("first", 1).with_receipt().await,
        Err(Error::Distributed(_))
    ));
    clock.advance(Duration::from_secs(9));
    backend.failed.store(false, Ordering::SeqCst);
    assert!(matches!(
        cache.set("skipped", 2).with_receipt().await,
        Err(Error::CircuitOpen {
            component: CircuitComponent::Distributed
        })
    ));
    assert_eq!(backend.attempts.load(Ordering::SeqCst), 1);
    clock.advance(Duration::from_secs(2));
    let receipt = cache
        .set("after-cooldown", 3)
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(matches!(receipt.distributed, EffectOutcome::Applied));
    assert_eq!(backend.attempts.load(Ordering::SeqCst), 2);
    cache.shutdown().await.unwrap();
}

#[derive(Clone, Copy)]
enum MarkerMutation {
    Tag,
    Tags,
    ClearExpire,
    ClearRemove,
}
async fn skipped_marker_write(mutation: MarkerMutation) {
    let clock = Arc::new(ManualClock::default());
    let backend = Arc::new(InMemoryDistributedCache::new(clock.clone()));
    let build = || {
        Cache::<u64>::builder()
            .clock(clock.clone())
            .distributed(backend.clone())
            .serializer(Arc::new(JsonSerializer))
            .auto_recovery(RecoveryConfig {
                enabled: false,
                ..RecoveryConfig::default()
            })
            .try_build()
            .unwrap()
    };
    let cache = build();
    let first = Tag::new("first").unwrap();
    let second = Tag::new("second").unwrap();
    cache
        .set("key", 7)
        .tags([first.clone(), second.clone()])
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    clock.advance(Duration::from_millis(1));
    let options = EntryOptions::default().with_skip_distributed(false, true);
    let report = match mutation {
        MarkerMutation::Tag => {
            cache
                .remove_by_tag(first.clone())
                .options(|_| options)
                .with_receipt()
                .await
        }
        MarkerMutation::Tags => {
            cache
                .remove_by_tag(first.clone())
                .and_tags([second.clone()])
                .options(|_| options)
                .with_receipt()
                .await
        }
        MarkerMutation::ClearExpire => {
            cache
                .clear(ClearMode::Expire)
                .options(|_| options)
                .with_receipt()
                .await
        }
        MarkerMutation::ClearRemove => {
            cache
                .clear(ClearMode::Remove)
                .options(|_| options)
                .with_receipt()
                .await
        }
    }
    .unwrap()
    .wait()
    .await
    .unwrap();
    assert!(
        match &report.distributed {
            EffectOutcome::Skipped(SkipReason::Policy) => true,
            EffectOutcome::Batch(batch) => batch
                .stages()
                .iter()
                .all(|stage| matches!(stage, EffectOutcome::Skipped(SkipReason::Policy))),
            _ => false,
        },
        "{report:?}"
    );
    assert_eq!(cache.pending_recovery(), 0);
    let scope = CacheScope::new("", "v2", KeyModifierMode::Prefix).unwrap();
    let markers = backend.invalidation_store().unwrap();
    for kind in [
        MarkerKind::Tag(first),
        MarkerKind::Tag(second),
        MarkerKind::ClearExpire,
        MarkerKind::ClearRemove,
    ] {
        assert_eq!(
            markers.read(&scope, &kind).await.unwrap(),
            None,
            "a skipped marker write modified durable state"
        );
    }
    let fresh_node = build();
    assert_eq!(
        fresh_node.read("key", None).await.unwrap().value(),
        Some(&7),
        "a local-only invalidation escaped into shared storage"
    );
    fresh_node.shutdown().await.unwrap();
    cache.shutdown().await.unwrap();
}
#[tokio::test]
async fn tag_mutation_skips_requested_distributed_write() {
    skipped_marker_write(MarkerMutation::Tag).await;
}
#[tokio::test]
async fn batched_tags_skip_requested_distributed_write() {
    skipped_marker_write(MarkerMutation::Tags).await;
}
#[tokio::test]
async fn clear_expire_skips_requested_distributed_write() {
    skipped_marker_write(MarkerMutation::ClearExpire).await;
}
#[tokio::test]
async fn clear_remove_skips_requested_distributed_write() {
    skipped_marker_write(MarkerMutation::ClearRemove).await;
}

struct ToggleBackplane {
    inner: InProcessBackplane,
    failed: AtomicBool,
    attempts: AtomicUsize,
}
#[async_trait]
impl Backplane for ToggleBackplane {
    async fn publish(&self, message: BackplaneMessage) -> Result<()> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        if self.failed.load(Ordering::SeqCst) {
            Err(Error::Backplane("publish fault".into()))
        } else {
            self.inner.publish(message).await
        }
    }
    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<BackplaneMessage> {
        self.inner.subscribe()
    }
    fn connection_state(&self) -> Option<tokio::sync::watch::Receiver<BackplaneState>> {
        self.inner.connection_state()
    }
}
#[tokio::test]
async fn skipped_publish_does_not_extend_backplane_circuit_cooldown() {
    let clock = Arc::new(ManualClock::default());
    let backplane = Arc::new(ToggleBackplane {
        inner: InProcessBackplane::default(),
        failed: AtomicBool::new(true),
        attempts: AtomicUsize::new(0),
    });
    let cache: Cache<u64> = Cache::builder()
        .clock(clock.clone())
        .backplane(backplane.clone())
        .backplane_circuit_breaker(Duration::from_secs(10))
        .auto_recovery(RecoveryConfig {
            enabled: false,
            ..RecoveryConfig::default()
        })
        .default_options(
            EntryOptions::default()
                .with_allow_background_backplane_operations(false)
                .with_rethrow_backplane_exceptions(true),
        )
        .try_build()
        .unwrap();
    assert!(matches!(
        cache.set("first", 1).with_receipt().await,
        Err(Error::Backplane(_))
    ));
    clock.advance(Duration::from_secs(9));
    backplane.failed.store(false, Ordering::SeqCst);
    assert!(matches!(
        cache.set("skipped", 2).with_receipt().await,
        Err(Error::CircuitOpen {
            component: CircuitComponent::Backplane
        })
    ));
    assert_eq!(backplane.attempts.load(Ordering::SeqCst), 1);
    clock.advance(Duration::from_secs(2));
    let receipt = cache
        .set("after-cooldown", 3)
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(matches!(receipt.backplane, EffectOutcome::Applied));
    assert_eq!(backplane.attempts.load(Ordering::SeqCst), 2);
    cache.shutdown().await.unwrap();
}
