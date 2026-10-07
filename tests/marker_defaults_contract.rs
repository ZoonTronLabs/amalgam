use amalgam::*;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

struct Provider(Arc<AtomicUsize>);
impl DefaultEntryOptionsProvider for Provider {
    fn options_for(&self, _: &str) -> Option<EntryOptions> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Some(EntryOptions::default().with_skip_distributed(false, true))
    }
}

#[tokio::test]
async fn tag_defaults_are_independent_and_explicit_operation_options_take_precedence() {
    let calls = Arc::new(AtomicUsize::new(0));
    let backend = Arc::new(InMemoryDistributedCache::new(Arc::new(SystemClock)));
    let cache = Cache::<u64>::builder()
        .name("orders")
        .instance_id("node-a")
        .distributed(backend.clone())
        .serializer(Arc::new(JsonSerializer))
        .default_options(EntryOptions::default().with_skip_distributed(false, true))
        .default_options_provider(Arc::new(Provider(calls.clone())))
        .tags_default_options(EntryOptions::tag_defaults())
        .try_build()
        .unwrap();
    assert_eq!(cache.name(), "orders");
    assert_eq!(cache.instance_id(), "node-a");
    assert!(cache.distributed_cache().is_some());
    assert!(cache.backplane().is_none());
    assert!(cache.distributed_locker().is_none());
    assert!(cache.entry_options().skip_distributed_write());
    assert!(!cache.tags_entry_options().skip_distributed_write());
    let receipt = cache
        .remove_by_tag(Tag::new("stored").unwrap())
        .with_receipt()
        .await
        .unwrap();
    assert!(matches!(receipt, MutationReceipt::Completed(_)));
    assert!(matches!(
        receipt.wait().await.unwrap().distributed,
        EffectOutcome::Applied
    ));
    let skipped = cache
        .clear(ClearMode::Expire)
        .options(|_| EntryOptions::default().with_skip_distributed(false, true))
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(matches!(
        skipped.distributed,
        EffectOutcome::Skipped(SkipReason::Policy)
    ));
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "key provider must never choose control-marker policy"
    );
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn configured_tag_policy_is_used_by_both_single_batch_and_clear_operations() {
    let backend = Arc::new(InMemoryDistributedCache::new(Arc::new(SystemClock)));
    let cache = Cache::<u64>::builder()
        .distributed(backend)
        .serializer(Arc::new(JsonSerializer))
        .tags_default_options(EntryOptions::tag_defaults().with_skip_distributed(false, true))
        .try_build()
        .unwrap();
    let receipts = [
        cache
            .remove_by_tag(Tag::new("one").unwrap())
            .with_receipt()
            .await
            .unwrap(),
        cache
            .remove_by_tag(Tag::new("two").unwrap())
            .and_tags::<_, &str>([])
            .with_receipt()
            .await
            .unwrap(),
        cache.clear(ClearMode::Expire).with_receipt().await.unwrap(),
        cache.clear(ClearMode::Remove).with_receipt().await.unwrap(),
    ];
    for receipt in receipts {
        assert!(matches!(
            receipt.wait().await.unwrap().distributed,
            EffectOutcome::Skipped(SkipReason::Policy)
        ));
    }
    cache.shutdown().await.unwrap();
}

#[test]
fn invalid_tag_defaults_are_rejected_before_any_cache_is_built() {
    let result = Cache::<u64>::builder()
        .tags_default_options(EntryOptions::default().with_size(-1))
        .try_build();
    assert!(matches!(
        result,
        Err(Error::Config(ConfigError::NegativeEntryWeight { size: -1 }))
    ));
    let defaults = EntryOptions::tag_defaults();
    assert_eq!(defaults.duration(), Duration::from_secs(3600));
    assert!(!defaults.allow_background_backplane_operations());
    assert!(defaults.skip_distributed_locker());
    assert_eq!(defaults.priority(), Priority::NeverRemove);
}
