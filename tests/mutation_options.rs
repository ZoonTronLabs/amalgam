//! Dynamic mutation policy is observable at both layers and explicit options win.

use amalgam::{
    Cache, Clock, ConfigError, DefaultEntryOptionsProvider, DistributedCache,
    DistributedSerializer, EffectOutcome, EntryOptions, Error, InMemoryDistributedCache,
    JsonSerializer, KeyModifierMode, LocalEffect, ManualClock, RecoveryConfig, SkipReason,
    Timestamp,
};
use std::sync::{Arc, Mutex};
use std::time::Duration;

struct RecordingProvider {
    options: EntryOptions,
    requested: Mutex<Vec<String>>,
}

impl DefaultEntryOptionsProvider for RecordingProvider {
    fn options_for(&self, key: &str) -> Option<EntryOptions> {
        self.requested.lock().unwrap().push(key.to_owned());
        Some(self.options.clone())
    }
}

#[derive(Clone, Copy)]
enum Mutation {
    Remove,
    Expire,
}

fn options() -> EntryOptions {
    EntryOptions::new(Duration::from_secs(60))
        .with_fail_safe(true, Some(Duration::from_secs(600)), None)
        .with_allow_background_backplane_operations(false)
}

struct Fixture {
    cache: Cache<i32>,
    clock: Arc<ManualClock>,
    store: Arc<InMemoryDistributedCache>,
    provider: Arc<RecordingProvider>,
}

fn fixture(provider_options: EntryOptions) -> Fixture {
    let clock = Arc::new(ManualClock::new(Timestamp::from_ticks(1_000_000)));
    let store = Arc::new(InMemoryDistributedCache::new(clock.clone()));
    let provider = Arc::new(RecordingProvider {
        options: provider_options,
        requested: Mutex::new(Vec::new()),
    });
    let cache = Cache::builder()
        .clock(clock.clone())
        .distributed(store.clone())
        .serializer(Arc::new(JsonSerializer))
        .key_prefix("physical:")
        .distributed_key_modifier_mode(KeyModifierMode::None)
        .default_options(options())
        .default_options_provider(provider.clone())
        .auto_recovery(RecoveryConfig {
            enabled: false,
            ..RecoveryConfig::default()
        })
        .try_build()
        .unwrap();
    Fixture {
        cache,
        clock,
        store,
        provider,
    }
}

async fn seed(cache: &Cache<i32>) {
    cache
        .try_set_full("tenant:item", 42, Some(options()), Box::new([]))
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
}

async fn memory_value(cache: &Cache<i32>) -> Option<i32> {
    cache
        .read(
            "tenant:item",
            Some(
                options()
                    .with_skip_distributed(true, true)
                    .with_fail_safe(false, None, None),
            ),
        )
        .await
        .unwrap()
        .into_value()
}

#[tokio::test]
async fn dynamic_remove_and_expire_use_raw_key_policy_and_preserve_skipped_layers() {
    let Fixture {
        cache,
        clock,
        store,
        provider,
    } = fixture(
        options()
            .with_skip_memory(false, true)
            .with_skip_distributed(false, true),
    );
    for mutation in [Mutation::Remove, Mutation::Expire] {
        seed(&cache).await;
        let original = store.get("physical:tenant:item").await.unwrap().unwrap();
        clock.advance(Duration::from_millis(1));
        let receipt = match mutation {
            Mutation::Remove => cache.try_remove("tenant:item").await,
            Mutation::Expire => cache.try_expire("tenant:item").await,
        }
        .unwrap();
        let report = receipt.wait().await.unwrap();
        assert!(matches!(report.local, LocalEffect::Skipped));
        assert!(matches!(
            report.distributed,
            EffectOutcome::Skipped(SkipReason::Policy)
        ));
        assert_eq!(memory_value(&cache).await, Some(42));
        assert_eq!(
            store.get("physical:tenant:item").await.unwrap(),
            Some(original)
        );
    }
    assert_eq!(
        *provider.requested.lock().unwrap(),
        ["tenant:item", "tenant:item"]
    );
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn explicit_mutation_options_bypass_provider_and_apply_each_layer() {
    let Fixture {
        cache,
        clock,
        store,
        provider,
    } = fixture(
        options()
            .with_skip_memory(false, true)
            .with_skip_distributed(false, true),
    );
    for mutation in [Mutation::Expire, Mutation::Remove] {
        seed(&cache).await;
        let original = store.get("physical:tenant:item").await.unwrap().unwrap();
        let codec: &dyn DistributedSerializer<i32> = &JsonSerializer;
        let before = codec.deserialize_snapshot(&original).unwrap();
        clock.advance(Duration::from_millis(1));
        let receipt = match mutation {
            Mutation::Remove => cache.try_remove_with("tenant:item", Some(options())).await,
            Mutation::Expire => {
                cache
                    .try_expire_with_policy(
                        "tenant:item",
                        Some(options()),
                        amalgam::DistributedExpirePolicy::RetainStale,
                    )
                    .await
            }
        }
        .unwrap();
        let report = receipt.wait().await.unwrap();
        assert!(matches!(report.distributed, EffectOutcome::Applied));
        assert_eq!(memory_value(&cache).await, None);
        match mutation {
            Mutation::Remove => {
                assert!(matches!(report.local, LocalEffect::Removed));
                assert!(store.get("physical:tenant:item").await.unwrap().is_none());
            }
            Mutation::Expire => {
                assert!(matches!(report.local, LocalEffect::Expired));
                let bytes = store.get("physical:tenant:item").await.unwrap().unwrap();
                let expired = codec.deserialize_snapshot(&bytes).unwrap();
                assert_eq!(expired.entry().value, 42);
                assert!(expired.entry().logical_expiration_ticks <= clock.now().ticks());
                assert_eq!(
                    expired.entry().physical_expiration_ticks,
                    before.entry().physical_expiration_ticks
                );
            }
        }
    }
    assert!(provider.requested.lock().unwrap().is_empty());
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn invalid_dynamic_mutation_options_reject_before_changing_either_layer() {
    let Fixture {
        cache,
        clock,
        store,
        provider,
    } = fixture(options().with_size(-1));
    for mutation in [Mutation::Remove, Mutation::Expire] {
        seed(&cache).await;
        let original = store.get("physical:tenant:item").await.unwrap().unwrap();
        clock.advance(Duration::from_millis(1));
        let result = match mutation {
            Mutation::Remove => cache.try_remove("tenant:item").await,
            Mutation::Expire => cache.try_expire("tenant:item").await,
        };
        assert!(matches!(
            result,
            Err(Error::Config(ConfigError::NegativeEntryWeight { size: -1 }))
        ));
        assert_eq!(memory_value(&cache).await, Some(42));
        assert_eq!(
            store.get("physical:tenant:item").await.unwrap(),
            Some(original)
        );
    }
    assert_eq!(
        *provider.requested.lock().unwrap(),
        ["tenant:item", "tenant:item"]
    );
    cache.shutdown().await.unwrap();
}
