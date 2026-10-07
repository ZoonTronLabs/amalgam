use amalgam::provider::*;
use amalgam::*;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Default)]
struct ContextProvider(Mutex<Vec<(String, Duration)>>);
impl DefaultEntryOptionsProvider for ContextProvider {
    fn options_for_with_defaults(
        &self,
        key: &str,
        defaults: &EntryOptions,
    ) -> Option<EntryOptions> {
        self.0
            .lock()
            .unwrap()
            .push((key.to_owned(), defaults.duration()));
        match key {
            "short" => Some(defaults.clone().with_duration(Duration::from_secs(1))),
            _ => None,
        }
    }
}

fn node(clock: Arc<ManualClock>, provider: Arc<ContextProvider>, duration: Duration) -> Cache<u64> {
    Cache::builder()
        .clock(clock)
        .key_prefix("tenant:")
        .default_options(EntryOptions::new(duration))
        .default_options_provider(provider)
        .try_build()
        .unwrap()
}

#[tokio::test]
async fn provider_gets_each_caches_defaults_and_raw_key_without_capturing_a_stale_baseline() {
    let clock = Arc::new(ManualClock::default());
    let provider = Arc::new(ContextProvider::default());
    let first = node(clock.clone(), provider.clone(), Duration::from_secs(10));
    let second = node(clock.clone(), provider.clone(), Duration::from_secs(30));
    first.set("ordinary", 1).with_receipt().await.unwrap();
    first.set("short", 2).with_receipt().await.unwrap();
    second.set("ordinary", 3).with_receipt().await.unwrap();
    clock.advance(Duration::from_secs(2));
    assert!(!first.read("short", None).await.unwrap().has_value());
    clock.advance(Duration::from_secs(9));
    assert!(!first.read("ordinary", None).await.unwrap().has_value());
    assert_eq!(
        second.read("ordinary", None).await.unwrap().value(),
        Some(&3)
    );
    {
        let observations = provider.0.lock().unwrap();
        assert!(
            observations
                .iter()
                .any(|(key, ttl)| key == "ordinary" && *ttl == Duration::from_secs(10))
        );
        assert!(
            observations
                .iter()
                .any(|(key, ttl)| key == "ordinary" && *ttl == Duration::from_secs(30))
        );
        assert!(
            observations
                .iter()
                .all(|(key, _)| !key.starts_with("tenant:"))
        );
    }
    first.shutdown().await.unwrap();
    second.shutdown().await.unwrap();
}

#[tokio::test]
async fn explicit_options_bypass_both_provider_hooks_and_legacy_providers_still_work() {
    let provider = Arc::new(ContextProvider::default());
    let cache = Cache::<u64>::builder()
        .default_options_provider(provider.clone())
        .try_build()
        .unwrap();
    cache
        .set("key", 7)
        .options(|_| EntryOptions::default())
        .with_receipt()
        .await
        .unwrap();
    assert_eq!(
        cache
            .read("key", Some(EntryOptions::default()))
            .await
            .unwrap()
            .value(),
        Some(&7)
    );
    assert!(provider.0.lock().unwrap().is_empty());
    cache.shutdown().await.unwrap();
    struct LegacyProvider;
    impl DefaultEntryOptionsProvider for LegacyProvider {
        fn options_for(&self, _: &str) -> Option<EntryOptions> {
            Some(EntryOptions::new(Duration::ZERO))
        }
    }
    let cache = Cache::<u64>::builder()
        .default_options_provider(Arc::new(LegacyProvider))
        .try_build()
        .unwrap();
    cache.set("key", 7).with_receipt().await.unwrap();
    assert!(!cache.read("key", None).await.unwrap().has_value());
    cache.shutdown().await.unwrap();
}
