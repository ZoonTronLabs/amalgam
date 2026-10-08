//! Lookup defaults stay immutable across L2 and adaptive owned origin work.
use amalgam::provider::{
    DefaultEntryOptionsProvider, DistributedCache, InMemoryDistributedCache, InvalidationStore,
    JsonSerializer, ManualClock,
};
use amalgam::{Cache, EntryOptions, Result, source};
use async_trait::async_trait;
use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Default)]
struct DefaultsProvider(Mutex<Vec<(String, Duration)>>);
impl DefaultEntryOptionsProvider for DefaultsProvider {
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

#[tokio::test]
async fn l2_lookup_preserves_raw_provider_context_and_explicit_options_precedence() -> Result<()> {
    let clock = Arc::new(ManualClock::default());
    let backend = Arc::new(InMemoryDistributedCache::new(clock.clone()));
    let writer = Cache::<u64>::builder()
        .clock(clock.clone())
        .key_prefix("tenant:")
        .distributed(backend.clone())
        .serializer(Arc::new(JsonSerializer))
        .default_options(EntryOptions::new(Duration::from_secs(60)))
        .try_build()?;
    writer
        .set("ordinary", 11)
        .with_receipt()
        .await?
        .wait()
        .await?;
    writer.set("short", 12).with_receipt().await?.wait().await?;

    let provider = Arc::new(DefaultsProvider::default());
    let defaults = EntryOptions::new(Duration::from_secs(10)).with_skip_memory(true, false);
    let reader = Cache::<u64>::builder()
        .clock(clock)
        .key_prefix("tenant:")
        .distributed(backend)
        .serializer(Arc::new(JsonSerializer))
        .default_options(defaults.clone())
        .default_options_provider(provider.clone())
        .try_build()?;
    assert_eq!(reader.try_get("ordinary").await?.as_ref(), Some(&11));
    assert_eq!(
        reader
            .get_or_set("short", |_| async {
                panic!("a successful L2 lookup must not run the factory");
                #[allow(unreachable_code)]
                Ok::<_, Infallible>(99)
            })
            .await?,
        12
    );
    assert_eq!(
        reader
            .try_get("ordinary")
            .options(|_| defaults)
            .await?
            .as_ref(),
        Some(&11)
    );

    let produced = reader
        .get_or_set(
            "absent",
            source::factory(|ctx| async move {
                assert_eq!(ctx.options().duration(), Duration::from_secs(3));
                Ok::<_, Infallible>(13)
            }),
        )
        .options(|options| options.with_duration(Duration::from_secs(3)))
        .await?;
    assert_eq!(produced, 13);
    assert_eq!(reader.entry_options().duration(), Duration::from_secs(10));
    assert_eq!(
        *provider.0.lock().unwrap(),
        vec![
            ("ordinary".to_owned(), Duration::from_secs(10)),
            ("short".to_owned(), Duration::from_secs(10)),
        ]
    );
    reader.shutdown().await?;
    writer.shutdown().await?;
    Ok(())
}

struct YieldingBackend(Arc<InMemoryDistributedCache>);
#[async_trait]
impl DistributedCache for YieldingBackend {
    async fn get(&self, key: &str) -> Result<Option<amalgam::provider::DistributedBytes>> {
        tokio::task::yield_now().await;
        self.0.get(key).await
    }
    async fn set(&self, key: &str, value: Vec<u8>, ttl: Option<Duration>) -> Result<()> {
        self.0.set(key, value, ttl).await
    }
    async fn remove(&self, key: &str) -> Result<()> {
        self.0.remove(key).await
    }
    fn invalidation_store(&self) -> Option<Arc<dyn InvalidationStore>> {
        self.0.invalidation_store()
    }
}

#[tokio::test]
async fn pending_l2_miss_materializes_independent_adaptive_factory_options() -> Result<()> {
    let clock = Arc::new(ManualClock::default());
    let backend = Arc::new(YieldingBackend(Arc::new(InMemoryDistributedCache::new(
        clock.clone(),
    ))));
    let cache = Cache::<u64>::builder()
        .clock(clock.clone())
        .distributed(backend)
        .serializer(Arc::new(JsonSerializer))
        .default_options(EntryOptions::new(Duration::from_secs(60)))
        .try_build()?;
    assert_eq!(
        cache
            .get_or_set(
                "adaptive",
                source::factory(|mut ctx| async move {
                    assert_eq!(ctx.options().duration(), Duration::from_secs(60));
                    let options = ctx.options().clone().with_duration(Duration::from_secs(1));
                    *ctx.options_mut() = options;
                    tokio::task::yield_now().await;
                    Ok::<_, Infallible>(42)
                })
            )
            .with_receipt()
            .await?
            .value,
        42
    );
    assert_eq!(cache.entry_options().duration(), Duration::from_secs(60));
    clock.advance(Duration::from_secs(2));
    assert!(cache.try_get("adaptive").await?.is_none());
    cache.shutdown().await?;
    Ok(())
}
