//! Scalar hydration preserves source facts separately from the local limits.
use amalgam::provider::{
    Clock, InMemoryDistributedCache, JsonSerializer, ManualClock, MemoryStorage, ValueCloner,
};
use amalgam::{Cache, CloneError, EntryOptions, EntryWeight, Priority, source};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

#[path = "support/memory_storage_fixture.rs"]
mod storage_fixture;
use storage_fixture::MapStorage;

#[derive(Clone, Copy)]
enum Retrieval {
    Read,
    Origin,
}

#[tokio::test]
async fn scalar_l2_lookup_returns_the_source_while_l1_keeps_its_shorter_limits()
-> amalgam::Result<()> {
    for retrieval in [Retrieval::Read, Retrieval::Origin] {
        let clock = Arc::new(ManualClock::default());
        let created = clock.now();
        let memory = MapStorage::new();
        let options = EntryOptions::new(Duration::from_secs(60))
            .with_fail_safe(true, Some(Duration::from_secs(120)), None)
            .with_priority(Priority::High)
            .with_size(9)
            .with_skip_memory(true, false);
        let cache = Cache::<u64>::builder()
            .clock(clock.clone())
            .distributed(Arc::new(InMemoryDistributedCache::new(clock.clone())))
            .serializer(Arc::new(JsonSerializer))
            .memory_storage(memory.clone())
            .default_options(options)
            .try_build()?;
        cache
            .set("scalar", 7)
            .options(|options| options.with_skip_memory(false, true))
            .tags(["alpha", "beta"])
            .with_receipt()
            .await?
            .wait()
            .await?;
        clock.advance(Duration::from_secs(10));
        let lookup = cache
            .entry_options()
            .with_duration(Duration::from_secs(5))
            .with_fail_safe(true, Some(Duration::from_secs(10)), None)
            .with_size(3)
            .with_priority(Priority::Low);
        let value = match retrieval {
            Retrieval::Read => cache.try_get("scalar").options(|_| lookup).await?.unwrap(),
            Retrieval::Origin => {
                cache
                    .get_or_set(
                        "scalar",
                        source::factory(|_| async {
                            panic!("a fresh L2 source must not run the factory");
                            #[allow(unreachable_code)]
                            Ok::<u64, amalgam::FactoryError>(99)
                        }),
                    )
                    .options(|_| lookup)
                    .await?
            }
        };
        assert_eq!(value, 7);
        let record = memory.get("scalar")?.unwrap();
        let entry = record.entry();
        assert_eq!(entry.meta().created(), created);
        assert_eq!(
            entry.meta().logical_expiration(),
            created.saturating_add(Duration::from_secs(15))
        );
        assert_eq!(
            entry.meta().physical_expiration(),
            created.saturating_add(Duration::from_secs(20))
        );
        assert_eq!(entry.meta().size(), Some(EntryWeight::new(9)));
        assert_eq!(entry.meta().priority(), Priority::High);
        assert_eq!(
            entry
                .meta()
                .tags()
                .iter()
                .map(|tag| tag.as_str())
                .collect::<Vec<_>>(),
            ["alpha", "beta"]
        );
        drop(record);
        clock.advance(Duration::from_secs(6));
        let local_only = cache
            .entry_options()
            .with_skip_memory(false, true)
            .with_skip_distributed(true, true);
        assert!(
            cache
                .try_get("scalar")
                .options(|_| local_only)
                .await?
                .is_none()
        );
        assert_eq!(cache.try_get("scalar").await?, Some(7));
        cache.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn scalar_stale_factory_keeps_source_fallback_after_the_hydrated_l1_expires()
-> amalgam::Result<()> {
    let clock = Arc::new(ManualClock::default());
    let cache = Cache::<u64>::builder()
        .clock(clock.clone())
        .distributed(Arc::new(InMemoryDistributedCache::new(clock.clone())))
        .serializer(Arc::new(JsonSerializer))
        .default_options(
            EntryOptions::new(Duration::from_secs(1))
                .with_fail_safe(true, Some(Duration::from_secs(60)), None)
                .with_skip_memory(true, false),
        )
        .try_build()?;
    cache
        .set("stale", 7)
        .options(|options| options.with_skip_memory(false, true))
        .tags(["source"])
        .with_receipt()
        .await?
        .wait()
        .await?;
    clock.advance(Duration::from_secs(2));
    let factory_clock = clock.clone();
    let result = cache
        .get_or_set(
            "stale",
            source::factory(move |ctx| async move {
                assert_eq!(ctx.stale_value(), Some(&7));
                assert_eq!(
                    ctx.stale_tags()
                        .unwrap()
                        .iter()
                        .map(|tag| tag.as_str())
                        .collect::<Vec<_>>(),
                    ["source"]
                );
                factory_clock.advance(Duration::from_secs(2));
                Err::<u64, _>(ctx.fail("source unavailable"))
            }),
        )
        .options(|options| options.with_fail_safe(true, Some(Duration::from_secs(1)), None))
        .await?;
    assert_eq!(
        result, 7,
        "expired L1 must not shorten the original L2 fallback"
    );
    cache.shutdown().await?;
    Ok(())
}

#[derive(Default)]
struct SelectedCopy(AtomicUsize);
impl ValueCloner<u64> for SelectedCopy {
    fn clone_value(&self, value: &u64) -> Result<u64, CloneError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(*value)
    }
}

#[tokio::test]
async fn scalar_call_override_preserves_both_selected_auto_clone_copies() -> amalgam::Result<()> {
    let clock = Arc::new(ManualClock::default());
    let copy = Arc::new(SelectedCopy::default());
    let cache = Cache::<u64>::builder()
        .clock(clock.clone())
        .distributed(Arc::new(InMemoryDistributedCache::new(clock)))
        .serializer(Arc::new(JsonSerializer))
        .value_cloner(copy.clone())
        .default_options(EntryOptions::new(Duration::from_secs(60)).with_skip_memory(true, false))
        .try_build()?;
    cache
        .set("copied", 7)
        .options(|options| options.with_skip_memory(false, true))
        .with_receipt()
        .await?
        .wait()
        .await?;
    assert_eq!(copy.0.load(Ordering::SeqCst), 0);
    let options = cache.entry_options().with_enable_auto_clone(true);
    assert_eq!(cache.try_get("copied").options(|_| options).await?, Some(7));
    assert_eq!(
        copy.0.load(Ordering::SeqCst),
        2,
        "L1 and caller retain their selected copies"
    );
    cache.shutdown().await?;
    Ok(())
}
