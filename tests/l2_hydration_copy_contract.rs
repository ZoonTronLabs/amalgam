use amalgam::provider::{InMemoryDistributedCache, JsonSerializer, ManualClock, ValueCloner};
use amalgam::{Cache, CloneError, EntryOptions};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

static ORDINARY_CLONES: AtomicUsize = AtomicUsize::new(0);

#[derive(Serialize, Deserialize)]
struct Payload(u64);
impl Clone for Payload {
    fn clone(&self) -> Self {
        ORDINARY_CLONES.fetch_add(1, Ordering::SeqCst);
        Self(self.0)
    }
}
#[derive(Default)]
struct SuppliedCopy(AtomicUsize);
impl ValueCloner<Payload> for SuppliedCopy {
    fn clone_value(&self, value: &Payload) -> Result<Payload, CloneError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Payload(value.0))
    }
}

#[tokio::test]
async fn l2_hydration_uses_the_supplied_copy_without_an_ordinary_clone() -> amalgam::Result<()> {
    let clock = Arc::new(ManualClock::default());
    let backend = Arc::new(InMemoryDistributedCache::new(clock.clone()));
    let copy = Arc::new(SuppliedCopy::default());
    let options = EntryOptions::new(Duration::from_secs(60))
        .with_enable_auto_clone(true)
        .with_skip_memory(true, false);
    let cache = Cache::<Payload>::builder()
        .clock(clock)
        .distributed(backend)
        .serializer(Arc::new(JsonSerializer))
        .value_cloner(copy.clone())
        .default_options(options.clone())
        .try_build()?;
    cache
        .set("payload", Payload(42))
        .options(|options| options.with_skip_memory(false, true))
        .with_receipt()
        .await?
        .wait()
        .await?;
    ORDINARY_CLONES.store(0, Ordering::SeqCst);
    copy.0.store(0, Ordering::SeqCst);

    let from_l2 = cache.read("payload", None).await?;
    assert_eq!(from_l2.value().map(|value| value.0), Some(42));
    assert_eq!(ORDINARY_CLONES.load(Ordering::SeqCst), 0);
    assert_eq!(
        copy.0.load(Ordering::SeqCst),
        2,
        "L1 and caller each copy once"
    );

    let from_l1 = cache
        .read(
            "payload",
            Some(
                options
                    .with_skip_memory(false, false)
                    .with_skip_distributed(true, false),
            ),
        )
        .await?;
    assert_eq!(from_l1.value().map(|value| value.0), Some(42));
    assert_eq!(ORDINARY_CLONES.load(Ordering::SeqCst), 0);
    assert_eq!(copy.0.load(Ordering::SeqCst), 3);
    cache.shutdown().await?;
    Ok(())
}
