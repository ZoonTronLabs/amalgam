use std::{sync::Arc, time::Duration};

use amalgam::{Cache, ClearMode, EntryOptions, InMemoryDistributedCache, JsonSerializer, SystemClock};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let clock = Arc::new(SystemClock);
    let backend = Arc::new(InMemoryDistributedCache::new(clock.clone()));
    let build = || {
        Cache::<u64>::builder()
            .clock(clock.clone())
            .distributed(backend.clone())
            .serializer(Arc::new(JsonSerializer))
            .name("package-consumer")
            .default_options(EntryOptions::new(Duration::from_secs(60)))
            .try_build()
    };
    let first = build()?;
    first.try_set("answer", 42).await?.wait().await?;
    assert_eq!(first.read("answer", None).await?.value(), Some(&42));

    let cold = build()?;
    assert_eq!(cold.read("answer", None).await?.value(), Some(&42));
    cold.try_remove("answer").await?.wait().await?;
    first.try_clear(ClearMode::Remove).await?.wait().await?;
    assert!(!first.read("answer", None).await?.has_value());
    first.shutdown().await?;
    cold.shutdown().await?;
    Ok(())
}
