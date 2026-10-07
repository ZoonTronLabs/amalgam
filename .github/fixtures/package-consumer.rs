use std::{sync::Arc, time::Duration};

use amalgam::{
    Cache, ClearMode, EntryOptions,
    provider::{InMemoryDistributedCache, JsonSerializer, SystemClock},
    source,
};

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
    first.set("answer", 42).with_receipt().await?.wait().await?;
    assert_eq!(first.read("answer", None).await?.value(), Some(&42));

    let cold = build()?;
    assert_eq!(cold.read("answer", None).await?.value(), Some(&42));
    cold.remove("answer").with_receipt().await?.wait().await?;
    first
        .clear(ClearMode::Remove)
        .with_receipt()
        .await?
        .wait()
        .await?;
    assert!(!first.read("answer", None).await?.has_value());
    first.shutdown().await?;
    cold.shutdown().await?;

    let native = amalgam::BlockingCache::<Option<u64>>::new()?;
    let value = native
        .get_or_set("null", source::value(None))
        .cancellation(amalgam::CancellationSource::new().token())
        .with_receipt()
        .execute()?;
    assert_eq!(value.value, None);
    if let amalgam::advanced::BlockingCommitReceipt::Mutation(receipt) = value.commit {
        receipt.wait()?;
    }
    let asynchronous = native.as_async().clone();
    drop(native);
    assert_eq!(
        asynchronous.read("null", None).await?.into_value(),
        Some(None)
    );
    asynchronous.shutdown().await?;
    Ok(())
}
