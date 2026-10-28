//! Shared snapshots keep reads stable across provider replacement/removal.
#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

use std::sync::Arc;
use std::time::Duration;

use amalgam::provider::{
    DistributedBytes, DistributedCache, InMemoryDistributedCache, ManualClock,
};

#[tokio::test]
async fn immutable_reads_survive_replacement_removal_and_mutable_conversion() -> amalgam::Result<()>
{
    let clock = Arc::new(ManualClock::default());
    let backend = InMemoryDistributedCache::new(clock.clone());
    backend
        .set("snapshot", vec![1, 2, 3], Some(Duration::from_secs(10)))
        .await?;
    let first = backend.get("snapshot").await?.unwrap();
    let second = backend.get("snapshot").await?.unwrap();
    assert_eq!(
        first.as_ref(),
        [1, 2, 3],
        "contract: immutable reads survive replacement removal and mutable conversion"
    );
    assert_eq!(
        first.as_ptr(),
        second.as_ptr(),
        "in-memory snapshots share their byte allocation"
    );
    let mut editable = Vec::from(first.clone());
    editable[0] = 9;
    assert_eq!(
        backend.get("snapshot").await?.unwrap().as_ref(),
        [1, 2, 3],
        "contract: immutable reads survive replacement removal and mutable conversion"
    );
    backend.set("snapshot", vec![4, 5], None).await?;
    assert_eq!(
        first.as_ref(),
        [1, 2, 3],
        "contract: immutable reads survive replacement removal and mutable conversion"
    );
    assert_eq!(
        second.as_ref(),
        [1, 2, 3],
        "contract: immutable reads survive replacement removal and mutable conversion"
    );
    assert_eq!(
        backend.get("snapshot").await?.unwrap().as_ref(),
        [4, 5],
        "contract: immutable reads survive replacement removal and mutable conversion"
    );
    backend.remove("snapshot").await?;
    assert!(
        backend.get("snapshot").await?.is_none(),
        "contract: immutable reads survive replacement removal and mutable conversion"
    );
    assert_eq!(
        first.as_ref(),
        [1, 2, 3],
        "contract: immutable reads survive replacement removal and mutable conversion"
    );
    Ok(())
}
#[tokio::test]
async fn ttl_does_not_extend_key_lifetime_or_invalidate_a_returned_snapshot() -> amalgam::Result<()>
{
    let clock = Arc::new(ManualClock::default());
    let backend = InMemoryDistributedCache::new(clock.clone());
    backend
        .set("ttl", vec![7], Some(Duration::from_secs(1)))
        .await?;
    let retained = backend.get("ttl").await?.unwrap();
    clock.advance(Duration::from_secs(1));
    assert!(
        backend.get("ttl").await?.is_none(),
        "contract: ttl does not extend key lifetime or invalidate a returned snapshot"
    );
    assert_eq!(
        retained.as_ref(),
        [7],
        "contract: ttl does not extend key lifetime or invalidate a returned snapshot"
    );
    assert_eq!(
        Vec::from(DistributedBytes::from(Vec::new())),
        Vec::<u8>::new(),
        "contract: ttl does not extend key lifetime or invalidate a returned snapshot"
    );
    Ok(())
}
