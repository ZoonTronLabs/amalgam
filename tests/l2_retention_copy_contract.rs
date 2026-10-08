//! Attaching persisted retention to an owned L2 value must not clone it.
use amalgam::provider::{
    Clock, DistributedEntry, DistributedSnapshot, InMemoryDistributedCache, JsonSerializer,
    ManualClock, MemoryStorage, SnapshotRetention, ValueCloner,
};
use amalgam::{Cache, CloneError, EntryOptions, EntryWeight, Priority, Timestamp, source};
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

#[path = "support/memory_storage_fixture.rs"]
mod storage_fixture;
use storage_fixture::MapStorage;

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
#[derive(Clone, Copy)]
enum Retrieval {
    Read,
    Origin,
}
#[tokio::test]
async fn cache_l2_retention_uses_selected_copies_and_preserves_all_metadata() -> amalgam::Result<()>
{
    for retrieval in [Retrieval::Read, Retrieval::Origin] {
        let clock = Arc::new(ManualClock::default());
        let created = clock.now();
        let backend = Arc::new(InMemoryDistributedCache::new(clock.clone()));
        let memory = MapStorage::new();
        let copy = Arc::new(SuppliedCopy::default());
        let options = EntryOptions::new(Duration::from_secs(60))
            .with_fail_safe(
                true,
                Some(Duration::from_secs(120)),
                Some(Duration::from_secs(1)),
            )
            .with_priority(Priority::High)
            .with_size(9)
            .with_enable_auto_clone(true)
            .with_skip_memory(true, false);
        let cache = Cache::builder()
            .clock(clock.clone())
            .distributed(backend)
            .serializer(Arc::new(JsonSerializer))
            .memory_storage(memory.clone())
            .value_cloner(copy.clone())
            .default_options(options.clone())
            .try_build()?;
        cache
            .set("payload", Payload(42))
            .options(|options| options.with_skip_memory(false, true))
            .tags(["alpha", "beta"])
            .with_receipt()
            .await?
            .wait()
            .await?;
        assert!(memory.get("payload")?.is_none());
        clock.advance(Duration::from_secs(10));
        ORDINARY_CLONES.store(0, Ordering::SeqCst);
        copy.0.store(0, Ordering::SeqCst);
        let lookup = options
            .with_duration(Duration::from_secs(7))
            .with_size(3)
            .with_priority(Priority::Low);
        let result = match retrieval {
            Retrieval::Read => cache.try_get("payload").options(|_| lookup).await?.unwrap(),
            Retrieval::Origin => {
                cache
                    .get_or_set(
                        "payload",
                        source::factory(|_| async {
                            panic!("a valid L2 snapshot must not invoke the factory");
                            #[allow(unreachable_code)]
                            Ok::<_, Infallible>(Payload(99))
                        }),
                    )
                    .options(|_| lookup)
                    .await?
            }
        };
        assert_eq!(result.0, 42);
        assert_eq!(
            ORDINARY_CLONES.load(Ordering::SeqCst),
            0,
            "attaching retention must move the owned decoded value"
        );
        assert_eq!(
            copy.0.load(Ordering::SeqCst),
            2,
            "L1 and caller each copy once"
        );
        let record = memory.get("payload")?.unwrap();
        let stored = record.entry();
        assert_eq!(stored.value().0, 42);
        assert_eq!(stored.meta().created(), created);
        assert_eq!(stored.meta().size(), Some(EntryWeight::new(9)));
        assert_eq!(stored.meta().priority(), Priority::High);
        assert_eq!(
            stored.meta().logical_expiration(),
            created.saturating_add(Duration::from_secs(17))
        );
        assert_eq!(
            stored.meta().physical_expiration(),
            created.saturating_add(Duration::from_secs(120))
        );
        assert_eq!(
            stored
                .meta()
                .tags()
                .iter()
                .map(|tag| tag.as_str())
                .collect::<Vec<_>>(),
            ["alpha", "beta"]
        );
        drop(record);
        cache.shutdown().await?;
    }
    Ok(())
}

static PUBLIC_CLONES: AtomicUsize = AtomicUsize::new(0);
struct PublicPayload(u64);
impl Clone for PublicPayload {
    fn clone(&self) -> Self {
        PUBLIC_CLONES.fetch_add(1, Ordering::SeqCst);
        Self(self.0)
    }
}
#[test]
fn public_snapshot_retention_helper_keeps_its_existing_clone_contract() {
    PUBLIC_CLONES.store(0, Ordering::SeqCst);
    let snapshot = DistributedSnapshot::new(
        DistributedEntry {
            value: PublicPayload(7),
            created_ticks: 0,
            logical_expiration_ticks: 10,
            physical_expiration_ticks: 20,
            is_from_fail_safe: false,
            etag: Some("validator".into()),
            last_modified_ticks: Some(1),
            tags: vec!["alpha".into()],
        },
        Timestamp::from_ticks(0),
        SnapshotRetention::Specified {
            size: Some(EntryWeight::new(9)),
            priority: Priority::High,
        },
    )
    .unwrap();
    let entry = snapshot.try_into_entry(Timestamp::from_ticks(2)).unwrap();
    assert_eq!(entry.value().0, 7);
    assert_eq!(entry.meta().size(), Some(EntryWeight::new(9)));
    assert_eq!(entry.meta().priority(), Priority::High);
    assert_eq!(entry.meta().etag(), Some("validator"));
    assert_eq!(entry.meta().last_modified(), Some(Timestamp::from_ticks(1)));
    assert_eq!(PUBLIC_CLONES.load(Ordering::SeqCst), 1);
}
