use amalgam::*;
use async_trait::async_trait;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

#[derive(Default)]
struct Counts {
    sync_encode: AtomicUsize,
    sync_decode: AtomicUsize,
    async_encode: AtomicUsize,
    async_decode: AtomicUsize,
    sync_queries: AtomicUsize,
}
enum Counterpart {
    AsyncOnly,
    Dual,
}
struct Codec {
    counts: Arc<Counts>,
    counterpart: Counterpart,
}
impl DistributedSerializer<u64> for Codec {
    fn serialize(&self, entry: &DistributedEntry<u64>) -> Result<Vec<u8>> {
        self.counts.sync_encode.fetch_add(1, Ordering::SeqCst);
        JsonSerializer.serialize(entry)
    }
    fn deserialize(&self, bytes: &[u8]) -> Result<DistributedEntry<u64>> {
        self.counts.sync_decode.fetch_add(1, Ordering::SeqCst);
        JsonSerializer.deserialize(bytes)
    }
}
#[async_trait]
impl AsyncDistributedSerializer<u64> for Codec {
    async fn serialize_snapshot(&self, snapshot: &DistributedSnapshot<u64>) -> Result<Vec<u8>> {
        self.counts.async_encode.fetch_add(1, Ordering::SeqCst);
        tokio::task::yield_now().await;
        JsonSerializer.serialize_snapshot(snapshot)
    }
    async fn deserialize_snapshot(&self, bytes: &[u8]) -> Result<DistributedSnapshot<u64>> {
        self.counts.async_decode.fetch_add(1, Ordering::SeqCst);
        tokio::task::yield_now().await;
        JsonSerializer.deserialize_snapshot(bytes)
    }
    fn sync_serializer(&self) -> Option<&dyn DistributedSerializer<u64>> {
        self.counts.sync_queries.fetch_add(1, Ordering::SeqCst);
        match self.counterpart {
            Counterpart::AsyncOnly => None,
            Counterpart::Dual => Some(self),
        }
    }
}

#[tokio::test]
async fn codec_preference_uses_the_available_model_for_write_read_and_expiration() {
    for (counterpart, mode, expected_async) in [
        (
            Counterpart::AsyncOnly,
            SerializationMode::SyncPreferred,
            true,
        ),
        (Counterpart::Dual, SerializationMode::AsyncPreferred, true),
        (Counterpart::Dual, SerializationMode::SyncPreferred, false),
    ] {
        let clock = Arc::new(ManualClock::default());
        let backend = Arc::new(InMemoryDistributedCache::new(clock.clone()));
        let counts = Arc::new(Counts::default());
        let codec = Arc::new(Codec {
            counts: counts.clone(),
            counterpart,
        });
        let cache = Cache::<u64>::builder()
            .clock(clock.clone())
            .distributed(backend)
            .async_serializer(codec)
            .serialization_mode(mode)
            .default_options(EntryOptions::new(Duration::from_secs(1)).with_fail_safe(
                true,
                Some(Duration::from_secs(60)),
                None,
            ))
            .try_build()
            .unwrap();
        // Builder discovery of an auto-cloner is independent of runtime codec
        // selection. Async preference must not query the sync hook during I/O.
        counts.sync_queries.store(0, Ordering::SeqCst);
        cache
            .try_set("value", 7)
            .await
            .unwrap()
            .wait()
            .await
            .unwrap();
        assert_eq!(
            cache
                .read(
                    "value",
                    Some(EntryOptions::default().with_skip_memory(true, false))
                )
                .await
                .unwrap()
                .value(),
            Some(&7)
        );
        clock.advance(Duration::from_millis(1));
        cache
            .try_expire("value")
            .await
            .unwrap()
            .wait()
            .await
            .unwrap();
        let stale = cache
            .read(
                "value",
                Some(
                    EntryOptions::default()
                        .with_skip_memory(true, false)
                        .with_allow_stale_on_read_only(true),
                ),
            )
            .await
            .unwrap();
        assert_eq!(stale.value(), Some(&7));
        if expected_async {
            assert!(counts.async_encode.load(Ordering::SeqCst) >= 2);
            assert!(counts.async_decode.load(Ordering::SeqCst) >= 2);
            assert_eq!(counts.sync_encode.load(Ordering::SeqCst), 0);
            assert_eq!(counts.sync_decode.load(Ordering::SeqCst), 0);
        } else {
            assert!(counts.sync_encode.load(Ordering::SeqCst) >= 2);
            assert!(counts.sync_decode.load(Ordering::SeqCst) >= 2);
            assert_eq!(counts.async_encode.load(Ordering::SeqCst), 0);
            assert_eq!(counts.async_decode.load(Ordering::SeqCst), 0);
        }
        match mode {
            SerializationMode::AsyncPreferred => {
                assert_eq!(counts.sync_queries.load(Ordering::SeqCst), 0);
            }
            SerializationMode::SyncPreferred => {
                assert!(counts.sync_queries.load(Ordering::SeqCst) >= 4);
            }
        }
        cache.shutdown().await.unwrap();
    }
}

struct ParkedCodec {
    entered: Arc<tokio::sync::Notify>,
    dropped: Arc<AtomicUsize>,
}
struct PendingCopy(Arc<AtomicUsize>);
impl Drop for PendingCopy {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[async_trait]
impl AsyncDistributedSerializer<u64> for ParkedCodec {
    async fn serialize_snapshot(&self, _: &DistributedSnapshot<u64>) -> Result<Vec<u8>> {
        let _pending = PendingCopy(self.dropped.clone());
        self.entered.notify_one();
        std::future::pending().await
    }
    async fn deserialize_snapshot(&self, bytes: &[u8]) -> Result<DistributedSnapshot<u64>> {
        JsonSerializer.deserialize_snapshot(bytes)
    }
}

#[tokio::test]
async fn cancelling_a_parked_async_codec_releases_it_without_committing_either_layer() {
    let entered = Arc::new(tokio::sync::Notify::new());
    let dropped = Arc::new(AtomicUsize::new(0));
    let backend = Arc::new(InMemoryDistributedCache::new(Arc::new(SystemClock)));
    let cache = Cache::<u64>::builder()
        .distributed(backend.clone())
        .async_serializer(Arc::new(ParkedCodec {
            entered: entered.clone(),
            dropped: dropped.clone(),
        }))
        .try_build()
        .unwrap();
    let source = CancellationSource::new();
    let mut operation =
        Box::pin(cache.try_set_full_cancellable("value", 7, None, Box::from([]), source.token()));
    std::future::poll_fn(|cx| {
        assert!(operation.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    entered.notified().await;
    assert_eq!(source.cancel(), CancellationRequest::Cancelled);
    assert_eq!(
        dropped.load(Ordering::SeqCst),
        1,
        "cancellation must drop the codec before a caller repolls"
    );
    assert!(matches!(
        operation.await,
        Err(Error::OperationCancelled {
            reason: FactoryCancellationReason::CallerCancelled
        })
    ));
    assert!(
        !cache
            .read(
                "value",
                Some(EntryOptions::default().with_skip_distributed(true, false))
            )
            .await
            .unwrap()
            .has_value()
    );
    assert!(backend.get("v2:value").await.unwrap().is_none());
    cache.shutdown().await.unwrap();
}

struct OuterOnlyCodec;
impl DistributedSerializer<u64> for OuterOnlyCodec {
    fn serialize(&self, _: &DistributedEntry<u64>) -> Result<Vec<u8>> {
        Err(Error::Serialization(
            "raw payload path must not be selected".into(),
        ))
    }
    fn deserialize(&self, _: &[u8]) -> Result<DistributedEntry<u64>> {
        Err(Error::Deserialization(
            "raw payload path must not be selected".into(),
        ))
    }
    fn serialize_snapshot(&self, snapshot: &DistributedSnapshot<u64>) -> Result<Vec<u8>> {
        JsonSerializer.serialize_snapshot(snapshot)
    }
    fn deserialize_snapshot(&self, bytes: &[u8]) -> Result<DistributedSnapshot<u64>> {
        JsonSerializer.deserialize_snapshot(bytes)
    }
}
#[tokio::test]
async fn legacy_snapshot_overrides_keep_their_contract_even_with_async_preference() {
    let backend = Arc::new(InMemoryDistributedCache::new(Arc::new(SystemClock)));
    let cache = Cache::<u64>::builder()
        .distributed(backend)
        .serializer(Arc::new(OuterOnlyCodec))
        .serialization_mode(SerializationMode::AsyncPreferred)
        .try_build()
        .unwrap();
    cache
        .try_set("value", 7)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert_eq!(
        cache
            .read(
                "value",
                Some(EntryOptions::default().with_skip_memory(true, false))
            )
            .await
            .unwrap()
            .value(),
        Some(&7)
    );
    cache.shutdown().await.unwrap();
}
