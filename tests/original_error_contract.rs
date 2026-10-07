//! Codec/provider boundaries retain concrete causes and the selected failure policy.
use amalgam::*;
use serde::{Deserialize, Serialize};
use std::error::Error as _;
use std::sync::Arc;

#[derive(Clone, Debug, Deserialize)]
struct RejectedValue;
impl Serialize for RejectedValue {
    fn serialize<S: serde::Serializer>(&self, _: S) -> std::result::Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom("origin encoding rejected"))
    }
}

fn entry() -> DistributedEntry<RejectedValue> {
    DistributedEntry {
        value: RejectedValue,
        created_ticks: 1,
        logical_expiration_ticks: 2,
        physical_expiration_ticks: 3,
        is_from_fail_safe: false,
        etag: None,
        last_modified_ticks: None,
        tags: Vec::new(),
    }
}

fn assert_encode_source<E: std::error::Error + 'static>(
    codec: &dyn DistributedSerializer<RejectedValue>,
) {
    let error = codec.serialize(&entry()).unwrap_err();
    assert!(matches!(
        error,
        Error::Codec(CodecError::Serialization { .. })
    ));
    assert!(error.source().unwrap().downcast_ref::<E>().is_some());
}
fn assert_decode_source<E: std::error::Error + 'static>(codec: &dyn DistributedSerializer<u64>) {
    let error = codec.deserialize(&[]).unwrap_err();
    assert!(matches!(
        error,
        Error::Codec(CodecError::Deserialization { .. })
    ));
    assert!(error.source().unwrap().downcast_ref::<E>().is_some());
}

#[test]
fn native_codecs_retain_their_concrete_errors_for_both_directions() {
    assert_encode_source::<serde_json::Error>(&JsonSerializer);
    assert_decode_source::<serde_json::Error>(&JsonSerializer);
    #[cfg(feature = "messagepack")]
    {
        assert_encode_source::<rmp_serde::encode::Error>(&MessagePackSerializer);
        assert_decode_source::<rmp_serde::decode::Error>(&MessagePackSerializer);
    }
    #[cfg(feature = "postcard")]
    {
        assert_encode_source::<postcard::Error>(&PostcardSerializer);
        assert_decode_source::<postcard::Error>(&PostcardSerializer);
    }
}

#[tokio::test]
async fn preserved_codec_source_uses_codec_policy_without_tripping_the_transport_circuit() {
    let backend = Arc::new(InMemoryDistributedCache::new(Arc::new(SystemClock)));
    backend
        .set("v2:key", b"invalid JSON".to_vec(), None)
        .await
        .unwrap();
    let cache = Cache::<u64>::builder()
        .distributed(backend)
        .serializer(Arc::new(JsonSerializer))
        .default_options(
            EntryOptions::default()
                .with_rethrow_distributed_exceptions(true)
                .with_rethrow_serialization_exceptions(false),
        )
        .distributed_circuit_breaker(std::time::Duration::from_secs(60))
        .try_build()
        .unwrap();
    let mut events = cache.events().subscribe();
    // Read-only operations preserve a failure despite suppression; origin
    // operations honor codec suppression independently of transport policy.
    let error = cache.read("key", None).await.unwrap_err();
    assert!(
        error
            .source()
            .unwrap()
            .downcast_ref::<serde_json::Error>()
            .is_some()
    );
    let value = cache
        .get_or_set("key", |ctx| async move {
            Ok::<_, amalgam::FactoryError>(ctx.modified(7).done())
        })
        .await
        .unwrap();
    assert_eq!(value, 7);
    let mut decode_events = 0;
    let mut circuit_events = 0;
    while let Ok(event) = events.try_recv() {
        match event {
            CacheEvent::DeserializationError { .. } => decode_events += 1,
            CacheEvent::CircuitBreakerChange { .. } => circuit_events += 1,
            _ => {}
        }
    }
    assert_eq!(decode_events, 2);
    assert_eq!(circuit_events, 0);
    cache.shutdown().await.unwrap();
}

#[derive(Debug, thiserror::Error)]
#[error("original provider failure")]
struct OriginalSource(Arc<()>);

#[test]
fn provider_adapters_keep_original_identity_and_component_classification() {
    let id = Arc::new(());
    for (error, expected) in [
        (
            Error::distributed(OriginalSource(id.clone())),
            OperationOutcome::DistributedError,
        ),
        (
            Error::backplane(OriginalSource(id.clone())),
            OperationOutcome::BackplaneError,
        ),
    ] {
        let source = error
            .source()
            .unwrap()
            .downcast_ref::<OriginalSource>()
            .unwrap();
        assert!(Arc::ptr_eq(&id, &source.0));
        assert_eq!(OperationOutcome::from_error(&error), expected);
    }
}

#[cfg(feature = "redis")]
#[tokio::test]
async fn redis_connection_boundaries_retain_native_errors_without_network_access() {
    let distributed = RedisDistributedCache::connect("invalid://connection")
        .await
        .err()
        .unwrap();
    assert_eq!(
        OperationOutcome::from_error(&distributed),
        OperationOutcome::DistributedError
    );
    assert!(
        distributed
            .source()
            .unwrap()
            .downcast_ref::<redis::RedisError>()
            .is_some()
    );
    let backplane = RedisBackplane::connect("invalid://connection")
        .await
        .err()
        .unwrap();
    assert_eq!(
        OperationOutcome::from_error(&backplane),
        OperationOutcome::BackplaneError
    );
    assert!(
        backplane
            .source()
            .unwrap()
            .downcast_ref::<redis::RedisError>()
            .is_some()
    );
}
