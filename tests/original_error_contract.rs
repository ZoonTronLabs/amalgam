//! Codec/provider boundaries retain concrete causes and the selected failure policy.
#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

use std::error::Error as _;
use std::result::Result as StdResult;
use std::sync::Arc;
use std::time::Duration;

#[cfg(feature = "messagepack")]
use amalgam::provider::MessagePackSerializer;
#[cfg(feature = "postcard")]
use amalgam::provider::PostcardSerializer;
use amalgam::provider::{
    DistributedCache, DistributedEntry, DistributedSerializer, InMemoryDistributedCache,
    JsonSerializer, SystemClock,
};
#[cfg(feature = "redis")]
use amalgam::provider::{RedisBackplane, RedisDistributedCache};
use amalgam::{Cache, CacheEvent, CodecError, EntryOptions, Error, FactoryError, OperationOutcome};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize)]
struct RejectedValue;
impl Serialize for RejectedValue {
    fn serialize<S: serde::Serializer>(&self, _: S) -> StdResult<S::Ok, S::Error> {
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
    assert!(
        matches!(error, Error::Codec(CodecError::Serialization { .. })),
        "contract: assert encode source"
    );
    assert!(
        error.source().unwrap().downcast_ref::<E>().is_some(),
        "contract: assert encode source"
    );
}
fn assert_decode_source<E: std::error::Error + 'static>(codec: &dyn DistributedSerializer<u64>) {
    let error = codec.deserialize(&[]).unwrap_err();
    assert!(
        matches!(error, Error::Codec(CodecError::Deserialization { .. })),
        "contract: assert decode source"
    );
    assert!(
        error.source().unwrap().downcast_ref::<E>().is_some(),
        "contract: assert decode source"
    );
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
        .distributed_circuit_breaker(Duration::from_secs(60))
        .try_build()
        .unwrap();
    let mut events = cache.events().subscribe();
    // Select codec rethrow to inspect its original cause; the origin then uses
    // the configured codec suppression independently of transport policy.
    let error = cache
        .try_get("key")
        .options(|o| o.with_rethrow_serialization_exceptions(true))
        .await
        .unwrap_err();
    assert!(
        error
            .source()
            .unwrap()
            .downcast_ref::<serde_json::Error>()
            .is_some(),
        "contract: preserved codec source uses codec policy without tripping the transport circuit"
    );
    let value = cache
        .get_or_set(
            "key",
            amalgam::source::factory(
                |ctx| async move { Ok::<_, FactoryError>(ctx.modified(7).done()) },
            ),
        )
        .await
        .unwrap();
    assert_eq!(
        value, 7,
        "contract: preserved codec source uses codec policy without tripping the transport circuit"
    );
    let mut decode_events = 0;
    let mut circuit_events = 0;
    while let Ok(event) = events.try_recv() {
        match event {
            CacheEvent::DeserializationError { .. } => decode_events += 1,
            CacheEvent::CircuitBreakerChange { .. } => circuit_events += 1,
            _ => {}
        }
    }
    assert_eq!(
        decode_events, 2,
        "contract: preserved codec source uses codec policy without tripping the transport circuit"
    );
    assert_eq!(
        circuit_events, 0,
        "contract: preserved codec source uses codec policy without tripping the transport circuit"
    );
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
        assert!(
            Arc::ptr_eq(&id, &source.0),
            "contract: provider adapters keep original identity and component classification"
        );
        assert_eq!(
            OperationOutcome::from_error(&error),
            expected,
            "contract: provider adapters keep original identity and component classification"
        );
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
        OperationOutcome::DistributedError,
        "contract: redis connection boundaries retain native errors without network access"
    );
    assert!(
        distributed
            .source()
            .unwrap()
            .downcast_ref::<redis::RedisError>()
            .is_some(),
        "contract: redis connection boundaries retain native errors without network access"
    );
    let backplane = RedisBackplane::connect("invalid://connection")
        .await
        .err()
        .unwrap();
    assert_eq!(
        OperationOutcome::from_error(&backplane),
        OperationOutcome::BackplaneError,
        "contract: redis connection boundaries retain native errors without network access"
    );
    assert!(
        backplane
            .source()
            .unwrap()
            .downcast_ref::<redis::RedisError>()
            .is_some(),
        "contract: redis connection boundaries retain native errors without network access"
    );
}
