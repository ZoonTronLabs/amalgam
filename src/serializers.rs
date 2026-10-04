//! Additional L2 serializers behind feature flags.
//!
//! The default [`JsonSerializer`](crate::JsonSerializer) lives in
//! [`crate::distributed`]; this module hosts alternative formats.

use crate::error::{CloneError, ConfigError, Result};
use crate::options::EntryOptions;

/// An extensible deep-copy strategy, independent of Serde and distributed I/O.
///
/// A successful copy must preserve the logical value while isolating mutable
/// state from the input. Returning an ordinary shared `Arc` clone would violate
/// this contract. Implementations may use serialization or a custom copy graph.
pub trait ValueCloner<V>: Send + Sync {
    /// Produces an isolated value or preserves the original cloning failure.
    fn clone_value(&self, value: &V) -> std::result::Result<V, CloneError>;
}

/// The common input/output boundary for a cache's requested isolation policy.
///
/// Auto-clone never silently falls back to ordinary [`Clone`]. This helper also
/// validates legacy option requests before invoking an external strategy.
pub fn copy_value<V: Clone>(
    value: &V,
    options: &EntryOptions,
    cloner: Option<&dyn ValueCloner<V>>,
) -> Result<V> {
    options.validate()?;
    if !options.enable_auto_clone() {
        return Ok(value.clone());
    }
    match cloner {
        Some(cloner) => cloner.clone_value(value).map_err(Into::into),
        None => Err(ConfigError::AutoCloneWithoutCloner.into()),
    }
}

impl<V: serde::Serialize + serde::de::DeserializeOwned> ValueCloner<V>
    for crate::distributed::JsonSerializer
{
    fn clone_value(&self, value: &V) -> std::result::Result<V, CloneError> {
        let bytes = serde_json::to_vec(value).map_err(|source| CloneError::Serialization {
            source: Box::new(source),
        })?;
        serde_json::from_slice(&bytes).map_err(|source| CloneError::Deserialization {
            source: Box::new(source),
        })
    }
}

#[cfg(feature = "messagepack")]
mod messagepack {
    use serde::Serialize;
    use serde::de::DeserializeOwned;

    use crate::distributed::{DistributedEntry, DistributedSerializer};
    use crate::error::CloneError;
    use crate::error::{Error, Result};
    use crate::serializers::ValueCloner;

    /// A compact MessagePack serializer (feature `messagepack`), backed by
    /// `rmp-serde`. A drop-in alternative to `JsonSerializer` for smaller L2
    /// payloads.
    #[derive(Debug, Clone, Copy, Default)]
    pub struct MessagePackSerializer;

    impl<V: Serialize + DeserializeOwned> ValueCloner<V> for MessagePackSerializer {
        fn clone_value(&self, value: &V) -> std::result::Result<V, CloneError> {
            let bytes = rmp_serde::to_vec(value).map_err(|source| CloneError::Serialization {
                source: Box::new(source),
            })?;
            rmp_serde::from_slice(&bytes).map_err(|source| CloneError::Deserialization {
                source: Box::new(source),
            })
        }
    }

    impl<V> DistributedSerializer<V> for MessagePackSerializer
    where
        V: Serialize + DeserializeOwned,
    {
        fn value_cloner(&self) -> Option<std::sync::Arc<dyn ValueCloner<V>>> {
            Some(std::sync::Arc::new(*self))
        }

        fn serialize(&self, entry: &DistributedEntry<V>) -> Result<Vec<u8>> {
            rmp_serde::to_vec(entry).map_err(|e| Error::Serialization(e.to_string()))
        }

        fn deserialize(&self, bytes: &[u8]) -> Result<DistributedEntry<V>> {
            rmp_serde::from_slice(bytes).map_err(|e| Error::Deserialization(e.to_string()))
        }
    }
}

#[cfg(feature = "messagepack")]
pub use messagepack::MessagePackSerializer;

#[cfg(feature = "postcard")]
mod postcard_serializer {
    use serde::Serialize;
    use serde::de::DeserializeOwned;

    use crate::distributed::{DistributedEntry, DistributedSerializer};
    use crate::error::CloneError;
    use crate::error::{Error, Result};
    use crate::serializers::ValueCloner;

    /// A compact, zero-dependency binary serializer (feature `postcard`), backed
    /// by [`postcard`](https://docs.rs/postcard). The smallest of the built-in
    /// formats — a good fit for high-volume L2 payloads. The same
    /// [`DistributedSerializer`] seam accepts any other `serde` codec (bincode,
    /// protobuf, …) just as easily.
    #[derive(Debug, Clone, Copy, Default)]
    pub struct PostcardSerializer;

    impl<V: Serialize + DeserializeOwned> ValueCloner<V> for PostcardSerializer {
        fn clone_value(&self, value: &V) -> std::result::Result<V, CloneError> {
            let bytes =
                postcard::to_allocvec(value).map_err(|source| CloneError::Serialization {
                    source: Box::new(source),
                })?;
            postcard::from_bytes(&bytes).map_err(|source| CloneError::Deserialization {
                source: Box::new(source),
            })
        }
    }

    impl<V> DistributedSerializer<V> for PostcardSerializer
    where
        V: Serialize + DeserializeOwned,
    {
        fn value_cloner(&self) -> Option<std::sync::Arc<dyn ValueCloner<V>>> {
            Some(std::sync::Arc::new(*self))
        }

        fn serialize(&self, entry: &DistributedEntry<V>) -> Result<Vec<u8>> {
            postcard::to_allocvec(entry).map_err(|e| Error::Serialization(e.to_string()))
        }

        fn deserialize(&self, bytes: &[u8]) -> Result<DistributedEntry<V>> {
            postcard::from_bytes(bytes).map_err(|e| Error::Deserialization(e.to_string()))
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn round_trips_envelope() {
            let entry = DistributedEntry {
                value: "v".to_owned(),
                created_ticks: 1,
                logical_expiration_ticks: 2,
                physical_expiration_ticks: 3,
                is_from_fail_safe: false,
                etag: None,
                last_modified_ticks: None,
                tags: vec!["t".to_owned()],
            };
            let ser = PostcardSerializer;
            let bytes = DistributedSerializer::<String>::serialize(&ser, &entry).unwrap();
            let back = DistributedSerializer::<String>::deserialize(&ser, &bytes).unwrap();
            assert_eq!(back.value, "v");
            assert_eq!(back.tags, vec!["t".to_owned()]);
        }
    }
}

#[cfg(feature = "postcard")]
pub use postcard_serializer::PostcardSerializer;
