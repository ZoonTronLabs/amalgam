//! Additional L2 serializers behind feature flags.
//!
//! The default [`JsonSerializer`](crate::JsonSerializer) lives in
//! [`crate::distributed`]; this module hosts alternative formats.

#[cfg(feature = "messagepack")]
mod messagepack {
    use serde::Serialize;
    use serde::de::DeserializeOwned;

    use crate::distributed::{DistributedEntry, DistributedSerializer};
    use crate::error::{Error, Result};

    /// A compact MessagePack serializer (feature `messagepack`), backed by
    /// `rmp-serde`. A drop-in alternative to `JsonSerializer` for smaller L2
    /// payloads.
    #[derive(Debug, Clone, Copy, Default)]
    pub struct MessagePackSerializer;

    impl<V> DistributedSerializer<V> for MessagePackSerializer
    where
        V: Serialize + DeserializeOwned,
    {
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
    use crate::error::{Error, Result};

    /// A compact, zero-dependency binary serializer (feature `postcard`), backed
    /// by [`postcard`](https://docs.rs/postcard). The smallest of the built-in
    /// formats — a good fit for high-volume L2 payloads. The same
    /// [`DistributedSerializer`] seam accepts any other `serde` codec (bincode,
    /// protobuf, …) just as easily.
    #[derive(Debug, Clone, Copy, Default)]
    pub struct PostcardSerializer;

    impl<V> DistributedSerializer<V> for PostcardSerializer
    where
        V: Serialize + DeserializeOwned,
    {
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
