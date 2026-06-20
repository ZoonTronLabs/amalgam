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
