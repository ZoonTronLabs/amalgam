//! Immutable byte snapshots at the distributed provider boundary.
use std::ops::Deref;

/// An immutable owned L2 byte snapshot.
///
/// Clones may share their backing allocation. A provider must replace a stored
/// snapshot on writes and keep existing snapshots unchanged after removal.
/// Conversion back to `Vec<u8>` permits mutation of an independent owned buffer.
///
/// ```compile_fail
/// use amalgam::provider::DistributedBytes;
/// let mut bytes = DistributedBytes::from(vec![1, 2, 3]);
/// bytes[0] = 9;
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DistributedBytes(::bytes::Bytes);
impl From<Vec<u8>> for DistributedBytes {
    fn from(value: Vec<u8>) -> Self {
        Self(value.into())
    }
}
impl From<DistributedBytes> for Vec<u8> {
    fn from(value: DistributedBytes) -> Self {
        value.0.into()
    }
}
impl AsRef<[u8]> for DistributedBytes {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}
impl Deref for DistributedBytes {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}
