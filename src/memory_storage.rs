//! Open, synchronous in-process storage for opaque cache-owned records.
use crate::entry::Entry;
use crate::{CapacityRejection, MemoryEvictionReason, MemoryUsage, Timestamp};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Expected storage failure; a failed lookup never represents an absent key.
#[derive(Debug, Clone, thiserror::Error)]
pub enum MemoryStorageError {
    /// An implementation preserves its original typed cause.
    #[error("memory storage provider failed: {source}")]
    Provider {
        /// The original provider cause.
        #[source]
        source: Arc<dyn std::error::Error + Send + Sync>,
    },
    /// Clear generations must never wrap and make old records visible again.
    #[error("memory storage generation exhausted")]
    GenerationExhausted,
}
impl MemoryStorageError {
    /// Preserves a concrete provider failure through the canonical cache API.
    pub fn provider(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Provider {
            source: Arc::new(source),
        }
    }
}

/// One processed-key prefix sharing clear visibility in a supplied keyspace.
/// Providers retain distinct stable epochs for distinct namespaces.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MemoryNamespace(Arc<str>);
impl MemoryNamespace {
    pub(crate) fn new(prefix: Option<&str>) -> Self {
        Self(Arc::from(prefix.unwrap_or("")))
    }
    /// The exact configured prefix; no prefix is the empty namespace.
    pub fn key_prefix(&self) -> &str {
        &self.0
    }
}

/// One stable identity shared by every cache using the same provider keyspace.
/// Only the host advances it; providers expose the same handle on every call.
#[derive(Debug, Clone)]
pub struct MemoryStorageEpoch(Arc<AtomicU64>);
impl Default for MemoryStorageEpoch {
    fn default() -> Self {
        Self::new()
    }
}
impl MemoryStorageEpoch {
    /// Starts a live, non-reusable record generation.
    pub fn new() -> Self {
        Self(Arc::new(AtomicU64::new(1)))
    }
    pub(crate) fn same(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    pub(crate) fn current(&self) -> Result<MemoryGeneration, MemoryStorageError> {
        let value = self.0.load(Ordering::Acquire);
        if value == 0 {
            Err(MemoryStorageError::GenerationExhausted)
        } else {
            Ok(MemoryGeneration {
                epoch: self.clone(),
                value,
            })
        }
    }
    pub(crate) fn advance(&self) -> Result<MemoryGeneration, MemoryStorageError> {
        #[allow(deprecated, reason = "Atomic::try_update is unavailable on Rust 1.88")]
        let result = self
            .0
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                if value == 0 {
                    None
                } else {
                    Some(value.checked_add(1).unwrap_or(0))
                }
            });
        match result {
            Ok(value) => match value.checked_add(1) {
                Some(value) => Ok(MemoryGeneration {
                    epoch: self.clone(),
                    value,
                }),
                None => Err(MemoryStorageError::GenerationExhausted),
            },
            Err(_) => Err(MemoryStorageError::GenerationExhausted),
        }
    }
}
/// A host-issued visibility barrier. Earlier clears cannot remove newer writes.
#[derive(Debug, Clone)]
pub struct MemoryGeneration {
    epoch: MemoryStorageEpoch,
    value: u64,
}
impl MemoryGeneration {
    /// Selects records preceding this barrier in this provider's keyspace.
    pub fn precedes<V>(&self, record: &MemoryRecord<V>) -> bool {
        self.epoch.same(&record.inner.generation.epoch)
            && record.inner.generation.value < self.value
    }
    pub(crate) fn is_current(&self) -> bool {
        self.value != 0 && self.epoch.0.load(Ordering::Acquire) == self.value
    }
}

/// An immutable host-created record. Cloning never calls `V::clone`.
/// Its metadata, original observer route and generation cannot be modified.
pub struct MemoryRecord<V> {
    pub(crate) inner: Arc<RecordInner<V>>,
}
pub(crate) struct RecordInner<V> {
    pub(crate) key: Arc<str>,
    pub(crate) entry: Entry<V>,
    pub(crate) generation: MemoryGeneration,
    pub(crate) expires: Option<std::time::Instant>,
    pub(crate) owner: crate::memory::MemoryObserver<V>,
    pub(crate) capture: crate::memory::CaptureAdmission,
}
impl<V> Clone for MemoryRecord<V> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}
impl<V> std::fmt::Debug for MemoryRecord<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryRecord")
            .field("key", &self.key())
            .finish_non_exhaustive()
    }
}
impl<V> MemoryRecord<V> {
    /// The processed key, including an explicitly configured cache prefix.
    pub fn key(&self) -> &str {
        &self.inner.key
    }
    /// Original value and all expiration, tags, validators and weight metadata.
    pub fn entry(&self) -> &Entry<V> {
        &self.inner.entry
    }
    /// A representation must be both physically live and still read-eligible.
    /// Providers use this inside the same guard as conditional admission.
    pub fn is_live_at(&self, now: Timestamp) -> bool {
        self.retirement_reason(now).is_none()
    }
    /// The physical retirement cause, or None for a live representation.
    pub fn retirement_reason(&self, now: Timestamp) -> Option<MemoryEvictionReason> {
        if !self.inner.generation.is_current() || !self.entry().is_read_eligible() {
            Some(MemoryEvictionReason::Removed)
        } else if self.entry().is_physically_expired(now)
            || self
                .inner
                .expires
                .is_some_and(|deadline| std::time::Instant::now() >= deadline)
        {
            Some(MemoryEvictionReason::Expired)
        } else {
            None
        }
    }
    /// Identifies the exact immutable representation, never value equality.
    pub fn is_same_entry(&self, entry: &Entry<V>) -> bool {
        self.entry().is_same_instance(entry)
    }
    /// Actual stored weight for capacity accounting.
    pub fn weight(&self) -> u128 {
        u128::from(
            self.entry()
                .meta()
                .size()
                .map_or(1, |weight| weight.units()),
        )
    }
}

/// The atomic write condition, evaluated against a physically live record.
#[derive(Debug)]
pub enum MemoryCondition<'a, V> {
    /// Explicit writes may replace the current representation.
    Any,
    /// An absent or ineligible old slot may be admitted.
    Absent,
    /// Hydration/expiry may replace only the exact selected representation.
    Same(&'a Entry<V>),
}
impl<V> Copy for MemoryCondition<'_, V> {}
impl<V> Clone for MemoryCondition<'_, V> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<V> MemoryCondition<'_, V> {
    /// Evaluate while holding the same provider guard used for insertion.
    pub fn matches(&self, current: Option<&MemoryRecord<V>>, now: Timestamp) -> bool {
        let current = current.filter(|record| record.is_live_at(now));
        match self {
            Self::Any => true,
            Self::Absent => current.is_none(),
            Self::Same(entry) => current.is_some_and(|record| record.is_same_entry(entry)),
        }
    }
}
/// Physical removal ownership transferred out of provider guards.
#[derive(Debug)]
pub struct MemoryRetirement<V> {
    pub(crate) record: MemoryRecord<V>,
    pub(crate) reason: MemoryEvictionReason,
}
impl<V> MemoryRetirement<V> {
    /// Return every removed record to the host for observation and reclamation.
    pub fn new(record: MemoryRecord<V>, reason: MemoryEvictionReason) -> Self {
        Self { record, reason }
    }
}
/// Explicit admission and owned retirements. Rejection preserves valid contents.
#[derive(Debug)]
pub enum MemoryStorageWrite<V> {
    /// A previously absent/ineligible slot was filled.
    Admitted {
        /// Removed expired or capacity-victim records, already outside guards.
        retired: Box<[MemoryRetirement<V>]>,
    },
    /// A live slot was replaced. The host distinguishes set from metadata expiry.
    Replaced {
        /// The exact old representation from the replaced slot.
        previous: MemoryRecord<V>,
        /// Additional physically removed records, already outside guards.
        retired: Box<[MemoryRetirement<V>]>,
    },
    /// No candidate was stored; opportunistically expired slots may be returned.
    Rejected {
        /// Why this candidate was not stored.
        reason: CapacityRejection,
        /// Opportunistically removed expired records, already outside guards.
        retired: Box<[MemoryRetirement<V>]>,
    },
}
/// Supplied in-process storage. Operations are synchronous and do not invoke
/// observers. `try_get` must not wait; `None` permits the ordinary lookup path.
///
/// All mutations are atomic. On error contents are unchanged. Return removed
/// records after releasing all provider guards; never destroy their last owned
/// handles under a guard. Admission validates the candidate's `is_live_at(now)`
/// and the condition in the same transaction. `clear_before` removes only records
/// selected by the supplied barrier, preserving concurrently admitted new writes.
/// Providers own capacity/priority policy; records expose validated metadata.
/// The cache retains only an Arc and does not dispose a shared provider at shutdown.
pub trait MemoryStorage<V>: Send + Sync + 'static {
    /// A stable identity for this namespace, distinct from other namespaces.
    fn epoch(&self, namespace: &MemoryNamespace) -> MemoryStorageEpoch;
    /// Lookup failure and an absent record are distinct outcomes.
    fn get(&self, key: &str) -> Result<Option<MemoryRecord<V>>, MemoryStorageError>;
    /// Nonblocking ready lookup; a provider may choose the ordinary path instead.
    fn try_get(&self, _key: &str) -> Result<Option<MemoryRecord<V>>, MemoryStorageError> {
        Ok(None)
    }
    /// Conditional admission, including all physically removed representations.
    fn insert(
        &self,
        candidate: MemoryRecord<V>,
        condition: MemoryCondition<'_, V>,
        now: Timestamp,
    ) -> Result<MemoryStorageWrite<V>, MemoryStorageError>;
    /// Remove only the requested current representation when one is supplied.
    fn remove(
        &self,
        key: &str,
        expected: Option<&Entry<V>>,
    ) -> Result<Option<MemoryRecord<V>>, MemoryStorageError>;
    /// Extract pre-barrier records after the host has invalidated their visibility.
    fn clear_before(
        &self,
        barrier: &MemoryGeneration,
    ) -> Result<Box<[MemoryRecord<V>]>, MemoryStorageError>;
    /// Optional expiry/capacity cleanup, returning owned removed records.
    fn maintain(&self, _now: Timestamp) -> Result<Box<[MemoryRetirement<V>]>, MemoryStorageError> {
        Ok(Box::new([]))
    }
    /// A diagnostic retained-count/weight snapshot.
    fn usage(&self) -> Result<MemoryUsage, MemoryStorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exhausted_epoch_never_reactivates_an_old_generation() {
        let epoch = MemoryStorageEpoch(Arc::new(AtomicU64::new(u64::MAX)));
        let old = epoch.current().unwrap();
        assert!(old.is_current());
        assert!(matches!(
            epoch.advance(),
            Err(MemoryStorageError::GenerationExhausted)
        ));
        assert!(!old.is_current());
        assert!(matches!(
            epoch.current(),
            Err(MemoryStorageError::GenerationExhausted)
        ));
        assert!(matches!(
            epoch.advance(),
            Err(MemoryStorageError::GenerationExhausted)
        ));
        assert!(!old.is_current());
    }
}
