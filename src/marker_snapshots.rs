//! Optional expiring control snapshots, separate from durable invalidation facts.
#![doc = include_str!("../docs/MARKER_SNAPSHOTS.md")]

use crate::distributed_lock::{LeaseError, LeaseProof};
use crate::error::{Error, FactoryCancellationReason};
use crate::execution::FactoryCancellation;
use crate::options::EntryOptions;
use crate::tags::{CacheScope, MarkerError, MarkerKind, MarkerVersion};
use crate::time::Timestamp;
use async_trait::async_trait;
use std::time::Duration;

/// Lifetime model for options-controlled secondary marker checks.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum MarkerLifecyclePolicy {
    /// Read durable maxima directly, preserving existing providers and behavior.
    #[default]
    DurableOnly,
    /// Read expiring control snapshots and repair misses from durable facts.
    /// Requires an options-controlled read policy and a snapshot capability.
    CachedSnapshots,
}

/// Invalid construction of a control snapshot or its resource bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum MarkerSnapshotValidationError {
    /// A snapshot must satisfy created <= logical <= physical expiration.
    #[error("marker snapshot deadlines must satisfy created <= logical <= physical")]
    InvalidDeadlines,
    /// A snapshot cache must admit at least one record.
    #[error("marker snapshot capacity must be positive")]
    ZeroCapacity,
}

/// A validated, physically retained observation of an actual invalidation revision.
///
/// An absent marker is not a snapshot. Expiring this record never expires the
/// provider's durable maximum. No serializer can bypass its validating factory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarkerSnapshot {
    version: MarkerVersion,
    created: Timestamp,
    logical: Timestamp,
    physical: Timestamp,
}

impl MarkerSnapshot {
    /// Validates absolute deadlines supplied by a provider or injected clock.
    pub fn new(
        version: MarkerVersion,
        created: Timestamp,
        logical: Timestamp,
        physical: Timestamp,
    ) -> Result<Self, MarkerSnapshotValidationError> {
        if created > logical || logical > physical {
            return Err(MarkerSnapshotValidationError::InvalidDeadlines);
        }
        Ok(Self {
            version,
            created,
            logical,
            physical,
        })
    }

    /// Creates independent distributed deadlines without memory jitter.
    #[must_use]
    pub fn fresh(version: MarkerVersion, options: &EntryOptions, now: Timestamp) -> Self {
        Self {
            version,
            created: now,
            logical: now.saturating_add(options.resolved_distributed_duration()),
            physical: now.saturating_add(options.distributed_physical_ttl()),
        }
    }

    /// The actual invalidation fact carried by this snapshot.
    #[must_use]
    pub const fn version(self) -> MarkerVersion {
        self.version
    }
    /// The insertion boundary; renewal does not change the invalidation revision.
    #[must_use]
    pub const fn created(self) -> Timestamp {
        self.created
    }
    /// The distributed logical expiration.
    #[must_use]
    pub const fn logical_expiration(self) -> Timestamp {
        self.logical
    }
    /// The distributed physical retention boundary.
    #[must_use]
    pub const fn physical_expiration(self) -> Timestamp {
        self.physical
    }
    /// Whether the snapshot is fresh at the supplied time.
    #[must_use]
    pub fn is_fresh(self, now: Timestamp) -> bool {
        now < self.logical && now < self.physical
    }
    /// Whether the snapshot can no longer be retained.
    #[must_use]
    pub fn is_physically_expired(self, now: Timestamp) -> bool {
        now >= self.physical
    }
    /// Remaining backend TTL at the actual write boundary.
    #[must_use]
    pub fn remaining_ttl(self, now: Timestamp) -> Duration {
        self.physical.saturating_duration_since(now)
    }
    pub(crate) fn with_maximum(self, maximum: Option<MarkerVersion>) -> Self {
        Self {
            version: maximum.map_or(self.version, |v| v.max(self.version)),
            ..self
        }
    }
    pub(crate) fn supersedes(self, other: Self) -> bool {
        (self.version, self.created) > (other.version, other.created)
    }
}

/// Positive bound for expendable snapshots, independent from durable scopes/tags.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarkerSnapshotLimits {
    max_entries: usize,
}
impl MarkerSnapshotLimits {
    /// Validates an entry-count bound.
    pub fn new(max_entries: usize) -> Result<Self, MarkerSnapshotValidationError> {
        if max_entries == 0 {
            Err(MarkerSnapshotValidationError::ZeroCapacity)
        } else {
            Ok(Self { max_entries })
        }
    }
    /// Maximum snapshots retained by the in-memory provider.
    #[must_use]
    pub const fn max_entries(self) -> usize {
        self.max_entries
    }
}
impl Default for MarkerSnapshotLimits {
    fn default() -> Self {
        Self { max_entries: 4096 }
    }
}

/// Complete result of renewing an expendable control snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarkerSnapshotRenewal {
    /// The requested lifetime was committed with the strongest known revision.
    Stored(MarkerSnapshot),
    /// A concurrent higher revision or later insertion was preserved unchanged.
    KeptNewer(MarkerSnapshot),
    /// The proposed physical deadline had already elapsed; nothing was written.
    Expired,
}

/// An atomic observation read, including the durable fact on a TTL-cache miss.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarkerSnapshotRead {
    /// A physically live snapshot, reconciled with the durable journal.
    Snapshot(MarkerSnapshot),
    /// The expendable record is missing/expired; the journal was read atomically.
    Missing {
        /// An actual durable revision, or confirmed durable absence.
        maximum: Option<MarkerVersion>,
    },
}
impl MarkerSnapshotRead {
    /// Optional physically live snapshot; a miss still carries its journal fact.
    #[must_use]
    pub const fn snapshot(self) -> Option<MarkerSnapshot> {
        match self {
            Self::Snapshot(snapshot) => Some(snapshot),
            Self::Missing { .. } => None,
        }
    }
    /// The strongest fact confirmed by this atomic provider operation.
    #[must_use]
    pub const fn maximum(self) -> Option<MarkerVersion> {
        match self {
            Self::Snapshot(snapshot) => Some(snapshot.version()),
            Self::Missing { maximum } => maximum,
        }
    }
}

/// Bounded diagnostic outcome of an actual snapshot renewal attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarkerSnapshotWriteOutcome {
    /// The snapshot was committed.
    Stored,
    /// Concurrent newer snapshot admission won.
    KeptNewer,
    /// The proposed physical lifetime had ended.
    Expired,
    /// The provider failed with its original backend cause.
    BackendFailure,
    /// The provider rejected its control protocol.
    ProtocolFailure,
}

/// Closed failure family of a cooperative snapshot read or renewal.
#[derive(Debug, thiserror::Error)]
pub enum MarkerSnapshotCacheError {
    /// Original storage/protocol failure.
    #[error(transparent)]
    Provider(#[from] MarkerError),
    /// Strict ownership or native fencing failed.
    #[error(transparent)]
    Lease(#[from] LeaseError),
    /// The operation's actual owning phase ended.
    #[error("marker snapshot operation cancelled: {}", reason.as_str())]
    Cancelled {
        /// Exact owning-scope cancellation reason.
        reason: FactoryCancellationReason,
    },
}
impl MarkerSnapshotCacheError {
    /// Checks cancellation without disguising it as a provider failure.
    pub fn check_cancellation(cancellation: &FactoryCancellation) -> Result<(), Self> {
        match cancellation.reason() {
            Some(reason) => Err(Self::Cancelled { reason }),
            None => Ok(()),
        }
    }
}
impl From<MarkerSnapshotCacheError> for Error {
    fn from(error: MarkerSnapshotCacheError) -> Self {
        match error {
            MarkerSnapshotCacheError::Provider(error) => Self::Marker(error),
            MarkerSnapshotCacheError::Lease(error) => Self::Lease(error),
            MarkerSnapshotCacheError::Cancelled { reason } => Self::OperationCancelled { reason },
        }
    }
}

/// Open provider capability for expiring control records in a separate namespace.
///
/// Reads/renewals must atomically reconcile the durable maximum and any current
/// snapshot. Renewal never advances/compacts the invalidation journal or emits
/// a backplane command. Lower revisions and older same-revision insertions must
/// not replace newer snapshots. Physical expiry removes only the snapshot.
#[allow(
    clippy::double_must_use,
    reason = "async-trait emits must_use on boxed futures"
)]
#[async_trait]
pub trait MarkerSnapshotCache: Send + Sync {
    /// Returns a physically live snapshot, merging any concurrent durable fact.
    async fn read_snapshot(
        &self,
        scope: &CacheScope,
        kind: &MarkerKind,
        now: Timestamp,
        cancellation: FactoryCancellation,
    ) -> Result<MarkerSnapshotRead, MarkerSnapshotCacheError>;
    /// Renews only the expendable snapshot, using its remaining physical TTL.
    async fn renew_snapshot(
        &self,
        scope: &CacheScope,
        kind: &MarkerKind,
        snapshot: MarkerSnapshot,
        now: Timestamp,
        cancellation: FactoryCancellation,
    ) -> Result<MarkerSnapshotRenewal, MarkerSnapshotCacheError>;
    /// Optional native atomic ownership check plus snapshot renewal.
    async fn renew_snapshot_with_lease(
        &self,
        _scope: &CacheScope,
        _kind: &MarkerKind,
        _snapshot: MarkerSnapshot,
        _now: Timestamp,
        _proof: &LeaseProof,
        _cancellation: FactoryCancellation,
    ) -> Result<MarkerSnapshotRenewal, MarkerSnapshotCacheError> {
        Err(LeaseError::UnsupportedFencing.into())
    }
}
