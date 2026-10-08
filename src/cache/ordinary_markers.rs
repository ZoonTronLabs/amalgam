//! Ordinary byte markers are available without claiming atomic maxima.
use super::{Arc, DistributedCache, FactoryCancellation, InvalidationStore};
use crate::distributed::MarkerReadError;
use crate::marker_snapshots::MarkerSnapshot;
use crate::tags::{
    CacheScope, MarkerAdvanceOutcome, MarkerError, MarkerKind, MarkerVersion, StoredMarker,
};
use crate::time::Timestamp;

pub(super) struct OrdinaryMarkers {
    backend: Arc<dyn DistributedCache>,
}
impl OrdinaryMarkers {
    pub(super) fn new(backend: Arc<dyn DistributedCache>) -> Self {
        Self { backend }
    }
    // Category and length-delimited scope/tag bytes prevent marker collisions.
    // The prefix is reserved for controls when value-key modification is None.
    fn key(scope: &CacheScope, kind: &MarkerKind) -> String {
        let scope = scope.storage_id();
        match kind {
            MarkerKind::ClearRemove => format!("amalgam:byte-marker:{scope}:remove"),
            MarkerKind::ClearExpire => format!("amalgam:byte-marker:{scope}:expire"),
            MarkerKind::Tag(tag) => format!(
                "amalgam:byte-marker:{scope}:tag:{}:{}",
                tag.as_str().len(),
                tag.as_str()
            ),
        }
    }
    async fn read(
        &self,
        scope: &CacheScope,
        kind: &MarkerKind,
    ) -> Result<Option<MarkerVersion>, MarkerError> {
        self.backend
            .get(&Self::key(scope, kind))
            .await
            .map_err(MarkerError::backend)?
            .map(|bytes| {
                let encoded = std::str::from_utf8(&bytes).map_err(MarkerError::protocol)?;
                MarkerVersion::from_ordered_hex(encoded)
            })
            .transpose()
    }
    pub(super) async fn write(
        &self,
        scope: &CacheScope,
        kind: MarkerKind,
        snapshot: MarkerSnapshot,
        now: Timestamp,
    ) -> Result<StoredMarker, MarkerError> {
        let version = snapshot.version();
        let ttl = snapshot.remaining_ttl(now);
        self.backend
            .set(
                &Self::key(scope, &kind),
                version.ordered_hex().into_bytes(),
                Some(ttl),
            )
            .await
            .map_err(MarkerError::backend)?;
        Ok(StoredMarker::new(kind, version))
    }
}

/// A read needs presence and an exact version, not an atomic mutation capability.
#[derive(Clone)]
pub(super) enum MarkerReadStorage {
    Atomic(Arc<dyn InvalidationStore>),
    Ordinary(Arc<OrdinaryMarkers>),
}
impl MarkerReadStorage {
    pub(super) async fn read_with_cancellation(
        &self,
        scope: &CacheScope,
        kind: &MarkerKind,
        cancellation: FactoryCancellation,
    ) -> Result<Option<MarkerVersion>, MarkerReadError> {
        match self {
            Self::Atomic(store) => {
                store
                    .read_with_cancellation(scope, kind, cancellation)
                    .await
            }
            Self::Ordinary(store) => {
                MarkerReadError::check_cancellation(&cancellation)?;
                let result = store.read(scope, kind).await;
                MarkerReadError::check_cancellation(&cancellation)?;
                Ok(result?)
            }
        }
    }
}

/// Acknowledged write facts shared by ordinary and genuinely atomic storage.
pub(super) enum MarkerWriteOutcome {
    Written(StoredMarker),
    Compacted {
        marker: StoredMarker,
        clear_remove: MarkerVersion,
    },
}
impl MarkerWriteOutcome {
    pub(super) fn marker(&self) -> &StoredMarker {
        match self {
            Self::Written(marker) | Self::Compacted { marker, .. } => marker,
        }
    }
}
impl From<MarkerAdvanceOutcome> for MarkerWriteOutcome {
    fn from(outcome: MarkerAdvanceOutcome) -> Self {
        match outcome {
            MarkerAdvanceOutcome::Advanced(marker) => Self::Written(marker),
            MarkerAdvanceOutcome::Compacted {
                marker,
                clear_remove,
            } => Self::Compacted {
                marker,
                clear_remove,
            },
        }
    }
}
