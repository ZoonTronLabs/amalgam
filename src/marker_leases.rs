//! Closed ownership states for a secondary marker refresh.

use crate::backplane::encode_hex;
use crate::distributed_lock::{DistributedLease, LeaseError, LeaseProof, LeaseState};
use crate::marker_reads::MarkerObservations;
use crate::tags::{CacheScope, MarkerKind};
use std::sync::Arc;

/// A control lease identity cannot alias an ordinary value's lock identity.
pub(crate) struct MarkerLeaseKey(Arc<str>);

impl MarkerLeaseKey {
    pub(crate) fn new(scope: &CacheScope, kind: &MarkerKind) -> Self {
        Self(Arc::from(format!(
            "\x1famalgam/v2/marker-locks/{}/{}",
            encode_hex(scope.storage_id().as_bytes()),
            encode_hex(MarkerObservations::key(kind).as_bytes()),
        )))
    }

    pub(crate) fn into_arc(self) -> Arc<str> {
        self.0
    }
}

pub(crate) enum MarkerLease {
    Unleased,
    Fenced(DistributedLease),
    Cooperative(DistributedLease),
}

impl MarkerLease {
    pub(crate) fn is_held(&self) -> bool {
        matches!(self, Self::Fenced(_) | Self::Cooperative(_))
    }

    pub(crate) fn proof(&self) -> Result<Option<LeaseProof>, LeaseError> {
        match self {
            Self::Unleased => Ok(None),
            Self::Fenced(lease) => lease.proof().map(Some),
            Self::Cooperative(lease) => match *lease.state().borrow() {
                LeaseState::Held => Ok(None),
                LeaseState::Lost | LeaseState::Released => Err(LeaseError::Lost),
            },
        }
    }

    pub(crate) async fn release(self) -> Result<(), LeaseError> {
        match self {
            Self::Unleased => Ok(()),
            Self::Fenced(lease) | Self::Cooperative(lease) => lease.release().await,
        }
    }
}
