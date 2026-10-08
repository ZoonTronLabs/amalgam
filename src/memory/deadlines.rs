//! Stored local deadlines avoid projecting elapsed time into UTC on each hit.
use crate::entry::{Freshness, Metadata};
use crate::time::local::{LocalClock, MonotonicDeadline, WriteTime};
use crate::{Timestamp, provider::MemoryExpiry};
use std::sync::Arc;
use std::time::Instant;

pub(super) enum Timing {
    Interoperable,
    Local(Arc<LocalClock>),
}
impl Timing {
    #[inline]
    pub(super) fn prepare(
        &self,
        meta: &Metadata,
        time: WriteTime,
        expiry: MemoryExpiry,
    ) -> Deadlines {
        self.prepare_boundaries(
            meta.logical_expiration(),
            meta.physical_expiration(),
            time,
            expiry,
        )
    }
    #[inline]
    pub(super) fn prepare_boundaries(
        &self,
        logical: Timestamp,
        physical: Timestamp,
        time: WriteTime,
        expiry: MemoryExpiry,
    ) -> Deadlines {
        match self {
            Self::Interoperable => Deadlines::Interoperable(match expiry {
                MemoryExpiry::ClockDriven => None,
                MemoryExpiry::RealTime => time
                    .physical_start()
                    .checked_add(physical.saturating_duration_since(time.now())),
            }),
            Self::Local(clock) => {
                let logical_deadline = clock.deadline(logical);
                Deadlines::Local {
                    logical: logical_deadline,
                    physical: if physical == logical {
                        logical_deadline
                    } else {
                        clock.deadline(physical)
                    },
                }
            }
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum Deadlines {
    Interoperable(Option<Instant>),
    Local {
        logical: MonotonicDeadline,
        physical: MonotonicDeadline,
    },
}
impl Deadlines {
    #[inline]
    pub(super) fn physically_expired(
        &self,
        meta: &Metadata,
        now: Option<Timestamp>,
        elapsed: Option<Instant>,
    ) -> bool {
        match self {
            Self::Interoperable(physical) => {
                now.is_some_and(|now| now >= meta.physical_expiration())
                    || physical
                        .is_some_and(|deadline| elapsed.unwrap_or_else(Instant::now) >= deadline)
            }
            Self::Local { physical, .. } => physical.expired(elapsed.unwrap_or_else(Instant::now)),
        }
    }
    #[inline]
    pub(super) fn freshness(&self, now: Instant) -> Freshness {
        match self {
            Self::Local { logical, .. } => {
                if logical.expired(now) {
                    Freshness::Stale
                } else {
                    Freshness::Fresh
                }
            }
            Self::Interoperable(_) => {
                unreachable!("elapsed reads require the build-selected local map")
            }
        }
    }
}
