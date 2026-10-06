//! Build-selected clock domains keep interoperable timestamps on UTC.
use super::{Arc, Clock, ClockTiming, MemoryExpiry, SystemClock};
use crate::time::local::CacheClock;

pub(super) enum ClockDomain {
    Local,
    Interoperable,
}
pub(super) fn select(supplied: Option<Arc<dyn Clock>>, domain: ClockDomain) -> CacheClock {
    match (supplied, domain) {
        (Some(clock), _) => CacheClock::Shared(clock),
        (None, ClockDomain::Local) => CacheClock::local(),
        (None, ClockDomain::Interoperable) => CacheClock::Shared(Arc::new(SystemClock)),
    }
}
pub(super) fn expiry(clock: &CacheClock) -> MemoryExpiry {
    match clock.timing_model() {
        ClockTiming::RealTime => MemoryExpiry::RealTime,
        ClockTiming::Controlled => MemoryExpiry::ClockDriven,
    }
}
