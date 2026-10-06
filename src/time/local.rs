//! One coherent elapsed-time sample for private local-cache lifetimes.
use super::{Clock, ClockTiming, SystemClock, Timestamp};
use std::sync::Arc;
use std::time::Instant;

#[derive(Clone, Copy)]
pub(crate) enum WriteTime {
    Clock(Timestamp),
    Elapsed { now: Timestamp, instant: Instant },
}
impl WriteTime {
    pub(crate) fn now(self) -> Timestamp {
        match self {
            Self::Clock(now) | Self::Elapsed { now, .. } => now,
        }
    }
    pub(crate) fn physical_start(self) -> Instant {
        match self {
            Self::Clock(_) => Instant::now(),
            Self::Elapsed { instant, .. } => instant,
        }
    }
}

pub(crate) enum CacheClock {
    Local(Arc<LocalClock>),
    Shared(Arc<dyn Clock>),
}
impl CacheClock {
    pub(crate) fn local() -> Self {
        Self::Local(Arc::new(LocalClock {
            epoch: SystemClock.now(),
            origin: Instant::now(),
        }))
    }
    pub(crate) fn shared(&self) -> Arc<dyn Clock> {
        match self {
            Self::Local(clock) => Arc::clone(clock) as Arc<dyn Clock>,
            Self::Shared(clock) => Arc::clone(clock),
        }
    }
    pub(crate) fn now(&self) -> Timestamp {
        match self {
            Self::Local(clock) => clock.now(),
            Self::Shared(clock) => clock.now(),
        }
    }
    pub(crate) fn write_time(&self) -> WriteTime {
        match self {
            Self::Local(clock) => {
                let (now, instant) = clock.sample();
                WriteTime::Elapsed { now, instant }
            }
            Self::Shared(clock) => WriteTime::Clock(clock.now()),
        }
    }
    pub(crate) fn timing_model(&self) -> ClockTiming {
        match self {
            Self::Local(clock) => clock.timing_model(),
            Self::Shared(clock) => clock.timing_model(),
        }
    }
}

// This concrete type cannot call an injected/user clock under the reader slot.
pub(crate) struct LocalClock {
    epoch: Timestamp,
    origin: Instant,
}
impl LocalClock {
    pub(crate) fn sample(&self) -> (Timestamp, Instant) {
        let elapsed = Instant::now();
        (
            self.epoch
                .saturating_add(elapsed.duration_since(self.origin)),
            elapsed,
        )
    }
}
impl Clock for LocalClock {
    fn now(&self) -> Timestamp {
        self.sample().0
    }
    fn timing_model(&self) -> ClockTiming {
        ClockTiming::RealTime
    }
}
