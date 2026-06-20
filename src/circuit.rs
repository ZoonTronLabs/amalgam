//! A circuit breaker for L2 / backplane operations.
//!
//! After a failure, the breaker "opens" for a fixed duration; while open, guarded
//! operations are skipped without being attempted (avoiding hammering a known-bad
//! dependency). A zero-duration breaker is permanently closed — this is
//! FusionCache's default (`DistributedCacheCircuitBreakerDuration = 0`).

use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use crate::time::{Timestamp, duration_to_ticks};

const CLOSED: i64 = i64::MIN;

/// A time-based circuit breaker. Cheap, lock-free, shareable.
#[derive(Debug)]
pub struct CircuitBreaker {
    /// How long the breaker stays open after a trip; 0 ⇒ disabled (always closed).
    open_duration_ticks: i64,
    /// The tick at which the breaker re-closes, or [`CLOSED`] when closed.
    reopen_at: AtomicI64,
}

impl CircuitBreaker {
    /// Creates a breaker that opens for `open_duration` after a failure.
    /// `Duration::ZERO` disables it (it stays permanently closed).
    #[must_use]
    pub fn new(open_duration: Duration) -> Self {
        Self {
            open_duration_ticks: duration_to_ticks(open_duration),
            reopen_at: AtomicI64::new(CLOSED),
        }
    }

    /// `true` if this breaker is disabled (zero open-duration).
    #[must_use]
    pub fn is_disabled(&self) -> bool {
        self.open_duration_ticks == 0
    }

    /// `true` if the breaker is closed (operations may proceed) at `now`.
    ///
    /// If the open window has elapsed, the breaker auto-closes as a side effect.
    #[must_use]
    pub fn is_closed(&self, now: Timestamp) -> bool {
        if self.is_disabled() {
            return true;
        }
        let reopen = self.reopen_at.load(Ordering::Acquire);
        if reopen == CLOSED {
            return true;
        }
        if now.ticks() >= reopen {
            // Window elapsed: auto-close.
            let _ = self.reopen_at.compare_exchange(
                reopen,
                CLOSED,
                Ordering::AcqRel,
                Ordering::Relaxed,
            );
            true
        } else {
            false
        }
    }

    /// Trips the breaker open until `now + open_duration`.
    ///
    /// Returns `true` if this call transitioned the breaker from closed to open
    /// (so the caller can fire a `CircuitBreakerChange` event exactly once).
    pub fn trip(&self, now: Timestamp) -> bool {
        if self.is_disabled() {
            return false;
        }
        let reopen = now.ticks().saturating_add(self.open_duration_ticks);
        let previous = self.reopen_at.swap(reopen, Ordering::AcqRel);
        previous == CLOSED
    }

    /// Forces the breaker closed (e.g. after a successful operation or a received
    /// backplane message). Returns `true` if it transitioned from open to closed.
    pub fn close(&self) -> bool {
        let previous = self.reopen_at.swap(CLOSED, Ordering::AcqRel);
        previous != CLOSED
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_breaker_is_always_closed() {
        let cb = CircuitBreaker::new(Duration::ZERO);
        assert!(cb.is_disabled());
        assert!(cb.is_closed(Timestamp::from_ticks(0)));
        assert!(!cb.trip(Timestamp::from_ticks(0)));
        assert!(cb.is_closed(Timestamp::from_ticks(1_000_000)));
    }

    #[test]
    fn trip_opens_then_auto_closes_after_window() {
        let cb = CircuitBreaker::new(Duration::from_secs(10));
        let t0 = Timestamp::from_ticks(0);
        assert!(cb.is_closed(t0));
        assert!(cb.trip(t0)); // closed -> open transition
        assert!(!cb.trip(t0)); // already open, no transition
        assert!(!cb.is_closed(t0.saturating_add(Duration::from_secs(5))));
        // After the window, it auto-closes.
        assert!(cb.is_closed(t0.saturating_add(Duration::from_secs(11))));
    }

    #[test]
    fn close_reports_transition() {
        let cb = CircuitBreaker::new(Duration::from_secs(10));
        let t0 = Timestamp::from_ticks(0);
        cb.trip(t0);
        assert!(cb.close()); // open -> closed
        assert!(!cb.close()); // already closed
    }
}
