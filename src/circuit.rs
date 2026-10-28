//! A circuit breaker for L2 / backplane operations.
//!
//! After a failure, the breaker "opens" for a fixed duration; while open, guarded
//! operations are skipped without being attempted (avoiding hammering a known-bad
//! dependency). A zero-duration breaker is permanently closed — this is
//! FusionCache's default (`DistributedCacheCircuitBreakerDuration = 0`).

use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use crate::time::Timestamp;

const CLOSED: i64 = i64::MIN;

/// Admission result, including an observable automatic close transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitCheck {
    /// Admission was already open to operations.
    Closed,
    /// Operations remain blocked until the timestamp.
    Open {
        /// End of the blocked window.
        until: Timestamp,
    },
    /// This caller performed the cooldown close transition.
    ClosedAfterCooldown,
}

/// A time-based circuit breaker. Cheap, lock-free, shareable.
#[derive(Debug)]
pub struct CircuitBreaker {
    /// How long the breaker stays open after a trip; 0 ⇒ disabled (always closed).
    open_duration: Duration,
    /// The tick at which the breaker re-closes, or [`CLOSED`] when closed.
    reopen_at: AtomicI64,
}

impl CircuitBreaker {
    /// Creates a breaker that opens for `open_duration` after a failure.
    /// `Duration::ZERO` disables it (it stays permanently closed).
    #[must_use]
    pub fn new(open_duration: Duration) -> Self {
        Self {
            open_duration,
            reopen_at: AtomicI64::new(CLOSED),
        }
    }

    /// `true` if this breaker is disabled (zero open-duration).
    #[must_use]
    pub fn is_disabled(&self) -> bool {
        self.open_duration.is_zero()
    }

    /// `true` if the breaker is closed (operations may proceed) at `now`.
    ///
    /// If the open window has elapsed, the breaker auto-closes as a side effect.
    #[must_use]
    pub fn is_closed(&self, now: Timestamp) -> bool {
        !matches!(self.check(now), CircuitCheck::Open { .. })
    }

    /// `true` when admission is certain without a clock sample: the breaker is
    /// disabled or currently closed. An open breaker still needs [`check`](Self::check).
    #[must_use]
    pub fn is_closed_without_clock(&self) -> bool {
        self.is_disabled() || self.reopen_at.load(Ordering::Acquire) == CLOSED
    }

    /// Checks admission and reports the single winning cooldown transition.
    pub fn check(&self, now: Timestamp) -> CircuitCheck {
        if self.is_disabled() {
            return CircuitCheck::Closed;
        }
        loop {
            let reopen = self.reopen_at.load(Ordering::Acquire);
            if reopen == CLOSED {
                return CircuitCheck::Closed;
            }
            if now.ticks() < reopen {
                return CircuitCheck::Open {
                    until: Timestamp::from_ticks(reopen),
                };
            }
            match self.reopen_at.compare_exchange(
                reopen,
                CLOSED,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return CircuitCheck::ClosedAfterCooldown,
                Err(_) => continue,
            }
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
        let reopen = now
            .saturating_add(self.open_duration)
            .ticks()
            .max(now.ticks().saturating_add(1));
        // Delayed failures from an older clock snapshot cannot shorten a newer
        // open window. The transition winner still emits exactly one open event.
        let previous = self.reopen_at.fetch_max(reopen, Ordering::AcqRel);
        previous == CLOSED
    }

    /// Forces the breaker closed (e.g. after a successful operation or a received
    /// backplane message). Returns `true` if it transitioned from open to closed.
    pub fn close(&self) -> bool {
        // Successful operations close an already closed breaker on every call;
        // skip the shared write in that common case.
        if self.reopen_at.load(Ordering::Acquire) == CLOSED {
            return false;
        }
        let previous = self.reopen_at.swap(CLOSED, Ordering::AcqRel);
        previous != CLOSED
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn closed_admission_needs_no_clock_and_close_is_idempotent() {
        let disabled = CircuitBreaker::new(Duration::ZERO);
        assert!(
            disabled.is_closed_without_clock(),
            "contract: closed admission needs no clock and close is idempotent"
        );
        let breaker = CircuitBreaker::new(Duration::from_secs(1));
        assert!(
            breaker.is_closed_without_clock(),
            "contract: closed admission needs no clock and close is idempotent"
        );
        assert!(
            !breaker.close(),
            "contract: closed admission needs no clock and close is idempotent"
        );
        assert!(
            breaker.trip(Timestamp::from_ticks(0)),
            "contract: closed admission needs no clock and close is idempotent"
        );
        assert!(
            !breaker.is_closed_without_clock(),
            "contract: closed admission needs no clock and close is idempotent"
        );
        assert!(
            breaker.close(),
            "contract: closed admission needs no clock and close is idempotent"
        );
        assert!(
            breaker.is_closed_without_clock(),
            "contract: closed admission needs no clock and close is idempotent"
        );
        assert!(
            !breaker.close(),
            "contract: closed admission needs no clock and close is idempotent"
        );
    }

    #[test]
    fn disabled_breaker_is_always_closed() {
        let cb = CircuitBreaker::new(Duration::ZERO);
        assert!(
            cb.is_disabled(),
            "contract: disabled breaker is always closed"
        );
        assert!(
            cb.is_closed(Timestamp::from_ticks(0)),
            "contract: disabled breaker is always closed"
        );
        assert!(
            !cb.trip(Timestamp::from_ticks(0)),
            "contract: disabled breaker is always closed"
        );
        assert!(
            cb.is_closed(Timestamp::from_ticks(1_000_000)),
            "contract: disabled breaker is always closed"
        );
    }

    #[test]
    fn trip_opens_then_auto_closes_after_window() {
        let cb = CircuitBreaker::new(Duration::from_secs(10));
        let t0 = Timestamp::from_ticks(0);
        assert!(
            cb.is_closed(t0),
            "contract: trip opens then auto closes after window"
        );
        assert!(
            cb.trip(t0),
            "contract: trip opens then auto closes after window"
        ); // closed -> open transition
        assert!(
            !cb.trip(t0),
            "contract: trip opens then auto closes after window"
        ); // already open, no transition
        assert!(
            !cb.is_closed(t0.saturating_add(Duration::from_secs(5))),
            "contract: trip opens then auto closes after window"
        );
        // After the window, it auto-closes.
        assert!(
            cb.is_closed(t0.saturating_add(Duration::from_secs(11))),
            "contract: trip opens then auto closes after window"
        );
    }

    #[test]
    fn close_reports_transition() {
        let cb = CircuitBreaker::new(Duration::from_secs(10));
        let t0 = Timestamp::from_ticks(0);
        cb.trip(t0);
        assert!(cb.close(), "contract: close reports transition"); // open -> closed
        assert!(!cb.close(), "contract: close reports transition"); // already closed
    }

    #[test]
    fn concurrent_cooldown_has_one_transition_and_older_failures_do_not_shorten_reopen() {
        let cb = Arc::new(CircuitBreaker::new(Duration::from_secs(1)));
        cb.trip(Timestamp::from_ticks(0));
        let now = Timestamp::from_ticks(20_000_000);
        let handles: Vec<_> = (0..32)
            .map(|_| {
                let cb = Arc::clone(&cb);
                std::thread::spawn(move || cb.check(now))
            })
            .collect();
        assert_eq!(
            handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .filter(|check| *check == CircuitCheck::ClosedAfterCooldown)
                .count(),
            1,
            "contract: concurrent cooldown has one transition and older failures do not shorten reopen"
        );
        assert!(
            cb.trip(now),
            "contract: concurrent cooldown has one transition and older failures do not shorten reopen"
        );
        assert!(
            !cb.trip(Timestamp::from_ticks(0)),
            "contract: concurrent cooldown has one transition and older failures do not shorten reopen"
        );
        assert!(
            matches!(
                cb.check(Timestamp::from_ticks(25_000_000)),
                CircuitCheck::Open { .. }
            ),
            "contract: concurrent cooldown has one transition and older failures do not shorten reopen"
        );
    }

    #[test]
    fn large_duration_saturates_at_upper_clock_bound() {
        let cb = CircuitBreaker::new(Duration::MAX);
        assert!(
            cb.trip(Timestamp::MIN),
            "contract: large duration saturates at upper clock bound"
        );
        assert!(
            matches!(cb.check(Timestamp::from_ticks(0)),CircuitCheck::Open {until} if until==Timestamp::MAX),
            "contract: large duration saturates at upper clock bound"
        );
    }
}
