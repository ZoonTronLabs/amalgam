//! Time abstractions.
//!
//! Cache logic must never read the wall clock directly (it would be untestable
//! and non-deterministic). Instead, every timestamp flows from an injected
//! [`Clock`]. This mirrors FusionCache's use of `DateTimeOffset.UtcNow.UtcTicks`
//! while keeping the domain pure: see the "Time" rule in the project guidelines.
//!
//! Two distinct notions of time exist in a hybrid cache:
//!
//! * **Logical / physical expiration** within a node — handled by comparing
//!   [`Timestamp`]s produced by the same [`Clock`].
//! * **Cross-node ordering** (backplane messages, tag markers, "newer wins") —
//!   also a [`Timestamp`], deliberately a wall-clock value so independent nodes
//!   can compare them.

use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Number of 100-nanosecond ticks in one second.
///
/// A tick is the same unit FusionCache uses (`DateTime.Ticks`), chosen so that
/// sub-microsecond expiration math stays in cheap integer arithmetic.
pub const TICKS_PER_SECOND: i64 = 10_000_000;

const NANOS_PER_TICK: i64 = 100;

/// A point in time, measured in 100-nanosecond ticks since the Unix epoch.
///
/// Unlike a raw `i64`, a `Timestamp` cannot be accidentally mixed with a
/// duration or another integer quantity — it is a value object with explicit,
/// total ordering. It is `Copy` and allocation-free.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(i64);

impl Timestamp {
    /// The earliest representable timestamp.
    pub const MIN: Timestamp = Timestamp(i64::MIN);
    /// The latest representable timestamp.
    pub const MAX: Timestamp = Timestamp(i64::MAX);

    /// Creates a timestamp from raw 100ns ticks since the Unix epoch.
    #[must_use]
    pub const fn from_ticks(ticks: i64) -> Self {
        Self(ticks)
    }

    /// Returns the raw 100ns tick count since the Unix epoch.
    #[must_use]
    pub const fn ticks(self) -> i64 {
        self.0
    }

    /// Adds a [`Duration`], saturating at [`Timestamp::MAX`] instead of
    /// overflowing. Used to derive expiration points from a "now".
    #[must_use]
    pub fn saturating_add(self, duration: Duration) -> Self {
        Self(self.0.saturating_add(duration_to_ticks(duration)))
    }

    /// Returns the duration elapsed from `earlier` to `self`, or
    /// [`Duration::ZERO`] if `self` is not after `earlier`.
    #[must_use]
    pub fn saturating_duration_since(self, earlier: Timestamp) -> Duration {
        let delta = self.0.saturating_sub(earlier.0);
        if delta <= 0 {
            Duration::ZERO
        } else {
            ticks_to_duration(delta)
        }
    }

    /// `true` if `self` is strictly before `other`.
    #[must_use]
    pub fn is_before(self, other: Timestamp) -> bool {
        self < other
    }
}

/// Converts a [`Duration`] to 100ns ticks, saturating on overflow.
#[must_use]
pub fn duration_to_ticks(duration: Duration) -> i64 {
    let nanos = duration.as_nanos();
    let ticks = nanos / (NANOS_PER_TICK as u128);
    i64::try_from(ticks).unwrap_or(i64::MAX)
}

/// Converts a non-negative tick count to a [`Duration`].
#[must_use]
pub fn ticks_to_duration(ticks: i64) -> Duration {
    let nanos = (ticks.max(0) as u64).saturating_mul(NANOS_PER_TICK as u64);
    Duration::from_nanos(nanos)
}

/// Source of the current time.
///
/// Inject a custom implementation in tests to make every expiration, throttle
/// and timeout window deterministic. Production code uses [`SystemClock`].
pub trait Clock: Send + Sync {
    /// Returns the current wall-clock instant as a [`Timestamp`].
    fn now(&self) -> Timestamp;
}

impl<T: Clock + ?Sized> Clock for std::sync::Arc<T> {
    fn now(&self) -> Timestamp {
        (**self).now()
    }
}

/// The real system clock, backed by [`SystemTime`].
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Timestamp {
        let since_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO);
        Timestamp(duration_to_ticks(since_epoch))
    }
}

/// A manually-controlled clock for tests.
///
/// Start at an arbitrary epoch and [`advance`](ManualClock::advance) it to drive
/// expiration, throttling and timeout behaviour deterministically.
#[derive(Debug)]
pub struct ManualClock {
    ticks: AtomicI64,
}

impl ManualClock {
    /// Creates a clock starting at the given number of ticks since the epoch.
    #[must_use]
    pub fn new(start: Timestamp) -> Self {
        Self {
            ticks: AtomicI64::new(start.0),
        }
    }

    /// Moves the clock forward by `duration`.
    pub fn advance(&self, duration: Duration) {
        self.ticks
            .fetch_add(duration_to_ticks(duration), Ordering::SeqCst);
    }

    /// Sets the clock to an absolute timestamp.
    pub fn set(&self, at: Timestamp) {
        self.ticks.store(at.0, Ordering::SeqCst);
    }
}

impl Default for ManualClock {
    fn default() -> Self {
        // An arbitrary, comfortably-positive starting point (~2001-09-09).
        Self::new(Timestamp(1_000_000_000 * TICKS_PER_SECOND))
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Timestamp {
        Timestamp(self.ticks.load(Ordering::SeqCst))
    }
}

/// A timeout that is either unbounded or a finite [`Duration`].
///
/// FusionCache encodes "no timeout" as `Timeout.InfiniteTimeSpan` (a `-1ms`
/// sentinel). Modelling that as a negative `Duration` is a footgun; an explicit
/// two-variant enum makes "infinite" a first-class, unmistakable state and keeps
/// every illegal negative-duration combination unrepresentable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Timeout {
    /// Wait forever / never time out.
    #[default]
    Infinite,
    /// Time out after the given finite duration.
    After(Duration),
}

impl Timeout {
    /// Builds a timeout from an optional duration (`None` ⇒ [`Timeout::Infinite`]).
    #[must_use]
    pub fn from_option(duration: Option<Duration>) -> Self {
        match duration {
            Some(d) => Timeout::After(d),
            None => Timeout::Infinite,
        }
    }

    /// `true` if this timeout never fires.
    #[must_use]
    pub fn is_infinite(self) -> bool {
        matches!(self, Timeout::Infinite)
    }

    /// `true` if this is a finite, zero-length timeout (fire immediately).
    #[must_use]
    pub fn is_immediate(self) -> bool {
        matches!(self, Timeout::After(d) if d.is_zero())
    }

    /// The finite duration, or `None` when infinite.
    #[must_use]
    pub fn as_duration(self) -> Option<Duration> {
        match self {
            Timeout::Infinite => None,
            Timeout::After(d) => Some(d),
        }
    }

    /// Returns the shorter of two timeouts (infinite is treated as longest).
    #[must_use]
    pub fn min(self, other: Timeout) -> Timeout {
        match (self, other) {
            (Timeout::Infinite, o) => o,
            (s, Timeout::Infinite) => s,
            (Timeout::After(a), Timeout::After(b)) => Timeout::After(a.min(b)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duration_round_trips_through_ticks() {
        let d = Duration::from_millis(1500);
        assert_eq!(ticks_to_duration(duration_to_ticks(d)), d);
    }

    #[test]
    fn saturating_add_then_since_recovers_duration() {
        let now = Timestamp::from_ticks(5 * TICKS_PER_SECOND);
        let later = now.saturating_add(Duration::from_secs(3));
        assert_eq!(later.saturating_duration_since(now), Duration::from_secs(3));
        assert_eq!(now.saturating_duration_since(later), Duration::ZERO);
    }

    #[test]
    fn manual_clock_advances() {
        let clock = ManualClock::new(Timestamp::from_ticks(0));
        assert_eq!(clock.now(), Timestamp::from_ticks(0));
        clock.advance(Duration::from_secs(2));
        assert_eq!(clock.now(), Timestamp::from_ticks(2 * TICKS_PER_SECOND));
    }

    #[test]
    fn timeout_min_treats_infinite_as_longest() {
        assert_eq!(
            Timeout::Infinite.min(Timeout::After(Duration::from_secs(1))),
            Timeout::After(Duration::from_secs(1))
        );
        assert_eq!(
            Timeout::After(Duration::from_secs(2)).min(Timeout::After(Duration::from_secs(1))),
            Timeout::After(Duration::from_secs(1))
        );
        assert_eq!(Timeout::Infinite.min(Timeout::Infinite), Timeout::Infinite);
    }
}
