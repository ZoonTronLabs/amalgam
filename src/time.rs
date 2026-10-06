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
//!
//! Standalone caches with built-in local storage use a UTC anchor plus elapsed
//! monotonic time for duration lifetimes. One private sample under the reader
//! slot supplies both freshness and physical expiry on the plain ready path.
//! Hybrid caches, supplied storage/markers, distributed lockers/backplanes and
//! explicitly supplied clocks preserve their
//! interoperable clock domain and elapsed backend deadlines. [`SystemClock`]
//! always returns live system UTC when explicitly selected.

pub(crate) mod local;

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
        let result = i128::from(self.0) + duration_ticks_wide(duration);
        Self(i64::try_from(result).unwrap_or(i64::MAX))
    }

    /// Returns the duration elapsed from `earlier` to `self`, or
    /// [`Duration::ZERO`] if `self` is not after `earlier`.
    #[must_use]
    pub fn saturating_duration_since(self, earlier: Timestamp) -> Duration {
        if self <= earlier {
            Duration::ZERO
        } else {
            unsigned_ticks_to_duration(self.0.abs_diff(earlier.0))
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
    i64::try_from(duration_ticks_wide(duration)).unwrap_or(i64::MAX)
}

/// Converts a non-negative tick count to a [`Duration`].
#[must_use]
pub fn ticks_to_duration(ticks: i64) -> Duration {
    unsigned_ticks_to_duration(ticks.max(0) as u64)
}

/// Validated nonnegative lifetime prepared once from a Duration.
#[derive(Clone, Copy)]
pub(crate) struct LifetimeSpan(i128);
impl LifetimeSpan {
    pub(crate) fn new(duration: Duration) -> Self {
        Self(duration_ticks_wide(duration))
    }
    pub(crate) fn after(self, timestamp: Timestamp) -> Timestamp {
        Timestamp(i64::try_from(i128::from(timestamp.0) + self.0).unwrap_or(i64::MAX))
    }
}

fn duration_ticks_wide(duration: Duration) -> i128 {
    // Duration's full u64-second range fits in i128 at 100ns precision.
    (duration.as_nanos() / NANOS_PER_TICK as u128) as i128
}

fn unsigned_ticks_to_duration(ticks: u64) -> Duration {
    let seconds = ticks / TICKS_PER_SECOND as u64;
    let nanos = (ticks % TICKS_PER_SECOND as u64) * NANOS_PER_TICK as u64;
    Duration::new(seconds, nanos as u32)
}

/// Whether a clock follows elapsed real time or is externally controlled.
///
/// Expiry cleanup must respect the injected clock. Runtime I/O timeouts still
/// use monotonic runtime time independently of this capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ClockTiming {
    /// Real elapsed time can drive physical-expiry maintenance.
    RealTime,
    /// Only explicit clock readings/advances can expire entries.
    #[default]
    Controlled,
}

/// Source of the current time.
///
/// Inject a custom implementation in tests to make every expiration, throttle
/// and timeout window deterministic. Production code uses [`SystemClock`].
pub trait Clock: Send + Sync {
    /// Returns a timestamp in this clock's domain. Hybrid caches require
    /// UTC-comparable timestamps; isolated duration lifetimes can use a
    /// UTC-anchored elapsed scale.
    fn now(&self) -> Timestamp;

    /// Declares physical-expiry timing. Custom clocks are controlled by default
    /// so a frozen clock cannot lose entries to an unrelated real timer.
    fn timing_model(&self) -> ClockTiming {
        ClockTiming::Controlled
    }
}

impl<T: Clock + ?Sized> Clock for std::sync::Arc<T> {
    fn now(&self) -> Timestamp {
        (**self).now()
    }

    fn timing_model(&self) -> ClockTiming {
        (**self).timing_model()
    }
}

/// The real system clock, backed by [`SystemTime`].
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Timestamp {
        timestamp_from_system_time(SystemTime::now())
    }

    fn timing_model(&self) -> ClockTiming {
        ClockTiming::RealTime
    }
}

fn timestamp_from_system_time(at: SystemTime) -> Timestamp {
    match at.duration_since(UNIX_EPOCH) {
        Ok(elapsed) => Timestamp(duration_to_ticks(elapsed)),
        Err(before_epoch) => {
            // Floor a point in time at tick precision, including negative times.
            let ticks = before_epoch
                .duration()
                .as_nanos()
                .div_ceil(NANOS_PER_TICK as u128);
            Timestamp(i64::try_from(-(ticks as i128)).unwrap_or(i64::MIN))
        }
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

    /// Moves the clock forward by `duration`, saturating at [`Timestamp::MAX`].
    pub fn advance(&self, duration: Duration) {
        // The closure always returns Some, so fetch_update cannot reject this
        // update. Its retry loop preserves concurrent advances without wrapping.
        #[allow(
            deprecated,
            reason = "Atomic::try_update is unavailable on the supported Rust 1.88"
        )]
        let _ = self
            .ticks
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |ticks| {
                Some(Timestamp(ticks).saturating_add(duration).ticks())
            });
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

    #[test]
    fn system_time_before_epoch_is_a_negative_timestamp() {
        assert_eq!(
            timestamp_from_system_time(UNIX_EPOCH),
            Timestamp::from_ticks(0)
        );
        assert_eq!(
            timestamp_from_system_time(UNIX_EPOCH - Duration::from_secs(1)),
            Timestamp::from_ticks(-TICKS_PER_SECOND)
        );
        assert_eq!(
            timestamp_from_system_time(UNIX_EPOCH - Duration::from_nanos(100)),
            Timestamp::from_ticks(-1)
        );
        // Windows SystemTime stores 100ns ticks and truncates a 1ns subtraction.
        #[cfg(not(windows))]
        assert_eq!(
            timestamp_from_system_time(UNIX_EPOCH - Duration::from_nanos(1)),
            Timestamp::from_ticks(-1)
        );
    }
}
