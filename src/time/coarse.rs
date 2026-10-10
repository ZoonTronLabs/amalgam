//! A cheap upper bound of the monotonic clock for local hit decisions.
//!
//! On Linux `CLOCK_MONOTONIC_COARSE` is `CLOCK_MONOTONIC`, the clock behind
//! [`Instant`], as of the last timekeeping update: never ahead of it and
//! normally behind by up to about two ticks. On the measured x86 CI VM it costs
//! 5.6 ns against 29 ns for the precise clock. An anchor pairs a coarse reading
//! with a later `Instant`, so `anchor + (coarse now - coarse anchor) + slack`
//! is no earlier than the precise clock now while the coarse clock lags by less
//! than the slack. A hit uses that bound only to prove that a deadline is still
//! ahead; every other decision reads the precise clock.
//!
//! That lag is an assumption, not a kernel guarantee. The timekeeping update can
//! stall: kernels 5.18+ (and 5.15.127+) let another CPU take over after five of
//! its own ticks, older kernels do not, and after a whole-VM pause or an SMI a
//! reader can run before the first overdue tick catches up. The slack is
//! therefore eight ticks but at least one second, far above ordinary stalls; a
//! hit within it of a deadline reads the precise clock. A stall longer than the
//! slack can let a hit near its deadline count as fresh for up to the excess,
//! until the next timekeeping update. Kernels with `nohz_full` CPUs, where a
//! busy CPU can stop ticking, a reported tick outside 0.5-10 ms, other platforms
//! and Miri use the precise clock throughout. Without a vDSO the coarse read is
//! a system call: slower than the precise clock, still correct.
//!
//! The anchor is probed when a cache creates its local clock, outside any
//! reader slot; a hit only reads it.
use std::time::Instant;

/// Probes the coarse clock once, so hits never initialize it under a guard.
#[cfg(all(target_os = "linux", not(miri)))]
pub(crate) fn prepare() {
    linux::prepare();
}

/// Probes the coarse clock once, so hits never initialize it under a guard.
#[cfg(not(all(target_os = "linux", not(miri))))]
pub(crate) fn prepare() {}

/// An instant no earlier than the precise monotonic clock now, when this
/// platform offers a cheap and bounded one and [`prepare`] has run.
#[cfg(all(target_os = "linux", not(miri)))]
#[inline]
pub(crate) fn upper_bound() -> Option<Instant> {
    linux::upper_bound()
}

/// An instant no earlier than the precise monotonic clock now, when this
/// platform offers a cheap and bounded one and [`prepare`] has run.
#[cfg(not(all(target_os = "linux", not(miri))))]
#[inline]
pub(crate) fn upper_bound() -> Option<Instant> {
    None
}

#[cfg(all(target_os = "linux", not(miri)))]
mod linux {
    use rustix::time::{ClockId, Timespec, clock_getres, clock_gettime};
    use std::sync::OnceLock;
    use std::time::{Duration, Instant};

    const SLACK_TICKS: u32 = 8;
    const MIN_SLACK: Duration = Duration::from_secs(1);
    // Linux reports one tick, 1-10 ms for HZ 1000-100. A much finer resolution
    // (a compatibility layer reporting its counter frequency) says nothing
    // about how often the coarse clock actually steps.
    const MIN_TICK: Duration = Duration::from_micros(500);
    const MAX_TICK: Duration = Duration::from_millis(10);

    pub(super) struct Anchor {
        coarse: Duration,
        precise: Instant,
        slack: Duration,
    }

    static ANCHOR: OnceLock<Option<Anchor>> = OnceLock::new();

    pub(super) fn prepare() -> Option<&'static Anchor> {
        ANCHOR.get_or_init(Anchor::probe).as_ref()
    }

    #[inline]
    pub(super) fn upper_bound() -> Option<Instant> {
        ANCHOR.get()?.as_ref()?.bound()
    }

    impl Anchor {
        #[cold]
        fn probe() -> Option<Self> {
            if nohz_full() {
                return None;
            }
            let tick = tick().filter(|tick| (MIN_TICK..=MAX_TICK).contains(tick))?;
            // Coarse first: the precise reading taken after it cannot be earlier.
            let coarse = duration(clock_gettime(ClockId::MonotonicCoarse))?;
            Some(Self {
                coarse,
                precise: Instant::now(),
                slack: (tick * SLACK_TICKS).max(MIN_SLACK),
            })
        }

        #[inline]
        fn bound(&self) -> Option<Instant> {
            let since =
                duration(clock_gettime(ClockId::MonotonicCoarse))?.checked_sub(self.coarse)?;
            self.precise.checked_add(since.checked_add(self.slack)?)
        }

        #[cfg(test)]
        pub(super) fn slack(&self) -> Duration {
            self.slack
        }
    }

    /// The coarse clock's step as the kernel reports it.
    pub(super) fn tick() -> Option<Duration> {
        duration(clock_getres(ClockId::MonotonicCoarse))
    }

    /// Whether [`Anchor::probe`] accepts this tick.
    #[cfg(test)]
    pub(super) fn usable_tick(tick: Duration) -> bool {
        (MIN_TICK..=MAX_TICK).contains(&tick)
    }

    #[inline]
    fn duration(at: Timespec) -> Option<Duration> {
        let nanos = u32::try_from(at.tv_nsec)
            .ok()
            .filter(|nanos| *nanos < 1_000_000_000)?;
        Some(Duration::new(u64::try_from(at.tv_sec).ok()?, nanos))
    }

    /// The list is empty or "(null)" when no CPU runs tickless, and the file is
    /// absent on kernels built without `NO_HZ_FULL`.
    pub(super) fn nohz_full() -> bool {
        std::fs::read_to_string("/sys/devices/system/cpu/nohz_full")
            .is_ok_and(|cpus| !matches!(cpus.trim(), "" | "(null)"))
    }
}

#[cfg(all(test, target_os = "linux", not(miri)))]
mod tests {
    use super::{linux, upper_bound};
    use std::time::Instant;

    #[test]
    fn anchors_unless_cpus_run_tickless_or_the_tick_is_unusable() {
        // CI kernels tick at 1-4 ms, so there the coarse path is exercised.
        let usable = linux::tick().is_some_and(linux::usable_tick);
        assert_eq!(linux::prepare().is_some(), usable && !linux::nohz_full());
    }

    #[test]
    fn slack_is_at_least_one_second() {
        if let Some(anchor) = linux::prepare() {
            assert!(anchor.slack() >= std::time::Duration::from_secs(1));
        }
    }

    #[test]
    fn bound_is_never_before_the_precise_clock() {
        if linux::prepare().is_none() {
            return;
        }
        for _ in 0..10_000 {
            let before = Instant::now();
            let bound = upper_bound().expect("a prepared anchor keeps producing bounds");
            assert!(
                bound >= before,
                "coarse bound fell behind the precise clock"
            );
        }
    }

    #[test]
    fn bound_stays_within_twice_its_slack_of_the_precise_clock() {
        let Some(anchor) = linux::prepare() else {
            return;
        };
        for _ in 0..1_000 {
            let bound = upper_bound().expect("a prepared anchor keeps producing bounds");
            let after = Instant::now();
            // The anchor pair and the latest coarse reading each lag by at most
            // the slack, so the bound cannot run further ahead than twice it.
            assert!(
                bound <= after + anchor.slack() * 2,
                "coarse bound ran far ahead"
            );
        }
    }
}
