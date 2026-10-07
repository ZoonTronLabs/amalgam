//! Public range, concurrent-clock and timing-capability contracts.

use std::sync::Arc;
use std::time::Duration;

use amalgam::time::{TICKS_PER_SECOND, duration_to_ticks, ticks_to_duration};
use amalgam::{
    Timestamp, provider::Clock, provider::ClockTiming, provider::ManualClock, provider::SystemClock,
};

#[test]
fn representable_long_duration_roundtrips_without_nanosecond_truncation() {
    let years_1000 = Duration::from_secs(1000 * 365 * 24 * 60 * 60);
    assert_eq!(ticks_to_duration(duration_to_ticks(years_1000)), years_1000);
    assert_eq!(
        ticks_to_duration(i64::MAX),
        Duration::new(922_337_203_685, 477_580_700)
    );
    assert_eq!(ticks_to_duration(-1), Duration::ZERO);
}

#[test]
fn full_timestamp_range_preserves_elapsed_duration() {
    let expected = Duration::new(1_844_674_407_370, 955_161_500);
    assert_eq!(
        Timestamp::MAX.saturating_duration_since(Timestamp::MIN),
        expected
    );
    assert_eq!(
        Timestamp::MIN.saturating_duration_since(Timestamp::MAX),
        Duration::ZERO
    );
    assert_eq!(Timestamp::MIN.saturating_add(expected), Timestamp::MAX);
}

#[test]
fn timestamp_addition_clamps_only_the_final_point_in_time() {
    let full_span = Duration::new(1_844_674_407_370, 955_161_500);
    assert_eq!(
        Timestamp::from_ticks(-1).saturating_add(full_span),
        Timestamp::MAX
    );
    assert_eq!(Timestamp::MIN.saturating_add(Duration::MAX), Timestamp::MAX);
    assert_eq!(
        Timestamp::MIN.saturating_add(Duration::ZERO),
        Timestamp::MIN
    );
    assert_eq!(
        Timestamp::MAX.saturating_add(Duration::from_nanos(100)),
        Timestamp::MAX
    );
    assert_eq!(
        Timestamp::from_ticks(1).saturating_add(Duration::from_nanos(99)),
        Timestamp::from_ticks(1)
    );
    assert_eq!(
        Timestamp::from_ticks(1).saturating_add(Duration::from_nanos(100)),
        Timestamp::from_ticks(2)
    );
}

#[test]
fn manual_clock_advance_saturates_instead_of_wrapping_into_the_past() {
    let clock = ManualClock::new(Timestamp::from_ticks(i64::MAX - TICKS_PER_SECOND));
    clock.advance(Duration::from_secs(2));
    assert_eq!(clock.now(), Timestamp::MAX);
    clock.advance(Duration::MAX);
    assert_eq!(clock.now(), Timestamp::MAX);
    clock.set(Timestamp::MIN);
    clock.advance(Duration::new(1_844_674_407_370, 955_161_500));
    assert_eq!(clock.now(), Timestamp::MAX);
}

#[test]
fn concurrent_manual_clock_advances_are_not_lost_or_wrapped() {
    let clock = Arc::new(ManualClock::new(Timestamp::from_ticks(0)));
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let clock = clock.clone();
            std::thread::spawn(move || {
                for _ in 0..10_000 {
                    clock.advance(Duration::from_nanos(100));
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
    assert_eq!(clock.now(), Timestamp::from_ticks(80_000));
}

#[test]
fn clock_timing_capability_survives_explicit_trait_object_and_arc_wrappers() {
    struct Frozen;
    impl Clock for Frozen {
        fn now(&self) -> Timestamp {
            Timestamp::MIN
        }
    }
    let real: Arc<dyn Clock> = Arc::new(SystemClock);
    let controlled: Arc<dyn Clock> = Arc::new(ManualClock::default());
    assert_eq!(real.timing_model(), ClockTiming::RealTime);
    assert_eq!(controlled.timing_model(), ClockTiming::Controlled);
    assert_eq!(Frozen.timing_model(), ClockTiming::Controlled);
}

struct FrozenRealTime(ManualClock);
impl Clock for FrozenRealTime {
    fn now(&self) -> Timestamp {
        self.0.now()
    }
    fn timing_model(&self) -> ClockTiming {
        ClockTiming::RealTime
    }
}
#[tokio::test]
async fn explicit_real_time_clock_keeps_elapsed_expiry_when_its_timestamp_stops() {
    for capacity in [None, Some(2)] {
        let clock = Arc::new(FrozenRealTime(ManualClock::default()));
        let mut builder = amalgam::Cache::builder()
            .clock(clock)
            .default_options(amalgam::EntryOptions::new(Duration::from_millis(15)));
        if let Some(capacity) = capacity {
            builder = builder.max_capacity(capacity);
        }
        let cache = builder.try_build().unwrap();
        cache
            .set("k", 7_u64)
            .with_receipt()
            .await
            .unwrap()
            .wait()
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(45)).await;
        assert!(cache.read("k", None).await.unwrap().into_value().is_none());
        assert_eq!(cache.memory_usage().unwrap().entries, 0);
        cache.shutdown().await.unwrap();
    }
}
