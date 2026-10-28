//! Public range, concurrent-clock and timing-capability contracts.

#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

use std::sync::Arc;
use std::time::Duration;

use amalgam::provider::{Clock, ClockTiming, ManualClock, SystemClock};
use amalgam::time::TICKS_PER_SECOND;
use amalgam::{Timestamp, time};

#[test]
fn representable_long_duration_roundtrips_without_nanosecond_truncation() {
    let years_1000 = Duration::from_secs(1000 * 365 * 24 * 60 * 60);
    assert_eq!(
        time::ticks_to_duration(time::duration_to_ticks(years_1000)),
        years_1000,
        "contract: representable long duration roundtrips without nanosecond truncation"
    );
    assert_eq!(
        time::ticks_to_duration(i64::MAX),
        Duration::new(922_337_203_685, 477_580_700),
        "contract: representable long duration roundtrips without nanosecond truncation"
    );
    assert_eq!(
        time::ticks_to_duration(-1),
        Duration::ZERO,
        "contract: representable long duration roundtrips without nanosecond truncation"
    );
}

#[test]
fn full_timestamp_range_preserves_elapsed_duration() {
    let expected = Duration::new(1_844_674_407_370, 955_161_500);
    assert_eq!(
        Timestamp::MAX.saturating_duration_since(Timestamp::MIN),
        expected,
        "contract: full timestamp range preserves elapsed duration"
    );
    assert_eq!(
        Timestamp::MIN.saturating_duration_since(Timestamp::MAX),
        Duration::ZERO,
        "contract: full timestamp range preserves elapsed duration"
    );
    assert_eq!(
        Timestamp::MIN.saturating_add(expected),
        Timestamp::MAX,
        "contract: full timestamp range preserves elapsed duration"
    );
}

#[test]
fn timestamp_addition_clamps_only_the_final_point_in_time() {
    let full_span = Duration::new(1_844_674_407_370, 955_161_500);
    assert_eq!(
        Timestamp::from_ticks(-1).saturating_add(full_span),
        Timestamp::MAX,
        "contract: timestamp addition clamps only the final point in time"
    );
    assert_eq!(
        Timestamp::MIN.saturating_add(Duration::MAX),
        Timestamp::MAX,
        "contract: timestamp addition clamps only the final point in time"
    );
    assert_eq!(
        Timestamp::MIN.saturating_add(Duration::ZERO),
        Timestamp::MIN,
        "contract: timestamp addition clamps only the final point in time"
    );
    assert_eq!(
        Timestamp::MAX.saturating_add(Duration::from_nanos(100)),
        Timestamp::MAX,
        "contract: timestamp addition clamps only the final point in time"
    );
    assert_eq!(
        Timestamp::from_ticks(1).saturating_add(Duration::from_nanos(99)),
        Timestamp::from_ticks(1),
        "contract: timestamp addition clamps only the final point in time"
    );
    assert_eq!(
        Timestamp::from_ticks(1).saturating_add(Duration::from_nanos(100)),
        Timestamp::from_ticks(2),
        "contract: timestamp addition clamps only the final point in time"
    );
}

#[test]
fn manual_clock_advance_saturates_instead_of_wrapping_into_the_past() {
    let clock = ManualClock::new(Timestamp::from_ticks(i64::MAX - TICKS_PER_SECOND));
    clock.advance(Duration::from_secs(2));
    assert_eq!(
        clock.now(),
        Timestamp::MAX,
        "contract: manual clock advance saturates instead of wrapping into the past"
    );
    clock.advance(Duration::MAX);
    assert_eq!(
        clock.now(),
        Timestamp::MAX,
        "contract: manual clock advance saturates instead of wrapping into the past"
    );
    clock.set(Timestamp::MIN);
    clock.advance(Duration::new(1_844_674_407_370, 955_161_500));
    assert_eq!(
        clock.now(),
        Timestamp::MAX,
        "contract: manual clock advance saturates instead of wrapping into the past"
    );
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
    assert_eq!(
        clock.now(),
        Timestamp::from_ticks(80_000),
        "contract: concurrent manual clock advances are not lost or wrapped"
    );
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
    assert_eq!(
        real.timing_model(),
        ClockTiming::RealTime,
        "contract: clock timing capability survives explicit trait object and arc wrappers"
    );
    assert_eq!(
        controlled.timing_model(),
        ClockTiming::Controlled,
        "contract: clock timing capability survives explicit trait object and arc wrappers"
    );
    assert_eq!(
        Frozen.timing_model(),
        ClockTiming::Controlled,
        "contract: clock timing capability survives explicit trait object and arc wrappers"
    );
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
        let _ = cache
            .set("k", 7_u64)
            .with_receipt()
            .await
            .unwrap()
            .wait()
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(45)).await;
        assert!(
            cache.try_get("k").await.unwrap().is_none(),
            "contract: explicit real time clock keeps elapsed expiry when its timestamp stops"
        );
        assert_eq!(
            cache.memory_usage().unwrap().entries,
            0,
            "contract: explicit real time clock keeps elapsed expiry when its timestamp stops"
        );
        cache.shutdown().await.unwrap();
    }
}
