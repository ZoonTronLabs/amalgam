//! Default and call-specific lifetimes agree at precision and range boundaries.
use amalgam::{Cache, EntryOptions, Timestamp, provider::ManualClock, source};
use std::sync::Arc;
use std::time::Duration;

async fn compare_at_boundaries(
    duration: Duration,
    start: Timestamp,
    advances: &[(Duration, bool)],
) -> amalgam::Result<()> {
    let clock = Arc::new(ManualClock::new(start));
    let options = EntryOptions::new(duration);
    let default = Cache::<u64>::builder()
        .clock(clock.clone())
        .default_options(options.clone())
        .try_build()?;
    let explicit = Cache::<u64>::builder().clock(clock.clone()).try_build()?;
    default.set("value", 7).await?;
    explicit
        .set("value", 7)
        .options(|_| options.clone())
        .await?;
    for &(advance, present) in advances {
        clock.advance(advance);
        let default_value = default.try_get("value").await?;
        let explicit_value = explicit
            .try_get("value")
            .options(|_| options.clone())
            .await?;
        assert_eq!(default_value, explicit_value);
        assert_eq!(default_value, present.then_some(7));
    }
    default.shutdown().await?;
    explicit.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn default_and_explicit_writes_keep_tick_flooring_and_saturated_expiration()
-> amalgam::Result<()> {
    compare_at_boundaries(
        Duration::from_nanos(99),
        Timestamp::from_ticks(0),
        &[(Duration::ZERO, false)],
    )
    .await?;
    compare_at_boundaries(
        Duration::from_nanos(250),
        Timestamp::from_ticks(0),
        &[
            (Duration::ZERO, true),
            (Duration::from_nanos(100), true),
            (Duration::from_nanos(100), false),
        ],
    )
    .await?;
    compare_at_boundaries(
        Duration::from_nanos(500),
        Timestamp::from_ticks(i64::MAX - 2),
        &[
            (Duration::from_nanos(100), true),
            (Duration::from_nanos(100), false),
        ],
    )
    .await?;
    compare_at_boundaries(
        Duration::MAX,
        Timestamp::MIN,
        &[(Duration::ZERO, true), (Duration::MAX, false)],
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn local_default_fail_safe_keeps_its_distinct_physical_deadline() -> amalgam::Result<()> {
    let cache = Cache::<u64>::builder()
        .default_options(EntryOptions::new(Duration::from_millis(40)).with_fail_safe(
            true,
            Some(Duration::from_secs(3)),
            None,
        ))
        .try_build()?;
    cache.set("value", 7).await?;
    tokio::time::sleep(Duration::from_millis(80)).await;
    let value = cache
        .get_or_set(
            "value",
            source::factory(|ctx| async move { Err::<u64, _>(ctx.fail("origin unavailable")) }),
        )
        .await?;
    assert_eq!(value, 7);
    cache.shutdown().await?;
    Ok(())
}
