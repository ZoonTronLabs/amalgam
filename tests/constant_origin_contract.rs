//! A supplied value is not a user factory: deadlines, eager work and events differ.
use amalgam::*;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

#[derive(Default)]
struct OriginEvents(AtomicUsize);
impl Plugin for OriginEvents {
    fn name(&self) -> &str {
        "constant-origin-events"
    }
    fn on_event(&self, event: &CacheEvent) {
        if matches!(
            event,
            CacheEvent::FactorySuccess { .. }
                | CacheEvent::EagerRefresh { .. }
                | CacheEvent::FactorySyntheticTimeout { .. }
        ) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
}
fn zero_budget() -> EntryOptions {
    EntryOptions::new(Duration::from_secs(60)).with_factory_timeouts(
        Timeout::Infinite,
        Timeout::After(Duration::ZERO),
        false,
    )
}
fn eager() -> EntryOptions {
    EntryOptions::new(Duration::from_secs(60)).with_eager_refresh(EagerThreshold::new(0.5))
}

#[tokio::test]
async fn async_constant_ignores_factory_budget_and_does_not_report_factory_success() {
    let events = Arc::new(OriginEvents::default());
    let cache = Cache::<u64>::builder()
        .plugin(events.clone())
        .try_build()
        .unwrap();
    let answer = cache
        .get_or_set_value("constant", 42, Some(zero_budget()))
        .await;
    let factory = cache
        .get_or_set_with(
            "factory",
            |ctx| async move { Ok(ctx.value(99)) },
            zero_budget(),
        )
        .await;
    cache.shutdown().await.unwrap();
    assert_eq!(answer.unwrap(), 42);
    assert!(matches!(factory, Err(Error::FactoryTimeout { .. })));
    assert_eq!(
        events.0.load(Ordering::SeqCst),
        0,
        "supplied values create no factory events; a rejected zero budget starts no factory"
    );
}

#[test]
fn native_constant_ignores_factory_budget_and_does_not_report_factory_success() {
    let events = Arc::new(OriginEvents::default());
    let cache =
        BlockingCache::<u64>::from_builder(Cache::builder().plugin(events.clone())).unwrap();
    let answer = cache.get_or_set_value("constant", 42, Some(zero_budget()));
    cache.shutdown().unwrap();
    assert_eq!(answer.unwrap(), 42);
    assert_eq!(events.0.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn async_warm_constant_never_eagerly_replaces_the_existing_value() {
    let events = Arc::new(OriginEvents::default());
    let clock = Arc::new(ManualClock::new(Timestamp::from_ticks(100_000_000)));
    let cache = Cache::<u64>::builder()
        .clock(clock.clone())
        .plugin(events.clone())
        .try_build()
        .unwrap();
    cache
        .try_set_full("warm", 1, Some(eager()), Box::from([]))
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    clock.set(Timestamp::from_ticks(410_000_000));
    assert_eq!(
        cache
            .get_or_set_value("warm", 99, Some(eager()))
            .await
            .unwrap(),
        1
    );
    cache.flush_pending().await.unwrap();
    let after = cache.read("warm", None).await.unwrap().into_value();
    cache.shutdown().await.unwrap();
    assert_eq!(after, Some(1));
    assert_eq!(events.0.load(Ordering::SeqCst), 0);
}

#[test]
fn native_warm_constant_never_eagerly_replaces_the_existing_value() {
    let events = Arc::new(OriginEvents::default());
    let clock = Arc::new(ManualClock::new(Timestamp::from_ticks(100_000_000)));
    let cache = BlockingCache::<u64>::from_builder(
        Cache::builder().clock(clock.clone()).plugin(events.clone()),
    )
    .unwrap();
    cache
        .try_set_full("warm", 1, Some(eager()), Box::from([]))
        .unwrap()
        .wait()
        .unwrap();
    clock.set(Timestamp::from_ticks(410_000_000));
    assert_eq!(
        cache.get_or_set_value("warm", 99, Some(eager())).unwrap(),
        1
    );
    cache.flush_pending().unwrap();
    let after = cache.read("warm", None).unwrap().into_value();
    cache.shutdown().unwrap();
    assert_eq!(after, Some(1));
    assert_eq!(events.0.load(Ordering::SeqCst), 0);
}
