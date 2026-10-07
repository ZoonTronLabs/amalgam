//! Actual SDK aggregation/export proves native metrics without a facade recorder.
#![cfg(feature = "opentelemetry")]
use amalgam::*;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData, ResourceMetrics};
use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider, Temporality};
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Clone, Debug)]
enum Value {
    Counter(u64),
    Histogram(u64),
}
#[derive(Clone, Debug)]
struct Point {
    scope: String,
    name: String,
    attributes: BTreeMap<String, String>,
    value: Value,
}
#[derive(Clone, Default, Debug)]
struct Capture {
    points: Arc<Mutex<Vec<Point>>>,
}
impl PushMetricExporter for Capture {
    async fn export(&self, metrics: &ResourceMetrics) -> OTelSdkResult {
        let mut points = Vec::new();
        for scope in metrics.scope_metrics() {
            for metric in scope.metrics() {
                match metric.data() {
                    AggregatedMetrics::U64(MetricData::Sum(sum)) => {
                        for point in sum.data_points() {
                            points.push(Point {
                                scope: scope.scope().name().to_owned(),
                                name: metric.name().to_owned(),
                                attributes: point
                                    .attributes()
                                    .map(|attr| {
                                        (attr.key.as_str().to_owned(), attr.value.to_string())
                                    })
                                    .collect(),
                                value: Value::Counter(point.value()),
                            });
                        }
                    }
                    AggregatedMetrics::F64(MetricData::Histogram(histogram)) => {
                        assert_eq!(metric.unit(), "s");
                        for point in histogram.data_points() {
                            points.push(Point {
                                scope: scope.scope().name().to_owned(),
                                name: metric.name().to_owned(),
                                attributes: point
                                    .attributes()
                                    .map(|attr| {
                                        (attr.key.as_str().to_owned(), attr.value.to_string())
                                    })
                                    .collect(),
                                value: Value::Histogram(point.count()),
                            });
                        }
                    }
                    // This exporter intentionally ignores instruments it does not collect.
                    _ => {}
                }
            }
        }
        *self.points.lock().unwrap() = points;
        Ok(())
    }
    fn force_flush(&self) -> OTelSdkResult {
        Ok(())
    }
    fn shutdown_with_timeout(&self, _: Duration) -> OTelSdkResult {
        Ok(())
    }
    fn temporality(&self) -> Temporality {
        Temporality::Cumulative
    }
}
impl Capture {
    fn points(&self) -> Vec<Point> {
        self.points.lock().unwrap().clone()
    }
    fn counter(&self, name: &str, cache: &str) -> u64 {
        self.points()
            .iter()
            .filter(|p| {
                p.name == name
                    && p.attributes
                        .get("cache_name")
                        .is_some_and(|label| label == cache)
            })
            .map(|p| match p.value {
                Value::Counter(value) => value,
                Value::Histogram(_) => panic!("counter selected a histogram"),
            })
            .sum()
    }
}
fn sdk() -> (SdkMeterProvider, Capture) {
    let capture = Capture::default();
    let reader = PeriodicReader::builder(capture.clone())
        .with_interval(Duration::from_secs(3600))
        .build();
    (
        SdkMeterProvider::builder().with_reader(reader).build(),
        capture,
    )
}
async fn flush(provider: &SdkMeterProvider) {
    let provider = provider.clone();
    tokio::task::spawn_blocking(move || provider.force_flush())
        .await
        .unwrap()
        .unwrap();
}
async fn stop(provider: SdkMeterProvider) {
    tokio::task::spawn_blocking(move || provider.shutdown())
        .await
        .unwrap()
        .unwrap();
}
fn options() -> EntryOptions {
    EntryOptions::new(Duration::from_secs(60))
        .with_allow_background_distributed_operations(false)
        .with_allow_background_backplane_operations(false)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_sdk_counts_all_warm_reads_even_when_broadcast_loses_events() {
    let (provider, capture) = sdk();
    let plugin = Arc::new(OtelMetricsPlugin::from_provider(&provider));
    let cache = Cache::<u64>::builder()
        .name("warm")
        .instance_id("secret-node")
        .events_capacity(1)
        .default_options(options())
        .plugin(plugin)
        .build();
    let mut stream = cache.events().subscribe_layers();
    cache
        .try_set("secret-key", 17)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    for _ in 0..1000 {
        assert_eq!(
            cache.read("secret-key", None).await.unwrap().value(),
            Some(&17)
        );
    }
    assert!(matches!(
        stream.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_))
    ));
    flush(&provider).await;
    assert_eq!(capture.counter("amalgam.cache.try_get", "warm"), 1000);
    assert_eq!(capture.counter("amalgam.cache.hit", "warm"), 1000);
    assert_eq!(capture.counter("amalgam.memory.get", "warm"), 1000);
    assert_eq!(capture.counter("amalgam.memory.hit", "warm"), 1000);
    assert_eq!(capture.counter("amalgam.cache.set", "warm"), 1);
    assert_eq!(capture.counter("amalgam.memory.set", "warm"), 1);
    let points = capture.points();
    assert!(
        points
            .iter()
            .any(|p| p.scope == "amalgam.memory" && p.name == "amalgam.memory.hit")
    );
    let histograms: u64 = points
        .iter()
        .filter_map(|p| match p.value {
            Value::Histogram(count) if p.name == "amalgam.operation.duration" => Some(count),
            _ => None,
        })
        .sum();
    assert_eq!(histograms, 1001);
    assert!(
        points
            .iter()
            .flat_map(|p| p.attributes.iter())
            .all(|(key, value)| !key.contains("instance")
                && !key.contains("key")
                && !value.contains("secret"))
    );
    cache.shutdown().await.unwrap();
    stop(provider).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cold_l2_hydration_and_remove_are_separate_from_logical_cache_metrics() {
    let (provider, capture) = sdk();
    let plugin = Arc::new(OtelMetricsPlugin::from_provider(&provider));
    let clock = Arc::new(ManualClock::new(Timestamp::from_ticks(0)));
    let backend = Arc::new(InMemoryDistributedCache::new(clock.clone()));
    let build = |id| {
        Cache::<u64>::builder()
            .name("l2")
            .instance_id(id)
            .clock(clock.clone())
            .distributed(backend.clone())
            .serializer(Arc::new(JsonSerializer))
            .default_options(options())
            .plugin(plugin.clone())
            .build()
    };
    let a = build("secret-a");
    let b = build("secret-b");
    a.try_set("secret-key", 23)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert_eq!(b.read("secret-key", None).await.unwrap().value(), Some(&23));
    flush(&provider).await;
    assert_eq!(capture.counter("amalgam.cache.set", "l2"), 1);
    assert_eq!(
        capture.counter("amalgam.memory.set", "l2"),
        2,
        "hydration is a physical L1 set"
    );
    assert_eq!(capture.counter("amalgam.distributed.set", "l2"), 1);
    assert_eq!(capture.counter("amalgam.distributed.get", "l2"), 1);
    assert_eq!(capture.counter("amalgam.distributed.hit", "l2"), 1);
    assert_eq!(capture.counter("amalgam.cache.hit", "l2"), 1);
    a.remove("secret-key")
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    flush(&provider).await;
    assert_eq!(capture.counter("amalgam.distributed.remove", "l2"), 1);
    assert_eq!(capture.counter("amalgam.memory.evict", "l2"), 1);
    assert_eq!(capture.counter("amalgam.cache.remove", "l2"), 1);
    assert!(
        capture
            .points()
            .iter()
            .any(|p| p.scope == "amalgam.distributed" && p.name == "amalgam.distributed.hit")
    );
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
    stop(provider).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn individual_tags_are_exported_only_when_explicitly_requested() {
    for tags in [MetricTags::Exclude, MetricTags::Include] {
        let (provider, capture) = sdk();
        let cache = Cache::<u64>::builder()
            .name("tagged")
            .plugin(Arc::new(
                OtelMetricsPlugin::from_provider(&provider).with_tags(tags),
            ))
            .build();
        let tag = Tag::new("private-tag").unwrap();
        cache
            .try_set_full("key", 17, None, Box::from([tag.clone()]))
            .await
            .unwrap()
            .wait()
            .await
            .unwrap();
        cache
            .remove_by_tag(tag)
            .with_receipt()
            .await
            .unwrap()
            .wait()
            .await
            .unwrap();
        flush(&provider).await;
        assert_eq!(capture.counter("amalgam.cache.remove_by_tag", "tagged"), 1);
        let point = capture
            .points()
            .into_iter()
            .find(|p| p.name == "amalgam.cache.remove_by_tag")
            .unwrap();
        match tags {
            MetricTags::Exclude => assert!(!point.attributes.contains_key("operation_tag")),
            MetricTags::Include => assert_eq!(point.attributes["operation_tag"], "private-tag"),
        }
        assert!(!cache.read("key", None).await.unwrap().has_value());
        cache.shutdown().await.unwrap();
        stop(provider).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cache_name_budget_is_historical_shared_and_does_not_mix_named_series() {
    let (provider, capture) = sdk();
    let budget = Arc::new(CacheLabelBudget::with_limits(
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(8).unwrap(),
    ));
    let plugin =
        Arc::new(OtelMetricsPlugin::from_provider(&provider).with_label_budget(budget.clone()));
    for name in ["one", "two", "three", "name-that-is-too-long"] {
        let cache = Cache::<u64>::builder()
            .name(name)
            .plugin(plugin.clone())
            .build();
        cache
            .try_set("private-key", 17)
            .await
            .unwrap()
            .wait()
            .await
            .unwrap();
        assert!(cache.read("private-key", None).await.unwrap().has_value());
        cache.shutdown().await.unwrap();
    }
    flush(&provider).await;
    assert_eq!(budget.assigned_names(), 2);
    assert_eq!(capture.counter("amalgam.cache.set", "one"), 1);
    assert_eq!(capture.counter("amalgam.cache.set", "two"), 1);
    assert_eq!(capture.counter("amalgam.cache.set", "other"), 2);
    stop(provider).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn factory_failure_stale_hit_timeout_and_background_success_have_distinct_attributes() {
    let (provider, capture) = sdk();
    let clock = Arc::new(ManualClock::new(Timestamp::from_ticks(0)));
    let opts = EntryOptions::new(Duration::from_millis(200)).with_fail_safe(
        true,
        Some(Duration::from_secs(60)),
        Some(Duration::from_millis(1)),
    );
    let cache = Cache::<u64>::builder()
        .name("factory")
        .clock(clock.clone())
        .plugin(Arc::new(OtelMetricsPlugin::from_provider(&provider)))
        .build();
    cache
        .get_or_set_with(
            "failure",
            |ctx| async move { Ok::<_, amalgam::FactoryError>(ctx.value(17)) },
            opts.clone(),
        )
        .await
        .unwrap();
    clock.advance(Duration::from_millis(201));
    assert_eq!(
        cache
            .get_or_set_with(
                "failure",
                |ctx| async move { Err(ctx.fail("private factory error")) },
                opts.clone()
            )
            .await
            .unwrap(),
        17
    );
    let soft = opts.with_factory_timeouts(
        Timeout::After(Duration::from_millis(20)),
        Timeout::Infinite,
        true,
    );
    cache
        .get_or_set_with(
            "soft",
            |ctx| async move { Ok::<_, amalgam::FactoryError>(ctx.value(23)) },
            soft.clone(),
        )
        .await
        .unwrap();
    clock.advance(Duration::from_millis(201));
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let gate = release.clone();
    let signal = entered.clone();
    assert_eq!(
        cache
            .get_or_set_with(
                "soft",
                move |ctx| async move {
                    signal.notify_one();
                    gate.acquire().await.unwrap().forget();
                    Ok::<_, amalgam::FactoryError>(ctx.value(29))
                },
                soft
            )
            .await
            .unwrap(),
        23
    );
    entered.notified().await;
    release.add_permits(1);
    cache.flush_pending().await.unwrap();
    flush(&provider).await;
    assert_eq!(
        capture.counter("amalgam.factory.success", "factory"),
        3,
        "two foreground seeds and one background completion"
    );
    assert_eq!(capture.counter("amalgam.factory.error", "factory"), 1);
    assert_eq!(
        capture.counter("amalgam.factory.synthetic_timeout", "factory"),
        1
    );
    assert!(capture.counter("amalgam.failsafe_activate", "factory") >= 2);
    let points = capture.points();
    assert!(
        points.iter().any(|p| p.name == "amalgam.cache.hit"
            && p.attributes.get("stale").is_some_and(|v| v == "true"))
    );
    assert!(points.iter().any(|p| {
        p.name == "amalgam.factory.success"
            && p.attributes
                .get("operation_background")
                .is_some_and(|v| v == "true")
    }));
    assert!(
        points
            .iter()
            .all(|p| !p.attributes.values().any(|v| v.contains("private")))
    );
    assert_eq!(cache.read("soft", None).await.unwrap().value(), Some(&29));
    cache.shutdown().await.unwrap();
    stop(provider).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backplane_has_its_own_scope_and_records_only_foreign_receives() {
    let (provider, capture) = sdk();
    let plugin = Arc::new(OtelMetricsPlugin::from_provider(&provider));
    let backend = Arc::new(InMemoryDistributedCache::new(Arc::new(SystemClock)));
    let bp = Arc::new(InProcessBackplane::default());
    let build = |id| {
        Cache::<u64>::builder()
            .name("backplane")
            .instance_id(id)
            .distributed(backend.clone())
            .serializer(Arc::new(JsonSerializer))
            .backplane(bp.clone())
            .default_options(options())
            .plugin(plugin.clone())
            .build()
    };
    let a = build("a");
    let b = build("b");
    a.try_set("key", 17).await.unwrap().wait().await.unwrap();
    let started = std::time::Instant::now();
    loop {
        flush(&provider).await;
        if capture.counter("amalgam.backplane.receive", "backplane") == 1 {
            break;
        }
        assert!(started.elapsed() < Duration::from_secs(5));
        tokio::task::yield_now().await;
    }
    assert_eq!(capture.counter("amalgam.backplane.publish", "backplane"), 1);
    assert_eq!(capture.counter("amalgam.backplane.receive", "backplane"), 1);
    assert!(
        capture
            .points()
            .iter()
            .any(|p| p.scope == "amalgam.backplane" && p.name == "amalgam.backplane.receive")
    );
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
    stop(provider).await;
}

#[test]
fn otlp_metric_constructor_returns_typed_configuration_failures_without_global_installation() {
    assert!(matches!(
        otlp_meter_provider("", "http://127.0.0.1:4317"),
        Err(OtelInitError::BlankServiceName)
    ));
    assert!(matches!(
        otlp_meter_provider("service", "http://127.0.0.1:4317"),
        Err(OtelInitError::MissingRuntime)
    ));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let error = runtime
        .block_on(async { otlp_meter_provider("service", "http://[") })
        .unwrap_err();
    assert!(matches!(error, OtelInitError::Exporter(_)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conditional_success_counts_once_in_the_actual_foreground_or_eager_context() {
    let (provider, capture) = sdk();
    let clock = Arc::new(ManualClock::default());
    let opts = EntryOptions::new(Duration::from_millis(200))
        .with_fail_safe(
            true,
            Some(Duration::from_secs(60)),
            Some(Duration::from_millis(1)),
        )
        .with_eager_refresh(EagerThreshold::new(0.5));
    let cache = Cache::<u64>::builder()
        .name("conditional")
        .clock(clock.clone())
        .default_options(opts)
        .plugin(Arc::new(OtelMetricsPlugin::from_provider(&provider)))
        .build();
    cache
        .get_or_set::<_, _, _, amalgam::FactoryError>("key", |ctx| async move {
            Ok::<_, amalgam::FactoryError>(ctx.value(17))
        })
        .await
        .unwrap();
    clock.advance(Duration::from_millis(201));
    assert_eq!(
        cache
            .get_or_set::<_, _, _, amalgam::FactoryError>(
                "key",
                |ctx| async move { ctx.not_modified() }
            )
            .await
            .unwrap(),
        17
    );
    clock.advance(Duration::from_millis(101));
    assert_eq!(
        cache
            .get_or_set::<_, _, _, amalgam::FactoryError>(
                "key",
                |ctx| async move { ctx.not_modified() }
            )
            .await
            .unwrap(),
        17
    );
    cache.flush_pending().await.unwrap();
    flush(&provider).await;
    assert_eq!(capture.counter("amalgam.factory.success", "conditional"), 3);
    assert_eq!(capture.counter("amalgam.eager_refresh", "conditional"), 1);
    let mut modes = BTreeMap::new();
    for point in capture
        .points()
        .into_iter()
        .filter(|p| p.name == "amalgam.factory.success")
    {
        let Value::Counter(count) = point.value else {
            panic!("factory success must be a counter")
        };
        modes.insert(point.attributes["operation_background"].clone(), count);
    }
    assert_eq!(
        modes,
        BTreeMap::from([("false".to_owned(), 2), ("true".to_owned(), 1)])
    );
    cache.shutdown().await.unwrap();
    stop(provider).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn eager_refresh_that_reuses_a_newer_l2_value_does_not_fabricate_factory_success() {
    let (provider, capture) = sdk();
    let clock = Arc::new(ManualClock::default());
    let backend = Arc::new(InMemoryDistributedCache::new(clock.clone()));
    let opts = options().with_eager_refresh(EagerThreshold::new(0.5));
    let a = Cache::<u64>::builder()
        .name("eager-reuse")
        .instance_id("a")
        .clock(clock.clone())
        .distributed(backend.clone())
        .serializer(Arc::new(JsonSerializer))
        .default_options(opts)
        .plugin(Arc::new(OtelMetricsPlugin::from_provider(&provider)))
        .build();
    let b = Cache::<u64>::builder()
        .instance_id("b")
        .clock(clock.clone())
        .distributed(backend)
        .serializer(Arc::new(JsonSerializer))
        .default_options(options())
        .build();
    a.try_set("key", 17).await.unwrap().wait().await.unwrap();
    clock.advance(Duration::from_secs(31));
    b.try_set("key", 23).await.unwrap().wait().await.unwrap();
    assert_eq!(
        a.get_or_set::<_, _, _, amalgam::FactoryError>("key", |_| async {
            panic!("newer L2 must bypass origin")
        })
        .await
        .unwrap(),
        17
    );
    a.flush_pending().await.unwrap();
    assert_eq!(a.read("key", None).await.unwrap().value(), Some(&23));
    flush(&provider).await;
    assert_eq!(capture.counter("amalgam.eager_refresh", "eager-reuse"), 1);
    assert_eq!(capture.counter("amalgam.factory.success", "eager-reuse"), 0);
    assert_eq!(capture.counter("amalgam.factory.error", "eager-reuse"), 0);
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
    stop(provider).await;
}

struct Unavailable(std::sync::atomic::AtomicUsize);
#[async_trait::async_trait]
impl DistributedCache for Unavailable {
    async fn get(&self, _: &str) -> Result<Option<Vec<u8>>> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Err(Error::distributed(std::io::Error::other(
            "private backend cause",
        )))
    }
    async fn set(&self, _: &str, _: Vec<u8>, _: Option<Duration>) -> Result<()> {
        unreachable!("read-only fixture")
    }
    async fn remove(&self, _: &str) -> Result<()> {
        unreachable!("read-only fixture")
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn throwing_backend_read_counts_the_attempt_but_circuit_and_policy_skips_do_not() {
    let (provider, capture) = sdk();
    let backend = Arc::new(Unavailable(std::sync::atomic::AtomicUsize::new(0)));
    let cache = Cache::<u64>::builder()
        .name("failed-read")
        .distributed(backend.clone())
        .serializer(Arc::new(JsonSerializer))
        .distributed_circuit_breaker(Duration::from_secs(60))
        .plugin(Arc::new(OtelMetricsPlugin::from_provider(&provider)))
        .build();
    assert!(matches!(
        cache.read("first", None).await,
        Err(Error::Transport(TransportError::Distributed { .. }))
    ));
    assert_eq!(cache.get_or_default("open-circuit", 7, None).await, 7);
    assert!(
        !cache
            .read(
                "policy-skip",
                Some(options().with_skip_distributed(true, false))
            )
            .await
            .unwrap()
            .has_value()
    );
    flush(&provider).await;
    assert_eq!(backend.0.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(capture.counter("amalgam.distributed.get", "failed-read"), 1);
    assert_eq!(capture.counter("amalgam.distributed.hit", "failed-read"), 0);
    assert_eq!(
        capture.counter("amalgam.distributed.circuit_breaker_change", "failed-read"),
        1
    );
    assert!(capture.points().iter().any(|p| {
        p.name == "amalgam.operation.duration"
            && p.attributes
                .get("outcome")
                .is_some_and(|v| v == "distributed_error")
    }));
    assert!(
        capture
            .points()
            .iter()
            .all(|p| !p.attributes.values().any(|v| v.contains("private")))
    );
    cache.shutdown().await.unwrap();
    stop(provider).await;
}
