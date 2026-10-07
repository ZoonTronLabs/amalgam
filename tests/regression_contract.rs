//! Public contract acceptance for repaired audit findings.

use std::sync::atomic::{AtomicI32, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;

use amalgam::{
    Cache, CacheEvent, CacheRegistry, Clock, DistributedCache, EntryOptions, FactoryError,
    InMemoryDistributedCache, JsonSerializer, ManualClock, Plugin, Priority, RecoveryConfig,
};

fn no_recovery() -> RecoveryConfig {
    RecoveryConfig {
        enabled: false,
        ..RecoveryConfig::default()
    }
}

#[tokio::test]
async fn read_only_reads_l2_even_when_memory_reads_are_skipped() {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::default());
    let l2: Arc<dyn DistributedCache> = Arc::new(InMemoryDistributedCache::new(clock.clone()));
    let make = || {
        Cache::<i32>::builder()
            .clock(clock.clone())
            .distributed(l2.clone())
            .serializer(Arc::new(JsonSerializer))
            .auto_recovery(no_recovery())
            .build()
    };
    let a = make();
    let b = make();
    a.set("shared", 42).await.unwrap();
    assert_eq!(b.read("shared", None).await.unwrap().value(), Some(&42));
    assert_eq!(b.read_or_default("shared", -1, None).await.unwrap(), 42);
    assert_eq!(
        b.get_or_set("shared", amalgam::source::value(999))
            .await
            .unwrap(),
        42,
        "the authoritative value was present in L2 all along"
    );
    let skip_memory = EntryOptions::default().with_skip_memory(true, false);
    assert_eq!(
        b.read("shared", Some(skip_memory)).await.unwrap().value(),
        Some(&42)
    );
}

#[tokio::test]
async fn registry_concurrent_get_or_create_returns_one_shared_cache() {
    let registry = Arc::new(CacheRegistry::<i32>::new());
    let barrier = Arc::new(Barrier::new(2));
    let builds = Arc::new(AtomicUsize::new(0));
    let spawn = || {
        let registry = registry.clone();
        let barrier = barrier.clone();
        let builds = builds.clone();
        std::thread::spawn(move || {
            barrier.wait(); // Synchronize callers before entering initialization.
            registry.get_or_create("same-name", || {
                builds.fetch_add(1, Ordering::SeqCst);
                Cache::new()
            })
        })
    };
    let first = spawn();
    let second = spawn();
    let first = first.join().unwrap();
    let second = second.join().unwrap();
    assert_eq!(builds.load(Ordering::SeqCst), 1);
    assert_eq!(registry.len(), 1);
    first.set("private-to-first", 1).await.unwrap();
    assert_eq!(
        second.try_get("private-to-first", None).await.value(),
        Some(&1)
    );
}

#[tokio::test]
async fn oversized_weighted_entry_is_rejected() {
    let cache = Cache::<i32>::builder().max_weighted_capacity(1).build();
    cache
        .set_full(
            "oversized",
            1,
            Some(EntryOptions::default().with_size(1_000_000)),
            Box::from([]),
        )
        .await;
    cache.run_pending_tasks().await;
    assert!(!cache.read("oversized", None).await.unwrap().has_value());
}

#[tokio::test]
async fn first_admitted_never_remove_entry_survives_capacity_pressure() {
    let cache = Cache::<i32>::builder().max_capacity(1).build();
    let pinned = EntryOptions::default().with_priority(Priority::NeverRemove);
    for i in 0..20 {
        cache
            .set_full(
                format!("pinned-{i}"),
                i,
                Some(pinned.clone()),
                Box::from([]),
            )
            .await;
    }
    cache.run_pending_tasks().await;
    let mut retained = 0;
    for i in 0..20 {
        if cache.try_get(format!("pinned-{i}"), None).await.has_value() {
            retained += 1;
        }
    }
    assert_eq!(
        retained, 1,
        "pinned entries still consume the configured capacity"
    );
    assert_eq!(cache.try_get("pinned-0", None).await.value(), Some(&0));
}

#[tokio::test]
async fn auto_clone_isolates_mutable_arc_at_public_boundaries() {
    struct AtomicCloner;
    impl amalgam::ValueCloner<Arc<AtomicI32>> for AtomicCloner {
        fn clone_value(
            &self,
            value: &Arc<AtomicI32>,
        ) -> std::result::Result<Arc<AtomicI32>, amalgam::CloneError> {
            Ok(Arc::new(AtomicI32::new(value.load(Ordering::SeqCst))))
        }
    }
    let options = EntryOptions::default().with_enable_auto_clone(true);
    let cache = Cache::<Arc<AtomicI32>>::builder()
        .default_options(options)
        .value_cloner(Arc::new(AtomicCloner))
        .build();
    cache
        .set("shared-mutable", Arc::new(AtomicI32::new(1)))
        .await
        .unwrap();
    let returned = cache.try_get("shared-mutable", None).await;
    returned.value().unwrap().store(2, Ordering::SeqCst);
    assert_eq!(
        cache
            .try_get("shared-mutable", None)
            .await
            .value()
            .unwrap()
            .load(Ordering::SeqCst),
        1,
        "mutating a returned clone must not change the stored graph"
    );
}

#[derive(Default)]
struct Observer {
    misses: AtomicUsize,
    evictions: AtomicUsize,
}

impl Plugin for Observer {
    fn name(&self) -> &str {
        "audit-observer"
    }
    fn on_event(&self, event: &CacheEvent) {
        match event {
            CacheEvent::Miss { .. } => {
                self.misses.fetch_add(1, Ordering::SeqCst);
            }
            CacheEvent::Eviction { .. } => {
                self.evictions.fetch_add(1, Ordering::SeqCst);
            }
            _ => {}
        }
    }
}

#[tokio::test]
async fn cold_get_or_set_emits_one_logical_miss() {
    let observer = Arc::new(Observer::default());
    let cache = Cache::<i32>::builder().plugin(observer.clone()).build();
    cache
        .get_or_set("cold", amalgam::source::value(1))
        .await
        .unwrap();
    assert_eq!(observer.misses.load(Ordering::SeqCst), 1);
    cache.try_get("absent", None).await;
    assert_eq!(
        observer.misses.load(Ordering::SeqCst),
        2,
        "factory and read-only misses reach the same observer"
    );
}

#[tokio::test]
async fn eviction_events_reach_plugins_exactly_once() {
    let observer = Arc::new(Observer::default());
    let cache = Cache::<i32>::builder()
        .max_capacity(1)
        .plugin(observer.clone())
        .build();
    let mut events = cache.events().subscribe();
    for i in 0..20 {
        cache.set(format!("key-{i}"), i).await.unwrap();
    }
    cache.run_pending_tasks().await;
    let mut hub_evictions = 0;
    while let Ok(event) = events.try_recv() {
        if matches!(event, CacheEvent::Eviction { .. }) {
            hub_evictions += 1;
        }
    }
    assert!(
        hub_evictions > 0,
        "the cache emitted actual eviction events"
    );
    assert_eq!(
        observer.evictions.load(Ordering::SeqCst),
        hub_evictions,
        "every eviction reaches the plugin exactly once"
    );
}

#[tokio::test]
async fn factory_error_source_survives_cache_boundary() {
    let cache = Cache::<i32>::new();
    let error = cache
        .get_or_set(
            "fail",
            amalgam::source::factory(|_ctx| async move {
                Err(FactoryError::from_source(std::io::Error::other(
                    "upstream I/O failure",
                )))
            }),
        )
        .await
        .unwrap_err();
    let wrapped = std::error::Error::source(&error).expect("factory wrapper");
    let original = wrapped.source().expect("original origin failure");
    assert_eq!(original.to_string(), "upstream I/O failure");
}

#[tokio::test]
async fn corrupt_l2_entry_does_not_trip_transport_circuit() {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::default());
    let l2: Arc<dyn DistributedCache> = Arc::new(InMemoryDistributedCache::new(clock.clone()));
    let build = || {
        Cache::<i32>::builder()
            .clock(clock.clone())
            .distributed(l2.clone())
            .serializer(Arc::new(JsonSerializer))
            .auto_recovery(no_recovery())
    };
    let writer = build().build();
    writer.set("healthy-key", 42).await.unwrap();
    l2.set(
        "v2:corrupt-key",
        b"not valid json".to_vec(),
        Some(Duration::from_secs(60)),
    )
    .await
    .unwrap();
    let reader = build()
        .distributed_circuit_breaker(Duration::from_secs(3600))
        .default_options(EntryOptions::default().with_rethrow_serialization_exceptions(false))
        .build();
    assert_eq!(
        reader
            .get_or_set("corrupt-key", amalgam::source::value(1))
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        reader
            .get_or_set("healthy-key", amalgam::source::value(999))
            .await
            .unwrap(),
        42,
        "a value-specific codec error must preserve unrelated healthy reads"
    );
    assert_eq!(
        build()
            .build()
            .get_or_set("healthy-key", amalgam::source::value(-1))
            .await
            .unwrap(),
        42,
        "the distributed cache was healthy and kept the unrelated value intact"
    );
}

#[tokio::test]
async fn missing_serializer_is_a_typed_build_rejection() {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::default());
    let l2: Arc<dyn DistributedCache> = Arc::new(InMemoryDistributedCache::new(clock.clone()));
    let result = Cache::<i32>::builder()
        .clock(clock)
        .distributed(l2)
        .auto_recovery(no_recovery())
        .try_build();
    assert!(matches!(
        result,
        Err(amalgam::Error::Config(
            amalgam::ConfigError::DistributedWithoutSerializer
        ))
    ));
}

#[tokio::test]
async fn resilient_observer_survives_transient_lag() {
    let cache = Cache::<i32>::builder().events_capacity(1).build();
    let mut events = cache.events().subscribe_resilient();
    for i in 0..10 {
        cache
            .try_set(format!("k-{i}"), i)
            .await
            .unwrap()
            .wait()
            .await
            .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(1), events.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(events.lost_events() > 0);
    cache
        .try_set("after-lag", 11)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if matches!(
                events.recv().await.unwrap(),
                CacheEvent::OperationCompleted { .. }
            ) {
                break;
            }
        }
    })
    .await
    .unwrap();
}

#[cfg(feature = "metrics")]
#[tokio::test]
async fn metrics_include_factory_misses_and_distinct_cache_labels() {
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _local = metrics::set_default_local_recorder(&recorder);
    let a = Cache::<i32>::builder()
        .name("alpha")
        .plugin(Arc::new(amalgam::MetricsPlugin::new()))
        .build();
    let b = Cache::<i32>::builder()
        .name("beta")
        .plugin(Arc::new(amalgam::MetricsPlugin::new()))
        .build();
    for cache in [a, b] {
        cache
            .get_or_set("k", amalgam::source::value(1))
            .await
            .unwrap(); // Actual cache miss.
        cache
            .get_or_set("k", amalgam::source::value(2))
            .await
            .unwrap(); // Actual cache hit.
    }
    let exposition = handle.render();
    assert!(
        exposition.contains("amalgam_misses_total"),
        "real factory misses must be counted"
    );
    assert!(exposition.contains("amalgam_hits_total"));
    assert!(
        exposition.contains("alpha") && exposition.contains("beta"),
        "distinct caches must retain finite cache labels"
    );
}

#[cfg(feature = "opentelemetry")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn otlp_failed_init_preserves_global_provider() {
    use opentelemetry::trace::{Span, Tracer};
    use tracing_subscriber::util::SubscriberInitExt;

    opentelemetry::global::set_tracer_provider(
        opentelemetry::trace::noop::NoopTracerProvider::new(),
    );
    assert!(
        !opentelemetry::global::tracer("before")
            .start("probe-before")
            .span_context()
            .is_valid()
    );
    tracing_subscriber::registry().try_init().unwrap(); // Application already owns tracing setup.
    let result = amalgam::init_otlp("audit-service", "http://127.0.0.1:1");
    assert!(
        result.is_err(),
        "the existing subscriber causes initialization to fail"
    );
    let span = opentelemetry::global::tracer("after").start("probe-after");
    assert!(
        !span.span_context().is_valid(),
        "failed initialization must leave the global provider intact"
    );
    drop(span);
    opentelemetry::global::set_tracer_provider(
        opentelemetry::trace::noop::NoopTracerProvider::new(),
    );
}
