//! Feature-gated diagnostics contracts. Global initialization runs in a child
//! test process so unrelated application/test subscribers are never replaced.

#![cfg(any(feature = "metrics", feature = "opentelemetry"))]

#[cfg(feature = "metrics")]
#[test]
fn named_metrics_attribute_sessions_and_bound_historical_labels() {
    use amalgam::{
        CacheEvent, CacheLevel, CacheOperation, Events, MetricsPlugin, OperationOutcome, Plugin,
        PluginContext, advanced::CacheLabelBudget, advanced::PluginHost,
    };
    use std::collections::BTreeSet;
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::time::Duration;

    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _local = metrics::set_default_local_recorder(&recorder);
    let budget = Arc::new(CacheLabelBudget::with_limits(
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(8).unwrap(),
    ));
    let plugin: Arc<dyn Plugin> = Arc::new(MetricsPlugin::with_label_budget(budget.clone()));
    for (name, hits) in [("alpha", 2), ("beta", 1)] {
        let events = Events::default();
        let host = PluginHost::try_new(
            PluginContext::new(name, "private-instance", events.clone()).unwrap(),
            vec![plugin.clone()],
        )
        .unwrap();
        for _ in 0..hits {
            events.emit(CacheEvent::Hit {
                key: Arc::from("private-key"),
                stale: false,
            });
        }
        events.emit(CacheEvent::Miss {
            key: Arc::from("private-key"),
        });
        events.emit(CacheEvent::OperationCompleted {
            operation: CacheOperation::TryGet,
            outcome: OperationOutcome::Miss,
            elapsed: Duration::from_millis(1),
            level: Some(CacheLevel::Memory),
        });
        drop(host);
    }
    for index in 0..1_000 {
        let events = Events::default();
        let host = PluginHost::try_new(
            PluginContext::new(format!("cache-{index}"), "secret-instance", events.clone())
                .unwrap(),
            vec![plugin.clone()],
        )
        .unwrap();
        events.emit(CacheEvent::Miss {
            key: Arc::from(format!("secret-key-{index}")),
        });
        drop(host);
    }
    assert_eq!(budget.assigned_names(), 2);
    assert_eq!(&*budget.label_for("unreasonably-long-name"), "other");
    let rendered = handle.render();
    assert!(
        rendered
            .lines()
            .any(|line| line.starts_with("amalgam_hits_total{")
                && line.contains("cache_name=\"alpha\"")
                && line.ends_with(" 2"))
    );
    assert!(
        rendered
            .lines()
            .any(|line| line.starts_with("amalgam_hits_total{")
                && line.contains("cache_name=\"beta\"")
                && line.ends_with(" 1"))
    );
    assert!(
        rendered
            .lines()
            .any(|line| line.starts_with("amalgam_misses_total{")
                && line.contains("cache_name=\"other\"")
                && line.ends_with(" 1000"))
    );
    assert!(
        rendered
            .lines()
            .any(|line| line.starts_with("amalgam_operations_total{")
                && line.contains("operation=\"try_get\"")
                && line.contains("outcome=\"miss\"")
                && line.contains("level=\"memory\""))
    );
    assert!(rendered.contains("amalgam_operation_duration_seconds"));
    let labels: BTreeSet<_> = rendered
        .lines()
        .filter_map(|line| {
            line.split("cache_name=\"")
                .nth(1)
                .map(|value| value.split('"').next().unwrap())
        })
        .collect();
    assert_eq!(labels, BTreeSet::from(["alpha", "beta", "other"]));
    assert!(!rendered.contains("private-key"));
    assert!(!rendered.contains("secret-key"));
    assert!(!rendered.contains("instance"));
}

#[cfg(feature = "opentelemetry")]
mod tracing_contract {
    use amalgam::observability::{OperationObservation, component_span};
    use amalgam::{CacheLevel, CacheOperation, Events, OperationOutcome};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use tracing::field::{Field, Visit};
    use tracing::span::{Attributes, Id, Record};
    use tracing_subscriber::layer::{Context, SubscriberExt as _};
    use tracing_subscriber::{Layer, Registry};

    #[derive(Default)]
    struct Captured {
        names: Mutex<Vec<String>>,
        fields: Mutex<Vec<(String, String)>>,
        parents: Mutex<Vec<Option<Id>>>,
        events: AtomicUsize,
    }
    struct CaptureLayer(Arc<Captured>);
    struct Fields<'a>(&'a Captured);
    impl Visit for Fields<'_> {
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            self.0
                .fields
                .lock()
                .unwrap()
                .push((field.name().to_owned(), format!("{value:?}")));
        }
    }
    impl<S: tracing::Subscriber> Layer<S> for CaptureLayer {
        fn on_new_span(&self, attrs: &Attributes<'_>, _: &Id, ctx: Context<'_, S>) {
            self.0
                .names
                .lock()
                .unwrap()
                .push(attrs.metadata().name().to_owned());
            self.0.parents.lock().unwrap().push(
                attrs
                    .parent()
                    .cloned()
                    .or_else(|| ctx.current_span().id().cloned()),
            );
            attrs.record(&mut Fields(&self.0));
        }
        fn on_record(&self, _: &Id, values: &Record<'_>, _: Context<'_, S>) {
            values.record(&mut Fields(&self.0));
        }
        fn on_event(&self, _: &tracing::Event<'_>, _: Context<'_, S>) {
            self.0.events.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn logical_and_component_spans_keep_trace_identity_and_final_outcome() {
        let captured = Arc::new(Captured::default());
        let subscriber = Registry::default().with(CaptureLayer(captured.clone()));
        tracing::subscriber::with_default(subscriber, || {
            let observation = OperationObservation::new(
                Events::default(),
                "named-cache",
                "trace-instance",
                CacheOperation::TryGet,
                Some("trace-key"),
            );
            observation.span().in_scope(|| {
                let child = component_span(
                    "named-cache",
                    CacheLevel::Distributed,
                    "read",
                    Some("trace-key"),
                );
                child.in_scope(|| {});
            });
            observation.finish(OperationOutcome::Hit);
        });
        assert_eq!(
            *captured.names.lock().unwrap(),
            ["amalgam.operation", "amalgam.component"]
        );
        assert!(captured.parents.lock().unwrap()[1].is_some());
        let fields = captured.fields.lock().unwrap();
        for (name, value) in [
            ("cache_name", "named-cache"),
            ("instance_id", "trace-instance"),
            ("operation", "try_get"),
            ("key", "trace-key"),
            ("outcome", "hit"),
        ] {
            assert!(
                fields
                    .iter()
                    .any(|(field, recorded)| field == name && recorded.contains(value)),
                "missing span field {name}"
            );
        }
    }

    #[test]
    fn otlp_requires_runtime_deliberately_and_validates_identity() {
        assert!(matches!(
            amalgam::advanced::otlp_layer::<Registry>("service", "http://127.0.0.1:4317"),
            Err(amalgam::advanced::OtelInitError::MissingRuntime)
        ));
        assert!(matches!(
            amalgam::advanced::otlp_layer::<Registry>("  ", "http://127.0.0.1:4317"),
            Err(amalgam::advanced::OtelInitError::BlankServiceName)
        ));
    }

    #[tokio::test]
    async fn composable_otlp_layer_preserves_application_subscriber_and_globals() {
        use opentelemetry::trace::{Span as _, Tracer as _};
        let before = opentelemetry::global::tracer("before")
            .start("before")
            .span_context()
            .is_valid();
        let captured = Arc::new(Captured::default());
        let (layer, guard) =
            amalgam::advanced::otlp_layer("composed-service", "http://127.0.0.1:4317").unwrap();
        let subscriber = Registry::default()
            .with(CaptureLayer(captured.clone()))
            .with(layer);
        tracing::subscriber::with_default(subscriber, || tracing::debug!("application event"));
        assert_eq!(captured.events.load(Ordering::SeqCst), 1);
        let after = opentelemetry::global::tracer("after")
            .start("after")
            .span_context()
            .is_valid();
        assert_eq!(after, before);
        guard.shutdown().unwrap();
    }

    #[tokio::test]
    async fn failed_otlp_initialization_retains_original_global_provider() {
        const CHILD: &str = "AMALGAM_FOUNDATION_OTEL_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let result = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "tracing_contract::failed_otlp_initialization_retains_original_global_provider",
                    "--nocapture",
                ])
                .env(CHILD, "1")
                .output()
                .unwrap();
            assert!(
                result.status.success(),
                "child failed: {}",
                String::from_utf8_lossy(&result.stderr)
            );
            return;
        }
        use opentelemetry::trace::{Span as _, SpanId, TraceId, Tracer as _};
        #[derive(Debug)]
        struct FixedIds;
        impl opentelemetry_sdk::trace::IdGenerator for FixedIds {
            fn new_trace_id(&self) -> TraceId {
                TraceId::from(42_u128)
            }
            fn new_span_id(&self) -> SpanId {
                SpanId::from(7_u64)
            }
        }
        let original = opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_id_generator(FixedIds)
            .build();
        opentelemetry::global::set_tracer_provider(original.clone());
        tracing::subscriber::set_global_default(Registry::default()).unwrap();
        assert!(matches!(
            amalgam::advanced::try_init_otlp("rejected", "http://127.0.0.1:4317"),
            Err(amalgam::advanced::OtelInitError::Subscriber(_))
        ));
        let span = opentelemetry::global::tracer("verify-original").start("verify-original");
        assert_eq!(span.span_context().trace_id(), TraceId::from(42_u128));
        drop(span);
        original.shutdown().unwrap();
    }
}
