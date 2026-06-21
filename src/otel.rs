//! OpenTelemetry tracing export (feature `opentelemetry`).
//!
//! `amalgam`'s core already emits [`tracing`] spans and events (for example
//! `amalgam.get_or_set`). This module wires those spans to an OTLP collector
//! such as [Jaeger] or the [OpenTelemetry Collector] so you can see them on a
//! distributed-tracing backend, with no changes to your cache code.
//!
//! Call [`init_otlp`] once at startup and keep the returned [`OtelGuard`] alive
//! for the lifetime of the process; dropping it flushes any buffered spans and
//! shuts the exporter down cleanly.
//!
//! ```no_run
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! // Keep the guard alive for as long as you want spans exported.
//! let _otel = amalgam::otel::init_otlp("my-service", "http://127.0.0.1:4317")?;
//!
//! let cache: amalgam::Cache<String> = amalgam::Cache::builder().build();
//! let _ = cache
//!     .get_or_set("k", |ctx| async move { Ok(ctx.value("v".to_owned())) })
//!     .await?;
//! // `_otel` is dropped here, flushing spans to the collector.
//! # Ok(())
//! # }
//! ```
//!
//! [Jaeger]: https://www.jaegertracing.io/
//! [OpenTelemetry Collector]: https://opentelemetry.io/docs/collector/

use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig as _;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;
use tracing_subscriber::{EnvFilter, fmt};

/// Default [`EnvFilter`] directives used when `RUST_LOG` is unset: everything at
/// `info`, but `amalgam`'s own spans/events down to `debug`.
const DEFAULT_FILTER: &str = "info,amalgam=debug";

/// Flushes and shuts down the OpenTelemetry tracer provider on drop.
///
/// Returned by [`init_otlp`]. Hold it for as long as you want spans exported —
/// typically for the whole program. When it drops, buffered spans are flushed
/// to the collector and the provider is shut down.
///
/// This type is `#[must_use]`: binding it to `_` would drop it immediately and
/// tear the exporter down before any spans are recorded. Bind it to a named
/// variable (for example `let _otel = init_otlp(..)?;`) instead.
#[must_use = "dropping the guard flushes and shuts down span export; bind it to a named variable to keep tracing alive"]
#[derive(Debug)]
pub struct OtelGuard {
    provider: SdkTracerProvider,
}

impl Drop for OtelGuard {
    fn drop(&mut self) {
        // Best-effort flush + shutdown: we are tearing down, so a failing
        // collector must not panic the process. Surface it on the tracing
        // pipeline that is still installed.
        if let Err(err) = self.provider.shutdown() {
            tracing::warn!(error = %err, "amalgam: OpenTelemetry tracer shutdown failed");
        }
    }
}

/// Initializes OTLP span export and installs a global `tracing` subscriber.
///
/// Builds an OTLP span exporter (gRPC/tonic) pointed at `endpoint`, wraps it in
/// a batching [`SdkTracerProvider`] whose [`Resource`] carries
/// `service.name = service_name`, sets that provider as the global
/// OpenTelemetry tracer provider, and installs a [`tracing_subscriber`]
/// [`Registry`](tracing_subscriber::Registry) composed of:
///
/// * a [`tracing_opentelemetry`] layer bridging `tracing` spans to OpenTelemetry,
/// * an [`EnvFilter`] (from `RUST_LOG`, defaulting to `info,amalgam=debug`), and
/// * a `fmt` layer for human-readable console output.
///
/// After this returns, every `tracing` span the crate emits (such as
/// `amalgam.get_or_set`) is exported to the collector at `endpoint`.
///
/// `endpoint` is a gRPC URL, for example `http://127.0.0.1:4317` (the default
/// OTLP/gRPC port).
///
/// Keep the returned [`OtelGuard`] alive for as long as you want spans exported;
/// see its documentation.
///
/// # Errors
///
/// Returns an error if the OTLP exporter cannot be built (for example an invalid
/// `endpoint`), or if a global `tracing` subscriber is already installed.
///
/// # Examples
///
/// ```no_run
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let _otel = amalgam::otel::init_otlp("amalgam-example", "http://127.0.0.1:4317")?;
/// # Ok(())
/// # }
/// ```
pub fn init_otlp(
    service_name: &str,
    endpoint: &str,
) -> Result<OtelGuard, Box<dyn std::error::Error>> {
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()?;

    let resource = Resource::builder()
        .with_service_name(service_name.to_owned())
        .build();

    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource)
        .build();

    let tracer = provider.tracer("amalgam");

    // Set the provider globally so any `opentelemetry`-aware code shares it, but
    // keep the original in the guard so we can shut it down later.
    opentelemetry::global::set_tracer_provider(provider.clone());

    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(DEFAULT_FILTER));

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer())
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .try_init()?;

    Ok(OtelGuard { provider })
}
