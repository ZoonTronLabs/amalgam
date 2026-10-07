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
//! let cache = amalgam::Cache::<String>::new();
//! let _ = cache
//!     .get_or_set("k", |_| async { Ok::<_, std::convert::Infallible>("v".to_owned()) })
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
use opentelemetry_sdk::trace::{SdkTracer, SdkTracerProvider};
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::registry::LookupSpan;
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
    state: OtelGuardState,
}

#[derive(Debug)]
enum OtelGuardState {
    Live(SdkTracerProvider),
    Shutdown,
}

/// A closed initialization/shutdown failure with its original source.
#[derive(Debug, thiserror::Error)]
pub enum OtelInitError {
    /// OTLP's Tokio transport needs an active runtime.
    #[error("OTLP transport requires a Tokio runtime")]
    MissingRuntime,
    /// A service must have a diagnostic identity.
    #[error("OpenTelemetry service name must not be blank")]
    BlankServiceName,
    /// The OTLP exporter configuration was rejected.
    #[error("OTLP exporter could not be built: {0}")]
    Exporter(#[from] opentelemetry_otlp::ExporterBuildError),
    /// Another subscriber already owns global tracing initialization.
    #[error("tracing subscriber could not be installed: {0}")]
    Subscriber(#[from] tracing_subscriber::util::TryInitError),
    /// Flushing/shutting down the owned provider failed.
    #[error("OpenTelemetry shutdown failed: {0}")]
    Shutdown(#[from] opentelemetry_sdk::error::OTelSdkError),
}

impl OtelGuard {
    /// Explicitly flushes/shuts down the owned provider with typed failure.
    /// The consumed guard cannot shut it down a second time during Drop.
    pub fn shutdown(mut self) -> Result<(), OtelInitError> {
        match std::mem::replace(&mut self.state, OtelGuardState::Shutdown) {
            OtelGuardState::Live(provider) => provider.shutdown().map_err(Into::into),
            OtelGuardState::Shutdown => Ok(()),
        }
    }

    fn publish_global(&self) {
        match &self.state {
            OtelGuardState::Live(provider) => {
                opentelemetry::global::set_tracer_provider(provider.clone());
            }
            OtelGuardState::Shutdown => unreachable!("a newly built guard owns its live provider"),
        }
    }
}

impl Drop for OtelGuard {
    fn drop(&mut self) {
        // Best-effort flush + shutdown: we are tearing down, so a failing
        // collector must not panic the process. Surface it on the tracing
        // pipeline that is still installed.
        if let OtelGuardState::Live(provider) = &self.state
            && let Err(err) = provider.shutdown()
        {
            tracing::warn!(error = %err, "amalgam: OpenTelemetry tracer shutdown failed");
        }
    }
}

/// Builds a composable OTLP layer and its resource guard without modifying
/// either global tracing or the global OpenTelemetry provider.
///
/// Attach the layer to the application's own subscriber and retain the guard.
/// Its generic subscriber type is inferred by the subsequent `with(layer)`.
pub fn otlp_layer<S>(
    service_name: &str,
    endpoint: &str,
) -> Result<
    (
        tracing_opentelemetry::OpenTelemetryLayer<S, SdkTracer>,
        OtelGuard,
    ),
    OtelInitError,
>
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
{
    if service_name.trim().is_empty() {
        return Err(OtelInitError::BlankServiceName);
    }
    if tokio::runtime::Handle::try_current().is_err() {
        return Err(OtelInitError::MissingRuntime);
    }
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
    let layer = tracing_opentelemetry::layer().with_tracer(tracer);
    Ok((
        layer,
        OtelGuard {
            state: OtelGuardState::Live(provider),
        },
    ))
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
    try_init_otlp(service_name, endpoint)
        .map_err(|error| Box::new(error) as Box<dyn std::error::Error>)
}

/// Transactionally installs a standalone OTLP subscriber. If installation is
/// rejected, the previous global provider remains intact and owned resources
/// are shut down. Applications with their own subscriber use [`otlp_layer`].
pub fn try_init_otlp(service_name: &str, endpoint: &str) -> Result<OtelGuard, OtelInitError> {
    let (layer, guard) = otlp_layer(service_name, endpoint)?;
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(DEFAULT_FILTER));

    let installed = tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer())
        .with(layer)
        .try_init();
    match installed {
        Err(error) => {
            if let Err(cleanup) = guard.shutdown() {
                tracing::warn!(error = %cleanup, "amalgam: rejected OTLP initialization cleanup failed");
            }
            Err(OtelInitError::Subscriber(error))
        }
        Ok(()) => {
            guard.publish_global();
            Ok(guard)
        }
    }
}

/// Builds an application-owned native OTLP metric provider without modifying
/// global tracing or the global meter provider. Use
/// [`crate::OtelMetricsPlugin::from_provider`] with this provider, then shut caches
/// down before flushing/shutting the provider down. With a current-thread Tokio
/// runtime, invoke the SDK's blocking `force_flush`/`shutdown` via `spawn_blocking`.
///
/// # Errors
/// Returns typed runtime, service identity or original exporter build failures.
///
/// ```no_run
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let provider = amalgam::otel::otlp_meter_provider("service", "http://127.0.0.1:4317")?;
/// let metrics = std::sync::Arc::new(amalgam::OtelMetricsPlugin::from_provider(&provider));
/// let cache = amalgam::Cache::<u64>::builder().plugin(metrics).build();
/// cache.set("key", 1).with_receipt().await?.wait().await?;
/// cache.shutdown().await?;
/// tokio::task::spawn_blocking(move || provider.shutdown()).await??;
/// # Ok(())
/// # }
/// ```
pub fn otlp_meter_provider(
    service_name: &str,
    endpoint: &str,
) -> Result<opentelemetry_sdk::metrics::SdkMeterProvider, OtelInitError> {
    if service_name.trim().is_empty() {
        return Err(OtelInitError::BlankServiceName);
    }
    if tokio::runtime::Handle::try_current().is_err() {
        return Err(OtelInitError::MissingRuntime);
    }
    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()?;
    let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(exporter).build();
    let resource = Resource::builder()
        .with_service_name(service_name.to_owned())
        .build();
    Ok(opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(resource)
        .build())
}
