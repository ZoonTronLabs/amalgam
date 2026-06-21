//! OpenTelemetry span-export example.
//!
//! Exports `amalgam`'s `tracing` spans (`amalgam.get_or_set`, …) to an OTLP
//! collector over gRPC. Needs the `opentelemetry` feature and a running
//! collector / Jaeger:
//!
//! ```text
//! # All-in-one Jaeger with an OTLP gRPC receiver on :4317 and UI on :16686
//! docker run --rm -p 16686:16686 -p 4317:4317 jaegertracing/all-in-one:latest
//!
//! cargo run --example otel --features opentelemetry
//! # then open http://127.0.0.1:16686 and pick the "amalgam-example" service
//! ```
//!
//! Set `OTEL_EXPORTER_OTLP_ENDPOINT` to point at a different collector.

#[cfg(feature = "opentelemetry")]
mod demo {
    use amalgam::Cache;

    const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:4317";

    pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
        let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
            .unwrap_or_else(|_| DEFAULT_ENDPOINT.to_owned());
        println!("exporting spans to {endpoint} …");

        // Keep the guard alive until the end of `run`; dropping it flushes spans.
        let _otel = amalgam::otel::init_otlp("amalgam-example", &endpoint)?;

        let cache: Cache<String> = Cache::builder().build();

        // Each of these cache operations emits `tracing` spans that are bridged
        // to OpenTelemetry and exported to the collector.
        let first = cache
            .get_or_set("greeting", |ctx| async move {
                println!("  factory ran (cache miss)");
                Ok(ctx.value("hello from an OpenTelemetry-traced amalgam".to_owned()))
            })
            .await?;
        println!("get_or_set #1 → {first}");

        let again = cache
            .get_or_set("greeting", |ctx| async move {
                println!("  (should NOT print — served from cache)");
                Ok(ctx.value("unused".to_owned()))
            })
            .await?;
        println!("get_or_set #2 → {again}");

        cache.set("farewell", "goodbye".to_owned()).await;
        println!("set farewell");

        cache.remove("greeting").await;
        println!("removed greeting");

        println!();
        println!("spans flushed on exit. View them in your collector UI,");
        println!("e.g. Jaeger at http://127.0.0.1:16686 (service: amalgam-example).");
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    #[cfg(feature = "opentelemetry")]
    if let Err(err) = demo::run().await {
        eprintln!("error: {err}");
    }

    #[cfg(not(feature = "opentelemetry"))]
    {
        eprintln!("This example requires the `opentelemetry` feature:");
        eprintln!("  cargo run --example otel --features opentelemetry");
    }
}
