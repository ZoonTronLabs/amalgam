//! Prometheus-metrics scrape example.
//!
//! Attaches [`amalgam::MetricsPlugin`] to a cache, then runs a workload so the
//! `amalgam_*` counters move, and exposes them on a `/metrics` HTTP endpoint via
//! [`metrics_exporter_prometheus::PrometheusBuilder`] that a Prometheus server
//! can scrape. Needs the `metrics` feature:
//!
//! ```text
//! # 1. start Prometheus (it scrapes host.docker.internal:9000 — see prometheus.yml)
//! docker compose up -d prometheus
//!
//! # 2. run this example; it serves metrics on 0.0.0.0:9000
//! cargo run --example prometheus --features metrics
//!
//! # 3. inspect the live metrics, or open Prometheus at http://localhost:9090
//! curl http://127.0.0.1:9000/metrics
//! ```

#[cfg(feature = "metrics")]
mod demo {
    use std::sync::Arc;
    use std::time::Duration;

    use amalgam::{Cache, MetricsPlugin};
    use metrics_exporter_prometheus::PrometheusBuilder;

    /// Address the scrape endpoint binds to. `0.0.0.0` (not `127.0.0.1`) so the
    /// Prometheus container can reach it from outside the host's loopback.
    const LISTEN_ADDR: ([u8; 4], u16) = ([0, 0, 0, 0], 9000);

    pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
        // Install the Prometheus recorder as the global `metrics` recorder and
        // spawn its HTTP scrape server. `install()` detects the current Tokio
        // runtime (we're inside `#[tokio::main]`) and spawns the listener onto
        // it; with the default `http-listener` feature it serves the standard
        // Prometheus exposition format at `/metrics` on the bound address.
        PrometheusBuilder::new()
            .with_http_listener(LISTEN_ADDR)
            .install()?;

        println!("metrics exposed at http://127.0.0.1:9000/metrics");
        println!(
            "Prometheus (docker compose) scrapes host.docker.internal:9000 — UI: http://localhost:9090"
        );
        println!();

        // The MetricsPlugin translates every CacheEvent into an `amalgam_*`
        // counter (amalgam_hits_total, amalgam_misses_total, amalgam_sets_total,
        // amalgam_hits_stale_total, …) via the `metrics` facade we just wired up.
        let cache: Cache<String> = Cache::builder()
            .plugin(Arc::new(MetricsPlugin::new()))
            .build();

        // Bounded so the example terminates on its own; press Ctrl-C to stop
        // earlier. Each iteration produces a mix of misses, hits, sets and
        // removes so several different counters advance and are visible to a
        // scrape between iterations.
        const ITERATIONS: u32 = 120;
        println!("running a {ITERATIONS}-iteration workload (Ctrl-C to stop early) …");

        for i in 0..ITERATIONS {
            // Cycle over a small key space so the same keys are first a miss
            // (factory runs) and then hits (served from L1).
            let key = format!("item-{}", i % 4);

            // get_or_set: miss on first touch of a key (counter: misses + sets),
            // hit afterwards (counter: hits).
            let _ = cache
                .get_or_set(key.clone(), move |ctx| async move {
                    Ok(ctx.value(format!("value-for-{key}")))
                })
                .await?;

            // An explicit set every few iterations (counter: sets).
            if i % 5 == 0 {
                cache
                    .set(format!("explicit-{i}"), "set-directly".to_owned())
                    .await;
            }

            // A remove every few iterations so the keys churn and re-miss later.
            if i % 8 == 0 {
                cache.remove(format!("item-{}", i % 4)).await;
            }

            if i % 20 == 0 {
                println!("  iteration {i}: counters advancing — scrape /metrics to watch them");
            }

            // Pace the loop so a scrape (Prometheus interval ~5s) lands between
            // steps and sees the counters at rest, rather than spinning the CPU.
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        println!();
        println!("workload complete. The recorder stays installed; the process now");
        println!("idles so a final scrape can read the totals. Press Ctrl-C to exit.");

        // Keep the scrape endpoint alive after the workload so the last values
        // can still be collected. Bounded so the example is not a true infinite
        // loop; Ctrl-C exits immediately.
        for _ in 0..60 {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() {
    #[cfg(feature = "metrics")]
    if let Err(err) = demo::run().await {
        eprintln!("error: {err}");
    }

    #[cfg(not(feature = "metrics"))]
    {
        eprintln!("This example requires the `metrics` feature:");
        eprintln!("  cargo run --example prometheus --features metrics");
    }
}
