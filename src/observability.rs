//! Metrics, exposed as a [`Plugin`](crate::plugins::Plugin) (feature `metrics`).
//!
//! `MetricsPlugin` records counters via the [`metrics`](https://docs.rs/metrics)
//! facade, so any compatible exporter (Prometheus, OTLP, …) can surface them.
//! Attach it with `Cache::builder().plugin(Arc::new(MetricsPlugin::new()))`.

#[cfg(feature = "metrics")]
mod imp {
    use crate::events::CacheEvent;
    use crate::plugins::Plugin;

    /// Records cache metrics via the `metrics` facade.
    #[derive(Debug)]
    pub struct MetricsPlugin {
        name: String,
    }

    impl MetricsPlugin {
        /// Creates the plugin.
        #[must_use]
        pub fn new() -> Self {
            Self {
                name: "amalgam-metrics".to_owned(),
            }
        }
    }

    impl Default for MetricsPlugin {
        fn default() -> Self {
            Self::new()
        }
    }

    impl Plugin for MetricsPlugin {
        fn name(&self) -> &str {
            &self.name
        }

        fn on_event(&self, event: &CacheEvent) {
            match event {
                CacheEvent::Hit { stale, .. } => {
                    if *stale {
                        metrics::counter!("amalgam_hits_stale_total").increment(1);
                    } else {
                        metrics::counter!("amalgam_hits_total").increment(1);
                    }
                }
                CacheEvent::Miss { .. } => metrics::counter!("amalgam_misses_total").increment(1),
                CacheEvent::Set { .. } => metrics::counter!("amalgam_sets_total").increment(1),
                CacheEvent::FactoryError { .. } => {
                    metrics::counter!("amalgam_factory_errors_total").increment(1);
                }
                CacheEvent::FailSafeActivate { .. } => {
                    metrics::counter!("amalgam_failsafe_activations_total").increment(1);
                }
                CacheEvent::FactorySyntheticTimeout { .. } => {
                    metrics::counter!("amalgam_factory_timeouts_total").increment(1);
                }
                CacheEvent::EagerRefresh { .. } => {
                    metrics::counter!("amalgam_eager_refreshes_total").increment(1);
                }
                _ => {}
            }
        }
    }
}

#[cfg(feature = "metrics")]
pub use imp::MetricsPlugin;
