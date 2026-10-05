//! Operation spans and optional bounded cache-name metrics.

use std::time::Instant;

use crate::events::{CacheEvent, CacheLevel, CacheOperation, Events, OperationOutcome};

/// Creates the logical operation span. Keys/instance IDs belong to traces only.
#[must_use]
pub fn operation_span(
    cache_name: &str,
    instance_id: &str,
    operation: CacheOperation,
    key: Option<&str>,
) -> tracing::Span {
    tracing::debug_span!(
        "amalgam.operation",
        cache_name,
        instance_id,
        operation = operation.as_str(),
        key,
        outcome = tracing::field::Empty
    )
}

/// Creates a child span for a cache component. Instrument the awaited/background
/// future with this span rather than holding an entered guard across an await.
#[must_use]
pub fn component_span(
    cache_name: &str,
    level: CacheLevel,
    action: &'static str,
    key: Option<&str>,
) -> tracing::Span {
    tracing::debug_span!(
        "amalgam.component",
        cache_name,
        level = level.as_str(),
        action,
        key
    )
}

enum ObservationState {
    Pending,
    Completed,
}

/// Records exactly one completion, including cancellation when an operation
/// future is dropped. Orchestration explicitly records normal/error outcomes.
#[must_use = "dropping an unfinished observation records cancellation"]
pub struct OperationObservation {
    events: Events,
    operation: CacheOperation,
    level: Option<CacheLevel>,
    started: Instant,
    span: tracing::Span,
    state: ObservationState,
}

impl OperationObservation {
    /// Begins observing a single logical operation.
    pub fn new(
        events: Events,
        cache_name: &str,
        instance_id: &str,
        operation: CacheOperation,
        key: Option<&str>,
    ) -> Self {
        Self {
            events,
            operation,
            level: None,
            started: Instant::now(),
            span: operation_span(cache_name, instance_id, operation, key),
            state: ObservationState::Pending,
        }
    }

    /// The span to instrument this operation's future.
    #[must_use]
    pub fn span(&self) -> tracing::Span {
        self.span.clone()
    }

    /// Selects the servicing level where one level describes the final outcome.
    pub fn set_level(&mut self, level: CacheLevel) {
        self.level = Some(level);
    }

    /// Finishes once with an explicit typed outcome.
    pub fn finish(mut self, outcome: OperationOutcome) {
        self.complete(outcome);
    }

    fn complete(&mut self, outcome: OperationOutcome) {
        self.state = ObservationState::Completed;
        self.span.record("outcome", outcome.as_str());
        self.events.emit(CacheEvent::OperationCompleted {
            operation: self.operation,
            outcome,
            elapsed: self.started.elapsed(),
            level: self.level,
        });
    }
}

impl Drop for OperationObservation {
    fn drop(&mut self) {
        match self.state {
            ObservationState::Pending => self.complete(if std::thread::panicking() {
                OperationOutcome::Panicked
            } else {
                OperationOutcome::Cancelled
            }),
            ObservationState::Completed => {}
        }
    }
}

#[cfg(feature = "metrics")]
mod imp {
    use std::collections::HashSet;
    use std::num::NonZeroUsize;
    use std::sync::{Arc, Mutex, OnceLock};

    use crate::events::CacheEvent;
    use crate::plugins::{Plugin, PluginContext, PluginError, PluginSession};

    /// A shared, historical cache-name label budget. Labels are never released:
    /// repeatedly creating/dropping caches cannot grow exporter cardinality.
    ///
    /// MetricsPlugin::new uses a process-wide budget of 128 names, each at most
    /// 64 bytes. Overflow/long names aggregate under the label "other".
    #[derive(Debug)]
    pub struct CacheLabelBudget {
        max_names: NonZeroUsize,
        max_bytes: NonZeroUsize,
        names: Mutex<HashSet<Arc<str>>>,
        overflow: Arc<str>,
    }

    impl CacheLabelBudget {
        /// Creates a bounded catalog with a 64-byte maximum label.
        #[must_use]
        pub fn new(max_names: NonZeroUsize) -> Self {
            Self::with_limits(
                max_names,
                NonZeroUsize::new(64).unwrap_or(NonZeroUsize::MIN),
            )
        }

        /// Creates explicit count/byte limits. Share one catalog across plugins
        /// that report to the same exporter.
        #[must_use]
        pub fn with_limits(max_names: NonZeroUsize, max_bytes: NonZeroUsize) -> Self {
            Self {
                max_names,
                max_bytes,
                names: Mutex::new(HashSet::with_capacity(max_names.get().min(128))),
                overflow: Arc::from("other"),
            }
        }

        /// Allocates a label at attachment time, never on a cache-key hot path.
        #[must_use]
        pub fn label_for(&self, cache_name: &str) -> Arc<str> {
            if cache_name.len() > self.max_bytes.get() {
                return Arc::clone(&self.overflow);
            }
            let mut names = self
                .names
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(name) = names.get(cache_name) {
                return Arc::clone(name);
            }
            if names.len() >= self.max_names.get() {
                return Arc::clone(&self.overflow);
            }
            let label: Arc<str> = Arc::from(cache_name);
            names.insert(Arc::clone(&label));
            label
        }

        /// Number of historically assigned distinct named labels.
        #[must_use]
        pub fn assigned_names(&self) -> usize {
            self.names
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len()
        }
    }

    fn process_budget() -> Arc<CacheLabelBudget> {
        static BUDGET: OnceLock<Arc<CacheLabelBudget>> = OnceLock::new();
        Arc::clone(BUDGET.get_or_init(|| {
            Arc::new(CacheLabelBudget::new(
                NonZeroUsize::new(128).unwrap_or(NonZeroUsize::MIN),
            ))
        }))
    }

    /// Metrics facade plugin with independently attributed per-cache sessions.
    pub struct MetricsPlugin {
        name: String,
        budget: Arc<CacheLabelBudget>,
        legacy: OnceLock<MetricsSession>,
    }

    impl MetricsPlugin {
        /// Uses the shared process-wide cache-name budget.
        #[must_use]
        pub fn new() -> Self {
            Self {
                name: "amalgam-metrics".to_owned(),
                budget: process_budget(),
                legacy: OnceLock::new(),
            }
        }

        /// Uses an application-owned shared label catalog.
        #[must_use]
        pub fn with_label_budget(budget: Arc<CacheLabelBudget>) -> Self {
            Self {
                name: "amalgam-metrics".to_owned(),
                budget,
                legacy: OnceLock::new(),
            }
        }
    }

    impl Default for MetricsPlugin {
        fn default() -> Self {
            Self::new()
        }
    }

    impl std::fmt::Debug for MetricsPlugin {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("MetricsPlugin")
                .field("name", &self.name)
                .field("assigned_cache_names", &self.budget.assigned_names())
                .finish()
        }
    }

    impl Plugin for MetricsPlugin {
        fn name(&self) -> &str {
            &self.name
        }

        fn on_event(&self, event: &CacheEvent) {
            let session = self
                .legacy
                .get_or_init(|| MetricsSession::new(self.budget.label_for("amalgam")));
            if let Err(error) = session.on_event(event) {
                tracing::warn!(error = %error, "amalgam: metrics recording failed");
            }
        }

        fn attach(
            &self,
            context: &PluginContext,
        ) -> Result<Option<Box<dyn PluginSession>>, PluginError> {
            Ok(Some(Box::new(MetricsSession::new(
                self.budget.label_for(context.cache_name()),
            ))))
        }
    }

    struct EventCounters {
        hits: metrics::Counter,
        stale: metrics::Counter,
        misses: metrics::Counter,
        sets: metrics::Counter,
        removes: metrics::Counter,
        expires: metrics::Counter,
        factory_success: metrics::Counter,
        factory_errors: metrics::Counter,
        failsafe: metrics::Counter,
        timeouts: metrics::Counter,
        eager: metrics::Counter,
        background_success: metrics::Counter,
        background_errors: metrics::Counter,
        tags: metrics::Counter,
        clears: metrics::Counter,
        evictions: metrics::Counter,
        admissions_rejected: metrics::Counter,
        codec_encode: metrics::Counter,
        codec_decode: metrics::Counter,
        published: metrics::Counter,
        received: metrics::Counter,
        marker_reads: metrics::Counter,
        markers_received: metrics::Counter,
    }

    struct MetricsSession {
        label: Arc<str>,
        counters: EventCounters,
    }

    impl MetricsSession {
        fn new(label: Arc<str>) -> Self {
            let labels = vec![metrics::Label::new("cache_name", label.to_string())];
            let counters = EventCounters {
                hits: metrics::counter!("amalgam_hits_total", labels.clone()),
                stale: metrics::counter!("amalgam_hits_stale_total", labels.clone()),
                misses: metrics::counter!("amalgam_misses_total", labels.clone()),
                sets: metrics::counter!("amalgam_sets_total", labels.clone()),
                removes: metrics::counter!("amalgam_removes_total", labels.clone()),
                expires: metrics::counter!("amalgam_expires_total", labels.clone()),
                factory_success: metrics::counter!("amalgam_factory_success_total", labels.clone()),
                factory_errors: metrics::counter!("amalgam_factory_errors_total", labels.clone()),
                failsafe: metrics::counter!("amalgam_failsafe_activations_total", labels.clone()),
                timeouts: metrics::counter!("amalgam_factory_timeouts_total", labels.clone()),
                eager: metrics::counter!("amalgam_eager_refreshes_total", labels.clone()),
                background_success: metrics::counter!(
                    "amalgam_background_factory_success_total",
                    labels.clone()
                ),
                background_errors: metrics::counter!(
                    "amalgam_background_factory_errors_total",
                    labels.clone()
                ),
                tags: metrics::counter!("amalgam_tag_invalidations_total", labels.clone()),
                clears: metrics::counter!("amalgam_clears_total", labels.clone()),
                evictions: metrics::counter!("amalgam_evictions_total", labels.clone()),
                admissions_rejected: metrics::counter!(
                    "amalgam_memory_admissions_rejected_total",
                    labels.clone()
                ),
                codec_encode: metrics::counter!(
                    "amalgam_serialization_errors_total",
                    labels.clone()
                ),
                codec_decode: metrics::counter!(
                    "amalgam_deserialization_errors_total",
                    labels.clone()
                ),
                published: metrics::counter!("amalgam_messages_published_total", labels.clone()),
                received: metrics::counter!("amalgam_messages_received_total", labels.clone()),
                marker_reads: metrics::counter!("amalgam_marker_reads_total", labels.clone()),
                markers_received: metrics::counter!(
                    "amalgam_marker_messages_received_total",
                    labels
                ),
            };
            Self { label, counters }
        }
    }

    impl PluginSession for MetricsSession {
        fn on_event(&self, event: &CacheEvent) -> Result<(), PluginError> {
            let counter = match event {
                CacheEvent::MarkerRead { .. } => Some(&self.counters.marker_reads),
                CacheEvent::MarkerReceived { .. } => Some(&self.counters.markers_received),
                CacheEvent::Hit { stale: false, .. } => Some(&self.counters.hits),
                CacheEvent::Hit { stale: true, .. } => Some(&self.counters.stale),
                CacheEvent::Miss { .. } => Some(&self.counters.misses),
                CacheEvent::Set { .. } => Some(&self.counters.sets),
                CacheEvent::Remove { .. } => Some(&self.counters.removes),
                CacheEvent::Expire { .. } => Some(&self.counters.expires),
                CacheEvent::FactorySuccess { .. } => Some(&self.counters.factory_success),
                CacheEvent::FactoryError { .. } => Some(&self.counters.factory_errors),
                CacheEvent::FactorySyntheticTimeout { .. } => Some(&self.counters.timeouts),
                CacheEvent::FailSafeActivate { .. } => Some(&self.counters.failsafe),
                CacheEvent::EagerRefresh { .. } => Some(&self.counters.eager),
                CacheEvent::BackgroundFactorySuccess { .. } => {
                    Some(&self.counters.background_success)
                }
                CacheEvent::BackgroundFactoryError { .. }
                | CacheEvent::BackgroundCommitError { .. } => {
                    Some(&self.counters.background_errors)
                }
                CacheEvent::RemoveByTag { .. } => Some(&self.counters.tags),
                CacheEvent::Clear => Some(&self.counters.clears),
                CacheEvent::Eviction { .. } => Some(&self.counters.evictions),
                CacheEvent::MemoryAdmissionRejected { .. } => {
                    Some(&self.counters.admissions_rejected)
                }
                CacheEvent::SerializationError { .. } => Some(&self.counters.codec_encode),
                CacheEvent::DeserializationError { .. } => Some(&self.counters.codec_decode),
                CacheEvent::MessagePublished { .. } => Some(&self.counters.published),
                CacheEvent::MessageReceived { .. } => Some(&self.counters.received),
                CacheEvent::CircuitBreakerChange { component, closed } => {
                    let component = match component {
                        crate::CircuitComponent::Distributed => "distributed",
                        crate::CircuitComponent::Backplane => "backplane",
                    };
                    metrics::counter!("amalgam_circuit_transitions_total", "cache_name" => self.label.to_string(), "component" => component, "state" => if *closed { "closed" } else { "open" }).increment(1);
                    None
                }
                CacheEvent::OperationCompleted {
                    operation,
                    outcome,
                    elapsed,
                    level,
                } => {
                    let level = level.map_or("all", |level| level.as_str());
                    metrics::counter!("amalgam_operations_total", "cache_name" => self.label.to_string(), "operation" => operation.as_str(), "outcome" => outcome.as_str(), "level" => level).increment(1);
                    metrics::histogram!("amalgam_operation_duration_seconds", "cache_name" => self.label.to_string(), "operation" => operation.as_str(), "level" => level).record(elapsed.as_secs_f64());
                    None
                }
            };
            if let Some(counter) = counter {
                counter.increment(1);
            }
            Ok(())
        }
    }
}

#[cfg(feature = "metrics")]
pub use imp::{CacheLabelBudget, MetricsPlugin};
