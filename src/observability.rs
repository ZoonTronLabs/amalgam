//! Operation spans and optional bounded cache-name metrics.

use std::time::{Duration, Instant};

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

#[derive(Clone, Copy)]
enum OperationTiming {
    Unobserved,
    Measured(Instant),
}
impl OperationTiming {
    fn start(events: &Events) -> Self {
        if events.observes_operations() {
            Self::Measured(Instant::now())
        } else {
            Self::Unobserved
        }
    }
    fn elapsed(self) -> Duration {
        match self {
            Self::Unobserved => Duration::ZERO,
            Self::Measured(started) => started.elapsed(),
        }
    }
}

// Ownership of the event source differs, but completion and unwind semantics
// are shared so an inline-to-owned handoff remains one logical observation.
struct Observation {
    operation: CacheOperation,
    level: Option<CacheLevel>,
    timing: OperationTiming,
    span: tracing::Span,
    state: ObservationState,
}
impl Observation {
    fn new(
        events: &Events,
        cache_name: &str,
        instance_id: &str,
        operation: CacheOperation,
        key: Option<&str>,
    ) -> Self {
        events.operation_started(operation);
        Self {
            operation,
            level: None,
            timing: OperationTiming::start(events),
            span: operation_span(cache_name, instance_id, operation, key),
            state: ObservationState::Pending,
        }
    }
    fn complete(&mut self, events: &Events, outcome: OperationOutcome) {
        self.state = ObservationState::Completed;
        self.span.record("outcome", outcome.as_str());
        events.emit_lazy(|| CacheEvent::OperationCompleted {
            operation: self.operation,
            outcome,
            elapsed: self.timing.elapsed(),
            level: self.level,
        });
    }
    fn finish_on_drop(&mut self, events: &Events) {
        match self.state {
            ObservationState::Pending => self.complete(
                events,
                if std::thread::panicking() {
                    OperationOutcome::Panicked
                } else {
                    OperationOutcome::Cancelled
                },
            ),
            ObservationState::Completed => {}
        }
    }
    fn transfer(&mut self) -> Self {
        let observation = Self {
            operation: self.operation,
            level: self.level,
            timing: self.timing,
            span: self.span.clone(),
            state: ObservationState::Pending,
        };
        self.state = ObservationState::Completed;
        observation
    }
}

/// Records exactly one completion, including cancellation when an operation
/// future is dropped. Orchestration explicitly records normal/error outcomes.
#[must_use = "dropping an unfinished observation records cancellation"]
pub struct OperationObservation {
    events: Events,
    observation: Observation,
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
        let observation = Observation::new(&events, cache_name, instance_id, operation, key);
        Self {
            events,
            observation,
        }
    }
    /// The span to instrument this operation's future.
    #[must_use]
    pub fn span(&self) -> tracing::Span {
        self.observation.span.clone()
    }
    /// Selects the servicing level where one level describes the final outcome.
    pub fn set_level(&mut self, level: CacheLevel) {
        self.observation.level = Some(level);
    }
    /// Finishes once with an explicit typed outcome.
    pub fn finish(mut self, outcome: OperationOutcome) {
        self.observation.complete(&self.events, outcome);
    }
}
impl Drop for OperationObservation {
    fn drop(&mut self) {
        self.observation.finish_on_drop(&self.events);
    }
}

/// Inline observation borrows the event hub; ownership is acquired only when
/// work actually leaves the ready lookup path.
#[must_use = "dropping an unfinished observation records cancellation"]
pub(crate) struct ReadyObservation<'a> {
    events: &'a Events,
    observation: Observation,
}
impl<'a> ReadyObservation<'a> {
    pub(crate) fn new(
        events: &'a Events,
        cache_name: &str,
        instance_id: &str,
        operation: CacheOperation,
        key: Option<&str>,
    ) -> Self {
        Self {
            events,
            observation: Observation::new(events, cache_name, instance_id, operation, key),
        }
    }
    pub(crate) fn span(&self) -> tracing::Span {
        self.observation.span.clone()
    }
    pub(crate) fn set_level(&mut self, level: CacheLevel) {
        self.observation.level = Some(level);
    }
    pub(crate) fn finish(mut self, outcome: OperationOutcome) {
        self.observation.complete(self.events, outcome);
    }
    pub(crate) fn into_owned(mut self) -> OperationObservation {
        OperationObservation {
            events: self.events.clone(),
            observation: self.observation.transfer(),
        }
    }
}
impl Drop for ReadyObservation<'_> {
    fn drop(&mut self) {
        self.observation.finish_on_drop(self.events);
    }
}

// A plain, untraced lookup has no span or timer state to transport. Late
// recipients still observe its one terminal outcome, including unwind.
pub(crate) struct QuietObservation<'a> {
    events: &'a Events,
    operation: CacheOperation,
    state: ObservationState,
}
impl<'a> QuietObservation<'a> {
    pub(crate) fn events(&self) -> &'a Events {
        self.events
    }
    pub(crate) fn new(events: &'a Events, operation: CacheOperation) -> Self {
        Self {
            events,
            operation,
            state: ObservationState::Pending,
        }
    }
    pub(crate) fn finish(mut self, outcome: OperationOutcome, level: Option<CacheLevel>) {
        self.state = ObservationState::Completed;
        self.events.emit_lazy(|| CacheEvent::OperationCompleted {
            operation: self.operation,
            outcome,
            elapsed: Duration::ZERO,
            level,
        });
    }
    /// Preserve one unobserved operation while a miss selects its origin plan.
    pub(crate) fn into_ready(mut self) -> ReadyObservation<'a> {
        self.state = ObservationState::Completed;
        ReadyObservation {
            events: self.events,
            observation: Observation {
                operation: self.operation,
                level: None,
                timing: OperationTiming::Unobserved,
                span: tracing::Span::none(),
                state: ObservationState::Pending,
            },
        }
    }
    pub(crate) fn into_owned(mut self) -> OperationObservation {
        self.state = ObservationState::Completed;
        OperationObservation {
            events: self.events.clone(),
            observation: Observation {
                operation: self.operation,
                level: None,
                timing: OperationTiming::Unobserved,
                span: tracing::Span::none(),
                state: ObservationState::Pending,
            },
        }
    }
}
impl Drop for QuietObservation<'_> {
    fn drop(&mut self) {
        match self.state {
            ObservationState::Pending => self.events.emit_lazy(|| CacheEvent::OperationCompleted {
                operation: self.operation,
                outcome: if std::thread::panicking() {
                    OperationOutcome::Panicked
                } else {
                    OperationOutcome::Cancelled
                },
                elapsed: Duration::ZERO,
                level: None,
            }),
            ObservationState::Completed => {}
        }
    }
}

#[cfg(feature = "metrics")]
mod imp {
    use super::labels::{CacheLabelBudget, process_budget};
    use std::sync::{Arc, OnceLock};

    use crate::events::CacheEvent;
    use crate::plugins::{Plugin, PluginContext, PluginError, PluginSession};

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
                CacheEvent::MarkerEagerRefresh { .. } => {
                    metrics::counter!("amalgam_marker_eager_refresh_total", "cache_name" => self.label.to_string()).increment(1);
                    None
                }
                CacheEvent::MarkerSnapshotWrite { outcome, .. } => {
                    let outcome = match outcome {
                        crate::MarkerSnapshotWriteOutcome::Stored => "stored",
                        crate::MarkerSnapshotWriteOutcome::KeptNewer => "kept_newer",
                        crate::MarkerSnapshotWriteOutcome::Expired => "expired",
                        crate::MarkerSnapshotWriteOutcome::BackendFailure => "backend_failure",
                        crate::MarkerSnapshotWriteOutcome::ProtocolFailure => "protocol_failure",
                    };
                    metrics::counter!("amalgam_marker_snapshot_writes_total", "cache_name" => self.label.to_string(), "outcome" => outcome).increment(1);
                    None
                }
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
pub use imp::MetricsPlugin;

#[cfg(any(feature = "metrics", feature = "opentelemetry"))]
mod labels;
#[cfg(any(feature = "metrics", feature = "opentelemetry"))]
pub use labels::CacheLabelBudget;
#[cfg(feature = "opentelemetry")]
mod otel_metrics;
#[cfg(feature = "opentelemetry")]
pub use otel_metrics::{MetricTags, OtelMetricMeters, OtelMetricsPlugin};
