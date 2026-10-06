//! Native OpenTelemetry instruments. The application owns its meter provider.

use std::sync::{Arc, OnceLock};

use opentelemetry::metrics::{Counter, Histogram, Meter, MeterProvider};
use opentelemetry::{InstrumentationScope, KeyValue};

use super::labels::{CacheLabelBudget, process_budget};
use crate::events::{
    BackplaneEvent, CacheEvent, CacheOperation, ComponentRead, DistributedEvent, LayerEvent,
    MemoryEvent,
};
use crate::plugins::{Plugin, PluginContext, PluginError, PluginObservations, PluginSession};

mod catalog;
use catalog::{MetricCounter, MetricScope};

/// Whether individual invalidated tags may become metric attributes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum MetricTags {
    /// Omit tag values; the default avoids key/tag/instance cardinality.
    #[default]
    Exclude,
    /// Include `operation_tag` on tag invalidation counters. Applications must
    /// bound their tag vocabulary or apply an SDK view/cardinality limit.
    Include,
}

/// Independently configurable cache, L1, L2 and backplane instrumentation scopes.
#[derive(Debug, Clone)]
pub struct OtelMetricMeters {
    cache: Meter,
    memory: Meter,
    distributed: Meter,
    backplane: Meter,
}
impl OtelMetricMeters {
    /// Uses an application-owned provider with separate versioned scopes.
    #[must_use]
    pub fn from_provider(provider: &impl MeterProvider) -> Self {
        let meter = |name| {
            provider.meter_with_scope(
                InstrumentationScope::builder(name)
                    .with_version(env!("CARGO_PKG_VERSION"))
                    .build(),
            )
        };
        Self {
            cache: meter("amalgam"),
            memory: meter("amalgam.memory"),
            distributed: meter("amalgam.distributed"),
            backplane: meter("amalgam.backplane"),
        }
    }
    /// Uses explicitly supplied meters; no global provider is modified.
    #[must_use]
    pub fn new(cache: Meter, memory: Meter, distributed: Meter, backplane: Meter) -> Self {
        Self {
            cache,
            memory,
            distributed,
            backplane,
        }
    }
    /// Records all instruments through one explicitly supplied meter.
    #[must_use]
    pub fn shared(meter: Meter) -> Self {
        Self::new(meter.clone(), meter.clone(), meter.clone(), meter)
    }
    fn meter(&self, scope: MetricScope) -> &Meter {
        match scope {
            MetricScope::Cache => &self.cache,
            MetricScope::Memory => &self.memory,
            MetricScope::Distributed => &self.distributed,
            MetricScope::Backplane => &self.backplane,
        }
    }
}

/// Native OTel counters/histogram with lossless owned event dispatch.
///
/// This plugin uses the OpenTelemetry API directly; it does not require the
/// `metrics` feature or a metrics-facade recorder. Keep the application-owned
/// SDK provider alive, shut caches down first, then flush/shut the provider down.
/// Default attributes contain a bounded cache name and finite diagnostic labels,
/// never cache keys, instance IDs, error strings or factory values.
pub struct OtelMetricsPlugin {
    meters: OtelMetricMeters,
    budget: Arc<CacheLabelBudget>,
    tags: MetricTags,
    legacy: OnceLock<MetricSession>,
}
impl OtelMetricsPlugin {
    /// Uses a single supplied meter and the shared bounded cache-name catalog.
    #[must_use]
    pub fn new(meter: Meter) -> Self {
        Self::with_meters(OtelMetricMeters::shared(meter))
    }
    /// Uses distinct versioned scopes from an application-owned meter provider.
    #[must_use]
    pub fn from_provider(provider: &impl MeterProvider) -> Self {
        Self::with_meters(OtelMetricMeters::from_provider(provider))
    }
    /// Uses explicit instrumentation scopes without installing global state.
    #[must_use]
    pub fn with_meters(meters: OtelMetricMeters) -> Self {
        Self {
            meters,
            budget: process_budget(),
            tags: MetricTags::Exclude,
            legacy: OnceLock::new(),
        }
    }
    /// Shares the same bounded historical cache-name catalog as facade metrics.
    #[must_use]
    pub fn with_label_budget(mut self, budget: Arc<CacheLabelBudget>) -> Self {
        self.budget = budget;
        self
    }
    /// Controls optional individual invalidation-tag attributes.
    #[must_use]
    pub fn with_tags(mut self, tags: MetricTags) -> Self {
        self.tags = tags;
        self
    }
    fn session(&self, cache_name: &str) -> MetricSession {
        MetricSession::new(&self.meters, self.budget.label_for(cache_name), self.tags)
    }
    fn legacy(&self) -> &MetricSession {
        self.legacy.get_or_init(|| self.session("amalgam"))
    }
}
impl std::fmt::Debug for OtelMetricsPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OtelMetricsPlugin")
            .field("meters", &self.meters)
            .field("tags", &self.tags)
            .field("assigned_cache_names", &self.budget.assigned_names())
            .finish_non_exhaustive()
    }
}
impl Plugin for OtelMetricsPlugin {
    fn name(&self) -> &str {
        "amalgam-otel-metrics"
    }
    fn observations(&self) -> PluginObservations {
        PluginObservations::All
    }
    fn on_event(&self, event: &CacheEvent) {
        self.legacy().logical(event);
    }
    fn on_layer_event(&self, event: &LayerEvent) {
        self.legacy().layer(event);
    }
    fn on_operation_started(&self, operation: CacheOperation) {
        self.legacy().started(operation);
    }
    fn on_component_read(&self, component: ComponentRead) {
        self.legacy().component_read(component);
    }
    fn attach(
        &self,
        context: &PluginContext,
    ) -> Result<Option<Box<dyn PluginSession>>, PluginError> {
        Ok(Some(Box::new(self.session(context.cache_name()))))
    }
}

struct MetricSession {
    counters: [Counter<u64>; MetricCounter::COUNT],
    duration: Histogram<f64>,
    common: [KeyValue; 1],
    fresh: [KeyValue; 2],
    stale: [KeyValue; 2],
    foreground: [KeyValue; 2],
    background: [KeyValue; 2],
    tags: MetricTags,
}
impl MetricSession {
    fn new(meters: &OtelMetricMeters, label: Arc<str>, tags: MetricTags) -> Self {
        let cache = KeyValue::new("cache_name", label.to_string());
        Self {
            counters: std::array::from_fn(|index| {
                let kind = MetricCounter::ALL[index];
                meters
                    .meter(kind.scope())
                    .u64_counter(kind.name())
                    .with_unit("{event}")
                    .build()
            }),
            duration: meters
                .cache
                .f64_histogram("amalgam.operation.duration")
                .with_unit("s")
                .with_description(
                    "Logical cache operation duration, including failures and cancellation",
                )
                .build(),
            common: [cache.clone()],
            fresh: [cache.clone(), KeyValue::new("stale", false)],
            stale: [cache.clone(), KeyValue::new("stale", true)],
            foreground: [cache.clone(), KeyValue::new("operation_background", false)],
            background: [cache, KeyValue::new("operation_background", true)],
            tags,
        }
    }
    fn counter(&self, counter: MetricCounter) -> &Counter<u64> {
        &self.counters[counter as usize]
    }
    fn add(&self, counter: MetricCounter) {
        self.counter(counter).add(1, &self.common);
    }
    fn hit(&self, counter: MetricCounter, stale: bool) {
        self.counter(counter)
            .add(1, if stale { &self.stale } else { &self.fresh });
    }
    fn started(&self, operation: CacheOperation) {
        self.counter(MetricCounter::OperationStarted).add(
            1,
            &[
                self.common[0].clone(),
                KeyValue::new("operation", operation.as_str()),
            ],
        );
        match operation {
            CacheOperation::TryGet => self.add(MetricCounter::TryGet),
            CacheOperation::GetOrDefault => self.add(MetricCounter::GetOrDefault),
            CacheOperation::GetOrSet => self.add(MetricCounter::GetOrSet),
            CacheOperation::Set
            | CacheOperation::Remove
            | CacheOperation::Expire
            | CacheOperation::RemoveByTag
            | CacheOperation::RemoveByTags
            | CacheOperation::Clear => {}
        }
    }
    fn component_read(&self, component: ComponentRead) {
        self.add(match component {
            ComponentRead::Memory => MetricCounter::MemoryGet,
            ComponentRead::Distributed => MetricCounter::DistributedGet,
        });
    }
    fn logical(&self, event: &CacheEvent) {
        match event {
            CacheEvent::Hit { stale, .. } => self.hit(MetricCounter::Hit, *stale),
            CacheEvent::Miss { .. } => self.add(MetricCounter::Miss),
            CacheEvent::Set { .. } => self.add(MetricCounter::Set),
            CacheEvent::Remove { .. } => self.add(MetricCounter::Remove),
            CacheEvent::Expire { .. } => self.add(MetricCounter::Expire),
            CacheEvent::Clear => self.add(MetricCounter::Clear),
            CacheEvent::RemoveByTag { tag } => match self.tags {
                MetricTags::Exclude => self.add(MetricCounter::RemoveByTag),
                MetricTags::Include => self.counter(MetricCounter::RemoveByTag).add(
                    1,
                    &[
                        self.common[0].clone(),
                        KeyValue::new("operation_tag", tag.as_str().to_owned()),
                    ],
                ),
            },
            CacheEvent::FactorySuccess { .. } => self
                .counter(MetricCounter::FactorySuccess)
                .add(1, &self.foreground),
            CacheEvent::FactoryError { .. } => self
                .counter(MetricCounter::FactoryError)
                .add(1, &self.foreground),
            CacheEvent::BackgroundFactorySuccess { .. } => self
                .counter(MetricCounter::FactorySuccess)
                .add(1, &self.background),
            CacheEvent::BackgroundFactoryError { .. } => self
                .counter(MetricCounter::FactoryError)
                .add(1, &self.background),
            CacheEvent::BackgroundCommitError { .. } => {
                self.add(MetricCounter::BackgroundCommitError)
            }
            CacheEvent::FactorySyntheticTimeout { .. } => {
                self.add(MetricCounter::FactorySyntheticTimeout)
            }
            CacheEvent::FailSafeActivate { .. } => self.add(MetricCounter::FailSafeActivate),
            CacheEvent::EagerRefresh { .. } => self.add(MetricCounter::EagerRefresh),
            CacheEvent::MemoryAdmissionRejected { .. } => {
                self.add(MetricCounter::MemoryAdmissionRejected)
            }
            CacheEvent::MarkerRead { .. } => self.add(MetricCounter::MarkerRead),
            CacheEvent::MarkerReceived { .. } => self.add(MetricCounter::MarkerReceived),
            CacheEvent::MarkerSnapshotWrite { .. } => self.add(MetricCounter::MarkerSnapshotWrite),
            CacheEvent::MarkerEagerRefresh { .. } => self.add(MetricCounter::MarkerEagerRefresh),
            CacheEvent::OperationCompleted {
                operation,
                outcome,
                elapsed,
                level,
            } => {
                let attributes = [
                    self.common[0].clone(),
                    KeyValue::new("operation", operation.as_str()),
                    KeyValue::new("outcome", outcome.as_str()),
                    KeyValue::new("level", level.map_or("all", |level| level.as_str())),
                ];
                self.counter(MetricCounter::OperationCompleted)
                    .add(1, &attributes);
                self.duration.record(elapsed.as_secs_f64(), &attributes);
            }
            // These logical compatibility events duplicate physical facts below.
            CacheEvent::Eviction { .. }
            | CacheEvent::SerializationError { .. }
            | CacheEvent::DeserializationError { .. }
            | CacheEvent::CircuitBreakerChange { .. }
            | CacheEvent::MessagePublished { .. }
            | CacheEvent::MessageReceived { .. } => {}
        }
    }
    fn layer(&self, event: &LayerEvent) {
        match event {
            LayerEvent::Memory(event) => match event {
                MemoryEvent::Hit { stale, .. } => self.hit(MetricCounter::MemoryHit, *stale),
                MemoryEvent::Miss { .. } => self.add(MetricCounter::MemoryMiss),
                MemoryEvent::Set { .. } => self.add(MetricCounter::MemorySet),
                MemoryEvent::Remove { .. } => self.add(MetricCounter::MemoryRemove),
                MemoryEvent::Expire { .. } => self.add(MetricCounter::MemoryExpire),
                MemoryEvent::Eviction { .. } => self.add(MetricCounter::MemoryEvict),
            },
            LayerEvent::Distributed(event) => match event {
                DistributedEvent::Hit { stale, .. } => {
                    self.hit(MetricCounter::DistributedHit, *stale)
                }
                DistributedEvent::Miss { .. } => self.add(MetricCounter::DistributedMiss),
                DistributedEvent::Set { .. } => self.add(MetricCounter::DistributedSet),
                DistributedEvent::Remove { .. } => self.add(MetricCounter::DistributedRemove),
                DistributedEvent::CircuitBreakerChange { closed } => self
                    .counter(MetricCounter::DistributedCircuitBreakerChange)
                    .add(
                        1,
                        &[self.common[0].clone(), KeyValue::new("closed", *closed)],
                    ),
                DistributedEvent::SerializationError { .. } => {
                    self.add(MetricCounter::SerializationError)
                }
                DistributedEvent::DeserializationError { .. } => {
                    self.add(MetricCounter::DeserializationError)
                }
            },
            LayerEvent::Backplane(event) => match event {
                BackplaneEvent::MessagePublished { .. } => {
                    self.add(MetricCounter::BackplanePublish)
                }
                BackplaneEvent::MessageReceived { .. } => self.add(MetricCounter::BackplaneReceive),
                BackplaneEvent::CircuitBreakerChange { closed } => self
                    .counter(MetricCounter::BackplaneCircuitBreakerChange)
                    .add(
                        1,
                        &[self.common[0].clone(), KeyValue::new("closed", *closed)],
                    ),
            },
        }
    }
}
impl PluginSession for MetricSession {
    fn observations(&self) -> PluginObservations {
        PluginObservations::All
    }
    fn on_event(&self, event: &CacheEvent) -> Result<(), PluginError> {
        self.logical(event);
        Ok(())
    }
    fn on_layer_event(&self, event: &LayerEvent) -> Result<(), PluginError> {
        self.layer(event);
        Ok(())
    }
    fn on_operation_started(&self, operation: CacheOperation) -> Result<(), PluginError> {
        self.started(operation);
        Ok(())
    }
    fn on_component_read(&self, component: ComponentRead) -> Result<(), PluginError> {
        self.component_read(component);
        Ok(())
    }
}
