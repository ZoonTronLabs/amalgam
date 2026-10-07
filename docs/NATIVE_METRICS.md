# Native OpenTelemetry metrics (unreleased)

Enable `opentelemetry` and attach `OtelMetricsPlugin` using the application's meter
provider. No metrics-facade recorder, global subscriber replacement or global
meter-provider installation is required. The published crates.io 0.3.1 archive
has not changed; a working manifest with the same version is not that release.

```rust
use std::sync::Arc;
use amalgam::{Cache, OtelMetricsPlugin, advanced::otlp_meter_provider};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let provider = otlp_meter_provider("service", "http://127.0.0.1:4317")?;
    let cache = Cache::<u64>::builder()
        .name("profiles")
        .plugin(Arc::new(OtelMetricsPlugin::from_provider(&provider)))
        .try_build()?;
    cache.set("answer", 42).with_receipt().await?.wait().await?;
    assert_eq!(cache.read("answer", None).await?.value(), Some(&42));
    cache.shutdown().await?;
    tokio::task::spawn_blocking(move || provider.shutdown()).await??;
    Ok(())
}
```

The SDK provider owns export and aggregation. Supply its views, aggregation,
cardinality limits, resource and export interval through the usual SDK builder.
The OTLP helper is an optional gRPC convenience; construction does not confirm
collector delivery. Initialize it within a driven Tokio runtime. SDK flush and
shutdown block; use `spawn_blocking` with a current-thread runtime. Drain caches
before shutting the provider down so captured plugin callbacks can still record.
`OtelMetricMeters::new` accepts independently supplied meters;
`from_provider` creates versioned scopes `amalgam`, `amalgam.memory`,
`amalgam.distributed` and `amalgam.backplane`.

## Catalog and meaning

The 33 reference counter names below follow the inspected official FusionCache
2.9 catalog at `af09f81a3ea8d7ed71183b46501946da801a2a22`, with the `amalgam`
prefix. This is a catalog mapping, not a promise of identical .NET event timing,
defaults or every option combination. Counters use unsigned event counts.

| Scope | Counters |
|---|---|
| `amalgam` | `amalgam.cache.set`, `amalgam.cache.try_get`, `amalgam.cache.get_or_default`, `amalgam.cache.get_or_set`, `amalgam.cache.remove`, `amalgam.cache.expire`, `amalgam.cache.remove_by_tag`, `amalgam.cache.clear`, `amalgam.cache.hit`, `amalgam.cache.miss`, `amalgam.factory.synthetic_timeout`, `amalgam.factory.error`, `amalgam.factory.success`, `amalgam.failsafe_activate`, `amalgam.eager_refresh` |
| `amalgam.memory` | `amalgam.memory.set`, `amalgam.memory.get`, `amalgam.memory.expire`, `amalgam.memory.remove`, `amalgam.memory.evict`, `amalgam.memory.hit`, `amalgam.memory.miss` |
| `amalgam.distributed` | `amalgam.distributed.set`, `amalgam.distributed.get`, `amalgam.distributed.remove`, `amalgam.distributed.hit`, `amalgam.distributed.miss`, `amalgam.distributed.circuit_breaker_change`, `amalgam.serialize_error`, `amalgam.deserialize_error` |
| `amalgam.backplane` | `amalgam.backplane.publish`, `amalgam.backplane.receive`, `amalgam.backplane.circuit_breaker_change` |

Read entry counters record logical starts, including subsequent failure.
Memory Get records lookup attempts, including a ready lookup followed by the
owned slow-path recheck. Distributed Get records value read-through attempts that
reach the provider, including throwing failures. Skips, open circuits and zero
read budgets do not create provider attempts. Raw provider calls during marker
reconciliation/recovery are not a general provider-I/O meter.

Layer Hit/Miss describe physical lookup facts, before final marker acceptance;
a physical hit can precede an operation error or logical miss. Logical and
physical Set differ during L2 hydration. Mutation counters follow their actual
logical events/provider effects; they are not every requested mutation start.
Physical eviction counts current retirements, including values admitted before
plugin attachment; original-value subscription capture remains a separate policy.

Factory success records an accepted completed origin product, including
`NotModified`, in its actual foreground/background completion context. Soft
continuation and eager success do not also count foreground success. An eager
L2 reuse or a supplied constant produces no factory-success event. Owned
background-origin errors retain the existing pipeline semantics, including
commit failure/panic, rather than pretending to measure only user-callback time.
`stale`, `closed` and `operation_background` are finite boolean attributes.

Eight additional counters expose owned operation starts/completions, memory
admission rejection, background commit errors and marker read/receive/snapshot
write/eager facts. `amalgam.operation.duration` is a seconds histogram with
bounded `operation`, `outcome` and servicing `level` attributes, including error
and cancellation outcomes. It measures the logical operation, not a later
mutation-receipt wait or all background work.

## Labels and delivery

Both facade and native plugins share `CacheLabelBudget`: default 128 historical
cache names, at most 64 bytes per accepted name. Excess names map to `other`.
Capacity is historical and is not reclaimed on cache detach. Share an explicit
budget to choose its limits. Instance IDs, keys, values and error strings never
become metric attributes. `MetricTags::Include` explicitly adds `operation_tag`
only to tag-invalidation counters; its vocabulary needs an application/SDK bound.
Tags are excluded by default. A literal cache name `other` shares the overflow
series by design.

Native metrics select `PluginObservations::All` and use counted callbacks rather than a bounded broadcast
reader. Broadcast lag cannot drop their observations. Callbacks captured during
an owned operation run after all its coordination guards/clones are released;
background work can postpone them. Plugin stop and cache shutdown drain captured
callbacks. Manual detach admits no new observations but waits for captured work.
This guarantees delivery to an attached session, not successful export by an
external collector or unlimited SDK cardinality.

`otel_metrics_contract` uses actual SDK aggregation and a custom push exporter:
1000 warm reads despite stream lag, L2 hydration, backplane receives, optional
tags, shared label limits, throwing provider attempts, conditional factories,
soft continuation and eager L2 reuse. `layer_plugin_contract` verifies old
interest defaults, reentrant same-key calls, cancellation/detach drainage,
late physical retirement, original errors and failed-attachment cleanup.
Configurable handler scheduling/rethrow, category log levels, optional trace/log
tags and broader provider/replay combinations remain in
[FULL_CONTRACT.md](FULL_CONTRACT.md).
