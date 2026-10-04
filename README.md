<p align="center">
  <img src="https://raw.githubusercontent.com/ZoonTronLabs/amalgam/main/assets/amalgam-icon.png" alt="amalgam" width="168" height="168">
</p>

<h1 align="center">amalgam</h1>

<p align="center">
  <a href="https://crates.io/crates/amalgam-cache"><img src="https://img.shields.io/crates/v/amalgam-cache.svg" alt="crates.io"></a>
  <a href="https://docs.rs/amalgam-cache"><img src="https://img.shields.io/docsrs/amalgam-cache" alt="docs.rs"></a>
  <a href="https://github.com/ZoonTronLabs/amalgam/actions/workflows/ci.yml"><img src="https://github.com/ZoonTronLabs/amalgam/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="MIT">
  <img src="https://img.shields.io/badge/unsafe-forbidden-success.svg" alt="forbid unsafe">
</p>

An async Rust hybrid cache inspired by [FusionCache](https://github.com/ZiggyCreatures/FusionCache), with local caching, optional distributed storage, fail-safe values, background refresh and observable mutations. Minimum Rust version: **1.88**, edition 2024.

This README describes **0.3.0**. Install the crate from crates.io:

```toml
[dependencies]
amalgam = { package = "amalgam-cache", version = "0.3" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

The published package is `amalgam-cache`; the Rust library is imported as `amalgam`.

## Basic use

```rust
use amalgam::Cache;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cache: Cache<String> = Cache::builder().try_build()?;
    let greeting = cache
        .get_or_set("greeting", |ctx| async move {
            Ok(ctx.value("hello, world".to_owned()))
        })
        .await?;
    assert_eq!(greeting, "hello, world");

    cache
        .try_set("greeting", "hello again".to_owned())
        .await?
        .wait()
        .await?;
    assert_eq!(
        cache
            .read("greeting", None)
            .await?
            .value()
            .map(String::as_str),
        Some("hello again")
    );
    cache.try_remove("greeting").await?.wait().await?;
    assert!(!cache.read("greeting", None).await?.has_value());
    cache.shutdown().await?;
    Ok(())
}
```

Use fallible APIs for new callers. `read` distinguishes a successful miss from a storage, copy or configuration failure. A mutation returns a `MutationReceipt`: `Completed` contains its stage report; `Scheduled` contains awaitable cache-owned completion. `wait()` observes actual completion, including a requested error rethrow. A completed report can also record skipped stages, suppressed failures and work admitted to recovery, according to the configured policy.

Legacy `build`, `set`, `try_get`, `remove`, `expire` and `clear(bool)` signatures remain compatibility adapters. Their original signatures cannot return every newly modeled failure; diagnostics retain observed failures. Prefer `try_build`, fallible reads/mutations and explicit shutdown when handling those failures matters.

## Freshness, origin work and cancellation

Entries have independent logical freshness and physical fail-safe retention. Fail-safe can serve a captured stale value after an ordinary origin failure or timeout, within its physical lifetime. Cancellation stays a cancellation and bypasses fail-safe.

Same-key requests coordinate through per-key ownership. A configured finite lock timeout deliberately permits the best-effort factory path when no fallback is available; it weakens unconditional single-flight. Different keys do not serialize because they happen to share a map shard.

Soft factory timeout applies when a fail-safe fallback is available. With background completion enabled, the origin and its ownership move into supervised work; that continuation does not acquire a new hard-timeout budget. Eager refresh is request-driven and does not use the ordinary factory timeout. Adaptive options and conditional `not_modified` are validated before storage. Snapshot creation order and actual insertion time are separate: delayed origin work cannot invent newer source ordering or renew a replay's physical lifetime.

An explicit caller token and `FactoryContext` cancellation state identify cancellation reasons. `close` initiates cancellation; `shutdown` waits for owned work and plugin cleanup. Dropping the last public cache handle also initiates cleanup. Ordinary late origin completion after a concurrent remove remains a FusionCache-compatible behavior; cache removal does not cancel unrelated origin work.

## Distributed storage and continuity

L2 backends, serializers, backplanes, lockers and copy strategies are open traits. JSON and reference in-memory providers are available by default; Redis, MessagePack and Postcard are optional. L1 and L2 freshness/retention are configured separately. Hydration caps local deadlines by the remaining source lifetime.

Healthy L1 reads stay local. Cold L2 reads reconcile durable tag/clear markers; tags and clear against a custom L2 require an atomic invalidation provider. Ordinary legacy byte-store I/O remains usable without that capability. Control markers live outside ordinary value keys and are scoped by the effective physical namespace.

A backplane continuity gap, queue overflow or changed connection epoch requires reconciliation. Native Redis becomes connected only after a matching subscription acknowledgement. `ready()` and `try_build_ready()` expose that admission; the default initial-wait policy gates operations. A healthless adapter explicitly reports `BackplaneReadiness::BestEffort`. Custom backplanes without a health stream use the documented conservative reconciliation policy.

Distributed lease capabilities are explicit. Native owned acquisition, renewal and token-checked release prevent abandoned ownership. Strict stale-owner commit rejection additionally requires an atomic backend ownership check; renewal alone cannot provide it during a partition. An explicitly selected legacy provider mode carries weaker guarantees; an opaque acquisition that never completes cannot promise bounded cancellation drainage. A finite lock timeout, deliberate lock skipping or another writer outside the protocol can also permit duplicate origin work.

Failed or skipped distributed effects can enter recovery with their original bytes, remaining lifetime and pending stage. Local same-key commit lanes order replay, foreground effects and publication. This protects an awaited newer local mutation from an older replay. Independent nodes and custom writes are not globally linearizable without a participating conditional backend protocol.

## Copying and capacity

Rust `Clone` does not isolate mutable state inside `Arc` or similar shared handles. `with_enable_auto_clone(true)` requires an actual fallible `ValueCloner`; built-in serializers can provide value round trips. Isolation is applied at storage and caller-output boundaries, including stale/fail-safe values. A failed copy is an error, never an ordinary shared clone fallback.

Entry count and entry weight are separate limits. Under pressure the priority policy evicts Low before Normal before High and uses recency for ties. `NeverRemove` entries still consume capacity and expire at their physical deadline; further admission can be rejected. Admission and eviction are reported through the shared event/plugin route.

## Recovery, events and diagnostics

Recovery defaults to enabled for a configured distributed provider, with a 2-second delay and **1024 queued items**. Explicit `max_items: None` permits an unlimited queue. The bound is a deliberate difference from the FusionCache/0.2 default. Queue-full rejection and exhausted retry budgets are observable. Markers are compacted conservatively rather than silently forgotten.

Transport failures trip the corresponding circuit breaker; codec or value-copy failures do not declare every key's transport unhealthy. Default breaker duration is zero, meaning disabled. Read I/O budgets and provider lifecycle budgets are distinct from the intentionally unbounded default cache write/remove contract.

Each cache has its own plugin sessions, including when a plugin object is shared. Dynamic registration detaches and stops exactly once. One event hub reports reads, misses, admission, eviction, origins, distributed effects and operation outcomes. Use the resilient event subscription when a slow observer must recover from broadcast lag.

Metrics use a bounded cache-name label budget. Keys and instance IDs belong in traces rather than metric labels. OpenTelemetry exposes a composable layer; the convenience global initializer preserves an existing subscriber/provider on failure.

## Features

| Feature | Integration |
|---|---|
| default | Local cache, JSON and in-memory distributed/backplane/locker providers |
| `redis` | Redis value store, durable markers, pub/sub and owned leases |
| `messagepack` | MessagePack snapshot/value-copy codec |
| `postcard` | Postcard snapshot/value-copy codec |
| `metrics` | Exporter-independent metrics plugin |
| `opentelemetry` | Composable tracing and OTLP convenience initialization |
| `full` | All integrations above |

The default distributed namespace is **v2 with Prefix**. New codecs read legacy raw payloads, but running 0.2 nodes cannot read 0.3 framed snapshots. Use a coordinated fresh namespace. `KeyModifierMode::None` or intentional reuse of v1 requires a fresh physical prefix or a coordinated migration; it does not make a mixed-version rollout safe.

See [migration and tested FusionCache contract](docs/PARITY.md), [validation and limits](docs/AUDIT.md), [porting design](PORTING.md), [examples](examples) and the [changelog](CHANGELOG.md). These documents describe supported contracts and evidence; they do not claim universal one-to-one parity or equal performance for every workload.

## Acknowledgements

[FusionCache](https://github.com/ZiggyCreatures/FusionCache) by ZiggyCreatures provides the resiliency model and comparison reference. Amalgam uses Tokio, Moka and the open Rust integration ecosystem. Distributed providers and copying strategies remain extensible; internal finite outcomes use typed enums. The crate forbids unsafe code.
