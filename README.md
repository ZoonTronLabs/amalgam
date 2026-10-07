<p align="center">
  <img src="https://raw.githubusercontent.com/ZoonTronLabs/amalgam/main/assets/amalgam-icon.png" alt="amalgam" width="168" height="168">
</p>

<h1 align="center">amalgam</h1>

<p align="center">
  <a href="https://crates.io/crates/amalgam-cache"><img src="https://img.shields.io/crates/v/amalgam-cache.svg" alt="crates.io"></a>
  <a href="https://docs.rs/amalgam-cache"><img src="https://img.shields.io/docsrs/amalgam-cache" alt="docs.rs"></a>
  <a href="https://github.com/ZoonTronLabs/amalgam/actions/workflows/ci.yml"><img src="https://github.com/ZoonTronLabs/amalgam/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="MIT">
</p>

A Rust hybrid cache with async operations, inspired by [FusionCache](https://github.com/ZiggyCreatures/FusionCache), with local caching, optional distributed storage, fail-safe values, background refresh and observable mutations. Minimum Rust version: **1.88**, edition 2024.

The published package is **0.3.1**; **0.4 is under development** in this checkout. The outage-policy section describes the new source defaults. Install the published crate from crates.io:

```toml
[dependencies]
amalgam = { package = "amalgam-cache", version = "0.3" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

The published package is `amalgam-cache`; the Rust library is imported as `amalgam`.
Unreleased additions and the still-open full functionality inventory are tracked
in [FULL_CONTRACT.md](docs/FULL_CONTRACT.md) and the [changelog](CHANGELOG.md).

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

Legacy `build`, `try_get`, `remove`, `expire` and `clear(bool)` signatures remain compatibility adapters in this developing source API. Their original signatures cannot return every newly modeled failure; diagnostics retain observed failures. Prefer `try_build`, fallible reads/mutations and explicit shutdown when handling those failures matters.


## Fluent requests (unreleased source)

`set` and `get_or_set` execute when awaited. An option transformation starts
from a copy of the cache defaults, so changing duration retains fail-safe and
other settings. String tags are validated before storage or factory work.

```rust
use std::time::Duration;

let value = cache
    .get_or_set("profile", |ctx| async move { Ok(ctx.value("Alice".to_owned())) })
    .options(|options| options.with_duration(Duration::from_secs(30)))
    .tags(["users"])
    .await?;

cache.set("profile", value).tags(["users"]).await?;
let observed = cache
    .get_or_set("profile", |_| async { panic!("already cached") })
    .with_receipt()
    .await?;
observed.commit.wait().await?;
```

Optional `.fail_safe_default(value)` and `.cancellation(token)` apply to the
individual request. A manually polled request first calls `.into_future()`;
this separates configuration from pinned execution. The remaining operation
names and factory-value API are still being migrated for 0.4.

## Synchronous use (unreleased source)

`BlockingCache<V>` provides caller-thread operations and `as_async()` for the
same entries and lifecycle. Timed/cancellable/eager factories use owned,
bounded callback pools. Mutation receipts expose actual completion.
[Dispatch, resource bounds and lifecycle](docs/SYNC.md) describe the tested
contracts and intentional differences. This addition is available in the
source tree; the published package has not been updated by this work.

## Freshness, origin work and cancellation

Entries have independent logical freshness and physical fail-safe retention. Fail-safe can serve a captured stale value after an ordinary origin failure or timeout, within its physical lifetime. Cancellation stays a cancellation and bypasses fail-safe.

Built-in standalone caches measure duration lifetimes with a monotonic clock
anchored to UTC at construction. Civil-clock corrections do not extend or shorten
those local lifetimes. Hybrid caches, distributed lockers/backplanes and supplied
storage or markers retain live UTC ordering and elapsed physical deadlines.
Explicitly injecting `SystemClock` selects live system UTC; custom clock callbacks
run outside L1 reader slots.

Same-key requests coordinate through per-key ownership. A configured finite lock timeout deliberately permits the best-effort factory path when no fallback is available; it weakens unconditional single-flight. Different keys do not serialize because they happen to share a map shard.

Soft factory timeout applies when a fail-safe fallback is available. With background completion enabled, the origin and its ownership move into supervised work; that continuation does not acquire a new hard-timeout budget. Eager refresh is request-driven and does not use the ordinary factory timeout. Adaptive options and conditional `not_modified` are validated before storage. Snapshot creation order and actual insertion time are separate: delayed origin work cannot invent newer source ordering or renew a replay's physical lifetime.

An explicit caller token and `FactoryContext` cancellation state identify cancellation reasons. `close` initiates cancellation; `shutdown` waits for owned work and plugin cleanup. Dropping the last public cache handle also initiates cleanup. Built-in standalone origin versions prevent a late completion from replacing a newer awaited mutation; removing a key does not cancel unrelated origin work.

## Distributed storage and continuity

L2 backends, serializers, backplanes, lockers and copy strategies are open traits. Unreleased [custom local coordination](docs/MEMORY_LOCKER.md) adds an owned `MemoryLocker` provider for values and secondary markers, with optional distinct native acquisition through `BlockingMemoryLocker`; unreleased [supplied L1 storage](docs/MEMORY_STORAGE.md) provides the actual value store with typed failures and atomic conditional admission. JSON and reference in-memory providers are available by default; Redis, MessagePack and Postcard are optional. L1 and L2 freshness/retention are configured separately. Hydration caps local deadlines by the remaining source lifetime.

Default healthy L1 reads stay local. Cold L2 reads reconcile durable tag/clear markers; tags and clear against a custom L2 require an atomic invalidation provider. Ordinary legacy byte-store I/O remains usable without that capability. Control markers live outside ordinary value keys and are scoped by the effective physical namespace. Unreleased opt-in [independent marker reads](docs/MARKER_READS.md) can revalidate L1 hits using separate tag defaults, budgets and observation limits.

Unreleased [expiring marker snapshots](docs/MARKER_SNAPSHOTS.md) add independent remote lifetimes and nonzero read repair through an optional atomic provider capability. Snapshot expiry preserves the durable invalidation fact; this addition does not close the remaining marker eager/locker/recovery contract.

In the 0.4 source, backplane gaps and reconnects preserve L1 within its normal
freshness and fail-safe deadlines. L2-only caches have no periodic global L1
clearing. Known invalidations still apply. `ready()` and `try_build_ready()` can
await native subscription acknowledgement; ordinary operations do not wait by
default. `CacheBuilder::strict()` selects conservative reconciliation and initial
subscription admission. See [backplane outage policies](docs/BACKPLANE_OUTAGES.md).

Distributed lease capabilities are explicit. Native owned acquisition, renewal and token-checked release prevent abandoned ownership. Strict stale-owner commit rejection additionally requires an atomic backend ownership check; renewal alone cannot provide it during a partition. An explicitly selected legacy provider mode carries weaker guarantees; an opaque acquisition that never completes cannot promise bounded cancellation drainage. A finite lock timeout, deliberate lock skipping or another writer outside the protocol can also permit duplicate origin work.


### Redis outages and lease policy

The 0.4 source defaults to `LeasePolicy::Cooperative`: a suppressed distributed
locker error permits the ordinary factory to run. Fresh L1 hits contact neither
the locker nor L2. Retained stale data can serve configured fail-safe during
an outage. Nodes may compute concurrently while distributed coordination is
unavailable, and missed peer invalidations can remain invisible until expiration.

Use `Cache::<V>::builder().strict()` when invalidation continuity and lease-fenced
commits take priority over outage availability. Fenced configuration is rejected
at construction if the locker lacks owned-token lifetime support or L2 lacks
atomic fenced writes. Custom providers declare those capabilities explicitly.

Published **0.3.1** has different defaults: fenced ownership, conservative gap
cleanup, and periodic clearing for L2-only configurations. Updating this source
does not update an existing consumer. Explicit cancellation remains an error;
owned cleanup failures can still be reported by `shutdown()` after recovery.

Failed or skipped distributed effects can enter recovery with their original bytes, remaining lifetime and pending stage. Local same-key commit lanes order replay, foreground effects and publication. This protects an awaited newer local mutation from an older replay. Independent nodes and custom writes are not globally linearizable without a participating conditional backend protocol.

## Copying and capacity

Rust `Clone` does not isolate mutable state inside `Arc` or similar shared handles. `with_enable_auto_clone(true)` requires an actual fallible `ValueCloner`; built-in serializers can provide value round trips. Isolation is applied at storage and caller-output boundaries, including stale/fail-safe values. A failed copy is an error, never an ordinary shared clone fallback.

Entry count and entry weight are separate limits. Under pressure the priority policy evicts Low before Normal before High and uses recency for ties. `NeverRemove` entries still consume capacity and expire at their physical deadline; further admission can be rejected. Admission and eviction are reported through the shared event/plugin route.

## Recovery, events and diagnostics

Recovery defaults to enabled for a configured distributed provider, with a 5-second delay and **1024 queued items** in the developing 0.4 source. Explicit `max_items: None` permits an unlimited queue. The bound is a deliberate difference from the FusionCache/0.2 default. Queue-full rejection and exhausted retry budgets are observable. Markers are compacted conservatively rather than silently forgotten.

Transport failures trip the corresponding circuit breaker; codec or value-copy failures do not declare every key's transport unhealthy. Default breaker duration is zero, meaning disabled. Read I/O budgets and provider lifecycle budgets are distinct from the intentionally unbounded default cache write/remove contract.

Unreleased `CachePlugin<V>` adds operational access to the same cache through a weak typed context. Start, event handlers and Stop can read, compute and mutate; views do not keep the application lifecycle alive. See [plugin cache operations](docs/PLUGIN_CACHE.md) for lifetime and teardown boundaries.

Unreleased `subscribe_layers()` exposes typed memory, distributed and backplane facts independently of logical outcomes. Full peer payloads, physical hit eligibility and typed read-deadline behavior are documented in [component events](docs/LAYER_EVENTS.md). Original-value eviction and handler policy remain open.

Each cache has its own plugin sessions, including when a plugin object is shared. Dynamic registration detaches and stops exactly once. One event hub reports reads, misses, admission, eviction, origins, distributed effects and operation outcomes. Use the resilient event subscription when a slow observer must recover from broadcast lag.

Metrics use a bounded cache-name label budget. Keys and instance IDs belong in traces rather than metric labels. OpenTelemetry exposes a composable tracing layer and [native metric plugin](docs/NATIVE_METRICS.md) with application-owned providers and separate L1/L2/backplane scopes. The convenience tracing initializer preserves an existing subscriber/provider on failure.

## Development performance

The developing 0.4 source has zero-allocation warm L1 reads and replacements.
Seven local paired runs against FusionCache 2.9 pass L1, cold, write, L2 JSON
and eight-core scaling budgets. Exact-source Linux qualification and complete
behavioral parity remain open. See
[the measured tables and method](docs/PERFORMANCE.md) and
[release requirements](docs/ROADMAP.md) for source, runtime and machine boundaries.

## Features

| Feature | Integration |
|---|---|
| default | Local cache, JSON and in-memory distributed/backplane/locker providers |
| `redis` | Redis value store, durable markers, pub/sub and owned leases |
| `messagepack` | MessagePack snapshot/value-copy codec |
| `postcard` | Postcard snapshot/value-copy codec |
| `metrics` | Exporter-independent metrics plugin |
| `opentelemetry` | Composable tracing, native metrics and OTLP provider helpers |
| `full` | All integrations above |

The default distributed namespace is **v2 with Prefix**. New codecs read legacy raw payloads, but running 0.2 nodes cannot read 0.3 framed snapshots. Use a coordinated fresh namespace. `KeyModifierMode::None` or intentional reuse of v1 requires a fresh physical prefix or a coordinated migration; it does not make a mixed-version rollout safe.

See [migration and tested FusionCache contract](docs/PARITY.md), [validation and limits](docs/AUDIT.md), [porting design](PORTING.md), [examples](examples), [cache internals](docs/CACHE_INTERNALS.md) and the [changelog](CHANGELOG.md). These documents describe supported contracts and evidence; they do not claim universal one-to-one parity or equal performance for every workload.

## Acknowledgements

[FusionCache](https://github.com/ZiggyCreatures/FusionCache) by ZiggyCreatures provides the resiliency model and comparison reference. Amalgam uses Tokio, independently locked L1 storage and the open Rust integration ecosystem. Distributed providers and copying strategies remain extensible; internal finite outcomes use typed enums. Cache logic uses safe Rust; the private reader-slot synchronization boundary contains documented unsafe operations.
