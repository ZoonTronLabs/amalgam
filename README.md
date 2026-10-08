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

The **0.4 release series** provides the eight-operation API documented below. The outage-policy section describes its defaults. Install the published crate:

```toml
[dependencies]
amalgam = { package = "amalgam-cache", version = "0.4" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

The published package is `amalgam-cache`; the Rust library is imported as `amalgam`.
The release scope and still-open full functionality inventory are tracked
in [FULL_CONTRACT.md](docs/FULL_CONTRACT.md) and the [changelog](CHANGELOG.md).

## Amalgam vs FusionCache 2.9

Time per operation, one worker — **lower is better**. Both libraries run the
same workload on the same machine in alternating processes. FusionCache 2.9.0
uses normal .NET tiered compilation with Dynamic PGO after a settled warmup;
Amalgam is built with the default release profile (no LTO). "Through L2" uses
an in-memory distributed cache with the JSON serializer and skips L1 reads.

**Linux** — GitHub CI runner, AMD EPYC 9V74 ([run](https://github.com/ZoonTronLabs/amalgam/actions/runs/37793789026))

| Operation | Amalgam | FusionCache 2.9 | Amalgam is |
|---|---:|---:|:---|
| `try_get`, value in memory | 112 ns | 155 ns | **1.4× faster** |
| `get_or_set`, value in memory | 109 ns | 193 ns | **1.8× faster** |
| Synchronous `try_get` | 77 ns | 111 ns | **1.4× faster** |
| Synchronous `get_or_set` | 90 ns | 144 ns | **1.6× faster** |
| `set` (replace a value) | 188 ns | 220 ns | **1.2× faster** |
| `try_get` through L2 | 715 ns | 1,176 ns | **1.6× faster** |
| `get_or_set` through L2 | 946 ns | 1,388 ns | **1.5× faster** |
| Cold `get_or_set` (runs the factory) ¹ | 2,402 ns | 3,430 ns | **1.4× faster** |

**macOS** — Apple M4 Pro, local

| Operation | Amalgam | FusionCache 2.9 | Amalgam is |
|---|---:|---:|:---|
| `try_get`, value in memory | 39 ns | 199 ns | **5.1× faster** |
| `get_or_set`, value in memory | 43 ns | 226 ns | **5.3× faster** |
| Synchronous `try_get` | 37 ns | 180 ns | **4.8× faster** |
| Synchronous `get_or_set` | 30 ns | 205 ns | **6.9× faster** |
| `set` (replace a value) | 78 ns | 115 ns | **1.5× faster** |
| `try_get` through L2 | 414 ns | 1,188 ns | **2.9× faster** |
| `get_or_set` through L2 | 518 ns | 1,343 ns | **2.6× faster** |
| Cold `get_or_set` (runs the factory) | 1,048 ns | 1,769 ns | **1.7× faster** |
| `try_get`, 8 workers on distinct keys ² | 5.4 ns | 38.0 ns | **7.0× faster** |

¹ FusionCache's cold warmup did not settle on the Linux runner; treat this row
as diagnostic. ² Aggregate elapsed time divided by completed operations.

Warm memory hits and `set` allocate nothing; an L2 read allocates three times.
With Redis, a cold L2 read under the default durable-marker policy is one round
trip (pipelined GET and HMGET): on a local Valkey it fell from 386 µs to 200 µs.
These are cache microbenchmarks, not whole-application speedups. CPU models of
hosted runners vary between runs, so compare numbers only within one run. Ranges,
allocation counts, the same-runner improvement over 0.4.1 and open budgets are in
[PERFORMANCE](docs/PERFORMANCE.md).

## 0.4.0 implementation and measured results

The release completes eight lazy fluent operations: `get_or_set`, `try_get`,
`get_or_default`, `set`, `remove`, `expire`, `remove_by_tag` and `clear`.
`try_get` distinguishes a miss from a typed error; mutations have opt-in receipts.
Native and async callers share cache state and lifecycle. Legacy operation
adapters and `MaybeValue` are removed. Measured warm L1 reads and replacement
sets use zero allocations per operation. See [migration](docs/MIGRATION_0_4.md).

The release commit passes [all 20 CI jobs](https://github.com/ZoonTronLabs/amalgam/actions/runs/37757753609).
Local validation includes 770 default / 835 all-feature checks with live Redis
and doctests, Rust 1.88/stable Clippy, package consumers and a clean publish dry-run.
The FR/RS inventory has 20 Same, 6 Diff and 2 Gap rows. Complete paired FC
qualification and arbitrary custom L2 marker parity remain open; see [PARITY](docs/PARITY.md).

### Compared with the published 0.3.1 package

Each cell is **0.4.0 time / 0.3.1 time** for the same workload and machine;
**lower is faster**. For example, 0.640 means 36% less time per operation.
Both tables use the final Rust source and three alternating process pairs.
All eight workloads pass the blocking regression guard with settled warmups.

| Workload | M4 Pro | Linux |
|---|---:|---:|
| Cold factory / 1 worker | 0.203 | 0.289 |
| Warm L1 get_or_set / 1 worker | 0.133 | 0.160 |
| Warm L1 get_or_set / 8 workers | 0.009 | 0.101 |
| Warm L1 read / 1 worker | 0.149 | 0.172 |
| Warm L1 read / 8 workers | 0.007 | 0.093 |
| L2 JSON get_or_set / 1 worker | 0.640 | 0.673 |
| L2 JSON read / 1 worker | 0.544 | 0.635 |
| Replacement set / 1 worker | 0.022 | 0.041 |

Eight-worker values describe aggregate throughput, not individual request
latency. These are cache microbenchmarks, not whole-application speedups.
The regression fixture is separate from the FC fixture below; their ns/op
values must not be mixed. [Full values, source identities and method](docs/PERFORMANCE.md#published-031-regression-guard--final-m4-source).

### Compared with FusionCache 2.9.0 at 0.4.0

At 0.4.0, Amalgam was faster than FusionCache for memory hits, `set` and cold
factories, but slower through L2 on Linux (1.38× FC time for `try_get`, 1.56× for
`get_or_set`). The unreleased work above closes that gap. The 0.4.0 tables,
budgets and limitations remain in
[PERFORMANCE](docs/PERFORMANCE.md#040-measurements-history).

## Basic use

```rust
use amalgam::Cache;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cache: Cache<String> = Cache::builder().try_build()?;
    let greeting = cache
        .get_or_set("greeting", |_| async move {
            Ok::<_, std::convert::Infallible>("hello, world".to_owned())
        })
        .await?;
    assert_eq!(greeting, "hello, world");

    cache.set("greeting", "hello again".to_owned()).await?;
    assert_eq!(
        cache
            .try_get("greeting")
            .await?
            .as_ref()
            .map(String::as_str),
        Some("hello again")
    );
    cache.remove("greeting").await?;
    assert!(!cache.try_get("greeting").await?.is_some());
    cache.shutdown().await?;
    Ok(())
}
```

Use fallible APIs for new callers. `try_get` returns `Result<Option<V>>`,
distinguishing a successful miss from a storage, copy or configuration failure.
`get_or_default` returns `Result<V>`. Mutations return `Result<()>`;
`.with_receipt()` explicitly requests a
`MutationReceipt`. `Completed` contains its stage report; `Scheduled` contains
awaitable cache-owned completion. `wait()` observes completion and requested
error rethrows. Reports also expose skipped stages, suppressed failures and
recovery admission according to the configured policy.

Legacy read and invalidation adapters have been removed in 0.4.0. See
[the 0.4 migration guide](docs/MIGRATION_0_4.md) for replacement APIs.

## Fluent requests

`set`, `get_or_set`, remove, expire, tag invalidation and clear execute when awaited. An option transformation starts
from a copy of the cache defaults, so changing duration retains fail-safe and
other settings. String tags are validated before storage or factory work.

```rust
use std::time::Duration;

let value = cache
    .get_or_set("profile", |_| async move { Ok::<_, std::convert::Infallible>("Alice".to_owned()) })
    .options(|options| options.with_duration(Duration::from_secs(30)))
    .tags(["users"])
    .await?;

cache.set("profile", value).tags(["users"]).await?;
let observed = cache
    .get_or_set("profile", |_| async { Ok::<_, std::convert::Infallible>("Alice".to_owned()) })
    .with_receipt()
    .await?;
observed.commit.wait().await?;
```

Optional `.fail_safe_default(Some(value))` and `.cancellation(token)` apply to
the individual request. `None` removes the fallback; for nullable cached values,
`Some(None)` means a present null fallback. A manually polled request first calls `.into_future()`;
this separates configuration from pinned execution. Use `source::value(value)` as the second argument to `get_or_set` for a supplied
value. It preserves constant-source behavior without factory timeouts or eager
refresh. `source::factory(|ctx| ...)` supplies the inferred context type when
the callback uses its methods.

## Synchronous use

`BlockingCache<V>` provides caller-thread operations and `as_async()` for the
same entries and lifecycle. Timed/cancellable/eager factories use owned,
bounded callback pools. Mutation receipts expose actual completion.
[Dispatch, resource bounds and lifecycle](docs/SYNC.md) describe the tested
contracts and intentional differences in 0.4.0.

```rust
use amalgam::{BlockingCache, source};

let cache = BlockingCache::<u64>::new()?;
let value = cache.get_or_set("number", source::factory(|_| {
    Ok::<_, std::convert::Infallible>(7)
})).execute()?;
cache.set("number", value + 1).execute()?;
cache.remove("number").with_receipt().execute()?.wait()?;
cache.shutdown()?;
```

Native retrieval and mutation requests are lazy until `.execute()`. Request
options, tags, fallback and cancellation use the same fluent choices as async.

## Optional provider and advanced APIs

`provider` contains storage, serialization, backplane, Redis, locking and clock
interfaces/implementations. `advanced` contains explicit receipts, stronger
policies, marker snapshots, recovery inspection and detailed event payloads.
The root exposes the ordinary cache, options, factory context and errors.
Imports preserve existing runtime behavior; additional contracts are selected
through configuration or request choices.

In 0.4.0, L2 providers return immutable owned `provider::DistributedBytes`
snapshots. The in-memory backend shares stored bytes across reads; Redis adopts
its received buffer. Snapshots survive replacement, removal and expiration.
Custom providers must update their get signature; see
[the provider migration](docs/MIGRATION_0_4.md#immutable-l2-provider-bytes).

Providers can also declare `provider::ReadCompletion::Immediate` and answer
`get_immediate` / `read_many_immediate` from in-process state. A cache whose
value and durable-marker providers are immediate, with a synchronous codec and
built-in L1, completes warm L2 reads inline on the caller's first poll; the
reference in-memory providers do this. A value provider which owns its marker
store can implement `get_marked` to read clear markers with the value in one
round trip; Redis pipelines GET and HMGET. All three methods have defaults, so
existing providers keep their behavior.

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

L2 backends, serializers, backplanes, lockers and copy strategies are open traits. [Custom local coordination](docs/MEMORY_LOCKER.md) adds an owned `MemoryLocker` provider for values and secondary markers, with optional distinct native acquisition through `BlockingMemoryLocker`; [supplied L1 storage](docs/MEMORY_STORAGE.md) provides the actual value store with typed failures and atomic conditional admission. JSON and reference in-memory providers are available by default; Redis, MessagePack and Postcard are optional. L1 and L2 freshness/retention are configured separately. Hydration caps local deadlines by the remaining source lifetime.

Warm healthy L1 reads stay local. Cold L2 reads reconcile tag/clear markers;
when the value provider owns the durable markers, untagged cold reads fetch the
clear markers with the value in one round trip. Atomic guarantees require an atomic invalidation provider; ordinary byte-store I/O retains its weaker contract. Control markers live outside ordinary value keys and are scoped by the effective physical namespace. Version 0.4.0 includes [independent marker reads](docs/MARKER_READS.md); arbitrary custom L2 marker parity remains an explicit [Gap](docs/PARITY.md). These reads use separate tag defaults, budgets and observation limits.

[Expiring marker snapshots](docs/MARKER_SNAPSHOTS.md) add independent remote lifetimes and nonzero read repair through an optional atomic provider capability. Snapshot expiry preserves the durable invalidation fact; this addition does not close the remaining marker eager/locker/recovery contract.

In 0.4.0, backplane gaps and reconnects preserve L1 within its normal
freshness and fail-safe deadlines. L2-only caches have no periodic global L1
clearing. Known invalidations still apply. `ready()` and `try_build_ready()` can
await native subscription acknowledgement; ordinary operations do not wait by
default. `CacheBuilder::strict()` selects conservative reconciliation and initial
subscription admission. See [backplane outage policies](docs/BACKPLANE_OUTAGES.md).

Distributed lease capabilities are explicit. Native owned acquisition, renewal and token-checked release prevent abandoned ownership. Strict stale-owner commit rejection additionally requires an atomic backend ownership check; renewal alone cannot provide it during a partition. An explicitly selected legacy provider mode carries weaker guarantees; an opaque acquisition that never completes cannot promise bounded cancellation drainage. A finite lock timeout, deliberate lock skipping or another writer outside the protocol can also permit duplicate origin work.


### Redis outages and lease policy

Version 0.4.0 defaults to `LeasePolicy::Cooperative`: a suppressed distributed
locker error permits the ordinary factory to run. Fresh L1 hits contact neither
the locker nor L2. Retained stale data can serve configured fail-safe during
an outage. Nodes may compute concurrently while distributed coordination is
unavailable, and missed peer invalidations can remain invisible until expiration.

Use `Cache::<V>::builder().strict()` when invalidation continuity and lease-fenced
commits take priority over outage availability. Fenced configuration is rejected
at construction if the locker lacks owned-token lifetime support or L2 lacks
atomic fenced writes. Custom providers declare those capabilities explicitly.

Published **0.3.1** has different defaults: fenced ownership, conservative gap
cleanup, and periodic clearing for L2-only configurations. Existing consumers
retain those defaults until they upgrade the crate. Explicit cancellation remains an error;
owned cleanup failures can still be reported by `shutdown()` after recovery.

Failed or skipped distributed effects can enter recovery with their original bytes, remaining lifetime and pending stage. Local same-key commit lanes order replay, foreground effects and publication. This protects an awaited newer local mutation from an older replay. Independent nodes and custom writes are not globally linearizable without a participating conditional backend protocol.

## Copying and capacity

Rust `Clone` does not isolate mutable state inside `Arc` or similar shared handles. `with_enable_auto_clone(true)` requires an actual fallible `ValueCloner`; built-in serializers can provide value round trips. Isolation is applied at storage and caller-output boundaries, including stale/fail-safe values. A failed copy is an error, never an ordinary shared clone fallback.

Entry count and entry weight are separate limits. Under pressure the priority policy evicts Low before Normal before High and uses recency for ties. `NeverRemove` entries still consume capacity and expire at their physical deadline; further admission can be rejected. Admission and eviction are reported through the shared event/plugin route.

## Recovery, events and diagnostics

Recovery defaults to enabled for a configured distributed provider, with a 5-second delay and **1024 queued items** in 0.4.0. Explicit `max_items: None` permits an unlimited queue. The bound is a deliberate difference from the FusionCache/0.2 default. Queue-full rejection and exhausted retry budgets are observable. Markers are compacted conservatively rather than silently forgotten.

Transport failures trip the corresponding circuit breaker; codec or value-copy failures do not declare every key's transport unhealthy. Default breaker duration is zero, meaning disabled. Read I/O budgets and provider lifecycle budgets are distinct from the intentionally unbounded default cache write/remove contract.

`CachePlugin<V>` adds operational access to the same cache through a weak typed context. Start, event handlers and Stop can read, compute and mutate; views do not keep the application lifecycle alive. See [plugin cache operations](docs/PLUGIN_CACHE.md) for lifetime and teardown boundaries.

`subscribe_layers()` exposes typed memory, distributed and backplane facts independently of logical outcomes. Full peer payloads, physical hit eligibility and typed read-deadline behavior are documented in [component events](docs/LAYER_EVENTS.md). Original-value eviction and handler policy remain open.

Each cache has its own plugin sessions, including when a plugin object is shared. Dynamic registration detaches and stops exactly once. One event hub reports reads, misses, admission, eviction, origins, distributed effects and operation outcomes. Use the resilient event subscription when a slow observer must recover from broadcast lag.

Metrics use a bounded cache-name label budget. Keys and instance IDs belong in traces rather than metric labels. OpenTelemetry exposes a composable tracing layer and [native metric plugin](docs/NATIVE_METRICS.md) with application-owned providers and separate L1/L2/backplane scopes. The convenience tracing initializer preserves an existing subscriber/provider on failure.

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
