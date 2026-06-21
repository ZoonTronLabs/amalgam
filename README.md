# amalgam

**A robust, multi-level, fail-safe cache for Rust** — a faithful, idiomatic port
of the resiliency model pioneered by .NET's
[FusionCache](https://github.com/ZiggyCreatures/FusionCache).

An *amalgam* is a fusion of metals. This crate fuses an in-memory **L1** cache
(built on [`moka`](https://crates.io/crates/moka)) with an optional distributed
**L2** cache and a multi-node **backplane**, and gives you the features that make
a cache *robust* rather than merely fast — on top of `tokio`.

```toml
[dependencies]
amalgam = "0.1"
```

## Why a cache needs more than `get`/`set`

A plain TTL cache collapses under load and failure: when a hot key expires, every
request stampedes the database at once; when the database hiccups, every request
fails. `amalgam` solves both, the way FusionCache does:

- **Cache-stampede protection** — only one factory runs per key; everyone else
  awaits that single result (single-flight).
- **Fail-safe** — if the factory fails, serve the last known-good (stale) value
  instead of propagating an error.
- **Soft / hard timeouts** — a slow factory returns a stale value *immediately*
  and finishes in the background.
- **Eager refresh** — refresh proactively before expiration, off the hot path.
- **Adaptive caching** — the factory can change an entry's options per call.
- **Conditional refresh** — HTTP-style `NotModified` reuse of a stale value.
- **Tagging** — invalidate many entries at once, lazily, by tag.
- **L1 + L2 + backplane** — a pluggable distributed cache and multi-node sync.

See [`docs/PARITY.md`](docs/PARITY.md) for the feature-by-feature mapping to
FusionCache, and [`PORTING.md`](PORTING.md) for the C#→Rust translation method.

## Quickstart

```rust
use amalgam::{Cache, FactoryError};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cache: Cache<String> = Cache::new();

    // Runs the factory once; concurrent callers for the same key coalesce.
    let greeting = cache
        .get_or_set("greeting", |ctx| async move {
            // ...expensive work (DB, HTTP, ...)...
            Ok(ctx.value("hello, world".to_owned()))
        })
        .await?;

    assert_eq!(greeting, "hello, world");
    Ok(())
}
```

Signal a factory failure with `ctx.fail(..)` (or return any `FactoryError`); wrap
a source error with `FactoryError::from_source(e)`:

```rust,ignore
let user = cache
    .get_or_set("user:42", |ctx| async move {
        match load_user(42).await {
            Ok(u) => Ok(ctx.value(u)),
            Err(e) => Err(FactoryError::from_source(e)),
        }
    })
    .await?;
```

## Fail-safe + timeouts

Configure resiliency per call through `EntryOptions`:

```rust,ignore
use amalgam::{EntryOptions, Timeout};
use std::time::Duration;

let opts = cache
    .entry_options()
    .with_duration(Duration::from_secs(60))
    // enable fail-safe: keep values for up to 1h, re-serve stale for 30s between retries
    .with_fail_safe(true, Some(Duration::from_secs(3600)), Some(Duration::from_secs(30)))
    // if the factory takes > 100ms and a stale value exists, return it now and finish in the background
    .with_factory_timeouts(Timeout::After(Duration::from_millis(100)), Timeout::Infinite, true);

let value = cache.get_or_set_with("report", factory, opts).await?;
```

If the factory later errors, `amalgam` serves the stale value and fires a
`FailSafeActivate` event instead of returning an `Err`.

## Adaptive & conditional refresh

The factory receives a [`FactoryContext`] it can use to adapt caching or do an
HTTP-style conditional request:

```rust,ignore
let html = cache.get_or_set("page", |mut ctx| async move {
    // Adaptive caching: cache an empty result for less time.
    // Conditional refresh: reuse the stale body on a 304.
    if let Some(etag) = ctx.stale_etag() {
        if not_modified_since(etag).await {
            return ctx.not_modified();           // reuse stale value, bump expiration
        }
    }
    let (body, etag) = fetch_page().await.map_err(FactoryError::from_source)?;
    Ok(ctx.modified(body).etag(etag).done())     // store with a fresh ETag
}).await?;
```

## Multi-level: L1 + L2 + backplane

Add a distributed L2 (anything implementing `DistributedCache`) and a backplane
to keep several nodes' L1 caches coherent. Reference in-memory/in-process backends
ship by default for testing and single-process multi-instance setups; a real
Redis L2 + backplane + locker ship behind the [`redis` feature](#cargo-features).

```rust,ignore
use amalgam::{Cache, InMemoryDistributedCache, InProcessBackplane, JsonSerializer,
              SystemClock, DistributedCache, DistributedSerializer, Backplane, Clock};
use std::sync::Arc;

let clock: Arc<dyn Clock> = Arc::new(SystemClock);
let l2: Arc<dyn DistributedCache> = Arc::new(InMemoryDistributedCache::new(clock.clone()));
let backplane: Arc<dyn Backplane> = Arc::new(InProcessBackplane::default());

// `V` must be `Clone + Serialize + DeserializeOwned` to cross the L2 wire.
let cache: Cache<MyData> = Cache::builder()
    .distributed(l2)
    .serializer(Arc::new(JsonSerializer))
    .backplane(backplane)
    .instance_id("node-1")
    .build();
```

Now `get_or_set` reads through L1 → L2 → factory and writes through to both; a
`set`/`remove`/`expire` on one node publishes a backplane message so peers drop
their stale L1 copy and re-pull the authoritative value from L2.

## Observe what the cache is doing

```rust,ignore
use amalgam::CacheEvent;

let mut events = cache.events().subscribe();
tokio::spawn(async move {
    while let Ok(event) = events.recv().await {
        match event {
            CacheEvent::FailSafeActivate { key } => eprintln!("served stale for {key}"),
            CacheEvent::FactoryError { key, message } => eprintln!("factory failed for {key}: {message}"),
            _ => {}
        }
    }
});
```

## Resilience: circuit breakers + auto-recovery

When the L2 cache or backplane is flaky, two FusionCache mechanisms keep the cache
fast and self-healing:

- **Circuit breakers** stop hammering a known-bad dependency. After a failure the
  breaker opens for a fixed window; while open, L2 / backplane ops are skipped
  (and queued for recovery), then it auto-closes. A `Duration::ZERO` breaker is
  permanently closed — the default, matching FusionCache.
- **Auto-recovery** queues the ops that failed (or were skipped while a breaker was
  open) and replays them on a background drain, with **latest-wins dedup** per key
  and a bounded queue + retry budget. It is enabled by default whenever an L2 or
  backplane is configured.

```rust,ignore
use amalgam::{Cache, RecoveryConfig};
use std::time::Duration;

let cache: Cache<MyData> = Cache::builder()
    .distributed(l2)
    .serializer(serializer)
    .backplane(backplane)
    // open the L2 breaker for 5s after a failure (ZERO = disabled, the default)
    .distributed_circuit_breaker(Duration::from_secs(5))
    .backplane_circuit_breaker(Duration::from_secs(5))
    // tune the retry queue (this is also the default when L2/backplane is present)
    .auto_recovery(RecoveryConfig {
        enabled: true,
        delay: Duration::from_secs(5),   // drain cadence + post-reconnect barrier
        max_items: Some(1000),
        max_retries: Some(5),
    })
    .build();
```

A breaker opening or closing fires `CacheEvent::CircuitBreakerChange { component, closed }`.

## Cross-node single-flight (distributed locker)

In-process stampede protection runs one factory per key *per node*. A
`DistributedLocker` extends that to **one factory per key across the cluster** — it
is acquired after the local lock. Share one `InMemoryDistributedLocker` between
caches in a single process, or use the Redis-backed locker for a real cluster.
Opt a single call out with `EntryOptions::with_skip_distributed_locker`.

```rust,ignore
use amalgam::{Cache, DistributedLocker, InMemoryDistributedLocker, Clock, SystemClock};
use std::sync::Arc;

let clock: Arc<dyn Clock> = Arc::new(SystemClock);
let locker: Arc<dyn DistributedLocker> = Arc::new(InMemoryDistributedLocker::new(clock));

let cache: Cache<MyData> = Cache::builder()
    .distributed(l2)
    .serializer(serializer)
    .distributed_locker(locker)
    .build();
```

## Plugins & metrics

A `Plugin` observes every `CacheEvent` (and an `on_start` lifecycle hook) — the
Rust counterpart of `IFusionCachePlugin`. Plugins run on the (already cheap,
non-blocking) event path, so a plugin must not block; offload real work to its own
task.

```rust,ignore
use amalgam::{CacheEvent, Plugin};

struct LogPlugin;
impl Plugin for LogPlugin {
    fn name(&self) -> &str { "log" }
    fn on_event(&self, event: &CacheEvent) {
        if let CacheEvent::FailSafeActivate { key } = event {
            eprintln!("fail-safe for {key}");
        }
    }
}

let cache: Cache<MyData> = Cache::builder()
    .plugin(std::sync::Arc::new(LogPlugin))
    .build();
```

With the `metrics` feature, `MetricsPlugin` is a ready-made plugin that records
counters (hits, misses, sets, factory errors/timeouts, fail-safe activations, eager
refreshes) through the [`metrics`](https://docs.rs/metrics) facade — point any
compatible exporter (Prometheus, OTLP, …) at it:

```rust,ignore
use amalgam::MetricsPlugin; // requires `features = ["metrics"]`

let cache: Cache<MyData> = Cache::builder()
    .plugin(std::sync::Arc::new(MetricsPlugin::new()))
    .build();
```

## Named caches & dynamic defaults

Where FusionCache resolves named caches from a DI container, `amalgam` offers a
`CacheRegistry` (register/resolve by name) and a `DefaultEntryOptionsProvider`
(per-key default options, consulted when a call passes no explicit options):

```rust,ignore
use amalgam::{Cache, CacheRegistry, DefaultEntryOptionsProvider, EntryOptions};
use std::sync::Arc;
use std::time::Duration;

let registry: CacheRegistry<String> = CacheRegistry::new();
let users = registry.get_or_create("users", || Cache::builder().name("users").build());

struct PerKeyDefaults;
impl DefaultEntryOptionsProvider for PerKeyDefaults {
    fn options_for(&self, key: &str) -> Option<EntryOptions> {
        key.starts_with("hot:")
            .then(|| EntryOptions::new(Duration::from_secs(5)))
    }
}

let cache: Cache<String> = Cache::builder()
    .default_options_provider(Arc::new(PerKeyDefaults))
    .build();
let _ = (users, cache);
```

## Cargo features

All distributed *backends* and extras are opt-in; the default build is
dependency-light and uses the in-memory / in-process reference backends.

| Feature | Enables |
|---------|---------|
| *(default)* | L1 + reference L2/backplane/locker (`InMemoryDistributedCache`, `InProcessBackplane`, `InMemoryDistributedLocker`), `JsonSerializer`. |
| `redis` | `RedisDistributedCache`, `RedisBackplane`, `RedisDistributedLocker` on `redis::aio::ConnectionManager`. |
| `messagepack` | `MessagePackSerializer` (compact L2 payloads via `rmp-serde`). |
| `postcard` | `PostcardSerializer` (smallest L2 payloads, `serde`-native binary). |
| `metrics` | `MetricsPlugin` (counters via the `metrics` facade). |
| `opentelemetry` | `otel::init_otlp(..)` — export the crate's `tracing` spans over OTLP. |
| `full` | all of the above. |

```toml
[dependencies]
amalgam = { version = "0.1", features = ["full"] }
```

The Redis adapters connect with an async constructor:

```rust,ignore
use amalgam::{Cache, RedisDistributedCache, RedisBackplane, JsonSerializer,
              DistributedCache, Backplane};
use std::sync::Arc;

let l2: Arc<dyn DistributedCache> =
    Arc::new(RedisDistributedCache::connect("redis://127.0.0.1/").await?);
let backplane: Arc<dyn Backplane> =
    Arc::new(RedisBackplane::connect("redis://127.0.0.1/").await?);

let cache: Cache<MyData> = Cache::builder()
    .distributed(l2)
    .serializer(Arc::new(JsonSerializer))
    .backplane(backplane)
    .instance_id("node-1")
    .build();
```

## Design notes

`amalgam` is a *type-driven* port: where FusionCache leans on .NET runtime type
info, exceptions, or `null`, `amalgam` uses Rust idioms that make whole bug
classes unrepresentable.

- **`Cache<V>`** is generic over one value type — no `dyn Any` downcasts.
- **`Timeout { Infinite, After(Duration) }`** replaces the `-1ms` sentinel.
- **`MaybeValue<V>` / `Result`** replace `null` / exceptions; a cache *miss* is
  never an error, and a fail-safe-rescued failure returns a value, not an `Err`.
- **`Clock`** is injected (`SystemClock` / `ManualClock`), so all expiration is
  deterministic in tests.
- `#![forbid(unsafe_code)]`.

## Status

The full FusionCache feature set is implemented: the L1 resiliency model
(stampede, fail-safe, soft/hard timeouts with background completion, eager refresh,
adaptive + conditional refresh, tagging) **plus** L1 + L2 + backplane with
read-through / write-through, multi-node invalidation and cross-node tag/clear
propagation, **circuit breakers**, **auto-recovery**, a **cross-node distributed
locker**, **plugins**, a **named-cache registry** with a **dynamic default-options
provider**, and an optional **Redis** backend (L2 + backplane + locker),
**MessagePack** serializer, and **metrics** plugin behind feature flags.

It is verified by a behavioural test oracle (`tests/behavior.rs`), end-to-end
multi-level tests (`tests/multilevel.rs`), feature/recovery tests, and Redis
integration tests run against a live server via `docker-compose.yml`: the default
suite (60 tests) is green and `cargo clippy` is warning-clean on the default build
*and* `--features full`; `#![forbid(unsafe_code)]`.

OpenTelemetry is supported: an always-on `tracing` span (`amalgam.get_or_set`)
works with any subscriber, and `otel::init_otlp(service, endpoint)` (feature
`opentelemetry`) exports spans over OTLP/gRPC to a collector such as Jaeger — try
`docker compose up -d` then `cargo run --example otel --features opentelemetry`.

Still roadmap (kept honest): a `Microsoft.Extensions.DependencyInjection`-style DI
integration (the registry is the Rust-idiomatic substitute); and serializers beyond
JSON / MessagePack (Protobuf / MemoryPack). See [`docs/PARITY.md`](docs/PARITY.md)
for the precise, row-by-row status of every feature.

## Acknowledgements

- **[FusionCache](https://github.com/ZiggyCreatures/FusionCache)** by Jody Donetti
  — the design this crate ports.
- The **C#→Rust porting methodology** ([`PORTING.md`](PORTING.md)) adapts the
  decision-table approach Bun used for its Zig→Rust AI port.
- Built on **[moka](https://github.com/moka-rs/moka)** and **[tokio](https://tokio.rs)**.
