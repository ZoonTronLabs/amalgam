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
ship for testing and single-process multi-instance setups; a Redis adapter is on
the roadmap.

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

The L1 resiliency model is complete and covered by a behavioural test oracle
(`tests/behavior.rs`); L2 + backplane are wired and tested end-to-end with
reference backends (`tests/multilevel.rs`). Roadmap: Redis L2/backplane adapters,
an auto-recovery retry queue, a cross-node distributed locker, and OpenTelemetry
export. See [`docs/PARITY.md`](docs/PARITY.md) for the precise status of every
feature.

## Acknowledgements

- **[FusionCache](https://github.com/ZiggyCreatures/FusionCache)** by Jody Donetti
  — the design this crate ports.
- The **C#→Rust porting methodology** ([`PORTING.md`](PORTING.md)) adapts the
  decision-table approach Bun used for its Zig→Rust AI port.
- Built on **[moka](https://github.com/moka-rs/moka)** and **[tokio](https://tokio.rs)**.
