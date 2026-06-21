# `amalgam` ⇄ FusionCache — Feature-Parity Reference

`amalgam` is a faithful, idiomatic Rust port of the resiliency model pioneered by
.NET's [FusionCache](https://github.com/ZiggyCreatures/FusionCache). It fuses an
in-memory **L1** cache (built on [`moka`](https://crates.io/crates/moka)) with an
optional distributed **L2** cache and a multi-node **backplane**, and reproduces
the behaviours that make a cache *robust* rather than merely fast — cache-stampede
protection, fail-safe (serve-stale-on-error), soft/hard factory timeouts with
background completion, eager (proactive) refresh, adaptive caching, conditional
(HTTP-style) refresh, and lazy tag invalidation — on top of `tokio`. Where a
FusionCache concept relies on .NET runtime type information, exceptions, or `null`,
`amalgam` substitutes the idiomatic Rust equivalent (a single-typed `Cache<V>`,
`Result`, `Option`/`MaybeValue`, a sum-typed `Timeout`), so a whole class of
misuse becomes a compile error rather than a runtime surprise.

> **Honesty note.** This table is the proof of "1:1 functional parity"; it does
> not overclaim. Read the status legend and the **[Build & test status](#build--test-status)**
> section before relying on any single row. The whole FusionCache feature set is
> now implemented and wired into the `Cache` request flow: cache-stampede
> protection, fail-safe, timeouts, eager refresh, adaptive/conditional refresh,
> tagging, **L1 + L2 + backplane** (read-through, write-through, multi-node
> invalidation, cross-node tag/clear propagation), **circuit breakers**,
> **auto-recovery**, a **cross-node distributed locker**, **plugins**, a **named-cache
> registry**, a **dynamic default-options provider**, and an optional **Redis**
> backend (L2 + backplane + locker), **MessagePack** serializer, and **metrics**
> plugin behind feature flags. The L2/backplane traits ship with in-memory /
> in-process reference backends so the default suite needs no external services;
> the production Redis backend lives behind the `redis` feature and is exercised by
> env-gated integration tests. What genuinely remains is narrow — see the
> [Still roadmap](#still-roadmap) subsection.

## Status legend

| Marker | Meaning |
|--------|---------|
| ✅ | Implemented & tested — present in `src/`, with a behavioural test in `tests/behavior.rs`/`tests/multilevel.rs` or a unit test in the owning module. Feature-gated rows (Redis, MessagePack, metrics) compile and are tested under their feature. |
| 🟡 | Implemented & wired, but the *default* build ships only an in-memory / in-process reference backend; a production backend (Redis) is available behind the `redis` feature. |
| ⬜ | Roadmap — a seam exists but no implementation ships yet. |

---

## Naming map

How a FusionCache concept is spelled in `amalgam`.

| FusionCache (.NET) | `amalgam` (Rust) | Notes |
|--------------------|------------------|-------|
| `IFusionCache` | `Cache<V>` | Generic over a single value type (see [divergences](#deliberate-idiomatic-divergences)). |
| `FusionCacheOptions` (per-cache) | `CacheBuilder<V>` | Cache-wide config via the builder. |
| `FusionCacheEntryOptions` (per-call) | `EntryOptions` | Per-entry knobs; `with_*` chainable setters. |
| `CacheItemPriority` | `Priority` | `Low` / `Normal` / `High` / `NeverRemove`. |
| `MaybeValue<T>` | `MaybeValue<V>` | "A value, or explicitly none"; converts to/from `Option<V>`. |
| `GetOrSetAsync` | `get_or_set` / `get_or_set_with` / `get_or_set_value` / `get_or_set_full` | The marquee read-through method. |
| `SetAsync` | `set` / `set_full` | Write-through write. |
| `TryGetAsync` | `try_get` | Read without running a factory; miss ⇒ `MaybeValue::none()`. |
| `GetOrDefaultAsync` | `get_or_default` | Read, or a caller-supplied default. |
| `RemoveAsync` | `remove` | Hard-remove from L1. |
| `ExpireAsync` | `expire` | Logical expiration (fail-safe may still serve). |
| `RemoveByTagAsync` | `remove_by_tag` / `remove_by_tags` | Lazy, marker-based tag invalidation. |
| `ClearAsync` | `clear(allow_fail_safe)` | Expire-all (fail-safe) or remove-all. |
| `IDistributedCache` (L2) | `DistributedCache` (trait) | Byte-oriented backend; `InMemoryDistributedCache` reference + `RedisDistributedCache` (feature `redis`). |
| `IFusionCacheSerializer` | `DistributedSerializer<V>` (trait) | `JsonSerializer` (serde_json) + `MessagePackSerializer` (feature `messagepack`). |
| L2 wire payload | `DistributedEntry<V>` | Value + freshness metadata, serialized across the wire. |
| `IFusionCacheBackplane` | `Backplane` (trait) | Multi-node notifications; `InProcessBackplane` reference + `RedisBackplane` (feature `redis`). |
| Backplane message | `BackplaneMessage` + `BackplaneAction` | Carries *what changed*, never the value. |
| `IFusionCacheDistributedLocker` | `DistributedLocker` (trait) | Cross-node single-flight; `InMemoryDistributedLocker` reference + `RedisDistributedLocker` (feature `redis`). |
| Auto-recovery queue | `AutoRecoveryService` + `RecoveryExecutor` | Latest-wins dedup queue + background drain; the cache is the executor. |
| Circuit breaker | `CircuitBreaker` | Time-based, lock-free; one for L2, one for the backplane. |
| `IFusionCachePlugin` | `Plugin` + `PluginHost` | Notified on every `CacheEvent` (and `on_start`). |
| Named/keyed caches (DI) | `CacheRegistry<V>` | Register/resolve caches by name. |
| `DefaultEntryOptionsProvider` | `DefaultEntryOptionsProvider` (trait) | Per-key dynamic default options. |
| `FusionCacheFactoryExecutionContext` | `FactoryContext<V>` | Handed to the factory; stale value, adapt, conditional outcomes. |
| `ctx.Fail(...)` | `ctx.fail(...)` | Signal a factory failure (triggers fail-safe). |
| `ctx.NotModified()` | `ctx.not_modified()` | Reuse the stale value (HTTP `304`). |
| `ctx.Modified(value)` | `ctx.modified(value).etag(..).last_modified(..).done()` | Produce a new value with conditional metadata. |
| Adaptive caching (`ctx.Options`) | `ctx.adapt(\|o\| ..)` / `ctx.options_mut()` | Mutate the produced entry's options. |
| Events hub (`cache.Events`) | `cache.events()` → `CacheEvent` over a broadcast channel | Observe hits/misses/fail-safe/… without blocking the hot path. |
| `TimeSpan` / `Timeout.InfiniteTimeSpan` | `std::time::Duration` / `Timeout::Infinite` | No `-1ms` sentinel — an explicit enum. |
| `IFusionCache` factory error | `Error::Factory` / `FactoryError` | `thiserror` typed errors; a miss is **not** an error. |

---

## Feature parity

| Feature | FusionCache | `amalgam` | Status | Notes |
|---------|-------------|-----------|--------|-------|
| **Read-through factory** | `GetOrSetAsync` | `get_or_set` / `get_or_set_with` / `get_or_set_value` / `get_or_set_full` | ✅ | Three arities: defaults; explicit `EntryOptions`; full (`options` + `tags` + `fail_safe_default`). |
| **Cache-stampede protection (single-flight)** | per-key factory coalescing | sharded async mutexes (`KeyedLock`), re-check L1 under the lock | ✅ | `stampede_runs_factory_once`: 32 concurrent callers ⇒ factory runs once. Sharded (bounded memory), not per-key map. |
| **Write-through set** | `SetAsync` | `set` / `set_full` | ✅ | `set_full` carries `EntryOptions` + tags. |
| **Try-get (no factory)** | `TryGetAsync` | `try_get` | ✅ | Miss ⇒ `MaybeValue::none()`; stale hidden unless `allow_stale_on_read_only`. |
| **Get-or-default** | `GetOrDefaultAsync` | `get_or_default` | ✅ | Never runs a factory. |
| **Remove** | `RemoveAsync` | `remove` | ✅ | Hard-removes from L1. |
| **Expire (logical)** | `ExpireAsync` | `expire` | ✅ | Marks stale; physical boundary kept for fail-safe. |
| **Fail-safe: serve stale on factory error** | `IsFailSafeEnabled` | stale fallback in `try_serve_fallback` | ✅ | `fail_safe_serves_stale_on_factory_error`. A rescued factory failure produces **no** `Error`. |
| **Fail-safe: logical vs physical expiration** | `FailSafeMaxDuration` | physical TTL = `max(duration, fail_safe_max_duration)` | ✅ | `physical_ttl_uses_max_not_sum` — it is `max`, **never** the sum. |
| **Fail-safe: throttle window** | `FailSafeThrottleDuration` | `Entry::throttled` re-serves stale for the throttle window | ✅ | Keeps the original physical boundary; resets the logical window. |
| **Fail-safe: default value** | `failSafeDefaultValue` | `fail_safe_default: MaybeValue<V>` (last resort) | ✅ | `fail_safe_default_used_when_no_stale`. |
| **Factory soft timeout** | `FactorySoftTimeout` | applies only with fail-safe **and** a fallback | ✅ | `soft_timeout_*` unit tests + `soft_timeout_returns_stale_then_completes_in_background`. |
| **Factory hard timeout** | `FactoryHardTimeout` | always applies; wins when shorter | ✅ | `hard_timeout_without_fallback_errors` ⇒ `Error::FactoryTimeout`. |
| **Timed-out factory background completion** | `AllowTimedOutFactoryBackgroundCompletion` | spawned factory keeps running, updates cache, holds the lock until done | ✅ | Stale returned immediately; background result replaces it. |
| **Eager (proactive) refresh** | `EagerRefreshThreshold` | non-blocking background refresh past the threshold | ✅ | `eager_refresh_triggers_background_update`; `try_lock` so it never stalls the caller. |
| **Adaptive caching** | mutate `ctx.Options` | `ctx.adapt(\|o\| ..)` / `ctx.options_mut()` | ✅ | `adaptive_caching_overrides_duration` — factory shortens its own entry's duration. |
| **Conditional refresh — not modified** | `ctx.NotModified()` | `ctx.not_modified()` reuses stale, bumps expiration | ✅ | `conditional_refresh_not_modified_*`; stale `ETag`/`LastModified` exposed to the factory. |
| **Conditional refresh — modified** | `ctx.Modified(v)` | `ctx.modified(v).etag(..).last_modified(..).done()` | ✅ | `conditional_refresh_modified_replaces_value`. |
| **Adaptive tagging** | `ctx` tag override | `ctx.set_tags(..)` / `.tags(..)` on the modified builder | ✅ | Overrides the tags passed to the call. |
| **Tagging — remove by tag (lazy)** | `RemoveByTagAsync` | per-tag timestamp markers in `TagRegistry`; checked on read | ✅ | `remove_by_tag_invalidates_matching_entries`. Markers are a typed registry, not magic keys. |
| **Remove-by-tag behaviour** | expire vs remove | `RemoveByTagBehavior::{Expire, Remove}` | ✅ | `Expire` is the FusionCache default (fail-safe may still serve). |
| **Clear (expire-all / remove-all)** | `ClearAsync` | `clear(allow_fail_safe)` via clear-markers | ✅ | `clear_removes_everything`. `true` ⇒ expire-all; `false` ⇒ remove-all + `invalidate_all`. |
| **Events** | `cache.Events.*` | `cache.events()` → `CacheEvent` broadcast stream | ✅ | `events_are_emitted`. See the [event list](#cacheevent-variants) below. |
| **Per-entry options surface** | `FusionCacheEntryOptions` | `EntryOptions` with FusionCache defaults | ✅ | Full surface incl. `skip_memory_*`, `skip_distributed_*`, `rethrow_*` (see [Defaults](#defaults)). |
| **Infinite vs finite timeout** | `Timeout.InfiniteTimeSpan` (`-1ms`) | `Timeout::{Infinite, After(Duration)}` | ✅ | Sum-typed; negative-duration footgun is unrepresentable. |
| **Validated eager threshold** | `float?` coerced to disabled | `EagerThreshold` newtype, open interval `(0, 1)` | ✅ | `eager_threshold_rejects_out_of_range`; out-of-range ⇒ `None` (disabled), not clamped. |
| **Expiration jitter** | `JitterMaxDuration` | `with_jitter_max`; applied to a fresh entry's logical expiration | ✅ | Anti-stampede across nodes. |
| **Key prefix** | `CacheKeyPrefix` | `key_prefix` (builder) | ✅ | Prepended to every key. |
| **Eviction priority** | `CacheItemPriority` | `Priority` | ✅ (carried) | moka's TinyLFU does not honour an explicit priority; retained for API parity and forwarded. |
| **Entry size weight** | `Size` | `with_size` | ✅ (carried) | Forwarded to the backend's size policy. |
| **Injectable clock / testable time** | `TimeProvider` | `Clock` trait (`SystemClock` / `ManualClock`) | ✅ | All expiration/throttle/timeout windows are deterministic in tests. |
| **L1 in-memory cache** | `MemoryCache` | `MemoryStore<V>` over `moka` (absolute expiry, optional capacity) | ✅ | Per-entry physical TTL via a custom `Expiry`. |
| **L1 + L2 hybrid (read/write-through)** | `IDistributedCache` | `DistributedCache` trait, wired into the flow | 🟡 | `get_or_set` does L2 read-through (a fresh L2 entry populates L1 and returns; a stale one becomes a fallback); writes go write-through to L1+L2. `l2_read_through_across_instances` proves a second instance serves from shared L2 without re-running the factory. `InMemoryDistributedCache` reference by default; `RedisDistributedCache` behind `redis`. |
| **L2 serialization** | `IFusionCacheSerializer` | `DistributedSerializer<V>` trait + `JsonSerializer` | ✅ | Object-safe; format pluggable. `DistributedEntry` wire envelope (`from_entry`/`into_entry`, `Entry::rehydrate`) round-trips and is exercised by the multilevel tests. `MessagePackSerializer` ships behind `messagepack`. |
| **Backplane (multi-node invalidation)** | `IFusionCacheBackplane` | `Backplane` trait, wired into the flow | 🟡 | Publishes `Set`/`Remove`/`Expire` on local changes; a background listener applies remote ones (evict/expire L1), filtering its own `instance_id`. `backplane_remove_invalidates_peer` + `backplane_set_makes_peer_repull_new_value`. `InProcessBackplane` reference by default; `RedisBackplane` (pub/sub) behind `redis`. |
| **L2 distributed timeouts** | `DistributedSoftTimeout` / `DistributedHardTimeout` | `with_distributed_timeouts(soft, hard)` | ✅ | `distributed_hard_timeout` is enforced on every L2 read (`read_l2_guarded`); a slow L2 reads back as a miss instead of stalling the caller. |
| **L2 error rethrow** | `ReThrowDistributedExceptions` | `rethrow_distributed_exceptions` | ✅ | Off by default; when on, an L2 backend error surfaces from `get_or_set` instead of degrading to a miss. |
| **(De)serialization error events** | serialization events | `SerializationError` / `DeserializationError` | ✅ | A serialize/deserialize failure on the L2 path emits the matching event (and is rethrown only if `rethrow_serialization_exceptions`, default `true`). |
| **L2 key wire-version prefix** | wire-format versioning | `distributed_wire_version` (builder, default `"v1"`) | ✅ | L2 keys are stored as `{version}:{key}` (`CacheInner::l2_key`) so incompatible cache versions can share one L2 without colliding. |
| **Multi-node tagging propagation** | tag markers over the backplane | reserved-key `Set` messages (`__amalgam:t:*`) | ✅ | `remove_by_tag` publishes a marker; receivers update their own `TagRegistry` (`apply_backplane`), so a tag invalidation on one node invalidates matching entries on every node. |
| **Multi-node clear propagation** | clear markers over the backplane | reserved-key `Set` messages (`__amalgam:clear:*`) | ✅ | `clear(allow_fail_safe)` publishes an expire-all / remove-all marker; receivers apply it to their tag registry (and `invalidate_all` for remove-all). |
| **Circuit breaker (L2)** | `DistributedCacheCircuitBreakerDuration` | `CircuitBreaker` + `distributed_circuit_breaker(Duration)` | ✅ | Gates every L2 op; trips on failure for the configured window, auto-closes after it, and fires `CircuitBreakerChange{Distributed, ..}`. `Duration::ZERO` (the FusionCache default) keeps it permanently closed. Unit-tested in `circuit::tests`. |
| **Circuit breaker (backplane)** | `BackplaneCircuitBreakerDuration` | `CircuitBreaker` + `backplane_circuit_breaker(Duration)` | ✅ | Same mechanism for backplane publishes; a *received* message proves health and closes it. Fires `CircuitBreakerChange{Backplane, ..}`. |
| **Auto-recovery (retry failed L2/backplane ops)** | auto-recovery queue | `AutoRecoveryService` + `auto_recovery(RecoveryConfig{..})` | ✅ | Failed L2 writes/removes and backplane publishes (and ops skipped while a breaker is open) are queued with latest-wins dedup (`max_items`, `max_retries`), then replayed by a background drain (`RecoveryExecutor::replay`, implemented by the cache). Enabled by default when an L2/backplane is configured; `RecoveryConfig{ enabled: false, .. }` opts out. |
| **Distributed locker (cross-node single-flight)** | `IFusionCacheDistributedLocker` | `DistributedLocker` + `distributed_locker(..)` | ✅ | Acquired *after* the local `KeyedLock`, so one factory runs per key cluster-wide; token-based (maps to `SET key token NX PX`). Per-entry `with_skip_distributed_locker`. `InMemoryDistributedLocker` reference + `RedisDistributedLocker` (`redis`). |
| **Plugins** | `IFusionCachePlugin` | `Plugin` + `PluginHost` + `plugin(Arc<dyn Plugin>)` | ✅ | Every plugin's `on_event` is called for each `CacheEvent` (and `on_start` at build); the cache skips the call entirely when no plugins are registered. |
| **Named caches & dynamic options** | keyed DI, `AddFusionCache` | `CacheRegistry<V>` + `DefaultEntryOptionsProvider` | ✅ | `CacheRegistry` registers/resolves caches by name (`get_or_create`); `default_options_provider(..)` supplies per-key defaults consulted (in `resolve_options`) whenever a call passes no explicit options. The Rust-idiomatic substitute for `Microsoft.Extensions.DependencyInjection` keyed registration. |
| **Metrics** | meters / counters | `MetricsPlugin` (feature `metrics`) | ✅ | A `Plugin` that records counters (hits, misses, sets, factory errors/timeouts, fail-safe activations, eager refreshes) via the `metrics` facade — exporter-agnostic (Prometheus, OTLP, …). |
| **Redis L2 adapter** | StackExchange.Redis `IDistributedCache` | `RedisDistributedCache` (feature `redis`) | ✅ | `GET`/`SET`/`DEL` on `redis::aio::ConnectionManager`, server-side TTL via `PX`. Env-gated integration tests (`AMALGAM_REDIS_URL`). |
| **Redis backplane adapter** | Redis pub/sub backplane | `RedisBackplane` (feature `redis`) | ✅ | `PUBLISH`/`SUBSCRIBE` over a compact wire format on a dedicated RESP3 connection, relayed to local subscribers; auto-resubscribes after reconnect. |
| **Redis distributed locker** | Redis lock | `RedisDistributedLocker` (feature `redis`) | ✅ | `SET key token NX PX`; release is an atomic compare-and-delete Lua script (never frees a lock re-taken by another node). |
| **Auto-clone of L1 values** | `EnableAutoClone` | `with_enable_auto_clone` | ✅ (inherent) | Reads already return an owned `V` (`value_cloned`), so a caller mutating the result never touches the cached copy — Rust satisfies this by construction. The flag exists for explicit parity. |
| **Observability — OpenTelemetry tracing spans** | OTel tracer + meters | `tracing` span `amalgam.get_or_set` + `metrics`-facade counters | ✅ | An always-on `#[tracing::instrument]` span per `get_or_set` (with `cache`/`key` fields) plus warn/debug events; export via `otel::init_otlp(service, endpoint)` (OTLP/gRPC → Jaeger, feature `opentelemetry`); counters via `MetricsPlugin` (feature `metrics`). |

### `CacheEvent` variants

The broadcast event enum (`#[non_exhaustive]`) covers the L1 set:
`Hit { stale }` · `Miss` · `Set` · `Remove` · `Expire` · `FactorySuccess` ·
`FactoryError` · `FactorySyntheticTimeout` · `FailSafeActivate` · `EagerRefresh` ·
`BackgroundFactorySuccess` · `BackgroundFactoryError` · `RemoveByTag` · `Clear` ·
`Eviction`,
plus the distributed/backplane set now that L2 + backplane are wired:
`CircuitBreakerChange { component, closed }` (with `CircuitComponent::{Distributed, Backplane}`) ·
`SerializationError` · `DeserializationError` · `MessagePublished` · `MessageReceived`.

### Builder methods (cache-wide config)

Available on `Cache::builder()`:
`.name()` · `.instance_id()` · `.key_prefix()` · `.default_options()` ·
`.default_options_provider(..)` · `.clock()` · `.max_capacity()` · `.lock_shards()` ·
`.remove_by_tag_behavior()` · `.events_capacity()` · `.distributed(..)` ·
`.serializer(..)` · `.backplane(..)` · `.distributed_locker(..)` · `.plugin(..)` ·
`.distributed_circuit_breaker(Duration)` · `.backplane_circuit_breaker(Duration)` ·
`.auto_recovery(RecoveryConfig)` · `.distributed_wire_version(..)` ·
`.ignore_incoming_backplane(bool)`.

> `.distributed()` + `.serializer()` enable L2; `.backplane()` enables multi-node
> invalidation. Either a backplane *or* an enabled `auto_recovery` spawns a
> background task, so `build()` must run inside a tokio runtime when one is
> configured.

---

## Build & test status

Verified state of the tree (`#![forbid(unsafe_code)]`):

- **Builds & lints clean — default *and* `--features full`.** `cargo build`,
  `cargo clippy --all-targets`, and `cargo test` all succeed with no warnings;
  `cargo clippy --all-targets --features full` (= `redis` + `messagepack` +
  `metrics`) and `cargo build --features full` are likewise warning-clean, so the
  feature-gated backends compile under their features.
- **Default suite is green: 48 tests.** 29 module unit tests + 16 behaviour
  (`tests/behavior.rs`) + 3 multilevel (`tests/multilevel.rs`), plus doctests
  (illustrative builder snippets that don't compile are marked `rust,ignore`).
- **L1 (in-memory) feature set — behavioural oracle.** `tests/behavior.rs` pins
  each FusionCache behaviour (stampede, fail-safe, soft/hard timeouts + background
  completion, eager refresh, adaptive, conditional refresh, tagging, clear, events,
  read-only stale); module unit tests cover `EntryOptions`, `Entry`, `TagRegistry`,
  `Timeout`/`Timestamp`, `MaybeValue`, `FactoryError`, `KeyedLock`, the L2 reference
  backend, the `CircuitBreaker`, and the `InMemoryDistributedLocker`.
- **L2 + backplane — wired and tested.** The traits (`DistributedCache`,
  `DistributedSerializer`, `Backplane`, `DistributedLocker`), the wire envelope
  (`DistributedEntry`), and the rehydration path are wired into
  `get_or_set`/`set`/`remove`/`expire` and covered end-to-end by
  `tests/multilevel.rs` (L2 read-through across instances; backplane remove/Set
  invalidation across peers). The default build uses the in-memory / in-process
  reference backends.
- **Circuit breakers, auto-recovery, distributed locker, plugins, registry,
  default-options provider — implemented and exercised** by unit tests and the
  multilevel/behaviour flow.
- **Redis backend — feature `redis`.** `RedisDistributedCache`, `RedisBackplane`,
  `RedisDistributedLocker` on `redis::aio::ConnectionManager`. Their 9 integration
  tests are env-gated: they no-op unless `AMALGAM_REDIS_URL` is set (so CI without a
  server stays green), e.g.
  `AMALGAM_REDIS_URL=redis://127.0.0.1/ cargo test --features redis`.
- **`messagepack` / `metrics` features** add `MessagePackSerializer` and
  `MetricsPlugin`; both compile and lint clean under `--features full`.

### Still roadmap

Kept honest — the small set that is *not* claimed as parity:

- **DI-container integration.** `CacheRegistry` + `DefaultEntryOptionsProvider` are
  the Rust-idiomatic substitute for named/keyed caches (see `examples/di.rs`);
  there is no `Microsoft.Extensions.DependencyInjection`-style `AddFusionCache`
  integration (Rust has no single canonical DI container).
- **Protobuf / MemoryPack serializers.** Three formats ship — `JsonSerializer`,
  `MessagePackSerializer` (feature `messagepack`), and `PostcardSerializer`
  (feature `postcard`, compact binary). Protobuf-net / MemoryPack are .NET-specific
  formats; their Rust analogues drop into the same `DistributedSerializer<V>` seam.

### Known minor differences

Faithful in behaviour, but narrower than FusionCache in these edge configs (none
affect the default single-cache deployment):

- **L2 soft timeout** is not separately enforced — only `distributed_hard_timeout`
  bounds L2 reads.
- **Single `lock_timeout`** rather than separate memory/distributed lock timeouts;
  `WaitForInitialBackplaneSubscribe` and a global `DisableTagging` are not exposed.

These are tracked deliberately rather than faked; each is an additive change behind
the existing seams.

> Closed since first audit: the Redis backplane channel is now configurable
> (`RedisBackplane::connect_with_channel`) so several caches can share one Redis,
> and the L2 key modifier supports `Prefix` / `Suffix` / `None`
> (`CacheBuilder::distributed_key_modifier_mode`).

---

## Deliberate idiomatic divergences

Where `amalgam` intentionally departs from a literal translation — each trades a
.NET idiom for a Rust one that makes a bug class unrepresentable.

- **Single-typed `Cache<V>` vs heterogeneous typed access.** FusionCache stores
  many value types in one instance and recovers the type at the call site (a
  runtime concern). `amalgam` is generic over one `V` per cache, so "wrong type
  for this key" cannot compile. Use several caches, a sum type, or
  `serde_json::Value` for heterogeneous values.
- **`Timeout` enum vs `-1ms` sentinel.** "No timeout" is `Timeout::Infinite`, not
  a negative `Duration`. Every illegal negative-duration state is unrepresentable,
  and `min` treats `Infinite` as the longest.
- **Tag markers as a typed registry vs magic keys.** Lazy "remove by tag" keeps
  FusionCache's marker-timestamp semantics, but stores markers in a dedicated
  `TagRegistry` instead of smuggling them through the value cache as
  `__fc:t:*` keys — no namespace collisions, no stringly-typed bookkeeping.
- **Events as a broadcast channel vs an events hub.** Observers `subscribe()` to a
  `tokio::sync::broadcast` stream of `CacheEvent`; emission never blocks the hot
  path, and a slow subscriber lags itself, never the cache. Events are
  observability, never a correctness mechanism.
- **`Result` / `MaybeValue` vs exceptions / `null`.** Business outcomes are in the
  type: a miss is `MaybeValue::none()` (distinct from "present but none"); a
  fail-safe-rescued factory failure yields a value and **no** `Error`; only an
  unrescued failure surfaces `Error::Factory` / `Error::FactoryTimeout`. Libraries
  use `thiserror`; there are no business-logic panics.
- **Injected `Clock` vs ambient `UtcNow`.** Domain logic never reads the wall
  clock directly; time flows from a `Clock` (`SystemClock` in production,
  `ManualClock` in tests), making every expiration/throttle/timeout deterministic.

---

## Defaults

`EntryOptions` mirrors FusionCache's defaults exactly.

| Option | Default | FusionCache equivalent |
|--------|---------|------------------------|
| `duration` (logical) | `30s` | `Duration` = 30s |
| `fail_safe_max_duration` | `1 day` | `FailSafeMaxDuration` = 1d |
| `fail_safe_throttle_duration` | `30s` | `FailSafeThrottleDuration` = 30s |
| `is_fail_safe_enabled` | `false` | off by default |
| `eager_refresh_threshold` | disabled (`None`); valid range open `(0, 1)` | `EagerRefreshThreshold` = null |
| `jitter_max` | `0` (none) | `JitterMaxDuration` = 0 |
| `lock_timeout` | `Infinite` | `FactoryHardTimeout`/lock = infinite |
| `factory_soft_timeout` / `factory_hard_timeout` | `Infinite` / `Infinite` | both infinite |
| `allow_timed_out_factory_background_completion` | `true` | true |
| `priority` | `Normal` | `CacheItemPriority.Normal` |
| `skip_memory_read` / `skip_memory_write` | `false` / `false` | false / false |
| `allow_stale_on_read_only` | `false` | stale hidden on plain reads |
| `rethrow_serialization_exceptions` | `true` | `ReThrowSerializationExceptions` = true |
| `rethrow_distributed_exceptions` / `rethrow_backplane_exceptions` | `false` / `false` | both false |
| `allow_background_distributed_operations` | `false` | false |
| `allow_background_backplane_operations` | `true` | true |
| `distributed_soft_timeout` / `distributed_hard_timeout` | `Infinite` / `Infinite` | both infinite |
| `skip_distributed_locker` | `false` | locker used when configured |
| `enable_auto_clone` | `false` (inherent in Rust — see [parity](#feature-parity)) | `EnableAutoClone` = false |
| `RecoveryConfig::delay` | `2s` (`enabled: true`, `max_items`/`max_retries` unbounded) | `AutoRecoveryDelay` = 2s |
| `distributed_wire_version` (cache-wide) | `"v1"` | wire-format version |
| `distributed_circuit_breaker` / `backplane_circuit_breaker` (cache-wide) | `Duration::ZERO` (disabled) | `…CircuitBreakerDuration` = 0 |

---

*Source of truth: the `amalgam` crate (`src/`, `tests/behavior.rs`,
`tests/multilevel.rs`). This document is hand-verified against that source; when
they disagree, the source wins — please update this file.*
