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
> section before relying on any single row. The in-memory (L1) feature set is
> complete with a behavioural test oracle. The distributed (L2) and backplane
> layers **are wired into the `Cache` request flow** (read-through, write-through,
> multi-node invalidation) and tested in `tests/multilevel.rs`, but ship with
> **in-memory / in-process reference backends**; production backends (Redis) are
> roadmap. Those rows are marked 🟡 to reflect "wired & tested, reference backend".

## Status legend

| Marker | Meaning |
|--------|---------|
| ✅ | Implemented & tested — present in `src/`, with a behavioural test in `tests/behavior.rs` (or a unit test in the owning module). |
| 🟡 | Wired & tested, but ships with an **in-memory / in-process reference backend** only — a production backend (e.g. Redis) is roadmap. |
| ⬜ | Roadmap — a trait seam exists so a production backend can be dropped in, but no such backend (and no wiring) ships today. |

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
| `GetOrSetAsync` | `get_or_set` / `get_or_set_with` / `get_or_set_full` | The marquee read-through method. |
| `SetAsync` | `set` / `set_full` | Write-through write. |
| `TryGetAsync` | `try_get` | Read without running a factory; miss ⇒ `MaybeValue::none()`. |
| `GetOrDefaultAsync` | `get_or_default` | Read, or a caller-supplied default. |
| `RemoveAsync` | `remove` | Hard-remove from L1. |
| `ExpireAsync` | `expire` | Logical expiration (fail-safe may still serve). |
| `RemoveByTagAsync` | `remove_by_tag` / `remove_by_tags` | Lazy, marker-based tag invalidation. |
| `ClearAsync` | `clear(allow_fail_safe)` | Expire-all (fail-safe) or remove-all. |
| `IDistributedCache` (L2) | `DistributedCache` (trait) | Byte-oriented backend; `InMemoryDistributedCache` reference impl. |
| `IFusionCacheSerializer` | `DistributedSerializer<V>` (trait) | `JsonSerializer` reference impl (serde_json). |
| L2 wire payload | `DistributedEntry<V>` | Value + freshness metadata, serialized across the wire. |
| `IFusionCacheBackplane` | `Backplane` (trait) | Multi-node notifications; `InProcessBackplane` reference impl. |
| Backplane message | `BackplaneMessage` + `BackplaneAction` | Carries *what changed*, never the value. |
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
| **Read-through factory** | `GetOrSetAsync` | `get_or_set` / `get_or_set_with` / `get_or_set_full` | ✅ | Three arities: defaults; explicit `EntryOptions`; full (`options` + `tags` + `fail_safe_default`). |
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
| **L1 + L2 hybrid (read/write-through)** | `IDistributedCache` | `DistributedCache` trait + `InMemoryDistributedCache` reference, wired into the flow | 🟡 | `get_or_set` does L2 read-through (a fresh L2 entry populates L1 and returns; a stale one becomes a fallback); writes go write-through to L1+L2. `l2_read_through_across_instances` proves a second instance serves from shared L2 without re-running the factory. In-memory reference backend (Redis ⬜). |
| **L2 serialization** | `IFusionCacheSerializer` | `DistributedSerializer<V>` trait + `JsonSerializer` | 🟡 | Object-safe; format pluggable. `DistributedEntry` wire envelope (`from_entry`/`into_entry`, `Entry::rehydrate`) round-trips and is exercised by the multilevel tests. |
| **Backplane (multi-node invalidation)** | `IFusionCacheBackplane` | `Backplane` trait + `InProcessBackplane` (broadcast) reference, wired into the flow | 🟡 | Publishes `Set`/`Remove`/`Expire` on local changes; a background listener applies remote ones (evict/expire L1), filtering its own `instance_id`. `backplane_remove_invalidates_peer` + `backplane_set_makes_peer_repull_new_value`. In-process reference backend (Redis ⬜). |
| **Redis L2 adapter** | StackExchange.Redis `IDistributedCache` | — | ⬜ | Trait seam ready (`DistributedCache`); no Redis backend ships. |
| **Redis backplane adapter** | Redis pub/sub backplane | — | ⬜ | Trait seam ready (`Backplane`); no Redis adapter ships. |
| **Auto-recovery (retry failed L2/backplane ops)** | auto-recovery queue | — | ⬜ | L2/backplane writes are currently best-effort (fire-and-forget); no retry queue yet. |
| **Distributed locker (cross-node single-flight)** | `IFusionCacheDistributedLocker` | — | ⬜ | Single-flight today is in-process (`KeyedLock`) only. |
| **Observability — OpenTelemetry / metrics / tracing spans** | OTel + meters | `tracing` is a dependency; structured spans/metrics not emitted | ⬜ | Events give in-process observability; no OTel export. |
| **Named caches & DI helpers** | keyed DI, `AddFusionCache` | — | ⬜ | Construct directly via `Cache::builder()`. |
| **Plugins** | `IFusionCachePlugin` | — | ⬜ | No plugin host. |
| **Auto-clone of L1 values** | `EnableAutoClone` | — | ⬜ | `V: Clone` is required; values are cloned on read by construction, but there is no opt-in deep-clone-on-get policy. |

### `CacheEvent` variants

The broadcast event enum (`#[non_exhaustive]`) covers:
`Hit { stale }` · `Miss` · `Set` · `Remove` · `Expire` · `FactorySuccess` ·
`FactoryError` · `FactorySyntheticTimeout` · `FailSafeActivate` · `EagerRefresh` ·
`BackgroundFactorySuccess` · `BackgroundFactoryError` · `RemoveByTag` · `Clear`.

> FusionCache also surfaces distinct distributed/backplane events (e.g. circuit
> breaker, backplane message received). Those are intentionally absent here until
> the L2/backplane layers are wired into the flow (see the 🟡 rows above).

### Builder methods (cache-wide config)

Available on `Cache::builder()`:
`.name()` · `.instance_id()` · `.key_prefix()` · `.default_options()` · `.clock()` ·
`.max_capacity()` · `.lock_shards()` · `.remove_by_tag_behavior()` ·
`.events_capacity()` · `.distributed(..)` · `.serializer(..)` · `.backplane(..)`.

> `.distributed()` + `.serializer()` enable L2; `.backplane()` enables multi-node
> invalidation (and spawns a listener task, so `build()` must run inside a tokio
> runtime when a backplane is configured).

---

## Build & test status

Verified state of the tree (`cargo clippy --all-targets` is warning-clean;
`#![forbid(unsafe_code)]`):

- **Builds & lints clean.** `cargo build`, `cargo clippy --all-targets`, and
  `cargo test` all succeed.
- **L1 (in-memory) feature set — implemented, with a behavioural test oracle.**
  `tests/behavior.rs` (16 tests) pins each FusionCache behaviour (stampede,
  fail-safe, soft/hard timeouts + background completion, eager refresh, adaptive,
  conditional refresh, tagging, clear, events, read-only stale); module unit tests
  (25) cover `EntryOptions`, `Entry`, `TagRegistry`, `Timeout`/`Timestamp`,
  `MaybeValue`, `FactoryError`, `KeyedLock`, and the L2 reference backend.
- **L2 + backplane — wired and tested, reference backends.** The traits
  (`DistributedCache`, `DistributedSerializer`, `Backplane`), reference backends
  (`InMemoryDistributedCache`, `JsonSerializer`, `InProcessBackplane`), the wire
  envelope (`DistributedEntry`), and the L1↔L2 rehydration path (`Entry::rehydrate`)
  are wired into `get_or_set`/`set`/`remove`/`expire` and covered end-to-end by
  `tests/multilevel.rs` (3 tests: L2 read-through across instances; backplane
  remove/Set invalidation across peers). Production backends (Redis) remain ⬜.
- **Totals:** 25 unit + 16 behaviour + 3 multilevel tests, all green, plus doctests.

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
| Auto-recovery delay | — (⬜ roadmap; FusionCache default `5s`) | `AutoRecoveryDelay` = 5s |

---

*Source of truth: the `amalgam` crate (`src/`, `tests/behavior.rs`). This document
is hand-verified against that source; when they disagree, the source wins — please
update this file.*
