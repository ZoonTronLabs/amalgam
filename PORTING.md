# PORTING.md — a C#→Rust porting guide (FusionCache → `amalgam`)

This document is the methodology used to port .NET's
[FusionCache](https://github.com/ZiggyCreatures/FusionCache) to idiomatic Rust as
the `amalgam` crate. It is a **C#→Rust** adaptation of the approach Bun used to
port its runtime **Zig→Rust** with AI agents (the `docs/PORTING.md` decision-table
that drove [oven-sh/bun#30412](https://github.com/oven-sh/bun/pull/30412)). The
ideas below are re-expressed in our own words and retargeted from Zig→Rust to
C#→Rust; none of Bun's text is reproduced here.

Why borrow that methodology at all? Because a faithful 1:1 port is not "rewrite
from the description" — it is a *translation* with a behavioural oracle. The Bun
effort showed that the leverage comes from four cheap, boring conventions, not
from cleverness:

1. a **deterministic decision table** so every ambiguous construct maps the same
   way every time;
2. an **up-front ownership decision** so lifetime/sharing calls are made once;
3. a **two-pass** flow — mechanical first, idiomatic second — with a small,
   greppable **flag vocabulary**;
4. the **original test suite as an immutable oracle** for behavioural equivalence.

It also showed the failure mode to engineer *out*: "class-level finds,
instance-level escapes" — auditing categories of bugs while individual instances
slip through — plus importing the source language's un-idiomatic shapes (Bun's
port carried ~13k `unsafe` blocks). We counter both explicitly below.

---

## 0. Provenance & ground rules

- **Source of truth:** the FusionCache source and docs. When this guide and the
  source disagree, the source wins; fix the guide.
- **Concurrency stance (the biggest C#→Rust flip):** FusionCache is `async`/`Task`
  first. We commit to **`tokio` + `async`/`await`** and forbid blocking the
  runtime in cache paths. (Bun *banned* async because the host owned the event
  loop; we do the opposite, but the doctrine "pin one concurrency model and
  forbid alternatives" still holds.)
- **Safety budget:** `#![forbid(unsafe_code)]` at the crate root. We start from a
  memory-safe source, so we **earn** Rust's guarantees rather than importing
  unsafety. If a future hot path needs `unsafe`, it gets a `// SAFETY:` block and
  a test — it does not get a blanket exception.
- **One file/module per source concept:** a FusionCache type maps to one Rust
  module (`FusionCacheEntryOptions` → `options.rs`, `MaybeValue<T>` → `maybe.rs`,
  the backplane → `backplane.rs`, …). Namespaces → module paths.

## 1. The flag vocabulary (small, controlled, greppable)

Every uncertainty is recorded in code with one of these, never left implicit:

| Marker | Meaning |
|---|---|
| `// TODO(port): <reason>` | a construct not yet confidently translated |
| `// PERF(port): <what>` | a C# perf idiom flattened to plain Rust; revisit under a profiler |
| `// PORT NOTE: <why>` | a deliberate divergence from the source's shape (so reviewers know it is intentional, not a logic change) |
| `// SAFETY: <invariant>` | required on any `unsafe` block (none today) |

Grep the tree for `TODO(port)`/`PERF(port)` to see all remaining risk at a glance.

## 2. The two-pass flow

- **Pass A — faithful draft.** Mirror the source's control flow and naming
  (re-cased to `snake_case`). Capture *intent* even if it isn't idiomatic yet.
  Flag anything unclear instead of guessing.
- **Pass B — make it Rust.** Resolve flags; replace exception flow with `Result`;
  replace `enum + nullable fields` with sum types; reshape only where the borrow
  checker forces it, tagging each reshape with `// PORT NOTE`.

The point of separating them is that Pass A is verifiable against the source
line-by-line, and Pass B is verifiable against the tests.

## 3. The ownership decision (made once, up front)

C# is reference-by-default; Rust forces a choice per field. Decide it *before*
writing code, not per call site. For `amalgam` the decisions were:

| Concern | Decision |
|---|---|
| Shared cache handle | `Cache<V> = Arc<CacheInner<V>>`; cloning shares one instance |
| Stored entry | immutable `Entry<V> = Arc<EntryInner<V>>`; "mutation" = copy-on-write + re-insert |
| Background tasks (timeout completion, eager refresh) | factory bound `Send + 'static`; the single-flight guard is an `OwnedMutexGuard` so it can move into a spawned task |
| Backplane listener | holds a `Weak<CacheInner<V>>` so it never keeps the cache alive (no leak) |

## 4. The idiom map (C# → Rust)

The heart of the guide: each ambiguous C# construct has one canonical Rust target.

| C# construct | Rust target | As applied in `amalgam` |
|---|---|---|
| `Exception` / `throw` / `try`-`catch` | `Result<T, E>` + `thiserror`; `?` to propagate | `error::Error` (typed, `#[non_exhaustive]`); factories return `Result<_, FactoryError>` |
| business failure vs bug | `Result` for business outcomes; `panic!` only for contract violations | a cache *miss* is `None`/`MaybeValue::none`, never an error; the factory failing is `Err`, not a panic |
| nullable `T?` / `out` params | `Option<T>` / tuple or `Result` returns | `MaybeValue<V>` for "value or explicitly none"; no `out` params |
| `enum Status` + "valid only sometimes" nullable fields | a sum type carrying only the data valid in each state | `Freshness {Fresh, Stale}`; `Timeout {Infinite, After(Duration)}`; `FactoryRun`, `LockOutcome` |
| `IDisposable` / `using` / `Dispose()` | `impl Drop` / RAII guard | single-flight lock guard releases on drop; background completion holds it until done |
| `Task` / `ValueTask` / `async`-`await` / `CancellationToken` | `Future` / `async` / `tokio` / timeouts via `tokio::time` | the whole `get_or_set` flow |
| events / `Action` / `Func` / `event` | `Box<dyn Fn>` or a channel | events as a `tokio::sync::broadcast` of `CacheEvent` (decoupled, non-blocking) |
| LINQ / `IEnumerable<T>` | iterator adapters | tag collection, fallback selection |
| generics + `where` constraints | generics + trait bounds | `Cache<V>` / `DistributedSerializer<V>` |
| `DateTimeOffset.UtcNow`, `Guid.NewGuid()` | injected, never called in domain logic | `Clock` trait (`SystemClock`/`ManualClock`); `Timestamp` newtype in 100ns ticks; ids via injected randomness at the boundary |
| `IDistributedCache` (byte store) | object-safe `async_trait` | `DistributedCache` (+ `InMemoryDistributedCache` reference) |
| magic-string keys (`__fc:t:*`) | a typed structure | `TagRegistry` of typed `Tag` markers, not strings smuggled through the value cache |
| `-1ms` "infinite" sentinel | an explicit variant | `Timeout::Infinite` |

## 5. Behaviour-preservation gotchas found during this port

These are the spots where "obvious" translations are wrong; each is pinned by a
test in `tests/behavior.rs`:

- **Physical TTL with fail-safe is `max(duration, fail_safe_max_duration)`, not
  the sum.** (`options::tests::physical_ttl_uses_max_not_sum`)
- **The soft timeout only applies when fail-safe is on *and* a fallback exists**;
  the hard timeout always applies and wins when shorter.
- **A timed-out factory keeps running** (when background completion is allowed):
  spawn it as a task and race a timer; *don't* `select!` it inline (that would
  cancel it on timeout).
- **A background factory failure does *not* re-activate fail-safe** — the
  throttled stale value already returned stands.
- **Tag invalidation is lazy and inclusive** (`entry_created <= marker`); a new
  entry created in the same tick as a `remove_by_tag` marker is also invalidated.
- **`rethrow_serialization` defaults to `true`** while every other `rethrow_*`
  defaults to `false`.

## 6. The oracle (don't let it move)

Behavioural equivalence is judged by tests, not by "looks right":

- `tests/behavior.rs` is the L1 oracle (stampede, fail-safe, timeouts, eager
  refresh, adaptive, conditional refresh, tagging, events).
- `tests/multilevel.rs` is the L2 + backplane oracle (read-through, cross-node
  invalidation).
- All time-dependent assertions run on an injected `ManualClock`, so they are
  deterministic — no `sleep`-and-hope for *expiration* logic (only real factory
  timeouts use the real timer, because those are wall-clock by nature).
- **Rule:** never weaken a test to make the port "pass". If behaviour must
  change, change the assertion deliberately and say why.

## 7. What got built in the full-parity pass

The original draft of this guide stopped at "L1 complete; L2/backplane wired with
reference backends; the rest is roadmap". A follow-up pass closed that gap and the
crate now implements the **full** FusionCache feature set. The same two-pass /
idiom-map discipline applied; each new module maps one FusionCache concept to one
Rust module, and each is wired into the `cache.rs` request flow (not left as a
dangling trait):

- **`circuit.rs`** — `CircuitBreaker` (time-based, lock-free) gating L2 and
  backplane ops; trips/auto-closes and drives `CacheEvent::CircuitBreakerChange`.
  `Duration::ZERO` ⇒ permanently closed (the FusionCache default).
- **`recovery.rs`** — `AutoRecoveryService` (latest-wins dedup queue, bounded
  `max_items`/`max_retries`, background drain) + the `RecoveryExecutor` trait, which
  `CacheInner` implements by re-doing the L2 write / backplane publish.
- **`distributed_lock.rs`** — the `DistributedLocker` seam (token-based, so a Redis
  `SET key token NX PX` maps cleanly) + `InMemoryDistributedLocker`; acquired after
  the local `KeyedLock` for cluster-wide single-flight.
- **`plugins.rs`** — `Plugin` + `PluginHost`, notified on every `CacheEvent`.
- **`registry.rs`** — `CacheRegistry` (named caches) + `DefaultEntryOptionsProvider`
  (per-key dynamic defaults); the Rust-idiomatic substitute for DI keyed caches.
- **`observability.rs`** — `MetricsPlugin` (feature `metrics`), recording counters
  via the `metrics` facade as a plugin.
- **`serializers.rs`** — `MessagePackSerializer` (feature `messagepack`).
- **`redis_backend.rs`** — `RedisDistributedCache` / `RedisBackplane` /
  `RedisDistributedLocker` (feature `redis`) on `redis::aio::ConnectionManager`;
  integration tests are env-gated on `AMALGAM_REDIS_URL`.

Multi-node **tag/clear** invalidation rides reserved-key backplane messages
(`__amalgam:t:*`, `__amalgam:clear:*`), and L2 keys carry a wire-version prefix
(`distributed_wire_version`, default `"v1"`).

## 8. Per-module status

Each source module records where it stands. Implemented & tested: `time`,
`maybe`, `error`, `options`, `tags`, `entry`, `events`, `memory`, `locking`,
`factory`, `cache` (the full `get_or_set` flow), `circuit`, `recovery`,
`distributed_lock`, `plugins`, `registry`. Implemented behind feature flags:
`observability` (`metrics`), `serializers` (`messagepack`), `redis_backend`
(`redis`). Reference implementations wired into the flow: `distributed` (L2 trait +
in-memory backend + JSON serializer, read/write-through), `backplane` (trait +
in-process backend + listener).

Genuinely remaining (see the "Still roadmap" section of `docs/PARITY.md`):
first-class OpenTelemetry tracing **spans** (today: `tracing` log lines at
factory-error/fail-safe plus `metrics`-facade counters), a DI-container
integration (the registry is the Rust-idiomatic stand-in), and serializers beyond
JSON / MessagePack. These are tracked as `TODO(port)` where they touch existing
code.
