# Changelog

All notable changes to `amalgam` are documented here. The format is loosely based
on [Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

## [0.4.1] — 2026-10-08

### Documentation

- Put the measured 0.4.0/0.3.1 regression and FusionCache comparison tables
  directly in the README, with separate M4/Linux sources and explicit limits.
- Correct stale publication status in linked guides and identify historical
  0.3 audit timings separately from current release evidence.
- Preserve failed FC budgets, the second Linux run and the qualification Gaps.
- Rust implementation, dependencies, API and runtime behavior are unchanged
  from 0.4.0. This patch updates the packaged documentation on crates.io.

## [0.4.0] — 2026-10-08

### Breaking API changes

- Complete the lazy eight-operation facade: `get_or_set`, `try_get`,
  `get_or_default`, `set`, `remove`, `expire`, `remove_by_tag` and `clear`.
  `try_get` returns `Result<Option<V>>`; `get_or_default` returns `Result<V>`;
  mutations return `Result<()>`. Configure options, cancellation, tags and
  opt-in commit receipts fluently. Native callers use `execute()`.
- Remove legacy read/mutation overloads, error-discarding adapters and
  `MaybeValue`. Factories return ordinary `Result<V, E>`; `source::factory`
  preserves context inference and `source::value` supplies a constant origin.
  Use `try_build()` for fallible configured construction.
- Keep ordinary operations/options/errors in the root, provider contracts in
  `provider`, and opt-in receipts/stronger policies in `advanced`. Custom L2
  reads return immutable owned `DistributedBytes`; write inputs and snapshot
  wire formats retain their existing contracts.

### Availability and lifecycle

- Ordinary defaults use cooperative distributed ownership, retain eligible L1
  entries across backplane gaps, avoid periodic L1 clearing for L2-only caches,
  and do not wait for an initial subscription. `strict()` explicitly selects
  conservative reconciliation, subscription admission and fenced leases.
- `expire` removes L2 by default while retaining eligible L1 fail-safe data.
  Factory soft timeouts require usable stale data; a fail-safe default alone
  does not enable them. The default recovery delay is five seconds.
- Native `BlockingCache` and `BlockingRuntime` share cache state, providers,
  coalescing and final-owner lifetime with the async view. Preserve pending
  shared work after caller destruction, explicit cancellation, original causes,
  stable pinning, commit ownership and actual shutdown drainage.
- Preserve ReaderSlots, original-value lifetime/copy and marker contracts.
  Validate actual ReaderSlots with Loom, Miri and TSan; document the released
  parking dependency's Linux futex ABI limitation for contended Miri coverage.

### Validation and release scope

- Publish FR/RS statuses as Same, Diff with a reason, or Gap. Full paired
  FusionCache option coverage and arbitrary custom L2 marker parity remain
  explicit Gaps; this release does not claim complete FC equivalence.
- Measure FC 2.9 with normal tiering/Dynamic PGO and settled warmups; retain TC=0
  diagnostics, source identities and failed warmups/budgets. Report M4 and Linux
  separately. FC budgets are informational; the blocking performance guard uses
  the actual published crates.io 0.3.1 package with three counterbalanced pairs
  and a 5% noise allowance. FC-relative L2/set/Linux hot-hit/scaling work is
  deferred to 0.4.x.
- Replace raw benchmark JSON and inflated implementation journals with concise
  summaries. Exclude measurements/histories from Cargo archives and validate
  packaged consumers, default/full feature resolution and documentation links.

Migration and limits: [Migration guide](docs/MIGRATION_0_4.md),
[FR/RS matrix](docs/PARITY.md), [Performance](docs/PERFORMANCE.md) and
[Native API](docs/SYNC.md).

## [0.3.1] — 2026-10-05

### Fixed

- Heap-own mutation work before composing observation adapters. The 0.3.0 `set`, `remove`, and `clear` futures embedded roughly 55–68 KiB of state, causing a stack overflow when nested in traced backend refresh functions on ordinary test-thread stacks. Mutation futures now occupy about 1.3–1.5 KiB with unchanged cancellation, observation, and shutdown ownership.
- Add a public mutation-future size budget and a traced read/refresh regression on a 2 MiB thread stack. The ready L1 read path retains its existing representation.

## [0.3.0] — 2026-10-05

### Changed

- Shared typed origin/commit/lifecycle ownership, per-key locks, explicit cancellation and awaited shutdown.
- Fallible construction and reads, typed mutation receipts and observed background completion; legacy signatures remain adapters.
- Separate source/insertion ordering and L1/L2 lifetime, eager L2/lease coordination, zero/finite timeout handling.
- Durable namespaced invalidation, ordinary/passive hydration fences, exact recovery identities/stages, data-before-publication and stronger local replay ordering.
- Full L2 read deadlines include required marker validation; explicit tag/clear layer skips, honest stale diagnostics and circuit admission preserve policy.
- Dynamic plugin startup drains with teardown; uncertain known-token leases are cleaned; recovery releases extensions outside queue locks and resumes after malformed connected control frames.
- Delayed hydration preserves newer L1 identities and preferred stale values; private continuity eligibility survives copying/insertion races. Shutdown waits for the owning plugin session destructor after its last callback.
- Canonical stale reads recheck retention after I/O; legacy captured-fallback compatibility is tested against actual FusionCache 2.9.
- Actual fallible clone isolation, count/weight/priority retention, bounded resources and a common event/plugin route.
- Owned lease capabilities and token-checked fencing; real Redis subscription acknowledgement, reconnect continuity and cleanup.
- Source-preserving failures, bounded metrics, operation diagnostics and composable transactional OpenTelemetry initialization.
- Full-range clock arithmetic and minimum-version-compatible patched dependencies.
- Native portability, individual-feature, mandatory Redis, actual-package consumer and advisory CI gates.

### Migration

- Default distributed keys use the v2 Prefix namespace. New codecs accept legacy raw DTOs; running 0.2 readers cannot read new frames. Use a coordinated fresh namespace.
- Auto-clone needs a registered `ValueCloner`; ordinary `Clone` may share mutable state.
- Recovery queue defaults to 1024 items; explicit `max_items: None` retains unlimited admission.
- Custom atomic invalidation and strict lease guarantees require participating provider capabilities.
- Package name remains `amalgam-cache`; Rust library name remains `amalgam`.

See [contract and migration](docs/PARITY.md) and [validation](docs/AUDIT.md). Source delivery does not publish crates.io or deploy application consumers.

## [0.2.0] — distributed integrations

Introduced distributed integrations; subsequent audit corrections are recorded in 0.3. Historical feature additions do not establish complete behavioral equivalence.

### Added

- **Auto-recovery** — `AutoRecoveryService` queues failed L2 / backplane operations
  (latest-wins dedup by key, `max_items`, `max_retries`, background drain) and
  replays them when the dependency recovers. Builder: `.auto_recovery(RecoveryConfig)`.
- **Circuit breakers** — gate L2 and backplane operations and emit
  `CacheEvent::CircuitBreakerChange`. Builder: `.distributed_circuit_breaker(..)`,
  `.backplane_circuit_breaker(..)` (zero = disabled, the default).
- **Distributed locker** — cross-node single-flight via the `DistributedLocker`
  trait, wired into lock acquisition after the local lock. `InMemoryDistributedLocker`
  reference impl + Redis impl. Builder: `.distributed_locker(..)`; per-entry
  `with_skip_distributed_locker`.
- **Plugins** — the `Plugin` trait + `PluginHost`, notified on every event.
  Builder: `.plugin(..)`.
- **Named caches & dynamic options** — `CacheRegistry` and the
  `DefaultEntryOptionsProvider` trait (`.default_options_provider(..)`).
- **Multi-node tagging & clear** — tag/clear markers propagate across nodes over
  the backplane (reserved-key messages); receivers update their tag registry.
- **L2 distributed timeouts, rethrow & key versioning** — `distributed_hard_timeout`
  on L2 reads, `rethrow_distributed_exceptions`, `SerializationError`/
  `DeserializationError` events, and a wire-version key prefix
  (`.distributed_wire_version(..)`).
- **Backplane events & controls** — `MessagePublished`, `MessageReceived`,
  `ignore_incoming_backplane`.
- **Redis backend** (feature `redis`) — `RedisDistributedCache`, `RedisBackplane`,
  `RedisDistributedLocker` on `redis::aio::ConnectionManager`.
- **MessagePack serializer** (feature `messagepack`) — `MessagePackSerializer`.
- **Metrics** (feature `metrics`) — `MetricsPlugin` records counters via the
  `metrics` facade (exporter-agnostic).

### Notes

- Historical correction: an owned `V` returned by `Clone` can still share interior mutable state. 0.3 requires an actual copy strategy for auto-clone.
- Cargo features: `redis`, `messagepack`, `metrics`, and `full` (all three).

## [0.1.0] — L1 core

- `Cache<V>` with `get_or_set` (+ `_with` / `_full`), `set`, `try_get`,
  `get_or_default`, `remove`, `expire`, `remove_by_tag(s)`, `clear`.
- Cache-stampede protection (single-flight), fail-safe (logical vs physical
  expiration, throttle, default value), soft/hard factory timeouts with
  background completion, eager refresh, adaptive caching, conditional refresh,
  lazy tagging, events (broadcast `CacheEvent`).
- Full `EntryOptions` surface with FusionCache defaults; `Timeout` enum instead of
  the `-1ms` sentinel; validated `EagerThreshold`; injectable `Clock`.
- L1 (moka) + optional L2 (`DistributedCache` + `InMemoryDistributedCache`,
  `JsonSerializer`) + backplane (`InProcessBackplane`) reference implementations.
- `#![forbid(unsafe_code)]`.
