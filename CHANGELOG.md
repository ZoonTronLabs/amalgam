# Changelog

All notable changes to `amalgam` are documented here. The format is loosely based
on [Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

### Fixed

- Initialize reader-thread parking metadata during its first slot admission,
  before acquiring a slot. A warmed hit remains allocation-free when it first
  contends with a writer; waiting still returns the current entry.
- Synchronize eager-refresh and native-provider regressions with actual work
  completion and captured provider behavior, preserving their original assertions.

- Preserve the published shutdown cancellation reason when a caller drops nested work before shutdown reaches its registry entry; retain an existing cancellation during panic cleanup. Native provider callbacks still drain before shutdown completes.

- Typed original-value memory eviction subscriptions and physical reason facts;
  explicit insertion/retirement capture, bounded independent lag and deferred
  value reclamation through origin/lane guards. Independently locked unbounded
  L1 replaces Moka; bounded priority/capacity admission is retained. New closed
  enum variants require exhaustive consumers to handle their new cases.


- Add independent typed component event subscriptions with actual memory/L2 effects, complete backplane commands/envelopes, lazy payloads and explicit loss accounting. Preserve the existing logical event/plugin stream. Distinguish `DistributedTimeout` from transport failure so the selected read budget does not trip the Redis breaker; canonical deadline errors stay typed. Original-value eviction and configurable handler policy remain open; see `docs/LAYER_EVENTS.md`.

- Add `CachePlugin<V>`, `CachePluginContext<V>` and non-owning `PluginCache<V>` with the complete same-cache async/sync operation surface. Preserve interleaved legacy registration order. Stop uses bounded owned cleanup admission; final-owner closure, deferred startup/callback teardown, source failures and native Redis operations have public regressions. See `docs/PLUGIN_CACHE.md`.

- Add `ReconciliationPolicy::BackplaneBestEffort` to retain local/hydrated fresh and physically retained stale L1 over notification gaps/reconnects. Combined with cooperative ownership and suppressed locker errors it supports ordinary outage availability. Known invalidations, deadlines, cancellation and recovery ownership still apply; strict defaults are unchanged. See `docs/BACKPLANE_OUTAGES.md`.

- Clarify the existing Redis outage contract: strict fenced acquisition rejects errors even with locker rethrow disabled; cooperative foreground suppression and backplane L1 invalidation are separate policies. Add public outage-policy regressions.

- Avoid the local marker mutex until the first marker is observed. Publish that transition before changing marker state; tag/clear maxima and conservative compaction remain fully checked afterward. Public contracts cover the first revision, compacted clear fences and concurrent visibility.

- Sample custom L1 maintenance clocks before taking the retention mutex, so a clock can safely reenter storage. A public regression checks actual completion.


### Added

- Native `BlockingCache` and `BlockingRuntime` share cache state, coalescing, providers and final-owner lifetime with the async view. Caller-thread factories, bounded offloaded callbacks, cancellation, actual mutation receipts and awaited shutdown are supported; see `docs/SYNC.md` for explicit limits and reference differences.
- Typed runtime resource limits and synchronous factory nesting admission. Self-draining shutdown/flush returns `ReentrantDrain` before closing the cache; nested calls across runtimes use strictly increasing callback depths.
- Factory original key, current tags and stale tags; instance/provider inspection.
- Independent tag/clear defaults (`tags_default_options`) and default policy factory.
- Opt-in `MarkerReadPolicy::OptionsControlled`: independent secondary read options and observation bounds, per-marker deadlines/cancellation, typed control authority and peer-command events. CAS admission preserves newer facts, continuity fences reject old observations, and control checks stop at the first invalidation; see the field matrix in `docs/MARKER_READS.md`.
- Additional `MarkerLifecyclePolicy::CachedSnapshots`: validated expiring control observations, an optional atomic provider facet, nonzero miss/stale repair, independent L1/L2 lifetimes and zero factory budgets. Native memory/Redis renewals preserve newer facts and snapshot ages; Redis uses real TTL and exact integer frames. Foreground/background writes retain owned cancellation and drainage, original fault causes, finite outcome events and metrics. Durable journal facts never expire; snapshot recovery/population and remaining option combinations remain open. Participating repair now owns independent scoped acquisition, peer recheck and foreground/background release; strict repair cannot admit fresh L1 authority before atomic fencing succeeds.
- Owned marker eager refresh: bounded single flight, peer freshness/lifetime preflight, zero-wait lease participation, independent foreground factory budgets, cancellation and shutdown drainage. Skipped or suppressed failed reads can run a factory over known revisions; unknown absence remains explicit degraded authority. Independent writes on skipped reads are a documented improvement over the reference dependency on locker/backplane.
- Async complete-snapshot serializer contract and explicit sync/async preference, used by all L2/expire/replay paths. Existing synchronous codec implementations and snapshot overrides remain supported.
- Additive cooperative snapshot-codec hooks and `FactoryCancellation::check`. Every codec receives its owned operation signal; a linked L2 deadline scope publishes the exact timeout before dropping work without cancelling a later origin. Background factory/eager/passive/replay signals survive caller completion and end on owned shutdown.
- Cancellation returned by a codec is always propagated, independently of serialization/transport suppression policies; synchronous callbacks cannot commit after cancelling their caller.
- Explicit conditional-result builder with retain/replace/clear validator updates and typed absent-source rejection. It defaults to stale tags; existing adaptive `not_modified` remains a compatibility adapter.
- Explicit distributed expire policy: retain stale L2 or remove L2 while retaining stale L1, matching the released FusionCache reference for the latter.
- Public contracts for null versus miss, L2/clone/conditional/fail-safe null, codec cancellation, eager request tags and independent marker options.
- Source-preserving `CodecError`/`TransportError` with unchanged concrete causes and independent failure-policy classification; legacy message-only errors remain adapters.
- A sealed immutable copy capability for scalar/string/container values, selected with `immutable_values`, without admitting shared mutable allocations.
- Per-key providers can derive options from the owning cache's current default snapshot using `options_for_with_defaults`; legacy hooks remain supported.

### Changed

- Canonical read queries carry a lazy input instead of reserving the asynchronous preparation frame on every hit. They create owned asynchronous work only after a real miss, preserving cancellation and observation; explicit per-call option snapshots pay for their own storage.

- Built-in standalone caches use one UTC-anchored monotonic time sample for
  ready-read freshness and expiry. Local duration lifetimes remain steady across
  civil-clock corrections. Hybrid and external components, plus explicitly
  supplied clocks, retain their existing time model.
- In-memory shard routing uses a randomly keyed aHash builder. Full string
  equality still decides key identity; hashes are neither persisted nor sent
  between nodes.

- Memory-only caches select a plain read plan at construction. Unobserved hits
  carry no span or timing envelope; late subscribers still receive one terminal
  event. Thread-bound admission publishes its own count and releases it with a
  store, while transferred work and colliding thread indices retain independent
  atomic accounting. Borrowed guards cannot enter parked futures.
- Built-in L1 hits use padded reader slots sized for available parallelism,
  copy values under the slot without entry reference counting, borrow observers,
  and use striped shutdown admission. Custom callbacks and retired values remain
  outside storage locks; ordinary value `Clone` must not reenter the same cache.
- Tag invalidation reads immutable marker snapshots without taking the writer
  mutex. Background L1 maintenance rotates through shards with non-blocking
  write admission, while explicit maintenance still drains the whole store.
- Per-key option providers are resolved once and fresh eager hits return the
  current value immediately before scheduling refresh. Memory-only hits no
  longer wait for a backplane subscription.
- Native ready memory reads share the async admission path without entering
  the executor. Operations without observers skip timing; subscriptions added
  during an operation still receive completion with an unmeasured zero duration.
- Add paired public-API scaling fixtures and a CI gate against the locked
  FusionCache reference, with native reads, allocation checks and full reports.

- Observed operations transfer owned work before awaiting, keeping lookup futures small. Background pipelines start their owned receipt scope directly instead of embedding an unused foreground receipt future. Cancellation, completion events and shutdown drainage retain the same ownership.

- Cache orchestration is split into private API, builder, read, write, marker, recovery and runtime modules. Public `cache::Cache`/`cache::CacheBuilder` paths and cache field layout are preserved; see `docs/CACHE_INTERNALS.md`.

- Tag/clear operations now use separate marker defaults with foreground backplane completion rather than ordinary value defaults or key providers. Explicit operation options still take precedence. Durable marker lifetime is unchanged.
- Ready hits avoid key/event materialization without listeners; late plugin attachment during user callbacks remains observable.
- Empty plugin delivery avoids a registration read lock through a count published under the registration write lock; callback admission and shutdown drainage remain authoritative.
- Cancellation uses a single closed terminal state and Notify registration-before-check, preserving reasons and owned cancellation/drainage with fewer allocations.

### Fixed

- Supplied values use a constant origin: factory timeouts, eager origin work and factory events no longer apply. Factory-origin results retain the existing policies; four public regressions are checked against released FusionCache 2.9.0.
- Cache-owned executor lifetime remains valid after converting a native handle to an async view and dropping the native handle. Concurrent final drops drain actual work; cancelled queued callbacks release admission without waiting for another cache's running factory.
- Eager factories receive triggering request tags separately from saved stale tags.
- Redis backplane connection failures are classified as backplane failures, retaining the original Redis/timeout cause.
- Typed control-frame errors retain parsing/validation causes; malformed incoming Redis frames retain and log typed JSON/UTF-8/numeric failures before continuity reconciliation.
- Redis backplane stop is terminal across late connection acknowledgements and disconnect callbacks; shutdown serializes admission with its liveness barrier.

Full FusionCache functionality remains in progress; see [the complete inventory](docs/FULL_CONTRACT.md). These entries do not constitute a published package or consumer rollout.

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
