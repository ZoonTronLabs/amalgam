# `amalgam` ⇄ FusionCache — Adversarial Parity Audit

**Auditor stance:** adversarial. The project's own `docs/PARITY.md` was treated as an
*overclaim* and ignored as evidence; every row below was re-derived from (a) FusionCache's
authoritative behavior — official docs under `ZiggyCreatures/FusionCache/main/docs/*.md`, the
`FusionCacheEntryOptions.cs` source, and deepwiki — and (b) `amalgam`'s actual source
(`src/*.rs`) and tests (`tests/*.rs`), cited by `file:line`.

**Scope note — two in-flight fixes are EXCLUDED from gap-scoring** (being added concurrently;
not counted against parity): configurable Redis backplane channel; `DistributedCacheKeyModifierMode`
`Suffix`/`None`.

---

## VERDICT

**Full functional port: YES (with 7 minor notes).** Every FusionCache capability is present
and behaviorally faithful on the load-bearing semantics (single-flight, fail-safe physical-TTL,
soft/hard timeouts + background completion, eager refresh, adaptive/conditional refresh, lazy
tagging, L1+L2 read/write-through, backplane, circuit breakers, auto-recovery, cross-node
locker). The divergences found are narrow edge-config or mechanism-vs-result differences — some
self-disclosed in PARITY.md's "Known minor differences" (L2 soft timeout, single lock_timeout),
others net-new from this audit (strict-vs-inclusive tag comparison, backplane evict-vs-refresh,
auto-recovery delay default, events-hub split) — **no blockers**. One genuine
1-tick correctness divergence (tag-marker inclusive vs strict comparison) and the
non-enforced L2 soft-timeout are the most substantive findings; both are minor.

**Findings: 0 blockers · 7 minor · 4 cosmetic.**

> ⚠️ **Tree state at audit time:** the working tree does **not compile** — `src/cache.rs:1516`
> omits the `distributed_key_modifier_mode` field that the in-flight `KeyModifierMode` edit added
> to `CacheInner` (`src/cache.rs:75`), and `CacheBuilder` has no setter for it yet
> (uncommitted changes in `cache.rs`/`lib.rs`/`options.rs`). This is precisely in-flight fix #2
> mid-application, so it is **not** scored as a gap — but the default suite cannot currently be
> run to green against the working tree. The committed `HEAD` is what the coverage assessment
> below relies on (read-verified; PARITY.md reports 48 green tests there). I did not run `git`
> or mutate the tree, per instructions.

---

## Feature parity table

Severity: **blocker** = breaks functional parity / data-correctness in a default deployment ·
**minor** = narrower than FusionCache in an edge/optional config, result still correct ·
**cosmetic** = behavior-equivalent, differs only in mechanism/naming/boundary-tick.

| Feature | FusionCache semantics (authoritative) | amalgam status | Evidence (`file:line`) | Severity | Note |
|---|---|---|---|---|---|
| Read-through factory `GetOrSet` | search L1→L2, else run factory; 1 factory/key | ✅ verified | `cache.rs:189-359` | — | 4 arities incl. `get_or_set_value` constant form (`cache.rs:164-178`). |
| Cache-stampede / single-flight | only 1 factory per key runs concurrently | ✅ verified | `cache.rs:232-255`, `locking.rs:33-66` | — | Sharded async mutexes; re-checks L1 + L2 under lock. Test `behavior.rs:30`. |
| Write-through `Set` | write L1 (+L2 if present), notify backplane | ✅ verified | `cache.rs:364-382`, `655-669` | — | `set_full` carries opts+tags. |
| `TryGet` (no factory) | miss⇒none; stale hidden unless opted in | ✅ verified | `cache.rs:386-418` | — | `allow_stale_on_read_only` gate (`cache.rs:406`). Test `behavior.rs:451`. |
| `GetOrDefault` | read or caller default, no factory | ✅ verified | `cache.rs:421-428` | — | |
| `Remove` | hard-remove L1+L2, evict peers | ✅ verified | `cache.rs:431-439` | — | |
| `Expire` (logical) | mark stale, keep physical for fail-safe | ✅ verified | `cache.rs:443-458`, `entry.rs:258-272` | — | Propagates to L2 + backplane. |
| **Fail-safe: serve stale on factory error** | reuse expired value, no exception bubbles | ✅ verified | `cache.rs:308-333`, `959-993` | — | Rescued failure yields a value + **no** `Error`. Test `behavior.rs:59`. |
| **Fail-safe: physical TTL** | physical = `FailSafeMaxDuration` (= `max(Duration, FSMax)` in sane config); **never the sum** | ✅ verified | `options.rs:533-540` | — | `logical.max(fail_safe_max)`. See "Confirmed matches" — amalgam is *more* robust on the inverted `Duration>FSMax` case than FC's literal prose. Test `options.rs:611`, `entry.rs:341`. |
| **Fail-safe: throttle window** | re-serve stale for `FailSafeThrottleDuration`, keep **original** physical boundary | ✅ verified | `entry.rs:225-252` | — | `physical_expiration = source.physical_expiration` (`entry.rs:232`); only logical reset (`entry.rs:234`). Test `entry.rs:355`. |
| Fail-safe default value | last-resort `failSafeDefaultValue` when no stale | ✅ verified | `cache.rs:983-991`, `entry.rs:308-325` | — | `MaybeValue<V>`. Test `behavior.rs:83`. |
| **Factory soft timeout** | applies only with a stale fallback (⇒ fail-safe on + value cached) | ✅ verified | `options.rs:560-566` | — | `is_fail_safe_enabled && has_fallback && soft!=∞`. Tests `options.rs:635-660`. |
| **Factory hard timeout** | applies "in any case", wins when shorter | ✅ verified | `options.rs:565`, `cache.rs:353-356` | — | `selected.min(hard)`. ⇒`Error::FactoryTimeout`. Test `behavior.rs:158`. |
| **Timed-out factory bg completion** | `AllowTimedOutFactoryBackgroundCompletion`=**true** default; keeps running, updates cache | ✅ verified | `cache.rs:334-340`, `1010-1041`, `1247-1278` | — | Holds single-flight guard until bg resolves (`cache.rs:1018-1019`). Default true (`options.rs:158`). Test `behavior.rs:110`. |
| Background factory failure ⇒ no fail-safe re-activation | FC docs **silent**; amalgam: throttled stale already returned stands | ⚠️ asserted-by-amalgam | `cache.rs:1025-1032` | cosmetic | Reasonable; not doc-confirmed (FC `Timeouts.md` does not cover the path). Code-only assertion, no test. |
| **Eager refresh** | `EagerRefreshThreshold`∈(0,1) request-driven; return valid value now, refresh in bg | ✅ verified | `cache.rs:211-220`, `entry.rs:171-179`, `options.rs:589-594` | — | `try_lock` so never stalls caller (`cache.rs:1054`). Off by default (`options.rs:148`). Test `behavior.rs:179`. |
| Validated eager threshold | out-of-range coerced to disabled | ✅ verified (stronger) | `options.rs:67-85` | — | `EagerThreshold` newtype, `(0,1)` open; out-of-range⇒`None`. Test `options.rs:601`. |
| **Adaptive caching** | factory mutates `ctx.Options` (e.g. Duration) for this entry | ✅ verified | `factory.rs:93-116`, `cache.rs:628-651` | — | `ctx.adapt(\|o\|..)` / `options_mut`. Produced opts drive expiration. Test `behavior.rs:271`. |
| **Conditional refresh — NotModified** | reuse stale as fresh; **bump expiration** a full Duration | ✅ verified | `factory.rs:196-211`, `cache.rs:628-651` | — | `not_modified()`⇒`reused_stale`; `store_product` re-creates fresh entry ⇒ expiration bumped. stale ETag/LastModified exposed (`factory.rs:131-140`). Test `behavior.rs:307`. |
| Conditional refresh — Modified | new value + ETag/LastModified | ✅ verified | `factory.rs:174-182`, `233-269` | — | `modified(v).etag(..).last_modified(..).done()`. Test `behavior.rs:351`. |
| Adaptive tagging | `ctx` tag override | ✅ verified | `factory.rs:144-156`, `247-254` | — | `set_tags` / `.tags()` override call tags. |
| **Tagging — remove-by-tag (lazy)** | per-tag timestamp marker; entry invalid when **marker > created** (strict `<`) | ⚠️ partial | `tags.rs:145,151,160` | **minor** | amalgam uses **inclusive** `created <= marker`; FC uses **strict** `created < marker` ("greater than the timestamp at which the cache entry has been created"). 1-tick boundary divergence. See gaps. Test `behavior.rs:379`. |
| Remove-by-tag behavior | expire (default) vs remove | ✅ verified | `options.rs:36-44`, `tags.rs:152-157` | — | `Expire` default (`options.rs:40`). |
| Clear (expire-all / remove-all) | FC: special internal `*` tag | ⚠️ partial | `cache.rs:490-502`, `tags.rs:120-128` | cosmetic | amalgam uses dedicated `clear_expire`/`clear_remove` markers, not a `*` tag — same effect, different structure (a deliberate idiomatic divergence). Test `behavior.rs:427`. |
| Events hub | rich event set on bg threads; FC splits into general/`.Memory`/`.Distributed` hubs | ⚠️ partial | `events.rs:13-176`, `cache.rs:519-524` | cosmetic | 22 `CacheEvent` variants over a **single** broadcast (level encoded in the variant) vs FC's 3 hubs — info-equivalent, routing differs (gap #8). Test `behavior.rs:468`. |
| Per-entry options surface + defaults | `FusionCacheEntryOptions` | ✅ verified | `options.rs:92-182` | — | Defaults match (see "Defaults cross-check"). |
| Infinite vs finite timeout | `Timeout.InfiniteTimeSpan` (`-1ms`) | ✅ verified (stronger) | `time.rs:171-220` | — | Sum-typed `Timeout`; negative-duration footgun unrepresentable. |
| Expiration jitter | `JitterMaxDuration` | ✅ verified | `options.rs:570-584` | — | Applied to fresh logical expiration. |
| Key prefix | `CacheKeyPrefix` | ✅ verified | `cache.rs:512-517` | — | |
| Eviction priority | `CacheItemPriority` | ⚠️ carried-only | `options.rs:21-32` | cosmetic | moka TinyLFU ignores explicit priority; retained for API parity (honestly disclosed). |
| Entry size weight | `Size` | ⚠️ carried-only | `options.rs:285-288` | cosmetic | Forwarded to size policy; tracked. |
| Injectable clock | `TimeProvider` | ✅ verified (stronger) | `time.rs:98-163` | — | `Clock` trait; domain never reads wall clock. |
| L1 in-memory | `MemoryCache` | ✅ verified | `memory.rs:1-103` | — | moka + per-entry physical TTL via custom `Expiry` (`memory.rs:21-45`), absolute (no sliding). |
| **L1+L2 read/write-through** | L2 hit populates L1; writes go L1+L2 | ✅ verified | `cache.rs:257-293`, `655-704` | — | Fresh L2⇒populate L1 + return (`cache.rs:274-288`); stale L2⇒fallback. Test `multilevel.rs:20`. |
| L2 serialization | `IFusionCacheSerializer` (mandatory for L2) | ✅ verified | `distributed.rs:88-117`, `serializers.rs` | — | `DistributedEntry` wire envelope round-trips (`distributed.rs:48-81`). JSON default; MessagePack/Postcard gated. |
| **Backplane (multi-node invalidation)** | Set⇒nodes-with-entry **refresh from L2**; Remove⇒evict; Expire⇒mark-expired | ⚠️ partial | `cache.rs:880-927` | **minor** | amalgam on incoming **Set evicts L1** (re-pull on next read; `cache.rs:922-925`) rather than actively refreshing present entries. Eventual result equivalent; mechanism differs (lazy vs FC's eager passive update). Remove (`910-912`) + Expire (`913-921`) match. Self-filters own id (`cache.rs:942`). Tests `multilevel.rs:74,114`. |
| L2 distributed timeouts | `DistributedCacheSoftTimeout` / `HardTimeout` | ⚠️ partial | `cache.rs:708-745` | **minor** | Only **hard** timeout enforced on L2 read (`cache.rs:718-724`); **soft** L2 timeout is accepted into options (`options.rs:464-466`) but never applied. Self-disclosed. See gaps. |
| L2 error rethrow | `ReThrowDistributedCacheExceptions` (false default) | ✅ verified | `cache.rs:736-742`, `options.rs:169` | — | Transport vs (de)serialization split honoured. |
| (De)serialization error events | serialization events | ✅ verified | `cache.rs:766-777`, `events.rs:103-116` | — | `rethrow_serialization_exceptions` default true (`options.rs:170`). |
| L2 key wire-version | wire-format versioning | ✅ verified | `cache.rs:1092-1099` | — | `{version}:{key}`. (Suffix/None mode in-flight — excluded.) |
| Multi-node tag propagation | tag markers over backplane | ✅ verified | `cache.rs:462-472`, `866-907` | — | Reserved-key `Set` markers (`__amalgam:t:*`) update peer `TagRegistry`. Test `features.rs:285`. |
| Multi-node clear propagation | clear markers over backplane | ✅ verified | `cache.rs:490-502`, `898-906` | — | Reserved `__amalgam:clear:*` markers. |
| **Circuit breaker (L2)** | `DistributedCacheCircuitBreakerDuration` (0 default = off) | ✅ verified | `circuit.rs:1-122`, `cache.rs:678-681,714` | — | Gates L2 ops; trips/auto-closes; fires `CircuitBreakerChange{Distributed}`. 0⇒permanently closed (`circuit.rs:37-39`). Test `features.rs:195`. |
| Circuit breaker (backplane) | `BackplaneCircuitBreakerDuration` | ✅ verified | `cache.rs:828-831`, `886-888` | — | Received message proves health ⇒ closes (`cache.rs:888`). |
| **Auto-recovery** | bounded latest-wins dedup queue + retry + post-reconnect barrier; `EnableAutoRecovery`=true, `AutoRecoveryDelay`=**2s**, max-items/retries=null default | ⚠️ partial | `recovery.rs:1-223`, `cache.rs:804-814,1151-1175` | **minor** | Mechanics match: dedup newest-wins (`recovery.rs:126-128`); `max_items` evict-soonest (`recovery.rs:130-138,147-158`); `max_retries` budget (`recovery.rs:209-221`); item TTL 600s (`cache.rs:1184`); enabled + unbounded defaults match. **But `delay` default is `5s` not FC's `2s`** (`recovery.rs:61`) — see gap #7. Tests `recovery.rs` (5 cases) + `features.rs:195`. |
| **Distributed locker (cross-node single-flight)** | `IFusionCacheDistributedLocker` | ✅ verified | `cache.rs:594-624`, `distributed_lock.rs` | — | Acquired **after** local `KeyedLock` ⇒ one factory/key cluster-wide; token-based (maps to `SET NX PX`). Test `features.rs:95`. |
| Plugins | `IFusionCachePlugin` | ✅ verified | `plugins.rs:1-64`, `cache.rs:519-524` | — | `on_event` per `CacheEvent` + `on_start`; skipped when none. Test `features.rs:55`. |
| Named caches & dynamic options | keyed DI, `DefaultEntryOptionsProvider` | ✅ verified | `registry.rs:1-80`, `cache.rs:528-538` | — | `CacheRegistry` + provider consulted in `resolve_options`. Tests `features.rs:370,433`. |
| Metrics | meters/counters | ✅ verified (feature) | `observability.rs:7-69` | — | `MetricsPlugin` via `metrics` facade (feature `metrics`). |
| OpenTelemetry | OTel tracer/meters | ✅ verified (feature) | `cache.rs:183-188`, `otel.rs` | — | `#[tracing::instrument]` span `amalgam.get_or_set`; OTLP export (feature `opentelemetry`). |
| Redis L2 / backplane / locker | StackExchange.Redis adapters | ✅ verified (feature, env-gated) | `redis_backend.rs:100-340` | — | `GET/SET/DEL` PX TTL; pub/sub RESP3 relay; `SET NX PX` + Lua compare-del release. Channel fixed (in-flight — excluded). |
| Auto-clone of L1 values | `EnableAutoClone` | ✅ verified (inherent) | `options.rs:444-460` | — | Reads return owned `V` (`entry.rs:185-187`); inherent in Rust. |

---

## Confirmed behavioral matches (tricky semantics verified against source)

Each cross-checked against FusionCache authoritative behavior **and** amalgam source:

1. **Physical TTL = `max(duration, fail_safe_max)`, never the sum.** FusionCache's
   `FusionCacheEntryOptions.GetMemoryAbsoluteExpiration` computes
   `physicalDuration = (FailSafeMaxDuration < durationToUse) ? durationToUse : FailSafeMaxDuration`
   — literally `max(Duration, FailSafeMaxDuration)` — and `Options.md` states "the duration in the
   cache will be `FailSafeMaxDuration`" (true because the sane default `FailSafeMaxDuration`=1d ≥
   `Duration`=30s). amalgam: `options.rs:533-540` `if is_fail_safe_enabled { logical.max(fail_safe_max) } else { logical }`.
   **Match — and amalgam is strictly more robust** on the inverted `Duration > FailSafeMaxDuration`
   case (it keeps the entry physically alive for the logical window; FC's literal prose would drop
   it early). Test `options.rs:611-632` asserts both `max(10,100)=100` and `max(200,100)=200≠300`.

2. **Soft timeout only with fail-safe + a fallback value.** `options.rs:560-566`:
   `is_fail_safe_enabled && has_fallback && !soft.is_infinite()` selects soft, else `Infinite`;
   then `.min(hard)`. Matches FC `Timeouts.md` ("to be used if there's an expired cache entry to
   use as a fallback"). `has_fallback` = stale entry present OR fail_safe_default present
   (`cache.rs:298`). Tests `options.rs:635,646,663`.

3. **Timed-out factory keeps running in the background and updates the cache; holds the
   single-flight lock until done.** `run_factory` spawns the factory for finite timeouts so it
   outlives the timeout (`cache.rs:1260-1276`); on soft-timeout the stale value is served
   immediately (`cache.rs:341-350`) and `spawn_background_completion` keeps the guard alive until
   the bg factory resolves (`cache.rs:1010-1019`), then `store_product` replaces the value.
   Default `allow_timed_out_factory_background_completion=true` (`options.rs:158`). Matches FC
   default-on behavior. Test `behavior.rs:110-156`.

4. **Background factory failure does NOT re-activate fail-safe.** `cache.rs:1025-1032`: a bg
   `Err` only emits `BackgroundFactoryError`; the already-returned throttled stale stands. FC docs
   are silent on this path, so this is amalgam's (reasonable) design assertion, not doc-confirmed.

5. **Tag invalidation is lazy & marker-based.** `remove_by_tag` writes a per-tag timestamp
   (`tags.rs:105-110`) and never scans entries; reads evaluate via `TagRegistry::evaluate`
   (`cache.rs:545-549`, `tags.rs:137-165`). Matches FC's `__fc:t:<tag>` marker model — amalgam
   stores them in a typed `DashMap` instead of magic value-cache keys (deliberate divergence).
   **Caveat:** comparison is inclusive (see gap #1).

6. **Fail-safe throttle keeps the original physical boundary.** `Entry::throttled`
   (`entry.rs:225-252`) sets `physical_expiration = source.physical_expiration` and only
   `logical_expiration = now + throttle`; returns `None` if already physically dead
   (`entry.rs:229`). Matches FC ("put back … with a new cache duration of
   `FailSafeThrottleDuration`" while `FailSafeMaxDuration` still caps total retention). Test
   `entry.rs:355-378`.

7. **Conditional-refresh `NotModified` bumps expiration.** `not_modified()` returns a product
   with `reused_stale=true` carrying the stale value + its ETag/LastModified (`factory.rs:196-211`);
   `store_product` builds a **fresh** `Entry` at `now` (`cache.rs:628-641`), so the entry lives
   another full Duration. Matches FC. Test `behavior.rs:307-349` asserts the value is fresh again
   afterward.

8. **L2 read-through populates L1.** A fresh L2 entry is inserted into L1 then returned
   (`cache.rs:274-288`); a stale L2 entry becomes a (possibly newer) fail-safe fallback
   (`cache.rs:289`), choosing the later-created of L1/L2 stale (`newer_of`, `cache.rs:1558-1563`).
   Test `multilevel.rs:20-72` (second instance serves from shared L2, factory runs once).

9. **Backplane effects.** Remove⇒evict L1 (`cache.rs:910-912`); Expire⇒logical-expire L1 keeping
   physical (`cache.rs:913-921`); a node ignores its own messages by `source_id`/`instance_id`
   (`cache.rs:942-943`). Tag/clear markers ride reserved-key `Set` messages and update the peer
   `TagRegistry` (`cache.rs:890-907`). **Caveat:** incoming *value* `Set` evicts rather than
   eagerly refreshes (see gap #2).

10. **Auto-recovery dedup / retry / bounds.** Latest-timestamp wins on enqueue
    (`recovery.rs:126-128`); when `max_items` full, evict the soonest-expiring item that expires
    before the newcomer's bound, else reject (`recovery.rs:130-138`, `147-158`); `max_retries`
    decremented on failure, dropped at 0 (`recovery.rs:209-221`); succeeded items removed, failed
    kept (`recovery.rs:199-205`). Tests `recovery.rs:107-248` cover all four.

11. **Circuit-breaker gating.** L2 read/write/remove and backplane publish all short-circuit when
    the relevant breaker is open and enqueue a recovery item instead
    (`cache.rs:678-681,714,753-755,828-831`); zero-duration breaker = permanently closed (FC
    default, `circuit.rs:37-39,45-48`). A received backplane message closes the backplane breaker
    (`cache.rs:888`). Tests `circuit.rs:88-121`, `features.rs:195-279`.

12. **Distributed locker = cross-node single-flight, acquired AFTER the local lock.** The local
    `KeyedLock` is taken first (`cache.rs:571-593`), then the distributed lock
    (`cache.rs:594-598`, `603-624`); both released on guard drop (`cache.rs:1197-1218`). One
    factory per key cluster-wide. Test `features.rs:95-159` (two instances, shared locker, factory
    runs once).

---

## Genuine gaps / divergences (excluding the two in-flight fixes)

### 1. Tag-marker comparison is inclusive (`created <= marker`); FusionCache is strict (`created < marker`) — **minor**
`tags.rs:145` (`created <= clear_remove`), `:151` (`created <= marker`), `:160`
(`created <= clear_expire`). FusionCache's `Tagging.md`: an entry is expired when the marker
"is **greater than** the timestamp at which the cache entry has been created" — i.e. strict
`marker > created` ⇔ `created < marker`. Consequence: an entry created in the **same tick** as the
invalidation marker is dropped by amalgam but **kept** by FusionCache. In practice both use
100ns-tick wall-clock timestamps, so a real collision is vanishingly rare, and amalgam's own tests
deliberately advance the clock around the marker to avoid the boundary (`features.rs:324-335`,
`behavior.rs:403-405`) — which both proves the authors know about the boundary and means the
inclusive bias is never exercised adversarially.
**Recommendation:** change the three comparisons to strict `<` to match FusionCache exactly, then
add a same-tick boundary test (entry created at `marker` tick must remain **valid**). Low effort,
removes the only genuine correctness divergence.

### 2. Incoming backplane value-`Set` evicts L1 instead of eagerly refreshing from L2 — **minor**
`cache.rs:922-925`: on a remote `Set`, amalgam removes the local L1 copy and lets the next read
re-pull from L2. FusionCache (`Backplane.md`) "immediately updates the data in L1 (taken from L2)
but ONLY on the nodes where the cache entry is already present on their L1." The **eventual
observable result is identical** (peers converge on the new value), but FC refreshes proactively
whereas amalgam refreshes lazily-on-next-read. Under a read-heavy multi-node workload this adds one
extra L2 round-trip per peer per update on first post-invalidation read.
**Recommendation:** optionally, on a received `Set` for a key currently present in L1, kick a
background L2 read-through to repopulate (mirroring FC's passive update). Behind a flag if you want
to keep the cheaper evict-only default. Test `multilevel.rs:114` already asserts convergence, so a
refresh variant would need its own timing-sensitive test.

### 3. L2 (distributed) **soft** timeout is not enforced — only the hard timeout bounds L2 reads — **minor**
`cache.rs:718-724` applies only `distributed_hard_timeout`; `distributed_soft_timeout`
(`options.rs:464-466`, `with_distributed_timeouts`) is stored but never consulted. FusionCache
distinguishes `DistributedCacheSoftTimeout` (used when a fallback exists) from
`DistributedCacheHardTimeout`. Self-disclosed in PARITY.md "Known minor differences".
**Recommendation:** mirror the factory soft/hard selection (`appropriate_factory_timeout`) for L2:
when a stale/fallback exists, bound the awaited L2 read by `min(soft, hard)`; otherwise by `hard`.
Small, fits the existing `read_l2_guarded` seam.

### 4. Single `lock_timeout` rather than separate memory/distributed lock timeouts — **minor**
`options.rs:102` exposes one `lock_timeout` (`time.rs` `Timeout`); FusionCache has distinct
factory/lock timeout knobs per level. Self-disclosed. Functionally, single-flight still works;
only the per-level tunability is missing.
**Recommendation:** additive — split into `memory_lock_timeout` / `distributed_lock_timeout` if a
user needs different cluster-vs-local wait budgets; current single knob is a reasonable default.

### 5. `Clear()` is not modelled as a special `*` tag; uses dedicated clear markers — **cosmetic**
`cache.rs:490-502` + `tags.rs:120-128` (`clear_expire`/`clear_remove` atomics). FusionCache
implements `Clear` internally via a reserved tag. Same observable effect (expire-all / remove-all,
propagated over the backplane); different internal structure. No behavioral consequence.
**Recommendation:** none needed; the typed-marker form is cleaner. Documented as a deliberate
divergence.

### 6. `Priority` / `Size` are carried but not honoured by the L1 backend — **cosmetic**
`options.rs:21-32,285-288`. moka's TinyLFU eviction ignores explicit per-entry priority/size
weighting; the fields exist for API parity and are forwarded where a backend could use them.
Honestly disclosed in PARITY.md.
**Recommendation:** none required for functional parity; note in docs that eviction is TinyLFU, not
priority-ordered (already noted in `options.rs:18-20`).

### 7. Auto-recovery delay default is `5s`; FusionCache `AutoRecoveryDelay` default is `2s` — **minor**
`recovery.rs:61` `delay: Duration::from_secs(5)`. FusionCache's `Options.md` lists
`AutoRecoveryDelay` default = **2s** (independently confirmed). PARITY.md's Defaults table
(`docs/PARITY.md`, "RecoveryConfig::delay … 5s … AutoRecoveryDelay = 5s") therefore both diverges
from FusionCache *and* mis-states FusionCache's value. Functionally harmless (it is the backpressure
wait before draining the queue, and is fully configurable via `auto_recovery(RecoveryConfig{..})`),
but it is a defaults-fidelity miss given PARITY.md's "mirrors FusionCache's defaults exactly" claim.
**Recommendation:** change the default to `Duration::from_secs(2)` and fix the PARITY.md row to
read "2s". One-line change.

### 8. Events are a single broadcast stream; FusionCache has three hubs (general / `.Memory` / `.Distributed`) — **cosmetic**
`events.rs:138-169` exposes one `Events` broadcast of `CacheEvent`; FusionCache splits into
`cache.Events`, `cache.Events.Memory`, `cache.Events.Distributed` (`Eviction` is memory-only there).
amalgam instead tags the level inside the event (e.g. `CircuitBreakerChange{component}`,
`Eviction{key}` emitted only from the L1 store at `memory.rs:60-66`), so all information is present —
only the *routing* differs. This is the documented "events as a broadcast channel vs an events hub"
idiomatic divergence; a subscriber filters by variant rather than by hub. No behavioral loss.
**Recommendation:** none required; optionally document the variant→hub mapping for users porting
event-subscription code.

#### Not gaps (verified absent-from-FC-docs, so amalgam is free):
- Background-factory-failure ⇒ no fail-safe re-activation (FC `Timeouts.md` silent) — amalgam's
  choice is reasonable.
- `WaitForInitialBackplaneSubscribe` and a global `DisableTagging` toggle are not exposed
  (self-disclosed); neither affects default-deployment parity.

---

## Test-coverage assessment

**Behaviors with real end-to-end / unit tests (the parity oracle):**
- Single-flight stampede — `behavior.rs:30` (32 concurrent callers ⇒ 1 factory) + `locking.rs:80`.
- Fail-safe serve-stale, default-value, error-propagation-without-failsafe — `behavior.rs:59,83,101`.
- Physical-TTL `max`-not-sum, throttle keeps physical boundary, throttle-none-when-dead —
  `options.rs:611,629`, `entry.rs:341,355,372`.
- Soft/hard timeout selection (with/without fallback, hard-wins) — `options.rs:635,646,663`.
- Soft-timeout returns stale + background completion replaces value — `behavior.rs:110`.
- Hard-timeout ⇒ `FactoryTimeout` — `behavior.rs:158`.
- Eager refresh background update (returns valid value now) — `behavior.rs:179`; threshold
  validation — `options.rs:601`.
- Adaptive duration override — `behavior.rs:271`.
- Conditional refresh NotModified (reuse + bump + stale-ETag exposed) and Modified — `behavior.rs:307,351`.
- Remove-by-tag invalidation (single-node) — `behavior.rs:379`; cross-node tag propagation —
  `features.rs:285`.
- Clear remove-all — `behavior.rs:427`; events emitted — `behavior.rs:468`.
- try_get / get_or_default / allow_stale_on_read_only — `behavior.rs:441,451`.
- L2 read-through across instances — `multilevel.rs:20`; backplane Remove + Set peer invalidation —
  `multilevel.rs:74,114`.
- Circuit breaker opens + auto-recovery replays write — `features.rs:195`.
- Auto-recovery: dedup-latest-wins, max_items reject + evict-soonest, max_retries exhaustion,
  remove-succeeded/keep-failed — `recovery.rs:107,135,163,191,216`.
- Distributed locker cross-instance single-flight — `features.rs:95`.
- Plugins, named registry, per-key default-options provider — `features.rs:55,370,433`.
- `get_or_set_value` constant form + size-eviction event — `gaps.rs:8,21`.
- Circuit breaker mechanics, in-memory locker, in-memory L2, MaybeValue, FactoryError,
  Timeout/Timestamp — module unit tests in `circuit.rs`, `distributed_lock.rs`, `distributed.rs`,
  `maybe.rs`, `error.rs`, `time.rs`.
- Redis adapters — `redis_backend.rs` unit tests for wire codec (always run) + 6 env-gated
  integration tests (`AMALGAM_REDIS_URL`; no-op in CI without a server).

**Behaviors asserted ONLY by reading code (no dedicated test):**
- **Background factory *failure* path** (`cache.rs:1025-1032`) — bg success is covered transitively
  by `behavior.rs:110`, but the bg-*error*-does-not-reactivate-failsafe branch has no test.
- **Backplane Expire incoming** (`cache.rs:913-921`) — Remove and Set incoming are tested
  (`multilevel.rs:74,114`); the Expire-marks-stale-keeping-physical branch is not.
- **Multi-node clear propagation** (`cache.rs:898-906`) — cross-node *tag* propagation is tested
  (`features.rs:285`), cross-node *clear* is not.
- **`rethrow_distributed_exceptions` / serialization-error rethrow + events** (`cache.rs:736-742,766-777`)
  — no test drives a serializer failure or asserts the rethrow toggle / `SerializationError` event.
- **Backplane circuit breaker** (`cache.rs:828-831,886-888`) — only the *L2* breaker is tested
  (`features.rs:195`); the backplane breaker (and "received message closes it") is untested.
- **Jitter** (`options.rs:570-584`) — no test asserts jitter widens logical expiration within bound.
- **`skip_*` option family** (memory/distributed read/write, backplane notifications, distributed
  locker) — exercised indirectly but no dedicated assertion per flag.
- **Tag-marker boundary** — the inclusive-vs-strict gap (#1) is specifically **not** tested at the
  same-tick boundary (tests step the clock around it), so the divergence is invisible to the suite.

**Overall:** the oracle suite is strong and pins every load-bearing happy-path semantic with
deterministic `ManualClock` timing. The untested set is concentrated in (a) error/rethrow paths,
(b) the backplane Expire/clear/breaker branches, and (c) the exact boundary where gap #1 lives.
None of the untested branches is a *blocker*, but gap #1 and the serialization-rethrow path are the
two places where "passes tests" most overstates "matches FusionCache."

---

## Bottom line

`amalgam` is a **true functional port** of FusionCache, not a façade: the resiliency core
(stampede, fail-safe with correct physical-TTL arithmetic, soft/hard timeouts + background
completion, eager refresh, adaptive + conditional refresh, lazy tagging, L1+L2 read/write-through,
backplane, circuit breakers, auto-recovery, cross-node locker) is genuinely implemented in
`src/cache.rs` and friends — verified line-by-line, not taken on the docs' word. PARITY.md is, on
audit, **largely honest rather than overclaiming**: it self-discloses 4 of the divergences found
here (L2 soft timeout, single lock_timeout, KeyModifierMode, Redis channel). The exceptions to its
honesty are narrow: its claim to "mirror FusionCache's defaults **exactly**" is contradicted by the
auto-recovery `delay` default (`5s` vs FC's `2s`, gap #7) — the one place a stated default is both
wrong about FusionCache and divergent. The two excluded in-flight fixes (Redis channel config,
KeyModifierMode Suffix/None) plus the 7 minor / 4 cosmetic notes above are the complete delta to
literal 1:1 parity. The single change worth prioritising is **gap #1 (strict tag comparison)** — the
only finding touching correctness rather than tunability, defaults, or mechanism; gaps #3 (L2 soft
timeout) and #7 (recovery-delay default) are the cheap, high-value follow-ups.
