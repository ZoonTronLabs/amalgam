# Tested FusionCache contract and 0.3 migration

Amalgam adapts FusionCache's hybrid-cache and resiliency model to Rust. A feature name or a green example is insufficient evidence of full behavioral equivalence. This document states the supported contracts and their limits.

The comparison pins are distinct:

- Inspected upstream source: FusionCache commit `af09f81a3ea8d7ed71183b46501946da801a2a22`.
- Executed reference: published NuGet FusionCache **2.9.0**, binary informational version `2.9.0+c2af1f39d3ad50791109bb9d48c0fdaffba010dd`.
- Amalgam: **0.3.1**.

Unreleased additions are tracked in [the full functionality inventory](FULL_CONTRACT.md).
They include original/current/stale factory metadata, independent marker policy,
async snapshot codecs, explicit conditional/expiry choices, and the shared
synchronous facade. They are not part
of the published 0.3.1 package. The remaining public-surface gaps stay open. Unreleased typed plugin contexts now provide complete same-cache operation access, including operational Stop after ordinary admission closes; see [plugin boundaries](PLUGIN_CACHE.md).

Unreleased [component observations](LAYER_EVENTS.md) distinguish physical layer hits/misses and committed writes from final logical acceptance. They retain complete peer payloads and separate read deadlines from transport faults. Original-value eviction, callback policy and broader combinations remain open.

Released 2.9 experiments additionally establish that eager current tags come from
the triggering request, while stale tags describe the cached source. Its
`NotModified` resets tags to stale tags but preserves caller changes or clearing
of validators. The new `not_modified_builder` expresses that contract;
`ValidatorUpdate` distinguishes retain, replace and clear. Existing `not_modified`
continues to honor adaptive tags. The new conditional builder rejects a missing
source with `ConditionalRefreshError` and rejects invalid legacy tag products.

Released FusionCache Expire retains stale L1 but physically removes L2. Existing
Rust `try_expire` deliberately retains a physically live L2 snapshot;
`try_expire_with_policy(..., DistributedExpirePolicy::Remove)` now selects the
released reference effect. Both paths preserve explicit layer skips and receipts.

Unreleased tag/clear operations use separate `tags_default_options`. Their
foreground backplane default is independent of ordinary values; durable marker
lifetime is the existing stronger protocol. `AsyncDistributedSerializer` owns
the complete validated snapshot; `SerializationMode` selects an available model.
`SyncPreferred` preserves existing synchronous codec behavior and falls back to an
async-only provider. `AsyncPreferred` uses a configured asynchronous provider;
sync-only providers retain their established snapshot overrides.

Default durable control reads retain required validation within the value read
budget. Opt-in `MarkerReadPolicy::OptionsControlled` applies independent marker
read options, observation retention, per-marker budgets and typed authority,
including fresh L1 hits. [The marker field matrix](MARKER_READS.md) describes
remaining lifecycle gaps and deliberate stronger boundaries. Additional opt-in
[CachedSnapshots](MARKER_SNAPSHOTS.md) supports expiring remote observations and
nonzero repair with atomic maxima, independent deadlines and owned writes;
the journal stays permanent. Owned marker locker orchestration, eager preflight
and original-policy staged recovery are implemented with native acceptance.
Remaining budget/read/locker combinations stay open in the inventory. Async
codecs receive the actual owned cooperative operation signal through additive hooks.

New codec/provider error envelopes preserve the concrete source for downcasting.
Rust's typed envelope is an idiomatic diagnostic adaptation; it does not expose
FusionCache's optional same-exception-object rethrow API. Legacy message-only
variants cannot reconstruct a cause already converted into text. The sealed
`ImmutableValue` capability permits codec-free copy for supported built-in types;
custom values continue through their chosen `ValueCloner`. Providers may now use
`options_for_with_defaults` to inspect the owning cache's current defaults.

The [synchronous facade](SYNC.md) and its async view share the same cache and
final-owner lifetime. Its explicit thread/depth bounds and fail-safe default
soft-budget behavior are Rust differences. Supplied values are now distinct
from user factories in both views: zero factory deadlines do not reject them,
eager refresh does not overwrite a warm value, and factory events are absent.
These outcomes were executed against released FusionCache 2.9 and reproduced
as Rust regressions before repair.

Unreleased [supplied value L1](MEMORY_STORAGE.md) adds an actual in-process
`MemoryStorage<V>` provider shared by native/async views. Immutable records,
typed original failures, atomic conditional admission and prefix-scoped shared
clear barriers preserve value eligibility and newer writes. Storage sharing does
not imply shared factory ownership; providers own capacity and remain externally
owned. Optional `BlockingMemoryLocker` now supplies a distinct synchronous
acquisition callback; async views retain the async provider, and legacy
implementations keep their adapter. Independent bounded callback pools prevent
lock waiters from starving factories; started callbacks and late guards remain
owned until real completion. `ShutdownTask::MemoryLockerAcquisition` is an
additive closed variant requiring exhaustive downstream handling. See
[local coordination](MEMORY_LOCKER.md). Separate
[typed marker storage](MARKER_MEMORY_STORAGE.md) now participates in actual local
and durable observation reads/writes; broader provider matrices remain open.

## Observable behavior

| Concern | Amalgam contract |
|---|---|
| Local coordination | Same-key callers share ownership. Different keys have independent flights, including keys with colliding lock-map shards. A finite lock timeout can deliberately permit best-effort origin work without a lock. |
| Fresh and stale values | Logical freshness differs from physical fail-safe retention. Ordinary failure can use an eligible captured fallback; explicit cancellation remains an error. |
| Soft factory timeout | Applies with fail-safe and an available fallback. An allowed background continuation retains the origin and ownership without acquiring a new hard-timeout budget. |
| Hard factory timeout | Bounds foreground origin work when selected. A zero budget cannot start origin work. |
| Eager refresh | Request-driven, nonblocking refresh. Rechecks L2 and participates in a configured distributed locker; it does not apply the ordinary factory timeout. |
| Snapshot ordering | Origin snapshot time is captured before origin work. Insertion time controls TTL separately. Delayed work does not acquire newer source ordering merely by finishing later. |
| Conditional/adaptive origin | Products and updated options are validated before storage. `not_modified` needs a usable source value. |
| Read-only lookup | Canonical reads preserve value I/O, codec, deadline, circuit and copy failures. Default L2 budget covers retrieval and required durable validation; explicit OptionsControlled uses independent marker phases and documented marker fault policy. Canonical stale service rechecks physical retention and hard invalidation after waiting; legacy adapters preserve captured fallback compatibility. |
| L1/L2 lifetime | Layer freshness and physical retention are independent. Hydration uses remaining source lifetime and cannot renew an expired source. |
| Tags and clear | Durable markers use the effective physical namespace. Invalidation is **inclusive**, `entry_created <= marker`; a present minimum timestamp remains a marker. Explicit skipped distributed writes apply to single/batched tags and both clear modes, with honest per-stage receipts. |
| Expire and notifications | Cold expire updates L2. Ordinary and passive hydration capture generation, continuity and expected L1 identity; delayed reads cannot overwrite newer hydration or completed local mutations. Preferred newer/equal-stamp stale memory is retained. Optional hydration skips a busy newer commit; private local epoch eligibility prevents post-gap insertion from becoming readable. |
| Background effects | Requested background work has an awaitable receipt. Notifications follow the corresponding data commit or an explicit recovery decision. |
| Recovery | Exact item identities, generations, pending stages and original payloads survive retries. Successful stages are not repeated just because a later publication failed. |
| Copy isolation | Requires a real fallible copy strategy. Ordinary Rust `Clone` may share interior mutable state. |
| Capacity | Count and weight are separate. Pinned entries occupy capacity; further admission may be refused. Physical expiry still applies. |
| Diagnostics/lifecycle | One event route, accurate stale servicing-layer attribution, per-cache plugin sessions, typed outcomes, bounded metric labels and owned shutdown. Dynamic attachment startup, callbacks, explicit teardown and owned session destruction are drained; repeated shutdown preserves original failures. |

Actual 2.9 reference experiments cover all nine combinations of tag/expire-clear/remove-clear and entry timestamps immediately before, equal to and after the persisted marker. Only the later timestamp remains valid. A real size-one MemoryCache reference confirms that a `NeverRemove` entry occupies capacity and additional admission can fail.

## Shared reference behavior

The regression suite preserves these observed reference behaviors:

- A previously captured physically valid fallback may be served after a lock wait.
- Actual NuGet 2.9 read-only lookup can return an eligible captured stale value after its physical lifetime elapses while awaiting L2. Legacy Amalgam `try_get` retains this behavior; canonical `read` deliberately rechecks retention and hard invalidation before stale service.
- A soft-timeout continuation receives no remaining foreground hard-timeout budget.
- Normal eager refresh uses no ordinary factory timeout.
- Ordinary late background origin completion may repopulate a key after `remove`; removal does not cancel that origin.
- Eligible tag-invalidated stale fallback can be retried according to fail-safe throttle policy.
- Default cache L2 writes/removes are not assigned an invented read-timeout budget.

Four actual NuGet 2.9 experiments also reproduce an already-started recovery write/remove overtaking a newer write. Amalgam deliberately provides **stronger local ordering** through same-key commit lanes. Awaiting a newer local mutation orders it after an older local replay that already owns that lane. This does not establish global linearizability across independent nodes, custom writers or nonparticipating backends.

## Coordination capabilities

A distributed lease reports its ownership token, lifetime and renewal capability. Native in-memory and Redis providers support token-checked release. An atomic backend ownership check is required to reject a stale owner's data commit; renewal alone cannot guarantee that during a partition. The explicitly selected legacy lease mode provides weaker coordination. An opaque backend-selected acquisition that never completes cannot promise bounded cancellation drainage; that limitation belongs to the provider contract.

Circuit refusal is an admission outcome, not a new transport failure; skipped operations do not restart cooldown. Known caller-selected acquisition tokens receive supervised compare-release after uncertain error replies. Recovery validates representable reconnect delays before starting and invokes/drops external fence behavior outside its queue mutex. A malformed inner control envelope triggers a post-gap recovery barrier when the retained connection is still healthy; conservative policies also discard L1, while explicit best-effort reconciliation retains it.

Durable tag/clear operations require an atomic invalidation store. Existing custom byte stores remain usable for ordinary L2 reads/writes without that optional capability. A custom implementation cannot silently claim atomic markers or fencing that it does not provide.

Default healthy L1 reads remain local. Cold L2 reads reconcile durable markers. OptionsControlled may revalidate markers on L1 hits according to independent tag defaults. By default, backplane continuity gaps, broadcast overflow and changed Redis connection epochs discard L1; Redis reports connected only after a matching subscription acknowledgement. `ready()` or `try_build_ready()` can await admission; healthless adapters report an explicit `BestEffort` outcome. A healthless custom backplane defaults to periodic reconciliation. Explicit `BackplaneBestEffort` retains L1 for either kind; see [outage policies](BACKPLANE_OUTAGES.md). Timestamp-based marker ordering assumes a sufficiently consistent clock across participating nodes; injected clocks make local logic testable and do not solve distributed clock skew.


### Redis outage boundary (0.3.1 and current source)

`LeasePolicy::Fenced` is the default. Failed locker acquisition rejects ordinary
origin work even with `with_rethrow_distributed_locker_exceptions(false)`.
Explicit `CooperativeLegacy` and `false` permit the ordinary foreground origin
to continue without that lease, with weaker cross-node ownership guarantees.
The pinned FusionCache accessor instead suppresses an ordinary acquisition
exception when its rethrow option is false and permits origin work; these
contracts are not equivalent by default.

By default, a Redis backplane continuity gap discards all L1 entries, including retained
fail-safe values. The next read is a miss; it either computes through the
origin or fails on the configured fenced locker. Losing only L2 or the locker
without a backplane gap does not invalidate an otherwise fresh L1 hit. This is
a deliberate correctness/availability tradeoff, not universal resiliency parity.
An owned cleanup failure during the outage can remain in `shutdown()` after
reconnection regardless of the acquisition rethrow option.

The unreleased `ReconciliationPolicy::BackplaneBestEffort` preserves fresh and
physically retained stale L1 over notification gaps and reconnects, without
periodic discarding. Received/local invalidations, expiry and cancellation still
apply. Combined with `CooperativeLegacy` and locker rethrow disabled, ordinary
cold misses may compute during an outage. Selecting it does not relax `Fenced`
miss admission. Missed peer changes can remain invisible until expiration;
existing independent marker read/repair admission still applies.

`locker_outage_contract` exercises both policies, local and hydrated L1 in
bounded/unbounded storage, fail-safe/physical expiry, cancellation, received and
local invalidations, and healthless-provider defaults. The independent
registry-pinned 0.3.1 reproduction covers the older real Redis outage behavior;
the new enum variant is absent from that package. [Outage policies](BACKPLANE_OUTAGES.md)
keep these package and source boundaries explicit.

## Selected defaults

| Setting | Default |
|---|---|
| Logical duration | 30 seconds |
| Fail-safe | Disabled; configured maximum retention one day, throttle 30 seconds |
| Eager refresh / jitter | Disabled / zero |
| Factory and lock budgets | Infinite |
| Wait for initial backplane subscription | Enabled |
| Distributed read budgets | Infinite |
| Background L2 operations | Disabled |
| Background backplane operations | Enabled |
| Rethrow serialization errors | Enabled |
| Rethrow value transport/backplane errors | Disabled |
| Lease policy | `Fenced`; failed acquisition rejects origin work |
| Rethrow locker acquisition errors | `false`; suppression applies to ordinary cooperative foreground acquisition |
| Circuit breaker duration | Zero, disabled |
| Recovery with configured distributed effects | Enabled; delay 2 seconds, queue bound 1024 |
| Distributed snapshot namespace | `v2`, Prefix modifier |

These are selected Amalgam defaults. The pinned FusionCache 2.9 source defaults to **no initial subscription wait**, a **5-second recovery delay** and an **unlimited recovery queue**. Amalgam keeps its earlier 2-second delay, gates native operations on subscription acknowledgement by default, and introduces a 1024-item queue bound. Each choice is configurable; they are deliberate differences, so the defaults are not universally identical. Explicit `RecoveryConfig.max_items = None` selects unlimited admission. Retry counts represent additional attempts after the original operation.

## Migrating from 0.2

1. Compile consumers against 0.3 using `amalgam = { package = "amalgam-cache", version = "0.3" }`. The package and imported library names differ.
2. Prefer `CacheBuilder::try_build`, fallible `read`/`read_or_default`, typed mutations and their receipts. Existing legacy signatures remain adapters; a signature without an error return cannot expose every failure.
3. Register an actual `ValueCloner` before enabling auto-clone. Configure count and weight independently and handle refused admission.
4. Choose participating marker/lease capabilities for the guarantees you need. Unsupported capabilities are typed failures, not successful no-ops.
5. Use a coordinated fresh distributed namespace. 0.3 decoders accept legacy unframed DTOs, but running 0.2 readers cannot decode new framed snapshots. The new default `v2` Prefix separates them. `KeyModifierMode::None` or intentional reuse of v1 requires a fresh physical prefix or a coordinated migration; it does not make mixed-version readers safe.
6. Adopt explicit cancellation and `shutdown` for observed drainage. Dropping the last public handle initiates cancellation; retaining an external event handle does not retain the cache.

Source compatibility, legacy decoding and mixed-version runtime compatibility are different claims. Performance depends on value ownership, configured guarantees, providers and workload. See [validation](AUDIT.md), the [README](../README.md) and [porting design](../PORTING.md).

### Cooperative async codec lifetime (unreleased)

The additive `serialize_snapshot_with_cancellation` and
`deserialize_snapshot_with_cancellation` hooks forward the signal of their
actual owned execution. The default hooks preserve old implementations and
check cancellation before and after their await. Cancellation always retains
its typed channel, independently of codec error-suppression policy. L2 reads
have a linked child scope, so a read timeout signals `SoftTimeout`/`HardTimeout`
before the codec is destroyed and does not cancel a subsequent factory.
Permitted timed-out factories retain their original scope after the caller
returns; eager, passive and recovery have independent scopes. Shutdown cancels
and drains all owned work. `FactoryCancellation::check` exposes the exact reason;
successful scope completion signals `ScopeFinished` as in existing factories.

The upstream 2.9 reference passes operation tokens to foreground codecs but
uses no caller token for late commit/passive decode. Its distributed timeout
covers backend get rather than decode/markers; its disposal is less strongly
owned. Amalgam retains its stronger combined deadlines and drainage. Twelve
public cooperative contracts and three existing codec-model contracts verify
this adaptation; source-only review is recorded separately.
