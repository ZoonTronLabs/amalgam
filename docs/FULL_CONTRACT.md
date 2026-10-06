# Full FusionCache functionality: active completion inventory

The complete FusionCache public surface is a broader target than the October
audit repair. This is an active inventory, not a claim that all functionality is
implemented. The reference pins are FusionCache source `v2.9.0` at
`af09f81a3ea8d7ed71183b46501946da801a2a22` and the independently checked released
NuGet binary `2.9.0+c2af1f39d3ad50791109bb9d48c0fdaffba010dd`.

## Current additions (unreleased)

| Capability | Implementation and useful public evidence |
|---|---|
| Native synchronous and async views | `BlockingCache`, driven `BlockingRuntime`, shared final-owner lifetime, caller affinity, deadlines/cancellation, depth-ordered callback admission and actual receipts; runtime/mixed/scheduling and mandatory `blocking_redis` contracts. [Bounds and differences](SYNC.md) remain explicit; full option-combination coverage remains open; verification records identify their exact source tree |
| Supplied value differs from user factory | Static `ConstantOrigin`; factory budgets/eager value work/events do not apply. `constant_origin_contract` contains four genuine regressions; independently executed released reference agrees |
| Factory original/processed key; current and stale tags | `original_key`, `tags`, `stale_tags`; `factory_context_contract` tests prefix ambiguity, cold L2 and eager |
| Eager request tags | Passed from the triggering call, distinct from stale tags; matches released reference |
| Separate marker mutation policy | `tags_default_options`, `tags_entry_options`, `EntryOptions::tag_defaults`; `marker_defaults_contract` tests explicit override and provider independence |
| Independent secondary marker reads | Opt-in `MarkerReadPolicy::OptionsControlled`, independent observation limits, per-marker budgets/skips/fault policy, typed authority and monotonic admission; `marker_read_contract` covers ready/L1/L2, peer races, continuity, short-circuit order and eager/passive cancellation. [Field boundaries](MARKER_READS.md) remain explicit |
| Expiring marker snapshots and nonzero repair | Additional `MarkerLifecyclePolicy::CachedSnapshots`, optional atomic provider capability, independent L1/L2 lifetimes, zero factory budgets, independent skipped/faulted-read factory selection and owned foreground/background/eager writes; `marker_locker_contract` covers eager preflight, same-created longer peer lifetime, contention and shutdown; `marker_snapshot_contract` and mandatory `marker_snapshot_redis` cover actual TTL, durable fact retention, exact revisions, races, causes, cancellation and provider fencing. [Remaining lifecycle limits](MARKER_SNAPSHOTS.md) are explicit |
| Async complete-snapshot codecs | `AsyncDistributedSerializer`, `SerializationMode`; model/legacy tests plus additive cooperative hooks receive the actual owned scope signal. `cooperative_codec_contract` covers both directions, deadline reasons, expiry, eager/passive/replay, background completion, synchronous callbacks and direct legacy adapters |
| Conditional validator replacement/clear | `not_modified_builder`, `ValidatorUpdate`, typed `ConditionalRefreshError`; `conditional_metadata_contract` verifies stored metadata on a cold L2 node |
| Distributed expire choice | `DistributedExpirePolicy::Remove` matches FC L2 removal with stale L1; `RetainStale` keeps existing Rust behavior; `expire_policy_contract` tests both and skips/cancellation |
| Pluggable local memory coordination | Additive `MemoryLocker`, owned `MemoryLockGuard`, typed acquisition/cause/cancellation, ordinary and marker eager/foreground coordination, shared native/async ownership and idempotent supervised per-cache shutdown. Optional `BlockingMemoryLocker` selects a real synchronous acquisition callback in native views, with independent bounded pools and counted late guards; `memory_locker_contract`, `blocking_memory_locker_contract` and [provider boundaries](MEMORY_LOCKER.md). Broader native/custom-provider combinations remain open |
| Supplied value L1 storage | Additive `MemoryStorage<V>`, immutable records, typed original failures, atomic conditions, prefix-scoped shared clear barriers and post-coordination retirement. Native/async views use the actual provider; `memory_storage_contract` and [provider responsibilities](MEMORY_STORAGE.md). Separate marker storage and broader provider matrices remain explicit |
| Instance and provider inspection | `instance_id`, `distributed_cache`, `backplane`, `distributed_locker` |
| Present null versus miss | `Cache<Option<T>>`; `null_value_contract` tests L1/L2/auto-clone, conditional refresh, fail-safe and explicit null default |
| Warm event overhead | Materialize key/Hit only when an observer/plugin needs it, after user copy/destruction callbacks; `ready_event_contract` covers late attachment |
| Pristine marker checks | Unmodified registries have a shared atomic read; the first observed marker permanently restores full locked validation. `marker_visibility_contract` verifies minimum revisions, empty compaction and completed concurrent advances |
| Plugin access to cache operations | Additive `CachePlugin<V>`/`CachePluginContext<V>` and non-owning full `PluginCache<V>` async/sync view. Start/Event/Stop operate on the same cache, including native providers; separate cleanup admission preserves Stop operations after owner close, and callbacks/late attachment/cleanup drain safely. `plugin_cache_contract` and [plugin boundaries](PLUGIN_CACHE.md) document retained views, errors and reentrancy |
| Physical layer event observations | Independent `LayerEvent` stream: 16 closed memory/L2/backplane forms, committed effects, full command/frame payloads, loss accounting and lazy materialization. Typed read deadlines do not trip the transport circuit; `layer_event_contract` and [component boundaries](LAYER_EVENTS.md) cover public/native paths. Original-value memory eviction is exposed through a bounded typed stream; callback policy remains open |
| Native OTel metrics and counted layer callbacks | `OtelMetricsPlugin`, four application-owned meter scopes, all 33 reference counter names, eight Rust counters and logical duration histogram; bounded cache labels and explicit optional tag attributes. Selected plugin callbacks survive broadcast lag, detach/cancellation and run after coordination. Conditional and background factory success are counted once; eager L2 reuse creates no factory success. `otel_metrics_contract`, `layer_plugin_contract` and [metric boundaries](NATIVE_METRICS.md); broader combinations and logging/trace configuration remain open |
| Backplane outage availability | Additive `ReconciliationPolicy::BackplaneBestEffort` retains local and hydrated L1 and controlled marker observations over gaps/reconnects, within normal deadlines. Combined with cooperative suppressed-locker errors it permits ordinary cold origins. Default strict reconciliation/fencing are unchanged; `locker_outage_contract` and [outage boundaries](BACKPLANE_OUTAGES.md) cover the explicit choice |
| Cancellation allocations | Closed atomic terminal state plus subscribe-before-check Notify; `cancellation_signal_contract` covers concurrent requests and registration races |
| Original codec/provider causes | `CodecError`, `TransportError`, preserving constructors and concrete `source`; `original_error_contract` covers native codecs, policy separation and Redis constructor boundaries |
| Immutable copy capability | `immutable_values`, sealed `ImmutableValue`; `immutable_copy_contract` covers isolated owned containers, shared immutable allocation identity and explicit later strategy precedence; compile-fail docs reject shared mutable containers |
| Per-key current default context | `DefaultEntryOptionsProvider::options_for_with_defaults`; `provider_context_contract` verifies raw keys, cache-specific defaults, explicit options and legacy providers |

## Remaining functionality and evidence

These cross-platform capabilities remain open and must not be relabeled as
platform-only simply to close the inventory:

| Capability | Current limitation |
|---|---|
| Full sync option-combination evidence | Native operations and mixed views are implemented; broader option/provider matrices, final delivery/package/performance evidence remain open |
| Heterogeneous values per cache and registry | A `Cache<V>` holds one value type |
| Runtime component/default/provider replacement | Configuration is fixed after build |
| Pluggable L1 and local memory locker | Local coordination and actual value L1 are supplied through `MemoryLocker` and `MemoryStorage`. Distinct native acquisition is supplied through optional `BlockingMemoryLocker`. Separate marker-store extensibility and broader native/custom-provider matrices remain open |
| Full per-layer event surface | Distinct layer operations and full backplane payloads are implemented; configurable handler scheduling/exception policy and the broader background/eager/replay matrix remain open; original stored values are exposed by [memory subscriptions](MEMORY_EVICTIONS.md) with explicit capture and reclamation |
| Logging/tracing/metrics configuration | Native OTel instruments, composable provider/scopes and optional metric tags are implemented. Category log levels, optional trace/log tags and the broader native provider/replay/export matrix remain open |
| Supported automatic backplane recovery | Retry-stage/expiry behavior still needs complete comparison. The old `EnableDistributedExpireOnBackplaneAutoRecovery` switch is inactive and `Obsolete(IsError=true)` in official2.9 source and released static DLL metadata; it is not a missing active option |
| Portable tag/clear over a byte-only store | Requires separate genuine atomic InvalidationStore |
| Full marker factory/renewal options | Independent reads and optional expiring snapshot renewal/repair are implemented. Participating repair has owned acquisition/recheck/fenced renewal/release, with `marker_locker_contract` and mandatory `marker_locker_redis` evidence. Owned eager preflight/zero-wait lease/write and skipped/suppressed-fault known factories are implemented. Original-policy snapshot population/recovery, bounded committed-clear preservation and authoritative clear compaction are delivered on main through `49c1e23`. Public/native gates passed (462 full runtime tests, six doc tests, all 15 exact-source CI jobs); paired timings cover preceding `ada2a41`, not a later source. Remaining factory-budget/read/locker combinations are open. Durable facts deliberately never expire |
| Full option-combination evidence | Stale-layer skips and locker degradation/bypass need a larger public matrix |

.NET ABI, Microsoft service containers, HybridCache and ASP.NET OutputCaching
packages are platform integrations. Equivalent Rust operation contracts and
adapters need their own explicit scope and evidence. Existing explicit Rust
builder/trait composition does not prove every upstream integration. Distributed
wire formats are separate; a matching `v2` name does not establish compatibility.

Stronger canonical reads, local replay ordering, strict fences and durable marker
lifetime are deliberate differences. Defaults and old `not_modified` adaptive
tags also differ. The additive conditional builder and expire policy express the
newly verified reference outcomes without changing those old adapters.

All tests and performance reports must identify their actual source hash.
Intermediate measurements do not describe a later release. See [contract and
migration](PARITY.md) and [validation](AUDIT.md).

Async codecs receive `FactoryCancellation` through the additive snapshot hooks;
legacy implementations retain default adapters. Cancellation is checked before
and after codec work and never suppressed or queued as a serialization failure.
A distributed read has its own owned deadline scope: timeout signals the exact
soft/hard reason before dropping the codec, without cancelling a subsequent
origin. This preserves Amalgam's existing combined get/decode/marker budget;
FusionCache 2.9 times only the backend read and decodes afterward. Background,
eager, passive and recovery work retain their own cancellation ownership and
cache shutdown drains them. Successful scope completion also ends its token.
These are explicit Rust lifetime/deadline adaptations, not literal .NET token
or timeout identity.
