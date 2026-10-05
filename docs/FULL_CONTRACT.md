# Full FusionCache functionality: active completion inventory

The complete FusionCache public surface is a broader target than the October
audit repair. This is an active inventory, not a claim that all functionality is
implemented. The reference pins are FusionCache source `v2.9.0` at
`af09f81a3ea8d7ed71183b46501946da801a2a22` and the independently checked released
NuGet binary `2.9.0+c2af1f39d3ad50791109bb9d48c0fdaffba010dd`.

## Current additions (unreleased)

| Capability | Implementation and useful public evidence |
|---|---|
| Factory original/processed key; current and stale tags | `original_key`, `tags`, `stale_tags`; `factory_context_contract` tests prefix ambiguity, cold L2 and eager |
| Eager request tags | Passed from the triggering call, distinct from stale tags; matches released reference |
| Separate marker mutation policy | `tags_default_options`, `tags_entry_options`, `EntryOptions::tag_defaults`; `marker_defaults_contract` tests explicit override and provider independence. Control-read fields are still unavailable, below |
| Async complete-snapshot codecs | `AsyncDistributedSerializer`, `SerializationMode`; `async_serializer_contract` tests both models, pending cancellation and legacy snapshot overrides |
| Conditional validator replacement/clear | `not_modified_builder`, `ValidatorUpdate`, typed `ConditionalRefreshError`; `conditional_metadata_contract` verifies stored metadata on a cold L2 node |
| Distributed expire choice | `DistributedExpirePolicy::Remove` matches FC L2 removal with stale L1; `RetainStale` keeps existing Rust behavior; `expire_policy_contract` tests both and skips/cancellation |
| Instance and provider inspection | `instance_id`, `distributed_cache`, `backplane`, `distributed_locker` |
| Present null versus miss | `Cache<Option<T>>`; `null_value_contract` tests L1/L2/auto-clone, conditional refresh, fail-safe and explicit null default |
| Warm event overhead | Materialize key/Hit only when an observer/plugin needs it, after user copy/destruction callbacks; `ready_event_contract` covers late attachment |
| Cancellation allocations | Closed atomic terminal state plus subscribe-before-check Notify; `cancellation_signal_contract` covers concurrent requests and registration races |
| Original codec/provider causes | `CodecError`, `TransportError`, preserving constructors and concrete `source`; `original_error_contract` covers native codecs, policy separation and Redis constructor boundaries |
| Immutable copy capability | `immutable_values`, sealed `ImmutableValue`; `immutable_copy_contract` covers isolated owned containers, shared immutable allocation identity and explicit later strategy precedence; compile-fail docs reject shared mutable containers |
| Per-key current default context | `DefaultEntryOptionsProvider::options_for_with_defaults`; `provider_context_contract` verifies raw keys, cache-specific defaults, explicit options and legacy providers |

## Remaining functionality and evidence

These cross-platform capabilities remain open and must not be relabeled as
platform-only simply to close the inventory:

| Capability | Current limitation |
|---|---|
| Native synchronous operations | Data operations are async |
| Heterogeneous values per cache and registry | A `Cache<V>` holds one value type |
| Runtime component/default/provider replacement | Configuration is fixed after build |
| Pluggable L1 and local memory locker | Built-in MemoryStore and KeyedLock only |
| Full per-layer event surface | Missing distinct layer hit/miss/set/remove, memory expire, complete eviction/backplane payloads |
| Plugin access to cache operations | Context supplies identity/events/stop state |
| Logging/tracing/metrics configuration | Category levels, optional tags and full native OTel metric integration incomplete |
| Optional distributed-expire-on-backplane-recovery | Exact replay policy only |
| Portable tag/clear over a byte-only store | Requires separate genuine atomic InvalidationStore |
| Cooperative codec cancellation | Async codec futures are owned/dropped, but no operation signal is forwarded to codecs yet |
| Marker control-read options | Durable validation currently ignores independent marker read skips/budgets; only mutation defaults are implemented |
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
