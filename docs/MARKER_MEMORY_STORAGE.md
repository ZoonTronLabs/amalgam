# Supplied tag/clear observation L1 (unreleased)

`CacheBuilder::marker_memory_storage(Arc<dyn MemoryStorage<MarkerObservation>>)`
selects the actual observation store used by independent secondary marker reads.
It requires `MarkerReadPolicy::OptionsControlled`. Ordinary `MemoryStorage<V>`
remains independent. Without this provider, the built-in observation cache keeps
its existing 4096-entry limit. A supplied provider owns capacity; an explicitly
nondefault `marker_read_limits` is a configuration error in either setter order.

## Facts and namespaces

Only the host constructs immutable records. `MarkerObservation` is a closed
payload: durable `Confirmed`, memory-only `Local`, `KnownMaximum`, or an explicit
`Retained { presence, failure }`. `MarkerPresence` separates present versions
from absence. A memory-only selection emits `MarkerReadOutcome::Local`, not a
claim of durable confirmation. Record metadata uses marker defaults, including
size, priority, independent freshness, retention and eager threshold.

Providers partition the complete `MemoryNamespace` value, not only `key_prefix`.
`purpose()` distinguishes values, local observations and durable observations;
`durable_scope()` exposes the validated physical scope. Marker keys are qualified
by authority, collision-free scope and typed kind. Durable scopes use canonical
physical prefix/suffix identity: a wire version affects isolation only when the
chosen key modifier includes it, just as it does in durable storage. Local
observation identity depends on the raw configured prefix. Two builds with the
same provider/namespace must return the same epoch; distinct namespaces require
distinct epochs. An unstable epoch or an epoch aliased to ordinary values is
rejected. Returned record key/epoch mismatches are typed contract failures.

## Actual operation behavior

Ready probes, ordinary reads, negative/known/fail-safe admission, atomic eager
claims, durable snapshot hydration/repair and maintenance use the supplied store.
The host keeps no shadow observation map. Foreign live maxima advance the
per-cache permanent invalidation journal before a value can be returned. The
ready path rechecks its value after accepting facts and stops at the first
invalidating control. Provider eviction does not erase an already learned fact.
Existing initialized backplane clear scalars remain a deliberate stronger local
shortcut; their continuity rules are unchanged.

Memory-only caches sharing a provider use the same marker records for tag and
remove/expire clear. Marker misses use lock/recheck and a total known/absent local
factory with independent factory budgets. Eager acquisition stays nonblocking,
with owned background completion. Native operations use the optional
`BlockingMemoryLocker`; async views retain the async method. Skipping an unused
L2 read does not suppress the memory-only factory. Atomic admission may read a
current record even when ordinary memory lookup is explicitly skipped.

`marker_memory_storage()` and fallible `marker_memory_usage()` inspect the actual
provider from both public views. Usage is absent when independent reads are
disabled. Providers remain application-owned across cache shutdown.

## Failures and continuity

`Error::MarkerMemoryStorage` retains original storage causes. These failures do
not become misses, do not run a value origin and do not trip a Redis circuit;
marker distributed-exception flags do not suppress failures of this local store.
`MemoryStorageError::InvalidRecord` identifies key/namespace violations.

A continuity gap advances both supplied visibility generations before any
physical cleanup callback. Both layers are attempted even when one fails. A
single failure keeps its original value/marker channel; simultaneous failures
use `Error::MemoryInvalidation`, with both causes available through typed getters.
Failed cleanup cannot restore old records. Delayed cleanup removes only records
preceding its barrier, preserving writes made afterward in either layer.

These are additive closed error/outcome variants: downstream exhaustive consumers
must be updated. The [marker read](MARKER_READS.md) and
[snapshot](MARKER_SNAPSHOTS.md) policy boundaries still apply. General byte-only
L2 support still requires a genuine atomic invalidation capability; this local
provider does not create one. Full option/provider combinations and the broader
[completion inventory](FULL_CONTRACT.md) remain open.

`marker_memory_storage_contract` uses an independent public-protocol map provider
shared with value-storage contracts. It checks actual records, local/durable and
physical-wire scope isolation, readiness/authority, hot invalidation, permanent
facts, provider faults/capacity/expiry, zero budgets/skips, native/async method
selection, eager ownership, snapshot hydration/repair, peer CAS races and failed
or delayed dual-layer cleanup. Exact package/performance evidence must identify
its actual source; this capability alone does not establish full parity.
