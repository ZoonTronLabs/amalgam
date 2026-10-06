# Supplied in-process L1 storage (unreleased)

`CacheBuilder::memory_storage(Arc<dyn MemoryStorage<V>>)` selects the actual value
store used by canonical reads, writes, factories, hydration and background work.
Without a supplied provider, the existing built-in store remains the default.
`Cache` and `BlockingCache` expose the same provider through `memory_storage()`;
`memory_usage()` reports its fallible retained-count/weight snapshot.

## Provider responsibilities

`MemoryStorage` is an open synchronous in-process interface. Its methods must be
short; it is not an asynchronous or remote L1 transport. Optional `try_get` must
not block. Returning `Ok(None)` from this ready probe permits an ordinary lookup;
returning an error preserves that error. Native and async views use this same
storage interface, not separate copies of the data.

The host creates immutable `MemoryRecord<V>` values. Their entries carry values,
physical/logical lifetimes, tags, validators, size and priority. Cloning a record
clones its handle, without invoking `V::clone`. Providers cannot construct or
modify records, their private hydration eligibility, generation or observer route.
Use `is_live_at(now)` under the provider mutation guard: logical staleness alone
is not physical expiry, because an eligible stale value can support fail-safe.

Providers implement atomic `insert`, `remove`, `clear_before` and optional
`maintain`. Insertion validates both candidate eligibility and `MemoryCondition`
inside the same transaction. `Any` permits explicit replacement; `Absent` accepts
an empty or ineligible slot; `Same` requires exact entry identity. Conditional
hydration therefore cannot overwrite a newer representation admitted by another
cache sharing this provider. An insertion rejected by capacity or condition must
preserve other live values. Return `MemoryStorageWrite::Rejected` with its real
reason; a factory can still return its computed value even when L1 admission is
rejected. A receipt's `MemoryAdmission` describes actual admission.

Capacity and victim selection belong to the provider. Weight uses full `u128`
accounting of the validated `u64` entry size, with a default weight of one. Priority
metadata is available through the entry. Supplying nondefault built-in
`MemoryLimits` together with external storage is a typed configuration rejection:
configure the external provider's capacity directly.

All physically removed records are returned as owned records/retirements **after**
releasing provider guards. Never destroy their last owned value handle under a
provider guard. The host retains values and defers observer callbacks until its
own factory/commit coordination is released. A shared-store replacement routes
retirement to the cache that inserted the original representation, respecting
that cache's eviction capture policy. Metadata-only expiry does not invent a
value replacement eviction.

## Clear visibility and sharing

`epoch(namespace)` must return one stable `MemoryStorageEpoch` for each exact
host-issued namespace, with distinct handles for distinct namespaces. Value
namespaces retain the configured key-prefix identity; marker namespaces also
identify their authority and canonical physical wire scope. Two same
namespace calls are checked during construction. Caches sharing a provider and
prefix share clear visibility; different prefixes retain separate visibility.
Keys are the real processed keys: deliberately choosing overlapping prefixes
that produce the same final key can still alias stored values.

The host advances the shared epoch before physical clearing. `clear_before`
removes only records selected by `barrier.precedes(record)`; a delayed clear
cannot remove newer writes. If physical clear fails, old records remain retained
but cannot become readable again. Epoch exhaustion is a terminal typed failure
and cannot wrap to an old live generation. `usage` describes retained storage,
including obsolete records awaiting maintenance and other shared namespaces.

A cache retains an `Arc` to the provider. Shutdown does not dispose storage shared
with another cache. Separate caches still have separate factory coordination
unless a shared `MemoryLocker` is deliberately supplied; storage sharing alone
does not promise cross-cache single flight or global transaction ordering.

## Errors, maintenance and boundaries

Lookup absence is `Ok(None)`. Provider failure is
`Error::MemoryStorage(MemoryStorageError::Provider { source })`, preserving the
concrete original cause. It does not become a cache miss, a factory attempt or a
Redis circuit failure. A failed provider mutation leaves its stored contents
unchanged. Host visibility can already have advanced when physical clear fails,
as described above. `try_run_pending_tasks()` preserves a maintenance failure;
the existing unit-returning adapter reports errors through its legacy error path.

The value interface also supports a separate typed [marker observation
provider](MARKER_MEMORY_STORAGE.md). The permanent invalidation journal and durable
snapshots retain their own lifecycles. Optional distinct synchronous local-lock
acquisition is documented in [MEMORY_LOCKER.md](MEMORY_LOCKER.md). Heterogeneous
values, runtime replacement and broader provider matrices remain in
[FULL_CONTRACT.md](FULL_CONTRACT.md); these additions do not establish full parity.

## Reference and verification

The pinned FusionCache 2.9
[MemoryCacheAccessor](https://github.com/ZiggyCreatures/FusionCache/blob/af09f81a3ea8d7ed71183b46501946da801a2a22/src/ZiggyCreatures.FusionCache/Internals/Memory/MemoryCacheAccessor.cs)
uses a supplied `IMemoryCache` for actual entries and does not own/dispose it.
Its externally supplied store cannot use the owned concrete MemoryCache clear
path. Rust adapts this capability into typed failures, immutable records, atomic
conditional admission and explicit shared visibility barriers. These stronger
provider requirements are not literal .NET interface compatibility.

`memory_storage_contract` implements an independent mutex/map provider and checks
actual basic operations, rejected admission, size/priority, clocks, same-key and
independent factories, fail-safe, soft/hard timeouts, eager and conditional
refresh, typed original causes, sharing, native/async views and skipped L1 paths.
It also checks failed and overlapping clear, older L2 hydration versus a newer
shared write, original-value destruction after guards, and a callback that
reenters the replacing native cache. The fixture is a contract probe, not a
production capacity algorithm. Exact delivery, package and performance evidence
must identify the verified source tree; no universal speed claim follows from
an optional custom store.

Native warm factory retrieval captures ownership without constructing an unused
retirement collector; the collector is created when origin dispatch actually
occurs. `ready_allocation_contract` measures zero heap allocations for a warmed,
unobserved scalar native retrieval; unused factory destruction still precedes
the final cancellation check. This does not cover every value/provider/options
combination.
