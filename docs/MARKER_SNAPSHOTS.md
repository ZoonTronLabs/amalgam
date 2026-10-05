# Expiring marker snapshots and read repair (unreleased)

`MarkerLifecyclePolicy::DurableOnly` preserves the existing default and legacy
providers. `CachedSnapshots` is an explicit additional capability for
`MarkerReadPolicy::OptionsControlled`. It reads and renews expiring observations
of tag/clear revisions, while retaining the permanent invalidation journal.

```rust
use amalgam::{Cache, EntryOptions, InMemoryDistributedCache, JsonSerializer,
    MarkerLifecyclePolicy, MarkerReadPolicy, SystemClock};
use std::{sync::Arc, time::Duration};

# #[tokio::main]
# async fn main() -> Result<(), amalgam::Error> {
let cache = Cache::<u64>::builder()
    .distributed(Arc::new(InMemoryDistributedCache::new(Arc::new(SystemClock))))
    .serializer(Arc::new(JsonSerializer))
    .tags_default_options(
        EntryOptions::tag_defaults()
            .with_memory_duration(Duration::from_secs(30))
            .with_distributed_duration(Duration::from_secs(90)),
    )
    .marker_read_policy(MarkerReadPolicy::OptionsControlled)
    .marker_lifecycle_policy(MarkerLifecyclePolicy::CachedSnapshots)
    .try_build()?;
# cache.shutdown().await?;
# Ok(())
# }
```

Building this mode without controlled reads or a provider's snapshot capability
returns a typed configuration error. Native in-memory and Redis providers offer
the capability through `InvalidationStore::snapshot_cache`. Old read/advance-only
providers still compile and use the default lifetime model.

## Separate facts, observations and lifetimes

A validated `MarkerSnapshot` carries a real `MarkerVersion`, insertion time,
logical deadline and physical deadline, satisfying `created <= logical <=
physical`. It is immutable and constructed through validating factories.
Confirmed absence is a separate outcome and never becomes a remote zero marker.

`MarkerSnapshotRead::Missing { maximum }` atomically reports the durable fact or
confirmed durable absence together with the TTL-cache miss. It needs one provider
operation and one owned read deadline. A live snapshot is also reconciled with
the durable maximum. Expiry, deletion or eviction of a snapshot cannot erase an
invalidation fact or revive a value invalidated by that fact.

Fresh L1 observations need no foreground provider read; a due eager threshold
can schedule an owned background check. A fresh L2 snapshot hydrates L1
within its remaining source lifetime and never renews L2. Missing or logically
stale L2 snapshots select the strongest confirmed revision, admit a fresh L1
observation when allowed, and renew a nonzero remote snapshot when writes are
allowed. Renewal changes observation age, not the invalidation revision. It emits
no backplane invalidation and does not advance or compact the journal.

| Field | CachedSnapshots behavior |
|---|---|
| Duration / MemoryCacheDuration / JitterMaxDuration | Independent local freshness; fresh factory admission uses validated memory jitter |
| DistributedDuration | Remote logical freshness; no memory jitter |
| FailSafeMaxDuration / DistributedFailSafeMaxDuration | Independent local/remote physical retention, normalized above the respective logical duration |
| FailSafeThrottleDuration | Excluded-factory fallback throttle, capped by the retained source's physical deadline |
| SkipMemoryRead / Write | Independent observation lookup/admission |
| SkipDistributedRead / ReadWhenStale | Read bypass never fabricates absence; a known revision can still run the independent factory/write. ReadWhenStale also bypasses eager preflight when a memory observation exists |
| SkipDistributedWrite | Suppresses renewal without changing the durable fact |
| FactorySoftTimeout / HardTimeout | The shared selection is immediate; zero excludes it, and soft timeout requires a retained marker with fail-safe |
| AllowBackgroundDistributedOperations | Owned renewal can outlive foreground completion; shutdown cancels and drains it |
| EagerRefreshThreshold | Fresh due observations consume one attempt and schedule independently owned preflight/lease/renewal |
| ReThrowDistributedCacheExceptions / ReThrowSerializationExceptions | Independent backend/protocol write-fault policy, preserving the original cause |

Positive factory budgets do not describe a delayed user callback: the shared
marker factory has no user-replaceable slow callback. Cold hard-zero with
fail-safe returns an explicit unavailable observation rather than caching a
fabricated negative result. Without fail-safe it returns a factory timeout.
Actual cancellation always propagates and never becomes a storage failure or
fail-safe success.

Writes use their own cooperative phase and are not capped by a distributed
**read** deadline. Foreground writes are awaited; allowed background writes stay
cache-owned. Native provider I/O bounds still apply. Cancellation observed after
a committed provider effect does not roll back that effect.

## Atomic providers and resource bounds

`MarkerSnapshotCache` is an open, typed provider trait with a closed read,
renewal and failure model. Renewal atomically merges the durable/current/candidate
maximum and preserves a higher revision or later same-revision insertion. It
returns `Stored`, `KeptNewer` or `Expired`. Custom providers must implement those
guarantees; a read followed by a blind byte write is insufficient.

The in-memory provider bounds expendable snapshots independently through positive
`MarkerSnapshotLimits` (default 4096). Evicting a snapshot never evicts journal
facts. Redis uses a separate private scoped namespace, real backend TTL and
fixed-width ordered integer frames; revisions above the floating-point exact
range remain exact. Ordinary value keys cannot collide with snapshot keys.

Native `renew_snapshot_with_lease` atomically checks the actual memory/Redis
authority. A replaced lease returns typed `LeaseError::Lost`; the default trait
implementation returns `UnsupportedFencing`. Strict marker repair defers fresh
L1 authority until this atomic fence accepts the actual renewal.

`CacheEvent::MarkerSnapshotWrite` records actual stored/newer/expired or suppressed
backend/protocol results; optional metrics use finite outcome labels. Suppressed
read faults can run the shared immediate factory over a known nonzero maximum.
Only an actual provider observation confirms absence; an unknown skipped or failed
read returns explicit degraded authority and never installs a fresh negative.
Strict rethrow policies still preserve the original backend/protocol cause.

## Marker repair ownership

For a missing or stale snapshot, participating `CachedSnapshots` repair selects
the tag defaults' `SkipDistributedLocker`, `DistributedLockTimeout` and transport
fault policy. Its scoped control key cannot alias an ordinary value-flight key.
An acquired lease triggers an independently owned snapshot recheck; a peer's
fresh result preserves its original deadlines and avoids duplicate renewal.

`LeasePolicy::Fenced` requires token ownership and actual provider fencing.
Contention, lost ownership and unsupported fencing remain typed failures. A
deliberate `CooperativeLegacy` policy allows best-effort contention and selected
backend-fault fallback; it does not acquire strict authority through suppression.
Acquisition, snapshot read, factory selection and write use their own phases.
Normal foreground release is awaited; allowed background renewal retains its
lease after public-call completion through the actual write and release. Caller
cancellation and shutdown retain supervised cleanup. A rejected strict write
cannot create a fresh local observation that hides the next remote retry.

`marker_locker_contract` covers recheck, contention, skip policy, independent
budgets/causes, cancellation, scoped identities, actual ownership loss and
foreground/background release. Mandatory `marker_locker_redis` drives the cache
against a genuine native locker/snapshot provider, replaces a real Redis token
while renewal is parked, verifies atomic rejection and preserves the replacement
lease across old-owner cleanup.

## Owned eager refresh and independent factory selection

Eager work returns the current value immediately and uses the same bounded
observation cache to consume one attempt, including on contention. A fresh peer
snapshot with a newer creation time **or longer logical deadline** hydrates L1
without another factory, write or distributed lease. Other attempts acquire the
marker lease with zero wait; contention stops eager work under either lease policy.
SkipDistributedRead and ReadWhenStale govern preflight and any owned recheck.

Eager factory selection is independent of foreground soft/hard factory budgets
and AllowTimedOutFactoryBackgroundCompletion. Preflight and write carry real owned
cancellation; shutdown cancels and drains them and releases the actual lease.
CacheEvent::MarkerEagerRefresh and its optional finite-label metric report an
accepted scheduled attempt, which may stop at peer hydration or contention.

Public tests cover single flight, parked reads/writes, shutdown, fresh peer
hydration, same-created longer-L2 lifetime, contention, zero foreground budgets
and skipped/faulted read selection. The released FusionCache2.9 executable oracle
confirms nine corresponding scenarios independently. A deliberate improvement:
Amalgam can renew a known revision while distributed reads are skipped even
without a locker/backplane. FusionCache's RequiresDistributedOperations disables
that write when no such component participates. Neither path invents a remote zero.
Amalgam's permanent journal and owned eager cancellation remain explicit differences.

## Remaining contract

Snapshot recovery replay/population and the remaining factory-budget/read/locker
option combinations remain open. The shared factory is a pure immediate selection;
there is no public replaceable delayed marker callback. Explicit
tag/clear mutations keep the existing durable mutation/backplane protocol and
do not immediately populate this remote snapshot namespace. Timestamp equality
uses Amalgam's existing expiration boundary. These limits and permanent facts
are deliberate distinctions from FusionCache 2.9, whose control entries can
physically expire. This feature does not establish full FusionCache parity.

Public `marker_snapshot_contract` probes cover deadlines, repair, physical
expiration, absent results, fault flags/causes, race admission, cancellation,
foreground/background ownership and limits. `marker_snapshot_redis` additionally
checks actual backend TTL, namespace isolation, exact revisions, protocol errors
and atomic lease rejection on the mandatory live Redis/Valkey fixture. See
[the complete inventory](FULL_CONTRACT.md) for the remaining whole-library work.
