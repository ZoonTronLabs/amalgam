# Expiring marker snapshots and read repair (unreleased)

`MarkerLifecyclePolicy::DurableOnly` preserves the existing default and legacy
providers. `CachedSnapshots` is an explicit additional capability for
`MarkerReadPolicy::OptionsControlled`. It reads and renews expiring observations
of tag/clear revisions, while retaining the permanent invalidation journal.

```rust
use amalgam::{Cache, EntryOptions, provider::InMemoryDistributedCache, provider::JsonSerializer, advanced::MarkerLifecyclePolicy, advanced::MarkerReadPolicy, provider::SystemClock};
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
deliberate `Cooperative` policy allows best-effort contention and selected
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

## Original-policy population and recovery candidate

The current main working tree additionally populates snapshots after explicit
tag/clear advancement. It captures the original mutation options and creation
time, including when distributed execution is deferred. Retry cannot renew the
original absolute physical deadline. A finite snapshot repair never advances or
publishes an invalidation; the permanent journal obligation remains independent.

Captured durable work uses closed Advance, Populate and Notify stages. An atomic
advance retains its actual maximum and any compacted clear before provider
population. Storage errors on retry stay errors, irrespective of the original
suppression flags; only real failures consume the retry budget. Notification
retry cannot redo an acknowledged durable advance or snapshot population.
Expired observations skip provider I/O while the committed fact can still be
published. Disabled recovery, skipped writes and rejected admission remain
explicit outcomes, retaining original failure causes in receipts.

Superseding a tag ticket retains a pending committed global clear. A validating
factory admits at most one inherited clear with no further inherited child, so
this obligation does not create an unbounded tree or another tracking map. Its
original age/options and population/notification stage remain independent from
the newer tag operation. Exact queue identities still prevent an older completion
from deleting or decrementing its replacement.

A failed fenced factory renewal captures authority participation rather than an
expired lease. Retry must reacquire a fresh token, atomically renew under its
actual proof, and release that token. Explicit population carries its actual
owned mutation cancellation source; background handoff preserves ownership, and
shutdown reaches the real provider signal before the future is dropped.

`marker_recovery_contract` and the additional `marker_locker_contract` probe
exercise these candidate behaviors through public cache/provider APIs. Shutdown,
durable supersession and notification-only supersession probes reproduced faults
before fixes. A newer notification, including after a strict population failure,
retains the previous committed clear within the same single queue slot. Failed
inherited publication remains notification-only and does not redo population.

Mandatory `marker_recovery_redis` injects genuine wrong-type and malformed-frame
failures into unique native Redis keys. Tag and both clear modes preserve their
durable fact; replay keeps original age and uses actual remaining backend TTL.
`marker_locker_redis` also verifies a failed fenced repair reacquires a new native
token and releases it without extending the original deadlines.

An independent executable oracle using the released FusionCache 2.9 binary and
only public cache/provider/serializer APIs passed eleven population/recovery
scenarios: tag and both clear modes, captured policy/age, strict and suppressed
faults, disabled recovery, write exclusion, expiry, same-key replacement and read
exclusion with/without a backplane. Several guarantees deliberately improve on
that reference:

- FusionCache preserves logical age during replay but renews physical backend
  expiration; Amalgam retains the original absolute physical deadline.
- A strict FusionCache distributed write throw does not enqueue that failed
  write; Amalgam preserves recovery before returning the original typed cause.
- FusionCache defers invalidation notification after failed marker storage.
  Amalgam can notify after the independent permanent fact has committed even if
  its expendable snapshot fails.
- With distributed reads excluded, FusionCache's replay can skip a failed marker
  write and publish only its notification. Amalgam keeps write repair independent
  of read exclusion.

These are deliberate behavioral differences, not evidence of exact equivalence.
Complete archive/performance/delivery validation remains required; new evidence
is not reassigned to previously pushed source651e5b8 or its CI/performance.

`RecoveryWork` and `RecoveryError` have new closed variants; downstream exhaustive
matches must be deliberately updated. RecoveryAction, RecoveryItem and MarkerReplay
remain unchanged. Legacy custom executors reject typed marker work explicitly.
All additions remain unreleased; no registry/consumer rollout is implied.

## Remaining contract

Complete source/archive/native-provider/reference/performance verification of
the recovery candidate and the remaining factory-budget/read/locker matrix remain
open. The shared factory is a pure immediate selection, with no public replaceable
delayed marker callback. Timestamp equality uses Amalgam's existing expiration
boundary. Permanent facts deliberately differ from FusionCache 2.9, whose control
entries can physically expire. This feature does not establish full parity.

Public `marker_snapshot_contract` probes cover deadlines, repair, physical
expiration, absent results, fault flags/causes, race admission, cancellation,
foreground/background ownership and limits. `marker_snapshot_redis` additionally
checks actual backend TTL, namespace isolation, exact revisions, protocol errors
and atomic lease rejection on the mandatory live Redis/Valkey fixture. See
[the complete inventory](FULL_CONTRACT.md) for the remaining whole-library work.
