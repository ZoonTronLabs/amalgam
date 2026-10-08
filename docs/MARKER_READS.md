# Independent secondary marker reads (unreleased)

Atomic invalidation providers retain `MarkerReadPolicy::DurableRequired` by
default. It preserves healthy-local L1 and durable cold hydration: value
retrieval, decoding and required marker validation share the value's distributed
deadline. It does not promise a durable check on every L1 hit. A byte-only L2
without an atomic invalidation provider automatically selects
`OptionsControlled`; explicit `DurableRequired` is rejected during construction
when tagging is enabled.

`MarkerReadPolicy::OptionsControlled` explicitly selects independent secondary
checks. They use `tags_default_options`, bypass the ordinary value-options
provider and run after the value read/decode deadline. Each marker has its own
owned deadline. A missing/stale observation can send a fresh value L1 hit through
the asynchronous owned pipeline.

```rust
use amalgam::{Cache, EntryOptions, advanced::MarkerReadPolicy, provider::MemoryLimits, Timeout};
use std::time::Duration;

let cache = Cache::<u64>::builder()
    .tags_default_options(
        EntryOptions::tag_defaults()
            .with_memory_duration(Duration::from_secs(30))
            .with_distributed_timeouts(
                Timeout::After(Duration::from_millis(20)),
                Timeout::After(Duration::from_millis(100)),
            ),
    )
    .marker_read_policy(MarkerReadPolicy::OptionsControlled)
    .marker_read_limits(MemoryLimits::new(Some(4096), None))
    .try_build()?;
# Ok::<(), amalgam::Error>(())
```

A supplied `InvalidationStore` provides genuine atomic maxima. A byte-only L2
uses separate ordinary control keys through its existing get/set interface;
it never advertises atomic capability. In a memory-only cache, local
invalidation facts still apply.

Ordinary control keys use a length-delimited scope and distinct tag, clear-remove
and clear-expire categories under `amalgam:byte-marker:`. That prefix is reserved
when value-key modification is disabled; prefix/suffix value encoding already
separates values. Marker bytes and namespaces are Amalgam's protocol, not FC's
wire format. Ordinary writes use the original marker revision and physical TTL
from tag options, including the fail-safe retention limit. Ordinary recovery retains the
captured revision and options but recomputes physical TTL at replay, as FC does.
Atomic snapshot recovery retains its original deadline. Re-observing a marker
does not rewrite it with a newer timestamp or extend its remote TTL.

A delayed or concurrent ordinary write can replace a stronger remote revision;
an unseen invalidation can disappear after TTL. The local ledger preserves facts
that a node has already observed, but cannot manufacture an unseen remote fact.
Use a genuine atomic provider for the stronger durable contract. `strict()`
rejects an unavailable atomic marker capability before I/O when tagging is
active. Optional `CachedSnapshots` still requires its actual provider capability.
The public `ordinary_marker_contract` verifies cold-node invalidation,
Expire/Remove fail-safe behavior, namespace isolation, original causes and
captured recovery policy. The [pinned released FC oracle](../tests/fusioncache/README.md)
checks the corresponding byte-provider behavior, including inclusive boundaries
and the two initial clear reads. Re-run the fixture to capture output for
the current source.
Full native/provider and option-combination qualification remains open.

## Authority, retention and cancellation

An evictable observation cache is separate from the monotonic invalidation
ledger. Expiring/evicting an observation never erases a known invalidation.
Successful absence is distinct from a skipped or failed check. Observation
admission atomically merges maxima; a delayed response cannot overwrite a newer
peer observation, and a degraded fallback cannot replace a concurrent refresh.
The developing 0.4 builder selects `ReconciliationPolicy::BackplaneBestEffort`
for a configured backplane. It preserves observation generations and existing
value lifetimes over notification gaps; received invalidations still apply,
while missed peer changes can remain unseen. `strict()` with acknowledged
backplane health selects continuity fencing, which revokes crossing observation
and hydration authority. Explicit reconciliation choices retain their own
policies. See [outage policies](BACKPLANE_OUTAGES.md).

`CacheEvent::MarkerRead` reports the actual authority: `Observed`, `Cached`,
`KnownMaximum`, `Local`, `Skipped`, `StaleFallback(reason)` or `Unavailable(reason)`.
`Local` identifies a memory-only factory, without durable confirmation.
`KnownMaximum` explicitly says that a remembered local maximum exceeds the
current lower/absent response; it does not claim that storage contains that
maximum. Failures are never cached as confirmed absence. `MarkerReceived`
preserves the complete scoped peer control command. Optional metrics count
marker reads and received control messages with bounded cache-name labels.

The checks run in the reference order: ClearRemove, attached tags, ClearExpire.
The first invalidating control stops further checks. Inclusive timestamp
comparison and the ledger's conservative compaction fence remain authoritative.

Soft distributed timeout requires an eligible retained marker and marker
fail-safe; a stale ordinary value does not qualify a cold marker. Hard timeout
always caps the phase. Synthetic marker timeouts may degrade in the explicitly
selected mode even when actual provider exceptions are configured to propagate.
Caller cancellation/drop and shutdown remain typed cancellation. The additive
`InvalidationStore::read_with_cancellation` hook receives the owned phase token;
legacy providers keep a before/after adapter. Timeout is signalled before the
provider future is destroyed. Eager/passive reads use their own refresh scopes.

The hook returns the closed `MarkerReadError` family: `Provider(MarkerError)` or
`Cancelled { reason }`. Transport/decoder implementations preserve original
causes through `MarkerError::backend` / `MarkerError::protocol`; they cannot
bypass marker exception flags by returning a generic value-layer error.
`MarkerReadError::check_cancellation` supports cooperative implementations while
keeping cancellation separate from the provider's failure policy.

With L2 and a backplane, initialized clear scalars remain available without
another remote read until continuity changes, independently of memory-read
skips. Their typed degraded/remembered outcome is preserved. Tag observations
retain their own freshness windows. Without that shortcut, clear observations
use the same expiring cache as tags.

## Entry-option field boundary

| Field | OptionsControlled secondary read behavior |
|---|---|
| Duration / MemoryCacheDuration | Observation freshness, independently of value freshness |
| JitterMaxDuration | Validated injected sample for fresh observation expiry |
| IsFailSafeEnabled / FailSafeMaxDuration | Stale observation eligibility and physical retention |
| FailSafeThrottleDuration | Fault fallback throttle, capped by the source's original physical deadline |
| SkipMemoryCacheRead / Write | Observation reads/admission; neither erases the ledger nor changes ordinary value L1 flags |
| SkipDistributedCacheRead | Explicitly skip the control provider, without fabricating absence |
| SkipDistributedCacheReadWhenStale | Applies to an existing marker observation during stale repair and eager preflight; ordinary value staleness is independent |
| DistributedSoftTimeout / HardTimeout | Independent per-marker provider phase; cold markers ignore soft timeout |
| ReThrowDistributedCacheExceptions | Controls actual marker backend faults |
| ReThrowSerializationExceptions | Controls typed marker protocol faults; control reads do not invoke `Serializer<V>` |
| LockTimeout / MemoryLockTimeout | Marker single-flight wait, with the memory override taking precedence |
| FactorySoftTimeout | Also supplies an otherwise infinite marker-lock wait when retained fail-safe is available |
| Size / Priority | Observation admission/eviction within independent `marker_read_limits`; default count limit is 4096 including clear observations |
| EnableAutoClone | Marker payloads are closed immutable Copy values; no ordinary `ValueCloner<V>` is required or called |
| AllowStaleOnReadOnly | Does not select secondary fallback; the marker helper is not an ordinary read-only value operation |

The default `MarkerLifecyclePolicy::DurableOnly` does not run a secondary marker
factory. Opt-in [CachedSnapshots](MARKER_SNAPSHOTS.md) adds nonzero renewal/repair,
independent remote deadlines, zero factory-budget handling and owned foreground/
background writes. Participating marker repair now has separately owned
distributed acquisition/recheck/release with independent tag defaults; see
[the ownership contract](MARKER_SNAPSHOTS.md#marker-repair-ownership). CachedSnapshots
also schedules owned marker eager refresh; fresh peer hydration precedes a
zero-wait lease attempt. Skipped or suppressed failed reads can run its independent
factory over known revisions without confirming an unknown absence. Snapshot
recovery/population and remaining option combinations stay open. DurableOnly
does not schedule marker eager work. SkipAutoCloneForImmutableObjects
does not force an ordinary value serializer over typed markers.

With an atomic provider, DistributedDuration and
DistributedFailSafeMaxDuration do not expire durable markers, including explicit
mutations: the atomic control protocol retains monotonic tombstones independently
of ordinary value TTL. In CachedSnapshots, those distributed durations govern
the expendable observation record. In byte-only mode they govern the ordinary
remote marker's physical TTL; DurableOnly selects no snapshot repair factory,
not an atomic storage guarantee.
SkipDistributedWrite and background distributed writes also select its renewal
behavior. Backplane notification options control explicit marker mutations;
read renewal never publishes an invalidation.
Mutation operation overrides affect their write/seeding; future read policy
still comes from cache-wide tag defaults.

Data recovery retains strict durable reconciliation/commit admission regardless
of this permissive read selection. Recovery marker advances update the ledger
without seeding this optional observation cache. These are explicit stronger
boundaries, not claims of literal upstream behavior.

`marker_read_contract` contains executable public probes for read independence,
freshness/jitter, skips, soft/hard and per-marker budgets, lock fallback, original
fault causes, cancellation/drop/shutdown, notification races, continuity gaps,
short-circuit order, eager/passive ownership and old provider compatibility.
The complete TagsDefaultEntryOptions factory/renewal contract and whole-library
functionality remain incomplete; see [FULL_CONTRACT.md](FULL_CONTRACT.md).

A separately supplied `MemoryStorage<MarkerObservation>` participates in actual
secondary storage, including memory-only shared tag/clear coordination. See
[provider, namespace and failure boundaries](MARKER_MEMORY_STORAGE.md).
