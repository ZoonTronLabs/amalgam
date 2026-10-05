# Independent secondary marker reads (unreleased)

`MarkerReadPolicy::DurableRequired` remains the default. It preserves the
existing healthy-local L1 contract and durable cold hydration: value retrieval,
decoding and required marker validation share the value's distributed deadline.
It does not promise a durable check on every L1 hit.

`MarkerReadPolicy::OptionsControlled` explicitly selects independent secondary
checks. They use `tags_default_options`, bypass the ordinary value-options
provider and run after the value read/decode deadline. Each marker has its own
owned deadline. A missing/stale observation can send a fresh value L1 hit through
the asynchronous owned pipeline.

```rust
use amalgam::{Cache, EntryOptions, MarkerReadPolicy, MemoryLimits, Timeout};
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

Attach a participating `InvalidationStore` for distributed control checks. In a
memory-only cache, local invalidation facts still apply. Selecting independent
checks with a byte-only L2 and no marker capability is a typed Unsupported error.

## Authority, retention and cancellation

An evictable observation cache is separate from the monotonic invalidation
ledger. Expiring/evicting an observation never erases a known invalidation.
Successful absence is distinct from a skipped or failed check. Observation
admission atomically merges maxima; a delayed response cannot overwrite a newer
peer observation, and a degraded fallback cannot replace a concurrent refresh.
Continuity generations fence observations and value hydration across gaps.

`CacheEvent::MarkerRead` reports the actual authority: `Observed`, `Cached`,
`KnownMaximum`, `Skipped`, `StaleFallback(reason)` or `Unavailable(reason)`.
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
| SkipDistributedCacheReadWhenStale | Applies to an existing stale marker observation, not a stale value |
| DistributedSoftTimeout / HardTimeout | Independent per-marker provider phase; cold markers ignore soft timeout |
| ReThrowDistributedCacheExceptions | Controls actual marker backend faults |
| ReThrowSerializationExceptions | Controls typed marker protocol faults; control reads do not invoke `Serializer<V>` |
| LockTimeout / MemoryLockTimeout | Marker single-flight wait, with the memory override taking precedence |
| FactorySoftTimeout | Also supplies an otherwise infinite marker-lock wait when retained fail-safe is available |
| Size / Priority | Observation admission/eviction within independent `marker_read_limits`; default count limit is 4096 including clear observations |
| EnableAutoClone | Marker payloads are closed immutable Copy values; no ordinary `ValueCloner<V>` is required or called |
| AllowStaleOnReadOnly | Does not select secondary fallback; the marker helper is not an ordinary read-only value operation |

The remaining fields are not implemented as secondary marker factory controls.
FactoryHardTimeout, background factory completion, marker eager refresh and
distributed marker locking remain open work. EagerRefreshThreshold metadata
alone does not start a marker eager refresh. SkipAutoCloneForImmutableObjects
does not force an ordinary value serializer over typed markers.

DistributedDuration and DistributedFailSafeMaxDuration do not expire durable
markers, including explicit mutations: the atomic control protocol retains
monotonic tombstones independently of ordinary value TTL. Secondary reads do
not currently perform FusionCache's stale/nonzero marker renewal/repair writes.
Accordingly SkipDistributedWrite, background distributed writes and backplane
notification options control explicit marker mutations, not read renewal.
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
