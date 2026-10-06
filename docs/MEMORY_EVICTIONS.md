# Original-value memory evictions (unreleased)

`Cache::memory_evictions()`, `BlockingCache::memory_evictions()` and
`MemoryStore::evictions()` expose independent bounded typed subscriptions.
`CachePluginContext::memory_evictions()` acquires the same stream through its
weak operational capability. They do not create another cache or own its public
lifetime. These APIs are absent from the published registry 0.3.1.

```rust
use amalgam::{Cache, MemoryEvictionReason};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cache = Cache::<String>::builder().try_build()?;
    let mut retired = cache.memory_evictions().subscribe();
    cache.try_set("key", "original".to_owned()).await?.wait().await?;
    cache.try_remove("key").await?.wait().await?;
    let event = retired.recv().await?;
    assert_eq!(event.key(), "key");
    assert_eq!(event.reason(), MemoryEvictionReason::Removed);
    assert_eq!(event.value(), "original");
    cache.shutdown().await?;
    Ok(())
}
```

## Physical retirement and capture

The closed reason family is `Removed`, `Replaced`, `Expired`, `Capacity`.
Removal includes explicit remove/clear and lost local read eligibility.
Physical expiry is distinct from logical freshness. Priority and pinned-entry
capacity policy remain unchanged. A rejected candidate or absent removal does
not invent an eviction. Logical `Expire` replaces immutable metadata without a
physical eviction event and preserves the prior capture admission.

Each event holds the exact retired `Entry<V>`. `value()` borrows its stored V;
cloning an event or delivering it to several receivers clones entry handles,
never V. This guarantee is about the stored representation. Existing explicit
copy policies and metadata transformations retain their own value-copy rules.
It does not equate Rust value addresses with .NET object identity or wire values.

Default `EvictionCapture::AtInsertion` follows the verified released FusionCache
late-registration rule: an original-value or component-layer receiver must exist
when the value is inserted. A later subscription cannot retroactively arm it.
`CacheBuilder::memory_eviction_capture(EvictionCapture::AtRetirement)` explicitly
also observes older values retired while a receiver exists. No past events replay.
The independently subscribed `MemoryEvent::Eviction { key, reason }` carries the
same eligible physical fact without erasing V's type. Legacy logical
`CacheEvent::Eviction` and inline plugin semantics retain their prior meanings.

## Bounded delivery and ownership

Queue capacity is the configured `events_capacity`. `try_recv()` distinguishes
an empty live stream from closure; `recv()` waits without losing registration
wakeups. Each cursor tracks overwritten records independently with `lost_events`.
The last receiver releases buffered values. If all producer handles are dropped,
receivers can drain retained records before reporting closure. Retaining a
separately cloned producer keeps that diagnostic stream open without keeping the
cache operational. Cache shutdown and producer destruction are separate events.

The ring may retain up to its capacity of original entries even after receivers
read them. These diagnostic and caller-owned entry handles are separate from the
store's count/weight admission budget. Cleanup runs on reads or maintenance;
there is no promise of a timer callback exactly at a physical deadline.
Unbounded clear changes its read-visibility generation immediately. Physical
extraction and corresponding original-value events occur on later reads or
maintenance; new admissions after that barrier survive cleanup. This keeps the
clear barrier independent of the number of retained values. Bounded clear keeps
its existing physical extraction behavior.

Unbounded L1 uses independent read/write sections with one precomputed keyed
hash; bounded L1 retains its atomic priority/capacity plan. Backend extraction
precedes event publication and arbitrary V destruction. A private operation
retains retirements and displaced queue slots through its lane/origin guards,
including a cancelled or unpolled future. Those guards release coordination
before their last reclamation owner. Reader-side value destruction occurs after
the queue guard. Observation introduces no new inline callback.

The contract tests check both stores, exact entry/payload identity, no diagnostic
V clones, late capture, metadata expiry, clear/rejection, physical/capacity causes,
independent lag and closure, same-key native destructor reentry, origin replacement,
suspended-write cancellation and unpolled/suspended guard destruction. These do
not close configurable callback scheduling/exception policy or the entire
background/eager/replay/marker/locker event matrix in [FULL_CONTRACT.md](FULL_CONTRACT.md).

The reference is the independently executed published FusionCache 2.9.0 net10
assembly (`2.9.0+c2af1f39d3ad50791109bb9d48c0fdaffba010dd`), real Microsoft
MemoryCache removal/replacement/capacity/expiry callbacks and late registration.
Its source tag is separately pinned to `af09f81a3ea8d7ed71183b46501946da801a2a22`.
Reference providers were controlled doubles; native Redis parity is a separate
claim requiring native evidence.
