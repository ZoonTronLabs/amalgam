# Component event observations (unreleased)

`Cache::events()` keeps the existing logical `CacheEvent` stream and plugin
callbacks. `subscribe_layers()` adds an independent stream of physical component
facts. `subscribe_layers_resilient()` continues after broadcast lag and exposes
`lost_events()`. The bounded buffer uses the hub's configured capacity. Events
are best-effort diagnostics; receipt is never an invalidation acknowledgement.

## Typed surface

| Component | Closed event family |
|---|---|
| Memory | `Hit { key, stale }`, `Miss`, `Set`, `Remove`, `Expire`, `Eviction { key, reason }` |
| Distributed | `Hit { key, stale }`, `Miss`, `Set`, `Remove`, `CircuitBreakerChange`, `SerializationError`, `DeserializationError` |
| Backplane | `CircuitBreakerChange`, `MessagePublished { command }`, `MessageReceived { message }` |

Keys in memory/distributed events are processed logical keys (including the
configured user prefix), without the L2 envelope prefix. Backplane payloads retain
the actual transport key. Published commands retain their complete data or scoped
marker payload; received envelopes retain source, revision, action and key before
scope/validation/conflict checks. Foreign malformed frames can therefore appear
in diagnostics while strict reconciliation still invalidates L1. Self frames
and configured IgnoreIncoming frames produce no received event.

`Hit.stale` describes logical expiry of the physically found entry, sampled before
tag/clear eligibility. A component hit can precede a final logical miss, timeout
or error. Fail-safe general stale metadata can also differ from layer logical
expiry. A cold origin normally produces two memory misses because it rechecks
under the key lock. No global one-event-per-call interpretation is valid.

Memory Set requires actual admission; skipped, capacity-rejected or outdated
candidates do not claim a Set. Passive L2 hydration can produce a Memory Set.
Memory Expire describes successful logical expiry; an absent key emits none.
Remove includes an explicit removal of an already absent key. Eligible physical
evictions additionally expose a typed reason; the original stored value is
available through [memory eviction subscriptions](MEMORY_EVICTIONS.md).

Distributed writes/removals are reported only after successful provider effects,
including fenced writes and recovery replay. Retained distributed expiry is a
Set; explicit `DistributedExpirePolicy::Remove` is a Remove. Read absence and
deliberately suppressed provider/deadline/codec failure produce a Miss. An already
decoded Hit is not followed by an invented Miss if marker validation later fails
or times out. Explicit skips and an open circuit do not claim a provider attempt.
Caller cancellation propagates without a fabricated miss or codec failure.

## Failure and delivery policy

`Error::DistributedTimeout { elapsed }` identifies the selected combined
get/decode/required-marker read budget. It is classified as `TimedOut`, does not
trip a transport circuit, and canonical reads preserve it. Origin/legacy fallback
retains the configured suppression policy. Real provider causes remain available
through typed operation/receipt errors. Codecs and read deadlines do not declare
the transport unhealthy. Breaker duration is zero (disabled) by default.

Subscribers own their readers and handler scheduling. A cache-aware plugin may
subscribe through `CachePluginContext::events()` and manage its reader in its
session. Component broadcasts execute no new user callback under storage/lane
locks. Legacy logical plugin dispatch and existing metrics remain unchanged.
The channel is created on first subscription; payloads are built only while it
has receivers. Late subscription observes future emissions, without replay.

## Example

```rust
use amalgam::{Cache, LayerEvent, MemoryEvent};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cache = Cache::<u64>::builder().try_build()?;
    let mut facts = cache.events().subscribe_layers_resilient();
    cache.try_set("answer", 42).await?.wait().await?;
    assert_eq!(facts.recv().await?, LayerEvent::Memory(MemoryEvent::Set {
        key: "answer".into(),
    }));
    assert_eq!(cache.read("answer", None).await?.value(), Some(&42));
    assert_eq!(facts.recv().await?, LayerEvent::Memory(MemoryEvent::Hit {
        key: "answer".into(), stale: false,
    }));
    assert_eq!(facts.lost_events(), 0);
    cache.shutdown().await?;
    Ok(())
}
```

## Reference and remaining contract

The comparison uses inspected official FusionCache v2.9.0 source
`af09f81a3ea8d7ed71183b46501946da801a2a22` and an independently executed published
NuGet 2.9.0 net10.0 assembly (informational version
`2.9.0+c2af1f39d3ad50791109bb9d48c0fdaffba010dd`, SHA-256
`d9deecffee82413ac35e658691a628a09dd91944c0217c18115433aa7daa2c38`). The reference
oracle exercises 16 event names through 16 cases and 176 assertions. Rust public
`layer_event_contract` separately covers logical/physical separation, skips,
codec/provider/deadline/cancellation, rich notifications and real Redis
L2/fenced/pub-sub effects. Reference doubles do not prove native Redis equivalence.

Sixteen distinct Rust event forms do not close the full event contract.
Original-value eviction is available through a separate typed bounded stream,
with explicit insertion/retirement capture and post-coordination reclamation.
Configurable handler scheduling/exception policy and the broader
background/eager/replay/marker/locker matrix remain open in
[FULL_CONTRACT.md](FULL_CONTRACT.md). Literal .NET callback senders and every
upstream mutable-message identity are separate ownership contracts. The published
Amalgam 0.3.1 registry package is unchanged.
