# Porting the FusionCache model to Rust

Amalgam adapts FusionCache's observable cache behavior rather than copying every C# implementation choice. Source inspection, released-reference experiments and public-API regression tests provide different kinds of evidence; none alone proves universal parity. The exact reference pins and supported behavior are in [PARITY](docs/PARITY.md).

Two approaches were considered for the repair: patch individual flag branches, or consolidate causally related ownership and commit decisions. The latter provides shared cancellation, lifecycle, ordering and error policy while keeping codecs, stores, backplanes, plugins and copy strategies extensible.

## Invariants before implementation

- A live origin or distributed lease has one owner. Cancellation releases pending work and guards; explicit handoff transfers that ownership into supervised background work.
- A successful miss is separate from an operation failure. Expected failure uses a typed result and preserves its original source.
- A source snapshot's revision time differs from the local insertion instant. Hydration or recovery cannot renew an exhausted source lifetime.
- L1 and L2 have independent freshness/physical deadlines.
- A replay owns an exact queue identity, generation and pending stage. Completing old work cannot remove a replacement queue item.
- Notifications follow committed effects. Same-key local commit lanes order foreground writes and already-started replay; independent nodes need a participating conditional protocol for stronger guarantees.
- Data and control messages are distinct types. Physical namespaces are injectively encoded and control keys cannot collide with ordinary data.
- Closed internal states carry only valid state data. Integration behavior stays open through traits.

## C# to Rust decisions

| C# concept | Rust adaptation |
|---|---|
| Shared cache reference | Cloneable public handle sharing cache state and an independent last-public-handle lifetime token |
| Immutable entry | Shared immutable snapshot; expiry/refresh create a new version |
| Business failure/exception | `Result` with typed error families; source chains retained at boundaries |
| Successful lookup miss | `MaybeValue::none`, inside a successful fallible read |
| State discriminator with linked nullable fields | Closed enum with variant-specific data |
| Infinite timeout sentinel | `Timeout::Infinite`; finite duration is an explicit variant |
| Cancellation token | Explicit cancellation source/request and origin cancellation context with typed reasons |
| Disposable ownership | RAII guards, owned leases, supervised work, `close` and awaited `shutdown` |
| Background task | Owned origin/effect continuation; no detached factory merely to implement a foreground timeout |
| `IDistributedCache` | Open byte-store trait plus optional atomic invalidation and conditional ownership capabilities |
| Serializer | Open trait; legacy DTO compatibility plus versioned snapshot frames |
| AutoClone | Registered fallible copy behavior; `Clone` alone does not isolate `Arc` interior mutability |
| Memory priority/size | Separate entry-count and weight limits, priority-aware admission and exact removal accounting |
| Event handlers/plugins | One event route, per-cache plugin attachments and deterministic registration teardown |
| Time provider | Injected domain clock; timer behavior explicitly distinguishes real monotonic I/O from controlled clocks |

The crate uses its pinned Rust 1.88 minimum and edition 2024. It forbids unsafe code. Constructor/factory validation rejects invalid configuration before background tasks begin; adding a data-state variant intentionally requires reviewing its exhaustive consumers.

## Ownership and cancellation

A caller can own a foreground origin, transfer it after a permitted soft timeout, or cancel it. Hard timeout, caller cancellation, cache shutdown and lease loss are different outcomes. Expected cancellation does not become fail-safe success. Registered scopes allow an explicit token to drop an entered pending origin even while its caller future is parked.

Cache-owned tasks do not own the public lifetime token. Dropping the last public handle initiates cleanup even if diagnostic event handles remain alive. Explicit shutdown waits for owned workers and plugin cleanup and reports failures. An externally supplied shared backplane remains owned by its supplier; detaching one cache does not shut down that shared provider.

## Distributed ordering and protocols

The effective physical namespace determines tag/clear scope. Durable atomic maximum markers prevent missed notifications from becoming permanent invalidation loss. Marker equality is inclusive: `created <= marker`. Finite marker-cache compaction preserves safety through conservative scope invalidation.

Recovery retains original serialized bytes, remaining physical lifetime and pending stages. A failed publication after a successful data write retries publication rather than rewriting old data. Queue supersession uses exact identities independently of wall-clock timestamps. A transport circuit breaker does not treat a codec error as a global network outage.

Owned leases expose their granted token and actual lifetime. Native token-checked release and atomic fenced writes reject replacement-owner races. Unknown custom lease lifetime is not replaced with an invented duration. Explicit legacy coordination is weaker and is documented as such.

The ordinary eight-field legacy DTO remains decodable. A versioned frame adds insertion/retention metadata without assuming that every custom serializer emits a particular format. Unknown or malformed frames fail explicitly. Older running readers still need the [namespace migration](docs/PARITY.md#migrating-from-02).

## Verification method

Preserve original observable tests and add controlled interleavings for identified faults. Do not weaken an assertion merely to make a repair green; use the released reference to resolve genuine semantic ambiguity. Real timers, Redis leases, pub/sub reconnection and downstream packages require real integration evidence in addition to injected-clock and in-memory tests.

Compare performance only after compiling both implementations, with matched ownership/workload and sequential repeated runs. A reference clone and a deep value copy are different experiments. Bounded stress tests, platform CI and security audits complement behavioral tests; they do not prove every external provider or production workload.

See [validation](docs/AUDIT.md), the [README](README.md) and [examples](examples).
