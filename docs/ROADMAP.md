# Amalgam 0.4 roadmap

The target is FusionCache 2.9 behavior for Rust, with an eight-operation fluent
API, predictable destruction/shutdown, and comparative performance budgets.
The published registry package remains 0.3.1 until release qualification finishes.

| Priority | Work | Current state |
|---|---|---|
| Must | Shared-write-free warmed L1 reads, tag verdicts and incremental maintenance | Reader slots, borrowed observations and build-selected primitive copies implemented; zero warm allocations and read budgets qualified for both APIs on Linux |
| Must | Ready factory and inline L1 writes; ownership only for suspended work | Inline commits, unobserved payload retirement, default replacement lifetime facts and general/hybrid caller-drop ownership implemented; cold budget qualified on Linux; final write-budget qualification pending |
| Must | Fail-safe, soft/hard timeouts, L2, backplane, eager, adaptive caching, tags and recovery | Existing contracts retained; finish the paired FC requirement matrix |
| Must | FC defaults and explicit `strict()` | Availability defaults, early fencing validation and public default contracts implemented |
| Must | Eight-operation API, overlays, string tags, `Option<V>`, provider/advanced modules | `set` and `get_or_set` are fluent and fallible; factory-value and remaining API migration open |
| Must | Custom L2 tag compatibility | Ordinary marker fallback and construction advice still open |
| Must | Benchmark budgets, packaged consumers, MSRV, live Redis, complete CI | Both APIs have paired CI measurements; comparative CI enforces warm, cold and write budgets; seven local pairs pass L1/cold/write and eight-core scaling for both APIs; L2+JSON factory retrieval is 13% slower than FC and blocks qualification; final release gates still open |
| Must | README, examples, migration and release tables | Update against the final API and qualified measurements before release |
| Should | Sharded bounded admission, shared sync executor, simplified plugins, testing helpers | After Must; optional features must have no disabled-path overhead |
| Won't | Runtime provider replacement, heterogeneous values, .NET DI/OutputCache adapters, cross-runtime value wire compatibility | Outside this release |

## Acceptance

Every Must needs a public Rust contract and an executed FC 2.9 counterpart where
behavior is observable. Rust-only destruction, cancellation and shutdown rules
remain separately tested. Measurements use alternating processes on the same
machine, medians and 1/2/4/8 threads. Warm hits allocate zero; cold/set budgets
must pass before release. A local pass does not replace Linux qualification.

Full package, consumer, MSRV and Valkey gates run at a milestone. Step checks use
formatting, strict clippy, contracts and scaling. Differences within measurement
noise are not evidence of an improvement. Release descriptions and tables will
state actual supported behavior and any remaining reference differences.
