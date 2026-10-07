# Amalgam 0.4 roadmap

The target is FusionCache 2.9 behavior for Rust, with an eight-operation fluent
API, predictable destruction/shutdown, and comparative performance budgets.
The published registry package remains 0.3.1 until release qualification finishes.

| Priority | Work | Current state |
|---|---|---|
| Must | Shared-write-free warmed L1 reads, tag verdicts and incremental maintenance | Reader slots, borrowed observations and build-selected primitive copies implemented; zero warm allocations verified; hot reads frozen; final default-PGO qualification required |
| Must | Ready factory and inline L1 writes; ownership only for suspended work | Inline commits, unobserved payload retirement, default replacement lifetime facts and general/hybrid caller-drop ownership implemented; cold meets the independent default-PGO comparison; set misses <=0.75x; final-source qualification pending |
| Must | Hybrid ordered writes through shard queues; ownership only where work suspends | Scalar ready admission and key-local FIFO waiter queues in shards implemented; no per-key Tokio mutation mutex. Default cache-bound operations promote only after Pending and unused watches are lazy; explicit caller and specialized ownership still need audit |
| Must | ReaderSlots safety and complexity | Actual cfg(loom) admission/guard/parking models pass, including deliberate race detection; x86-only branch removed and two ready plans retained; Linux Loom and native TSan passed; Miri subset passed with the upstream parking limitation documented |
| Must | Fail-safe, soft/hard timeouts, L2, backplane, eager, adaptive caching, tags and recovery | Existing contracts retained; finish the paired FC requirement matrix |
| Must | FC defaults and explicit `strict()` | Availability defaults, early fencing validation and public default contracts implemented |
| Must | Eight-operation API, overlays, string tags, `Option<V>`, provider/advanced modules | `set`, `get_or_set`, remove, expire, tag invalidation and clear are fluent and fallible; raw factory `Result<V, E>`, unified supplied/factory source and native request `execute()` implemented; 11 retrieval aliases removed per facade; read, remaining write/maintenance aliases and provider/root migration open |
| Must | Custom L2 tag compatibility | Ordinary marker fallback and construction advice still open |
| Must | Honest benchmarks, packaged consumers, MSRV, live Redis, complete CI | Default-PGO gate and separate TC=0 column implemented; both initial local matrices and focused L2 comparisons ran. L2 allocations reduced from 14 to 9; L2 and set remain outside budgets. Linux functional/safety jobs passed, mandatory performance gate failed. No main merge or publication until honest final-source qualification; see [measurements](PERFORMANCE.md) |
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

The current work uses a visible branch and PR while mandatory gates are red.
New performance work targets only L2 (<=1.0x honest FC) and set (<=0.75x).
API 0.4, legacy removal and migration proceed as release Must work.
