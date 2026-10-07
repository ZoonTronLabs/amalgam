# Development performance qualification

These measurements describe the developing 0.4 source. The published package
remains 0.3.1. Release qualification requires native Linux budgets, the final
API, the paired behavioral matrix and all final release gates.

## Local comparison, 2026-10-07

Seven alternating process pairs per API on macOS 26.6.2 arm64, with 12 available
physical cores and 12 logical CPUs. Rust 1.88.0; .NET SDK 10.0.300, runtime
10.0.8; released FusionCache 2.9.0, locked by `packages.lock.json`.
These working-tree measurements include the build-selected hybrid scalar-control
pool and a typed observer handle without a second boxed execution driver.

The workload uses `u64`, one hot key or one key per worker. Set and cold run in
separate fresh processes with identical operation counts. L2 uses the in-memory
providers and JSON, bypasses L1 reads through public options while preserving
hydration, seeds conflicting L1/L2 values, checks the result and prohibits the
factory. Serialization, lookup and hydration remain inside the timed operation.

| Operation | Amalgam ns/op | FusionCache ns/op | Qualification |
|---|---:|---:|---|
| Async L1 read, one worker | 36.130 | 300.951 | Pass |
| Async L1 `get_or_set`, one worker | 46.502 | 339.498 | Pass |
| Same key, eight workers, read | 4.786 | 78.686 | Pass |
| Distinct keys, eight workers, read | 4.795 | 55.714 | Pass |
| Same key, eight workers, `get_or_set` | 6.098 | 79.423 | Pass |
| Distinct keys, eight workers, `get_or_set` | 6.141 | 58.620 | Pass |
| L1 replacement | 91.437 | 142.059 | Pass |
| Cold factory | 969.857 | 2118.616 | Pass |
| L2 plus JSON, read | 1405.042 | 1811.911 | Pass |
| L2 plus JSON, `get_or_set` | 1690.867 | 1959.653 | Pass |

Eight-worker ns/op is elapsed time divided by all completed operations; it is
aggregate throughput, not the latency experienced by one caller. Distinct-key
scaling is 7.583x for read and 7.559x for `get_or_set`, exceeding the 6x budget
on this machine with at least eight physical cores. Warm hits and L1 replacement
allocate zero; cold allocates 5.009 per operation; both L2 APIs allocate 19.
All local budgets pass for both APIs.

Read L2 ranges were 1378.737–1424.115 ns versus
1794.050–1835.016 ns. Factory-retrieval L2 ranges were
1668.696–1730.804 ns versus 1944.020–1983.752 ns. Neither L2 pair overlaps.

These are working-tree measurements. Reports record runtime source and binary
hashes, including unrelated local edits. Exact committed Linux source and the
final release API require their own qualification.

## Typed execution observer

The caller now stores a movable typed execution handle. Its work remains pinned
and owned by the existing cache scope; the extra boxed observer driver is gone.
Explicit cancellation links every terminal reason on the first poll, wakes the
scope and retires pending work even when the caller never polls again. Caller
token ownership remains until completion. Failed preparation retains its prior
lazy adapter and captures. Root scope registration and shutdown ownership are
unchanged; removing general per-operation scopes remains a separate Must.

Three counterbalanced pairs against `5a20b7814e69041f1f27ee7e40b160254bedcddb`
use Rust 1.88.0, one frozen original timed harness and identical unrelated edits:

| Operation | Baseline ns/op | Candidate ns/op | Interpretation |
|---|---:|---:|---|
| L2 plus JSON, read | 1438.652 | 1416.439 | 20→19 allocations; timing change below 2%, no gain claimed |
| L2 plus JSON, `get_or_set` | 1782.023 | 1696.434 | 4.80% faster; 20→19 allocations |
| L1 replacement | 90.150 | 90.904 | No measured gain |
| Cold factory | 943.784 | 950.730 | Below 1%; no gain claimed |
| Synchronous L1 `get_or_set` | 39.197 | 41.429 | 5.69% slower; local reference budget still passes |

Factory-retrieval L2 ranges are separate: 1762.476–1805.581 versus
1688.197–1756.542 ns. Synchronous factory-retrieval ranges are also separate:
37.162–39.401 versus 39.612–41.568 ns. Unchanged timing for all L1 paths is
not claimed. Warm hits and writes still allocate zero; cold retains 5.009.
All 705 default contracts/doctests, 87 x86 unit tests, formatting, strict
all-target/all-feature MSRV clippy on ARM/x86 and both ownership probes pass.
Exact committed-source Linux qualification remains open for this candidate.

## Build-selected scalar coordination reuse

Idle local mutexes and mutation lanes previously lost their final strong owner
on every distributed lookup. Untimed allocation stacks identify three repeated
allocations in read-only L2 retrieval and four in factory retrieval.

Only hybrid storage now selects a pool of at most 16 scalar controls per shard,
across 64 shards. A sealed internal capability admits only `Mutex<()>` and
`KeyLane`; the pool cannot retain user values or factory work. Holders and
waiters retain the same weak-slot identity through pool eviction and explicit
maintenance. Tokio FIFO acquisition and per-key ordering remain.

Three counterbalanced pairs compare the candidate with
`7c01404f664d91702de673d7ba603272203f4205` on Rust 1.88.0. Both builds use
one original timed harness and the same unrelated local edits:

| Operation | Baseline ns/op | Candidate ns/op | Interpretation |
|---|---:|---:|---|
| L2 plus JSON, read | 1511.814 | 1449.279 | 4.14% faster; 23→20 allocations |
| L2 plus JSON, `get_or_set` | 1886.566 | 1775.102 | 5.91% faster; 24→20 allocations |
| L1 replacement | 89.315 | 91.068 | 1.96% slower median; no gain claimed |
| Cold factory | 967.295 | 987.970 | Overlapping ranges; no gain claimed |

Both L2 before/after ranges are separate: read 1510.127–1520.528 versus
1429.850–1461.361 ns, and factory retrieval 1854.037–1890.381 versus
1763.131–1820.304 ns. Cold ranges overlap at 931.430–983.981 versus
955.086–1002.952 ns, with the same 5.009 allocations. Some warm medians also
vary, including a 4.56% higher eight-worker distinct-key factory-retrieval
median with overlapping ranges. Unchanged timing for every L1 case is not claimed.

An unconditional pool was rejected: its standalone cold median was 7.95% slower
with separate ranges and extra allocations. Selecting the transient standalone
plan at construction removes that pool's maintenance and allocation cost from L1.
Untimed stack capture lives in a separate ignored benchmark executable; the
timed allocator and original comparative workloads remain unchanged.

All 702 default contracts/doctests, 84 x86 unit tests and strict all-target,
all-feature MSRV clippy on ARM/x86 pass. Both ownership probes pass.
The current per-key Tokio lane is still awaiting the target hybrid shard-queue
migration. This step does not complete that requirement or general scope
elimination. Its exact-source Linux qualification is recorded below; the full
release performance budgets remain open.

## Previous architectural step: borrowed L2 work

Three counterbalanced pairs compile one frozen current harness against both
the `fa6e0b257e6fc6b1ee3e73b8780fbdd187792d63` source and the candidate.
Dependencies and actual harness bytes match; input source hashes are recorded.

| Operation | Baseline ns/op | Candidate ns/op | Change |
|---|---:|---:|---:|
| L2 plus JSON, read | 1686.317 | 1499.558 | -11.08% |
| L2 plus JSON, `get_or_set` | 2065.743 | 1816.523 | -12.06% |
| Cold factory | 942.965 | 937.262 | No measured gain |
| L1 replacement | 90.796 | 90.314 | No measured gain |

A nested L2 phase borrows its work, key and worker from the existing cache-owned
parent. It retains an independent cancellation token and checkpoint, but no
second boxed future, owned worker or shutdown task registration. Its parent
already owns and drains that entire pinned frame, including pending work when
no caller polls again. Phase cancellation is published before cache-controlled
future destruction. Soft/hard L2 deadlines remain independent of the origin.
Measured L2 allocations fall by three locally and by two on native Linux;
compiler/platform differences are retained in the reports.

The same comparison recorded a 3–6% slowdown in some warm scenarios. A further
seven-pair comparison kept unrelated `cache.rs` editor changes identical in
both builds. L2 medians still improved by 8.68% and 10.38%, with the same three
fewer allocations. Warm one-worker medians were about 5% slower, with a larger
change in distinct-key factory retrieval and broad overlapping ranges at eight
workers. Thus these results do not claim unchanged warm timing or a gain in
set/cold. All comparative local budgets still pass; the architectural step is
judged against those budgets rather than a microbenchmark percentage gate.
Native Linux measurements below verify a smaller L2 gain. Final release
qualification remains open.

An available built-in per-key mutex is acquired synchronously; contention still
waits through Tokio's ordinary FIFO acquisition. Busy coordination never becomes
a cache miss. Cooperative scheduling remains enabled globally.

## Disabled distributed-locker ownership

Local coordination now carries a closed `Local` state. Only cooperative and
fenced leases own the task, event and key resources needed to release an actual
lease. This removes unused shared-owner increments without introducing invalid
policy/lease combinations. Release, lease-loss, fencing, recovery and post-lock
destruction retain their previous contracts.

Three same-machine, counterbalanced pairs compare this change against
`72f436c30fe70b6631c562c427d80515bcf9beb5`, using Rust 1.88.0 and identical
unrelated local edits in both variants:

| Operation | Baseline ns/op | Candidate ns/op | Interpretation |
|---|---:|---:|---|
| L2 plus JSON, read | 1486.278 | 1489.500 | No measured gain |
| L2 plus JSON, `get_or_set` | 1797.588 | 1825.580 | 1.56% slower median; no gain claimed |
| L1 replacement | 89.314 | 90.272 | Overlapping ranges; no gain claimed |
| Cold factory | 936.883 | 944.760 | Overlapping ranges; no gain claimed |

Allocation counts are unchanged. The step enforces disabled-feature ownership
and all local comparative budgets still pass; it is not reported as a timing
improvement. Minimum-compiler default contracts and strict all-target/all-feature
lints pass on ARM and x86. Exact-source native Linux results are recorded below; the later bounded-control
candidate has its own Linux qualification below.

## Latest native Linux comparison

The [CI run for 5a20b78](https://github.com/ZoonTronLabs/amalgam/actions/runs/37621686618)
completed with all 14 functional milestone jobs passing. The comparative and
aggregate jobs failed the L2 and L1 replacement performance budgets for both APIs.
The typed-observer candidate above has not yet been measured on this runner.

This runner was AMD EPYC 9V45 with **two available physical cores and four
logical CPUs**; Rust 1.88.0, .NET SDK 10.0.401/runtime 10.0.12 and locked
FusionCache 2.9.0. Seven pairs ran per API. Eight worker threads here do not
establish eight-core scaling.

| Operation on 5a20b78 | Amalgam ns/op | FusionCache ns/op | Result |
|---|---:|---:|---|
| L2 plus JSON, read | 2099.981 | 1523.896 | Fail: 37.80% slower |
| L2 plus JSON, `get_or_set` | 2496.249 | 1701.081 | Fail: 46.74% slower |
| L1 replacement, read fixture | 188.947 | 234.136 | Fail: 0.807×, budget ≤0.75× |
| L1 replacement, `get_or_set` fixture | 184.039 | 229.466 | Fail: 0.802×, budget ≤0.75× |
| Cold factory, read fixture | 2008.099 | 3442.174 | Pass |
| Cold factory, `get_or_set` fixture | 1991.438 | 3400.188 | Pass |

L2 read ranges were 2035.745–2139.187 versus 1483.819–1643.673 ns;
factory retrieval was 2408.922–2523.770 versus 1631.637–1785.497 ns.
Warm and cold budgets passed. L2 allocates 17 for both APIs; warm hits and
writes allocate zero. Writes beat the reference but miss the stricter budget.

A same-runner three-pair comparison against 7c01404 measured read
2192.918→2089.751 ns (-4.70%, ranges overlap) and factory retrieval
2563.028→2454.765 ns (-4.22%, ranges separate). Read/retrieval allocations
fell 20→17 and 21→17. Set changed 183.090→184.126 ns (+0.57%) and cold
2051.441→2027.514 ns (-1.17%), both with overlapping ranges; no timing gain
is claimed. The write budget was already missed on this baseline. This EPYC
and earlier Xeon runners differ; separate-run absolute values do not establish
a code-induced regression. Native budgets remain open.

## Previous native source: disabled locker ownership

The [CI run for 7c01404](https://github.com/ZoonTronLabs/amalgam/actions/runs/37617049677)
completed with all 14 functional milestone jobs passing. The comparative and
aggregate jobs failed only the L2 performance budget for both APIs.

This runner was Intel Xeon Platinum 8370C with **two available physical cores
and four logical CPUs**; Rust 1.88.0, .NET SDK 10.0.401/runtime 10.0.12 and
locked FusionCache 2.9.0. Seven pairs ran per API. Eight worker threads on this
runner do not establish eight-core scaling.

| L2 plus JSON on 7c01404 | Amalgam ns/op | FusionCache ns/op | Result |
|---|---:|---:|---|
| Read | 3151.362 | 2569.427 | Fail: 22.65% slower |
| `get_or_set` | 3758.472 | 2870.652 | Fail: 30.93% slower |

Read ranges were 3109.537–3166.951 ns versus 2525.945–2615.289 ns;
factory-retrieval ranges were 3729.865–3833.070 ns versus
2830.052–2883.710 ns. Warm, cold and write budgets passed. L2 read/retrieval
allocated 20/21; warm hits and writes allocated zero.

A same-runner, three-pair comparison against 72f436c measured read
3223.351→3136.218 ns (-2.70%) and factory retrieval
3859.182→3784.180 ns (-1.94%). The latter falls within the two-percent
interpretation threshold; no gain is claimed. Allocations were unchanged and
set/cold ranges overlapped. This Xeon and the preceding EPYC runners differ;
comparing their absolute values does not establish a code-induced regression.
The later bounded-control source has its own exact-source Linux run above.

## Previous native source: borrowed L2 work

The comparative portion of the [CI run for 72f436c](https://github.com/ZoonTronLabs/amalgam/actions/runs/37612542515)
completed: all 14 functional milestone jobs passed, including live Redis,
packaged consumers, MSRV and platform checks. The comparative job failed the L2
budget for both APIs, so the aggregate release gate failed.

The runner was AMD EPYC 9V74 with **two available physical cores and four
logical CPUs**; Rust 1.88.0, .NET SDK 10.0.401/runtime 10.0.12 and locked
FusionCache 2.9.0. It is a different machine from the local table. Eight worker
threads here do not establish eight-core scaling.

| L2 plus JSON on 72f436c | Amalgam ns/op | FusionCache ns/op | Result |
|---|---:|---:|---|
| Read | 2254.107 | 2110.394 | Fail: 6.81% slower |
| `get_or_set` | 2906.801 | 2334.241 | Fail: 24.53% slower |

Read ranges were 2238.674–2290.918 ns versus 2093.206–2143.781 ns;
factory-retrieval ranges were 2844.816–2923.430 ns versus
2303.075–2369.223 ns. Neither remaining failure is treated as noise.
Warm, cold and write budgets passed. Native L2 read/retrieval allocated 20/21
per operation; warm hits and writes remained allocation-free.

The same-runner three-pair source comparison against fa6 measured:

| Operation | Previous ns/op | Current ns/op | Change |
|---|---:|---:|---:|
| L2 plus JSON, read | 2399.417 | 2255.874 | -5.98% |
| L2 plus JSON, `get_or_set` | 3064.634 | 2905.989 | -5.18% |
| L1 replacement | 204.707 | 206.110 | No measured gain |
| Cold factory | 2341.516 | 2343.999 | No measured gain |

Both L2 improvements have non-overlapping before/after ranges and two fewer
allocations. Native warmed `get_or_set` changes are within 0.3% at one/eight
workers. One warmed same-key read sample at eight workers is 3.13% slower;
unchanged warm timing is not claimed. Absolute results from separate CI runs
cannot establish a change gain. This accepted architectural step improves L2,
but does not complete the native budget or release qualification.

## Reproduce

Use one shared build directory and retain each result directory. The reference
project restores the locked released package. Each report includes raw CSVs,
allocation samples, source and executable hashes, runtime identities and topology.

```sh
export CARGO_TARGET_DIR=/tmp/amalgam-build-shared
export CARGO_INCREMENTAL=0
export RUSTUP_TOOLCHAIN=1.88.0
python3 benches/run-scaling.py --api read --gate all --output /tmp/amalgam-read
python3 benches/run-scaling.py --api get-or-set --gate all --output /tmp/amalgam-get
python3 benches/run-before-after.py --baseline 5a20b7814e69041f1f27ee7e40b160254bedcddb --output /tmp/amalgam-before-after
cargo +1.88.0 test --release --locked --bench l2_ownership -- --ignored --nocapture --test-threads=1
```

CI enforces the same complete budgets for both APIs. Full package, consumer,
MSRV and live Redis checks run at milestones. Differences of about 1% are not
claimed as improvements. No performance result establishes complete behavioral
parity; see [the roadmap](ROADMAP.md) and [contract and migration notes](PARITY.md).
