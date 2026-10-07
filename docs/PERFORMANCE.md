# Development performance qualification

These measurements describe the developing 0.4 source. The published package
remains 0.3.1. Release qualification requires native Linux budgets, the final
API, the paired behavioral matrix and all final release gates.

## Local comparison, 2026-10-07

Seven alternating process pairs per API on macOS 26.6.2 arm64, with 12 available
physical cores and 12 logical CPUs. Rust 1.95.0; .NET SDK 10.0.300, runtime
10.0.8; released FusionCache 2.9.0, locked by `packages.lock.json`.
The minimum compiler, Rust 1.88.0, is checked separately for contracts and lints.

The workload uses `u64`, one hot key or one key per worker. Set and cold run in
separate fresh processes with identical operation counts. L2 uses the in-memory
providers and JSON, bypasses L1 reads through public options while preserving
hydration, seeds conflicting L1/L2 values, checks the result and prohibits the
factory. Serialization, lookup and hydration remain inside the timed operation.

| Operation | Amalgam ns/op | FusionCache ns/op | Qualification |
|---|---:|---:|---|
| Async L1 read, one worker | 38.504 | 298.852 | Pass |
| Async L1 `get_or_set`, one worker | 43.519 | 341.502 | Pass |
| Same key, eight workers, `get_or_set` | 5.798 | 79.277 | Pass |
| Distinct keys, eight workers, `get_or_set` | 5.871 | 59.557 | Pass |
| L1 replacement | 89.650 | 143.593 | Pass |
| Cold factory | 931.533 | 2155.897 | Pass |
| L2 plus JSON, read | 1527.862 | 1805.452 | Pass |
| L2 plus JSON, `get_or_set` | 1836.406 | 1973.109 | Pass |

Eight-worker ns/op is elapsed time divided by all completed operations; it is
aggregate throughput, not the latency experienced by one caller. Distinct-key
scaling is 7.441x for read and 7.416x for `get_or_set`, exceeding the 6x budget
on this machine with at least eight physical cores. Warm hits and L1 replacement
allocate zero; cold allocates 5.009 per operation; L2 read and factory retrieval
allocate 23 and 24 respectively. All local budgets pass for both APIs.

Read L2 ranges were 1523.020–1541.457 ns versus 1791.786–1813.829 ns.
Factory-retrieval L2 ranges were 1825.253–1897.241 ns versus
1948.193–1988.610 ns. The local comparison passes with non-overlapping L2 ranges.

These are working-tree measurements. Reports record runtime source and binary
hashes, including unrelated local edits. Exact committed Linux source and the
final release API require their own qualification.

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
lints pass on ARM and x86. Exact-source native Linux results for this step are
still required; the completed Linux run below precedes this change.

## Latest native Linux comparison

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
python3 benches/run-before-after.py --baseline fa6e0b257e6fc6b1ee3e73b8780fbdd187792d63 --output /tmp/amalgam-before-after
```

CI enforces the same complete budgets for both APIs. Full package, consumer,
MSRV and live Redis checks run at milestones. Differences of about 1% are not
claimed as improvements. No performance result establishes complete behavioral
parity; see [the roadmap](ROADMAP.md) and [contract and migration notes](PARITY.md).
