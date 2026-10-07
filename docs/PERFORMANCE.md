# Development performance qualification

These measurements describe the developing 0.4 source. The published package
remains 0.3.1. Release qualification requires native Linux budgets, the final
API, the paired behavioral matrix and all final release gates.

## Local comparison, 2026-10-07

Seven alternating process pairs per API on macOS 26.6.2 arm64, with 12 available
physical cores and 12 logical CPUs. Rust 1.88.0; .NET SDK 10.0.300, runtime
10.0.8; released FusionCache 2.9.0, locked by `packages.lock.json`.

The workload uses `u64`, one hot key or one key per worker. Set and cold run in
separate fresh processes with identical operation counts. L2 uses the in-memory
providers and JSON, bypasses L1 reads through public options while preserving
hydration, seeds conflicting L1/L2 values, checks the result and prohibits the
factory. Serialization, lookup and hydration remain inside the timed operation.

| Operation | Amalgam ns/op | FusionCache ns/op | Qualification |
|---|---:|---:|---|
| Async L1 read, one worker | 36.151 | 289.737 | Pass |
| Async L1 `get_or_set`, one worker | 46.369 | 336.520 | Pass |
| Same key, eight workers, `get_or_set` | 6.314 | 79.585 | Pass |
| Distinct keys, eight workers, `get_or_set` | 6.403 | 59.953 | Pass |
| L1 replacement | 92.180 | 142.242 | Pass |
| Cold factory | 935.510 | 2065.205 | Pass |
| L2 plus JSON, read | 1470.772 | 1753.618 | Pass |
| L2 plus JSON, `get_or_set` | 1848.484 | 1946.456 | Pass |

Eight-worker ns/op is elapsed time divided by all completed operations; it is
aggregate throughput, not the latency experienced by one caller. Distinct-key
scaling is 7.403x for read and 7.355x for `get_or_set`, exceeding the 6x budget
on this machine with at least eight physical cores. Warm hits and L1 replacement
allocate zero; cold allocates 5.009 per operation; L2 read and factory retrieval
allocate 23 and 24 respectively. All local budgets pass for both APIs.

Read L2 ranges were 1427.245–1491.179 ns versus 1721.212–1822.333 ns.
Factory-retrieval L2 ranges were 1824.488–1901.733 ns versus
1918.939–1974.268 ns. These improvements do not depend on overlapping ranges.

These are working-tree measurements. Reports record runtime source and binary
hashes, including unrelated local edits. Exact committed Linux source and the
final release API require their own qualification.

## Same-machine change comparison

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
This removes three further L2 allocations compared with the previous pipeline.

The same comparison recorded a 3–6% slowdown in some warm scenarios. A further
seven-pair comparison kept unrelated `cache.rs` editor changes identical in
both builds. L2 medians still improved by 8.68% and 10.38%, with the same three
fewer allocations. Warm one-worker medians were about 5% slower, with a larger
change in distinct-key factory retrieval and broad overlapping ranges at eight
workers. Thus these results do not claim unchanged warm timing or a gain in
set/cold. All comparative local budgets still pass; the architectural step is
judged against those budgets rather than a microbenchmark percentage gate.
Native Linux gain and final release qualification remain open.

An available built-in per-key mutex is acquired synchronously; contention still
waits through Tokio's ordinary FIFO acquisition. Busy coordination never becomes
a cache miss. Cooperative scheduling remains enabled globally.

## Last completed Linux qualification

The [CI run for fa6](https://github.com/ZoonTronLabs/amalgam/actions/runs/37608572833)
completed with 14 functional gates passing and the comparative gate failing.
Its CPU was AMD EPYC 9V74 with **two available physical cores and four logical
CPUs**. It is a different machine from the local table, and eight worker threads
here do not establish eight-core scaling. This run precedes the borrowed phase.

| L2 plus JSON on fa6 | Amalgam ns/op | FusionCache ns/op | Result |
|---|---:|---:|---|
| Read | 2405.747 | 2072.508 | Fail: 16.08% slower |
| `get_or_set` | 3058.638 | 2294.267 | Fail: 33.32% slower |

Warm, cold and write budgets passed on that runner. Its separate, same-runner
three-pair comparison against the preceding source measured L2 `get_or_set`
3236.144→3091.064 ns (-4.48%) and cold 2226.956→2166.386 ns (-2.72%).
L2 read changed 2457.516→2493.483 ns with overlapping ranges; no read gain is
claimed. Absolute results from separate CI runs cannot establish a change gain.
The borrowed phase needs its own native comparative and before/after results.

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
