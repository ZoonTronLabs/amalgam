# Development performance qualification

These measurements describe the developing 0.4 source, not the published 0.3.1
package. Release qualification is still open: L2 factory retrieval, the final
API, the paired behavioral matrix and the final release gates must pass.

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
| Async L1 read, one worker | 35.444 | 290.778 | Pass |
| Async L1 `get_or_set`, one worker | 44.470 | 341.938 | Pass |
| Same key, eight workers, `get_or_set` | 5.959 | 82.892 | Pass |
| Distinct keys, eight workers, `get_or_set` | 6.130 | 59.230 | Pass |
| L1 replacement | 91.576 | 143.340 | Pass |
| Cold factory | 946.778 | 2065.757 | Pass |
| L2 plus JSON, read | 1695.746 | 1762.752 | Pass |
| L2 plus JSON, `get_or_set` | 2055.380 | 1954.917 | **Fail: 5.14% slower** |

Eight-worker ns/op is elapsed time divided by all completed operations; it is
aggregate throughput, not the latency experienced by one caller. Distinct-key
scaling is 7.375x for read and 7.350x for `get_or_set`, exceeding the 6x budget
on this machine with at least eight physical cores. Warm hits and L1 replacement
allocate zero; cold allocates 5.009 per operation; L2 read and factory retrieval
allocate 26 and 27 respectively.

Read L2 ranges were 1649.178–1702.142 ns versus 1752.140–1802.557 ns.
Factory-retrieval L2 ranges were 2041.094–2112.747 ns versus
1932.344–1985.676 ns. The failing L2 result is not treated as measurement noise.

These are working-tree measurements. Reports record every runtime source and
binary hash, including unrelated local edits. Exact committed Linux source and
the final release API require their own qualification; this table is not that
final qualification.

## Same-machine change comparison

Three counterbalanced pairs compile one frozen current harness against both
the `dd50b41571af2529cf6e8d3c7bcffd590fa66962` source and the candidate.
Dependencies and actual harness bytes match; input source hashes are recorded.

| Operation | Baseline ns/op | Candidate ns/op | Change |
|---|---:|---:|---:|
| L2 plus JSON, read | 1791.559 | 1699.814 | -5.12% |
| L2 plus JSON, `get_or_set` | 2219.264 | 2047.436 | -7.74% |
| Cold factory | 1095.964 | 944.227 | -13.85% |
| L1 replacement | 90.455 | 90.564 | No measured gain |

Nested L2 phases retain their exact cancellation token, checkpoint and counted
polling, but register shutdown waiters and task tracking only when they suspend.
Available built-in per-key mutexes are acquired synchronously; contention still
waits through Tokio's ordinary FIFO acquisition. Busy coordination never becomes
a cache miss. This removes two L2 allocations and avoids scheduler participation
for an otherwise ready operation. Cooperative scheduling is not disabled globally.

## Last completed Linux qualification

The [CI run for dd50](https://github.com/ZoonTronLabs/amalgam/actions/runs/37603067620)
completed with 14 functional gates passing and the comparative gate failing.
Its CPU was AMD EPYC 9V74 with **two available physical cores and four logical
CPUs**. It is a different machine from the local table, and eight worker threads
here do not establish eight-core scaling.

| L2 plus JSON on dd50 | Amalgam ns/op | FusionCache ns/op | Result |
|---|---:|---:|---|
| Read | 3170.937 | 2670.806 | Fail: 18.73% slower |
| `get_or_set` | 4020.895 | 3052.316 | Fail: 31.73% slower |

The remaining warm, cold and write budgets passed on that runner. The new runtime
changes require a native Linux comparison against the same baseline on the same
runner; an FC pass on a different CPU is not proof of an improvement.

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
python3 benches/run-before-after.py --baseline dd50b41571af2529cf6e8d3c7bcffd590fa66962 --output /tmp/amalgam-before-after
```

CI enforces the same complete budgets for both APIs. Full package, consumer,
MSRV and live Redis checks run at milestones. Differences of about 1% are not
claimed as improvements. No performance result establishes complete behavioral
parity; see [the roadmap](ROADMAP.md) and [contract and migration notes](PARITY.md).
