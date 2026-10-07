# Development performance against FusionCache

The published package is 0.3.1; these measurements concern the developing 0.4
source. Release performance qualification is open.

## Method correction, 2026-10-07

The previous harness forced `DOTNET_TieredCompilation=0` and warmed only 20,000
operations. That disables Dynamic PGO and is not the production-default FC
reference. Previous FC PASS claims are withdrawn as release evidence.
The original reports and Rust-only diagnostics remain in
[the historical tables](PERFORMANCE_HISTORY_TC0.md).

The corrected harness runs three isolated modes: Rust, FC with runtime defaults,
and FC with tiering disabled. All six process orderings rotate across pairs.
The gate uses **FC with default tiering/PGO**; TC=0 is a diagnostic column only.
Inherited JIT overrides are removed and their names recorded; runtime discovery
settings remain. Embedded fixture tiering overrides are rejected.

Each scenario warms for at least three seconds. Operation-time windows contain
at least 100 ms; the last five must have max/min at most 1.10. Warmup stops at
15 seconds if it does not settle, and that run fails qualification. Both runtimes
use this same policy, and all settling windows/verdicts are saved. Cold warmup
uses fresh caches of at most 20,000 entries and excludes setup/cleanup from the
operation windows, avoiding unbounded memory growth. Timed operation counts,
checksums and the warmed zero-allocation check remain unchanged.

Tiering optimizes hot methods in the background; Dynamic PGO uses observed
types and paths. See [Microsoft's runtime configuration documentation](https://learn.microsoft.com/en-us/dotnet/core/runtime-config/compilation).
Separate warmup and workload phases follow the principle described by
[BenchmarkDotNet](https://benchmarkdotnet.org/articles/guides/how-it-works.html);
this fixture uses the explicit settling policy above, not BenchmarkDotNet itself.

## Independent reproduction supplied by the owner

Three runs on an M4 Pro using the same fixtures, commit `f277bf6`, and a
1,000,000-operation FC warmup. These are the owner's independent results,
not freshly qualified output from the corrected repository harness.

| Operation | Amalgam ns/op | FC TC=0 ns/op | FC default PGO ns/op | Amalgam / FC default | Required |
|---|---:|---:|---:|---:|---|
| L1 read, one worker | 39.4 | 312.0 | 195.2 | 0.20 | <=0.50 |
| L1 read, eight workers | 5.2 | 59.9 | 57.9 | 0.09 | Read budget passes |
| L1 get_or_set hit | 47.9 | 362.8 | 223.6 | 0.21 | <=0.50 |
| L1 replacement | 89.5 | 140.9 | 102.4 | 0.87 | **Fail: <=0.75** |
| Cold factory | 1017 | 2257 | 1638 | 0.62 | <=0.75 |
| L2 plus JSON, read | 1419 | 1907 | 1192 | 1.19 | **Fail: <=1.00** |
| L2 plus JSON, get_or_set | 1786 | 2076 | 1313 | 1.36 | **Fail: <=1.00** |

The corrected paired repository diagnostic is recorded below. Final-source
qualification remains required before release. Optimizations now target L2 and set;
hot reads are frozen apart from the explicitly requested simplification and
verification work.

Eight-worker ns/op represents aggregate elapsed / completed operations, not
one caller's latency. Eight-thread measurements on a two-core hosted runner
cannot demonstrate eight-core scaling. Qualification requires at least eight
physical cores for the sixfold scaling criterion.

## Repository reproduction with corrected warmup

Three process triplets per API on macOS 26.6.2 arm64, M4 Pro, 12 available
physical/logical cores; Rust 1.88.0, .NET SDK 10.0.300/runtime 10.0.8,
released FC 2.9.0. All recorded warmups settled. This initial diagnostic uses
the f277 runtime with the same two unrelated owner edits and the corrected
fixture. Runtime/fixture identities match between APIs. It predates the
ready-path simplification and real-Loom instrumentation and does not qualify
those changes or the final 0.4 API. Final qualification uses seven pairs.

| Operation | Amalgam ns/op | FC default PGO ns/op | FC TC=0 ns/op | Amalgam / FC default | Result |
|---|---:|---:|---:|---:|---|
| Async L1 read, one worker | 39.214 | 200.613 | 304.923 | 0.195 | Pass in this diagnostic |
| Async L1 get_or_set hit, one worker | 47.926 | 225.737 | 339.434 | 0.212 | Pass in this diagnostic |
| Same key, eight workers, read | 5.347 | 71.260 | 77.850 | 0.075 | Pass in this diagnostic |
| Distinct keys, eight workers, read | 5.228 | 36.918 | 71.729 | 0.142 | Pass in this diagnostic |
| Same key, eight workers, get_or_set | 6.813 | 72.552 | 79.649 | 0.094 | Pass in this diagnostic |
| Distinct keys, eight workers, get_or_set | 6.199 | 52.530 | 64.616 | 0.118 | Pass in this diagnostic |
| L1 replacement | 91.567 | 108.605 | 142.947 | 0.843 | **Fail: <=0.75** |
| Cold factory | 973.577 | 1671.965 | 1888.517 | 0.582 | Pass in this diagnostic |
| L2 plus JSON, read | 1325.387 | 1166.768 | 1824.795 | 1.136 | **Fail: <=1.00** |
| L2 plus JSON, get_or_set | 1681.109 | 1274.437 | 2025.353 | 1.319 | **Fail: <=1.00** |

Both APIs fail exactly the set and L2 budgets. Warm hits and replacement
allocate zero; cold allocates 5.009 per operation; L2 allocates 14.
Distinct-key scaling is 7.335x for read and 7.639x for get_or_set.
These throughput results support retaining the hot path and concentrating
new speed work on L2 and set. They do not establish a release-ready result.

read L2 ranges: Amalgam 1314.579-1338.438 ns; FC default 1142.919-1176.967; FC TC=0 1821.077-1832.342.

get-or-set L2 ranges: Amalgam 1659.685-1697.982 ns; FC default 1268.726-1283.017; FC TC=0 2000.192-2037.038.


## Release gate

The previous exact-source Linux CI completed all 14 functional jobs successfully
but failed its performance job. Its TC=0 comparison is archived, not substituted
for the corrected default-PGO gate. No main push/merge or 0.4.0 publication is
allowed with a red mandatory gate. Final API, paired FR/RS contracts, packaged
consumers, MSRV, live Redis/Valkey and the final source need qualification.

## Reproduce

```sh
python3 benches/run-scaling.py --api read --gate all --output /tmp/amalgam-read
python3 benches/run-scaling.py --api get-or-set --gate all --output /tmp/amalgam-get
```

Seven pairs are the default. Use `--pairs 3` for an initial diagnostic, not final
release qualification. Keep generated files outside the checkout and reuse one
Cargo target. Reports include both FC columns, source/binary hashes, identities,
topology, ranges, operation counts, allocations and full warmup evidence.
