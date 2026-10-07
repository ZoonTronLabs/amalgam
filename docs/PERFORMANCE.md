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

## Current repository diagnostic with corrected warmup

Three process triplets per API on macOS 26.6.2 arm64, M4 Pro, 12 available
physical/logical cores; Rust 1.88.0, .NET SDK 10.0.300/runtime 10.0.8,
released FC 2.9.0. This measures `a327912` plus two unrelated owner removals
of `#[inline]` in `src/cache.rs`. It includes the two ready plans and actual
ReaderSlots instrumentation. All 306 warmup records per API settled.
Runtime/fixture identities, ranges, allocations and gate failures are in
[the extracted diagnostic report](benchmarks/2026-10-07-honest-fc-two-ready.json).
Full per-window reports remain the local benchmark artifacts; the extracted
report contains their verdict summaries and source/binary hashes.
These are diagnostic results, not final-source or API 0.4 qualification.
Final release qualification uses seven process triplets.

| API / operation | Workers | Amalgam ns/op | FC default PGO ns/op | FC TC=0 ns/op | Amalgam / FC default | Budget result |
|---|---:|---:|---:|---:|---:|---|
| read: same key | 1 | 40.435 | 205.495 | 325.288 | 0.197 | Pass in diagnostic |
| read: distinct keys | 1 | 43.029 | 204.356 | 322.578 | 0.211 | Pass in diagnostic |
| read: same key | 2 | 22.977 | 110.830 | 208.408 | 0.207 | Pass in diagnostic |
| read: distinct keys | 2 | 21.681 | 105.414 | 192.730 | 0.206 | Pass in diagnostic |
| read: same key | 4 | 10.784 | 78.737 | 115.022 | 0.137 | Pass in diagnostic |
| read: distinct keys | 4 | 11.314 | 61.964 | 104.711 | 0.183 | Pass in diagnostic |
| read: same key | 8 | 8.242 | 76.981 | 91.725 | 0.107 | Pass in diagnostic |
| read: distinct keys | 8 | 7.967 | 41.761 | 70.528 | 0.191 | Pass in diagnostic |
| read: sync hit | 1 | 31.877 | 182.581 | 277.608 | 0.175 | Pass in diagnostic |
| read: L1 replacement | 1 | 95.202 | 112.871 | 150.951 | 0.843 | **Fail** |
| read: cold factory | 1 | 1136.762 | 2021.164 | 2241.473 | 0.562 | Pass in diagnostic |
| read: L2 plus JSON | 1 | 1415.105 | 1218.116 | 1921.003 | 1.162 | **Fail** |
| get-or-set: same key | 1 | 47.691 | 245.797 | 358.984 | 0.194 | Pass in diagnostic |
| get-or-set: distinct keys | 1 | 48.565 | 251.305 | 363.122 | 0.193 | Pass in diagnostic |
| get-or-set: same key | 2 | 25.077 | 142.338 | 229.694 | 0.176 | Pass in diagnostic |
| get-or-set: distinct keys | 2 | 24.359 | 129.381 | 187.692 | 0.188 | Pass in diagnostic |
| get-or-set: same key | 4 | 13.663 | 92.231 | 133.917 | 0.148 | Pass in diagnostic |
| get-or-set: distinct keys | 4 | 13.365 | 89.583 | 113.053 | 0.149 | Pass in diagnostic |
| get-or-set: same key | 8 | 9.730 | 91.584 | 90.649 | 0.106 | Pass in diagnostic |
| get-or-set: distinct keys | 8 | 9.879 | 67.025 | 85.042 | 0.147 | Pass in diagnostic |
| get-or-set: sync hit | 1 | 41.456 | 216.430 | 300.002 | 0.192 | Pass in diagnostic |
| get-or-set: L1 replacement | 1 | 95.333 | 118.035 | 152.124 | 0.808 | **Fail** |
| get-or-set: cold factory | 1 | 1152.338 | 1989.289 | 2346.730 | 0.579 | Pass in diagnostic |
| get-or-set: L2 plus JSON | 1 | 1773.824 | 1345.739 | 2132.849 | 1.318 | **Fail** |

**Both APIs fail the set and L2 budgets and the sixfold scaling gate.**
Distinct-key scaling is 5.401x for read and 4.916x for get_or_set.
The previous, pre-simplification diagnostic was 7.335x / 7.639x; this change
cannot be dismissed as noise or assigned to a runtime cause without a
controlled comparison. The scaling threshold is unchanged. Hot-read speed
relative to FC and zero allocations still pass; their runtime design is frozen.
New speed changes target L2 and set.

Warm hits and replacement allocate zero; cold allocates 5.009 per operation;
L2 allocates 14. The two API fixtures produce separate set/cold repetitions;
both repetitions are shown rather than selecting the faster one.

read L2 ranges: Amalgam 1391.016-1434.773 ns; FC default 1217.290-1270.044; FC TC=0 1911.018-2118.354.

get-or-set L2 ranges: Amalgam 1766.992-1852.467 ns; FC default 1310.891-1347.143; FC TC=0 2130.958-2173.934.

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


## ReaderSlots verification scope

Loom instruments the actual `src/reader_slots.rs` admission, guard and wakeup
algorithm through `cfg(loom)`, including full-lifetime tracked value access.
Deliberately bypassing the writer gate produces a tracked value-access race;
restored source passes its bounded models. Native TSan also runs the actual
parking and collision stress. These checks supplement contracts rather than
proving every possible execution.

Miri checks actual native guard borrowing and overlapping reader reservations.
The contended parking case explicitly remains ignored under Miri because the
released `parking_lot_core` 0.9.12 uses a C-variadic futex argument rejected by
current Miri. The cause and fix are documented by
[upstream parking_lot](https://github.com/Amanieu/parking_lot/pull/539).
No Miri UB/alias/race check is disabled, and the runtime algorithm/dependency is
not replaced for the check. Native TSan and Loom retain parking coverage.
Full contended Miri coverage remains open until a compatible released
dependency includes the upstream fix.
