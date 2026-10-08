# Performance measurements for Amalgam

## Unreleased — inline L2 reads and pipelined markers

Measured source: commit `6dbb8213cd7a7f537d21abaa0e4048d7b37b19dd`. It adds
inline L2 completion over immediate providers, clear markers with the value,
canonical JSON fast paths, fewer clock samples and cross-crate inlining (see the
[changelog](../CHANGELOG.md)). Policy, fixtures and harness are unchanged:
FusionCache **2.9.0** with normal tiering/Dynamic PGO, settled warmups, three
counterbalanced process pairs, Rust 1.88. Ratios are **Amalgam / FC time**;
lower is faster.

### Linux CI — AMD EPYC 9V74, two physical cores

GitHub-hosted runner, Linux 6.17 / glibc 2.39, .NET 10.0.12 (SDK 10.0.401),
[CI run 37793789026](https://github.com/ZoonTronLabs/amalgam/actions/runs/37793789026)
(`scaling-report` artifact).

| Workload / 1 worker | read API: Amalgam / FC ns | ratio | get_or_set API: Amalgam / FC ns | ratio |
|---|---:|---:|---:|---:|
| Warm L1 hit, same key | 112.5 / 155.1 | 0.725 | 108.7 / 192.6 | 0.564 |
| Warm L1 hit, distinct keys | 111.9 / 155.1 | 0.721 | 108.4 / 188.8 | 0.574 |
| Native hit | 76.9 / 110.7 | 0.695 | 89.7 / 143.8 | 0.624 |
| Replacement set | 187.6 / 219.7 | 0.854 | 191.8 / 221.8 | 0.865 |
| L2 JSON | 715.3 / 1175.5 | 0.609 | 945.8 / 1388.3 | 0.681 |
| Cold factory (diagnostic) | 2401.5 / 3429.9 | 0.700 | 2423.7 / 3677.9 | 0.659 |

Warm hits and sets allocate zero times; L2 operations allocate three times
(eight before). Every row is faster than FusionCache. The stricter project
budgets still fail on this runner for one-worker hits (≤0.50×) and set
(≤0.75×). FC cold warmups did not settle in several pairs, so cold remains
diagnostic. Two physical cores cannot qualify eight-core scaling.

### M4 Pro — same source, local (superseded: ru-RU FusionCache reference)

macOS 26.6.2, 12 physical cores, .NET 10.0.8 (SDK 10.0.300). `--gate all`
reported no budget failure and every warmup settled for both APIs.

**Superseded.** The FusionCache process inherited a ru-RU user locale. FC 2.9
compares keys culture-sensitively on hits and L2 reads, so those rows ran
through ICU collation and understate FC: an FC hit cost ~200 ns instead of
~84 ns, and an L2 read ~1206 ns instead of ~677 ns. Set and cold rows are
unaffected. The invariant-reference measurement below replaces this table.

| Workload / 1 worker | read API ratio | get_or_set API ratio |
|---|---:|---:|
| Warm L1 hit, same / distinct | 0.197 / 0.197 | 0.190 / 0.188 |
| Native hit | 0.207 | 0.145 |
| Replacement set | 0.676 | 0.721 |
| L2 JSON | 0.349 (414.0 / 1187.7 ns) | 0.386 (518.4 / 1343.0 ns) |
| Cold factory | 0.592 | 0.570 |

Distinct-key scaling reached 7.24× (read) and 6.68× (get_or_set) against the
6× target.

### M4 Pro — invariant FusionCache reference, local

Branch `perf/general-hit-path` (commit `166a4a9`), macOS 26.6.2, 12 physical
cores, Rust 1.95.0 (local stable; CI uses 1.88), .NET 10.0.8, FC 2.9.0 with
`DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1` and the verified culture recorded in
the report. Three counterbalanced pairs per API, `--gate all`.

| Workload / 1 worker | read: Amalgam / FC ns | ratio | get_or_set: Amalgam / FC ns | ratio |
|---|---:|---:|---:|---:|
| Warm L1 hit, same key | 38.0 / 84.0 | 0.453 | 42.1 / 121.3 | 0.347 |
| Warm L1 hit, distinct keys | 37.9 / 79.2 | 0.478 | 41.7 / 119.6 | 0.349 |
| Native hit | 33.4 / 62.5 | 0.534 | 36.5 / 91.3 | 0.399 |
| Replacement set | 72.8 / 114.1 | 0.638 | 72.7 / 111.9 | 0.650 |
| L2 JSON | 423.3 / 668.0 | 0.634 | 493.5 / 767.2 | 0.643 |
| Cold factory | 1022.8 / 1583.8 | 0.646 | 1007.2 / 1599.9 | 0.630 |

Eight workers: read 5.65 / 59.73 ns same key (0.095) and 5.73 / 25.37 ns
distinct keys (0.226); get_or_set 6.25 / 51.19 (0.122) and 6.22 / 27.21 (0.228).
Distinct-key scaling 6.60× (read) and 6.71× (get_or_set). The read run fails
the native-hit budget (0.534 > 0.50), and FC eight-worker distinct warmups did
not settle in its pairs 2–3 under background load, so that run is diagnostic.
The get_or_set run settled every warmup and met every budget.

### Linux CI — invariant FusionCache reference

[CI run 37821158960](https://github.com/ZoonTronLabs/amalgam/actions/runs/37821158960)
on the same branch, GitHub-hosted `ubuntu-latest` with two physical cores
(CPU model in the `scaling-report` artifact), Rust 1.88.0.

| Workload / 1 worker | read: Amalgam / FC ns | ratio | get_or_set: Amalgam / FC ns | ratio |
|---|---:|---:|---:|---:|
| Warm L1 hit, same key | 130.3 / 200.5 | 0.650 | 129.2 / 248.3 | 0.520 |
| Native hit | 93.6 / 153.9 | 0.608 | 106.3 / 185.6 | 0.573 |
| Replacement set | 228.4 / 289.5 | 0.789 | 230.8 / 288.6 | 0.800 |
| L2 JSON | 936.6 / 1602.5 | 0.584 | 1154.2 / 1916.1 | 0.602 |
| Cold factory (diagnostic) | 2818.8 / 4383.1 | 0.643 | 2760.7 / 4581.2 | 0.603 |

The Linux ratios match the earlier hosted runs, so `LANG=C.UTF-8` CI was not
affected by the locale issue. One-worker hit (≤0.50×) and set (≤0.75×) budgets
still fail on Linux; FC cold warmups did not settle, as before.

### Same-runner improvement over 0.4.1

[CI run 37789637748](https://github.com/ZoonTronLabs/amalgam/actions/runs/37789637748)
ran the frozen before/after diagnostic on one AMD EPYC 7763 runner, comparing
main `66fe979` with intermediate commit `d1824fd` of this work (before lazy
events, cross-crate inlining and ahash providers):

| Workload | 0.4.1 ns | `d1824fd` ns | ratio | allocations |
|---|---:|---:|---:|---:|
| L2 JSON read | 2338.3 | 1012.2 | 0.433 | 8 → 3 |
| L2 JSON get_or_set | 2897.5 | 1245.2 | 0.430 | 8 → 3 |

Warm L1 hits and sets were unchanged within ±2% at that commit; the same run
compared with FC at 0.625× (L2 read) and 0.646× (L2 get_or_set).

### Redis cold reads

A local Valkey 9.1.2 measured 3000 cold L2 reads with the default
`DurableRequired` policy: 385.8 µs per read before (GET, then HMGET) and
199.9 µs after (one pipelined round trip, matching `OptionsControlled`).

## 0.4.0 measurements (history)

Amalgam **0.4.0** is published on crates.io (8 October 2026). The regression
baseline is the actual published **0.3.1** package. The M4 FC table
measures the completed read facade before the final exhaustive-state review.
The final handler review preserves the operation contract but changes the source
fingerprint. The Linux tables measure the final Rust source, with reports from
the corresponding CI jobs linked below.
Source/binary fingerprints accompany every report. Eight-worker ns/op is
aggregate elapsed time divided by completed operations, not caller latency.

## Honest reference and measurement policy

The primary reference is released FusionCache **2.9.0**, running with normal
.NET tiered compilation and Dynamic PGO after a settled warmup. Inherited JIT
options are removed. Both runtimes warm each scenario for at least three seconds;
the last five operation-time windows must have max/min <=1.10. An unsettled run
is reported as a qualification failure. Process order rotates across at least
three pairs. TC=0 is a separate diagnostic column and never the release reference.
See [the reproducible harness](../benches/README.md).

Old comparisons that disabled tiering are withdrawn as release evidence.
Instrumented allocation/CPU timings are excluded from the timing tables.
Different operating systems and different source checkpoints are kept separate.

## M4 Pro read-facade checkpoint — 8 October

macOS 26.6.2, 12 physical/logical CPUs; Rust 1.88.0; FC 2.9.0 / .NET 10.0.8
(SDK 10.0.300). Three counterbalanced process pairs per API, normal tiering/PGO
as the primary reference and TC=0 only as a diagnostic. The source-set SHA-256 is
`ff9e172a4e60c7fbbcac77466b48c7e332dcff8a972f407fa3a82e9b219081a2`. This source checkpoint also passed the local published-0.3.1 guard; the final
source has its own independent guard and CI artifacts.

| API / workload | Amalgam ns/op | FC PGO ns/op | FC TC=0 diagnostic ns/op | Amalgam / FC PGO | Rust allocations/op |
|---|---:|---:|---:|---:|---:|
| read / same | 42.0 | 204.6 | 304.0 | 0.205 | 0.000 |
| read / distinct / 8 workers | 8.0 | 39.5 | 65.0 | 0.204 | 0.000 |
| read / sync | 29.5 | 169.0 | 261.1 | 0.174 | 0.000 |
| read / set | 91.6 | 120.9 | 144.7 | 0.758 | 0.000 |
| read / cold | 1065.9 | 1748.7 | 1863.5 | 0.610 | 5.009 |
| read / l2_json | 1042.8 | 1181.1 | 1886.5 | 0.883 | 8.000 |
| get-or-set / same | 43.0 | 229.7 | 338.1 | 0.187 | 0.000 |
| get-or-set / distinct / 8 workers | 6.9 | 51.4 | 76.7 | 0.135 | 0.000 |
| get-or-set / sync | 45.1 | 194.8 | 283.8 | 0.232 | 0.000 |
| get-or-set / set | 92.2 | 117.1 | 145.6 | 0.788 | 0.000 |
| get-or-set / cold | 1133.7 | 1743.7 | 1974.9 | 0.650 | 5.009 |
| get-or-set / l2_json | 1379.7 | 1275.9 | 2119.5 | 1.081 | 8.000 |

Warm L1 reads and replacement writes allocate zero times. L2 costs eight
allocations; cold factories about 5.009 per operation. Timing ranges are retained
in artifacts, including wide eight-worker and FC set ranges; no small-change or
neutral-hot-path optimization claim follows from these medians.

- read: distinct-key scaling 5.07x; unsettled warmups 0; recorded budget failures: ('set', 1): 0.758 x FC exceeds 0.75; distinct scaling: 5.07 below 6.00 for 12 available physical cores.
- get-or-set: distinct-key scaling 6.09x; unsettled warmups 0; recorded budget failures: ('set', 1): 0.788 x FC exceeds 0.75; ('l2_json', 1): 1.081 x FC exceeds 1.00.

Set's <=0.75x and L2 get_or_set's <=1.00x budgets remain open. Read's sixfold
eight-core scaling target also remains unqualified on this M4 run. This scope
publishes the failed criteria; closing them is deferred to 0.4.x.

An earlier native get_or_set development checkpoint was 45.4 ns versus 39.4 ns
(+15.2%, disjoint ranges). That development regression remains disclosed; it is
separate from the actual published 0.3.1 comparison and these checkpoint rows.

## Linux — final Rust source, 8 October

GitHub-hosted AMD EPYC 7763 runner: two available physical cores / four logical
CPUs, Linux 6.17 / glibc 2.39. Rust 1.88.0; FC 2.9.0 / .NET 10.0.12
(SDK 10.0.401). Three counterbalanced pairs per API, normal tiering/PGO; TC=0
is diagnostic. Source-set SHA-256:
`2319b32da1b24da9bfa128b4743d339226ead45b1695ee5454829e5eed7c5ab1`.
Reports are retained in `scaling-report` from
[the measured CI run](https://github.com/ZoonTronLabs/amalgam/actions/runs/37735287338).
The performance jobs completed; that run's feature matrix later hit its timeout.
This machine cannot qualify sixfold eight-core scaling.

| API / workload / 1 worker | Amalgam ns/op | FC PGO ns/op | FC TC=0 diagnostic ns/op | Amalgam / FC PGO | Rust allocations/op |
|---|---:|---:|---:|---:|---:|
| read / same | 130.6 | 202.1 | 338.4 | 0.646 | 0.000 |
| read / distinct | 127.8 | 199.6 | 338.9 | 0.640 | 0.000 |
| read / sync | 96.5 | 155.5 | 232.9 | 0.621 | 0.000 |
| read / set | 251.2 | 285.7 | 355.9 | 0.879 | 0.000 |
| read / cold (diagnostic only) | 2701.3 | 3862.9 | 4578.4 | 0.699 | 5.009 |
| read / l2_json | 2250.0 | 1631.7 | 2721.8 | 1.379 | 8.000 |
| get-or-set / same | 139.1 | 250.6 | 380.8 | 0.555 | 0.000 |
| get-or-set / distinct | 139.4 | 247.9 | 378.7 | 0.562 | 0.000 |
| get-or-set / sync | 124.5 | 183.3 | 281.4 | 0.679 | 0.000 |
| get-or-set / set | 247.7 | 283.2 | 358.7 | 0.875 | 0.000 |
| get-or-set / cold (diagnostic only) | 2708.0 | 4135.6 | 4658.7 | 0.655 | 5.009 |
| get-or-set / l2_json | 2983.3 | 1909.4 | 2998.9 | 1.562 | 8.000 |

Both APIs miss the <=0.50x one-worker same/distinct/native hit budgets,
<=0.75x set budget and <=1.00x L2 budget. L2 read is **1.379x** FC and L2
get_or_set **1.562x**. Cold results are **diagnostic only**: default FC failed
to settle in read pairs 2–3 and all three get_or_set pairs; TC=0 cold warmup
also failed in all read pairs and get_or_set pair 3. Other warmups settled.
These failures remain recorded by the informational job and are deferred to 0.4.x.

The older c921d31 run recorded L2 read 2.387x and get_or_set 1.666x; it remains
[a historical checkpoint](https://github.com/ZoonTronLabs/amalgam/actions/runs/37679769506),
not the source of the final-source rows above.

A repeat of the same final Rust source in
[PR CI](https://github.com/ZoonTronLabs/amalgam/actions/runs/37738357021)
measured L2 read **1.475x** FC, L2 get_or_set **1.527x** and set
**0.884–0.897x** across the two API fixtures. One-worker hot budgets still fail
and FC cold warmup remains unsettled. The first run above remains unchanged;
the repeat is retained alongside it rather than replacing its measurements.

## Published 0.3.1 regression guard — final M4 source

Actual registry package `amalgam-cache = 0.3.1`; same Rust 1.88 toolchain,
workload fixture and pinned direct harness dependencies. Three alternating
pairs, all warmups settled, all eight workloads pass the 1.05 noise allowance.
The final source-set SHA-256 is
`2319b32da1b24da9bfa128b4743d339226ead45b1695ee5454829e5eed7c5ab1`. This fixture has its own
counts and instrumentation; its ns/op values are not interchangeable with the
FC fixture. The local report retains this source identity.

| Workload / workers | Published 0.3.1 ns/op | Amalgam 0.4.0 ns/op | 0.4.0 / 0.3.1 |
|---|---:|---:|---:|
| cold / 1 | 5149.1 | 1043.5 | 0.203 |
| hot_get_or_set / 1 | 314.7 | 41.8 | 0.133 |
| hot_get_or_set / 8 | 662.6 | 5.8 | 0.009 |
| hot_read / 1 | 277.5 | 41.4 | 0.149 |
| hot_read / 8 | 784.2 | 5.6 | 0.007 |
| l2_get_or_set / 1 | 2164.8 | 1385.9 | 0.640 |
| l2_read / 1 | 1886.5 | 1026.7 | 0.544 |
| set / 1 | 4117.4 | 91.4 | 0.022 |

## Published 0.3.1 regression guard — final Linux source

The same final Rust source and registry baseline were measured by the mandatory
job in [the measured CI run](https://github.com/ZoonTronLabs/amalgam/actions/runs/37735287338).
Three alternating pairs, all warmups settled, all eight workloads **PASS**
with the 1.05 noise allowance. The `release-regression-report` artifact records
registry provenance, source/binary identities and raw process output.
These numbers use the independent regression fixture, not the FC fixture.

| Workload / workers | Published 0.3.1 ns/op | Amalgam 0.4.0 ns/op | 0.4.0 / 0.3.1 |
|---|---:|---:|---:|
| cold / 1 | 6146.3 | 1778.9 | 0.289 |
| hot_get_or_set / 1 | 548.5 | 87.8 | 0.160 |
| hot_get_or_set / 8 | 275.3 | 27.9 | 0.101 |
| hot_read / 1 | 515.8 | 88.6 | 0.172 |
| hot_read / 8 | 271.2 | 25.1 | 0.093 |
| l2_get_or_set / 1 | 2615.6 | 1759.8 | 0.673 |
| l2_read / 1 | 2250.5 | 1428.8 | 0.635 |
| set / 1 | 4212.0 | 174.1 | 0.041 |

## Gates for 0.4.0

The owner defers closing FC-relative L2, set, scaling and Linux hot-read gaps to 0.4.x.
The FC comparison is **informational**: it retains failed budgets, warmup
verdicts and raw evidence in seven-day CI artifacts. It does not block the
aggregate required `ci` gate. This is a scope decision, not a claim that the
budgets now pass.

The blocking performance job runs matched public workloads against the actual
crates.io **amalgam-cache 0.3.1** package. Three counterbalanced pairs cover L1
read/get_or_set with one and eight workers, replacement set, cold factory and
L2 read/get_or_set. It checks source/binary identities, complete settled warmup,
identical operation counts and medians. A ratio above **1.05** fails; 5% is the
explicit measurement-noise allowance. Missing or unstable evidence also fails.
This guard protects the published baseline and does not replace FC parity.
Native-only APIs did not exist in published 0.3.1 and have no matching baseline;
their FC measurements and existing allocation/lifecycle contracts remain visible.

Raw JSON and exploratory journals are kept out of the repository and crate.
The source worktree was archived locally before cleanup. New reports live
outside the checkout and are uploaded by CI.
