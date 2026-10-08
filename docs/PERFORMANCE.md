# Performance for the 0.4 preparation

Published package: **0.3.1** (registry checked 8 October 2026). The M4 table
measures the completed read facade before the final exhaustive-state review.
The final handler review preserves the operation contract but changes the source
fingerprint. Linux below is a separate historical checkpoint until exact-source
CI evidence is linked.
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

## Linux checkpoint, c921d31

GitHub-hosted AMD EPYC 7763 runner: two available physical cores / four logical
CPUs. Rust 1.88; FC 2.9.0 / .NET 10.0.12. The full read and get_or_set reports
used seven pairs. This machine cannot qualify sixfold eight-core scaling.

| API / workload | Amalgam ns/op | FC default PGO ns/op | Amalgam / FC |
|---|---:|---:|---:|
| read / same | 127.3 | 200.1 | 0.636 |
| read / sync | 93.9 | 155.3 | 0.604 |
| read / set | 253.2 | 279.0 | 0.907 |
| read / cold | 2655.7 | 3762.6 | 0.706 |
| read / l2_json | 3811.5 | 1596.8 | 2.387 |
| get-or-set / same | 132.0 | 250.9 | 0.526 |
| get-or-set / sync | 132.4 | 182.4 | 0.726 |
| get-or-set / set | 254.2 | 277.2 | 0.917 |
| get-or-set / cold | 2674.3 | 3601.7 | 0.742 |
| get-or-set / l2_json | 3146.6 | 1888.8 | 1.666 |

Linux L2 read is 2.387x FC and L2 get_or_set 1.666x FC. One-worker hot read,
native read and factory-retrieval budgets also fail. Two FC cold warmups in the
get_or_set run did not settle; its cold result is diagnostic evidence only.
See [the original CI run](https://github.com/ZoonTronLabs/amalgam/actions/runs/37679769506).

## Published 0.3.1 regression guard — final M4 source

Actual registry package `amalgam-cache = 0.3.1`; same Rust 1.88 toolchain,
workload fixture and pinned direct harness dependencies. Three alternating
pairs, all warmups settled, all eight workloads pass the 1.05 noise allowance.
The final source-set SHA-256 is
`2319b32da1b24da9bfa128b4743d339226ead45b1695ee5454829e5eed7c5ab1`. This fixture has its own
counts and instrumentation; its ns/op values are not interchangeable with the
FC fixture. Final-source Linux reports are collected by the jobs in
[PR #5](https://github.com/ZoonTronLabs/amalgam/pull/5) and retain these identities.

| Workload / workers | Published 0.3.1 ns/op | Candidate ns/op | Candidate / 0.3.1 |
|---|---:|---:|---:|
| cold / 1 | 5149.1 | 1043.5 | 0.203 |
| hot_get_or_set / 1 | 314.7 | 41.8 | 0.133 |
| hot_get_or_set / 8 | 662.6 | 5.8 | 0.009 |
| hot_read / 1 | 277.5 | 41.4 | 0.149 |
| hot_read / 8 | 784.2 | 5.6 | 0.007 |
| l2_get_or_set / 1 | 2164.8 | 1385.9 | 0.640 |
| l2_read / 1 | 1886.5 | 1026.7 | 0.544 |
| set / 1 | 4117.4 | 91.4 | 0.022 |

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
