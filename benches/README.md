# Reproducible performance checks

Run `python3 benches/run-scaling.py --gate hot` from a checkout with Rust and
.NET 10 installed. The default is seven alternating process pairs; use
`--output /absolute/path/outside/the/checkout` to keep the CSVs and JSON report.
All Cargo outputs share `CARGO_TARGET_DIR`; generated .NET files stay in the
report directory. The reference is the exact locked FusionCache 2.9.0 package.

The fixtures use a scalar value, a one-hour lifetime and default memory-only
storage. They verify actual returned values and compare public async reads for
the same key and distinct keys at 1/2/4/8 threads. `--api read` measures read-only
hits; `--api get-or-set` measures factory retrieval hits and the matching native
operation in both libraries. Each mode applies the same allocation and ratio
budgets, and its name is retained in the report. They also measure a native
synchronous hit, replacement `set`, and a new-key immediately ready factory.
Setup, keys, thread startup and warmup are outside timed loops.
Warm reads, mutations and distributed reads run in separate fresh processes
in both runtimes. The distributed fixture uses an in-memory L2 and the
standard JSON snapshot serializer, with public default options that skip
L1 reads. L1 hydration remains enabled in both libraries. It seeds different
L1/L2 values (11/7), checks every warmup result and validates the measured
checksum. Cache construction, seeding and shutdown are outside measurement.
Run either scaling executable with `--l2 --api read|get-or-set` for this
fixture alone; the factory-retrieval fixture fails if its factory runs.
The mutation process is identical for both selected read APIs: one million
replacement writes and one hundred thousand new-key factories. Raw CSVs identify
each process; scenario sets, operation counts and returned values are checked. Both cold
fixtures keep their preallocated input key collections alive during measurement;
input-key destruction is outside the loop in both runtimes. Parallel times
are aggregate elapsed time divided by all completed operations, rather than
per-request latency. Rust allocations use a thread-local allocator counter;
.NET reports allocated bytes, which are a different unit.

Release Rust is compared with FusionCache 2.9.0 using normal .NET tiered
compilation and Dynamic PGO. Inherited JIT overrides are removed. The .NET
processes run with invariant globalization: FusionCache compares keys
culture-sensitively on hits and L2 reads, and under a non-English user locale
(ICU collation) that alone made an M4 hit ~2.4x slower. Inherited locale
variables are removed; the reference measures its globalization mode, and the
harness rejects anything but invariant and records it in the report. Both runtimes
warm each scenario for at least three seconds and require the last five timing
windows to have max/min <=1.10. Unsettled warmup fails qualification. TC=0 runs
are separate diagnostics. At least three alternating pairs are required; seven
are the default. Reports include medians, ranges and source/binary fingerprints,
including the driver and excluding generated C# files. Small changes within two
percent need more evidence.

The `hot` gate requires zero Rust allocations on warmed hits, async/sync
single-thread and same-key eight-thread costs at most half of FusionCache, and
distinct-key eight-thread cost at most three quarters. Eight-core scaling must
be at least six times. An oversubscribed hosted runner checks a proportional
scaling floor based on known physical cores and records that it cannot verify
eight-core scaling; it still
runs all thread counts and the same relative FusionCache budgets.

`--gate cold` additionally requires the cold factory at most three quarters of
FusionCache with at most six allocations. `--gate all` also requires replacement
`set` at most three quarters of FusionCache with at most three allocations,
and in-memory L2 plus JSON at most the FusionCache cost. L2 allocations are
reported separately; the zero-allocation limit applies to warmed L1 hits.
`--gate report` records all measurements without enforcing speed ratios. CI
records `--gate all` failures for both `read` and `get-or-set` in the informational
FusionCache job. These budgets remain targets for 0.4.x; they do not block the
aggregate CI gate.

The report also includes `ready-costs.csv`, a diagnostic breakdown of clock,
hash, counter and framework costs. Constant clocks isolate framework and
physical-expiry overhead. Default live-clock reads and explicitly injected system
UTC reads are reported separately. The performance gate uses the actual cache's
default live clock and the same public APIs; constant clocks are diagnostic only.

The report records available physical cores separately from logical CPUs.
Eight-core qualification requires at least eight reported physical cores and
sixfold scaling. The hosted-runner floor is three quarters of available physical
cores, capped at six; SMT siblings are not additional cores. Unavailable topology
remains unverified. All relative FusionCache budgets and allocation limits apply
regardless of topology.

Run the scaling executable with `--metadata-costs` to diagnose plain, eager
and tagged entry construction, owned metadata snapshots, logical expiration
and replacement writes. This mode uses 100,000 warmup and three million measured operations
per case and reports elapsed time and allocation counts. It does not substitute
for the paired FusionCache gate. Entry lifetime math
uses an explicit timestamp and zero jitter. Cache replacements use the default
live clock, and raw request tag construction is included in the tagged case.
Each case verifies its source metadata and the final cached value.

The replacement diagnostic separately measures writes before and after an
actual read of the same L1 key. The paired replacement fixture performs its
value-verification read after its timed loop. Preserve that distinction when
reporting a storage optimization that applies only before reader registration.

For a storage or execution change, run
`python3 benches/run-before-after.py --baseline <local-commit> --output /absolute/path/outside/the/checkout`.
This diagnostic requires identical dependency manifests and compiles the
current frozen workloads against both source versions. It verifies identical
harness fingerprints, freezes both executables, restores the current source
before measurement,
and alternates their order for at least three pairs on the same machine. It
reports enabled metadata costs, writes before and after a read, cold factories
and both read scaling APIs. Raw counts, allocations, source fingerprints and
timing ranges accompany the medians; it leaves the FusionCache gates intact.
Use `--rust-target` only for an available cross target and identify emulated
results as such. CI manual runs can select the optional `diagnostic_baseline`
full commit SHA to collect native Linux before/after evidence on one runner.

Allocation ownership can be inspected independently with
`cargo +1.88.0 test --release --locked --bench l2_ownership -- --ignored --nocapture --test-threads=1`.
The two ignored probes warm the same public L2 read and factory-retrieval cases,
then capture allocation stacks for one untimed lookup. The conflicting L1/L2
seeds and no-factory assertions remain. Tracing uses a separate executable and
allocator; it cannot change the timed scaling counter. Counts include platform
runtime bookkeeping and must be compared on the same compiler and platform.

Native CPU sampling is a separate diagnostic after paired measurement:
`python3 benches/run-native-profile.py --reports /absolute/path/to/before-after --output /absolute/path/to/profile`.
On Linux it profiles the exact frozen baseline/candidate binaries, verifies
their paired-report hashes, repeats both L2 APIs, and saves self-time and stack
reports. It uses [perf's DWARF call-graph recording](https://man7.org/linux/man-pages/man1/perf-record.1.html).
Unavailable tools or incomplete profiles are recorded explicitly. Manual CI
diagnostics prepare the Linux tool and upload rendered reports; raw samples
stay on the ephemeral runner. Profile measurements are not performance gates.

## Published-release regression guard

Run `python3 benches/check-release-regression.py --output /absolute/path/outside/the/checkout`.
This blocking check builds an independent consumer of the actual crates.io
`amalgam-cache = 0.3.1` package and a consumer of the current checkout, using
the same pinned Rust 1.88 compiler and shared workload fixture. A small adapter
accounts for the breaking public API change; operations and timed counts match.

Three alternating pairs cover public L1 read/get_or_set at one/eight workers,
replacement set, cold factory and in-memory JSON L2 read/get_or_set. Source,
lockfile and frozen binary hashes accompany the report. Every warmup must settle
under the policy above. A median candidate/baseline ratio above 1.05 fails; 5%
is an explicit noise allowance. Missing or unsettled evidence also fails.
Native-only APIs have no published 0.3.1 equivalent and remain outside this
baseline comparison. The guard neither demonstrates full FC parity nor closes
the known FC budgets in [PERFORMANCE](../docs/PERFORMANCE.md).
