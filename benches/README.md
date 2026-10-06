# Performance gates

Run `python3 benches/run-scaling.py --gate hot` from a checkout with Rust and
.NET 10 installed. The default is seven alternating process pairs; use
`--output /absolute/path/outside/the/checkout` to keep the CSVs and JSON report.
All Cargo outputs share `CARGO_TARGET_DIR`; generated .NET files stay in the
report directory. The reference is the exact locked FusionCache 2.9.0 package.

The fixtures use a scalar value, a one-hour lifetime and default memory-only
storage. They verify actual returned values and compare public async reads for
the same key and distinct keys at 1/2/4/8 threads. They also measure a native
synchronous hit, replacement `set`, and a new-key immediately ready factory.
Setup, keys, thread startup and warmup are outside timed loops. Parallel times
are aggregate elapsed time divided by all completed operations, rather than
per-request latency. Rust allocations use a thread-local allocator counter;
.NET reports allocated bytes, which are a different unit.

Release Rust is compared with fully optimized .NET JIT code. Tiered compilation
is disabled in reference processes to keep tier transitions outside the timed
work. Seven medians and ranges are reported; small changes within two percent
need more evidence. Source and binary fingerprints make the report reproducible.

The `hot` gate requires zero Rust allocations on warmed hits, async/sync
single-thread and same-key eight-thread costs at most half of FusionCache, and
distinct-key eight-thread cost at most three quarters. Eight-core scaling must
be at least six times. An oversubscribed hosted runner checks a proportional
scaling floor and records that it cannot verify eight-core scaling; it still
runs all thread counts and the same relative FusionCache budgets.

`--gate cold` additionally requires the cold factory at most three quarters of
FusionCache with at most six allocations. `--gate all` also requires replacement
`set` at most three quarters of FusionCache with at most three allocations.
`--gate report` records all measurements without enforcing speed ratios. CI
currently enforces the hot milestone; later milestones promote the gate.

The report also includes `ready-costs.csv`, a diagnostic breakdown of clock,
hash, counter and framework costs. Constant clocks isolate framework and
physical-expiry overhead. Default live-clock reads and explicitly injected system
UTC reads are reported separately. The performance gate uses the actual cache's
default live clock and the same public APIs; constant clocks are diagnostic only.
