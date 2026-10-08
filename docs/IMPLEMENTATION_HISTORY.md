# Implementation checkpoints

Development history is excluded from the published package. The preparation
branch preserves the commits and a local snapshot of the original dirty tree.

- Published baseline: v0.3.1, available from crates.io.
- c921d31: fallible configured construction; honest Linux FC/PGO failures retained.
- 8f709c0: selected value copy used directly during L2 hydration.
- Saved uncommitted work: immutable owned L2 bytes, retention/copy contracts,
  ordinary marker prototype, configuration advice and focused diagnostic records.
- 0.4 preparation: one fluent read facade, Option values, typed outcomes,
  eight-operation migration, explicit parity gaps, lean package and separate
  published-baseline regression / informational FC jobs.

See [parity](PARITY.md), [performance](PERFORMANCE.md) and
[migration](MIGRATION_0_4.md) for the maintained release-facing summary.
Exploratory timing and rejected optimization journals are archived outside the
repository; this file does not claim their provisional checkpoints were releases.
