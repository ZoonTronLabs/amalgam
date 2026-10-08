# 0.4.0 delivery boundary

Complete only release preparation: the eight-operation fluent API and migration,
honest performance tables, FR-01…22 / RS-1…6 status matrix, package cleanup and
required checks. Custom L2 marker qualification remains a Gap. Further L2/set
optimization and closure of Linux FC budgets are deferred to 0.4.x.

Required gates: formatting, stable/MSRV 1.88 clippy, default/all-feature tests,
live Redis, docs, package consumers, safety/security and no regression against
published 0.3.1. FC-relative comparisons are informational and retain failed
budgets. See [performance](PERFORMANCE.md) and [parity](PARITY.md).

Each delivery step requires an explicit owner yes: merge the existing PR,
version/CHANGELOG, tag v0.4.0, GitHub release, crates.io publish. A fresh crates.io
token is supplied through the owner's terminal; chat tokens are never reused.
