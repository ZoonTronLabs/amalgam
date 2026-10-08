# 0.4 release boundary and remaining work

Version 0.4.0 is published on crates.io. Its delivery includes the complete
eight-operation fluent API and migration, honest M4/Linux performance tables,
FR-01…22 / RS-1…6 status matrix, package cleanup and required validation.
See the [release results](../README.md#040-implementation-and-measured-results),
[migration](MIGRATION_0_4.md), [performance](PERFORMANCE.md) and [parity](PARITY.md).

The release commit passed all 20 CI jobs: formatting, stable/MSRV 1.88 Clippy,
default/all-feature tests, live Redis, docs, package consumers, safety/security
and the regression guard against the actual published 0.3.1 package.

FC comparisons remain informational and retain failed warmups and budgets.
Custom L2 marker qualification and complete paired option coverage remain Gaps.
Further L2/set optimization, Linux FC hot-hit budgets and scaling qualification
are deferred to 0.4.x. The completed release does not close those criteria.
