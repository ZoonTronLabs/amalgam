# Audit repair and validation

The 0.3 source repair addresses the complete October 2026 Amalgam audit. It replaces earlier unsupported claims of inherent clone isolation and universal FusionCache equivalence with explicit contracts, typed failures and executable checks. The original audit observations remain historical evidence; the current [contract](PARITY.md) describes the repaired behavior.

## What the tests establish

| Test area | Evidence |
|---|---|
| Original behavior | Stampede coordination, freshness/fail-safe, soft/hard timeout, eager refresh, adaptive/conditional origin and lazy tags |
| Concurrency regressions | Independent colliding keys, canceled origin ownership, finite cold waits, queued stale fallback, eager L2/cluster coordination and source snapshot ordering |
| Distributed regressions | Durable cold-node markers, replay supersession, data-before-publication, passive fences, cold expire, continuity reconciliation, namespace isolation and separate L2 lifetime |
| Foundation contracts | Fallible copy strategies, weighted/priority retention, source-preserving errors, registry atomicity, plugin sessions and bounded diagnostics |
| Protocol tests | Legacy/new codec round trips, malformed input rejection, typed data/control messages, monotone markers, owned lease capabilities and exact replay identities |
| Independent review regressions | 54 public checks: overlapping/equal-stamp hydration, preferred stale-source preservation, continuity gaps during copying/storage insertion, marker deadlines, circuit admission, tag/clear skips, stale attribution, attachment/callback/destructor drainage, uncertain-token cleanup and reentrant recovery extensions |
| Clock boundaries | Long representable durations, signed full-range arithmetic, saturating manual advance and pre-epoch timestamps |
| Resource and seeded stress | Repeated cache teardown, parked explicit cancellation, bounded malformed inputs, varied retention admission and bounded marker compaction |
| Live Redis faults | Actual subscription acknowledgement, targeted connection kills/reconnect, continuity epochs, malformed/overflow reconciliation, socket drainage, granted-lease cancellation and fenced stale-owner rejection |

The checks exercise public outcomes and controlled providers. Source-text searches are guardrails rather than behavioral proof. The stress cases are bounded deterministic experiments; they do not constitute exhaustive formal verification or an extended production soak.

## Delivery gates

CI retains the required aggregate `ci` status and verifies:

- Formatting, strict Clippy and rustdoc.
- Native Linux, macOS and Windows on Rust 1.88 and stable, with default and all features.
- Each optional integration independently, including its minimum-compiler configuration.
- Required live Redis tests. An absent Redis URL may intentionally skip local optional tests; the mandatory live job fails on absent, malformed or unavailable configuration.
- A real packaged `.crate`, extracted and consumed from fresh and historic downstream locks. The package excludes private agent/owner instructions and checks its relative documentation links.
- RustSec dependency audits without ignored advisories or warnings. A clean library lock alone does not establish the safety of every downstream application's resolved lock.

Use `cargo test --all-features`, `cargo test --no-default-features`, `cargo clippy --all-targets --all-features -- -D warnings`, `cargo fmt --all -- --check` and `cargo doc --all-features --no-deps` for local gates. Configure `AMALGAM_REDIS_URL` and `AMALGAM_REQUIRE_REDIS=1` for a required live run. CI additionally runs feature and downstream-package matrices. The initial delivery CI exercised Rust 1.99 and Windows and exposed compatibility diagnostics and two scheduling/clock fixture assumptions. Scoped compiler compatibility retains Rust 1.88, fixed-size decoding uses the supported safe slice API, Windows checks a representable 100 ns time boundary, and the native recovery test parks its write before inspecting the pending ticket. No expected commit or cleanup assertion is removed.

## Reference and performance limits

Behavioral comparisons distinguish inspected FusionCache source from the actually executed NuGet 2.9 binary. Released-reference marker equality and pinned-capacity observations are covered in [PARITY](PARITY.md). An observed already-started recovery ordering limitation is shared with FusionCache; Amalgam's stronger local commit ordering is deliberate.

Performance experiments compile both implementations first, alternate sequential runs on one machine, warm hot entries and retain per-process ranges. Shared-reference payloads and owned payload copies are separate workloads. Independent 128-key batches verify 128 actual origins; same-key batches verify one. Codec timing records payload size and encoded size separately from cache timing. These experiments establish behavior for the measured configuration, not equal speed for every application or a universal runtime ranking.

The final reviewed-source measurement uses Rust 1.88 release (downstream Tokio 1.53.1) and the actual .NET 10/FusionCache 2.9 binary on one macOS ARM machine. Each cell is the median of three process medians, followed by the observed process range:

| Workload | Amalgam | FusionCache 2.9 |
|---|---:|---:|
| Warm `get_or_set`, u64 | 315 [314–316] ns | 243 [241–248] ns |
| Warm read, u64 | 280 [280–281] ns | 214 [212–220] ns |
| Warm read, owned 64 KiB copy | 990 [982–997] ns | 1978 [1971–2001] ns |
| Warm read, shared 64 KiB reference | 280 [280–281] ns | 219 [217–221] ns |
| Cold immediate origin, u64 | 5712 [5643–5747] ns | 1961 [1803–2220] ns |
| 128 same-key callers, 10 ms origin | 14.30 [13.10–14.32] ms | 11.36 [11.31–11.38] ms |
| 128 distinct keys, 10 ms origins | 14.70 [13.50–14.74] ms | 11.27 [11.07–11.29] ms |

The ready-path repair reduced measured allocation calls from about 19.1 to 1.1 per warm lookup while preserving counted shutdown ownership through synchronous callbacks and input destruction. These samples still show higher overhead for small/shared warm values and cold operations. They do not support a claim of universal equal performance. The pre-review cold allocation profile recorded about 66.5 allocations and 34.4 kB allocated per cold operation on this macOS/downstream runtime; this is allocated volume, not retained memory. Watch notifications, owned scopes, memory insertion and large async frames account for material costs. Future layout/channel changes need their own measured prototype and cancellation/lifecycle checks. Exact fixtures, hashes, raw rounds and historical unsuccessful checkpoints accompany the audit evidence; performance results must be associated with their measured source snapshot.

## Integration boundaries

- A custom byte store does not automatically provide durable atomic markers or fenced commits.
- Renewal cannot guarantee lease safety during arbitrary partitions without a backend ownership check.
- Local commit lanes cannot order independent external writers globally.
- Distributed timestamp comparisons require cross-node clock assumptions.
- A retained cache handle intentionally retains that cache; the last-handle teardown test retains only independent diagnostic handles.
- Legacy adapters retain their signatures and therefore cannot return every newly typed failure.
- New framed snapshots require a coordinated fresh namespace for older running readers.

The package's source version and passing source CI do not themselves publish a registry release or deploy an application's production backend. See the [migration guide](PARITY.md#migrating-from-02) before upgrading consumers.
