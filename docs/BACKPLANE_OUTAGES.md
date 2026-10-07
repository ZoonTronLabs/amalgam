# Backplane outage policies: 0.4 source

The published 0.3.1 package has conservative defaults and no `BackplaneBestEffort`
variant. The following describes developing 0.4 source, before registry release.

## Defaults and strict profile

| Configuration | Ordinary builder | `strict()` |
|---|---|---|
| Built-in L1 only | `LocalOnly` | `LocalOnly` |
| L2 without backplane | `Expiration`: retain L1 until its deadline | `Periodic(1s)` |
| Backplane with acknowledged health | `BackplaneBestEffort` | `BackplaneContinuity` |
| Healthless backplane | `BackplaneBestEffort` | `Periodic(1s)` |
| Distributed ownership | `Cooperative` | `Fenced` |
| Initial subscription wait | Disabled | Enabled |

Explicit setters refine the selected profile. `BackplaneBestEffort` requires a
backplane, and `BackplaneContinuity` requires its acknowledged health stream.
`Periodic` requires a positive representable interval. `Fenced` rejects a locker
without a declared lifetime/caller-owned tokens or L2 without atomic fenced writes
at construction, before external effects. Native memory and Redis providers
implement that capability; a custom declaration is an implementation contract.

```rust
use amalgam::Cache;

// Ordinary outage availability. Attach Redis providers to this builder.
let ordinary = Cache::<String>::builder();

// Conservative continuity and stale-owner commit rejection.
let strict = Cache::<String>::builder().strict();
```

## Behavior during an outage

A fresh L1 hit contacts neither L2 nor the locker. Ordinary backplane gaps and
reconnects preserve L1; stale values remain eligible only within configured
fail-safe retention. A suppressed cooperative locker failure permits a cold
factory, while `with_rethrow_distributed_locker_exceptions(true)` propagates it.
Local single flight remains active; different nodes may compute concurrently.
An explicitly fenced miss still requires ownership even if rethrow is disabled.

Missed peer remove/tag/clear notifications can leave old data visible until its
normal deadline. Reconnection does not recover pub/sub history. Local mutations
and received valid notifications still apply. Malformed or lagged frames do not
cause global L1 cleanup under best-effort policy. Strict policies discard L1 and
revoke crossing hydration/observation authority when continuity is lost.

Physical expiry, explicit cancellation, independent marker lifetimes and actual
provider failures retain their typed outcomes. Disconnect suspends owned recovery;
acknowledged reconnect uses the configured delay, five seconds by default. Hot
reads do not wait for recovery. Cleanup/eager/background failures can still reach
`shutdown()`; foreground suppression does not erase owned-work diagnostics.

## Reproducing the contract

[Rust outage tests](../tests/locker_outage_contract.rs) cover local/hydrated L1,
cooperative suppression/rethrow, explicit fencing, stale versus physical expiry,
cancellation, received/local invalidations and healthless adapters.
[Default tests](../tests/fusion_defaults_contract.rs) cover L2-only retention,
expire removing L2, soft timeout eligibility and early strict validation.
[FC 2.9 availability oracle](../tests/fusioncache/README.md) exercises the
corresponding observable behavior through the released public provider interfaces.
[Strict hydration tests](../tests/review_hydration_continuity.rs) select continuity
explicitly. Simulated outages establish semantics; native Redis timings and
protocol integration are separate checks.
