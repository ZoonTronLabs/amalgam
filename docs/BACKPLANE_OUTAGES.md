# Backplane outage policies (unreleased)

Published **amalgam-cache 0.3.1** does not contain `BackplaneBestEffort`.
The source addition below is not a registry release or consumer rollout.

## Explicit reconciliation choice

| Policy | Requirement | Notification gap / reconnect | Cold failed locker |
|---|---|---|---|
| `BackplaneContinuity` | Acknowledged health stream | Fence hydration/observations and discard all L1, including stale | Governed by `LeasePolicy` |
| `Periodic(interval)` | Positive representable interval | Same gap barrier; periodically discard L1 | Governed by `LeasePolicy` |
| `BackplaneBestEffort` | Configured backplane; health stream optional | Retain L1 and observations within normal lifetimes; no periodic discard | Governed by `LeasePolicy` |

Defaults are unchanged: memory-only uses `LocalOnly`, a native health stream
selects `BackplaneContinuity`, and other external configurations select a
one-second `Periodic` policy. A best-effort choice without a backplane fails
construction with `ConfigError::BestEffortReconciliationWithoutBackplane`.

For ordinary FusionCache-style availability, retain the configured Redis
providers and add both independent policies:

```rust
use amalgam::{Cache, EntryOptions, LeasePolicy, ReconciliationPolicy};

let builder = Cache::<String>::builder()
    .reconciliation_policy(ReconciliationPolicy::BackplaneBestEffort)
    .lease_policy(LeasePolicy::CooperativeLegacy)
    .default_options(EntryOptions::default()
        .with_rethrow_distributed_locker_exceptions(false));
```

This combination keeps a fresh L1 value local while Redis is unavailable,
retains stale for configured fail-safe, and permits an ordinary cold factory
without a distributed lease after a suppressed acquisition failure. Per-key
local single flight still applies. Cooperating nodes may compute simultaneously
during the partition; strict stale-owner fencing is not promised in this mode.
A best-effort backplane with `LeasePolicy::Fenced` still rejects failed cold
lease acquisition, even with the rethrow flag disabled. Hot L1 does not need it.

## Retained boundaries

Retaining L1 over a gap deliberately permits a missed peer remove/tag/clear to
leave an old value readable until its normal expiration. Reconnection does not
recover missed pub/sub history. Local mutations and received valid notifications
still apply. Malformed or lagged notifications do not cause a global L1 discard
in this explicit mode; valid later messages remain independently processed.

Freshness and physical retention deadlines do not change. A physically dead
value cannot be served as fail-safe. Explicit caller cancellation remains an
error. Existing independent marker-read lifetimes, budgets, faults and repair
admission still apply; retaining an observation does not make it immortal or
turn unknown durable authority into a known fact.

Disconnect suspends owned recovery; acknowledged reconnect applies the existing
replay delay. Hot reads do not wait for that delay. Local generation ordering,
remaining effect lifetime and normal received invalidations remain authoritative.
Acquisition cleanup/eager/background failures can still be reported by owned
work or `shutdown()`; foreground suppression does not erase those diagnostics.
The existing options do not promise identical behavior for every background or
marker-factory combination in FusionCache.

## Evidence and reference scope

The released FusionCache binary `2.9.0+c2af1f39d3ad50791109bb9d48c0fdaffba010dd`
was executed through its public provider interfaces with controlled backend/
locker errors and the real reconnect callback. It retained warm L1 across
publication failure and reconnect, skipped the locker on hot hits, permitted
an ordinary cold origin with rethrow disabled, rejected it with rethrow enabled,
and retained stale for fail-safe. This reference fixture is a controlled
provider test, not a native Redis timing comparison.

[Public outage contracts](../tests/locker_outage_contract.rs) check local and
hydrated L1, bounded/unbounded memory, default strict admission, cooperative
suppression/rethrow, stale versus physical expiry, cancellation, local
remove/tag/clear, malformed frames plus received removes, and healthless
periodic/default behavior. The older [hydration barrier contracts](../tests/review_hydration_continuity.rs)
remain active for the default conservative policies.

This addition covers the stated ordinary outage scenarios. Full FusionCache
functionality and option-combination evidence remain in [the active inventory](FULL_CONTRACT.md).
