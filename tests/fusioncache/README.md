# FusionCache 2.9 behavior oracles

These fixtures execute the pinned released FusionCache package through its public
cache and provider interfaces. Assertions fail the process; output describes the
observed values rather than a claimed overall parity percentage.

| Fixture | Rust public contracts | Checked behavior |
|---|---|---|
| `availability` | `fusion_defaults_contract`, `locker_outage_contract`, `readiness_options` | L1 retention over disconnect/reconnect, local hot hits, cooperative locker suppression/rethrow, stale fail-safe, L2-only retention, expire removing L2, soft timeout requiring a stale entry, startup and recovery defaults |
| `markers` | `ordinary_marker_contract` | Byte-only tag/clear, inclusive timestamp boundaries, Expire/Remove fail-safe, prefix isolation, marker reload, first hot initialization and captured recovery policy |

Run with .NET 10, keeping generated files outside the checkout:

```sh
dotnet build tests/fusioncache/availability/FusionAvailability.csproj -c Release -p:RestoreLockedMode=true -p:BaseIntermediateOutputPath=/tmp/amalgam-oracles/availability/obj/ -o /tmp/amalgam-oracles/availability/bin
dotnet /tmp/amalgam-oracles/availability/bin/FusionAvailability.dll
```

The Rust-specific shutdown, destruction and atomic-fencing construction contracts
have no direct .NET equivalent. Simulated provider outages verify cache behavior;
native Redis integration is checked separately.

The `markers` fixture uses the same locked released package and builds like
`availability`, substituting `markers/MarkerOracle.csproj` and an external
`markers` output directory. Its serialized boundary fixture injects a valid
entry just before, at and after the actual stored marker timestamp. Marker and
value wire formats are deliberately separate from Rust.

Ordinary recovery keeps the marker revision and captured policy while
recomputing physical TTL. Amalgam's stronger atomic snapshot recovery preserves
its original deadline. FC initializes two clear controls on the first untagged
hot read after set; subsequent hot reads reuse them. These focused fixtures
do not qualify the full FR matrix or custom-provider marker parity; see
[PARITY](../../docs/PARITY.md).
