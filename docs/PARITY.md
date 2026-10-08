# Functional parity: FusionCache 2.9 and Amalgam 0.4.0

Reference: released FC 2.9.0. Amalgam 0.4.0 is published on crates.io.
The table describes its source contracts and existing evidence. Full paired
qualification remains incomplete. **Same** means the stated contract follows the FC reference and
has an Amalgam witness; **Diff** is an intentional adaptation with its reason;
**Gap** identifies incomplete support or qualification. A Rust test alone does
not prove a complete paired FC matrix.

The [pinned FC oracles](../tests/fusioncache/README.md) cover focused availability,
default and marker scenarios. Full FR-by-option paired coverage remains a Gap;
0.4.0 publishes this limitation rather than adding new features to close it.

| ID | Status | Contract / reason | Existing witness |
|---|---|---|---|
| FR-01 | Same | L1 hit, lock/recheck, L2 then factory; local commit before release | ready_hot_path_contract, foundation_contract, core_contract |
| FR-02 | Same | Factory and supplied value share get_or_set; constant source omits factory-only work | lazy_origin_request_contract, native_output_contract |
| FR-03 | Same | Read-only L1/L2; Result<Option<V>>, typed cancellation, configured codec/transport policy; no origin or L2 write | read_request_contract, read_failure_policy, null_value_contract, multilevel |
| FR-04 | Same | set/remove update L1 then configured distributed effects; typed result, optional receipt | set_request_contract, inline_write_contract, review_mutation_policy |
| FR-05 | Same | Default expire retains eligible L1 stale and removes L2; RetainStale explicit | expire_policy_contract; FC availability oracle |
| FR-06 | Gap | Logical/physical durations, jitter and zero-duration contracts exist; complete paired edge matrix is not qualified | entry tests, distributed_protocol, readiness_options |
| FR-07 | Same | Fail-safe chooses eligible newer data, then captured stale/default; no invented L2 replay | behavior, regression_concurrency, cooperative_codec_contract |
| FR-08 | Same | Soft wait needs an eligible stale entry; hard timeout and owned continuation remain distinct | fusion_defaults_contract, retained_origin_contract; FC availability oracle |
| FR-09 | Diff | Read budget includes decode/markers; configured rethrow can preserve hard timeout. FC bounds backend get and treats timeout as miss | cooperative_codec_contract, review_read_policy, layer_event_contract |
| FR-10 | Same | Per-key stampede protection; finite lock policy may serve stale or permit best-effort origin | foundation_contract, regression_concurrency, parked_cancellation |
| FR-11 | Same | L2 freshness/skip policies and bounded L1 hydration with immutable snapshots | distributed_protocol, l2_hydration_copy_contract, l2_retention_copy_contract |
| FR-12 | Same | L2 precedes backplane; stages retain real completion/recovery evidence | review_mutation_policy, recovery |
| FR-13 | Same | Ordinary gaps retain L1; L2-only caches do not periodically clear L1. Strict profile is explicit | fusion_defaults_contract, locker_outage_contract; FC availability oracle |
| FR-14 | Diff | Last-write recovery, five-second delay; default queue is bounded to 1024. Local ordering rejects stale replay more strongly than FC | recovery, marker_recovery_contract; FC availability oracle |
| FR-15 | Gap | Native atomic tags work. Ordinary byte-provider work retained from the previous session has focused tests; arbitrary custom L2 marker parity is unqualified and is not a 0.4 promise | marker_visibility_contract, ordinary_marker_contract; focused FC marker oracle |
| FR-16 | Same | Clear Expire/Remove; memory-only physical clearing | fluent_invalidation_contract, behavior |
| FR-17 | Same | Eager refresh is factory-only, one owner, L2 preferred; error leaves usable data | regression_concurrency |
| FR-18 | Same | Mutable context adapts a request-local copy of options/tags/metadata; factory returns Result<V,E> | raw_factory_contract, lazy_origin_request_contract |
| FR-19 | Same | Conditional modified/not-modified refresh preserves metadata and typed failure | conditional_metadata_contract, core_contract |
| FR-20 | Diff | Cache<V>, random instance identity, effective prefixes and default provider; Rust type-specific cache replaces FC's heterogeneous cache | provider_context_contract, distributed_key_contract |
| FR-21 | Diff | Bounded event streams may lose events; deferred callbacks and panic isolation preserve caller progress | layer_event_contract, layer_plugin_contract, otel_metrics_contract |
| FR-22 | Diff | Owned shutdown drains work and callbacks; final handle Drop initiates close. Stronger lifecycle than FC disposal | core_contract, blocking_contract, retained_origin_contract |
| RS-1 | Same | Old value Drop is outside internal locks and synchronous before mutation return | eviction_value_contract, inline_write_contract |
| RS-2 | Diff | External callbacks/Drop run outside locks; ordinary Clone on a ready hit is the documented guarded exception | reader_slot_lifetime_contract, quiet_observer_contract |
| RS-3 | Same | Caller future Drop retains an already-started origin and its single-flight ownership | retained_origin_contract |
| RS-4 | Same | Leader panic propagates; followers receive a typed failure; later operations remain usable | regression_concurrency, retained_origin_contract |
| RS-5 | Same | Clone + Send + Sync + static; ready hit can be polled without a runtime; runtime-dependent plans validate explicitly | ready_hot_path_contract, read_request_contract, blocking_contract |
| RS-6 | Same | try_build rejects missing runtime/codec and unsupported fenced capability before effects | fusion_defaults_contract, readiness_options, foundation_diagnostics |

Rust RS contracts have no direct .NET analogue. Same/Diff there denotes the
required Rust contract, not an unexecuted FC experiment. Test names above are
entrypoints to the checked-in suites; complete paired coverage and custom L2
markers remain explicit Gaps.

## Selected ordinary defaults

30-second logical duration; fail-safe/eager/jitter off; infinite factory/lock/L2
budgets; no initial subscription wait; cooperative distributed lock; five-second
recovery delay with 1024-item bound; background L2 off and backplane on;
serialization rethrow on, transport/backplane rethrow off; v2 Prefix namespace.
Strict policies require real provider capabilities and do not fabricate atomicity.

Wire decoding, source compatibility and mixed-version runtime safety are separate
claims. Use a coordinated fresh physical namespace when older clients cannot
read the selected format. See [migration](MIGRATION_0_4.md),
[provider boundaries](MARKER_READS.md) and [performance](PERFORMANCE.md).
