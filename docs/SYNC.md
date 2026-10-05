# Synchronous and asynchronous operations on one cache

The unreleased source provides `BlockingCache<V>` for ordinary threads and
`as_async()` for the same `Cache<V>`. Both views share entries, key flights,
markers, providers, events, commits and public lifetime. They do not copy
values into a separate cache. Cloned async handles retain the driven executor
after the last native handle is dropped.

```rust
use amalgam::BlockingCache;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cache = BlockingCache::<u64>::new()?;
    assert_eq!(cache.get_or_set("answer", |ctx| Ok(ctx.value(42)))?, 42);
    cache.try_remove("answer")?.wait()?;
    assert!(!cache.read("answer", None)?.has_value());
    cache.shutdown()?;
    Ok(())
}
```

`from_builder` accepts the existing cache configuration. For explicitly shared
executors and transport dependencies, create a `BlockingRuntime`, construct
async providers through `runtime.run(...)`, and use `BlockingCache::on_runtime`.
A provider depending on a different executor still needs that executor alive.
The facade drives cache I/O and timers independently; its caller remains
occupied, including when called on a foreign current-thread Tokio worker.
Use the async view when the caller must keep its own event loop available.

## Factory dispatch and lifecycle

An ordinary infinite-budget factory runs inline on its initial caller thread.
Finite effective deadlines, explicit cancellation and eager refresh dispatch
onto separate callback pools. `FactoryContext::invocation()` distinguishes
foreground and eager work. Effective budgets use Amalgam's existing fail-safe
rules: an explicit default can activate its soft budget without stale data.
Released FusionCache 2.9 does not activate that soft budget without stale data;
this remains a documented semantic difference.

The default executor has two I/O workers and at most 32 root callback threads.
`with_workers` accepts positive counts and rejects more than 64 I/O workers or
512 root callback threads with `BlockingRuntimeError::ThreadLimit` before
starting any threads. I/O has a separate 32-thread blocking allowance.

Nested offloaded calls use their global callback depth, including across
executors. Every child waits for a deeper callback pool; opposite A-to-B and
B-to-A roots cannot wait on each other's occupied root pools. Up to 31 lazy
single-thread nested pools exist per executor. More than 32 offloaded ancestors
returns `BlockingDispatchError::NestingLimit` through the preserved factory
error chain. These explicit resource bounds differ from .NET ThreadPool policy.
They do not make arbitrary user-created dependency cycles or barriers safe.

Admission acquires a callback permit before submitting blocking work. Cancelled
queued captures are released without waiting for another cache's callback.
Started callbacks cannot be forcibly interrupted: cancellation can return to
the caller promptly, while shutdown still retains the callback, captures,
result destruction and original panic join cause until actual completion.

`close()` requests cancellation; `shutdown()` observes actual drainage.
`flush_pending()` observes current scheduled work. Calling either drainage
operation from an active synchronous factory of that cache returns
`Error::ReentrantDrain` before changing lifecycle. This guard also applies
through its async view and survives nested dispatch. Async factories awaiting
their own shutdown are not covered by this synchronous callback guard.
Dropping the last public sync/async handle requests close and retains the owned
executor through asynchronous drainage. Explicit shutdown reports failures.

## Supplied values and completion

`get_or_set_value*` supplies data, not a user factory. Factory timeouts and eager
value refresh do not apply, and factory success/eager events are not fabricated.
The configured options and tags still govern storage, reads and mutations.
Factory and supplied-value operations offer actual commit receipts, including
cancellable forms. `Scheduled` retains a driven completion handle; `wait()`
observes storage/publication and cleanup, not peer receipt of a notification.

Public runtime, mixed-view and real Redis/Valkey contracts are in
`blocking_contract`, `blocking_mixed_contract`, `blocking_scheduling`,
`blocking_redis` and `constant_origin_contract`. They are targeted evidence,
not a claim of complete FusionCache functionality. See [the open inventory](FULL_CONTRACT.md).
