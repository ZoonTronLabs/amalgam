# Synchronous and asynchronous operations on one cache

The unreleased source provides `BlockingCache<V>` for ordinary threads and
`as_async()` for the same `Cache<V>`. Both views share entries, key flights,
markers, providers, events, commits and public lifetime. They do not copy
values into a separate cache. Cloned async handles retain the driven executor
after the last native handle is dropped.

```rust
use amalgam::{BlockingCache, source};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cache = BlockingCache::<u64>::new()?;
    assert_eq!(
        cache
            .get_or_set("answer", source::factory(|_| Ok::<_, std::convert::Infallible>(42)))
            .execute()?,
        42
    );
    cache.remove("answer").with_receipt().execute()?.wait()?;
    assert!(!cache.try_get("answer").execute()?.is_some());
    cache.shutdown()?;
    Ok(())
}
```

`from_builder` accepts the existing cache configuration. For explicitly shared
executors and transport dependencies, create an `advanced::BlockingRuntime`,
construct async providers through `runtime.run(...)`, and use
`BlockingCache::on_runtime`. Storage, backplane, locker and serializer interfaces
and implementations are available in `amalgam::provider`.
A provider depending on a different executor still needs that executor alive.
The facade drives cache I/O and timers independently; its caller remains
occupied, including when called on a foreign current-thread Tokio worker.
Use the async view when the caller must keep its own event loop available.

## Factory dispatch and lifecycle

An ordinary infinite-budget factory runs inline on its initial caller thread.
Finite effective deadlines, explicit cancellation and eager refresh dispatch
onto separate callback pools. `FactoryContext::invocation()` distinguishes
foreground and eager work. Effective budgets use Amalgam's existing fail-safe
rules, matching FusionCache 2.9 defaults: a soft timeout requires a usable stale
entry. An explicit fail-safe default does not activate the soft timeout on its
own. The configured hard deadline still limits factory execution.

The default executor has two I/O workers and at most 32 root callback threads
per callback class. User factories and optional synchronous memory acquisition
have independent lazy pools: lock waiters cannot occupy their owning factories'
slots. The default built-in locker does not create an acquisition pool.
`with_workers` accepts positive counts and rejects more than 64 I/O workers or
512 root callback threads per class with `BlockingRuntimeError::ThreadLimit`
before starting any threads. I/O has a separate 32-thread blocking allowance.

Nested offloaded calls use their global callback depth, including across
executors. Every child waits for a deeper callback pool; opposite A-to-B and
B-to-A roots cannot wait on each other's occupied root pools. Up to 31 lazy
single-thread nested pools exist per executor per callback class. More than 32
offloaded ancestors returns `BlockingDispatchError::NestingLimit` through the
preserved factory or memory-locker error chain. These explicit resource bounds differ from .NET ThreadPool policy.
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
through its async view and survives nested dispatch. It also covers optional
synchronous memory acquisition and its guard release. Async factories awaiting
their own shutdown are not covered by this synchronous callback guard.
Dropping the last public sync/async handle requests close and retains the owned
executor through asynchronous drainage. Explicit shutdown reports failures.

## Supplied values and completion

`get_or_set(key, source::value(value))` supplies data. Factory timeouts and eager
value refresh do not apply, and factory success/eager events are not fabricated.
`source::factory(callback)` accepts an ordinary `Result<V, E>` callback; context
methods remain available for tags, options and conditional refresh. Both forms
return a lazy request. Choose `.options(...)`, `.tags(...)`,
`.fail_safe_default(...)`, `.cancellation(...)` and optional `.with_receipt()`
before calling `.execute()`. Dropping an unexecuted request drops its captures
without starting cache work.

Factory and supplied-value operations offer actual commit receipts. The receipt
types live in `amalgam::advanced`. `Scheduled` retains a driven completion handle;
`wait()` observes storage/publication and cleanup, not peer receipt of a
notification.

Public runtime, mixed-view and real Redis/Valkey contracts are in
`blocking_contract`, `blocking_mixed_contract`, `blocking_scheduling`,
`blocking_redis`, `blocking_memory_locker_contract` and
`constant_origin_contract`. They are targeted evidence,
not a claim of complete FusionCache functionality. See [the open inventory](FULL_CONTRACT.md).
