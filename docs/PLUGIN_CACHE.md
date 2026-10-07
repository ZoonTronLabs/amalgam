# Plugin access to cache operations (unreleased)

`CachePlugin<V>` receives `CachePluginContext<V>` after the cache components are
constructed. `context.cache()` acquires a `PluginCache<V>` view of that exact
cache. Through `Deref<Target = Cache<V>>` it supports the existing complete
typed API: reads, factories, writes, options, tags, invalidation, cancellation,
receipts, provider inspection and events. `view.blocking(runtime)` supplies the
same synchronous facade using an explicit driven executor.

Register with `CacheBuilder::cache_plugin`, `Cache::register_cache_plugin`, or
`BlockingCache::register_cache_plugin`. The existing `Plugin`/`PluginContext`
contracts and registration methods remain available. Legacy and cache-aware
builder registrations preserve their interleaved order. One shared plugin
definition receives independent sessions and cache contexts for every cache.

## Lifetime and stop operations

The context holds a weak cache reference. Operational views temporarily retain
cache state but never count as application owners. Even if a session retains a
view or a cloned underlying async cache, dropping the final application handle
still initiates close. A retained view can retain closed allocations until it is
dropped; it cannot keep the service running. `context.cache()` returns
`PluginError::HostStopped` when there is no available operation scope.

Normal plugin calls share the application's admission, cancellation and
same-key coordination. Their parked factories are cancelled when the cache
closes, just like ordinary caller work.

The session's synchronous `PluginSession::stop` hook receives a separate cleanup
scope. Its captured context and operational views can still read, compute,
write and invalidate when application admission has already closed. Providers,
cache contents and options remain the same. Await required mutation receipts
before returning from `stop`; unfinished cleanup work is cancelled at that
boundary. Owning `shutdown` drains sessions before the final supervised task
snapshot, including deferred stop after callbacks or a late startup race.

Awaiting owning `shutdown` or `flush_pending` from this session's stop hook would
wait for itself; it returns the existing typed `Error::ReentrantDrain`. Ordinary
cache calls in callbacks use normal coordination. Opted-in `PluginObservations::All` logical/layer
callbacks are deferred until their originating coordination guards and operation
clones are released, permitting same-key read/mutation from a cache-aware hook.
Selected hooks and drainage are described in [LAYER_EVENTS.md](LAYER_EVENTS.md).
Legacy `Logical` callbacks retain original inline timing and cannot wait for a
factory whose key they hold. Calling an owning drain from a hook still attempts
to wait for itself.

After manual detach, retained operational views continue to use ordinary cache
admission while application owners remain. After owning close, calls return
typed closed/cancellation errors. Failed startup preserves its source and does
not invent a stop call for a session that was never returned; completed cache
writes are not rolled back. A failed stop is retained by owning shutdown, which
continues draining other owned work. Teardown access closes even if user stop
panics.

## Example

```rust
use std::sync::Arc;
use amalgam::{BlockingCache, advanced::BlockingRuntime, Cache, CacheEvent, advanced::CachePlugin, advanced::CachePluginContext, advanced::PluginCache, PluginError, PluginSession, PluginStage};

struct Seed { runtime: BlockingRuntime }
struct Session { cache: PluginCache<String>, runtime: BlockingRuntime }

impl CachePlugin<String> for Seed {
    fn name(&self) -> &str { "seed" }
    fn attach(&self, context: &CachePluginContext<String>)
        -> Result<Box<dyn PluginSession>, PluginError>
    {
        let cache = context.cache()?;
        cache.blocking(self.runtime.clone()).set("status", "started".into()).with_receipt().execute()
            .and_then(|receipt| receipt.wait().map(|_| ()))
            .map_err(|error| PluginError::from_source("seed", PluginStage::Start, error))?;
        Ok(Box::new(Session { cache, runtime: self.runtime.clone() }))
    }
}
impl PluginSession for Session {
    fn on_event(&self, _: &CacheEvent) -> Result<(), PluginError> { Ok(()) }
    fn stop(&self) -> Result<(), PluginError> {
        self.cache.blocking(self.runtime.clone()).set("status", "stopped".into()).with_receipt().execute()
            .and_then(|receipt| receipt.wait().map(|_| ()))
            .map_err(|error| PluginError::from_source("seed", PluginStage::Stop, error))
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = BlockingRuntime::new()?;
    let cache = BlockingCache::from_builder(Cache::builder()
        .cache_plugin(Arc::new(Seed { runtime })))?;
    assert_eq!(cache.read("status", None)?.value().map(String::as_str), Some("started"));
    cache.shutdown()?;
    Ok(())
}
```

## Evidence and reference boundaries

`plugin_cache_contract` checks Start/Event/Stop operations, same cache and native
provider identities, retained-view final-owner cancellation, weak-context
retention, interleaved registration order, independent sessions, original
failures, late startup after close and unfinished cleanup destruction. Its Redis
case uses actual L2, pub/sub and fenced locker providers; CI makes service
availability mandatory with `AMALGAM_REQUIRE_REDIS=1`.

FusionCache's released 2.9 plugin interface passes the complete `IFusionCache`
to both Start and Stop. Amalgam supplies typed operational access with explicit
owner lifetime and typed failures. It preserves ordinary Stop operations while
retaining its stronger deterministic callback/task drainage. Upstream stop-under-
registration-lock, disposal-abort-on-stop-error and scheduled-handler races are
not guarantees of this Rust API. Event dispatch configuration and other full
surface gaps remain in [FULL_CONTRACT.md](FULL_CONTRACT.md).

This additive API is not in the published crates.io 0.3.1 archive. The working
manifest's version does not identify a newer released package.
