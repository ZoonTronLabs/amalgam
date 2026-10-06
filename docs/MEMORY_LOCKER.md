# Custom local memory coordination (unreleased)

`CacheBuilder::memory_locker(Arc<dyn MemoryLocker>)` supplies local coordination
for ordinary values and options-controlled secondary marker observations.
`BlockingCache` and its async view share the same provider, as do foreground,
soft-timeout background and eager work. The default `KeyedLock` remains a
concrete backend: its guard is not boxed and no provider dispatch is added to
warm hits. `KeyedLock` also implements the public trait for explicit integration.
This API is not present in the published registry package 0.3.1.

## Ownership and outcomes

An independently supplied implementation returns `MemoryLockOutcome::Acquired`
with a move-only `MemoryLock`, or `Unavailable`. Failure is a distinct
`MemoryLockerError`; the original typed source is retained in `Error::MemoryLocker`
and classified as a lock error, without opening the Redis transport circuit.
A cancelled acquisition remains `OperationCancelled`, never an origin failure.

`MemoryLock::new` takes an owned `MemoryLockGuard`. Its consuming `release`
callback runs once on completion, cancellation or drop. Successful acquisition
moves the guard into the actual background flight after a soft timeout and into
eager work. Automatic release errors are logged and preserve the computed value,
as FusionCache does; explicit `MemoryLock::release` returns the typed error.
Automatic release also isolates provider panics. No type erasure with
`Any` or unsafe code is needed to supply a guard.

Requests contain the cache name and instance ID, processed key, typed entry or
marker kind, budget and acquisition-only cancellation. Use `coordination_key`
for lock identity: entry and marker namespaces differ even when their text keys
are equal. A shared provider can additionally partition by cache name.

The host bounds asynchronous acquisition even when a provider ignores its
budget. A deadline signals `HardTimeout` before the pending provider future is
dropped; caller cancellation/drop and cache shutdown retain their own reasons.
Success ends this acquisition token with `ScopeFinished`. The factory receives
its separate active token; acquisition timeout cannot cancel it. With no guard,
the existing memory-lock policy serves eligible stale data or permits an unlocked
ordinary factory. Eager work uses `try_acquire`, which must never block; no guard
means skip. Cooperative cancellation cannot interrupt opaque synchronous code.

Explicit owning-cache shutdown first drains factories, counted callbacks and
owned cleanup, then awaits one provider hook for that cache context. The hook
remains owned if the caller drops its shutdown future. A later shutdown waits
the same work, rather than calling the hook again. Provider errors and panics
remain in the idempotent shutdown report under the memory-locker stage. A shared
provider must close only resources belonging to the supplied context. An async
cache's final handle requests close; callers requiring an awaited provider hook
must call shutdown. The native final-owner path also owns asynchronous drainage.
Custom providers own their idle-slot maintenance. The standard backend has one
constructor-time box to keep the cache context compact; no per-acquisition
guard box or virtual call is added. A busy standard eager attempt also creates
no factory token.

## Reference and evidence boundaries

The reference is FusionCache source v2.9.0 at
`af09f81a3ea8d7ed71183b46501946da801a2a22`,
[IFusionCacheMemoryLocker](https://github.com/ZiggyCreatures/FusionCache/blob/af09f81a3ea8d7ed71183b46501946da801a2a22/src/ZiggyCreatures.FusionCache/Locking/IFusionCacheMemoryLocker.cs).
Its sync/async methods return an opaque lock object and release separately;
Rust adapts ownership into a consuming guard and drives asynchronous acquisition
through the existing native executor. The Rust provider API does not add a
separate blocking acquisition callback. Its per-cache shutdown context permits
sharing; it is not a claim of literal .NET IDisposable behavior.

`memory_locker_contract` supplies an external implementation and checks same-key
single flight, independent keys, hot-hit bypass, finite waits, stale/null-lock
fallback, cause preservation, caller cancellation/drop, hard factory timeout,
background guard retention, entry/marker namespace separation during a cold L2
flight, value/marker eager attempts, native/async views, shared ownership and
interrupted/idempotent teardown. Broader custom-provider/native option matrices
remain part of the full contract inventory. [Supplied value L1](MEMORY_STORAGE.md)
is also implemented; the wider family and full FusionCache functionality remain
open.
