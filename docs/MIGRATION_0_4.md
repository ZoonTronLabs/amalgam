# Migration from 0.3 to 0.4

Version **0.4.0** is published on crates.io. This guide describes its breaking
changes from 0.3 and the complete eight-operation API. Behavioral differences
and remaining qualification gaps are documented in [PARITY.md](PARITY.md).

## Construction

`CacheBuilder::build()` is removed. Use the existing `try_build()` and handle
its typed configuration, provider, plugin or runtime rejection:

```rust
# fn example() -> amalgam::Result<()> {
let cache = amalgam::Cache::<String>::builder().name("profiles").try_build()?;
# let _ = cache;
# Ok(())
# }
```

`Cache::new()` remains an infallible, runtime-free constructor for the fixed
valid memory-only defaults. Configured native construction remains fallible
through `BlockingCache::from_builder`; acknowledged backplane construction uses
`try_build_ready().await?`. Construction no longer has a public panicking alias.

## Factory output

Factories return an ordinary `Result<V, E>` instead of a cache-specific product.
The error must implement `std::error::Error + Send + Sync + 'static`.
For an infallible origin, use `Infallible` explicitly:

```rust
use std::convert::Infallible;
use amalgam::Cache;

# async fn example() -> amalgam::Result<()> {
let cache = Cache::<String>::new();
let value = cache
    .get_or_set("name", |_| async {
        Ok::<_, Infallible>("Alice".to_owned())
    })
    .await?;
# Ok(())
# }
```

A fallible origin can return its own error directly. The cache error retains
that error as the factory error's source; callers can inspect its concrete
cause through `std::error::Error::source`.

`ctx.value(value)` still returns a plain `V`. The modified and not-modified
helpers publish their conditional metadata through the request rather than
returning a public product envelope. Factory option and tag edits apply to
that origin's eventual commit even when it suspends.

## One retrieval operation, two source kinds

The factory and supplied-value overloads use the same `get_or_set`. A plain
factory whose context is unused can remain a closure. When using context
methods, `source::factory` gives Rust the context type without allocating or
wrapping the callback at runtime:

```rust
use amalgam::{source, Cache};
# async fn example(cache: &Cache<String>) -> amalgam::Result<()> {
let value = cache
    .get_or_set("profile", source::factory(|mut ctx| async move {
        ctx.try_set_tags(["profiles"]).map_err(amalgam::FactoryError::from_source)?;
        Ok::<_, amalgam::FactoryError>("Alice".to_owned())
    }))
    .options(|options| options.with_duration(std::time::Duration::from_secs(30)))
    .await?;
let existing_or_supplied = cache.get_or_set("profile", source::value(value)).await?;
# let _ = existing_or_supplied;
# Ok(())
# }
```

A supplied value preserves the previous constant-source behavior: it does not
run factory-only timeouts, emit factory-success events or trigger eager refresh.
It returns an existing eligible value instead of unconditionally replacing it;
use `set` for replacement.

The former `get_or_set_with`, `get_or_set_full`, cancellable and commit variants,
and `get_or_set_value*` methods are removed. Use `.options(...)`, `.tags(...)`,
`.cancellation(...)` and `.with_receipt()` on the ordinary request. An optional
fallback uses `.fail_safe_default(Some(value))`; `None` removes the fallback.
For `Cache<Option<T>>`, `Some(None)` is a present null fallback, distinct from no
fallback. Factory capture destruction, cancellation and commit ownership retain
the same contracts.

Native requests use the same source choices and execute explicitly. Ordinary
native factories continue to run on the caller thread:

```rust
use amalgam::{source, BlockingCache};
# fn example(cache: &BlockingCache<String>) -> amalgam::Result<()> {
let value = cache
    .get_or_set("profile", source::factory(|_| {
        Ok::<_, std::convert::Infallible>("Alice".to_owned())
    }))
    .execute()?;
let observed = cache.get_or_set("profile", source::value(value))
    .with_receipt().execute()?;
match observed.commit {
    amalgam::advanced::BlockingCommitReceipt::Unchanged => {}
    amalgam::advanced::BlockingCommitReceipt::Mutation(receipt) => { receipt.wait()?; }
}
# Ok(())
# }
```

A dropped request does nothing. Manually polling an async request requires
`.into_future()` before pinning it; configuring the request itself does not
start work.

## Value writes

Ordinary writes return `Result<()>` and report failures to the caller:

```rust
# async fn example(cache: &amalgam::Cache<String>) -> amalgam::Result<()> {
cache.set("name", "Alice".to_owned()).await?;
cache
    .set("name", "Bob".to_owned())
    .options(|options| options.with_duration(std::time::Duration::from_secs(30)))
    .tags(["profiles"])
    .await?;
# Ok(())
# }
```

The `try_set`, `try_set_full`, `try_set_full_cancellable` and `set_full` aliases
are removed. Use the ordinary `set` request with `.options(...)`, `.tags(...)`,
`.cancellation(...)` and optional `.with_receipt()`. A manual future consumer
first calls `.into_future()`; native callers use `.execute()`.

An options edit starts from this cache's defaults, preserving settings that the
closure does not change. Requests validate string tags and return a typed error
for invalid input. A write needing distributed completion evidence explicitly
requests a receipt:

```rust
# async fn example(cache: &amalgam::Cache<String>) -> amalgam::Result<()> {
let receipt = cache.set("name", "Alice".to_owned()).with_receipt().await?;
let report = receipt.wait().await?;
# let _ = report;
# Ok(())
# }
```

## Invalidation

`remove`, `expire`, `remove_by_tag` and `clear` return lazy requests. Awaiting
returns `Result<()>`; use `.with_receipt()` only when completion evidence is
needed. There are no separate `try_remove*`, `try_expire*`, `try_clear*` or
`remove_by_tags` overloads.

```rust
# async fn example(cache: &amalgam::Cache<String>) -> amalgam::Result<()> {
cache.remove("name").await?;
cache.expire("profile").await?;
cache.remove_by_tag("profiles").and_tags(["settings"]).await?;
cache.clear(amalgam::ClearMode::Remove).await?;
# Ok(())
# }
```

`ClearMode::Expire` keeps eligible stale values for fail-safe;
`ClearMode::Remove` removes them. Ordinary `expire` expires L1 and removes L2.
Explicit advanced `.distributed_policy(DistributedExpirePolicy::RetainStale)`
selects L2 retention. A tag batch rejects invalid input before changing any tag.

Entry invalidation options start from `entry_options`; tag and clear requests
start from `tags_entry_options`. Per-key providers do not choose marker defaults.

Native mutations use the same request choices and execute explicitly:

```rust
# fn example(cache: &amalgam::BlockingCache<String>) -> amalgam::Result<()> {
cache.set("name", "Alice".to_owned()).execute()?;
cache.remove("name").with_receipt().execute()?.wait()?;
cache.clear(amalgam::ClearMode::Remove).execute()?;
# Ok(())
# }
```

A request that is dropped without awaiting or executing does nothing. Former
unit adapters hid failures. For example, invalidation with tagging disabled now
returns the existing `MarkerError::Unsupported`; it still leaves cached contents
unchanged. Handle this error explicitly if that configuration is intentional.

## Explicit maintenance

`run_pending_tasks` now returns `Result<()>` and preserves an external memory
provider's error. The separate `try_run_pending_tasks` method is removed. Await
or execute the existing maintenance operation and handle its result; this
operation performs maintenance and does not replace `flush_pending` for actual
background commit completion.

## Removed ineffective lock hints

`CacheBuilder::lock_shards` and the shard argument to `KeyedLock::new` are
removed. They did not select lookup sharding or serialize independent keys.
Use `KeyedLock::new()` or its default. Lookup sharding remains an implementation
detail; per-key ownership and timeout behavior are unchanged.

## Provider and advanced imports

The root keeps the ordinary cache, options, factory context, errors and common
plugins. Optional provider interfaces and implementations are imported from
`amalgam::provider`; explicit completion evidence and stronger/diagnostic
contracts are imported from `amalgam::advanced`.

```rust
use amalgam::{Cache, EntryOptions};
use amalgam::provider::{InMemoryDistributedCache, JsonSerializer, SystemClock};
use amalgam::advanced::{CommitReceipt, LeasePolicy, ReconciliationPolicy};
```

For Redis, import `RedisDistributedCache`, `RedisBackplane` and
`RedisDistributedLocker` from `provider` with the `redis` feature enabled.
Storage/locker capabilities and their associated outcomes remain beside those
provider traits. Marker snapshots, detailed layer/eviction events, recovery
tickets, receipt types and explicit runtime controls live in `advanced`.

This moves 193 former root reexports to the two namespaces, leaving 49 with all
features enabled. The types and execution guarantees are the same. Existing
implementation module paths remain accessible, but new examples use the two
intentional entry points. Imports do not enable a runtime feature or add work
to a disabled cache path.

## Immutable L2 provider bytes

`provider::DistributedCache::get` now returns
`Result<Option<provider::DistributedBytes>>` instead of `Result<Option<Vec<u8>>>`.
The new type is an immutable owned byte snapshot. A received snapshot must remain
valid after the provider replaces, removes or expires its stored value.

A provider receiving an owned network buffer can return
`buffer.map(DistributedBytes::from)`. To read or deserialize, use `as_ref()` or
borrow it as `&[u8]`. To obtain an independently editable buffer, convert an owned
snapshot with `Vec::<u8>::from(snapshot)`. Cloning a stored `DistributedBytes`
shares backing bytes; writes replace the snapshot instead of mutating its buffer.

The `set` input remains `Vec<u8>`. Serializer signatures, encoded snapshots,
physical namespaces, TTL, marker coordination and fenced-write capabilities keep
their existing contracts. This provider signature change does not add another
cache operation or change the wire format. The reference in-memory backend
avoids a serialized-buffer copy on repeated reads; its first sharing operation
may still allocate sharing metadata.

## Read-only operations

The sole read operation is `try_get(key)`: awaiting returns `Result<Option<V>>`.
Use `get_or_default(key, value)` for `Result<V>` with an explicit successful-miss
fallback. Native callers configure the same request and call `.execute()`.

```rust
# async fn example(cache: &amalgam::Cache<String>) -> amalgam::Result<()> {
let value: Option<String> = cache.try_get("profile").await?;
let fallback = cache.get_or_default("profile", "anonymous".to_owned()).await?;
let stale = cache.try_get("profile")
    .options(|o| o.with_allow_stale_on_read_only(true))
    .await?;
# let _ = (value, fallback, stale);
# Ok(())
# }
```

`read`, `read_cancellable`, `read_or_default`, `read_or_default_cancellable` and
error-swallowing legacy read signatures are removed. `MaybeValue` is removed;
use ordinary `Option`, including `Some(None)` for a present null cached value.
An explicit cancellation signal uses `.cancellation(token)`. Cancellation and
configuration/copy failures always retain their typed channel. Transport and
codec errors follow the selected rethrow options; an open circuit is an admission
skip. A configured suppressed read fault is a successful miss/fallback.

The supplied default is destroyed inside the admitted operation on a hit, so
shutdown waits its destructor and cancellation is rechecked before completion.
A dropped request performs no lookup. Manual Future consumers use
`.into_future()` before pinning. Read-only operations never call a factory or
write L2/backplane. See [the documented FC differences](PARITY.md).
