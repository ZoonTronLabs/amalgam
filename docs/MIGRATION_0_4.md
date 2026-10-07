# Migration from 0.3 to 0.4

Version 0.4 is under development and has not been published. This guide is being
updated alongside the API; the complete eight-operation migration is a release
requirement.

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

## Remaining migration work

The read and retrieval facades, remaining legacy adapters, provider and
advanced namespaces, and the complete examples are still being migrated.
They must be complete before publishing 0.4.0. The 0.3 `MaybeValue` and
error-swallowing adapters are not the target API.
