# Cache implementation layout

The public cache remains `amalgam::Cache<V>` and `amalgam::cache::Cache<V>`.
The builder is re-exported at its existing paths. Its implementation is split
into private modules so complete workflows can be read and reviewed together.

| File | Responsibility |
|---|---|
| [cache.rs](../src/cache.rs) | Public handles, shared private states and common operation policies |
| [api.rs](../src/cache/api.rs) | Public calls, ready-hit admission and observed execution boundaries |
| [builder.rs](../src/cache/builder.rs) | Configuration, invariant validation and construction |
| [origin.rs](../src/cache/origin.rs) | Static factory versus supplied-value strategy; no additional erased dispatch |
| [blocking.rs](../src/cache/blocking.rs) | Synchronous handles, completion types and owned factory supervision |
| [blocking/api.rs](../src/cache/blocking/api.rs) | Synchronous delegation to the same operation engine |
| [blocking/runtime.rs](../src/cache/blocking/runtime.rs) | Driven I/O, bounded callback admission, global depth and drain guards |
| [read.rs](../src/cache/read.rs) | Value reads, origin ownership, fail-safe and eager refresh |
| [immediate_read.rs](../src/cache/immediate_read.rs) | Build-selected inline completion of L2 reads over immediate providers |
| [write.rs](../src/cache/write.rs) | Value mutation admission and owned commit pipelines |
| [markers.rs](../src/cache/markers.rs) | Tag/clear observations, scoped repair ownership and marker mutations |
| [marker_eager.rs](../src/cache/marker_eager.rs) | Private marker child module: attempt admission, peer preflight and owned eager refresh |
| [recovery.rs](../src/cache/recovery.rs) | Captured stage-aware mutation replay |
| [runtime.rs](../src/cache/runtime.rs) | Backplane continuity, maintenance and deterministic shutdown |

Group new code by the workflow it implements. An arbitrary line-count split
would scatter decisions about the same operation. Module boundaries are for
navigation and responsibility; they do not create another runtime layer or
establish a performance improvement.

Keep shared closed states near their consumers; expose internal methods only
within this cache module when another workflow needs them. Extensible provider
behavior continues to use the existing traits. Synchronous facades delegate to
that same engine; avoid duplicate orchestration or public handles inside workers.

The mechanical extraction preserves operation bodies, closed states and
`CacheInner` fields. Existing contract/stack tests, mandatory native acceptance,
packaged consumers and separate release measurements verify each resulting
checkpoint. Merely having smaller files does not prove full FusionCache parity.
