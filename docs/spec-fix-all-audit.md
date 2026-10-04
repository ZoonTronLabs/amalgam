---
title: 'Исправить весь аудит Amalgam и проверить FusionCache-контракт'
type: bugfix
created: '2026-10-04'
status: done
baseline_commit: 'c82c2c71f7960751c1c2836a61dfb1dd3b3249fb'
context: []
---

<frozen-after-approval reason="Owner explicitly authorized fixing absolutely all audit findings">

## Intent
User: «Фикси абсолюно все». Исправить весь доказанный аудит amalgam-cache, устранить известные surface gaps, проверить остаточные риски, сохранить поддержку Rust1.88 и доставить проверенный исходный код без нового routine PR. Scope не сокращать по сложности или зелёным старым тестам.

## Boundaries & Constraints
Always: исходники/доказательства версии authoritative; owner typed-outcomes policy; closed внутренние состояния и открытые backend/serializer/plugin/cloner traits; корректная отмена, namespace/version fences, независимые TTL и явные expected errors. Preserve wire/data и существующие callers посредством совместимых adapters; breaking изменения только с явной versioned migration.
Never: молча выбрасывать возможности или делать unsupported flags no-op вместо исправления; смешивать shared FC semantics с port bugs; менять чужие untracked policies; публиковать юридические документы, менять production инфраструктуру или protections.
Delivery: owner разрешил implementation и обычный verified push/merge. Production rollout требует конкретного summary и отдельного explicit approval по сохранённому правилу; до него подготовить и проверить все artifacts.

## I/O & Edge-Case Matrix
| State | Action | Expected |
|---|---|---|
| Разные collided keys | Nested origin lookup | Завершается, независимые flights |
| Occupied/stale/cold flight | Timeout/cancel/eager | Finite budget; owned guards; cluster coordination |
| Older snapshot/message/replay | New invalidation/write | Проверка версии; новая операция сохраняется |
| Cold/restarted/lagged node | Read L2/tag/clear/expire | Durable invalidation и read-through |
| Split L1/L2 options | Write/hydrate | Каждая freshness/physical deadline независима |
| Codec/transport/config failure | Public call | Разные typed outcomes, policy один раз |
| Clone/weighted/priority/plugin/tracing | Configured behavior | Обещанные guarantees, lifecycle и attribution |

</frozen-after-approval>

## Code Map
- src/cache.rs: public API и pipeline; основная интеграция.
- src/locking.rs,distributed_lock.rs: per-key ownership и budget.
- src/recovery.rs,tags.rs,backplane.rs,redis_backend.rs: поколения/retry, markers, typed commands и совместимый wire.
- src/entry.rs,memory.rs,options.rs,distributed.rs: отдельные metadata, weighted retention и options.
- src/error.rs,registry.rs,plugins.rs,events.rs,observability.rs,otel.rs: typed failures, atomic registry, lifecycle и diagnostics.
- tests/regression_*.rs: перенесённые audit probes и полноценный acceptance.
- examples/: comparative perf/codec fixtures.
- Cargo.toml/Cargo.lock, .github/workflows/ci.yml, README.md,docs/PARITY.md,docs/AUDIT.md,CHANGELOG.md: dependencies, package/compatibility и validation.

## Tasks & Acceptance
Execution:
- [x] C01–C08: per-key locks, owned cancel-safe factory, finite waits, soft fallback, eager L2/cluster, origin snapshot ordering, immediate deadline и lease budget.
- [x] D1–D14 (D15=C08): durable namespaced markers, ordered background commit/publish, recovery supersession/CAS, passive fence, cold expire, lag reconcile, marker retry, separate L2 TTL, write outcomes, message ordering/equality, escaped wire.
- [x] AC01–AC13: readonly L2, atomic registry, actual clone isolation, weight/priority, miss/eviction events, codec isolation, error source, transactional OTel, package/defaults/build invariants и lag-tolerant observer.
- [x] S1/S2: patched transitive dependencies и CI MSRV/default/features/consumer/advisory/regression gates.
- [x] Additional gaps: deterministic per-cache plugin teardown, bounded cache-name metrics/spans, explicit factory cancellation context, dynamic mutation options, bounded marker/lifetime resources, reconnect/lease/clock/huge-duration scenarios.
- [x] Full original/new acceptance, real Redis faults, pinned .NET oracle and repeatable perf/codec validation; local native Linux/macOS and packaged consumer.

## Review and Delivery Gates
- [x] Independent adversarial reviews and repair of every actionable finding.
- [ ] Exact source commit passes native Linux/Windows/macOS CI before main delivery.
- [ ] Source delivery, release readiness/consumer impact, vault/ADR and requirement-by-requirement completion evidence.

Acceptance:
- Given every named finding, when its public-API regression runs, then the required invariant is observed, not merely an implementation-text match.
- Given supported public/wire consumers, when compiled/roundtripped, then compatibility survives or a deliberate versioned migration is tested.
- Given FC-shared cases, when compared to pinned2.9 oracle, then shared contract is documented and regression-protected.
- Given all feature configurations and minimum compiler, when checks run, then tests/lints/docs/package pass and runtime health is evidenced.
- Given workload of128 distinct cold keys, when origin10ms runs, then shard collisions do not serialize independent flights.
- Given remaining risks from full-audit sections10–12, when stronger evidence is collected, then each is resolved/tested or explicitly modeled as deliberate supported contract; no hidden deferred defect.

## Spec Change Log
2026-10-04: Owner approved entire existing audit through explicit «Фикси абсолюно все». No additional scope approval checkpoint necessary. Preserve full scope including maintenance/surface gaps.

## Design Notes
Compare local branch patches with a shared typed flight/commit pipeline. Choose incremental shared ownership/version/outcome mechanisms for causally related P1 repairs; mechanical doc/metric fixes remain local. TTL insertion time differs from snapshot ordering time. Replay generation differs from timestamp. Backend/codec/cloner/plugins remain extensible traits. Missing config is rejected before cache tasks start.

## Verification
Original85/default75; regression scenarios; realRedis7 loopback; strictfmt/Clippy/rustdoc; Rust1.88; each feature; packaged README consumer; RustSec; .NET2.9 oracle; Release final sequential perf samples; reconnect/lease/cancel/marker/recovery fault tests; independent blind/edge/acceptance review; inspect exact delivered commit.


2026-10-04: Remote main revalidated and fast-forwarded to c82c2c7 (README badges only). Residual time boundary probes proved long-duration conversion truncation and ManualClock wrapping; include R1/R2 in repair acceptance, keep full scope.

2026-10-04: R3 is independently reproduced: full Timestamp range subtraction and addition truncate through signed intermediate saturation. Included with R1/R2 under Q8; the4 public boundary probes fail before repair on Rust1.88.

2026-10-04: Foundation handoff36/36 and integrated3/3. Root integrated source version0.3.0, security minima/lock and clock boundary repairs. Root all-feature Rust1.88 foundation/time42/42; strict MSRV library/time Clippy and main RustSec0/0 passed. Distributed foundation phase begins; end-to-end integration and full release gates remain.

2026-10-04: Workflow phase organization clarified without changing frozen intent or acceptance: independent review and exact-commit remote/native delivery checks are explicit subsequent gates rather than circular preconditions for entering review. Local implementation/gates still cover every named finding; all remote gates must complete before the user goal can finish. The first measured repaired hot path is ~5x slower than FC2.9 and allocates~19 times/call; performance work remains open, no successful equivalence claim.

2026-10-04: All six implementation/local validation tasks complete. Frozen source passes current macOS Rust1.88 base228/allfeatures259 with required Redis, eachfeature/lint/docs, Linux ARM1.88 base228/allfeatures259 with required Redis, actual archive72 files/fresh+historic full consumers174 dependencies/README and unchanged Akzhol helper4. Three final sequential compiled Rust/.NET process pairs retain limits: hot get319ns vs252ns, read284ns vs216ns, cold5354ns vs2208ns, distinct128×10ms12.65ms vs11.27ms. No universal equal-speed claim. Independent reviews and exact-commit remote delivery remain required subsequent gates.

2026-10-04: Three independent fresh-context blind/edge/acceptance reviews completed on an immutable candidate. Eleven deduplicated implementation defects/validation gaps are patch repairs within accepted ownership, version, finite-budget and typed-policy invariants. Added35 public regression checks, all passing targeted Rust1.88. The actual NuGet2.9 captured stale-after-L2-wait experiment confirms a shared legacy behavior; canonical read intentionally strengthens final retention validation. Full post-review local validation and exact-source remote delivery remain pending.

2026-10-04: Independent followups exposed four incomplete implementation boundaries: hydration versus another hydration, preferred newer/equal-stamp stale memory, owned session destruction after the last callback, and continuity invalidation during external copy or final storage insertion. All are patch repairs under existing frozen ownership/version invariants. Added19 checks (54 review checks total), all targeted Rust1.88 passes; the same added checks against the first repaired candidate reproduce18 failures plus the valid stale-L2 compatibility case. Entry eligibility is private local metadata and leaves wire snapshots unchanged. The live Redis gate now buffers through response-line boundaries and delays successful SET acknowledgements specifically, preserving grant-before-cancellation evidence without confusing pending cleanup replies. Full final local/native/package/performance and exact-source remote delivery remain required.

## Suggested Review Order

**Вход и владение операциями**

- Единый fallible вход фиксирует результат, политику ошибок и наблюдаемую операцию.
  [cache.rs:858](../src/cache.rs#L858)

- Counted admission удерживает владение через отмену и синхронные внешние callbacks.
  [execution.rs:149](../src/execution.rs#L149)

**Порядок хранения и read-through**

- Hydration сохраняет generation, continuity и неизменённую идентичность L1.
  [cache.rs:1781](../src/cache.rs#L1781)

- Private epoch eligibility защищает финальную вставку без изменения wire.
  [entry.rs:190](../src/entry.rs#L190)

- Commit lanes и receipts упорядочивают данные, peer effects и recovery.
  [cache.rs:2596](../src/cache.rs#L2596)

- Версии и стадии предотвращают replay устаревших операций и повтор успешных этапов.
  [recovery.rs:564](../src/recovery.rs#L564)

**Границы внешних ресурсов**

- Известный nonce получает контролируемую очистку при неопределённом ответе.
  [distributed_lock.rs:851](../src/distributed_lock.rs#L851)

- Stopped публикуется после explicit stop и уничтожения owned session.
  [plugins.rs:364](../src/plugins.rs#L364)

- Subscription ACK и смена epoch задают реальные границы continuity.
  [redis_backend.rs:689](../src/redis_backend.rs#L689)

**Контракт, проверки и поставка**

- Документ отличает поддерживаемое поведение, усиления и versioned migration.
  [PARITY.md:1](PARITY.md#L1)

- Проверка блокирует реальную вставку после fence, затем наблюдает новое чтение.
  [review_hydration_continuity.rs:235](../tests/review_hydration_continuity.rs#L235)

- Живой Redis подтверждает фактическую выдачу и освобождение блокировки.
  [distributed_redis.rs:466](../tests/distributed_redis.rs#L466)

- Aggregate требует все native, feature, Redis, package и security gates.
  [ci.yml:156](../.github/workflows/ci.yml#L156)

2026-10-05: Final local proof is coherent: macOS/native Linux Rust1.88 default282/all-features313 with mandatory live Redis; all11 local gates and stable1.95 strict ClippyPASS. Actual archive80 files/five Markdown documents, fresh+historic full174-dependency consumers, executable README and RustSec1290/232 dependencies clean; unchanged Akzhol helper4PASS. Three independent final rechecks accept all15 patches; acceptance313 product+11private, edge12private+54public checks. Final compiled sequential three-pair comparison: warmget315ns/FC243ns, warmread280ns/214ns, owned64KiB990ns/1978ns, cold5712ns/1961ns,128distinct×10ms14.70ms/11.27ms with actual128 origins. No universal speed parity is claimed. Implementation workflow done; exact-source remote CI and final delivery gates remain active under the separate user goal. Existing explicit owner authorization for verified push/merge takes precedence over Step05 routine push offer; no new PR or protection change.
