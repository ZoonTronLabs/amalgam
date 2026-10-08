# Typed outcomes and closed data hierarchies

Policy version: 2026-09-21. Applies to every agent writing, reviewing, or refactoring this project. This is the owner's engineering policy, independent of the agent vendor. Preserve project-specific delivery, security, module-boundary, localization, and compatibility requirements.

Read this before changing domain models or fallible operations. Apply it to new and changed code and its affected callers; improve a touched design incrementally. Do not use this policy as permission for an unrelated rewrite, dependency upgrade, SDK change, or production deployment.

## 1. Design the shape before implementing the operation

State the invariants, legal variants, transitions, and expected failure cases. Compare at least two plausible designs and choose based on the likely direction of change. Explain this briefly in implementation notes; do not produce a ceremony for a mechanical edit.

- **Closed data/state:** a finite family owned together, with variant-specific data and frequently added operations. Use a sum type and exhaustive matching. Examples: payment state, arrival mode, command outcome, domain failure.
- **Open behavior:** independently supplied algorithms, strategies, integrations, or plugins. Use interfaces/traits or an intentionally extensible abstract base plus composition and dependency injection. Consumers invoke behavior rather than enumerate implementation classes.
- These choices can coexist: an open pricing strategy can consume a closed arrival model. Do not close services merely because closed data is preferred. Do not replace an open plugin contract with a type switch.
- Adding a variant to closed data requires updating exhaustive consumers. That is deliberate change visibility, not automatically a design smell. Adding an abstract operation to open behavior instead breaks implementers. Choose which axis must remain extensible.

Use value objects/newtypes for validated domain concepts and distinct entity identifiers. Parse wire strings once at a boundary. Do not branch on localized labels or put domain meaning in magic strings.

## 2. Result is the default for expected failure

Every domain/application operation that can fail in a way the caller is expected to handle returns a typed result: preferably `Result<TValue, TError>` or a project-standard equivalent whose failure carries a module-owned error type. An existing `Result<T>` is acceptable when its error channel is already suitably typed; use an explicit adapter when a legacy shared error envelope cannot express the domain family.

- Success carries only valid success data; failure carries only its error. Do not create new `IsSuccess + nullable Value + nullable Error` bags or boolean/out-parameter protocols. Do not use `null`, zero, an empty collection, or a default timestamp to disguise failure.
- Domain errors are a closed family with meaningful variants and relevant payloads: `AlreadyApproved`, `InsufficientFunds(Required, Available)`, `NotFound(Id)`. A string message, exception object, or generic `UnknownError` is not that model.
- Reuse the project's established Result abstraction. Do not introduce competing Result libraries per feature. When it is structurally weak, harden or adapt it in a staged, compatible migration with its callers and tests.
- Keep stable transport error codes/ProblemDetails in adapters. Localize explanations in presentation, not in the domain. Expected HTTP, validation, or retryable integration failures may retain an existing typed `ApiResult<T>` boundary and be mapped once to a domain outcome. Rust uses `Result<T, E>` and module-specific error enums; TypeScript uses discriminated unions and exhaustive narrowing.
- Compose with the existing `Match`, `Map`, `Bind`, or explicit exhaustive switch. Never discard failure or read `.Value` before success has been proven. Do not turn a failure into a fabricated successful default.
- Factories validate and return Result when rejection is a business case. After successful construction the invariant always holds; public setters, `init`, `with`, deserialization, and collection aliases must not reopen invalid states.
- A total pure calculation or predicate returns its actual value (`T`/`bool`); it does not need a fake failure branch. Optional absence uses an explicit option/nullable value when absence is a valid domain case. Neither is a failure sentinel.
- Cancellation remains cancellation: propagate the cancellation token and the platform's cancellation contract. Do not catch cancellation as generic business failure or success.
- Exceptions/panics are for programmer/contract defects or unrecoverable failures at the appropriate boundary, not routine business rejection. Handle anticipated I/O failures in infrastructure adapters; do not blanket-catch every exception into a meaningless Result. Keep diagnostic context and log at the responsible boundary.

## 3. Make incompatible states unrepresentable

Each variant carries exactly the data valid for that variant. Replace `enum + unrelated nullable fields`, boolean mode flags, and ambiguous tuples with named states/outcomes. Null means legitimate absence, not "this field belongs to another state".

Domain types must not also own storage, HTTP, serialization, UI, and orchestration. Keep persistence/wire snapshots separate and map explicitly. Data-centric derived queries and transformations may be pure functions/extensions over already-valid immutable values, in the owning domain/application module. A pure state transformation must use invariant-preserving construction and handle typed rejection; it cannot bypass an aggregate, mutate private state, or perform privileged side effects. Presentation formatting belongs in presentation; aggregate behavior that protects invariants remains at that boundary. Do not misread "data, not behavior" as permission for mutable property bags or bypassing factories.

Transitions consume the current valid state and return the next state or a typed rejection. Pass time, IDs, and randomness as values or inject their providers. No ambient clock/random/GUI/service-locator dependency in domain logic.

## 4. C# before native closed hierarchies

Use the SDK and language version actually pinned by the repository. An enum is suitable for data-free alternatives. For variant-specific data, use an explicit hierarchy.

**A private ordinary constructor does not fully close an abstract record.** A non-sealed record has a protected copy constructor, including a synthesized one. A derived record can call it even if the ordinary base constructor is private:

```csharp
public abstract record State
{
    private State() { }
    public sealed record Ready : State;
}
// Compiles on C# 14; the ordinary private constructor is not enough:
public record Unexpected(State Original) : State(Original);
```

For inheritance closure enforced by current C#, use an abstract **class** with a private constructor and nested sealed class variants when that guarantee is required. If record value semantics are useful, explicitly describe the family as convention-closed and enforce permitted descendants with an analyzer/architecture check. Never advertise that record encoding as the future language guarantee. Do not silently change existing public record value equality, `with`, serialization, or API contracts to obtain closure.

Current C# cannot infer exhaustive class-family coverage from either workaround. Keep a final `_ => throw new UnreachableException(...)` only as a tripwire for a violated internal closed-world assumption. It is not a business error and does not make adding a new variant a compile-time failure. Test transitions and permitted descendants, and review every consumer when a variant changes.

## 5. C# 15 migration gate

Checked 2026-09-21: native closed hierarchies are documented for C# 15 / .NET 11 preview. Do not enable `LangVersion=preview`, raise target frameworks, or use `closed` in projects pinned to older compilers merely because this document mentions it.

After the repository deliberately adopts a stable, supported toolchain in local development and CI:

1. Verify the feature in the pinned compiler, reference assemblies, analyzers, IDE/CI, and all consumer projects with a small compile probe.
2. Use the native root spelling, for example `public closed record State;`. `closed` is implicitly abstract; do not combine it with `abstract`, `sealed`, or `static`.
3. Direct descendants belong to the same assembly and module. Seal terminal variants. An unsealed leaf allows external descendants; an intermediate branch must itself be closed when the whole family must remain closed.
4. Keep every variant needed by exhaustive external consumers accessible. Handle `null` explicitly when the input is nullable.
5. Remove the artificial catch-all only after the compiler proves the intended exhaustive switch. Promote the relevant exhaustiveness diagnostic, including CS8509, to an error in that migration. A default arm that exists merely to silence diagnostics defeats the guarantee.
6. Preserve a catch-all when it has explicit domain meaning, such as "all other variants remain unchanged". For extensible external wire protocols, model a deliberate `Unknown/Unsupported` boundary variant instead of assuming the wire is a closed domain enum.
7. Rebuild and test dependent assemblies. Adding a variant can break recompiled exhaustive consumers; already-built code can fail at runtime on an unfamiliar variant. Plan versioning and rollout compatibility. Do not describe every variant addition as a binary linkage error.

Closed hierarchies and discriminated unions are related ideas, not interchangeable language features. Do not invent union syntax, claim automatic migration, or promise that every existing switch will become exhaustive.

## 6. Required review and incremental migration

For every meaningful model/result change, check:

- Can any public construction, `with`, deserializer, default struct value, or mutable collection produce an invalid state?
- Are failure, optional absence, cancellation, and successful empty data distinct?
- Is the error family typed and owned by the correct module? Is failure handled once rather than swallowed?
- Is this closed **data** or extensible **behavior**? Have leaves or an accidental catch-all defeated closure?
- What happens when a variant is added? Which consumers and wire/persistence contracts must change?
- Does the claimed compiler/analyzer enforcement actually run in this repository's build, with the correct namespaces and language version?
- Do tests cover meaningful success, rejection, transition, and compatibility cases rather than repeat implementation text?

Migrate a touched slice with its callers, mappings, and tests. Record unrelated legacy debt precisely; do not claim "Result everywhere" is already implemented merely because instructions were updated. For docs-only work, validate links, entrypoints, examples, and contradictions instead of rebuilding unrelated applications.

## Sources and attribution

The owner's supplied [Zoran Horvat video and transcript](https://www.youtube.com/watch?v=Nh0LNmqe7r4) motivate closed data versus open behavior and their opposite extension tradeoffs. The requirement to use Result for expected failures is the owner's additional policy; the video does not teach Result.

Primary language sources:

- [C# 15 status](https://learn.microsoft.com/en-us/dotnet/csharp/whats-new/csharp-15)
- [`closed` reference](https://learn.microsoft.com/en-us/dotnet/csharp/language-reference/keywords/closed)
- [Closed hierarchies specification](https://github.com/dotnet/csharplang/blob/main/proposals/csharp-15.0/closed-hierarchies.md)
- [Record specification: copy constructors](https://learn.microsoft.com/en-us/dotnet/csharp/language-reference/proposals/csharp-9.0/records)
- [Pattern diagnostics](https://learn.microsoft.com/en-us/dotnet/csharp/language-reference/compiler-messages/pattern-matching-warnings)
- [Switch expression runtime behavior](https://learn.microsoft.com/en-us/dotnet/csharp/language-reference/operators/switch-expression)
