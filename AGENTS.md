# Agent instructions

<!-- owner-engineering-policy:begin -->
## Required engineering policy (2026-09-21)

Before writing, reviewing, or refactoring code, read [Typed outcomes and closed data hierarchies](docs/engineering/typed-outcomes-and-closed-hierarchies.md). This owner policy applies across languages: typed Result for expected failure; closed data/state with exhaustive handling; open interfaces/traits for extensible behavior; invariant-preserving factories; incremental migration with callers and tests. Use the repository's pinned language version. C# 15 `closed` is gated on deliberate stable-toolchain adoption; a private constructor on an abstract record does **not** guarantee closure today.

This policy updates older advice about Result, record closure, and exhaustiveness. Preserve all unrelated project rules, public/wire compatibility, and delivery boundaries.
<!-- owner-engineering-policy:end -->
