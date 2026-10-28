# Rust style: Google's Rust Style Guide (owner rule, 2026-10-28)

The owner's Rust repositories follow
[Google's Rust Style Guide](https://google.github.io/styleguide/rustguide.html), which Google
open-sourced on 2026-10-21. Read the guide before you write, review or refactor Rust. This note
covers how the guide applies here and which owner policies take precedence over it. It
summarizes the guide in our own words and is not a copy; the guide itself is authoritative.

Repository copies live at `docs/engineering/google-rust-style.md`. The canonical copy is
`~/.agents/engineering/google-rust-style.md`.

## Precedence

1. **Owner policies win where they are stricter.** This covers the
   [typed outcomes and closed hierarchies](typed-outcomes-and-closed-hierarchies.md) policy:
   - a typed `Result` for expected failure;
   - closed enums matched exhaustively;
   - invariant-preserving constructors;
   - newtypes instead of primitives.

   It also covers each repository's AGENTS.md or CLAUDE.md: module boundaries, public and wire
   compatibility, the pinned toolchain and delivery rules.
2. **Then Google's guide.**
3. **Then the official Rust Style Guide and Rust API Guidelines**, on which Google's guide builds.

Rules tagged `[chrome]` or `[Fuchsia]` do not apply here. We use Cargo workspaces, async Rust,
threads and the Cargo directory layout.

## Library code and application code

- **Library crates are bound by the API-design rules.** These are crates consumed outside their
  workspace or published, such as `amalgam` and `eventlyrust`.
- **Application crates take those rules as strong guidance.** These are the `akzhol-rust`
  services, the `keycloak-rs` server and the `sufler` applications. Prefer simplicity and avoid
  speculative generality.

## The rules we apply

The guide has the detail. These are the points that most often matter in review.

### Organization and imports

- **No glob imports.** The exceptions:
  - `use super::*` in an inline test module;
  - variant globs inside a function about that enum;
  - `pub use submodule::*` in a non-leaf module.
- **One module per `use` statement.** Never nest `::` inside braces: the shape of rustfmt's
  `imports_granularity = "Module"`.
- **Free functions are called through their module**, for example `mem::take(..)` and
  `fs::read(..)`.
- **Types and traits are imported unqualified.** Rename an ambiguous one when you import it, for
  example `use std::io::Error as IoError`.
- **Import a trait locally** when only one function uses it.
- **A leaf module is named after its primary item.** New non-leaf modules hold module
  declarations and re-exports only. Existing modules migrate when you touch them.

### Formatting

- rustfmt is the authority, with `max_width = 100`.
- `#[rustfmt::skip]` is allowed only for a stated reason, never for taste.
- On a stable toolchain, unstable rustfmt options stay out of the committed `rustfmt.toml`. A
  one-off `cargo +nightly fmt -- --config imports_granularity=Module` pass is fine.

### Ownership and signatures

- **Borrow rather than clone.** Parameters take `&str`, `&[T]`, `&T` and `Option<&T>`, not
  `&String`, `&Vec<T>`, `&Box<T>` or `&Option<T>`.
- **`Rc` and `Arc` only where shared ownership is the design.** A shared async service or state
  behind `Arc<dyn Trait>` (the module-boundary service traits) is such a case. Never use them to
  dodge a borrow.
- **Enums instead of `bool` parameters.** This is also an owner rule.
- **Iterators in library code.** A library returns iterators and takes `impl IntoIterator`
  where it only iterates. Application code may use `Vec` and slices when that is simpler.
- **`impl Into<T>` parameters only in library APIs.**

### Errors

The owner policy is stricter than the guide here.

- **Expected failure** is a typed `Result` with a module-owned error enum. `Option` is used only
  when absence has exactly one obvious cause.
- **Bugs and broken invariants.** The guide allows `panic!` and `expect`. Our repositories deny
  `unwrap` and `expect` in production paths (clippy `unwrap_used`, `expect_used`). Model the
  invariant in the types, or return an internal error. Tests may `unwrap`, `expect` and panic.
- **Never use `panic::catch_unwind` for control flow**, and never reach into a poisoned mutex.

### Types and traits

- **Newtypes, not type aliases, for domain values.** This also follows from the owner's ban on
  primitive obsession.
- **Named fields, not tuples.** A newtype is the exception.
- **Implement the standard trait instead of a look-alike method:**
  - `FromStr`, `From` and `Display`;
  - `Default` when `new()` exists;
  - `is_empty()` alongside `len()`.

  Derive whatever can be derived. Use `PartialEq` and `PartialOrd` unless `Eq` and `Ord` are
  meant.
- **Traits only for:**
  - generic bounds;
  - `impl Trait` and `dyn Trait`;
  - extension traits, suffixed `Ext`.

  Never use a trait to organize code. Libraries prefer generics to `dyn`. `dyn` at an
  owner-approved boundary, such as a service trait across modules, is fine.
- **`#[non_exhaustive]` only on public enums and structs of library crates consumed outside the
  workspace.** Inside a workspace, keep enums closed and match them exhaustively without `_`
  (owner rule).
- **`#[must_use]` where ignoring the value is a bug:** pure getters, builders, guards, and
  error-carrying types that are not already `#[must_use]`.

### Naming

- **Constructors:** `new` and `new_with_*`. A fallible constructor is `try_new`. A conversion is
  `from_*`, paired with a `From` impl.
- **Conversion methods:** `as_`, `to_` and `into_`, each with its matching `self` convention.
- **Accessors:**
  - `foo()` and `foo_mut()`;
  - `set_foo()`, `take_foo()` and `replace_foo()`;
  - `with_foo()` for closure access behind interior mutability.
- **Lifetimes have descriptive names.** Avoid abbreviations an outsider would not recognize.

### Documentation

- Use `///` on public items, and `//!` for module and crate docs at the top of the file.
- Document panic conditions and the invariants of anything `unsafe`.
- A comment says what the code cannot say. Plain `//` is for implementation notes.

### Implementation

- **Write the type where inference hides it**, especially results of `.into()` and `.parse()`
  and long iterator chains. Prefer a typed `let` to a turbofish.
- **Use pattern matching rather than check-then-unwrap.**
- **Prefer a plain `for` loop to a long combinator chain with side effects.** Do not call
  `for_each` without a reason, and do not put a chain in a `for` header.
- **Use `format!` and `write!`** to join three or more parts and to fill templates.
- **No `unsafe` where safe code works.** Any `unsafe` sits behind a safe API with a
  `// SAFETY:` comment. Use `#![forbid(unsafe_code)]` where a crate needs none.
- **No unstable features, and no code before `main`** (the `ctor` crate).
- **An assertion carries a message** that says what was expected.

## Enforcement and migration

- **Lints.** Configure lints in `[workspace.lints]` or each crate's `[lints]`, on the pinned
  stable toolchain.
  - Clippy's defaults already cover `ptr_arg`, `borrowed_box`, `should_implement_trait`,
    `new_without_default`, `wrong_self_convention` and `len_without_is_empty`. Never allow them
    globally.
  - Add `wildcard_imports` and `undocumented_unsafe_blocks` as deny, and `needless_for_each` as
    warn.
  - Keep `unwrap_used` and `expect_used` as deny outside tests.
  - Library crates add `missing_docs` as warn.
  - Keep each repository's stricter lint set. Never weaken it to adopt the guide.
- **New code follows the guide fully.**
- **Existing code migrates in two ways:**
  - when you touch it;
  - in deliberate refactor passes that keep behavior, public and wire contracts, and tests
    unchanged, one area per commit.
- **No big-bang moves while other branches are in flight**: a refactor must not collide with
  parallel work.
- **Library public APIs change only with a deliberate semver decision.** This covers renames and
  added `#[non_exhaustive]`.
