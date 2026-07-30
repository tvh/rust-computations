//! Compile-fail coverage for `#[computation]`'s constraint-1 error cases
//! (see `computations_macros`' module docs): not `async`, first argument
//! not `&Ctx`, a `#[flow]` argument that isn't a reference to `Arc<T>`, and
//! a return type that isn't `Result<R, CompError>`.
//!
//! `.stderr` snapshots live alongside each fixture in `tests/ui/`.
//! Regenerate them (only after a deliberate wording change to this crate's
//! own error messages) with:
//!
//! ```text
//! TRYBUILD=overwrite cargo test -p computations --test computation_macro_compile_fail
//! ```
//!
//! Snapshot fragility across rustc versions is a known trybuild tradeoff —
//! see `docs/persistence-benchmark-notes.md`'s Stage 10 for why it was
//! still chosen over skipping compile-fail coverage entirely. One case
//! constraint 1 also describes -- a `#[flow]` argument correctly shaped as
//! `&Arc<T>` but where `T` implements neither `SourceBase` nor
//! `SinkBase` -- is deliberately NOT covered here: that failure surfaces as
//! an ordinary Rust trait-bound error whose exact wording is not this
//! crate's to pin down (see `computations::flow`'s `AsFlowId`/
//! `ResolveFlow` docs).
//!
//! Gated on the (default-on) `macros` feature: the fixtures use
//! `#[computation]` itself, which doesn't exist without it.
#![cfg(feature = "macros")]

#[test]
fn compile_fail() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/*.rs");
}
