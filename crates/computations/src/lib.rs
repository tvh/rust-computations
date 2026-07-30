//! `computations` is a coarse-grained self-adjusting computation (SAC) engine.
//!
//! Named, memoized computations form a dynamic dependency graph; external data
//! sources report changes, and the engine propagates those changes forward,
//! re-running only the computations affected by them (with early cutoff when
//! a recomputed value is unchanged).

pub mod ctx;
pub mod def;
pub mod driver;
pub mod engine;
pub mod error;
pub mod flow;
mod hashers;
pub mod key;
pub mod persist;
pub mod registry;
pub mod sink;
pub mod source;

#[cfg(any(test, feature = "testutil"))]
pub mod testutil;

pub use ctx::Ctx;
pub use def::{Comp, CompDef, define_comp, define_comp_rec, define_comp_rec_with, define_comp_with};
pub use engine::{DirtyPriority, Engine, EngineBuilder};
pub use flow::{ComputationEntry, FlowId, FlowResolver, FlowThunk, FlowThunkFut};
pub use key::{CompKey, CompParam, CompResult, DefId, Hash128, StableHash};
pub use persist::{Fingerprint, PersistOptions};
pub use registry::Registry;
pub use sink::{OutBytes, RawOutput, Sink, SinkBase, SinkId};
pub use source::{Dep, KeyBytes, RawDep, Request, Source, SourceBase, SourceId, VerBytes};

// Re-exported so `#[computation]`-generated code can spell
// `computations::postcard`/`computations::inventory` without requiring the
// annotated crate to depend on either directly — only on `computations`
// itself. `postcard` is always available (a mandatory dependency of this
// crate); `inventory` only exists as a dependency at all when the `macros`
// feature is on, which is also the only time generated code referencing it
// can exist in the first place.
pub use postcard;
#[cfg(feature = "macros")]
pub use inventory;

/// The `#[computation]` proc-macro (Phase B — see
/// `docs/persistence-benchmark-notes.md`'s Stage 10 for the full design
/// rationale, and `crate::flow`'s module docs for the hand-written
/// foundation it generates into). Gated behind this crate's default-on
/// `macros` feature.
#[cfg(feature = "macros")]
pub use computations_macros::computation;
