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
pub use flow::{FlowId, FlowResolver, FlowThunk};
pub use key::{CompKey, CompParam, CompResult, DefId, Hash128, StableHash};
pub use persist::{Fingerprint, PersistOptions};
pub use registry::Registry;
pub use sink::{OutBytes, RawOutput, Sink, SinkBase, SinkId};
pub use source::{Dep, KeyBytes, RawDep, Request, Source, SourceBase, SourceId, VerBytes};
