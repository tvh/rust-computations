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
pub mod key;
pub mod sink;
pub mod source;

#[cfg(any(test, feature = "testutil"))]
pub mod testutil;
