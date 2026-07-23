//! `computations-fs` provides filesystem-backed sources and sinks for the
//! `computations` engine, plus a `dirsync` demo that keeps two directory
//! trees in sync using incremental, push-based recomputation.

pub mod sink;
pub mod source;
