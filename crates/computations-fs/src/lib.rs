//! `computations-fs` provides filesystem-backed sources and sinks for the
//! `computations` engine.
//!
//! [`source::FsSource`] reads files ([`source::ReadFile`]) and lists
//! directories ([`source::ListDir`]), watching every path it has served so
//! the engine can be woken up when the filesystem changes underneath it.
//! [`sink::FsSink`] writes files ([`sink::WriteFile`]) and creates
//! directories ([`sink::MakeDirs`]) under a fixed root, reporting each write
//! as an output so the engine can garbage-collect outputs whose producing
//! computation died.
//!
//! Both are built entirely against `computations`'s public
//! [`Source`](computations::Source)/[`Sink`](computations::Sink) plugin
//! traits — this crate is the proof that the plugin interface works for an
//! external, real-world backend.

pub mod sink;
pub mod source;

pub use sink::{FsSink, MakeDirs, WriteFile};
pub use source::{DirEntry, EntryKind, FsSource, FsVer, ListDir, ReadFile, canonical_key};
