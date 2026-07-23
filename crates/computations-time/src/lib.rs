//! `computations-time` provides a wall-clock time source for the
//! `computations` engine.
//!
//! [`TimeSource`] answers two requests: [`RoundedTime`] (the current time
//! rounded down to a [`Bucket`] granularity) and [`IsAfter`] (has wall-clock
//! time passed a given instant). Both correspond to the paper's built-in
//! `compGetTime` source, applied to a granularity such as 1min or 5min (see
//! the workspace README's API mapping table).
//!
//! No polling: a single background task sleeps until the next bucket
//! boundary or deadline actually due, recomputed fresh from the wall clock
//! on every wakeup so drift, suspend, or a jumped clock can't desync it —
//! see the [`source`] module docs for the full scheduling strategy.

pub mod source;

pub use source::{Bucket, InvalidBucket, IsAfter, RoundedTime, TimeKey, TimeSource, TimeVer};
