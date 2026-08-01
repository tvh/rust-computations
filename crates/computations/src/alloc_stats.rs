//! Per-phase allocated/deallocated-byte accounting, gated entirely behind
//! the off-by-default `alloc-stats` cargo feature (`Cargo.toml`) so an
//! ordinary build or benchmark run is bit-for-bit unaffected — every number
//! in `docs/persistence-benchmark-notes.md` prior to Stage 12 was measured
//! without this feature enabled, and must stay comparable.
//!
//! Ported from the Haskell reference engine's Stage 12
//! (`bench/Control/Computations/Demos/Bench/{Main,Hospital}.hs`, reading
//! `GHC.Stats.getRTSStats`'s `allocated_bytes`): RSS/`max_live_bytes` only
//! ever show the *peak* resident/live set, which is blind to a fix that
//! reduces total churn (allocate-then-immediately-free traffic) without
//! moving the peak at all — the Haskell doc's own Stage 10 found exactly
//! such a fix. `allocated_bytes` is program-driven (a running counter of
//! every byte the allocator has ever handed out), not GC/RSS-driven, so for
//! a fixed program and input it is far more stable run to run than wall
//! time, which this codebase has independently observed to vary well
//! beyond the 5–15% effect sizes these benchmarks exist to detect.
//!
//! [`CountingAlloc`] is installed as this crate's `#[global_allocator]`
//! (see `lib.rs`) only when `alloc-stats` is enabled; [`snapshot`] reads the
//! two running totals at a phase boundary, and [`AllocSnapshot::delta`]
//! turns two snapshots into that phase's own allocation, independent of
//! when GC/OS-level reclamation happens to run.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};

/// Process-wide running totals, incremented by every [`CountingAlloc`] call.
/// `Relaxed` throughout: these are independent counters, not
/// synchronization, and at this crate's ~1M-instance benchmark scale a
/// single global pair proved cheap enough not to need sharding or
/// per-thread counters (see `docs/persistence-benchmark-notes.md`'s Stage
/// 12 for the measured overhead) — worth revisiting only if a future,
/// far-more-allocation-heavy workload shows otherwise.
static ALLOCATED: AtomicU64 = AtomicU64::new(0);
static DEALLOCATED: AtomicU64 = AtomicU64::new(0);

/// A `GlobalAlloc` that delegates every operation to [`System`] and counts
/// bytes allocated/deallocated along the way. Zero-cost when the
/// `alloc-stats` feature is off (the type doesn't even exist then — see
/// `lib.rs`'s conditional `#[global_allocator]`).
pub struct CountingAlloc;

// SAFETY: every method delegates directly to `System`, which is itself a
// correct `GlobalAlloc`; the only addition is a `Relaxed` atomic add before
// or after the delegated call, which touches no allocator state and cannot
// affect its safety contract.
unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATED.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        DEALLOCATED.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        ALLOCATED.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // A realloc that grows or shrinks in place changes live bytes by
        // the same delta a dealloc+alloc pair would, whether or not the
        // allocator actually moves the block -- attribute it that way so
        // ALLOCATED/DEALLOCATED stay a faithful decomposition of net bytes
        // live, not an artifact of how `System::realloc` happens to be
        // implemented on this platform.
        match new_size.cmp(&layout.size()) {
            std::cmp::Ordering::Greater => {
                ALLOCATED.fetch_add((new_size - layout.size()) as u64, Ordering::Relaxed);
            }
            std::cmp::Ordering::Less => {
                DEALLOCATED.fetch_add((layout.size() - new_size) as u64, Ordering::Relaxed);
            }
            std::cmp::Ordering::Equal => {}
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

/// A `(allocated_bytes, deallocated_bytes)` snapshot of the process-wide
/// running totals, taken at a phase boundary via [`snapshot`].
#[derive(Clone, Copy, Debug)]
pub struct AllocSnapshot {
    pub allocated: u64,
    pub deallocated: u64,
}

/// This phase's own allocation, computed as the difference between two
/// snapshots (see [`AllocSnapshot::delta`]) rather than either snapshot's
/// absolute value — a cumulative total would bury a later phase's own
/// (possibly much smaller) allocation under every earlier phase's.
#[derive(Clone, Copy, Debug)]
pub struct AllocDelta {
    pub allocated: u64,
    pub deallocated: u64,
}

impl AllocSnapshot {
    /// `later`'s totals minus `self`'s — the bytes allocated/deallocated
    /// strictly between the two snapshots.
    pub fn delta(&self, later: &AllocSnapshot) -> AllocDelta {
        AllocDelta {
            allocated: later.allocated.saturating_sub(self.allocated),
            deallocated: later.deallocated.saturating_sub(self.deallocated),
        }
    }
}

impl AllocDelta {
    /// `allocated - deallocated` over this delta's window: the net change
    /// in live bytes, which can be negative (a phase that frees more than
    /// it allocates, e.g. a GC-heavy round) — hence `i64`, not `u64`.
    pub fn net(&self) -> i64 {
        self.allocated as i64 - self.deallocated as i64
    }
}

/// Reads the current process-wide allocated/deallocated totals. Call once
/// before and once after the phase being measured; the difference (via
/// [`AllocSnapshot::delta`]) is that phase's own allocation.
pub fn snapshot() -> AllocSnapshot {
    AllocSnapshot {
        allocated: ALLOCATED.load(Ordering::Relaxed),
        deallocated: DEALLOCATED.load(Ordering::Relaxed),
    }
}
