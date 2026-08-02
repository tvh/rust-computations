//! Per-call-site lock-hold-time instrumentation for [`crate::engine`]'s
//! `Mutex<NodeTable>` (`nodes`) and `Mutex<HashMap<..>>` (`source_index`)
//! critical sections, gated at runtime by the `COMPUTATIONS_LOCK_STATS`
//! environment variable — never behind a cargo feature, unlike
//! `crate::alloc_stats`, since this is cheap enough to leave compiled in
//! unconditionally and only pay for when actually turned on.
//!
//! Ported from the Haskell reference engine's `COMP_ENGINE_LOCK_STATS`
//! (`src/Control/Computations/CompEngine/Run.hs`), which found that a
//! *single* `CompEngineStateIf` method (`capEvaluationFinished`) accounted
//! for 50–85% of that engine's total lock hold time — evidence a bare
//! aggregate ("62.5% of wall time is inside the lock") could never have
//! produced, and which is exactly the kind of attribution needed to decide
//! whether sharding this engine's own `Mutex<NodeTable>` is worth pursuing
//! (see Stage 6 of `docs/persistence-benchmark-notes.md`, which flagged
//! 0.7% direct lock-wait as a "watch item" with no way to say *which*
//! critical section dominates the lock-held work itself).
//!
//! # Design point carried over deliberately
//!
//! The Haskell version's load-bearing design choice is that the
//! enabled/disabled decision is made exactly once, at setup
//! (`setupSimpleStateIf` reads `COMP_ENGINE_LOCK_STATS` a single time and
//! bakes the choice into which closure gets built), never per call — a
//! per-acquisition environment check would itself perturb the very
//! hold-time baseline this exists to measure. This port keeps that
//! property: [`crate::engine::EngineInner::timed`] branches on a plain
//! `bool` field (`lock_stats_enabled`) read once in
//! [`crate::engine::EngineBuilder::build`], not on a fresh `std::env::var`
//! call per critical section. When disabled, every instrumented call site
//! pays exactly that one predictable branch and nothing else — no
//! `Instant::now`, no atomic write.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// One instrumented critical section. Each variant names a *semantic*
/// operation (matching the task's own examples: `record_call_dep`,
/// `remove_stale_source_index`, the liveness-GC sweep, dirty-priority
/// updates, `prepare`), not a raw `.lock()` call site — several of these
/// wrap a block that wasn't previously its own named function
/// (`EngineInner::run`'s post-execution bookkeeping in particular), and a
/// couple of semantic operations that acquire two different mutexes in
/// sequence (`record_source_deps`) get one variant per mutex, so a site's
/// nanoseconds are never a mix of two unrelated locks' hold times.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LockSite {
    /// `EngineInner::prepare` — cache-hit/dedup-join decision, or a fresh
    /// node's `NodeTable` row insert, under `nodes`.
    Prepare,
    /// `EngineInner::run` — recording the in-flight shared future under
    /// `nodes`, right before the body itself starts executing.
    RunSetInflight,
    /// `EngineInner::run` — writing a successful execution's result/hash/
    /// value and enqueuing it for persistence, under `nodes`.
    RunFinishSuccess,
    /// `EngineInner::run` — marking a failed execution's node dirty again,
    /// under `nodes`.
    RunFinishError,
    /// `EngineInner::record_call_dep` — pushing a caller/callee edge, under
    /// `nodes`.
    RecordCallDep,
    /// `EngineInner::record_source_deps`'s `src_key_interner`-locked half
    /// (Stage 13 — see `docs/persistence-benchmark-notes.md`): resolving
    /// each dep's raw `(source, key)` bytes to a `SrcKeyId`, run *before*
    /// either lock below so neither one ever hashes a byte vector.
    RecordSourceDepsIntern,
    /// `EngineInner::record_source_deps`'s `nodes`-locked half (extending
    /// the node's own recorded source deps).
    RecordSourceDepsNodes,
    /// `EngineInner::record_source_deps`'s `source_index`-locked half
    /// (registering the node against each interned key's `SrcKeyId`).
    RecordSourceDepsIndex,
    /// `EngineInner::record_outputs` — extending a node's recorded sink
    /// outputs, under `nodes`.
    RecordOutputs,
    /// `EngineInner::reconcile_source_deps` (Stage 13's renamed
    /// `remove_stale_source_index`) — retaining a fresh `src_key_interner`
    /// reference for every dependency identity a run just gained, then
    /// dropping `source_index` registrations and interner references for
    /// every one it no longer reads. Spans `src_key_interner` and
    /// `source_index`, held in sequence, never nested.
    RemoveStaleSourceIndex,
    /// `driver::EngineInner::affected_keys` — mapping a batch of changed
    /// source deps back to affected computations, under both `source_index`
    /// and `nodes` together (held jointly for the whole lookup). Resolving
    /// each dep's raw bytes to a `SrcKeyId` via `src_key_interner` happens
    /// first, in its own separate (and separately unmeasured, being cheap
    /// and low-volume here) critical section, so it never nests with either.
    AffectedKeys,
    /// `driver::EngineInner::mark_dirty_quiet` — the dirty-priority update
    /// itself ("max priority wins"), under `nodes`.
    MarkDirtyQuiet,
    /// `driver::EngineInner::split_by_tier` — reading each key's pending
    /// dirty priority to route it into the Input/Revalidate frontier, under
    /// `nodes`.
    SplitByTier,
    /// `driver::EngineInner::run_wave`'s first `nodes`-locked block:
    /// resolving the batch's node refs and snapshotting their param bytes
    /// before dispatching reruns.
    RunWaveDequeue,
    /// `driver::EngineInner::run_wave`'s second `nodes`-locked block:
    /// reading each rerun's outcome and computing the next frontier from
    /// changed nodes' rdeps.
    RunWaveRequeue,
    /// `driver::EngineInner::liveness_gc`'s mark-sweep: reachability scan,
    /// dead-node removal, and (nested, still under `nodes`) the
    /// `source_index` retain pass -- the GC sweep proper.
    LivenessGc,
    /// `driver::EngineInner::liveness_gc`'s later, separate `source_index`
    /// scan: deciding which now-unreferenced source keys to unregister.
    LivenessGcUnregister,
    /// `EngineInner::shrink_settled_defs` (Stage 22 — see
    /// `docs/persistence-benchmark-notes.md`): the per-def `DefTable`
    /// capacity-slack shrink pass run at each of `crate::driver`'s settle
    /// points, under `nodes`.
    ShrinkSettledDefs,
}

impl LockSite {
    const ALL: [LockSite; 18] = [
        LockSite::Prepare,
        LockSite::RunSetInflight,
        LockSite::RunFinishSuccess,
        LockSite::RunFinishError,
        LockSite::RecordCallDep,
        LockSite::RecordSourceDepsIntern,
        LockSite::RecordSourceDepsNodes,
        LockSite::RecordSourceDepsIndex,
        LockSite::RecordOutputs,
        LockSite::RemoveStaleSourceIndex,
        LockSite::AffectedKeys,
        LockSite::MarkDirtyQuiet,
        LockSite::SplitByTier,
        LockSite::RunWaveDequeue,
        LockSite::RunWaveRequeue,
        LockSite::LivenessGc,
        LockSite::LivenessGcUnregister,
        LockSite::ShrinkSettledDefs,
    ];

    fn name(self) -> &'static str {
        match self {
            LockSite::Prepare => "prepare",
            LockSite::RunSetInflight => "run/set_inflight",
            LockSite::RunFinishSuccess => "run/finish_success",
            LockSite::RunFinishError => "run/finish_error",
            LockSite::RecordCallDep => "record_call_dep",
            LockSite::RecordSourceDepsIntern => "record_source_deps/interner",
            LockSite::RecordSourceDepsNodes => "record_source_deps/nodes",
            LockSite::RecordSourceDepsIndex => "record_source_deps/source_index",
            LockSite::RecordOutputs => "record_outputs",
            LockSite::RemoveStaleSourceIndex => "remove_stale_source_index",
            LockSite::AffectedKeys => "affected_keys",
            LockSite::MarkDirtyQuiet => "mark_dirty_quiet",
            LockSite::SplitByTier => "split_by_tier",
            LockSite::RunWaveDequeue => "run_wave/dequeue",
            LockSite::RunWaveRequeue => "run_wave/requeue_rdeps",
            LockSite::LivenessGc => "liveness_gc/mark_sweep",
            LockSite::LivenessGcUnregister => "liveness_gc/unregister_source_keys",
            LockSite::ShrinkSettledDefs => "shrink_settled_defs",
        }
    }

    fn index(self) -> usize {
        self as usize
    }
}

/// One site's `(calls, total nanos)` accumulators.
#[derive(Default)]
struct SiteCounters {
    calls: AtomicU64,
    nanos: AtomicU64,
}

/// Per-[`LockSite`] accumulators. Always present on
/// [`crate::engine::EngineInner`] — [`LockSite::ALL`]`.len()` (17) pairs of
/// atomics, a few hundred bytes total, once per engine, never per node —
/// but only ever *written* to when `COMPUTATIONS_LOCK_STATS` was read as
/// enabled at `Engine::build()` time (see
/// `crate::engine::EngineInner::timed`), so an engine built without it set
/// pays nothing beyond the one bool check per instrumented call site.
pub(crate) struct LockStats {
    sites: [SiteCounters; LockSite::ALL.len()],
}

impl LockStats {
    pub(crate) fn new() -> Self {
        LockStats {
            sites: std::array::from_fn(|_| SiteCounters::default()),
        }
    }

    /// Records one call's elapsed time under `site`. `Relaxed`: this is
    /// pure statistics, not synchronization, and (unlike the allocator
    /// counters in `crate::alloc_stats`, which see one call per allocation)
    /// these accumulate at most once per lock acquisition on the already-
    /// serialized critical sections they wrap, so contention on the
    /// counters themselves was never expected to be a concern — confirmed
    /// by the overhead measurement in `docs/persistence-benchmark-notes.md`'s
    /// Stage 12.
    pub(crate) fn record(&self, site: LockSite, elapsed: Duration) {
        let counters = &self.sites[site.index()];
        counters.calls.fetch_add(1, Ordering::Relaxed);
        counters.nanos.fetch_add(elapsed.as_nanos() as u64, Ordering::Relaxed);
    }

    /// Sorted (descending by total hold time), formatted breakdown table —
    /// same shape as the Haskell reference engine's `COMP_ENGINE_LOCK_STATS`
    /// per-method table (`Run.hs`'s `printMethodStatsTable`), adapted to
    /// this engine's per-call-site critical sections. Sites with zero calls
    /// are omitted.
    pub(crate) fn report(&self) -> String {
        let mut rows: Vec<(&'static str, u64, u64)> = LockSite::ALL
            .iter()
            .map(|&site| {
                let counters = &self.sites[site.index()];
                (site.name(), counters.calls.load(Ordering::Relaxed), counters.nanos.load(Ordering::Relaxed))
            })
            .filter(|&(_, calls, _)| calls > 0)
            .collect();
        rows.sort_by(|a, b| b.2.cmp(&a.2));

        let total_nanos: u64 = rows.iter().map(|&(_, _, n)| n).sum();
        let total_calls: u64 = rows.iter().map(|&(_, c, _)| c).sum();

        let mut out = String::new();
        out.push_str("=== COMPUTATIONS_LOCK_STATS: per-call-site breakdown ===\n");
        out.push_str(&format!(
            "{:<36} {:>10} {:>14} {:>12} {:>10}\n",
            "site", "calls", "total (s)", "mean (ns)", "% of total"
        ));
        for (name, calls, nanos) in &rows {
            let mean_ns = if *calls == 0 { 0.0 } else { *nanos as f64 / *calls as f64 };
            let pct = if total_nanos == 0 { 0.0 } else { 100.0 * *nanos as f64 / total_nanos as f64 };
            out.push_str(&format!(
                "{:<36} {:>10} {:>14.6} {:>12.1} {:>9.2}%\n",
                name,
                calls,
                *nanos as f64 / 1e9,
                mean_ns,
                pct
            ));
        }
        out.push_str(&format!("{:<36} {:>10} {:>14.6}\n", "TOTAL", total_calls, total_nanos as f64 / 1e9));
        out
    }
}
