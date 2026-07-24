//! Scale benchmark for opt-in dependency-graph persistence (`computations::persist`).
//!
//! Builds a synthetic 10-level, 50-definition dependency graph (~1,000,000
//! computation applications at the default scale), then measures, with
//! plain `std::time::Instant` timers:
//!
//! 1. a cold initial evaluation with persistence configured against an empty
//!    database;
//! 2. an explicit `persist_now()` flush, plus the resulting database file
//!    size;
//! 3. a warm restart against the same database with no input changes
//!    (expect a near-total cache hit: ~0 reruns);
//! 4. a restart with exactly one changed source key (expect only that
//!    key's dependents to re-run);
//! 5. a cold restart baseline with persistence *not* configured at all (the
//!    "without persistence" comparison);
//! 6. a restart after a fingerprint change (expect every restored node to
//!    be revalidated, but early cutoff to still avoid cascading further);
//! 7. a *live*, steady-state incremental update on an already-running
//!    engine with persistence *not* configured: settle, mutate exactly one
//!    `MemKvSource` key, and time until the driver's next round completes
//!    (no restart involved — this is "how fast a single change propagates"
//!    with persistence out of the picture entirely); and
//! 8. the same live, steady-state incremental measurement with persistence
//!    *configured* on the running engine. Saving is decoupled from
//!    propagation (see `crate::persist`'s background persister task): a
//!    round's settle latency no longer waits on a flush, so phase 8's
//!    settle time should now land close to phase 7's. Each of phases 7 and
//!    8 additionally reports a second, "time-to-durable" `RESULT` line —
//!    that same settle latency plus an explicit, awaited `persist_now()`
//!    right after — making the (now fully separate) flush cost visible on
//!    its own.
//!
//! Run with:
//! ```text
//! cargo run -p computations --release --example persist_bench --features testutil
//! ```
//!
//! Scale is configurable via the `PERSIST_BENCH_SCALE` environment
//! variable: a float multiplier applied to every entry of the level-size
//! vector (see [`BASE_LEVEL_SIZES`]). The default, `1.0` (used when the
//! variable is unset or fails to parse), is the ~1,000,000-instance
//! configuration this module is tuned for; smaller values (e.g. `0.05`) are
//! useful for a quick sanity/compile check before committing to a full run.
//!
//! ## Process-per-phase architecture
//!
//! An earlier version of the engine gave every node a permanent, by-design
//! self-reference back to its own `EngineInner` (a `rerun` closure
//! capturing `Arc<EngineInner>` — see `crate::engine::Node`'s old `rerun`
//! field and `crate::driver::Engine::persist_close`'s doc comment): an
//! `Engine` was, in practice, never fully freed once any of its nodes had
//! run, for the rest of the process's life. The Tier-1 memory redesign
//! (see `crate::engine::EngineInner::rerun_node`) removed that cycle — an
//! `Engine` now genuinely drops once every handle to it is gone (see
//! `crate::engine::tests::engine_is_droppable_after_a_rerun`) — but this
//! benchmark still runs each phase in its own child process rather than
//! restructuring around that: it builds *ten* separate ~1,000,000-node
//! graphs in sequence, and process-per-phase keeps each phase's reported
//! peak RSS a meaningful "how much does *one* phase cost" figure,
//! comparable across every stage of this benchmark's history, rather than
//! an artifact of how many earlier phases happened to run first in the
//! same process.
//!
//! So each timed phase (or, for phase 3/4/5's paired trials, each trial)
//! runs in its own child process: [`main`] with no `PERSIST_BENCH_PHASE`
//! env var set acts as an orchestrator that re-execs itself
//! (`std::env::current_exe()`) once per phase with that variable set to the
//! phase's id, capturing each child's stdout (echoed live for visibility)
//! and parsing the `RESULT|...` lines described below out of it. Every
//! child that depends on `crate::persist` state left behind by an earlier
//! phase (3, 4, 6) points at the same on-disk database, passed via
//! `PERSIST_BENCH_DIR`; since the in-memory `MemKvSource` isn't persisted,
//! each such child replays whatever key mutations preceded it on the
//! original single-process timeline (see `make_kv`) so the versions probed
//! against the persisted graph line up exactly as they would have if
//! everything really had run in one process. This also makes the reported
//! peak RSS a meaningful "how much does *one* phase cost" figure rather
//! than an artifact of how many earlier phases happened to run first.
//!
//! ## Detecting "settled"
//!
//! `Engine::run` never returns (see its own docs): it performs the initial
//! evaluation, then loops forever. To time "until settled" from outside the
//! spawned task without depending on the top-level sink docs changing
//! (which they won't, on a pure cache-hit restart), each worker process
//! installs two tiny `tracing_subscriber::Layer`s that watch for
//! unconditional log events emitted by `crate::driver`: `"initial
//! evaluation complete"` (emitted once, right after the initial `eval_root`
//! call resolves) and `"propagation round complete"` (emitted once per
//! completed change-propagation round, i.e. once per iteration of
//! `Engine::run`'s main loop — used by phases 7 and 8 to time a single live
//! incremental update). Each phase records the relevant counter's value
//! before triggering work, then polls until it has advanced.
//!
//! ## Peak memory
//!
//! [`rss_mb`] shells out to `ps -o rss= -p <this process>` (the simplest
//! reliable way to read RSS from within the process on macOS/Linux without
//! extra dependencies) and is called at the end of every phase; the report
//! prints each phase's RSS, and the orchestrator prints the peak observed
//! across every child process.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use computations::testutil::{GetKey, MemKvSource, VecSink, WriteDoc};
use computations::{Comp, Engine, Fingerprint, PersistOptions, Registry};

use tracing::{Event, Subscriber};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::prelude::*;
use tracing_subscriber::registry::LookupSpan;

/// Number of dependency levels (level 0 reads sources; the last level is
/// the "top" level whose instances write to the sink).
const LEVELS: usize = 10;
/// Definitions per level; `LEVELS * DEFS_PER_LEVEL` is the total def count.
const DEFS_PER_LEVEL: usize = 5;
/// Distinct source keys level-0 instances read from (`index % SRC_KEYS`).
const SRC_KEYS: u32 = 300;
/// Fixed fan-in: how many level-`L-1` instances each level-`L` (`L > 0`)
/// instance folds together.
const FAN_IN: usize = 3;

/// Instances per level, bottom (level 0) to top (level 9), at
/// `PERSIST_BENCH_SCALE=1.0` (the default).
///
/// Front-loaded rather than flat: with a fixed fan-in of 3 spreading via
/// modular arithmetic from a fixed 1000 top-level writers, the number of
/// instances actually *reachable* at a level near the top is bounded by a
/// budget cascading down from the top (roughly `3x` the level above's
/// reachable count, minus collisions) regardless of that level's declared
/// size — so levels 5 through 8 are sized just above their natural
/// reachable ceiling (~2,700 / 8,200 / 24,000 / 71,000, empirically), while
/// levels 0 through 4 are sized large enough to be reached (and nearly
/// fully saturated) by the budget flowing in from level 5. These exact
/// numbers were tuned by simulating the same modular-arithmetic fan-in
/// formula used below (not the live engine, which is far slower to iterate
/// on) against candidate vectors until the total achieved count landed
/// within a fraction of a percent of 1,000,000. The actual achieved count
/// is measured at runtime (see `run_phase_1`), not assumed, and printed at
/// the top of the report.
const BASE_LEVEL_SIZES: [u32; LEVELS] = [
    205_000, 205_000, 205_000, 205_000, 205_000, 128_945, 101_885, 61_090, 27_060, 1_000,
];

/// Reads `PERSIST_BENCH_SCALE` (a float multiplier on [`BASE_LEVEL_SIZES`];
/// defaults to `1.0`, the ~1,000,000-instance configuration, if the
/// variable is unset or fails to parse) and returns the scaled level-size
/// vector, each entry floored at 1.
fn scaled_level_sizes() -> [u32; LEVELS] {
    let scale: f64 = std::env::var("PERSIST_BENCH_SCALE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1.0);
    let mut sizes = [0u32; LEVELS];
    for (i, &base) in BASE_LEVEL_SIZES.iter().enumerate() {
        sizes[i] = ((base as f64 * scale).round().max(1.0)) as u32;
    }
    sizes
}

/// One level's 5 definitions. `Comp<P, R>` is a cheap `Copy` handle, so this
/// whole array is `Copy` too — it can be captured by value in as many
/// per-definition closures as needed with no `Arc` or cloning required.
type Level = [Comp<u32, u64>; DEFS_PER_LEVEL];

/// Builds the full 50-definition, 10-level graph plus its root, wiring in
/// `kv`/`sink` and (optionally) persistence. `run_counter` is incremented
/// once per computation body invocation (every level's defs, plus root),
/// so the caller can read off exactly how many computations actually ran
/// during a given phase. `level_sizes` is the (possibly scaled) per-level
/// instance count vector — see [`scaled_level_sizes`].
fn build_graph(
    kv: &Arc<MemKvSource>,
    sink: &Arc<VecSink>,
    persist_opts: Option<PersistOptions>,
    run_counter: &Arc<AtomicUsize>,
    level_sizes: &[u32; LEVELS],
) -> (Engine, Comp<(), ()>) {
    let mut registry = Registry::default();
    registry.register_source(kv.clone());
    registry.register_sink(sink.clone());

    let mut builder = Engine::builder();
    builder.registry(registry);
    if let Some(opts) = persist_opts {
        builder.persistence(opts);
    }

    let level_sizes = *level_sizes;
    let mut levels: Vec<Level> = Vec::with_capacity(LEVELS);

    for level in 0..LEVELS {
        let is_top = level == LEVELS - 1;
        let prev: Option<Level> = if level == 0 {
            None
        } else {
            Some(levels[level - 1])
        };
        let prev_size = if level == 0 { 0 } else { level_sizes[level - 1] };

        let mut defs: Vec<Comp<u32, u64>> = Vec::with_capacity(DEFS_PER_LEVEL);
        for d in 0..DEFS_PER_LEVEL {
            let name: &'static str = Box::leak(format!("l{level}_d{d}").into_boxed_str());
            let d_u64 = d as u64;
            let counter = run_counter.clone();

            let comp: Comp<u32, u64> = if level == 0 {
                let kv_for_def = kv.clone();
                builder.define(name, move |ctx, i: u32| {
                    let kv = kv_for_def.clone();
                    let counter = counter.clone();
                    async move {
                        counter.fetch_add(1, Ordering::Relaxed);
                        let key = (i % SRC_KEYS).to_string();
                        let raw = ctx.src_req(&kv, GetKey(key)).await?;
                        let base: u64 = raw.and_then(|s| s.parse::<u64>().ok()).unwrap_or(0);
                        Ok(base.wrapping_add(i as u64).wrapping_add(d_u64))
                    }
                })
            } else {
                let prev_defs = prev.expect("prev level exists for level > 0");
                let sink_for_def = sink.clone();
                builder.define(name, move |ctx, i: u32| {
                    let sink = sink_for_def.clone();
                    let counter = counter.clone();
                    async move {
                        counter.fetch_add(1, Ordering::Relaxed);
                        let c0 = (i.wrapping_mul(2).wrapping_add(1)) % prev_size;
                        let c1 = (i.wrapping_mul(7).wrapping_add(13)) % prev_size;
                        let c2 = (i.wrapping_mul(31).wrapping_add(101)) % prev_size;
                        debug_assert_eq!(FAN_IN, 3, "fan-in edges above must match FAN_IN");

                        let r0 = ctx
                            .eval(prev_defs[c0 as usize % DEFS_PER_LEVEL], c0)
                            .await?;
                        let r1 = ctx
                            .eval(prev_defs[c1 as usize % DEFS_PER_LEVEL], c1)
                            .await?;
                        let r2 = ctx
                            .eval(prev_defs[c2 as usize % DEFS_PER_LEVEL], c2)
                            .await?;

                        let result = r0
                            .wrapping_add(r1)
                            .wrapping_add(r2)
                            .wrapping_add(d_u64)
                            .wrapping_add(i as u64);

                        if is_top {
                            ctx.sink_req(
                                &sink,
                                WriteDoc {
                                    name: format!("doc_{i}"),
                                    content: result.to_string(),
                                },
                            )
                            .await?;
                        }

                        Ok(result)
                    }
                })
            };
            defs.push(comp);
        }
        let arr: Level = defs.try_into().expect("exactly DEFS_PER_LEVEL defs built");
        levels.push(arr);
    }

    let top = levels[LEVELS - 1];
    let top_size = level_sizes[LEVELS - 1];
    let root_counter = run_counter.clone();

    let root: Comp<(), ()> = builder.define("root", move |ctx, _: ()| {
        let counter = root_counter.clone();
        async move {
            counter.fetch_add(1, Ordering::Relaxed);
            let mut groups: [Vec<u32>; DEFS_PER_LEVEL] = Default::default();
            for i in 0..top_size {
                groups[i as usize % DEFS_PER_LEVEL].push(i);
            }
            let futs = (0..DEFS_PER_LEVEL).map(|d| ctx.eval_all(top[d], groups[d].clone()));
            futures::future::try_join_all(futs).await?;
            Ok(())
        }
    });

    (builder.build(), root)
}

/// Populates the standard 300 default keys (`k -> k*7+3`), then optionally
/// replays the phase-4 mutations (key `"150"`, then key `"151"`, each set
/// to `"999999"`) — whichever prefix of that history the calling phase
/// needs to reproduce the same `MemKvSource` version numbers the original
/// single-process timeline would have produced by that point. See the
/// module-level "process-per-phase architecture" docs for why this replay
/// is necessary.
async fn make_kv(mutate_150: bool, mutate_151: bool) -> Arc<MemKvSource> {
    let kv = MemKvSource::new("kv");
    for k in 0..SRC_KEYS {
        kv.set(&k.to_string(), &(k as u64 * 7 + 3).to_string()).await;
    }
    if mutate_150 {
        kv.set("150", "999999").await;
    }
    if mutate_151 {
        kv.set("151", "999999").await;
    }
    kv
}

/// A `tracing_subscriber::Layer` that bumps `counter` every time it sees an
/// event whose rendered message contains `needle` — the only externally
/// observable way to detect either "the initial evaluation has settled" or
/// "one live propagation round just completed" from outside the spawned
/// `Engine::run` task (see the module docs' "Detecting settled" section).
struct MessageSignal {
    needle: &'static str,
    counter: Arc<AtomicU64>,
}

impl<S> Layer<S> for MessageSignal
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        struct MessageVisitor(String);
        impl tracing::field::Visit for MessageVisitor {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                if field.name() == "message" {
                    self.0 = format!("{value:?}");
                }
            }
        }
        let mut visitor = MessageVisitor(String::new());
        event.record(&mut visitor);
        if visitor.0.contains(self.needle) {
            self.counter.fetch_add(1, Ordering::SeqCst);
        }
    }
}

/// Polls `signal` until it has advanced past `baseline`, panicking if
/// `timeout` passes first (per the task's caution: a phase pathologically
/// stuck should fail loudly, not hang the benchmark). Used both for
/// "initial evaluation complete" and "propagation round complete" signals —
/// the wait logic is identical, only which counter is passed in differs.
async fn wait_for_signal(signal: &Arc<AtomicU64>, baseline: u64, timeout: Duration) {
    tokio::time::timeout(timeout, async {
        loop {
            if signal.load(Ordering::SeqCst) > baseline {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("engine did not report the expected signal within the timeout");
}

/// Shells out to `ps -o rss= -p <this process>` and returns resident set
/// size in MB (0.0 if `ps` is unavailable or its output doesn't parse —
/// this is a best-effort diagnostic, never load-bearing for correctness).
fn rss_mb() -> f64 {
    let pid = std::process::id().to_string();
    let output = std::process::Command::new("ps").args(["-o", "rss=", "-p", &pid]).output();
    match output {
        Ok(o) if o.status.success() => {
            String::from_utf8_lossy(&o.stdout).trim().parse::<f64>().map(|kb| kb / 1024.0).unwrap_or(0.0)
        }
        _ => 0.0,
    }
}

/// Prints one `RESULT|label|ms|reruns|rss_mb` line to stdout — the
/// orchestrator's only contract with each worker process (see the module
/// docs).
fn report(label: &str, ms: u128, reruns: usize) {
    let rss = rss_mb();
    println!("RESULT|{label}|{ms}|{reruns}|{rss:.1}");
}

struct PhaseResult {
    label: String,
    ms: u128,
    reruns: usize,
    rss_mb: f64,
}

fn main() {
    match std::env::var("PERSIST_BENCH_PHASE") {
        Ok(phase) => {
            // Worker mode: build and time exactly one phase (or trial) in
            // this process, then exit — letting the OS reclaim every
            // permanently-self-referential `Engine` this process built
            // (see the module docs) rather than accumulating them across
            // every phase in one process.
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("build tokio runtime")
                .block_on(worker_main(&phase));
        }
        Err(_) => orchestrator_main(),
    }
}

/// Spawns one child process per phase (see the module docs), each with
/// `PERSIST_BENCH_PHASE` set to that phase's id and `PERSIST_BENCH_DIR`
/// pointing at a shared temp directory holding the on-disk database that
/// phases 3/4/6 restart against. Parses each child's `RESULT|...` lines
/// into the final summary table.
fn orchestrator_main() {
    let level_sizes = scaled_level_sizes();
    let scale: f64 = std::env::var("PERSIST_BENCH_SCALE").ok().and_then(|s| s.parse().ok()).unwrap_or(1.0);
    let target_instances = 1_000_000.0 * scale;

    println!("=== persist_bench: graph shape ===");
    println!("PERSIST_BENCH_SCALE={scale} (target ~{target_instances:.0} achieved instances)");
    println!(
        "levels: {LEVELS}, defs/level: {DEFS_PER_LEVEL}, total defs: {}",
        LEVELS * DEFS_PER_LEVEL
    );
    println!("fixed fan-in: {FAN_IN}");
    println!("declared level sizes (bottom -> top): {level_sizes:?}");
    println!(
        "declared total (sum of level sizes + root): {}",
        level_sizes.iter().sum::<u32>() as u64 + 1
    );
    println!(
        "source keys: {SRC_KEYS}, top-level sink outputs: {}",
        level_sizes[LEVELS - 1]
    );
    println!();

    let exe = std::env::current_exe().expect("resolve current executable path for re-exec");
    let dir = tempfile::tempdir().expect("create shared temp dir for persist_bench databases");

    let phase_ids = ["1", "3-1", "3-2", "4-1", "4-2", "5-1", "5-2", "6", "7", "8"];

    let mut results: Vec<PhaseResult> = Vec::new();
    let mut peak_rss: f64 = 0.0;

    for phase_id in phase_ids {
        println!("--- spawning worker for phase {phase_id} ---");
        let output = Command::new(&exe)
            .env("PERSIST_BENCH_PHASE", phase_id)
            .env("PERSIST_BENCH_DIR", dir.path())
            .output()
            .unwrap_or_else(|e| panic!("failed to spawn worker for phase {phase_id}: {e}"));

        let stdout = String::from_utf8_lossy(&output.stdout);
        print!("{stdout}");
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            panic!("worker for phase {phase_id} exited with {}; stderr:\n{stderr}", output.status);
        }

        for line in stdout.lines() {
            let Some(rest) = line.strip_prefix("RESULT|") else { continue };
            let parts: Vec<&str> = rest.split('|').collect();
            let [label, ms, reruns, rss] = parts.as_slice() else {
                panic!("worker for phase {phase_id} emitted a malformed RESULT line: {line}");
            };
            let ms: u128 = ms.parse().unwrap_or_else(|e| panic!("bad ms in RESULT line {line}: {e}"));
            let reruns: usize = reruns.parse().unwrap_or_else(|e| panic!("bad reruns in RESULT line {line}: {e}"));
            let rss: f64 = rss.parse().unwrap_or_else(|e| panic!("bad rss in RESULT line {line}: {e}"));
            peak_rss = peak_rss.max(rss);
            results.push(PhaseResult {
                label: label.to_string(),
                ms,
                reruns,
                rss_mb: rss,
            });
        }
    }

    println!();
    println!("=== summary ===");
    println!(
        "{:<58} {:>12} {:>10} {:>10}",
        "phase", "time (ms)", "reruns", "RSS (MB)"
    );
    for r in &results {
        println!("{:<58} {:>12} {:>10} {:>10.1}", r.label, r.ms, r.reruns, r.rss_mb);
    }
    println!();
    println!("peak RSS observed across phases (max of each isolated worker process): {peak_rss:.1} MB");
}

/// Runs the single phase (or trial) named by `phase_id` in this (worker)
/// process, printing `RESULT|...` line(s) for the orchestrator to parse.
async fn worker_main(phase_id: &str) {
    let settle_signal = Arc::new(AtomicU64::new(0));
    let round_signal = Arc::new(AtomicU64::new(0));
    tracing_subscriber::registry()
        .with(MessageSignal {
            needle: "initial evaluation complete",
            counter: settle_signal.clone(),
        })
        .with(MessageSignal {
            needle: "propagation round complete",
            counter: round_signal.clone(),
        })
        .init();

    // Generous, but bounded: a phase that hasn't settled within 5 minutes
    // is, per this benchmark's own operating instructions, a sign to stop
    // and diagnose rather than let the run hang indefinitely.
    let phase_timeout = Duration::from_secs(300);

    let level_sizes = scaled_level_sizes();
    let dir = PathBuf::from(
        std::env::var("PERSIST_BENCH_DIR").expect("PERSIST_BENCH_DIR must be set by the orchestrator"),
    );
    let db_path: PathBuf = dir.join("persist_bench.redb");

    let fingerprint_v1 = Fingerprint::custom("persist-bench-v1");
    let fingerprint_v2 = Fingerprint::custom("persist-bench-v2");

    match phase_id {
        "1" => run_phase_1(&level_sizes, &db_path, fingerprint_v1, &settle_signal, phase_timeout).await,
        "3-1" => {
            run_restart_phase(
                "3. warm restart, no changes (trial 1)",
                &level_sizes,
                &db_path,
                fingerprint_v1,
                false,
                false,
                &settle_signal,
                phase_timeout,
            )
            .await
        }
        "3-2" => {
            run_restart_phase(
                "3. warm restart, no changes (trial 2)",
                &level_sizes,
                &db_path,
                fingerprint_v1,
                false,
                false,
                &settle_signal,
                phase_timeout,
            )
            .await
        }
        "4-1" => {
            run_restart_phase(
                "4. restart, 1 changed input (trial 1, key=150)",
                &level_sizes,
                &db_path,
                fingerprint_v1,
                true,
                false,
                &settle_signal,
                phase_timeout,
            )
            .await
        }
        "4-2" => {
            run_restart_phase(
                "4. restart, 1 changed input (trial 2, key=151)",
                &level_sizes,
                &db_path,
                fingerprint_v1,
                true,
                true,
                &settle_signal,
                phase_timeout,
            )
            .await
        }
        "5-1" => {
            run_no_persist_phase("5. cold restart, no persistence (trial 1)", &level_sizes, &settle_signal, phase_timeout).await
        }
        "5-2" => {
            run_no_persist_phase("5. cold restart, no persistence (trial 2)", &level_sizes, &settle_signal, phase_timeout).await
        }
        "6" => run_fingerprint_mismatch_phase(&level_sizes, &db_path, fingerprint_v2, &settle_signal, phase_timeout).await,
        "7" => {
            run_live_incremental_phase(
                "7. live incremental, 1 changed input (no persistence)",
                &level_sizes,
                None,
                &settle_signal,
                &round_signal,
                phase_timeout,
            )
            .await
        }
        "8" => {
            let db_path8 = dir.join("persist_bench_live.redb");
            let opts8 = PersistOptions::new(db_path8, fingerprint_v1);
            run_live_incremental_phase(
                "8. live incremental, 1 changed input (WITH persistence)",
                &level_sizes,
                Some(opts8),
                &settle_signal,
                &round_signal,
                phase_timeout,
            )
            .await
        }
        other => panic!("unknown PERSIST_BENCH_PHASE: {other}"),
    }
}

/// Phase 1 + 2 combined: cold initial eval against an empty, freshly
/// created database, then an explicit `persist_now()` flush and the
/// resulting file size. These two must share one process because phase 2
/// needs `persist_now()` called on the still-live `Engine` phase 1 just
/// built.
async fn run_phase_1(
    level_sizes: &[u32; LEVELS],
    db_path: &Path,
    fingerprint_v1: Fingerprint,
    settle_signal: &Arc<AtomicU64>,
    phase_timeout: Duration,
) {
    let target_instances =
        1_000_000.0 * std::env::var("PERSIST_BENCH_SCALE").ok().and_then(|s| s.parse().ok()).unwrap_or(1.0);

    let kv = make_kv(false, false).await;
    let sink = VecSink::new("docs");

    let counter1 = Arc::new(AtomicUsize::new(0));
    let opts1 = PersistOptions::new(db_path, fingerprint_v1);
    let (engine1, root1) = build_graph(&kv, &sink, Some(opts1), &counter1, level_sizes);

    let baseline = settle_signal.load(Ordering::SeqCst);
    let t0 = Instant::now();
    let handle1 = {
        let e = engine1.clone();
        tokio::spawn(async move { e.run(root1, ()).await })
    };
    wait_for_signal(settle_signal, baseline, phase_timeout).await;
    let phase1_ms = t0.elapsed().as_millis();
    let phase1_reruns = counter1.load(Ordering::Relaxed);
    eprintln!(
        "  -> achieved instance count: {phase1_reruns} (target ~{target_instances:.0}, {:+.1}%)",
        (phase1_reruns as f64 - target_instances) / target_instances * 100.0
    );
    assert_eq!(
        sink.names().len(),
        level_sizes[LEVELS - 1] as usize,
        "every top-level doc must have been written"
    );
    report("1. cold initial eval (persistence configured)", phase1_ms, phase1_reruns);

    let t1 = Instant::now();
    engine1.persist_now().await;
    let phase2_ms = t1.elapsed().as_millis();
    let db_size_bytes = std::fs::metadata(db_path).map(|m| m.len()).unwrap_or(0);
    report(
        &format!("2. persist_now [db={:.2} MB]", db_size_bytes as f64 / 1_000_000.0),
        phase2_ms,
        0,
    );

    engine1.persist_close();
    handle1.abort();
    let _ = handle1.await;
}

/// Phases 3 and 4: restart against the existing `db_path`, with `kv`
/// replayed up through whichever of the phase-4 mutations (`mutate_150`,
/// `mutate_151`) precede this trial on the original timeline. Re-saves
/// (`persist_now` + `persist_close`) afterward so a later phase restarting
/// against the same database sees this trial's result, exactly as the
/// original single-process version did.
#[allow(clippy::too_many_arguments)]
async fn run_restart_phase(
    label: &str,
    level_sizes: &[u32; LEVELS],
    db_path: &Path,
    fingerprint: Fingerprint,
    mutate_150: bool,
    mutate_151: bool,
    settle_signal: &Arc<AtomicU64>,
    phase_timeout: Duration,
) {
    let kv = make_kv(mutate_150, mutate_151).await;
    let sink = VecSink::new("docs");

    let counter = Arc::new(AtomicUsize::new(0));
    let opts = PersistOptions::new(db_path, fingerprint);
    let (engine, root) = build_graph(&kv, &sink, Some(opts), &counter, level_sizes);

    let baseline = settle_signal.load(Ordering::SeqCst);
    let t = Instant::now();
    let handle = {
        let e = engine.clone();
        tokio::spawn(async move { e.run(root, ()).await })
    };
    wait_for_signal(settle_signal, baseline, phase_timeout).await;
    let ms = t.elapsed().as_millis();
    let reruns = counter.load(Ordering::Relaxed);
    report(label, ms, reruns);

    engine.persist_now().await;
    engine.persist_close();
    handle.abort();
    let _ = handle.await;
}

/// Phase 5: cold restart baseline with persistence not configured at all.
/// Replays the full phase-4 mutation history for timeline fidelity, though
/// (since there is no persisted state to compare against) it has no effect
/// on the measured timing or rerun count — every node always reruns cold.
async fn run_no_persist_phase(
    label: &str,
    level_sizes: &[u32; LEVELS],
    settle_signal: &Arc<AtomicU64>,
    phase_timeout: Duration,
) {
    let kv = make_kv(true, true).await;
    let sink = VecSink::new("docs");

    let counter = Arc::new(AtomicUsize::new(0));
    let (engine, root) = build_graph(&kv, &sink, None, &counter, level_sizes);

    let baseline = settle_signal.load(Ordering::SeqCst);
    let t = Instant::now();
    let handle = {
        let e = engine.clone();
        tokio::spawn(async move { e.run(root, ()).await })
    };
    wait_for_signal(settle_signal, baseline, phase_timeout).await;
    let ms = t.elapsed().as_millis();
    let reruns = counter.load(Ordering::Relaxed);
    report(label, ms, reruns);

    handle.abort();
    let _ = handle.await;
}

/// Phase 6: restart against the existing `db_path` with a mismatched
/// fingerprint (`fingerprint_v2`, never written by any earlier phase),
/// forcing every restored node to be marked `Revalidate` regardless of
/// what `probe_versions` reports (see `crate::persist::persist_load`).
async fn run_fingerprint_mismatch_phase(
    level_sizes: &[u32; LEVELS],
    db_path: &Path,
    fingerprint_v2: Fingerprint,
    settle_signal: &Arc<AtomicU64>,
    phase_timeout: Duration,
) {
    let kv = make_kv(true, true).await;
    let sink = VecSink::new("docs");

    let counter = Arc::new(AtomicUsize::new(0));
    let opts = PersistOptions::new(db_path, fingerprint_v2);
    let (engine, root) = build_graph(&kv, &sink, Some(opts), &counter, level_sizes);

    let baseline = settle_signal.load(Ordering::SeqCst);
    let t = Instant::now();
    let handle = {
        let e = engine.clone();
        tokio::spawn(async move { e.run(root, ()).await })
    };
    wait_for_signal(settle_signal, baseline, phase_timeout).await;
    let ms = t.elapsed().as_millis();
    let reruns = counter.load(Ordering::Relaxed);
    report(
        "6. restart, fingerprint mismatch (full revalidation)",
        ms,
        reruns,
    );

    engine.persist_close();
    handle.abort();
    let _ = handle.await;
}

/// Phases 7 and 8: build a fresh, dedicated `MemKvSource`/`VecSink` pair
/// (rather than any shared/replayed `kv`) so the single triggering change
/// below is completely unambiguous, let the engine settle, then mutate
/// exactly one source key and time until the driver's next propagation
/// round completes. `persist_opts` is `None` for phase 7 and `Some` (its
/// own fresh database) for phase 8.
///
/// Saving no longer happens synchronously inside a round (see
/// `crate::persist`'s background persister task): a round's own
/// settle-latency (the `RESULT` line under `label`) should now land close
/// to identical between phases 7 and 8. The actual per-flush cost is
/// reported separately, as a second `RESULT` line under `"{label} +
/// persist_now (time-to-durable)"`: the same settle latency plus an
/// explicit, awaited `persist_now()` immediately afterward — i.e. "how
/// long until this update is not just live, but crash-safe". For phase 7
/// (no persistence configured) `persist_now()` is a fast no-op, so that
/// line is expected to sit right on top of the plain settle line.
async fn run_live_incremental_phase(
    label: &str,
    level_sizes: &[u32; LEVELS],
    persist_opts: Option<PersistOptions>,
    settle_signal: &Arc<AtomicU64>,
    round_signal: &Arc<AtomicU64>,
    phase_timeout: Duration,
) {
    let kv = MemKvSource::new("kv-live");
    for k in 0..SRC_KEYS {
        kv.set(&k.to_string(), &(k as u64 * 7 + 3).to_string()).await;
    }
    let sink = VecSink::new("docs-live");

    let counter = Arc::new(AtomicUsize::new(0));
    let (engine, root) = build_graph(&kv, &sink, persist_opts, &counter, level_sizes);

    let baseline = settle_signal.load(Ordering::SeqCst);
    let handle = {
        let e = engine.clone();
        tokio::spawn(async move { e.run(root, ()).await })
    };
    wait_for_signal(settle_signal, baseline, phase_timeout).await;

    // Now steady-state: mutate exactly one source key and time until the
    // driver's next propagation round completes. Key "0" is read by
    // level-0 instance `i` whenever `i % SRC_KEYS == 0`; level 0 is fully
    // saturated at the default scale (verified against BASE_LEVEL_SIZES),
    // so this is always live.
    let round_baseline = round_signal.load(Ordering::SeqCst);
    let reruns_before = counter.load(Ordering::Relaxed);
    let t = Instant::now();
    kv.set("0", "13371337").await;
    wait_for_signal(round_signal, round_baseline, phase_timeout).await;
    let settle_ms = t.elapsed().as_millis();
    let reruns = counter.load(Ordering::Relaxed) - reruns_before;
    report(label, settle_ms, reruns);

    // Time-to-durable: the same settle latency above, plus however long an
    // explicit, awaited `persist_now()` takes right after — the actual
    // flush cost, now fully decoupled from round-settle latency.
    let t2 = Instant::now();
    engine.persist_now().await;
    let durable_extra_ms = t2.elapsed().as_millis();
    report(&format!("{label} + persist_now (time-to-durable)"), settle_ms + durable_extra_ms, reruns);

    engine.persist_close();
    handle.abort();
    let _ = handle.await;
}
