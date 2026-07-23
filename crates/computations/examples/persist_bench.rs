//! Scale benchmark for opt-in dependency-graph persistence (`computations::persist`).
//!
//! Builds a synthetic 10-level, 50-definition dependency graph (~100,000
//! computation applications), then measures, with plain `std::time::Instant`
//! timers:
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
//!    "without persistence" comparison); and, if the earlier phases leave
//!    time,
//! 6. a restart after a fingerprint change (expect every restored node to
//!    be revalidated, but early cutoff to still avoid cascading further).
//!
//! Run with:
//! ```text
//! cargo run -p computations --release --example persist_bench --features testutil
//! ```
//!
//! ## Graph shape
//!
//! 10 levels, 5 definitions per level (50 `Comp<u32, u64>` definitions
//! total). Level 0 reads one of 300 `MemKvSource` keys (`index % 300`).
//! Level `L > 0` folds a fixed fan-in of 3 level-`L-1` instances, chosen by
//! fixed modular arithmetic on the instance index, spread across that
//! level's 5 definitions. The top level's 1000 instances each write one
//! small doc to a `VecSink`; a single `Comp<(), ()>` root evaluates all of
//! them via `ctx.eval_all`. Instance counts per level are front-loaded so
//! that root's fan-in genuinely reaches (almost) every declared instance —
//! see the module-level comment above `LEVEL_SIZES` for how those numbers
//! were chosen. The actual achieved instance count is measured (not just
//! declared) and printed at the top of the report: it is the number of
//! distinct computations that actually ran during the cold initial
//! evaluation.
//!
//! ## Detecting "settled"
//!
//! `Engine::run` never returns (see its own docs): it performs the initial
//! evaluation, then loops forever. To time "until settled" from outside the
//! spawned task without depending on the top-level sink docs changing
//! (which they won't, on a pure cache-hit restart), this benchmark installs
//! a tiny `tracing_subscriber::Layer` that watches for the
//! `driver::run`'s unconditional `"initial evaluation complete"` log event
//! (emitted right after the initial `eval_root` call resolves, regardless of
//! whether anything actually re-executed) and bumps a counter. Each phase
//! records that counter's value before spawning `run`, then polls until it
//! has advanced.

use std::path::PathBuf;
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

/// Instances per level, bottom (level 0) to top (level 9).
///
/// Front-loaded rather than flat: with a fixed fan-in of 3 spreading via
/// modular arithmetic, the set of instances actually *reachable* from the
/// 1000 top-level roots takes a few hops to saturate a level's full
/// declared size (a "coupon collector" effect). Sizing the levels nearest
/// the top smaller (so they saturate almost immediately) and the levels
/// nearest the bottom larger (where saturation has already kicked in)
/// keeps the number of instances actually evaluated close to what's
/// declared, landing the whole graph within ~10% of 100,000 total
/// applications. The exact achieved count is measured at runtime (see
/// `main`), not assumed.
const LEVEL_SIZES: [u32; LEVELS] = [
    15100, 15100, 15100, 15100, 15100, 9500, 7500, 4500, 2000, 1000,
];

/// One level's 5 definitions. `Comp<P, R>` is a cheap `Copy` handle, so this
/// whole array is `Copy` too — it can be captured by value in as many
/// per-definition closures as needed with no `Arc` or cloning required.
type Level = [Comp<u32, u64>; DEFS_PER_LEVEL];

/// Builds the full 50-definition, 10-level graph plus its root, wiring in
/// `kv`/`sink` and (optionally) persistence. `run_counter` is incremented
/// once per computation body invocation (every level's defs, plus root),
/// so the caller can read off exactly how many computations actually ran
/// during a given phase.
fn build_graph(
    kv: &Arc<MemKvSource>,
    sink: &Arc<VecSink>,
    persist_opts: Option<PersistOptions>,
    run_counter: &Arc<AtomicUsize>,
) -> (Engine, Comp<(), ()>) {
    let mut registry = Registry::default();
    registry.register_source(kv.clone());
    registry.register_sink(sink.clone());

    let mut builder = Engine::builder();
    builder.registry(registry);
    if let Some(opts) = persist_opts {
        builder.persistence(opts);
    }

    let mut levels: Vec<Level> = Vec::with_capacity(LEVELS);

    for level in 0..LEVELS {
        let is_top = level == LEVELS - 1;
        let prev: Option<Level> = if level == 0 {
            None
        } else {
            Some(levels[level - 1])
        };
        let prev_size = if level == 0 {
            0
        } else {
            LEVEL_SIZES[level - 1]
        };

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
    let top_size = LEVEL_SIZES[LEVELS - 1];
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

/// A `tracing_subscriber::Layer` that bumps `counter` every time it sees
/// `crate::driver`'s unconditional `"initial evaluation complete"` event —
/// emitted by `Engine::run` right after its initial `eval_root` call
/// resolves, regardless of whether that evaluation was a pure cache hit or
/// actually re-ran anything. This is the only externally observable
/// "the initial evaluation has settled" signal `Engine::run` offers (it
/// never returns on the happy path), so phases below poll this counter
/// instead of depending on sink content, which does *not* change on a pure
/// cache-hit restart.
struct SettleSignal {
    counter: Arc<AtomicU64>,
}

impl<S> Layer<S> for SettleSignal
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
        if visitor.0.contains("initial evaluation complete") {
            self.counter.fetch_add(1, Ordering::SeqCst);
        }
    }
}

/// Polls `signal` until it has advanced past `baseline`, panicking if
/// `timeout` passes first (per the task's caution: a phase pathologically
/// stuck should fail loudly, not hang the benchmark).
async fn wait_settled(signal: &Arc<AtomicU64>, baseline: u64, timeout: Duration) {
    tokio::time::timeout(timeout, async {
        loop {
            if signal.load(Ordering::SeqCst) > baseline {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("engine did not report a settled initial evaluation within the timeout");
}

struct PhaseResult {
    label: String,
    ms: u128,
    reruns: usize,
}

#[tokio::main]
async fn main() {
    let settle_signal = Arc::new(AtomicU64::new(0));
    tracing_subscriber::registry()
        .with(SettleSignal {
            counter: settle_signal.clone(),
        })
        .init();

    let phase_timeout = Duration::from_secs(240);

    let dir = tempfile::tempdir().expect("create temp dir for persist_bench.redb");
    let db_path: PathBuf = dir.path().join("persist_bench.redb");

    let kv = MemKvSource::new("kv");
    let sink = VecSink::new("docs");
    for k in 0..SRC_KEYS {
        kv.set(&k.to_string(), &(k as u64 * 7 + 3).to_string())
            .await;
    }

    let fingerprint_v1 = Fingerprint::custom("persist-bench-v1");
    let fingerprint_v2 = Fingerprint::custom("persist-bench-v2");

    println!("=== persist_bench: graph shape ===");
    println!(
        "levels: {LEVELS}, defs/level: {DEFS_PER_LEVEL}, total defs: {}",
        LEVELS * DEFS_PER_LEVEL
    );
    println!("fixed fan-in: {FAN_IN}");
    println!("declared level sizes (bottom -> top): {LEVEL_SIZES:?}");
    println!(
        "declared total (sum of level sizes + root): {}",
        LEVEL_SIZES.iter().sum::<u32>() as u64 + 1
    );
    println!(
        "source keys: {SRC_KEYS}, top-level sink outputs: {}",
        LEVEL_SIZES[LEVELS - 1]
    );
    println!();

    let mut results: Vec<PhaseResult> = Vec::new();

    // --- Phase 1: cold initial eval, persistence configured, empty db ---
    let counter1 = Arc::new(AtomicUsize::new(0));
    let opts1 = PersistOptions {
        path: db_path.clone(),
        fingerprint: fingerprint_v1,
    };
    let (engine1, root1) = build_graph(&kv, &sink, Some(opts1), &counter1);

    let baseline = settle_signal.load(Ordering::SeqCst);
    let t0 = Instant::now();
    let handle1 = {
        let e = engine1.clone();
        tokio::spawn(async move { e.run(root1, ()).await })
    };
    wait_settled(&settle_signal, baseline, phase_timeout).await;
    let phase1_ms = t0.elapsed().as_millis();
    let phase1_reruns = counter1.load(Ordering::Relaxed);
    println!("phase 1 (cold initial eval): {phase1_ms} ms, {phase1_reruns} computations run");
    println!(
        "  -> achieved instance count: {phase1_reruns} (target ~100,000, {:+.1}%)",
        (phase1_reruns as f64 - 100_000.0) / 100_000.0 * 100.0
    );
    assert_eq!(
        sink.names().len(),
        LEVEL_SIZES[LEVELS - 1] as usize,
        "every top-level doc must have been written"
    );
    results.push(PhaseResult {
        label: "1. cold initial eval (persistence configured)".to_string(),
        ms: phase1_ms,
        reruns: phase1_reruns,
    });

    // --- Phase 2: persist_now() + resulting db size ---
    let t1 = Instant::now();
    engine1.persist_now().await;
    let phase2_ms = t1.elapsed().as_millis();
    let db_size_bytes = std::fs::metadata(&db_path).map(|m| m.len()).unwrap_or(0);
    println!(
        "phase 2 (persist_now): {phase2_ms} ms, db size = {:.2} MB",
        db_size_bytes as f64 / 1_000_000.0
    );
    results.push(PhaseResult {
        label: format!(
            "2. persist_now [db={:.2} MB]",
            db_size_bytes as f64 / 1_000_000.0
        ),
        ms: phase2_ms,
        reruns: 0,
    });

    engine1.persist_close();
    handle1.abort();
    let _ = handle1.await;
    drop(engine1);

    // --- Phase 3: warm restart, no input changes (twice, for variance) ---
    for trial in 1..=2 {
        let counter3 = Arc::new(AtomicUsize::new(0));
        let opts3 = PersistOptions {
            path: db_path.clone(),
            fingerprint: fingerprint_v1,
        };
        let (engine3, root3) = build_graph(&kv, &sink, Some(opts3), &counter3);

        let baseline = settle_signal.load(Ordering::SeqCst);
        let t = Instant::now();
        let handle3 = {
            let e = engine3.clone();
            tokio::spawn(async move { e.run(root3, ()).await })
        };
        wait_settled(&settle_signal, baseline, phase_timeout).await;
        let ms = t.elapsed().as_millis();
        let reruns = counter3.load(Ordering::Relaxed);
        println!(
            "phase 3 (warm restart, no changes) trial {trial}: {ms} ms, {reruns} computations run"
        );

        engine3.persist_now().await;
        engine3.persist_close();
        handle3.abort();
        let _ = handle3.await;

        results.push(PhaseResult {
            label: format!("3. warm restart, no changes (trial {trial})"),
            ms,
            reruns,
        });
    }

    // --- Phase 4: restart with exactly one changed input (twice, distinct keys) ---
    for (trial, changed_key) in [(1, "150"), (2, "151")] {
        kv.set(changed_key, "999999").await;

        let counter4 = Arc::new(AtomicUsize::new(0));
        let opts4 = PersistOptions {
            path: db_path.clone(),
            fingerprint: fingerprint_v1,
        };
        let (engine4, root4) = build_graph(&kv, &sink, Some(opts4), &counter4);

        let baseline = settle_signal.load(Ordering::SeqCst);
        let t = Instant::now();
        let handle4 = {
            let e = engine4.clone();
            tokio::spawn(async move { e.run(root4, ()).await })
        };
        wait_settled(&settle_signal, baseline, phase_timeout).await;
        let ms = t.elapsed().as_millis();
        let reruns = counter4.load(Ordering::Relaxed);
        println!(
            "phase 4 (restart, 1 changed input key={changed_key}) trial {trial}: {ms} ms, {reruns} computations run \
             ({:.2}% of the full graph)",
            reruns as f64 / phase1_reruns as f64 * 100.0
        );

        engine4.persist_now().await;
        engine4.persist_close();
        handle4.abort();
        let _ = handle4.await;

        results.push(PhaseResult {
            label: format!("4. restart, 1 changed input (trial {trial}, key={changed_key})"),
            ms,
            reruns,
        });
    }

    // --- Phase 5: cold restart baseline, persistence NOT configured (twice) ---
    for trial in 1..=2 {
        let counter5 = Arc::new(AtomicUsize::new(0));
        let (engine5, root5) = build_graph(&kv, &sink, None, &counter5);

        let baseline = settle_signal.load(Ordering::SeqCst);
        let t = Instant::now();
        let handle5 = {
            let e = engine5.clone();
            tokio::spawn(async move { e.run(root5, ()).await })
        };
        wait_settled(&settle_signal, baseline, phase_timeout).await;
        let ms = t.elapsed().as_millis();
        let reruns = counter5.load(Ordering::Relaxed);
        println!(
            "phase 5 (cold restart, no persistence) trial {trial}: {ms} ms, {reruns} computations run"
        );

        handle5.abort();
        let _ = handle5.await;

        results.push(PhaseResult {
            label: format!("5. cold restart, no persistence (trial {trial})"),
            ms,
            reruns,
        });
    }

    // --- Phase 6 (optional): restart after a fingerprint change ---
    {
        let counter6 = Arc::new(AtomicUsize::new(0));
        let opts6 = PersistOptions {
            path: db_path.clone(),
            fingerprint: fingerprint_v2,
        };
        let (engine6, root6) = build_graph(&kv, &sink, Some(opts6), &counter6);

        let baseline = settle_signal.load(Ordering::SeqCst);
        let t = Instant::now();
        let handle6 = {
            let e = engine6.clone();
            tokio::spawn(async move { e.run(root6, ()).await })
        };
        wait_settled(&settle_signal, baseline, phase_timeout).await;
        let ms = t.elapsed().as_millis();
        let reruns = counter6.load(Ordering::Relaxed);
        println!(
            "phase 6 (restart, fingerprint mismatch): {ms} ms, {reruns} computations run (full revalidation)"
        );

        engine6.persist_close();
        handle6.abort();
        let _ = handle6.await;

        results.push(PhaseResult {
            label: "6. restart, fingerprint mismatch (full revalidation)".to_string(),
            ms,
            reruns,
        });
    }

    println!();
    println!("=== summary ===");
    println!("{:<55} {:>12} {:>10}", "phase", "time (ms)", "reruns");
    for r in &results {
        println!("{:<55} {:>12} {:>10}", r.label, r.ms, r.reruns);
    }
}
