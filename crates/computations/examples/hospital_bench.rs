//! Scale benchmark for the *unshared-key, concurrent-source* workload shape
//! that `examples/persist_bench.rs` cannot exercise.
//!
//! ## Why a second benchmark
//!
//! `persist_bench` builds 205,000 level-0 instances reading only 300
//! distinct source keys (`i % SRC_KEYS`) -- ~683 dependents share every key.
//! Its source (`MemKvSource`) has no latency, and its comp bodies read
//! exactly one key per body via a single `ctx.src_req(...).await`, never
//! more than one source call joined concurrently. Two real classes of
//! optimization can never show up in that graph's numbers regardless of how
//! they're implemented:
//!
//! 1. anything that pays per *distinct source key interned* (a shared-key
//!    workload interns 300 keys no matter how large the graph gets; an
//!    unshared-key workload interns one key per read);
//! 2. anything that pays per *source round trip* (with zero latency and no
//!    concurrent dispatch, a round trip is free to begin with, so nothing
//!    can show a win from doing fewer of them or overlapping them).
//!
//! This benchmark is built specifically so both stop being true: every leaf
//! read here uses a key that is never read by any other computation
//! instance (patient- and reading-scoped, e.g. `"vitals/value/p42/v7"`), and
//! [`LatencySource`] carries a configurable simulated per-call latency plus a
//! call counter and a concurrency high-water mark, so a run can report
//! whether -- and how much -- source reads actually overlapped.
//!
//! ## Relationship to the Haskell original
//!
//! Ported from `haskell-computations`'s
//! `bench/Control/Computations/Demos/Bench/Hospital.hs` (graph shape) and
//! `.../SystemSrc.hs` (the latency/call-counting source), adapted rather
//! than transliterated:
//!
//! - **Kept**: the five-source clinical-system shape (admissions/vitals/
//!   labs/pharmacy/notes), the unshared-key design, per-source latency +
//!   call counting + a concurrency high-water mark, a self-recursive
//!   lab-trend chain with a per-patient depth cap in `[1, 5]`, a
//!   cross-system fan-in comp that reads one key from all five sources at
//!   once, and ward/hospital-level rollups culminating in one root.
//! - **Adapted**: Haskell's `ApplicativeDo` desugars independent monadic
//!   binds into one applicative-combined source batch automatically; this
//!   crate's `Ctx` has no such desugaring (or an engine-level request-batching
//!   type at all -- see "What this benchmark cannot yet measure" below), so
//!   every genuinely-independent multi-key read below is written explicitly
//!   with `futures::try_join!`/`Ctx::eval_all`, which drives the underlying
//!   futures concurrently within this process's tokio runtime. This is a
//!   *more* honest port than a mechanical translation would be: nothing here
//!   secretly serializes what the source code visually presents as
//!   concurrent.
//! - **Dropped**: the Haskell module's entire "pack every multi-field
//!   param/result into a bare `Word64`" section. That whole exercise exists
//!   because GHC's per-def column storage (`DefTable`'s `mkColumn`) only
//!   unboxes a column whose type is one of a fixed literal whitelist
//!   (`Word32`/`Word64`/`Int`/`Char`/`Bool`/`Double`) -- a tuple or newtype
//!   never qualifies regardless of its fields' types. This crate's
//!   equivalent column (`crate::def::CompDef`'s `Mutex<Vec<Option<R>>>`) is
//!   an ordinary generic `Vec` with no such whitelist: a `(u32, u32)` param
//!   or result is exactly as cheap as a bare `u64` here, so
//!   `admission`'s result below is a plain `(WardId, u32)` tuple, not a
//!   hand-packed integer.
//! - **Simplified**: interactions check only *adjacent* medication pairs
//!   (`MEDS_PER_PATIENT - 1` per patient) rather than Haskell's full `C(18,
//!   2) = 153` all-pairs check -- both exist only to give `interaction`'s
//!   comp body two independent upstream reads to join concurrently; the
//!   combinatorial blow-up of all-pairs buys nothing further for that
//!   purpose and would cost roughly 8x this benchmark's per-patient
//!   instance count for no additional coverage.
//!
//! ## What this benchmark cannot yet measure (open candidates it now unlocks)
//!
//! See `docs/persistence-benchmark-notes.md`'s Stage 11 for the full
//! writeup. In short: this crate's `Source`/`Ctx` API has no
//! `compSrcExecuteBatch` analogue (a hook letting a source instance collapse
//! several requests bundled in one batch into a single round trip) and no
//! per-source-key interning table analogous to Haskell's `SrcIndex`/
//! `DefTable` (this crate stores each `RawDep`'s key as owned, uninterned
//! `Vec<u8>` -- see `crate::source::RawDep`). Both are real, identified
//! optimization candidates that *only* an unshared-key, latency-bearing
//! workload like this one can meaningfully evaluate; `persist_bench`'s
//! shared-key, zero-latency graph cannot exercise either.
//!
//! Run with:
//! ```text
//! cargo run -p computations --release --example hospital_bench --features testutil
//! ```
//!
//! `HOSPITAL_BENCH_SCALE` (default `1.0`) scales the patient/ward counts
//! continuously, exactly like `PERSIST_BENCH_SCALE`. `HOSPITAL_BENCH_SRC_LATENCY_US`
//! (default `0`) sets every source's simulated per-call latency for the main
//! phase. `HOSPITAL_BENCH_RERUN_KEYS`/`HOSPITAL_BENCH_RERUN_LOOPS` control the
//! rerun-heavy live-update phase (see [`run_rerun_heavy_phase`]'s docs).
//!
//! ## No width/concurrency knob
//!
//! The Haskell original has a `HOSPITAL_BENCH_CONCURRENCY` knob bounding a
//! hand-rolled worker pool that a wide `CompReqCombined` batch's source
//! leaves get dispatched to. This crate has no engine-level notion of a
//! worker pool at all: `Ctx::eval_all`/`futures::try_join!` simply build one
//! future tree that the tokio runtime polls cooperatively, and a source's
//! `execute` overlaps with any other pending call against the same instance
//! for free as long as its own implementation doesn't serialize itself
//! (`LatencySource` doesn't -- its simulated delay is a plain
//! `tokio::time::sleep`, held across no lock). There is deliberately no knob
//! added here to *cap* that concurrency: nothing in the engine's design
//! calls for one, and inventing a per-source-instance semaphore purely for
//! this benchmark would be measuring a feature that doesn't exist rather
//! than the engine that does. [`LatencySource::high_water_mark`] reports how
//! much overlap was actually, empirically achieved instead.
//!
//! ## Process-per-phase, RESULT-line protocol, settle detection, RSS
//!
//! All four straight from `persist_bench`'s own docs and re-implemented
//! here rather than shared: `main` re-execs itself once per phase with
//! `HOSPITAL_BENCH_PHASE` set, each child prints `RESULT|label|ms|reruns|rss`
//! lines the orchestrator parses, "settled" is detected via the same two
//! `crate::driver` tracing events (`"initial evaluation complete"`,
//! `"propagation round complete"`), and RSS is read via `ps -o rss=`. Not
//! factored into a shared module for the same reason the Haskell module
//! duplicates `Bench.Main`'s driver/RSS helpers instead of importing them:
//! `persist_bench` must stay exactly as it is, so nothing it doesn't already
//! export gets a new caller.

use std::collections::HashMap;
use std::collections::HashSet;
use std::process::Command;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use computations::error::SourceError;
use computations::testutil::{VecSink, WriteDoc};
use computations::{Comp, Dep, Engine, Registry, Request, Source, SourceBase, SourceId};

use tokio::sync::{Mutex as AsyncMutex, mpsc};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::prelude::*;
use tracing_subscriber::registry::LookupSpan;

type PatId = u32;
type WardId = u32;

// ---------------------------------------------------------------------
// Graph shape constants (scale 1.0)
// ---------------------------------------------------------------------

const BASE_PATIENTS: u32 = 1_500;
const BASE_WARDS: u32 = 30;

const VITALS_PER_PATIENT: u32 = 200;
const VITALS_PER_WINDOW: u32 = 5;
const WINDOWS_PER_PATIENT: u32 = VITALS_PER_PATIENT / VITALS_PER_WINDOW;
/// Divisible by every value in `[1, 5]` (LCM(1..5) = 60, 180 = 3 * 60), so
/// `lab_trend_chain_cap`'s per-patient segment reset always lands on a whole
/// segment boundary -- see that function's docs.
const LABS_PER_PATIENT: u32 = 180;
const MEDS_PER_PATIENT: u32 = 20;
/// Adjacent pairs only -- see the module docs' "Simplified" bullet.
const INTERACTIONS_PER_PATIENT: u32 = MEDS_PER_PATIENT - 1;
const NOTES_PER_PATIENT: u32 = 100;

/// Instances per patient: 1 (admission) + 200 (vital) + 40 (vital_window) +
/// 180 (lab_result) + 180 (lab_trend) + 20 (med_order) + 19 (interaction) +
/// 100 (note) + 1 (note_digest) + 1 (risk_score) + 1 (patient_summary) + 1
/// (patient_alert) = 744.
const INSTANCES_PER_PATIENT: u64 = 1
    + VITALS_PER_PATIENT as u64
    + WINDOWS_PER_PATIENT as u64
    + LABS_PER_PATIENT as u64
    + LABS_PER_PATIENT as u64
    + MEDS_PER_PATIENT as u64
    + INTERACTIONS_PER_PATIENT as u64
    + NOTES_PER_PATIENT as u64
    + 1
    + 1
    + 1
    + 1;

/// Source requests per patient: admission(1) + summary(1) = 2 against adt;
/// vital(200*3) + summary(1) = 601 against vitals; lab_result(180*3) +
/// summary(1) = 541 against labs; med_order(20*2) + summary(1) = 41 against
/// pharmacy; note(100*2) + summary(1) = 201 against notes.
const SOURCE_CALLS_PER_PATIENT: u64 = 2 + 601 + 541 + 41 + 201;

fn scaled_patient_count(scale: f64) -> u32 {
    ((BASE_PATIENTS as f64 * scale).round().max(1.0)) as u32
}

fn scaled_ward_count(scale: f64, patient_count: u32) -> u32 {
    let w = ((BASE_WARDS as f64 * scale).round().max(1.0)) as u32;
    w.min(patient_count)
}

/// The lab-trend recursion chain length for patient `p`, in `[1, 5]` --
/// mirrors the Haskell original's `labTrendChainCap` exactly (same formula,
/// same reasoning): this is what makes the graph's depth heterogeneous
/// rather than a uniform stack of levels. `LABS_PER_PATIENT` is divisible by
/// every possible value here, so every patient's chain splits into whole
/// segments with no remainder.
fn lab_trend_chain_cap(p: PatId) -> u32 {
    1 + (p % 5)
}

// ---------------------------------------------------------------------
// Ward partitioning (mirrors the Haskell original's wardOffsets/
// patientsOfWard/wardOfWith, continuous rather than a fixed 50/ward so the
// scale knob has no small-scale dead zone)
// ---------------------------------------------------------------------

fn ward_offsets(ward_count: u32, patient_count: u32) -> Vec<u32> {
    let base = patient_count / ward_count;
    let extra = patient_count % ward_count;
    let mut offsets = Vec::with_capacity(ward_count as usize + 1);
    offsets.push(0u32);
    let mut acc = 0u32;
    for w in 0..ward_count {
        acc += base + u32::from(w < extra);
        offsets.push(acc);
    }
    offsets
}

fn patients_of_ward(offsets: &[u32], w: WardId) -> Vec<PatId> {
    (offsets[w as usize]..offsets[w as usize + 1]).collect()
}

fn ward_of(offsets: &[u32], p: PatId) -> WardId {
    for w in 0..offsets.len() - 1 {
        if p < offsets[w + 1] {
            return w as u32;
        }
    }
    (offsets.len() - 2) as u32
}

// ---------------------------------------------------------------------
// LatencySource: the Rust SystemSrc -- a HashMap-backed source with a
// configurable simulated per-call latency, a call counter, and a
// concurrency high-water mark. Shaped like `testutil::MemKvSource` (see
// that type's docs for the base design this extends) plus the two things a
// real service call has that an in-memory map lookup does not.
// ---------------------------------------------------------------------

/// A `SysGet` request against [`LatencySource`]: reads the current value of
/// a key, if any -- the sole request type this benchmark's five simulated
/// clinical systems need.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SysGet(String);

impl Request for SysGet {
    type Output = Option<String>;
}

struct LatencyState {
    values: HashMap<String, String>,
    versions: HashMap<String, u64>,
    watched: HashSet<String>,
}

/// A synthetic source modelling one external clinical system. Backed by a
/// plain in-memory map (like `testutil::MemKvSource`), plus:
///
/// - a per-instance **latency** (a `tokio::time::sleep` inside `execute`,
///   standing in for real network/service time -- the entire reason this
///   type exists; `MemKvSource` has none);
/// - a **call counter** and a **concurrency high-water mark**, both plain
///   atomics bumped on every request served, reporting how many source
///   calls actually happened and how many were genuinely observed
///   overlapping.
///
/// No `compSrcExecuteBatch` analogue: this crate's `Source` trait has no
/// batch-execute hook, so a request-bundling/dedup feature analogous to the
/// Haskell original's has nothing to override here yet (see the module
/// docs' "What this benchmark cannot yet measure" section) -- every request
/// this source serves pays its own simulated round trip independently, even
/// when several land in the same `eval_all`/`try_join!` batch.
struct LatencySource {
    id: SourceId,
    state: Mutex<LatencyState>,
    changes_tx: mpsc::UnboundedSender<HashSet<Dep<String, u64>>>,
    changes_rx: AsyncMutex<mpsc::UnboundedReceiver<HashSet<Dep<String, u64>>>>,
    latency_us: u64,
    call_count: AtomicUsize,
    in_flight: AtomicUsize,
    high_water: AtomicUsize,
}

impl LatencySource {
    fn new(id: &str, latency_us: u64) -> Arc<Self> {
        let (changes_tx, changes_rx) = mpsc::unbounded_channel();
        Arc::new(LatencySource {
            id: SourceId::new(id),
            state: Mutex::new(LatencyState {
                values: HashMap::new(),
                versions: HashMap::new(),
                watched: HashSet::new(),
            }),
            changes_tx,
            changes_rx: AsyncMutex::new(changes_rx),
            latency_us,
            call_count: AtomicUsize::new(0),
            in_flight: AtomicUsize::new(0),
            high_water: AtomicUsize::new(0),
        })
    }

    /// Inserts or overwrites `key`, notifying watchers in one atomic batch
    /// (a single channel send) if it's currently watched.
    fn set(&self, key: &str, value: &str) {
        self.set_many(std::slice::from_ref(&(key.to_string(), value.to_string())));
    }

    /// Inserts or overwrites many `(key, value)` pairs against *this*
    /// source instance in one atomic batch -- mirrors the Haskell original's
    /// `sysInsertBatch`: every changed, watched key is pushed as a single
    /// `HashSet` over one channel send, not one send per key, so
    /// `wait_changes`'s first-message-plus-drain loop (below) can never
    /// observe only part of this call's mutations.
    ///
    /// This is atomic only *within* one `LatencySource` instance. Unlike
    /// Haskell's `sysInsertBatch` (a single STM transaction spanning
    /// however many different `SystemSrc` instances the caller names),
    /// nothing in this crate's engine has a matching cross-source atomic
    /// primitive: `crate::driver::Engine::run`'s propagation loop races
    /// each registered source's own `wait_changes()` independently
    /// (`select_all`, first-ready-wins per round) rather than folding every
    /// source's changes into one shared wait. A batch spanning several
    /// sources therefore settles over however many propagation rounds it
    /// takes to drain every source's queue, not necessarily one -- see
    /// [`run_rerun_heavy_phase`]'s quiescence-based wait, which is exactly
    /// why it doesn't just wait for one `"propagation round complete"`
    /// event the way the single-key live phase does.
    fn set_many(&self, pairs: &[(String, String)]) {
        let mut deps = HashSet::new();
        {
            let mut state = self.state.lock().unwrap();
            for (key, value) in pairs {
                state.values.insert(key.clone(), value.clone());
                let is_watched = state.watched.contains(key);
                let ver = state.versions.entry(key.clone()).or_insert(0);
                *ver += 1;
                if is_watched {
                    deps.insert(Dep {
                        key: key.clone(),
                        ver: *ver,
                    });
                }
            }
        }
        if !deps.is_empty() {
            let _ = self.changes_tx.send(deps);
        }
    }

    fn call_count(&self) -> usize {
        self.call_count.load(Ordering::Relaxed)
    }

    fn high_water_mark(&self) -> usize {
        self.high_water.load(Ordering::SeqCst)
    }
}

impl SourceBase for LatencySource {
    type Key = String;
    type Ver = u64;

    fn instance_id(&self) -> SourceId {
        self.id.clone()
    }

    /// Awaits the first queued change batch, then opportunistically drains
    /// any further already-queued batches into the same result -- mirrors
    /// `testutil::MemKvSource::wait_changes`, generalized to whole-batch
    /// channel items (see [`Self::set_many`]).
    async fn wait_changes(&self) -> HashSet<Dep<String, u64>> {
        let mut rx = self.changes_rx.lock().await;
        let mut batch = match rx.recv().await {
            Some(b) => b,
            None => return HashSet::new(),
        };
        while let Ok(more) = rx.try_recv() {
            batch.extend(more);
        }
        batch
    }

    fn unregister(&self, keys: &HashSet<String>) {
        let mut state = self.state.lock().unwrap();
        for key in keys {
            state.watched.remove(key);
        }
    }
}

impl Source<SysGet> for LatencySource {
    /// Bumps the call counter and the in-flight/high-water pair, optionally
    /// sleeps `latency_us` (standing in for a real service round trip, paid
    /// with no lock held -- so any number of concurrent calls against this
    /// same instance genuinely overlap their sleeps), then answers from a
    /// brief lock over the backing map. Mirrors the Haskell original's
    /// `executeImpl`/`simulateRoundTrip` split exactly.
    async fn execute(&self, req: SysGet) -> (Result<Option<String>, SourceError>, HashSet<Dep<String, u64>>) {
        let SysGet(key) = req;
        self.call_count.fetch_add(1, Ordering::Relaxed);

        let n = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.high_water.fetch_max(n, Ordering::SeqCst);
        if self.latency_us > 0 {
            tokio::time::sleep(Duration::from_micros(self.latency_us)).await;
        }
        self.in_flight.fetch_sub(1, Ordering::SeqCst);

        let mut state = self.state.lock().unwrap();
        state.watched.insert(key.clone());
        let value = state.values.get(&key).cloned();
        let ver = *state.versions.get(&key).unwrap_or(&0);
        let mut deps = HashSet::new();
        deps.insert(Dep { key, ver });
        (Ok(value), deps)
    }
}

/// The five simulated clinical systems `patient_summary` reads across in one
/// cross-system batch.
struct HospitalSrcs {
    adt: Arc<LatencySource>,
    vitals: Arc<LatencySource>,
    labs: Arc<LatencySource>,
    pharmacy: Arc<LatencySource>,
    notes: Arc<LatencySource>,
}

impl HospitalSrcs {
    fn new(latency_us: u64) -> Self {
        HospitalSrcs {
            adt: LatencySource::new("hospital-bench-adt", latency_us),
            vitals: LatencySource::new("hospital-bench-vitals", latency_us),
            labs: LatencySource::new("hospital-bench-labs", latency_us),
            pharmacy: LatencySource::new("hospital-bench-pharmacy", latency_us),
            notes: LatencySource::new("hospital-bench-notes", latency_us),
        }
    }

    fn named(&self) -> [(&'static str, &Arc<LatencySource>); 5] {
        [
            ("adt", &self.adt),
            ("vitals", &self.vitals),
            ("labs", &self.labs),
            ("pharmacy", &self.pharmacy),
            ("notes", &self.notes),
        ]
    }

    fn total_calls(&self) -> usize {
        self.named().iter().map(|(_, s)| s.call_count()).sum()
    }
}

// ---------------------------------------------------------------------
// Source/sink keys -- every key here is read by exactly one leaf
// computation instance (the unshared-key design the module docs describe).
// ---------------------------------------------------------------------

fn adt_key(p: PatId) -> String {
    format!("adt/p{p}")
}
fn vital_value_key(p: PatId, v: u32) -> String {
    format!("vitals/value/p{p}/v{v}")
}
fn vital_unit_key(p: PatId, v: u32) -> String {
    format!("vitals/unit/p{p}/v{v}")
}
fn vital_range_key(p: PatId, v: u32) -> String {
    format!("vitals/range/p{p}/v{v}")
}
fn lab_result_key(p: PatId, l: u32) -> String {
    format!("labs/result/p{p}/l{l}")
}
fn lab_range_key(p: PatId, l: u32) -> String {
    format!("labs/range/p{p}/l{l}")
}
fn lab_specimen_key(p: PatId, l: u32) -> String {
    format!("labs/specimen/p{p}/l{l}")
}
fn med_order_key(p: PatId, m: u32) -> String {
    format!("pharmacy/order/p{p}/m{m}")
}
fn med_drug_key(p: PatId, m: u32) -> String {
    format!("pharmacy/drug/p{p}/m{m}")
}
fn note_text_key(p: PatId, n: u32) -> String {
    format!("notes/text/p{p}/n{n}")
}
fn note_author_key(p: PatId, n: u32) -> String {
    format!("notes/author/p{p}/n{n}")
}

/// No key is pre-populated (unlike `persist_bench`'s 300-key `make_kv`
/// mirror): with ~2M distinct keys reachable at the default scale, seeding
/// them would itself dominate startup time for no benefit. Every read
/// resolves via this fallback the first time -- a short deterministic slice
/// of the key itself.
fn val_of(key: &str, val: Option<String>) -> String {
    val.unwrap_or_else(|| key.chars().take(8).collect())
}

/// Folds a value's bytes into a `u64`, cheaply and deterministically
/// content-dependent, so a live-update mutation that changes a value's
/// bytes changes every comp result derived from it.
fn val_word64(v: &str) -> u64 {
    v.bytes().fold(0u64, |acc, b| acc.wrapping_mul(31).wrapping_add(b as u64))
}

// ---------------------------------------------------------------------
// Graph construction
// ---------------------------------------------------------------------

/// Builds the full hospital graph and its root, wiring in `srcs`/`sink`.
/// `run_counter` is incremented once per computation body invocation, so
/// the caller can read off exactly how many computations ran during a given
/// phase -- mirrors `persist_bench::build_graph`'s own counter.
#[allow(clippy::too_many_lines)]
fn build_graph(
    srcs: &HospitalSrcs,
    sink: &Arc<VecSink>,
    run_counter: &Arc<AtomicUsize>,
    patient_count: u32,
    ward_count: u32,
) -> (Engine, Comp<(), ()>) {
    let mut registry = Registry::default();
    for (_, src) in srcs.named() {
        registry.register_source(src.clone());
    }
    registry.register_sink(sink.clone());

    let mut builder = Engine::builder();
    builder.registry(registry);

    let offsets = Arc::new(ward_offsets(ward_count, patient_count));

    // admission: 1 read against `adt`, plus which ward `p` belongs to.
    let adt_src = srcs.adt.clone();
    let offsets_for_admission = offsets.clone();
    let counter = run_counter.clone();
    let admission: Comp<PatId, (WardId, u32)> = builder.define("admission", move |ctx, p: PatId| {
        let adt_src = adt_src.clone();
        let offsets = offsets_for_admission.clone();
        let counter = counter.clone();
        async move {
            counter.fetch_add(1, Ordering::Relaxed);
            let key = adt_key(p);
            let raw = ctx.src_req(&adt_src, SysGet(key.clone())).await?;
            let v = val_of(&key, raw);
            Ok((ward_of(&offsets, p), v.len() as u32))
        }
    });

    // vital: 3 concurrently-joined reads against `vitals`.
    let vitals_src = srcs.vitals.clone();
    let counter = run_counter.clone();
    let vital: Comp<(PatId, u32), u64> = builder.define("vital", move |ctx, (p, v): (PatId, u32)| {
        let src = vitals_src.clone();
        let counter = counter.clone();
        async move {
            counter.fetch_add(1, Ordering::Relaxed);
            let (vk, uk, rk) = (vital_value_key(p, v), vital_unit_key(p, v), vital_range_key(p, v));
            let (value, unit, range) = futures::try_join!(
                ctx.src_req(&src, SysGet(vk.clone())),
                ctx.src_req(&src, SysGet(uk.clone())),
                ctx.src_req(&src, SysGet(rk.clone())),
            )?;
            Ok(val_word64(&val_of(&vk, value))
                .wrapping_add(val_word64(&val_of(&uk, unit)))
                .wrapping_add(val_word64(&val_of(&rk, range))))
        }
    });

    // vital_window: sums VITALS_PER_WINDOW consecutive vitals, evaluated
    // concurrently via eval_all.
    let counter = run_counter.clone();
    let vital_window: Comp<(PatId, u32), u64> = builder.define("vital_window", move |ctx, (p, w): (PatId, u32)| {
        let counter = counter.clone();
        async move {
            counter.fetch_add(1, Ordering::Relaxed);
            let base = w * VITALS_PER_WINDOW;
            let params: Vec<(PatId, u32)> = (base..base + VITALS_PER_WINDOW).map(|v| (p, v)).collect();
            let readings = ctx.eval_all(vital, params).await?;
            Ok(readings.into_iter().fold(0u64, u64::wrapping_add))
        }
    });

    // lab_result: 3 concurrently-joined reads against `labs`.
    let labs_src = srcs.labs.clone();
    let counter = run_counter.clone();
    let lab_result: Comp<(PatId, u32), u64> = builder.define("lab_result", move |ctx, (p, l): (PatId, u32)| {
        let src = labs_src.clone();
        let counter = counter.clone();
        async move {
            counter.fetch_add(1, Ordering::Relaxed);
            let (rk, gk, sk) = (lab_result_key(p, l), lab_range_key(p, l), lab_specimen_key(p, l));
            let (result, range, specimen) = futures::try_join!(
                ctx.src_req(&src, SysGet(rk.clone())),
                ctx.src_req(&src, SysGet(gk.clone())),
                ctx.src_req(&src, SysGet(sk.clone())),
            )?;
            Ok(val_word64(&val_of(&rk, result))
                .wrapping_add(val_word64(&val_of(&gk, range)))
                .wrapping_add(val_word64(&val_of(&sk, specimen))))
        }
    });

    // lab_trend: self-recursive, chain length capped at lab_trend_chain_cap(p)
    // (mirrors the Haskell original's defineRecursiveComp use exactly).
    let counter = run_counter.clone();
    let lab_trend: Comp<(PatId, u32), u64> =
        builder.define_rec("lab_trend", move |lab_trend_c: Comp<(PatId, u32), u64>, ctx, (p, l): (PatId, u32)| {
            let counter = counter.clone();
            async move {
                counter.fetch_add(1, Ordering::Relaxed);
                let cap = lab_trend_chain_cap(p);
                let s = l % cap;
                let result_val = ctx.eval(lab_result, (p, l)).await?;
                if s == 0 {
                    Ok(result_val)
                } else {
                    let prev = ctx.eval(lab_trend_c, (p, l - 1)).await?;
                    Ok(prev.wrapping_add(result_val))
                }
            }
        });

    // med_order: 2 concurrently-joined reads against `pharmacy`.
    let pharmacy_src = srcs.pharmacy.clone();
    let counter = run_counter.clone();
    let med_order: Comp<(PatId, u32), u64> = builder.define("med_order", move |ctx, (p, m): (PatId, u32)| {
        let src = pharmacy_src.clone();
        let counter = counter.clone();
        async move {
            counter.fetch_add(1, Ordering::Relaxed);
            let (ok, dk) = (med_order_key(p, m), med_drug_key(p, m));
            let (order, drug) =
                futures::try_join!(ctx.src_req(&src, SysGet(ok.clone())), ctx.src_req(&src, SysGet(dk.clone())))?;
            Ok(val_word64(&val_of(&ok, order)).wrapping_add(val_word64(&val_of(&dk, drug))))
        }
    });

    // interaction: compares two adjacent medication orders (see the module
    // docs' "Simplified" bullet), each fetched concurrently.
    let counter = run_counter.clone();
    let interaction: Comp<(PatId, u32), bool> = builder.define("interaction", move |ctx, (p, i): (PatId, u32)| {
        let counter = counter.clone();
        async move {
            counter.fetch_add(1, Ordering::Relaxed);
            let (r1, r2) = futures::try_join!(ctx.eval(med_order, (p, i)), ctx.eval(med_order, (p, i + 1)))?;
            Ok(r1 == r2)
        }
    });

    // note: 2 concurrently-joined reads against `notes`.
    let notes_src = srcs.notes.clone();
    let counter = run_counter.clone();
    let note: Comp<(PatId, u32), u64> = builder.define("note", move |ctx, (p, n): (PatId, u32)| {
        let src = notes_src.clone();
        let counter = counter.clone();
        async move {
            counter.fetch_add(1, Ordering::Relaxed);
            let (tk, ak) = (note_text_key(p, n), note_author_key(p, n));
            let (text, author) =
                futures::try_join!(ctx.src_req(&src, SysGet(tk.clone())), ctx.src_req(&src, SysGet(ak.clone())))?;
            Ok(val_word64(&val_of(&tk, text)).wrapping_add(val_word64(&val_of(&ak, author))))
        }
    });

    // note_digest: sum of all NOTES_PER_PATIENT notes, evaluated concurrently.
    let counter = run_counter.clone();
    let note_digest: Comp<PatId, u64> = builder.define("note_digest", move |ctx, p: PatId| {
        let counter = counter.clone();
        async move {
            counter.fetch_add(1, Ordering::Relaxed);
            let params: Vec<(PatId, u32)> = (0..NOTES_PER_PATIENT).map(|n| (p, n)).collect();
            let notes = ctx.eval_all(note, params).await?;
            Ok(notes.into_iter().fold(0u64, u64::wrapping_add))
        }
    });

    // risk_score: three independent eval_all rollups (windows, trends,
    // interactions), themselves joined concurrently.
    let counter = run_counter.clone();
    let risk_score: Comp<PatId, u64> = builder.define("risk_score", move |ctx, p: PatId| {
        let counter = counter.clone();
        async move {
            counter.fetch_add(1, Ordering::Relaxed);
            let windows: Vec<(PatId, u32)> = (0..WINDOWS_PER_PATIENT).map(|w| (p, w)).collect();
            let trends: Vec<(PatId, u32)> = (0..LABS_PER_PATIENT).map(|l| (p, l)).collect();
            let interactions: Vec<(PatId, u32)> = (0..INTERACTIONS_PER_PATIENT).map(|i| (p, i)).collect();
            let (window_vals, trend_vals, interaction_vals) = futures::try_join!(
                ctx.eval_all(vital_window, windows),
                ctx.eval_all(lab_trend, trends),
                ctx.eval_all(interaction, interactions),
            )?;
            let sum_windows = window_vals.into_iter().fold(0u64, u64::wrapping_add);
            let sum_trends = trend_vals.into_iter().fold(0u64, u64::wrapping_add);
            let interact_count = interaction_vals.into_iter().filter(|b| *b).count() as u64;
            Ok(sum_windows.wrapping_add(sum_trends).wrapping_add(interact_count))
        }
    });

    // patient_summary: the one deliberately cross-system batch -- one key
    // from each of the five sources, joined concurrently alongside the
    // three patient-level rollups. Also the graph's only sink write.
    let adt_for_summary = srcs.adt.clone();
    let vitals_for_summary = srcs.vitals.clone();
    let labs_for_summary = srcs.labs.clone();
    let pharmacy_for_summary = srcs.pharmacy.clone();
    let notes_for_summary = srcs.notes.clone();
    let sink_for_summary = sink.clone();
    let counter = run_counter.clone();
    let patient_summary: Comp<PatId, u64> = builder.define("patient_summary", move |ctx, p: PatId| {
        let adt_src = adt_for_summary.clone();
        let vitals_src = vitals_for_summary.clone();
        let labs_src = labs_for_summary.clone();
        let pharmacy_src = pharmacy_for_summary.clone();
        let notes_src = notes_for_summary.clone();
        let sink = sink_for_summary.clone();
        let counter = counter.clone();
        async move {
            counter.fetch_add(1, Ordering::Relaxed);
            let (risk, admission_val, note_digest_val) = futures::try_join!(
                ctx.eval(risk_score, p),
                ctx.eval(admission, p),
                ctx.eval(note_digest, p),
            )?;
            let (adt_k, vitals_k, labs_k, pharmacy_k, notes_k) = (
                adt_key(p),
                vital_value_key(p, 0),
                lab_result_key(p, 0),
                med_order_key(p, 0),
                note_text_key(p, 0),
            );
            let (adt_v, vitals_v, labs_v, pharmacy_v, notes_v) = futures::try_join!(
                ctx.src_req(&adt_src, SysGet(adt_k.clone())),
                ctx.src_req(&vitals_src, SysGet(vitals_k.clone())),
                ctx.src_req(&labs_src, SysGet(labs_k.clone())),
                ctx.src_req(&pharmacy_src, SysGet(pharmacy_k.clone())),
                ctx.src_req(&notes_src, SysGet(notes_k.clone())),
            )?;
            let (ward, _admit_len) = admission_val;
            let cross_len = val_word64(&val_of(&adt_k, adt_v))
                .wrapping_add(val_word64(&val_of(&vitals_k, vitals_v)))
                .wrapping_add(val_word64(&val_of(&labs_k, labs_v)))
                .wrapping_add(val_word64(&val_of(&pharmacy_k, pharmacy_v)))
                .wrapping_add(val_word64(&val_of(&notes_k, notes_v)));
            let summary = risk.wrapping_add(ward as u64).wrapping_add(note_digest_val).wrapping_add(cross_len);
            ctx.sink_req(
                &sink,
                WriteDoc {
                    name: format!("p{p}"),
                    content: summary.to_string(),
                },
            )
            .await?;
            Ok(summary)
        }
    });

    // patient_alert: risk/ward parity check.
    let counter = run_counter.clone();
    let patient_alert: Comp<PatId, bool> = builder.define("patient_alert", move |ctx, p: PatId| {
        let counter = counter.clone();
        async move {
            counter.fetch_add(1, Ordering::Relaxed);
            let (risk, admission_val) = futures::try_join!(ctx.eval(risk_score, p), ctx.eval(admission, p))?;
            let (ward, _) = admission_val;
            Ok(risk % 7 == u64::from(ward % 7))
        }
    });

    // Ward-level rollups.
    let offsets_for_census = offsets.clone();
    let counter = run_counter.clone();
    let ward_census: Comp<WardId, u64> = builder.define("ward_census", move |ctx, w: WardId| {
        let offsets = offsets_for_census.clone();
        let counter = counter.clone();
        async move {
            counter.fetch_add(1, Ordering::Relaxed);
            let pats = patients_of_ward(&offsets, w);
            let admissions = ctx.eval_all(admission, pats).await?;
            Ok(admissions.len() as u64)
        }
    });

    let offsets_for_occupancy = offsets.clone();
    let counter = run_counter.clone();
    let ward_occupancy: Comp<WardId, u64> = builder.define("ward_occupancy", move |ctx, w: WardId| {
        let offsets = offsets_for_occupancy.clone();
        let counter = counter.clone();
        async move {
            counter.fetch_add(1, Ordering::Relaxed);
            let pats = patients_of_ward(&offsets, w);
            let admissions = ctx.eval_all(admission, pats).await?;
            Ok(admissions.into_iter().fold(0u64, |acc, (_, len)| acc.wrapping_add(len as u64)))
        }
    });

    let offsets_for_risk_board = offsets.clone();
    let counter = run_counter.clone();
    let ward_risk_board: Comp<WardId, u64> = builder.define("ward_risk_board", move |ctx, w: WardId| {
        let offsets = offsets_for_risk_board.clone();
        let counter = counter.clone();
        async move {
            counter.fetch_add(1, Ordering::Relaxed);
            let pats = patients_of_ward(&offsets, w);
            let alerts = ctx.eval_all(patient_alert, pats).await?;
            Ok(alerts.into_iter().filter(|a| *a).count() as u64)
        }
    });

    // hospital_dashboard: sums the three ward rollups across every ward,
    // joined concurrently.
    let counter = run_counter.clone();
    let hospital_dashboard: Comp<(), u64> = builder.define("hospital_dashboard", move |ctx, (): ()| {
        let counter = counter.clone();
        async move {
            counter.fetch_add(1, Ordering::Relaxed);
            let wards: Vec<WardId> = (0..ward_count).collect();
            let (census, risk_board, occupancy) = futures::try_join!(
                ctx.eval_all(ward_census, wards.clone()),
                ctx.eval_all(ward_risk_board, wards.clone()),
                ctx.eval_all(ward_occupancy, wards),
            )?;
            let sum = |v: Vec<u64>| v.into_iter().fold(0u64, u64::wrapping_add);
            Ok(sum(census).wrapping_add(sum(risk_board)).wrapping_add(sum(occupancy)))
        }
    });

    // transfer_candidates: fans in over patient_summary (not risk_score
    // directly) so patient_summary has a caller of its own -- mirrors the
    // Haskell original's identical reasoning.
    let counter = run_counter.clone();
    let transfer_candidates: Comp<(), u64> = builder.define("transfer_candidates", move |ctx, (): ()| {
        let counter = counter.clone();
        async move {
            counter.fetch_add(1, Ordering::Relaxed);
            let pats: Vec<PatId> = (0..patient_count).collect();
            let (summaries, admissions) = futures::try_join!(
                ctx.eval_all(patient_summary, pats.clone()),
                ctx.eval_all(admission, pats),
            )?;
            let count = summaries
                .iter()
                .zip(admissions.iter())
                .filter(|(s, (ward, _))| **s > u64::from(*ward))
                .count() as u64;
            Ok(count)
        }
    });

    let counter = run_counter.clone();
    let root: Comp<(), ()> = builder.define("root", move |ctx, (): ()| {
        let counter = counter.clone();
        async move {
            counter.fetch_add(1, Ordering::Relaxed);
            futures::try_join!(ctx.eval(hospital_dashboard, ()), ctx.eval(transfer_candidates, ()))?;
            Ok(())
        }
    });

    (builder.build(), root)
}

// ---------------------------------------------------------------------
// Settle detection, RSS, RESULT-line reporting -- straight from
// persist_bench, reimplemented here rather than shared (see module docs).
// ---------------------------------------------------------------------

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

/// Polls `counter` until it stops increasing for `debounce`, then returns
/// its value. Used only by the rerun-heavy phase (see
/// [`run_rerun_heavy_phase`]'s docs for why a fixed round count can't be
/// waited on the way the single-key live phase's `wait_for_signal` can).
async fn wait_until_quiescent(counter: &Arc<AtomicUsize>, debounce: Duration, timeout: Duration) -> usize {
    tokio::time::timeout(timeout, async {
        let mut last = counter.load(Ordering::Relaxed);
        loop {
            tokio::time::sleep(debounce).await;
            let now = counter.load(Ordering::Relaxed);
            if now == last {
                return now;
            }
            last = now;
        }
    })
    .await
    .expect("rerun-heavy phase did not settle within the timeout")
}

fn rss_mb() -> f64 {
    let pid = std::process::id().to_string();
    let output = Command::new("ps").args(["-o", "rss=", "-p", &pid]).output();
    match output {
        Ok(o) if o.status.success() => {
            String::from_utf8_lossy(&o.stdout).trim().parse::<f64>().map(|kb| kb / 1024.0).unwrap_or(0.0)
        }
        _ => 0.0,
    }
}

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

// ---------------------------------------------------------------------
// Orchestration
// ---------------------------------------------------------------------

fn main() {
    match std::env::var("HOSPITAL_BENCH_PHASE") {
        Ok(phase) => {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("build tokio runtime")
                .block_on(worker_main(&phase));
        }
        Err(_) => orchestrator_main(),
    }
}

fn orchestrator_main() {
    let scale: f64 = std::env::var("HOSPITAL_BENCH_SCALE").ok().and_then(|s| s.parse().ok()).unwrap_or(1.0);
    let latency_us: u64 =
        std::env::var("HOSPITAL_BENCH_SRC_LATENCY_US").ok().and_then(|s| s.parse().ok()).unwrap_or(0);
    let patient_count = scaled_patient_count(scale);
    let ward_count = scaled_ward_count(scale, patient_count);
    let target_instances = patient_count as u64 * INSTANCES_PER_PATIENT + ward_count as u64 * 3 + 3;
    let target_source_calls = patient_count as u64 * SOURCE_CALLS_PER_PATIENT;

    println!("=== hospital_bench: graph shape (unshared-key, concurrent-source workload) ===");
    println!("HOSPITAL_BENCH_SCALE={scale} HOSPITAL_BENCH_SRC_LATENCY_US={latency_us}");
    println!(
        "patients: {patient_count}, wards: {ward_count} (avg {:.1} patients/ward)",
        patient_count as f64 / ward_count as f64
    );
    println!("target instances (analytic): {target_instances}");
    println!("target source calls (analytic): {target_source_calls}, each against a distinct key");
    println!();

    let exe = std::env::current_exe().expect("resolve current executable path for re-exec");

    let mut results: Vec<PhaseResult> = Vec::new();
    let mut peak_rss: f64 = 0.0;
    let mut extra_output = String::new();

    for phase_id in ["main", "demo"] {
        println!("--- spawning worker for phase {phase_id} ---");
        let output = Command::new(&exe)
            .env("HOSPITAL_BENCH_PHASE", phase_id)
            .env("HOSPITAL_BENCH_SCALE", scale.to_string())
            .env("HOSPITAL_BENCH_SRC_LATENCY_US", latency_us.to_string())
            .output()
            .unwrap_or_else(|e| panic!("failed to spawn worker for phase {phase_id}: {e}"));

        let stdout = String::from_utf8_lossy(&output.stdout);
        print!("{stdout}");
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            panic!("worker for phase {phase_id} exited with {}; stderr:\n{stderr}", output.status);
        }

        for line in stdout.lines() {
            if let Some(rest) = line.strip_prefix("RESULT|") {
                let parts: Vec<&str> = rest.split('|').collect();
                let [label, ms, reruns, rss] = parts.as_slice() else {
                    panic!("worker for phase {phase_id} emitted a malformed RESULT line: {line}");
                };
                let ms: u128 = ms.parse().unwrap_or_else(|e| panic!("bad ms in RESULT line {line}: {e}"));
                let reruns: usize =
                    reruns.parse().unwrap_or_else(|e| panic!("bad reruns in RESULT line {line}: {e}"));
                let rss: f64 = rss.parse().unwrap_or_else(|e| panic!("bad rss in RESULT line {line}: {e}"));
                peak_rss = peak_rss.max(rss);
                results.push(PhaseResult {
                    label: label.to_string(),
                    ms,
                    reruns,
                    rss_mb: rss,
                });
            } else if line.starts_with("SOURCE|") || line.starts_with("NOTE|") {
                extra_output.push_str(line);
                extra_output.push('\n');
            }
        }
    }

    println!();
    println!("=== summary ===");
    println!("{:<58} {:>12} {:>10} {:>10}", "phase", "time (ms)", "reruns", "RSS (MB)");
    for r in &results {
        println!("{:<58} {:>12} {:>10} {:>10.1}", r.label, r.ms, r.reruns, r.rss_mb);
    }
    println!();
    println!("peak RSS observed across phases (max of each isolated worker process): {peak_rss:.1} MB");

    if !extra_output.is_empty() {
        println!();
        println!("=== source call detail ===");
        print!("{extra_output}");
    }
}

/// Runs the phase named by `phase_id` in this (worker) process.
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

    let phase_timeout = Duration::from_secs(300);

    match phase_id {
        "main" => run_main_phase(&settle_signal, &round_signal, phase_timeout).await,
        "demo" => run_concurrency_demo_phase(&settle_signal, phase_timeout).await,
        other => panic!("unknown HOSPITAL_BENCH_PHASE: {other}"),
    }
}

/// The primary phase: cold eval at the configured scale/latency, a
/// single-key live update, the rerun-heavy multi-key live update, then a
/// per-source call-count report.
async fn run_main_phase(settle_signal: &Arc<AtomicU64>, round_signal: &Arc<AtomicU64>, phase_timeout: Duration) {
    let scale: f64 = std::env::var("HOSPITAL_BENCH_SCALE").ok().and_then(|s| s.parse().ok()).unwrap_or(1.0);
    let latency_us: u64 =
        std::env::var("HOSPITAL_BENCH_SRC_LATENCY_US").ok().and_then(|s| s.parse().ok()).unwrap_or(0);
    let patient_count = scaled_patient_count(scale);
    let ward_count = scaled_ward_count(scale, patient_count);
    let target_instances = patient_count as u64 * INSTANCES_PER_PATIENT + ward_count as u64 * 3 + 3;

    let srcs = HospitalSrcs::new(latency_us);
    let sink = VecSink::new("hospital-bench-out");
    let counter = Arc::new(AtomicUsize::new(0));
    let (engine, root) = build_graph(&srcs, &sink, &counter, patient_count, ward_count);

    // 1. Cold eval.
    let baseline = settle_signal.load(Ordering::SeqCst);
    let t0 = Instant::now();
    let handle = {
        let e = engine.clone();
        tokio::spawn(async move { e.run(root, ()).await })
    };
    wait_for_signal(settle_signal, baseline, phase_timeout).await;
    let cold_ms = t0.elapsed().as_millis();
    let cold_reruns = counter.load(Ordering::Relaxed);
    eprintln!(
        "  -> achieved instance count: {cold_reruns} (target {target_instances}, {:+.2}%)",
        (cold_reruns as f64 - target_instances as f64) / target_instances as f64 * 100.0
    );
    assert_eq!(
        sink.names().len(),
        patient_count as usize,
        "every patient summary must have been written to the sink"
    );
    report("1. cold eval", cold_ms, cold_reruns);

    // 2. Live incremental: mutate one vitals key (patient 0, vital 0),
    // watched since it was read directly by both `vital` and
    // `patient_summary`'s cross-system batch.
    let round_baseline = round_signal.load(Ordering::SeqCst);
    let reruns_before = counter.load(Ordering::Relaxed);
    let t1 = Instant::now();
    srcs.vitals.set(&vital_value_key(0, 0), &"x".repeat(40));
    wait_for_signal(round_signal, round_baseline, phase_timeout).await;
    let live_ms = t1.elapsed().as_millis();
    let live_reruns = counter.load(Ordering::Relaxed) - reruns_before;
    report("2. live incremental, 1 changed vitals key", live_ms, live_reruns);

    println!("SOURCE|--- source calls (after phases 1-2) ---");
    for (name, src) in srcs.named() {
        println!(
            "SOURCE|  {:<10} requests={:<10} concurrency high-water mark={}",
            name,
            src.call_count(),
            src.high_water_mark()
        );
    }
    println!("SOURCE|  total: {}", srcs.total_calls());

    // 3. Rerun-heavy live update: mutate many keys spread across all five
    // sources and across the full patient/ward range, then wait until the
    // resulting cascade quiesces (see run_rerun_heavy_phase's docs for why
    // this can't wait on a fixed round count).
    let rerun_keys: usize =
        std::env::var("HOSPITAL_BENCH_RERUN_KEYS").ok().and_then(|s| s.parse().ok()).unwrap_or(300);
    let rerun_loops: usize =
        std::env::var("HOSPITAL_BENCH_RERUN_LOOPS").ok().and_then(|s| s.parse().ok()).unwrap_or(1);
    if rerun_keys > 0 {
        run_rerun_heavy_phase(&srcs, &counter, patient_count, rerun_keys, rerun_loops, phase_timeout).await;
    }

    handle.abort();
    let _ = handle.await;
}

/// Distinct payload written to every rerun-phase mutation target, keyed by
/// a globally unique index `n` so no two mutations ever collide and every
/// mutation actually changes its target's content (not just a version
/// counter) -- mirrors the Haskell original's `rerunMutationVal`.
fn rerun_mutation_val(n: u64) -> String {
    format!("rerun-{n}-{}", "r".repeat(32))
}

/// The `n`-th (globally unique) rerun-phase mutation target: cycles through
/// the five sources every 5 steps and picks a patient via a large prime
/// stride (9973, coprime to every patient count this benchmark's scale knob
/// produces in practice) rather than `n % patient_count`, so a batch far
/// smaller than `patient_count` still spreads across the entire patient
/// (and therefore ward) range.
fn rerun_mutation_target(patient_count: u32, n: u64) -> (usize, String) {
    let p = ((n.wrapping_mul(9973)) % patient_count as u64) as u32;
    let sub = (n / 5) as u32;
    match n % 5 {
        0 => (0, adt_key(p)),
        1 => (1, vital_value_key(p, sub % VITALS_PER_PATIENT)),
        2 => (2, lab_result_key(p, sub % LABS_PER_PATIENT)),
        3 => (3, med_order_key(p, sub % MEDS_PER_PATIENT)),
        _ => (4, note_text_key(p, sub % NOTES_PER_PATIENT)),
    }
}

/// Mutates `rerun_keys` keys per round, `rerun_loops` rounds, spread across
/// all five sources and the full patient range via [`rerun_mutation_target`],
/// and reports keys mutated / wall time / reruns / µs-per-rerun.
///
/// Settle detection here is quiescence-based (`wait_until_quiescent`), not
/// the fixed-round-count `wait_for_signal` the single-key live phase above
/// uses: a batch spread across multiple `LatencySource` instances settles
/// over however many propagation rounds it takes this engine's
/// `select_all`-raced `wait_for_any_change` to drain every touched source's
/// queue (see [`LatencySource::set_many`]'s docs) -- not necessarily one.
async fn run_rerun_heavy_phase(
    srcs: &HospitalSrcs,
    counter: &Arc<AtomicUsize>,
    patient_count: u32,
    rerun_keys: usize,
    rerun_loops: usize,
    phase_timeout: Duration,
) {
    let t0 = Instant::now();
    let reruns_before = counter.load(Ordering::Relaxed);
    let all_srcs = [&srcs.adt, &srcs.vitals, &srcs.labs, &srcs.pharmacy, &srcs.notes];

    for round in 0..rerun_loops {
        let mut batches: [Vec<(String, String)>; 5] = Default::default();
        for i in 0..rerun_keys {
            let global_idx = (round * rerun_keys + i) as u64;
            let (src_idx, key) = rerun_mutation_target(patient_count, global_idx);
            batches[src_idx].push((key, rerun_mutation_val(global_idx)));
        }
        for (src, batch) in all_srcs.iter().zip(batches.iter()) {
            if !batch.is_empty() {
                src.set_many(batch);
            }
        }
    }
    let final_count = wait_until_quiescent(counter, Duration::from_millis(100), phase_timeout).await;
    let rerun_ms = t0.elapsed().as_millis();
    let reruns = final_count - reruns_before;
    let total_keys = rerun_keys * rerun_loops;

    report(
        &format!("3. rerun-heavy live update ({total_keys} keys mutated)"),
        rerun_ms,
        reruns,
    );
    if reruns > 0 {
        println!("SOURCE|  us/rerun: {:.3}", (rerun_ms as f64 * 1000.0) / reruns as f64);
    }
}

/// A small-scale, nonzero-latency demonstration of latency-hiding through
/// `Ctx::eval_all`/`futures::try_join!` alone -- no width knob exists to
/// drive (see the module docs), so this reports what concurrency the
/// engine achieves "for free" by construction. Deliberately much smaller
/// than the main phase (mirrors the Haskell original's own width x latency
/// grid, measured at scale 0.05 rather than 1.0): every leaf's simulated
/// latency is genuinely paid, and at full scale that would dominate wall
/// time far more than this benchmark's cold-eval-dominated design intends.
async fn run_concurrency_demo_phase(settle_signal: &Arc<AtomicU64>, phase_timeout: Duration) {
    const DEMO_PATIENTS: u32 = 10;
    const DEMO_WARDS: u32 = 2;
    const DEMO_LATENCY_US: u64 = 2_000;

    let srcs = HospitalSrcs::new(DEMO_LATENCY_US);
    let sink = VecSink::new("hospital-bench-demo-out");
    let counter = Arc::new(AtomicUsize::new(0));
    let (engine, root) = build_graph(&srcs, &sink, &counter, DEMO_PATIENTS, DEMO_WARDS);

    let baseline = settle_signal.load(Ordering::SeqCst);
    let t0 = Instant::now();
    let handle = {
        let e = engine.clone();
        tokio::spawn(async move { e.run(root, ()).await })
    };
    wait_for_signal(settle_signal, baseline, phase_timeout).await;
    let ms = t0.elapsed().as_millis();
    let reruns = counter.load(Ordering::Relaxed);
    report(
        &format!("4. concurrency demo ({DEMO_PATIENTS} patients, {DEMO_LATENCY_US}us/call latency)"),
        ms,
        reruns,
    );

    let total_calls = srcs.total_calls();
    let sequential_estimate_ms = (total_calls as u64 * DEMO_LATENCY_US) as f64 / 1000.0;
    println!("SOURCE|--- concurrency demo detail ---");
    for (name, src) in srcs.named() {
        println!(
            "SOURCE|  {:<10} requests={:<10} concurrency high-water mark={}",
            name,
            src.call_count(),
            src.high_water_mark()
        );
    }
    println!(
        "NOTE|  {total_calls} total source calls at {DEMO_LATENCY_US}us each; a fully sequential run would need \
         >= {sequential_estimate_ms:.0} ms (total_calls * latency); observed {ms} ms wall time -- the gap is \
         latency genuinely hidden by concurrent dispatch, with no width knob involved."
    );

    handle.abort();
    let _ = handle.await;
}
