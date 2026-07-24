# Persistence at 1M nodes — benchmark notes (raw material for a blog post)

> Working notes, not polished prose. Numbers are from real release-mode runs on an
> Apple-silicon Mac (Darwin 25.5). Each stage links the commit that produced it.
> Reproduce any stage with:
> `cargo run -p computations --release --example persist_bench --features testutil`
> (`PERSIST_BENCH_SCALE=0.1` for the ~100k variant; benchmark self-reports
> per-phase wall time, rerun counts, RSS, and db size.)

## System under test

- `computations`: coarse-grained self-adjusting computation engine (Rust port of the
  ideas in Wehr's FUNARCH '23 paper). Named memoized async computations, dynamic
  dependency graph, early cutoff via result hashes, push driver, pluggable
  sources/sinks.
- Persistence (opt-in): redb store; **load-anyway design** — the snapshot is loaded
  even when the code changed; a binary fingerprint mismatch marks every node
  `Revalidate` (background tier), probed input changes mark dependents `Input`
  (foreground tier); max priority wins; Input work preempts the revalidation sweep
  between waves.
- Benchmark graph: 50 definitions × 10 levels, fan-in 3, 300 in-memory KV inputs,
  1000 sink outputs. Front-loaded level sizes tuned by offline reachability
  simulation → **999,760 instances** (−0.02% off the 1M target). Bodies are trivial
  (`wrapping_add`) — deliberate, so the numbers measure *engine overhead floor*,
  not body cost. This understates persistence's real-world win (see caveats).
- Benchmark harness is an orchestrator that re-execs itself once per phase
  (`PERSIST_BENCH_PHASE`), because an `Engine` is never freed in-process (see
  "open items"); process-per-phase makes RSS per phase meaningful and prevents a
  ~15 GB cumulative leak across the 10 phases.

## Stage 0 — 96k instances, synchronous saves (commit `4859ab2`)

Data format (v1):
- redb, two tables. `meta`: format-version byte + binary fingerprint.
  `nodes`: key = `CompKey` bytes (48 B = `DefId` + 32-byte blake3 param hash),
  value = postcard record `{def name, param bytes, comp_deps: Vec<CompKey>,
  source_deps: Vec<RawDep{source id, key bytes, ver bytes}>, result_hash (32 B),
  value bytes, outputs}`.
- Save = one synchronous redb write txn after every settled propagation round
  (diff-only: changed/removed keys).
- In-memory node: 4 `HashSet`s (comp_deps/source_deps/rdeps/outputs of full
  48-byte `CompKey`s / raw byte keys), value `Arc<dyn Any>` **plus** a duplicate
  postcard `value_bytes`, `param_bytes`, a Debug-rendered `param_debug: String`,
  one boxed rerun closure.

Numbers (96,163 instances):
- cold eval 349 ms (~3.6 µs/node) · persist 216 ms · db 33.7 MB (~350 B/node)
- warm restart 215–238 ms, **0 reruns** · cold-no-persistence 286–293 ms
- 1 changed input across restart: ~320 ms, 20.5k–25.9k reruns (21–27% of graph —
  structural: fan-in 3 over ~9 hops reaches a big slice; not overhead)
- fingerprint mismatch (full revalidation): ~584 ms

Finding: warm ≈ cold when bodies are trivial (~2.4 µs/node to restore vs ~3 µs to
recompute). Persistence's win scales with body cost; this benchmark is the floor.

## Stage 1 — scale to 1M, same format (commit `c17efdb`)

Numbers (999,760 instances):
- cold eval 4.2–4.4 s (~4.3 µs/node — essentially linear from 96k)
- persist_now 3.1–3.3 s · db **539 MB** (~540 B/node)
- warm restart 3.2–3.6 s, 0 reruns · cold-no-persistence 3.6–3.9 s
- restart + 1 changed input 3.8–4.9 s (100k–137k reruns)
- fingerprint mismatch 7.4–7.8 s (~1.6× cold: recompute + hash-compare + store)
- **live incremental** (steady-state, engine already running), 1 changed input,
  80,767 reruns: **1.4–1.5 s without persistence, 5.6 s with** ← the headline
  problem: the synchronous ~80k-row post-round flush blocks propagation latency.
- peak RSS ~2.6 GB (~2.7 KB/node in memory)

Tried at this stage:
- Measured redb single-write-txn saves at 20k/200k/500k/1M rows: linear, no
  blow-up → **rejected chunked transactions** as unwarranted.
- Observed per-row cost of a small update against a large existing B-tree is
  ~4× worse than bulk-loading the same rows into an empty one.

## Stage 2 — async debounced saves (commit `0df1c99`)

Design change (format unchanged):
- Rounds no longer await writes. Post-round, changed/removed keys are snapshotted
  (under the node lock) into a **coalescing pending map** (`CompKey → Upsert|Remove`,
  latest wins, removal supersedes update — coalescing is structural via
  `HashMap::insert`, no cross-collection bookkeeping).
- Background persister task flushes on whichever fires first: quiet-period
  debounce (500 ms) / pending-size threshold (100k) / max staleness (5 s). One
  spawn_blocking redb txn per flush. Failed flushes retry ≤5 times then drop
  loudly (sound: the store is a cache; a crash loses only the unflushed window).
- `persist_now()` stays synchronous-on-demand (tests/benchmark determinism).
- Subtlety: the persister holds the db handle via `Weak` and waits on an
  independently-owned `Notify`, otherwise `persist_close()` can't release the
  redb file lock until a sleeping debounce elapses (real bug hit during dev).

Numbers (1M):
- live incremental with persistence: settle **5.6 s → 1.62 s** (baseline without:
  1.33 s). Remaining gap = synchronous snapshot-clone of ~80k records under the
  node lock. Flush (~3.1 s) now fully off the critical path, reported separately
  as "time-to-durable".

Bonus: the new tests exposed two pre-existing engine bugs, fixed here with
regression tests:
1. Re-reading a source key at a *new version* deleted the node's own
   `source_index` registration (diffed by full `RawDep` equality incl. version)
   → later real changes to that key matched no node, permanently.
2. Driver reruns went through `eval_root`, whose caller-less `Ctx::eval`
   unconditionally marks its argument a GC root; `roots` only grows → the first
   direct rerun of a node with a source dep made it immortal to liveness GC.

## Stage 3 — node memory diet (commit `cc5dd5e`), format v2

Four changes (constraints: on-disk records keep ALL info — full def names, param
bytes, dep identities, hashes, values; comp names must remain loggable):
- (a) `param_debug: String` removed; params now render lazily only under
  `tracing::enabled!(DEBUG)`; names always available via `DefId`.
- (b) duplicate `value_bytes` removed; per-def erased serializer
  (`Arc<dyn Any> → Vec<u8>`); value `Arc` cloned under the lock, serialized at
  flush time outside it. `param_bytes` deliberately kept (written once per node,
  small; erasing would cost more than it saves).
- (c) `Hash256 → Hash128` (truncated blake3) for param/result hashes — `CompKey`
  48→32 B; birthday risk at 1M keys ~10⁻²⁰ (the original paper used 128-bit
  hashes too). Persistence `FORMAT_VERSION` 1→2 (old caches self-discard).
- (d) interned node identities: slab (`Vec<Option<Node>>` + free list) +
  `HashMap<CompKey, NodeId(u32)>`; edges become `SmallVec<[NodeId; 4]>` with
  dedup-on-insert; `source_index` values become `NodeId` sets. Rerun closures
  capture only `DefId` + typed param — never `NodeId` — so GC slot reuse is safe.
  Disk still stores full `CompKey`s; restore is two-pass (assign ids, then wire
  edges). `size_of::<Node>()` = 296 B with a `<= 320` tripwire test.

Numbers (1M, independently re-verified):
- cold eval 3.72 s · persist_now 2.40 s · db **269 MB** (−50%)
- warm restart **1.59–1.64 s** (was 3.3 s) · cold-no-persistence 3.0–3.1 s
- restart + 1 changed input 2.1–2.3 s · fingerprint mismatch 5.26 s
- live incremental: **574 ms without persistence, 706 ms with** (time-to-durable
  3.2 s) — persistence latency tax now ~23% instead of ~4×
- peak RSS **1.78 GB** (was 2.6 GB); engine-only (no persistence configured)
  **~700 MB ≈ 0.7 KB/node** (was ~2.7 KB/node); the with-persistence delta
  (~1 GB) is redb's cache + in-flight pending snapshots — the price of the async
  flush window.

## Tried and rejected (with reasons)

- `SmallVec` for `source_deps`/`outputs`: measured a *regression* at 1M — most
  nodes have zero source deps/outputs; SmallVec's unconditional inline capacity
  loses to a never-allocated empty `HashSet` given `RawDep`/`RawOutput` sizes.
  Reverted; finding documented in code.
- Chunked redb write transactions: single txn measured linear to 1M rows.
- Hand-rolled WAL / okaywal: okaywal's own docs say format-unstable, don't use;
  a raw WAL still needs snapshot+compaction on top — redb txns give the
  journal property for free.
- sled (perpetual alpha rewrite), fjall (LSM = write-heavy optimization we don't
  need; feature development winding down), SQLite (C dep + SQL layer for a typed
  KV problem).
- proto/flatbuffers/capnp: no cross-language need; schema evolution is
  "version byte + discard" for a cache. postcard (already used for hashing)
  stays. rkyv/mmap is the noted fallback if snapshot load ever dominates.
- zstd compression: records are small; deferred behind the format-version byte
  (can add without migration machinery).

## Not tried yet / open items

- `--body-cost <µs>` busy-spin flag to demonstrate the realistic win (warm
  restart flat while cold scales linearly with body cost).
- `Weak` backrefs for the rerun-closure → `EngineInner` `Arc` cycle: engines are
  never freed in-process today (fine for the intended one-engine-per-process
  deployment; the benchmark works around it with process-per-phase workers).
- Remaining live-update overhead (~130 ms at 80k changed nodes) is the
  snapshot-clone under the node lock; could be sharded or made copy-on-write.
- Sharing one redb txn budget across concurrent flush + startup-load paths is
  untested (single engine per db file assumed; redb's file lock enforces it).
- hashCaching-style value-less records ("verifying traces" only — deps + hashes,
  no values) as a db-size/RSS option: halves the record but restart recomputes
  values on first parent access.
- Startup probe cost at 1M is bundled into warm-restart time; not separately
  instrumented.

## Caveats to state prominently in any writeup

- Bodies are `wrapping_add` — this measures the *engine floor*. Real workloads
  (parse documents, build UI models) shift every restore-vs-recompute comparison
  heavily toward persistence.
- 21–27% of the graph re-running for 1 changed input is a property of the
  benchmark's fan-in-3 topology, not framework overhead.
- Crash durability: at most the unflushed window (≤5 s staleness by default) is
  lost, and loss = recompute, never corruption. This "cache, not source of
  truth" framing is what makes the whole async design sound.
- Glitches during propagation are possible (same trade as the paper).

## Possible blog angles (later)

- "Load the stale cache anyway": restart persistence with priority tiers instead
  of discard-on-mismatch — old results serve until inputs prove them wrong,
  genuinely-changed inputs jump the queue.
- "It's a cache": how dropping the durability requirement collapses WAL
  engineering into a debounced background flush.
- Honest µs/node accounting of an incremental-computation engine at 1M nodes.
- Rust war stories: the `Arc` cycle that ate the benchmark, the SmallVec that
  made things slower, 48-byte keys × 2 directions × 3M edges.
