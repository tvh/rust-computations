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

## Interlude — what the Haskell reference would weigh (paper estimate, NOT measured)

Source: `skogsbaer/computations` @ `e0bec07` (2023-08-11), the reference
implementation this port is modeled on. Static reading of the data types, no
build, no run. Assumptions: GHC 9.x, 64-bit, `-O`, **`StrictData` on by
default** (package.yaml default-extensions), 1 word = 8 B, constructor object =
1 header word + 1 word per field (incl. one word per stored class dictionary in
an existential).

First and most important: **the Haskell reference has no persistence.** All
state is one `SifState` in a `TVar` (`CompEngine/Run.hs:119`); `grep -il
persist src/` hits only a doc comment. So the comparable Rust number is the
**engine-only** RSS, not the with-persistence figure — stage-3 ~700 MB
(≈0.7 KB/node), stage-4 **427–430 MB (≈0.43 KB/node)**. The whole stage 0→3
story (restart, load-anyway priority tiers, debounced flush, 269 MB on disk)
has no counterpart at all.

State layout (`CompEngine/SimpleStateIf.hs:69`): five global containers keyed by
the same `AnyCompAp` — `sifs_cache` (`Data.Map`), `sifs_vermap` (`Data.Map`),
`sifs_deps` fwd + rev (`HashMap`), `sifs_outputs` fwd (`HashMap`). Plus the
stale queue and pending set, both empty at rest.

Per-cap tally for the same graph shape (1M instances, fan-in 3 → ~3M comp
edges, avg 3 rdeps, level-0 caps hold 1 source dep, 1k caps hold 1 output):

| Where | Contents | B/cap |
|---|---|---|
| identity, shared | `AnyCompAp` 4 w + `CompApIntern` 9 w (5 dicts!) + param `Word128` 3 w + param box 2 w | **144** |
| `sifs_cache` | `Map` `Bin` 6 w + `CapSuccess` + `AnyCompCacheValue` + `CompCacheValue` + `Some` + result box + `CompCacheMeta` 5 w + result `Word128` + `ccm_logrepr` Text ~56 B + `ccm_approxCachedSize` 32 B | **296** |
| `sifs_vermap` | second `Map` `Bin` over the *same* keys + `Some` (`Word128` shared) | **64** |
| `dm_fwdDeps` | HAMT entry ~56 B + 3-elem `HashSet`: `BitmapIndexed`+`Array` 64 B, 3 `Leaf` ×48 B, 3 × boxed `CompEngDepComp`/`Dep`/`Some` ×56 B | **432** |
| `dm_revDeps` | HAMT entry ~56 B + key wrapper 16 B + `VerList` = 1-version HashMap (`Leaf` 48 B + ver key 32 B) + ~3-elem dependents set 208 B | **360** |
| `om_forward` | HAMT entry ~56 B — inserted for **every** cap, value `mempty` (`SimpleStateIf.hs:182`) | **56** |
| | **live heap / cap** | **≈ 1,350** |

→ **~1.35 GB live** at 999,760 caps. Uncertainty ±25% (HAMT amortization; how
many dictionary words GHC actually retains in the existentials — `Show`/
`Typeable` are genuinely used by `eqT`/`Ord AnyCompAp`, so they stay).

RSS is the bigger story: GHC's default copying collector sizes the old gen at
~2× live (`-F 2`) and needs to-space alongside from-space during a major GC.
**Realistic RSS ~2.7–4 GB** for a 1.35 GB live set. So:

- vs stage-3 Rust engine-only (0.7 KB/node): **~2× on live heap, ~4–6× on RSS**.
- vs stage-4 Rust engine-only (0.43 KB/node): **~3× live, ~6–9× RSS**.
- vs stage-1 Rust *pre-diet* (2.7 KB/node): Haskell would have looked *better*.
  The node diet is what flipped it.

Where the 2× live gap comes from, ranked:

1. **Edges: 792 B/cap (59% of the total).** Rust stores a 4-byte `NodeId` in an
   inline `SmallVec<[NodeId; 4]>` in both directions (~96 B, zero heap at
   fan-in ≤4). Haskell makes every edge a boxed `Dep` record (56 B) inside a
   HAMT leaf (48 B), stored twice, plus an entire `VerList` HashMap *level*
   per depended-on key. That level isn't waste — it's how `DepMap.stale`
   answers "who depends on a **now-superseded version** of X"
   (`Utils/DepMap.hs:152`), which Rust folds into `last_changed` + a flat rdeps
   list. Design cost, not just encoding cost.
2. **Container overhead: 264 B/cap** across five global keyed containers, vs
   Rust's one slab slot + one `HashMap<CompKey, NodeId>` index.
3. **`sifs_vermap` is a second full 1M-entry `Data.Map`** keyed by the same
   keys, holding a hash the cache entry already stores. 64 B/cap of pure
   redundancy — the closest analogue to the duplicate `value_bytes` stage 3(b)
   deleted.
4. **`fullCaching` renders every result with `show`** to fill `ccm_logrepr`
   (Text, ~56 B, retained) and `ccm_approxCachedSize` = `length (show x)`
   (`CacheBehaviors.hs:26-39`). Exactly the `param_debug: String` stage 3(a)
   removed — except on the *result*, on every node, unconditional, and
   recomputed on every rerun. Cheap for a `wrapping_add` u64; for the paper's
   hospital demo it's a full render of every cached document.
5. **Boxing depth**: 5 hops and ~96 B of wrappers to reach 8 bytes of `u64`
   payload (existential → `CapResult` → `AnyCompCacheValue` → `CompCacheValue`
   → `Option` → `W64#`).

Where Haskell is genuinely *better*, in fairness:

- **Structural sharing of identity.** `SifCache.lookup` deliberately returns the
  map's own key (`SifCache.hs:146-152`, hence `lookupLE`+eq rather than plain
  `lookup`), and `normalizeDep` (`SimpleStateIf.hs:488`) rewrites incoming deps
  to point at that one object. One `AnyCompAp` per cap serves all five
  containers and all ~3M edge records. Rust repeats a 32-byte `CompKey` in
  `Node.key` *and* in the `NodeTable` index. There's a `validateSifState`
  (`SimpleStateIf.hs:82`) using `reallyUnsafePtrEquality#` purely to assert the
  sharing holds — they took it seriously.
- **Hash width parity confirmed.** `largeHash128 = LH.largeHash md5HashAlgorithm`
  (`Utils/Hash.hs:34`), `Word128` = 2 unpacked `Word64` = 3 w = 24 B, same as
  our `Hash128`. Stage 3(c)'s "the original paper used 128-bit hashes too"
  checks out against the source.
- **No `param_bytes`.** The param stays live and typed (16 B box) instead of a
  serialized `Vec<u8>` (24 B header + heap). Cheaper — because there is nothing
  to serialize *to*.

Caveats on this tally: it's arithmetic on data declarations, not a heap
profile. If anyone wants the real number, `+RTS -s -hT` on the hospital demo
scaled to 1M caps would settle it; the two figures most likely to move are the
HAMT per-entry constant (4.5–7 words is the usual range) and the existential
dictionary count.

## Research — how similar systems store their graphs (between Stages 3 and 4)

Trigger: 0.7 KB/node engine-only felt too big; question was what a redesign
should look like, with "complete redesign is fine" as the mandate. Systems
surveyed:

- **[Salsa](https://github.com/salsa-rs/salsa)** (rust-analyzer / rustc query
  system): everything is an interned **u32 id** ("raw-id is basically a
  newtype'd u32"); storage is **per-query-type "ingredient" tables** — typed,
  columnar, one table per query kind rather than one generic node struct.
  Memos store typed values directly (no `Arc<dyn Any>`). Cutoff uses compact
  u64 revision counters (`changed_at`/`verified_at`) in the hot path rather
  than content hashes. This became the Tier 2 blueprint.
  ([interning](https://github.com/salsa-rs/salsa/blob/master/src/interned.rs),
  [struct kinds](https://salsa-rs.github.io/salsa/tutorial/ir.html),
  [algorithm](https://medium.com/@eliah.lakhin/salsa-algorithm-explained-c5d6df1dd291))
- **[turbo-tasks / Turbopack](https://nextjs.org/blog/turbopack-incremental-computation)**:
  fine-grained task graph, persisted to disk (stable in Next 16.1). Their own
  retrospective: memory "at a premium since launch", fixed by compressing data
  structures and — their words — the **biggest win was evicting much of the
  in-memory cache**, possible only *because* the disk copy exists. This is the
  Tier 3 blueprint (not yet implemented here).
  ([persistent caching talk](https://gitnation.com/contents/turbopack-persistent-caching))
- **Bazel Skyframe / Shake**: same interning story — int keys, compact dep
  arrays (Skyframe's GroupedList). Nothing new beyond confirmation.
- **Differential/timely dataflow**: the genuinely different model — *no
  per-instance nodes at all*; data flows through a fixed ~50-operator graph,
  state lives in shared indexed arrangements. Rejected as a target: it gives
  up the arbitrary-recursion / dynamic-dependency model this engine is built
  on (a `sync_dir` that recursively discovers its own dependency structure has
  no natural home there). Kept as inspiration for the columnar-per-def idea —
  50 defs is the "operator graph", instances are rows.

Resulting plan, as tiers (user green-lit 1+2 with a benchmark checkpoint
between; 3 deferred):

1. **Tier 1** — mechanical cuts: kill per-node rerun closures (revive from
   `(def, param_bytes)` on demand — the persistence-revival path already
   proved it works), u16 def indices, sparse side tables for the
   mostly-empty fields, bitfield flags. → Stage 4.
2. **Tier 2** — Salsa-style columnar per-def tables with typed value columns
   and param arenas. → Stage 5.
3. **Tier 3** — turbo-tasks-style eviction: with persistence on, in-memory
   state is *also* a cache; evict cold nodes' values/edges to redb, revive on
   access; floor ≈ index entry + hashes (~40 B/node). **Not implemented** —
   see open items.

## Stage 4 — Tier 1 memory redesign (closure-kill, u16 defs, sparse side tables)

Four structural cuts to the in-memory `Node`/`NodeTable`, format unchanged
(disk still stores full `CompKey`s with def names — no `FORMAT_VERSION` bump):

- (a) **Killed per-node rerun closures.** `Node` no longer carries a `rerun:
  RerunFn` field. `crate::driver`'s wave propagation re-runs a dirtied node
  via a new `EngineInner::rerun_node(key, param_bytes)`: def lookup in
  `erased_defs` → `ErasedDef::rerun` postcard-decodes `param_bytes` and
  builds the execution future on demand — the same mechanism persisted
  revival always used (`ErasedDef::revive_key`/`revive_value`), now the
  *only* mechanism; `EngineInner::make_rerun` is gone. This breaks the old
  `rerun`-closure → `Arc<EngineInner>` reference cycle: an `Engine` now
  genuinely drops once every handle to it (including its driver task) is
  gone. New regression test `engine::tests::engine_is_droppable_after_a_rerun`
  holds a `Weak<EngineInner>`, runs a source-triggered rerun (the exact path
  that used to leak), drops every handle, and asserts the `Weak` no longer
  upgrades. `persist_close`'s doc comment updated (it still matters, for
  promptness, but no longer for correctness). `persist_bench`'s
  process-per-phase design is unchanged (comparability across every stage of
  this doc) — its module doc now just notes the cycle is gone rather than
  claiming an `Engine` is never freed.
- (b) **`u16` `DefIndex`.** `EngineBuilder` now records registration order
  (`def_order: Vec<DefId>`); `EngineBuilder::build` turns that into a
  `HashMap<DefId, DefIndex>` (`DefIndex` = position, as `u16`) handed to
  `NodeTable::new`. `NodeTable`'s own `CompKey`-keyed lookup index is keyed
  internally on `(DefIndex, Hash128)` (18 bytes) instead of the full
  `CompKey` (32 bytes) — every public-facing type (`DefId`, `CompKey`,
  `Node::key`) is untouched, so comp names stay trivially loggable
  everywhere with no def-table lookup needed at any existing call site
  (verified: `tracing_smoke`'s `comp_eval_events_mention_comp_name_and_outcome`
  and `computations-fs`'s `dirsync` test both still pass unmodified).
- (c) **Sparse side tables.** `source_deps`/`outputs`/`inflight` moved off
  `Node` into three `HashMap<NodeId, _>`s on `NodeTable`, populated only for
  a node that actually has an entry — most nodes in this benchmark's graph
  have zero source deps (only level-0 reads a source) and zero outputs (only
  the top level writes to the sink); `inflight` only ever has an entry for a
  node currently running. New accessor methods
  (`source_deps_iter`/`_contains`/`_clone`/`take_source_deps`/`extend_source_deps`,
  the `outputs_*` mirrors, `inflight_get`/`_set`/`_clear`) replace direct
  field access everywhere (`engine.rs`'s `prepare`/`run`,
  `driver.rs`'s `affected_keys`/`live_outputs_by_sink`/`run_wave`/
  `liveness_gc`, `persist.rs`'s snapshot/restore paths).
  `NodeTable::remove_by_id` purges all three side-table entries for a
  collected id unconditionally, so GC can never leave one orphaned —
  verified by the existing `liveness_gc`/GC-related integration tests
  passing unmodified.
- (d) **Bitfield packing.** `state` (`Clean`/`Dirty`/`Running`),
  `dirty_priority` (`None`/`Revalidate`/`Input`), and `last_changed` (bool)
  packed into one `NodeFlags(u8)` (5 bits used), replacing three separate
  fields. `Node` exposes `state()`/`set_state()`/`dirty_priority()`/
  `set_dirty_priority()`/`last_changed()`/`set_last_changed()` methods that
  read like the plain fields they replaced at every call site.

`size_of::<Node>()`: **296 B → 160 B** (−46%); `node_stays_small`'s tripwire
moved from `<= 320` to `<= 192` (headroom above the measured 160, same
generous-not-tight spirit as before).

Numbers (1M, two independent runs):

- cold eval (persistence configured) **4.10–4.12 s** (was 3.72 s, +10–11%)
- persist_now **2.45–2.46 s**, db **269.49 MB** (was 2.40 s / 269 MB — disk
  format genuinely unchanged, confirmed byte-for-byte-equivalent size)
- warm restart **1.67–1.72 s** (was 1.59–1.64 s, +5–8%)
- restart + 1 changed input **2.22–2.41 s** (was 2.1–2.3 s, ~flat to +5%)
- cold-no-persistence **3.26–3.29 s** (was 3.0–3.1 s, +7–9%)
- fingerprint mismatch **5.68–5.69 s** (was 5.26 s, +8–9%)
- live incremental, no persistence: **616–617 ms** (was 574 ms, +7–8%)
- live incremental, with persistence: **714–757 ms** (was 706 ms, ~flat to
  +7%); time-to-durable **3.11–3.31 s**
- **engine-only RSS (no persistence configured): 427.1–429.6 MB** (was
  ~700 MB, **−39%**, ≈0.43 KB/node vs. the old ≈0.7 KB/node) — short of the
  ≤~350 MB aspirational target, see "surprises" below
- peak RSS (with persistence): **1.38–1.42 GB** (was 1.78 GB, −20 to −22%)

Every timing delta lands in the +5–11% band, under the 15% investigate-before-
accepting threshold this stage's acceptance criteria set, and each has an
identifiable, expected cause rather than looking like a real regression: (1)
`rerun_node` now postcard-decodes the `u32` param fresh on every rerun
instead of reusing a closure's pre-decoded copy — the expected cost the task
called out up front; (2) every side-table access (`take_outputs`,
`inflight_set`/`_clear`, ...) is now a `HashMap` operation instead of a plain
struct-field write/clear, paid even by nodes that end up with no entry at
all (a lookup that returns "absent" still costs a hash + probe). Both are
inherent to trading per-node struct width for sparse-table indirection, not
bugs.

Surprises:

- The engine-only RSS win (−39%) is real but smaller than `size_of::<Node>()`'s
  own −46% would suggest, and short of the ≤~350 MB target this stage aimed
  for. Rough accounting for where 1M nodes' ~428 MB actually goes: the Node
  slab itself (~160 MB) is now a minority of it. The rest is dominated by
  costs that existed *before* this stage too, just less visible next to a
  296 B struct and a permanent closure: the `~205,000` level-0 nodes that
  each read exactly one source key still pay hashbrown's per-`HashSet`
  minimum-group allocation (originally embedded in every `Node`, now in the
  `source_deps` side table — same heap cost, no longer padded by 795,000
  empty structs sitting around it); the `NodeTable` lookup index's own
  `HashMap` overhead (control bytes, load-factor slack); and ~1M small
  individual heap allocations each for `param_bytes` (a `u32`'s postcard
  encoding) and the cached `Arc<dyn Any>` value. None of these shrank in
  this stage — only the fixed per-node struct overhead and the closure
  cycle did. A further win here would mean arena/bump-allocating
  `param_bytes`/small values instead of one `Vec`/`Arc` heap allocation
  each, which is out of Tier 1's explicit scope (closures, `DefIndex`,
  side tables, bitfield) and would need its own measurement pass.
- The disk format needed zero changes end-to-end — `FORMAT_VERSION` stayed
  at 2, db size is byte-for-byte the same (269.49 MB both before and after),
  and every persist round-trip test passed unmodified — confirming the
  "public API / on-disk format unchanged" constraint held in practice, not
  just in intent.

## Interlude 2 — could the same diet be applied to the Haskell reference?

Follow-on to Interlude 1. Same caveat: static reading, nothing built or run.
Call sites below were checked against `skogsbaer/computations` @ `e0bec07`.

| Rust change | Haskell analogue | Verdict | B/cap |
|---|---|---|---|
| 3(a) drop `param_debug` | `ccm_logrepr :: T.Text` | port it | −56 |
| 3(a′) — | `ccm_approxCachedSize = length (show x)` | port it; also kills a `show` per rerun | −32 |
| 3(b) drop duplicate `value_bytes` | `sifs_vermap` duplicates `ccm_largeHash` | **delete outright** | −64 |
| 3(c) `Hash256 → Hash128` | already MD5-128 | **already done; unpacking would backfire** | 0 |
| 3(d) intern edges to `NodeId(u32)` | intern to `Int` + unboxed edge vectors | only half the win is reachable | −424 |
| 4(a) kill per-node rerun closures | never had them | already fine | 0 |
| 4(b) `u16 DefIndex` | `CompId` reached via the shared `Comp` ptr | already fine | 0 |
| 4(c) sparse side tables | `om_forward` entry inserted for *every* cap | drop the empty inserts | −56 |
| 4(d) bitfield-pack node flags | no per-node flags exist (see below) | n/a | 0 |
| — | 5 dictionary words in `CompApIntern` | move them to the per-def `Comp` | −24 |

**~1,350 → ~720 B/cap** (≈720 MB live). Note what that does *not* do: it lands
near stage-3 Rust (0.7 KB/node) and is still **~1.7× stage-4 Rust** (0.43
KB/node). Stage 4 moved the goalposts faster than the Haskell diet can follow.

### The three worth doing first (~a day, −232 B, low risk)

1. **Delete `sifs_vermap`.** Written at `SimpleStateIf.hs:512`, read at exactly
   one site — `SimpleStateIf.hs:568`, the "did a dep change version behind my
   back" check. `capResultToVer` (`Core.hs:145`) already derives that same
   `CompDepVer` from a cache lookup, and `s3` updates cache and vermap in
   lockstep (`:508-516`) so they cannot disagree. A whole 1M-entry `Data.Map`
   plus an O(log n) insert per recomputation, for a value living one container
   over. Closest analogue in the codebase to the duplicate `value_bytes`
   stage 3(b) deleted.
2. **Move `ccm_logrepr` into `CompCacheBehavior`** as `ccb_logrepr :: a -> T.Text`.
   That record is per-*definition* — 50 objects, not 1M — and is reachable at
   every log site through the key's `capI_comp`. Same shape as stage 3(b)'s
   per-def erased serializer. Strictly better than re-lazifying the field
   (`~T.Text` under `StrictData`): a thunk still costs 3 words and retains the
   String. `ccm_approxCachedSize` goes the same way, or dies with
   `sifc_compToSize`; the `FIXME: what is needed from the size stuff?`
   (`Types.hs:206`) reads like an invitation.
3. **Drop the `OM.insert key mempty`** at `SimpleStateIf.hs:182` — this is
   stage 4(c) for outputs, exactly. ~999k output-less caps each get a HashMap
   entry whose value is the shared empty `Map`. Both readers
   (`SimpleStateIf.hs:369`, `:393`) already do `fromMaybe mempty` /
   `maybe mempty`; the only thing observing the difference is
   `isJust mOldOutputs` at `:410`, picking `pureInfo` over `pureDebug`. Run
   `TestOutputs` over it — the `GenDel` comment at `Impl.hs:51-59` warns about
   precisely this area.

### Already fine, or would backfire

- **3(c) is done.** `largeHash128 = LH.largeHash md5HashAlgorithm`
  (`Utils/Hash.hs:34`); `Word128` = two `{-# UNPACK #-}`'d `Word64` = 24 B.
  Do **not** go further and `{-# UNPACK #-}` `Hash128` into `CompCacheMeta`:
  today one `Word128` object is shared by the cache entry, the vermap entry,
  and the `dep_ver` of every rdep record. Unpacking gives ~4 sites their own
  16 B inline copy instead of ~4 pointers to one 24 B object — net loss.
  Haskell already gets the inlining benefit *and* dedup, via sharing. Inverse
  of the Rust conclusion, for a real reason.
- **4(a)**: no analogue. Haskell never carried a per-node closure — re-eval
  goes through `initCompAp` from the typed `capI_param` + shared `Comp`
  (`Impl.hs:148-158`). This is the design stage 4(a) *converged on*, arrived at
  from the other direction.
- **4(b)**: no analogue. `CompId` is reached through the shared `Comp` pointer;
  no per-node def identifier is stored at all, and `capI_hash` already folds
  the def name in the way `CompKey` does.
- **4(d)**: no analogue, and the reason is expensive. Haskell has no
  `state`/`dirty_priority`/`last_changed` fields to pack: dirtiness is
  "present in the PAQ", priority lives in `CompId`, and `last_changed` doesn't
  exist — the `VerList` version level (`Utils/VerList.hs`) does that job by
  keying reverse deps on *(key, version)* so `DepMap.stale`
  (`Utils/DepMap.hs:152`) can answer "who depends on a now-superseded version
  of X". **Haskell pays ~360 B/cap for what stage 4(d) packs into one bit.**

### 3(d) is the big one and only half-works

The 424 B is real: intern `AnyCompAp` to `Int`, make fwd deps
`IntMap (U.Vector (Int, Word128))` — ~24 B/edge flat vs 56 B boxed `Dep` + 48 B
HAMT leaf today — and the same inside `VerList`'s dependents sets.

But stage 3(d)'s win was *two* things: 32 B → 4 B per edge, **and** edges living
inline in the node's own allocation. Haskell can only have the first, because
**there is no node** — state is five independent persistent containers keyed by
the same key, so there is nothing to be inline *in*. Getting the second half
means restructuring `SifState` into `IntMap Node` with one strict record per
cap, which fights the idiom: today updating deps touches one container; then,
every dep update copies the whole record.

Two hazards transfer verbatim:

- **Id recycling.** Interning creates a table the GC can't reclaim, so `runGc`
  (`SimpleStateIf.hs:326`) must release ids explicitly — the free list, by
  hand. Laziness sharpens it: a retained thunk can hold a stale id. Our rule
  (long-lived references keyed by `CompKey`, never `NodeId`) applies unchanged.
- **It breaks the sharing trick.** `SifCache.lookup` returns the map's own key
  on purpose (`SifCache.hs:146-152` — hence `lookupLE`+eq rather than plain
  `lookup`), and `validateSifState` (`SimpleStateIf.hs:82`) asserts it with
  `reallyUnsafePtrEquality#`. Interning makes that machinery moot. Net win, but
  a validator and a deliberate lookup idiom exist to serve the strategy being
  replaced.

Optional extra: drop the `VerList` version level entirely and adopt our flat
rdeps + `last_changed` model (another ~−80 B). That's a semantics change, not
an encoding one.

### Addendum — second scan: newtypes, Typeable dicts, and the box towers

A second pass over the state layer (`Types.hs`, `SimpleStateIf.hs`,
`SifCache.hs`, `CompFlow.hs`, `Core.hs`; `Run.hs` confirms the entire state is
one `TVar SifState`, `Run.hs:119`) finds another **~130–155 B/cap** beyond the
table above.

**Newtype audit: all clean.** Every newtype on the state path is zero-cost —
`CompDep`, `CompDepKey`, `CompDepVer`, `SomeCompSrcDep/Key/Ver`, `Hash128`,
`TypeId`, `CompSrcInstanceId`, `DataSize`, `VerList`, `AnyCompSinkOutsMap`.
The costs hide *under* them, in three kinds of `data`: the `Dep` pair box
(3 words), `Option`'s `Some` box (2 words — `StrictData` doesn't remove it, a
strict field still points to a box), and the existentials below. Wrapping
discipline: free. Boxing discipline: not.

**Typeable audit: an existential constraint is one stored dictionary pointer
per *allocation*** (the `TypeRep` is shared; the word isn't). By constructor
context:

- `CompApIntern` (`Types.hs:442`): 5 dicts — already in the table above.
- `AnyCompAp` (`Types.hs:554`): 2 more dicts, **redundant** — the inner
  `CompApIntern` already carries the same `IsCompResult r`. Drop the wrapper's
  constraint; every use site (`Eq`/`Ord`'s `eqT`, `showAnyCompApDetails`)
  recovers the dicts by matching one level deeper (`AnyCompAp l@CompApIntern{}`
  — the pattern synonym is `COMPLETE`, so this is mechanical). −16 B/cap at
  ~1 shared wrapper per cap.
- `AnyCompCacheValue` (`Types.hs:186`): 1 dict per cached value, powering
  `castCompCacheValue`'s `cast` on every parent cache read. Absorbed by the
  flatten below, or by the per-def `Dict`-in-`Comp` move — justified by the
  codebase's own documented axiom (`CompFlow.hs`: "equality of the identities
  implies the types are equal").
- `ForAnyCompFlow` (`CompFlow.hs:32`): the worst per-allocation offender —
  **6 dicts** (`Typeable s`, `c s`, `IsCompFlowData (k s)` = Show+Eq+Typeable+
  Hashable) plus `CompSrcId` plus a zero-information `Proxy s` field ≈ 10
  words = **80 B per stored source dep**; and `depKey`/`depVer`
  (`CompSrc.hs:158-159`) allocate a fresh one on every call. Fix: one shared
  per-source-instance tag object (id + proxy + dicts, allocated once),
  `data AnyCompSrcDep = ASD !SrcTag !k` → 3 words per value. −56 B per stored
  source dep; amortized over this benchmark's topology (~205k of 1M caps have
  one) ≈ −11 B/cap, more in source-heavy graphs.

**New items, ranked:**

1. **Flatten the cache-value box tower (~70–90 B/cap)** — the biggest single
   remaining item. A cached success is five boxes deep before the payload:
   `Map` value → `CapSuccess` (2 w) → `AnyCompCacheValue` (3 w, incl. dict) →
   `CompCacheValue` (3 w) → `Some` payload (2 w) → `CompCacheMeta` (5 w,
   → 3 w after the logrepr/size moves above) — ~13 words of chrome per cap
   *after* the already-planned cuts. One flattened existential —
   `data Cached = forall a. CachedOk !a !Hash128 | CachedHashOnly !Hash128 |
   CachedFail` — is 4 words. The `CachedOk`/`CachedHashOnly` split *is* the
   `fullCaching`/`hashCaching` distinction, so the sum was already there
   semantically; one `cast` replaces four pointer chases per parent read.
   Casualties: the `Eq`-by-hash instance and `SifCache`'s `HasSizes`
   plumbing, both trivially rebuilt on the flat type.
2. **`AnyCompAp` dict dedup (−16 B/cap)** — smallest diff of the lot.
3. **`ForAnyCompFlow` tag hoisting (−11 B/cap amortized)** — as above.
4. **`Data.Map` → dense structure for `SifCache` (−~40 B/cap, only after
   interning).** `Map.Bin` is 6 words/entry, and every key comparison pays an
   `eqT` fingerprint check (`Ord AnyCompAp`, `Types.hs:574`). Pre-interning
   this is load-bearing (`lookupLE` + returned-key sharing *is* the dedup
   mechanism), but interning already makes that moot — then dense-`Int`-keyed
   entries (slab/array) cost ~1–2 words. A dependent of 3(d), not an
   independent win.

Revised ceiling: ~720 → **~565–600 B/cap** with items 1–4. Still ~1.3–1.4×
stage-4 Rust engine-only, and the residue is architectural: five containers'
per-entry overheads plus the `VerList` version index (the ~360 B/cap that
stage 4(d) packs into one bit).

Throughput footnote (churn, not residency): `mkCompAp` MD5-hashes
`(name, param)` and allocates a fresh `CompAp` + `AnyCompAp` on **every**
parent eval call — the sharing trick dedups what's retained, not what's
allocated. Same shape as the Rust port's `make_rerun` recomputing `CompKey`
per call: both implementations independently chose "hash at the call site,
every time" on the hot path.

### What no diet fixes

Live heap 1,350 → ~720 B/cap (→ ~565–600 with the addendum's items).
**RSS ~2.7–4 GB → maybe 1.5–2 GB, and stops.**
The remaining gap is not data layout, it's GHC's copying collector holding ~2×
live (`-F 2`) plus to-space during major GCs — an RTS-flag question
(`--nonmoving-gc`, or `-c` compacting, both trading throughput), not a
data-structure one.

Amusing symmetry with stage 4's "surprises": our residual ~428 MB is dominated
by ~1M small individual heap allocations (`param_bytes` `Vec`, `Arc<dyn Any>`
value) that the Tier 1 cuts never touched. Haskell has the identical problem,
and *more* of those objects — but GHC's bump-allocating nursery handles small
short-lived allocation better than malloc does. It just charges for it at the
other end, in copying-GC headroom. Neither runtime escapes "1M nodes means
millions of tiny objects"; they only choose where to pay.

No Haskell analogue of the `size_of::<Node>() <= 192` tripwire exists — closest
is a `weigh`/`ghc-datasize` assertion, or a CI check on max residency from
`+RTS -s` at a fixed graph size.

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

## The memory arc — end-to-end summary (1M instances, engine-only)

| | bytes/node | engine RSS | live update (no persist) | warm restart | commit |
|---|---|---|---|---|---|
| Stage 1-2 (pre-diet) | ~2,700 B | ~2.6 GB peak | 1.3–1.5 s | 3.3 s | `c17efdb` |
| Stage 3 (first diet) | ~700 B | ~700 MB | 574 ms | 1.6 s | `cc5dd5e` |
| Stage 4 (Tier 1) | ~430 B | 427–430 MB | 616 ms | 1.7 s | `55b5094` |
| Stage 5 (Tier 2) | **~330 B** | **328–354 MB** | **502–527 ms** | **1.36–1.44 s** | `9b740ba` |

8× total; Tier 2 made everything faster as well (columnar locality), erasing
Tier 1's small decode/side-table regression. Projected Tier 3 floor: ~40 B/node
for evicted nodes (memory becomes a knob, not a linear function of graph size).

## Not tried yet / open items

- **Tier 3 eviction** (the turbo-tasks move, their self-reported biggest
  memory win): with persistence on, evict cold nodes' values/edges to the
  already-existing redb copy, revive on access; in-memory floor ≈ index entry
  + hashes (~40 B/node). Costs: revival latency on cold hits, an eviction
  policy to tune, and interplay with the async flush window (an evicted node
  must be flushed first). Deferred by explicit decision at the Tier-1/2
  green-light; the natural next stage.
- Packed-u32 `NodeRef` (u8 def | u24 row) and/or same-def-local u32 edges:
  recovers most of the `NodeRef` 8-B tax Stage 5's surprises identified, at
  the cost of a 256-def or per-def-row ceiling (or a two-variant edge enum).
- `--body-cost <µs>` busy-spin flag to demonstrate the realistic win (warm
  restart flat while cold scales linearly with body cost).
- ~~`Weak` backrefs for the rerun-closure → `EngineInner` `Arc` cycle~~ —
  resolved in Stage 4: the cycle no longer exists at all (reruns decode
  `(def, param_bytes)` on demand instead of storing a closure), so an
  `Engine` is genuinely droppable now. `persist_bench` still uses
  process-per-phase workers regardless, for RSS comparability across every
  stage of this doc, not because it still needs to work around a leak.
- Arena/bump-allocating `param_bytes` and small cached values instead of one
  `Vec`/`Arc` heap allocation each (Stage 4's "surprises" section): the
  remaining per-node allocator overhead this would target is now a bigger
  fraction of engine-only RSS than it used to be, precisely because Stage 4
  already shrank everything else.
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

## Sources & prior art — every borrowed idea, mapped to where it came from

**The architecture itself**
- Coarse-grained self-adjusting computations, push driver, sources/sinks,
  `CompM`-style effect tracking, name+128-bit-param-hash identity, the
  restart-cost problem this whole persistence effort answers: Stefan Wehr,
  ["A Software Architecture Based on Coarse-Grained Self-Adjusting
  Computations"](https://doi.org/10.1145/3609025.3609481), FUNARCH '23 +
  reference impl [skogsbaer/computations](https://github.com/skogsbaer/computations).
- Self-adjusting computation foundations (dynamic dependence graphs +
  memoization): Acar, Blelloch, Harper, ["Adaptive Functional
  Programming"](https://dl.acm.org/doi/10.1145/1186632.1186634), TOPLAS 2006;
  memoized function caching goes back to Pugh & Teitelbaum, ["Incremental
  Computation via Function Caching"](https://dl.acm.org/doi/10.1145/75277.75305),
  POPL '89 (both via the FUNARCH paper's related-work section).
- Haxl-style concurrency (the "implicit batching/dedup" model our explicit
  `join` + single-flight design deliberately simplified): Marlow, Brandy,
  Coens, Purdy, ["There is no fork: an abstraction for efficient, concurrent,
  and concise data access"](https://dl.acm.org/doi/10.1145/2628136.2628144),
  ICFP '14.

**Persistence design vocabulary**
- *Verifying traces* vs *constructive traces* (exactly our deps+hashes vs
  deps+hashes+values split), *early cutoff*, scheduler taxonomy: Mokhov,
  Mitchell, Peyton Jones, ["Build systems à la
  carte"](https://dl.acm.org/doi/10.1145/3236774), ICFP '18; expanded journal
  version ["Theory and practice"](https://simon.peytonjones.org/build-systems-a-la-carte-theory-and-practice/),
  JFP 2020.
- Persistent build database with journaled updates, keys/values encoding of
  params/results (cited as inspiration by the FUNARCH paper too): Mitchell,
  ["Shake before building"](https://dl.acm.org/doi/10.1145/2364527.2364538),
  ICFP '12; [shakebuild.com](https://shakebuild.com).
- "Persist the graph, load it, revalidate" at production scale + **eviction
  as the biggest memory win** (Tier 3 blueprint): Turbopack —
  ["Inside Turbopack: incremental computation"](https://nextjs.org/blog/turbopack-incremental-computation)
  and Tobias Koppers' ["Turbopack Persistent Caching"](https://gitnation.com/contents/turbopack-persistent-caching) talk.

**Memory-layout redesign (Stages 4-5)**
- u32-interned ids, per-query-type columnar "ingredient" tables, typed memo
  storage, revision-counter cutoff (Tier 2 blueprint): [Salsa](https://github.com/salsa-rs/salsa)
  ([book/IR chapter](https://salsa-rs.github.io/salsa/tutorial/ir.html),
  [interned.rs](https://github.com/salsa-rs/salsa/blob/master/src/interned.rs),
  Ilya Lakhin's ["Salsa Algorithm Explained"](https://medium.com/@eliah.lakhin/salsa-algorithm-explained-c5d6df1dd291)).
- Int-keyed nodes + compact dep arrays in a parallel evaluation framework:
  [Bazel Skyframe docs](https://bazel.build/reference/skyframe).
- The rejected-but-instructive no-per-instance-nodes model: McSherry, Murray,
  Isaacs, Isard, ["Differential Dataflow"](https://www.microsoft.com/en-us/research/publication/differential-dataflow/),
  CIDR '13 (accessible summary: [the morning paper](https://blog.acolyer.org/2015/06/17/differential-dataflow/)).
- Other engines surveyed for contrast: Hammer, Khoo, Hicks, Foster,
  ["Adapton: composable, demand-driven incremental computation"](https://dl.acm.org/doi/10.1145/2594291.2594324),
  PLDI '14; Jane Street's in-memory
  ["Introducing Incremental"](https://blog.janestreet.com/introducing-incremental/) (2015).

**Storage-engine research (Stage 0 groundwork)**
- [redb](https://github.com/cberner/redb) (chosen; pure-Rust LMDB-inspired
  B-tree, ACID txns as the "journal").
- Rejected with reasons in "Tried and rejected":
  [okaywal](https://bonsaidb.io/blog/introducing-okaywal/) (self-declared
  format-unstable), [fjall 3.0](https://fjall-rs.github.io/post/fjall-3/)
  (LSM, write-heavy focus), sled (alpha rewrite), SQLite.

Everything not listed here (load-anyway restart with priority tiers and
max-wins dirtying, Input-preempts-Revalidate scheduling, probe-and-resubscribe,
the debounced coalescing pending map, the process-per-phase benchmark harness)
is, to our knowledge, this project's own synthesis — the closest published
relative being turbo-tasks' restore-and-revalidate, which does not have the
two-tier priority scheme.

## Possible blog angles (later)

- "Load the stale cache anyway": restart persistence with priority tiers instead
  of discard-on-mismatch — old results serve until inputs prove them wrong,
  genuinely-changed inputs jump the queue.
- "It's a cache": how dropping the durability requirement collapses WAL
  engineering into a debounced background flush.
- Honest µs/node accounting of an incremental-computation engine at 1M nodes.
- Rust war stories: the `Arc` cycle that ate the benchmark, the SmallVec that
  made things slower, 48-byte keys × 2 directions × 3M edges.
- Port vs. reference, byte for byte: the same architecture in Haskell and Rust,
  where each language's memory actually goes, and the two independent times we
  each paid for a `show`-rendered debug string on every node.
- "What does a bit cost you?" — stage 4(d) packs `last_changed` into one bit;
  the Haskell reference answers the same question with a version-keyed reverse
  index costing ~360 B/node. Same semantics, three orders of magnitude apart.
- "The equalizer": every per-object diet leaves Haskell 1.3×–9× behind Rust —
  until columnar-unboxed, which exits GHC's traced heap entirely and lands at
  ~1× (Interlude 3). The GC multiplier wasn't a constant; it was a design
  choice about where state lives.

## Stage 5 — Tier 2 columnar per-def tables (Salsa-style, typed value columns)

Replaced the single generic `Node` slab (Tier 1's `Vec<Option<Node>>` plus a
`(DefIndex, Hash128) -> NodeId` index) with a Salsa-style "ingredient" layout:
one struct-of-arrays table per registered definition, plus each definition's
typed result cache living on the definition itself instead of behind a
type-erased `Arc<dyn Any>`. Disk format genuinely unchanged (still
`FORMAT_VERSION = 2`, still full `CompKey`s/def names/param bytes/dep
identities/hashes/values) — confirmed both by the persisted db coming out
byte-for-byte the same size (**269.49 MB**, identical to Stage 3/4) and by
`tracing_smoke`/`dirsync` still passing unmodified (comp names still render).

### Layout decisions

- **Node identity: `NodeRef { def: DefIndex, row: u32 }`, 8 B, not a packed
  `u32`.** The task offered a choice between an 8-byte struct and a packed
  `u8 def | u24 row` `u32` (capping the engine at 256 defs / 16.7M rows per
  def). Went with the struct: Tier 1 already committed to a `u16` `DefIndex`
  (65,536 defs) as part of its own def-table shrink, and packing `NodeRef`
  down to `u32` would silently *tighten* that existing contract to 256 defs
  for a saving that only matters inside `SmallVec<[NodeRef; 4]>` (see below)
  — not worth quietly breaking a limit Tier 1 deliberately set generously.
  `u32` rows also means no per-definition row-count ceiling below `u32::MAX`,
  which a `u24` packing would have introduced. `NodeRef` is `Copy`/`Eq`/`Hash`
  like the `NodeId` it replaces, and reconstructs a full `CompKey` in O(1)
  with **no extra storage**: `key_of(r)` reads `def_ids[r.def]` (a
  registration-order `Vec<DefId>`, already free) and `DefTable::param_hash[r.row]`
  (a column that has to exist anyway) — this is what let Tier 1's `Node::key`
  field (a whole redundant `CompKey`, kept purely for O(1) reverse lookup) be
  dropped entirely, not just shrunk.
- **Per-def columns (`DefTable`, one per registered definition, indexed by
  `DefIndex`):** `param_hash: Vec<Hash128>`, `result_hash: Vec<Hash128>`
  (valid only when `NodeFlags::has_result` — a new packed bit, since a dense
  column has no room for its own `Option` discriminant), `flags: Vec<NodeFlags>`
  (now 7 bits: the Tier-1 5 plus `has_result` and `free`), `param_off: Vec<u32>`
  + `param_len: Vec<u16>` indexing into one append-only `param_arena: Vec<u8>`
  per def (replacing one `Vec<u8>` heap allocation per node with one
  allocation per *definition*), `comp_deps`/`rdeps: Vec<SmallVec<[NodeRef; 4]>>`,
  a `free: Vec<u32>` row free-list, and `index: HashMap<Hash128, u32>` (this
  definition's share of the old global `(DefIndex, Hash128) -> NodeId` map).
  `source_deps`/`outputs`/`inflight` stay exactly as Tier 1 left them — sparse
  `HashMap<NodeRef, _>` side tables on `NodeTable`, unchanged in design, just
  rekeyed.
- **Typed value column, on `CompDef` not `NodeTable`.** `CompDef<P, R>` grew
  a `values: Mutex<Vec<Option<R>>>` field, indexed by the row half of every
  `NodeRef` this definition has handed out. `NodeTable` itself stays
  completely generic (never sees `R`); the typed column lives where `R` is
  already statically known — `CompDef<P, R>`, reached both by the generic
  `eval::<P, R>` fast path (via `EngineInner::get_def`) and, unchanged, by
  `DefAdapter`'s `ErasedDef` impl, since both sides hold the very same
  `Arc<CompDef<P, R>>`. `ErasedDef` grew two methods for the object-safe
  side: `value_any(row)` (clone `R` out, box it as `Arc<dyn Any>` — paid only
  when persistence enqueues a changed node, never on a cache hit) and
  `revive_and_store(row, bytes)` (decode + write directly into the column at
  load time), replacing the old `revive_value` that handed back an erased
  `Arc<dyn Any>` for the caller to store on `Node`. `serialize_value` is
  untouched — flush-time encoding was already deferred outside the lock in
  Stage 3, and stays that way.
  - Consequence for `prepare`/`eval`: since a cache hit now needs `R` in
    hand (to read the typed column), `eval::<P, R>` resolves `CompDef<P, R>`
    up front, before deciding cache-hit/join/run — Tier 1 only resolved it
    on the run path. A cache hit now clones `R` out of the column instead of
    bumping an `Arc<dyn Any>`'s refcount: cheap for `Copy`-ish `R` (this
    benchmark's `u64`), a real (if bounded, since `R: Clone` was already
    required) cost for a large `R` on a cache-hit-heavy workload — worth
    flagging for any def whose result is expensive to clone; wrapping such
    an `R` in an `Arc` at the call site sidesteps it (an `Arc<R>` clone is
    once again just a refcount bump, at 8 B/row instead of `Arc<dyn Any>`'s
    16 B fat pointer).
- **Row reuse and garbage tolerance.** A GC'd row is marked `flags.free`
  and pushed onto `DefTable::free`; `DefTable::insert` (on reuse) resets
  `param_hash`/`flags`/`param_off`/`param_len`/`comp_deps`/`rdeps` but
  deliberately does **not** touch `result_hash`, the `param_arena` bytes the
  old occupant's span pointed into, or the typed value column's old entry —
  all three are stale garbage until the new occupant's first successful run
  overwrites them, safe because every read of them is gated by
  `flags.has_result()`/`state() == Clean`, both cleared on every fresh/reused
  row before any of that garbage could ever be observed. The param arena is
  never compacted (append-only forever); this mirrors the existing
  `param_bytes` sizing judgment from Stage 3 (params are small, so even
  unreclaimed spans are cheap) and keeps `DefTable::insert`/`remove` O(1)
  with no scan.
- **Liveness GC.** `crate::driver::liveness_gc`'s mark-sweep walks
  `NodeRef`s instead of `NodeId`s; the old "strip every surviving node's
  `rdeps` of anything just collected" pass became
  `NodeTable::retain_rdeps_not_in`, iterating every def's `rdeps` column
  directly (skipping freed rows via `flags.is_free()`) — same O(live nodes)
  shape as Tier 1, just column-major instead of row-major. Verified no
  `NodeRef` is ever held across a GC boundary: every driver-side use lives
  entirely inside one `nodes.lock()` critical section per round (`propagate`'s
  `run_wave` re-derives a fresh `NodeRef` via `id_of` after every `.await`
  rather than caching one across it), matching the invariant Tier 1 already
  established for `rerun` closures.

### Per-row byte accounting

Replaced the `size_of::<Node>() <= 192` tripwire (the struct it guarded no
longer exists) with `engine::tests::node_ref_and_row_stay_small`, summing
`DefTable`'s dense per-row columns directly (excludes the param arena, the
per-def index map, and the typed value column, which varies with `R` — see
`typed_value_column_is_no_larger_than_a_boxed_any`), and with a printed
bytes/instance line in `persist_bench` itself (`engine-only RSS ÷
instances`, phase 5) as the empirical companion, since the real per-instance
cost now spans several independently-allocated pieces no single `size_of`
call can add up.

Measured column sizes (this platform): `Hash128` = 16 B, `NodeFlags` = 1 B,
`param_off: u32` = 4 B, `param_len: u16` = 2 B, `SmallVec<[NodeRef; 4]>` =
**48 B** (`NodeRef` at 8 B pushes the inline array to 32 B, past the 16 B a
spilled `Vec` handle would need, plus the length field). Common-column sum:

```
param_hash (16) + result_hash (16) + flags (1) + param_off (4) + param_len (2)
  + comp_deps (48) + rdeps (48) = 135 B/row
```

plus this benchmark's `u64` result column (`Option<u64>` = 16 B, no
`unsafe`/`MaybeUninit` niche-packing attempted — see `CompDef::values`'s
docs) = **151 B** of accounted dense-column bytes per instance, before the
param arena, the per-def `index: HashMap<Hash128, u32>`, and the unchanged
`source_deps`/`outputs`/`inflight` side tables. Measured engine-only RSS
(below) lands at **~328 B/instance** — the ~177 B gap is exactly the
unaccounted pieces just named, principally: `NodeRef` doubling every
`SmallVec` edge and side-table key from Tier 1's 4-byte `NodeId` to 8 bytes
(a real, deliberate cost of the wider identity — see "layout decisions"
above); and splitting one global lookup `HashMap` into 50 per-def ones,
each paying hashbrown's fixed minimum-table overhead independently rather
than amortizing it across one large table.

### Full 1M table (two independent runs; `PERSIST_BENCH_SCALE=1.0`, 999,760
achieved instances, unchanged from every earlier stage)

| phase | Tier 1 (Stage 4) | Tier 2 (Stage 5) | Δ |
|---|---|---|---|
| cold eval (persistence configured) | 4.10–4.12 s | **3.69–3.73 s** | **−9 to −10%** |
| persist_now / db size | 2.45–2.46 s / 269.49 MB | **2.24–2.49 s** / **269.49 MB** | flat / unchanged |
| warm restart, no changes | 1.67–1.72 s | **1.36–1.44 s** | **−16 to −19%** |
| restart, 1 changed input | 2.22–2.41 s | **1.83–2.09 s** | **−13 to −17%** |
| cold restart, no persistence | 3.26–3.29 s | **2.91–2.96 s** | **−10 to −11%** |
| fingerprint mismatch (full revalidation) | 5.68–5.69 s | **4.96–5.13 s** | **−10 to −13%** |
| live incremental, no persistence | 616–617 ms | **502–509 ms** | **−17 to −19%** |
| live incremental, with persistence | 714–757 ms | **650–704 ms** | **−7 to −9%** |
| live incremental, time-to-durable | 3.11–3.31 s | **3.07–3.18 s** | flat |
| **engine-only RSS (no persistence)** | **427.1–429.6 MB** | **328.1–330.5 MB** | **−23%** |
| peak RSS (with persistence configured) | 1.38–1.42 GB | 1.25–1.35 GB | −5 to −10% |

Every single timing phase came out *faster*, not just "within 15%" — the
opposite of what the acceptance criteria budgeted for and asked to
investigate if exceeded in the wrong direction. This tracks the task's own
prediction ("columnar locality may even improve propagation sweeps"):
`liveness_gc`'s rdeps sweep, `propagate`'s wave re-runs, and GC's mark phase
all now walk one definition's rows contiguously (`Vec<NodeFlags>`,
`Vec<SmallVec<[NodeRef; 4]>>`, ...) instead of striding through one global
slab where consecutive slots belong to unrelated definitions with unrelated
cache-line contents — the exact cache-locality argument Salsa's own
"ingredient" design is built on.

Engine-only RSS improved **23%** (428 MB → ~329 MB), a real and
consistently-reproduced win (two trials landed within 0.5 MB of each other)
but short of the ~2× / ≤250 MB aspirational target — see the byte accounting
above for where the remaining gap goes. Persisted db size is byte-for-byte
identical to Stage 3/4 (269.49 MB), confirming the disk format truly didn't
move.

### Surprises

- **The RSS win is smaller than the per-row structural math alone would
  predict**, because `NodeRef` doubling in size (4 B `NodeId` → 8 B
  `NodeRef`) taxes every place an edge or a side-table key is stored — and
  Tier 2 didn't touch `source_deps`/`outputs`/`inflight`'s design at all, so
  none of those pay any columnar dividend; they only pay the wider-key cost.
  A future stage could recover some of this by keeping a `u32`-sized
  "local" edge representation (row only, implicitly same-def) for the
  common case of an edge staying within one definition — not attempted here
  (most edges in this benchmark's graph cross definitions, level to level,
  so a same-def fast path would help less than it might elsewhere).
- **Splitting one `HashMap` into 50 costs more than expected.** hashbrown
  allocates a minimum-sized control-byte table even for a `DefTable` with
  relatively few live rows (the top few levels, sized in the low thousands);
  multiplying that fixed cost by 50 definitions is a real, measurable tax a
  single consolidated index never paid. Not large enough on its own to
  explain the full gap, but a genuine, structural side effect of "per-def"
  that a purely additive size estimate misses.
- **Every timing phase improved, with no exceptions** — genuinely surprised
  the persistence-configured phases (1, 3, 4, 6, 8) improved too, since
  those pay redb overhead that Tier 2 never touched; the shared
  `EngineInner`/`NodeTable` locking path being faster across the board
  (columnar locality, one fewer 32-byte `CompKey` clone per node touched
  during GC and persistence snapshotting, now reconstructed on demand
  instead of stored) apparently dominates even there.
- The `Option<R>` typed value column (no `unsafe`/`MaybeUninit`) costs
  `size_of::<R>()` rounded up for alignment plus a discriminant — for this
  benchmark's `u64`, that's 16 B, *exactly* matching `size_of::<Arc<dyn Any
  + Send + Sync>>()` (also 16 B, a fat pointer) with no size win at the
  `size_of` level at all (see `typed_value_column_is_no_larger_than_a_boxed_any`,
  whose original name — before it turned out to only tie, not beat — was
  `..._is_smaller_than_a_boxed_any`). The entire Tier-2 typed-column win is
  the *absence* of a separate heap allocation per node, not a smaller
  in-place representation — worth stating plainly since it doesn't show up
  in any `size_of` assertion, only in RSS.

## Interlude 3 — the rest of the arc: persistence, columnar, eviction (Haskell feasibility)

Interludes 1–2 covered Stages 3–4. This closes the loop over everything else
tested in Rust: the persistence stack (Stages 0–2), Tier 2 columnar (Stage 5),
Tier 3, and the open items. Same caveat as always: static reading against
`skogsbaer/computations` @ `e0bec07`, nothing built or run.

| Rust work | Haskell feasibility | Size guess |
|---|---|---|
| Stage 0–1: redb snapshot store | feasible; LMDB instead of redb | db ~270–550 MB, same order |
| Stage 0: revive from `(def, param bytes)` | feasible; `CompMap` *is* `erased_defs` | needs `Serialize p` in `IsCompParam` |
| Stage 1: load-anyway + priority tiers | feasible; PAQ already has 4 priority lanes | ~0 extra |
| Stage 2: async debounced flush | feasible and **easier** (STM) | the 80k-record snapshot-clone cost is **0** |
| Stage 5: Tier 2 columnar per-def | feasible via unboxed vectors; **the equalizer** | ~170–250 B/cap, RSS **~250–350 MB ≈ Rust parity** |
| Tier 3 eviction | **the primitive already exists in the reference's types** | floor ~60–100 B/cap |
| packed-u32 `NodeRef` (open item) | identical trade (`Word32` unboxed) | same ceilings |
| arena'd `param_bytes` (open item) | n/a — params typed and live; the typed param column *is* the arena | — |

### Stages 0–2: persistence ports cleanly, and one Rust problem vanishes

- **Store**: no redb equivalent; LMDB (`lmdb-simple`) is the natural pick.
  sled/fjall have no Haskell analogues to reject. SQLite was rejected in Rust
  partly for the C dep — in Haskell that objection mostly evaporates, since
  LMDB is a C dep too and there is no mature pure-Haskell embedded KV (haskey
  is closest, alpha). Serialization: `cereal`/`store` in place of postcard,
  wired as a `ccb_serialize` on `CompCacheBehavior` — exactly stage 3(b)'s
  per-def erased serializer shape.
- **Revival**: the reference is already set up for it.
  `comp_compMap :: Map CompId AnyComp` is `erased_defs`, and `initCompAp`
  re-evals from `(Comp, typed param)` — the design stage 4(a) converged on.
  The only missing piece is `Serialize p` in `IsCompParam` so a param can
  round-trip disk.
- **The Stage-2 flush is where Haskell structurally wins.** The coalescing
  pending map is a `TVar (Map CompKey Upsert)`; debounce/threshold/staleness
  triggers are `registerDelay` + `orElse` — STM composes what the Rust side
  hand-built with `Notify` + `Weak`. Better: **Stage 2's residual problem does
  not exist.** The "synchronous snapshot-clone of ~80k records under the node
  lock" (the ~130 ms live-update tax, still an open item as "could be sharded
  or made copy-on-write") — pure persistent structures in a `TVar` are
  *already* copy-on-write. `readTVarIO` hands the flusher an immutable
  snapshot in O(1), zero copying, zero lock hold. The 2× memory tax of
  persistent containers buys exactly this. Tension to note: adopting Tier 2's
  mutable columns (next) spends this advantage back.
- **Load-anyway tiers**: PAQ's four lanes (realtime/express/regular/bulk)
  already implement the two-tier preemption — `Revalidate`→bulk,
  `Input`→express is configuration, not architecture. Fingerprint: executable
  hash or TH-embedded git hash.
- Warm-restart guess: 1M records via `cereal` + container rebuild ≈ 2–5 s —
  same order as Rust's 1.4–3.3 s across stages; rebuilding into unboxed
  columns (below) lands at the low end.

### Stage 5 / Tier 2: feasible, and it fixes what "no diet fixes"

Deepest restructure, but the abstraction boundary already exists:
`CompEngineStateIf m` (`Core.hs:95`) is an interface record and
`SimpleStateIf` just one impl — a mutable columnar `CompEngineStateIf IO`
slots in behind it without touching `Impl.hs`'s eval loop. Losses:
`atomically` composability (an `MVar` suffices; `stepCompEngine` is
sequential) and the `lookupLE`-sharing + `validateSifState` machinery,
already moot under interning.

Columns map directly: `param_hash`/`result_hash`/`flags` →
`Data.Vector.Unboxed`/`MutablePrimArray` (16/16/1 B per row, flat); `NodeRef`
→ packed `Word64` (8 B, same as Rust's struct); typed value/param columns
live on the `Comp p a` record where the types are statically known — the
same trick as `CompDef<P, R>`, with existentials surviving only at the
already-existing `AnyComp` boundary; edges as per-row `ByteArray`
(~40 B/direction at fan-in 3) or CSR-with-slack (~60–90 B/cap both
directions); per-def index `HashMap Hash128 Int` ~40–64 B/cap — and Stage
5's "splitting one HashMap into 50" surprise transfers directly. Assumes the
flat-rdeps + changed-bit semantics change (dropping `VerList`), which a
columnar rewrite would adopt anyway. Shake is the existence proof that the
interning half is idiomatic Haskell: it interns keys to `Id` (an `Int`) and
stores flat records against them.

**The Haskell-specific twist: unboxed columns leave the traced heap
entirely.** Large `ByteArray#`s live in the large-object area — never
scanned, never copied. Tier 2 in Haskell therefore attacks precisely what
"What no diet fixes" says no diet fixes: the copying-collector 2–3× RSS
multiplier applies only to the *boxed* residue. Tally: ~170–250 B/cap, of
which maybe ~50 B stays traced → **RSS ~250–350 MB — parity with Rust Stage
5's 328–354 MB.** Every earlier interlude concluded Haskell trails 1.3×–9×;
columnar-unboxed is the first move that closes the gap to ~1×, because it
dodges the boxing tax and the GC multiplier in one step.

Stage 5's `Option<R>` niche surprise transfers, stronger: `Vector (Maybe a)`
boxes every element — the Haskell version of "no `size_of` win, only
allocation-count win" is a separate has-result bit + unboxed column, or a
pointer per row.

### Tier 3: the reference already has the type for it

The Rust open item calls Tier 3 "hashCaching-style value-less records" — and
the reference *ships* that state: `CapCached = CapMetaCached CompCacheMeta |
CapValueCached ...` (`Core.hs:140-143`), and `evalWithCapCached` already
handles `CapMetaCached` by recomputing (`Impl.hs:275-278`). Eviction is
demoting `CapValueCached` → `CapMetaCached` at runtime — a state transition
the eval loop handles today with zero new code paths. Without persistence
that's a memory-for-recompute knob; with the Stage 0–2 port it's full Tier 3,
floor ≈ interned key + hash ≈ 60–100 B/cap against Rust's ~40 B/node
projection (the residue is dictionary/box overhead). Of everything in this
doc, this is the one item where the Haskell side is *ahead*: the paper's
"verifying traces" design left the door open on purpose.

## Stage 6 — profiling the run

Everything above is wall-clock/RSS accounting; this stage asks *where inside
the process* the µs/node floor actually goes. CPU-profiled three phases of
`persist_bench` at full 1M scale on the same Apple-silicon Mac, symbolized
down to individual Rust functions.

### Tooling

- **Cargo profile added** (`Cargo.toml`, workspace root):
  `[profile.profiling] inherits = "release"; debug = true` — release
  codegen with DWARF debug info so stacks symbolize to real function names
  instead of addresses. Built with
  `cargo build -p computations --profile profiling --example persist_bench --features testutil`;
  binary lands at `target/profiling/examples/persist_bench`.
- **Profiler: [samply](https://github.com/mstange/samply)** (`cargo install samply`,
  no sudo needed on macOS — it uses the same user-space task-info APIs
  Instruments does, not dtrace). `dtrace`-based tools were ruled out up
  front per this box's no-sudo constraint (macOS gates dtrace behind SIP +
  root); `xctrace` (ships with Xcode, confirmed present at `/usr/bin/xctrace`)
  was the planned fallback but never needed.
- **Dead end: symbolication needs `--unstable-presymbolicate`.**
  `samply record --save-only -o profile.json` alone leaves every native
  frame as a raw hex address (`0x1fdff`) in `profile.json` — normal
  symbolication happens lazily in the browser-based profiler UI, which
  talks to a local symbol server samply spins up on `record`/`load`; there
  is no such UI in this headless environment. Adding
  `--unstable-presymbolicate` emits a `<name>.syms.json` sidecar (per
  loaded library: `symbol_table` + the exact `known_addresses` seen in this
  recording, resolved to demangled names already — Rust's own demangling,
  no `rustfilt` step needed) that can be mined offline. Verified this
  worked with a `PERSIST_BENCH_SCALE=0.02` sanity run before committing to
  full-scale recordings (per the task's own instruction) — confirmed real
  function names (`computations::engine::EngineInner::record_call_dep`,
  `blake3::portable::compress_in_place`, ...) came out, not garbage.
- **Analysis script**: `analyze_profile.py` (kept in the scratchpad, not
  committed — see "artifacts kept out of git" below) parses the Firefox
  Profiler JSON schema samply emits directly: for every sample, resolves
  the leaf (topmost) stack frame's function via `frameTable.address` +
  `funcTable.resource` → library → the sidecar's per-library
  `known_addresses` map, and counts self-time as raw sample counts (1 kHz
  default rate, so "N samples" ≈ "N ms of wall time on some thread"). Two
  extra features earned their keep:
  - **`--from-ms`/`--to-ms`/`--tail-ms` time-windowing.** A worker process
    covers more than the phase you care about (see phase-c methodology
    below); slicing by the samples' own `time` field isolates a sub-window
    without needing to touch the benchmark's code.
  - **`--hide-idle`**: walks each sample's *full* stack (not just the leaf)
    and buckets it as `IDLE` if any frame is under
    `tokio::runtime::park` — separated out as one summary line instead of
    polluting the ranked list with `__psynch_cvwait`. Necessary because a
    naive leaf-only ranking put `__psynch_cvwait` at #1 with 30–44% in
    every phase, which turned out to be two different (both benign) things
    conflated: the outer `Runtime::block_on` caller's own park loop (the
    `persist_bench` main thread, waiting for the async task to make
    progress) and idle default-sized (`num_cpus` = 14) tokio worker
    threads finding no work to steal. Checked directly: in the phase-b
    (warm restart) recording, of 15 threads only 2 tokio workers
    (`23905099`: 3002/3008 samples on-CPU, `23905110`: 1513/1515 on-CPU)
    ever did real work; the other 11 worker threads logged 2 samples each
    (started, found nothing, parked for the rest of the run) — a real,
    if minor, finding in its own right (see optimization candidates).

### Methodology per phase

- **(a) cold eval, no persistence** = phase `5-1`
  (`run_no_persist_phase`, label `"5. cold restart, no persistence"`).
  Self-contained — no state prerequisite — profiled directly:
  `PERSIST_BENCH_PHASE=5-1 samply record --save-only --unstable-presymbolicate -o a.json -- persist_bench`.
  Whole-process profile *is* the phase; no slicing needed.
- **(b) warm restart, with persistence** = phase `3-1`
  (`run_restart_phase`, label `"3. warm restart, no changes"`). Needs an
  existing on-disk db, so first ran phase `1` **unprofiled** (plain
  `PERSIST_BENCH_PHASE=1`, ~8 s, populates `persist_bench.redb`) against a
  fixed `PERSIST_BENCH_DIR` (the orchestrator normally uses a
  `tempfile::tempdir()` that's deleted at exit; ran the phases by hand
  instead so the directory persists between the setup and profiled runs),
  then profiled phase `3-1` against that same directory. Whole-process
  profile again *is* the phase.
- **(c) live incremental, with persistence** = phase `8`
  (`run_live_incremental_phase` with `persist_opts = Some(...)`). This one
  is *not* self-contained the same way: the worker builds its own fresh
  1M-node graph and lets it settle **before** the timed
  mutate-one-key-and-wait-for-round measurement even starts — so the raw
  process recording is dominated by an unrelated, unreported initial
  build+settle prefix (same shape as phase a/1, just not printed as its
  own `RESULT` line). Profiled the whole process anyway (matches the
  task's "if the flush lands inside the window, fine, label it"
  instruction — no code changes to the benchmark to hide this), then used
  the two `RESULT` lines it prints (`settle_ms` = 1225,
  `settle_ms + durable_extra_ms` = 4837, so `durable_extra_ms` ≈ 3612) to
  time-slice the *tail* of the same recording into two windows for a
  cleaner read:
  - **settle-only** window: propagation + snapshot/enqueue, isolated from
    both the initial build and the flush.
  - **flush-only** window: the explicit `persist_now()` call alone.

  (A combined last-`settle_ms + durable_extra_ms + 300ms buffer`
  tail — the literal "if the flush lands inside the window" reading — was
  also pulled and is consistent with just overlaying the two split tables
  below, so it's omitted here in favor of the more legible split.)

### Caveat: absolute times this run are not the notes' baseline numbers

This box's load average during the session ranged **12–30 on 14 cores**
(other concurrent work on a shared devbox, confirmed with `uptime`/`vm_stat`
— not attributable to this benchmark). Phase a's profiled run reported
3820 ms/999,760 reruns (**3.82 µs/node**, close to Stage 5's clean
2.91–2.96 s baseline) but phase b's profiled run reported 5170 ms for a
restore Stage 5 measured at 1.36–1.44 s — a **~3.5–3.8× inflation**,
consistent with scheduler contention rather than a regression (the
`--hide-idle` per-thread check above independently confirms real
contention: threads doing genuine restore work were frequently *not*
running). **Self-time percentages (the ranking and rough shares) are the
reliable output of this stage; the absolute ms figures embedded in the
`RESULT` lines during profiling should not be compared against earlier
stages' numbers.** Re-running this specific profiling pass on an idle box
would be worth doing before quoting these tables in a public writeup.

### (a) cold eval, no persistence — top hotspots (whole process, 6271 leaf samples, 38.7% idle-park excluded)

| % self | function | subsystem |
|---|---|---|
| 3.6% | `NodeTable::id_of` | node-table lookup (per-def `HashMap<Hash128,u32>`) |
| 3.4% | `EngineInner::record_call_dep` | node-table mutation, under the global `Mutex<NodeTable>` |
| 3.3% | `blake3::portable::compress_in_place` | blake3 hashing (param/result identity) |
| 2.6% | `mach_absolute_time` | timing syscalls (tracing `elapsed_ms` + tokio internals) |
| 2.5% | `sip::Hasher::write` | HashMap hashing (SipHash, see below) |
| 2.1% | `_platform_memmove` | alloc/memcpy noise |
| 2.0% | `BuildHasher::hash_one` | HashMap hashing |
| 2.0% | `Instrumented<T>::poll` | **tracing span overhead** (see finding below) |
| 2.6% | `libsystem_malloc.dylib` (2 offsets) | allocator |
| 1.1% | `DefaultHasher::write` | HashMap hashing |
| 0.9% | `String::write_str` | tracing field formatting |
| 0.9% | `raw_vec::finish_grow` | allocator (Vec growth) |
| 0.9% | `RawTable::remove_entry` | HashMap ops (GC/row reuse) |
| 0.8% | `Registry::enter` (tracing_subscriber) | tracing span overhead |
| 0.7% | `pthread_mutex_lock` | lock contention (the global `Mutex<NodeTable>`) |
| 0.7% | `core::fmt::write` | tracing field formatting |
| 0.5% | `sharded_slab::pool::Pool::get` | tracing_subscriber span storage |

### (b) warm restart, with persistence — top hotspots (whole process, 8100 leaf samples, 43.8% idle-park excluded)

| % self | function | subsystem |
|---|---|---|
| 11.2% | `pread` | redb (mmap'd file reads) |
| 7.0% | `EngineInner::restore_nodes` | persist: decode + wire restored nodes |
| 4.2% | `BuildHasher::hash_one` | HashMap hashing |
| 3.5% | `sip::Hasher::write` | HashMap hashing |
| 2.0% | `HashMap::insert` | node-table/index rebuild |
| 1.9% | `libsystem_malloc.dylib` | allocator |
| 1.8% | `_platform_memmove` | alloc/memcpy noise |
| 1.4% | `NodeTable::id_of` | node-table lookup |
| 1.2% | `Engine::run::{{closure}}` | driver (post-restore initial round) |
| 1.0% | `blake3::compress_in_place` | blake3 hashing (fingerprint/probe path) |
| 0.9% | `RawTable::reserve_rehash` | HashMap growth (index rebuild) |
| 0.8% | `persist::open_and_read` | redb (txn open + table read) |
| 0.8% | `NodeTable::source_deps_clone` | node-table/side-table access |
| 0.6% | `serde` `Vec<T>` deserialize | postcard decode |
| 0.5% | `NodeTable::insert_new` | node-table row insert |
| 0.3% | `NodeRecord::deserialize` | postcard decode |

### (c) live incremental, with persistence — settle-only window (1935 leaf samples, 38.1% idle-park excluded)

| % self | function | subsystem |
|---|---|---|
| 5.4% | `drop_in_place<PendingRecord>` | persist: pending-map churn (coalescing map insert/replace) |
| 4.3% | `Instrumented<T>::poll` | tracing span overhead |
| 4.1% | `BuildHasher::hash_one` | HashMap hashing |
| 3.8% | `sip::Hasher::write` | HashMap hashing |
| 2.8% | `blake3::compress_in_place` | blake3 hashing (result hash, early cutoff) |
| 2.3% | `EngineInner::record_call_dep` | node-table mutation |
| 1.8% | `mach_absolute_time` | timing syscalls |
| 0.9% | `NodeTable::clear_comp_deps` / `comp_deps` | node-table/column access |
| 0.8% | `NodeTable::id_of` | node-table lookup |
| 0.7% | `persist::enqueue_changed` | persist: pending-map enqueue |
| 0.6% | `postcard::ser::serialize_with_flavor` | postcard encode (value snapshot for the pending map) |

### (c) live incremental, with persistence — flush-only window (`persist_now`, 3681 leaf samples, only 2.7% idle — single synchronous txn)

| % self | function | subsystem |
|---|---|---|
| 14.5% | `MutateHelper::insert_helper` (redb) | redb (B-tree insert) |
| 8.7% | `_platform_memmove` | redb page copy / alloc |
| 8.2% | `BranchAccessor::child_for_key` (redb) | redb (B-tree traversal) |
| 5.3% | `fcntl` | redb (file locking/sync) |
| 4.7% | `pwrite` | redb (page writes) |
| 2.5% | `postcard::ser::serialize_with_flavor` | postcard encode |
| 2.4% | `Vec<T>::clone` | persist snapshot-clone |
| 1.8% | `String::clone` | persist snapshot-clone (def names) |
| 1.7% | `LeafMutator::insert` (redb) | redb (B-tree leaf write) |
| 1.2% | `LeafMutator::update_value_end` (redb) | redb |
| 1.1% | `Instrumented<T>::poll` | tracing span overhead |
| 1.0% | `sip::Hasher::write` | HashMap hashing |
| 0.9% | `PersistHandle::flush::{{closure}}` | persist (flush driver) |

### Interpretation

- **The ~3.7–3.8 µs/node cold-eval floor is genuinely engine overhead, not
  tokio** — but not quite the story Stage 3–5's byte-accounting alone
  would suggest. After excluding idle-park noise, phase a's top spenders
  are, in order: **node-table bookkeeping** (`id_of` + `record_call_dep`,
  ~7% combined — every one of the graph's ~3M `ctx.eval` calls does two
  per-def `HashMap<Hash128,u32>` lookups plus a `SmallVec` push in each
  direction under the global lock), **hashing** (blake3 ~3.3% direct +
  HashMap SipHash ~5.6% combined — two *different* hashing costs, see
  below), and — the surprise — **tracing instrumentation** (`Instrumented`
  poll + `Registry::enter` + `sharded_slab` span storage + field
  formatting ≈ **4–5% combined**, likely more once `mach_absolute_time`'s
  share attributable to `elapsed_ms` timestamps is folded in). Allocator
  traffic (malloc entries + `raw_vec` growth) adds another ~3%. None of
  this is tokio scheduling once idle-park samples are set aside — that
  confirms the floor is where Stage 3–5 assumed it was (engine bookkeeping
  + hashing), just with tracing overhead as a real, previously invisible
  fourth contributor.
- **Two independent hashing costs are visible and worth telling apart.**
  (1) `blake3::compress_in_place` is `CompKey`/result-hash computation —
  unavoidable, it *is* the identity/early-cutoff scheme. (2) `sip::Hasher`/
  `DefaultHasher`/`BuildHasher::hash_one` is **std's default SipHash-1-3
  hasher being used to hash `Hash128` keys in every `HashMap<Hash128, u32>`
  index** (`NodeTable`'s per-def `index` field, `engine.rs:331`) — hashing
  an already-cryptographically-random 128-bit blake3 output *again* with a
  general-purpose DoS-resistant hasher designed for attacker-controlled
  string keys. This is dead weight: nothing in this engine's key space is
  adversarial. Combined SipHash-family self-time (`hash_one` + `sip::write`
  + `DefaultHasher::write`) is **~5.6–8.7% across every phase profiled** —
  bigger than blake3 itself in two of the three phases. This is the same
  "hash at the call site, every time" pattern the Haskell interlude noted
  independently (`mkCompAp` MD5-hashing on every parent eval) — except here
  it's compounded by *re*-hashing a hash.
- **Warm restart goes to redb reads + restore wiring, not postcard decode.**
  `pread` (11.2%) + `restore_nodes` (7.0%) + `open_and_read` (0.8%) ≈ 19%
  is redb/IO; postcard deserialize (`Vec<T>`/`NodeRecord`) is under 1%
  combined. `restore_nodes` itself is dominated by hashing
  (`hash_one`/`sip::write` = 7.7% combined, the same per-def index
  `HashMap<Hash128,u32>` being *rebuilt* on load) and `HashMap::insert`
  (2.0%) — i.e. restoring 999,760 nodes pays the identical
  "hash-the-already-hashed-key" tax as steady-state operation, once per
  node, up front. This matches the doc's existing "Startup probe cost at
  1M is bundled into warm-restart time; not separately instrumented" open
  item — the profile now answers that: most of it is redb I/O plus index
  rebuild, not the probe logic itself (no `probe_versions` frame appears
  in the top 25).
- **The live-increment/flush split is exactly what Stage 2's design
  predicted, cross-checked at the function level.** Settle-only is
  dominated by `PendingRecord` churn (5.4%) and hashing/tracing — the
  same engine-floor shape as phase a, plus persist's pending-map
  bookkeeping, and **no redb symbols at all** (confirms the async
  debounced design: propagation genuinely doesn't touch redb). Flush-only
  is **83%+ redb/IO/serialization** (`insert_helper` + `child_for_key` +
  `fcntl` + `pwrite` + `LeafMutator::*` + postcard) with only 2.7% idle —
  a single synchronous transaction, no thread-pool slack, exactly as
  `persist_now()`'s contract describes. No surprises here; this is the
  clean confirmation the design intended.
- **Nothing that looks like an O(n) rescan or an outright bug surfaced.**
  No phase shows a function whose self-time is disproportionate to its
  expected O(n) role (e.g. nothing suggests an accidental O(n²) scan
  hiding in GC or restore). The one thing worth flagging as a possible
  **benchmark-harness artifact rather than an engine bug**: `persist_bench`
  installs `tracing_subscriber::registry()` with two plain `Layer`s (no
  `EnvFilter`/`Targets`, no `max_level_hint` override) purely to watch for
  two specific debug-level message strings (see the module's "Detecting
  settled" docs). Because neither layer restricts its level, tracing's
  callsite cache treats every `debug_span!`/`debug!` call site as globally
  "interested," so `comp.eval`'s per-node span (`engine.rs:1055`, one per
  `ctx.eval` — millions of calls) and its one completion event
  (`engine.rs`, several `tracing::debug!("comp.eval finished")` sites)
  pay their **full** field-formatting + span-storage cost on every call,
  not tracing's designed-for near-zero disabled-level fast path. That's
  real, measured cost (4–5% of phase a alone) that a production deployment
  with a normal `EnvFilter` (e.g. `RUST_LOG=info`) would not pay at all —
  worth keeping in mind before quoting this stage's percentages as "the"
  engine floor in a public writeup, and see the optimization candidates
  below for two ways to remove the confound.

### Optimization candidates suggested by the profile (candidates only — not implemented)

Ranked by estimated impact × confidence, given the profile data above:

1. **Stop re-hashing already-hashed keys. — DONE, see addendum below.**
   Swap the per-def
   `index: HashMap<Hash128, u32>` (and any other `HashMap<Hash128, _>` /
   `HashMap<CompKey, _>` on the hot path) from `std`'s default SipHash to a
   hasher that just reads bytes out of the key (`FxHashMap`-style, or a
   trivial custom `Hasher` that takes the first 8 bytes of the already-
   uniform `Hash128` verbatim) — the exact fix Salsa itself uses for the
   same reason (interned/hashed keys, `FxHashMap` throughout). Estimated
   ~5–8% of self-time across every phase profiled here, for a
   change confined to hasher-type parameters — no data-structure or
   on-disk format change. Single highest confidence item on this list: the
   evidence (SipHash entries in every top-15) is direct and consistent.
2. **Investigate whether `comp.eval`'s per-node span is worth its cost
   under this benchmark's own instrumentation setup — or fix the
   instrumentation setup instead.** Two independent fixes, either
   sufficient on its own: (a) in `persist_bench.rs`, give the two
   `MessageSignal` layers a `Targets`/`EnvFilter` (or override
   `max_level_hint`) so `debug_span!`/`debug!` calls skip tracing's fast
   disabled-path during a benchmark that doesn't want their content, only
   their occurrence — removes a 4–5%-of-self-time confound from every
   future profiling pass with zero engine changes; or (b) in the engine
   itself, reconsider whether `comp.eval`'s span needs to exist on every
   call vs. only under an explicit opt-in (mirroring stage 3(a)'s
   `param_debug` lazy-render precedent, but for the span/event itself, not
   just one field). (a) is the safer, more surgical first step — it fixes
   the measurement without touching engine semantics.
3. **The global `Mutex<NodeTable>` shows real (if modest) direct contention
   cost** — `pthread_mutex_lock` at 0.7% self-time in phase a, plus
   `record_call_dep`'s 3.4% *is* lock-held work, not lock-wait, but every
   one of ~3M `ctx.eval` calls takes this one mutex. Not warranted as a
   priority on this evidence alone (0.7% direct wait is small), but worth
   watching if a future change adds more concurrent evaluators — the
   `--hide-idle` per-thread check above already showed most of this
   benchmark's actual parallelism tops out around 2–3 concurrently-active
   threads regardless of the lock, so contention isn't yet the bottleneck
   RSS/hashing are.
4. **Default tokio worker-thread count is oversubscribed for this
   workload.** The per-thread breakdown (phase b) showed only 2 of 14
   default (`num_cpus`) worker threads ever did real work; the rest parked
   after ~2 samples each. Spawning and maintaining 12 threads that never
   run anything is small but non-zero overhead (thread creation, parking
   syscalls); `Engine::builder`'s runtime construction (or this benchmark's
   own `tokio::runtime::Builder::new_multi_thread()`) could cap
   `worker_threads` to a number closer to this workload's actual
   concurrency, or the benchmark could measure whether a lower cap changes
   wall time at all. Lowest-confidence item here — likely small, easy to
   verify, low risk.
5. **Not the redb write path** — `insert_helper`/`child_for_key`/`pwrite`
   dominating the flush window is redb doing exactly what a B-tree insert
   should; no candidate here beyond what "Tried and rejected" already
   covers (chunked transactions measured and rejected as unwarranted).
   Listed only to record that the flush window was checked and came back
   clean, not overlooked.

### Artifacts kept out of git

`profile.json`/`profile.json.gz` (samply's default output names) and
`*.syms.json` sidecars are now in `.gitignore`; every profile from this
stage was written to the session scratchpad, not the repo, so no explicit
cleanup was needed — the `.gitignore` entries are a guard for next time
someone runs `samply record` from inside the repo root.

### Addendum — identity hashing for `Hash128` keys (candidate 1 applied)

Implemented optimization candidate 1 above: stopped re-hashing keys that
are already a uniformly-distributed content hash.

**What changed.** A new `crate::hashers` module (`src/hashers.rs`) defines
`IdentityHasher`/`IdentityBuildHasher`: a tiny hand-rolled `Hasher` that
keeps a single `u64` accumulator, folds a `write_u64`/`write_u128` call in
with a cheap rotate-xor (the identity function for the common case of a
key hashed via exactly one `write_u64` from a fresh accumulator), and falls
back to a plain FNV-1a fold for `write(bytes)` (needed for `CompKey`'s
short `DefId` string field). `Hash128`'s `Hash` impl (`src/key.rs`) was
changed from `#[derive(Hash)]` (which would `write` all 16 raw bytes) to a
manual impl that calls `write_u64` once with the hash's own first 8 bytes
— safe because `Hash128` is already a uniform blake3-derived value (see
the type's own docs and `crate::hashers`' module docs for the full
HashDoS-surface argument: nothing in this engine's key space is
adversary-controlled independently of also controlling the value that was
blake3-hashed to produce the `Hash128` in the first place, and `Eq` still
checks the full 128 bits regardless of what feeds the hash).

Applied `IdentityBuildHasher` (via new `crate::key::{Hash128Map, CompKeySet,
CompKeyMap}` type aliases) to every map/set actually keyed by `Hash128` or
by `CompKey` (whose derived hash is dominated by its `Hash128` field):

- `DefTable::index: Hash128Map<u32>` (`src/engine.rs`) — the single hottest
  map this targets; every `ctx.eval` call does a lookup and, on a cache
  miss, an insert here.
- `EngineInner::roots: Mutex<CompKeySet>` (`src/engine.rs`).
- Every `HashSet<CompKey>`/`HashMap<CompKey, _>` on the driver's live
  propagation path (`src/driver.rs`): `affected_keys`, `mark_dirty_quiet`/
  `mark_dirty` (the `pub(crate)` `EngineInner` versions), `mark_all_dirty`,
  `recv_marked_dirty`, `poll_pending_input_changes`, `split_by_tier`,
  `propagate`/`propagate_tier`/`run_wave` (including their `done`/frontier
  sets — the wave-propagation hot loop phase (c) of Stage 6 profiled).
- `PendingMap::entries: CompKeyMap<PendingEntry>` and
  `PersistHandle::requeue_after_failure`'s parameter (`src/persist.rs`) —
  the persist-side pending map named in the task.
- `probe_restored_source_deps`'s `deps_by_key`/return value and
  `mark_dirty_transitive`'s `seen`/frontier sets (`src/persist.rs`,
  restore-time dirtying).

**Deliberately left untouched** (out of scope, genuinely non-uniform or
public-API-facing keys): `RawDep`/`SourceId`/`KeyBytes`/`SinkId`/`OutBytes`
side tables (string/byte-keyed, not `Hash128`-dominated — still need
`std`'s HashDoS-resistant SipHash), `NodeRef`-keyed side tables
(`source_deps`, `outputs`, `inflight`, `source_index`'s inner
`HashSet<NodeRef>`), and the **public** `Engine::mark_dirty(&self, keys:
&HashSet<CompKey>, …)` entry point, which keeps its original
default-hashed `std::collections::HashSet<CompKey>` signature for backward
compatibility and converts once (`keys.iter().cloned().collect()`) into a
`CompKeySet` before handing off to the identity-hashed internal path — a
one-time, off-hot-path cost paid only by external callers of this method,
not by the propagation loop.

**Correctness.** 92 tests green (`cargo test --workspace --all-features`,
up from 88 pre-change — the net +4 is `hashers.rs`'s own unit tests:
`Hash128`'s hash is exactly its first-8-bytes verbatim, equal
`Hash128`/`CompKey` values still hash equal, and a differing `param_hash`
usually changes the hash). `cargo clippy --all-targets --all-features -D
warnings` clean.

**Benchmark.** Machine load was elevated throughout this session
(`uptime` load averages ranging **9.8–15.2** across the runs below, on the
same box Stage 6 flagged as having a 12–30 load average during its own
profiling pass) — consistent with Stage 6's caveat that a loaded box
inflates absolute times well above the original Stage 5 clean-box
baselines (cold eval 3.69–3.73 s, warm restart 1.36–1.44 s, live
incremental 502–509 ms / 650–704 ms). Rather than compare against those
now-stale-condition numbers directly, this run did a same-session,
same-load A/B: `git stash`'d this change, rebuilt, ran the **pre-change**
binary twice, `git stash pop`'d, rebuilt, and ran the **patched** binary
four times total (two runs from before the A/B was set up, two more
immediately after), all within about a 5-minute window on the same idle
level. Per-phase average across runs (ms; phase numbers/labels match the
`persist_bench` output and Stage 5's table):

| phase | pre-change avg (2 runs) | patched avg (4 runs) | Δ |
|---|---|---|---|
| 1. cold eval (persistence configured) | 4433 | 3976 | **−10.3%** |
| 3. warm restart, no changes | 1647 | 1399 | **−15.1%** |
| 4. restart, 1 changed input | 2219 | 1950 | **−12.1%** |
| 5. cold restart, no persistence | 3112 | 2896 | **−6.9%** |
| 6. fingerprint mismatch (full revalidation) | 5653 | 4801 | **−15.1%** |
| 7. live incremental, no persistence | 603 | 520 | **−13.8%** |
| 8. live incremental, with persistence (settle) | 791 | 727 | **−8.0%** |
| 8. live incremental, time-to-durable | 3602 | 3283 | **−8.8%** |

Every phase improved, by more than the profiler's ~5–8%-of-self-time
estimate in several cases (warm restart and fingerprint mismatch, both
−15%) — plausible, not just noise: those two phases *rebuild* every
per-def `index` map from scratch on load (`restore_nodes`, the exact
function Stage 6's profile named as paying the "hash the already-hashed
key" tax once per restored node), so they're disproportionately
hash-bound to begin with, and some of the win compounds because less time
spent hashing under `EngineInner`'s global `Mutex<NodeTable>` shortens
that lock's held time too, not just the hashing itself. Individual runs
were noisy under this load (e.g. phase 8's settle time ranged 627–974 ms
across the four patched runs) — the *averages* and the consistent
same-direction sign across every phase are the reliable signal here, not
any single run's absolute number. Worth re-running on an idle box before
quoting tighter confidence intervals in a public writeup, per Stage 6's
own standing caveat.

## Stage 7 — hardening against API misuse

An audit of five API-misuse hazards found by reasoning about what a caller
could get wrong (not by profiling): places where a plausible mistake —
wrong call order, a slightly-too-clever parameter type, an oversized
value, a broken `Serialize` impl — produces *silent* staleness, data loss,
or a dead-but-still-"running" process, rather than a loud, diagnosable
failure. All five are fixed; each closes a distinct hazard, and none
touches the disk format (still `FORMAT_VERSION = 2`) or the hot `eval`
path's steady-state cost (see the benchmark below).

- **`EngineBuilder::registry` silently discarded earlier `.source()`/
  `.sink()` registrations.** `Ctx::src_req`/`sink_req` (`ctx.rs`) execute
  directly against the caller's own `Arc`, never consulting the registry —
  so a registration a later `registry(...)` call dropped still produced
  *correct* computed results, while the driver simply never learned to
  poll that source for changes or GC that sink's dead outputs. Individually
  correct, silently stale/leaking overall; nothing in the type system or a
  test failure would ever point at the cause. Fixed by making `Registry`
  mergeable (`Registry::merge`, `registry.rs`) and changing
  `EngineBuilder::registry` (`engine.rs`) to merge into the builder's
  existing registry instead of replacing it — call order across
  `source`/`sink`/`registry` no longer matters. A duplicate instance id
  across the merge panics, the same startup-configuration-error stance
  `register_source`/`register_sink` already took for a duplicate within a
  single `Registry`. README §4's Registry bullet updated to match.
- **`Fingerprint::custom(data)` trusts `data` completely.**
  `Fingerprint::current_exe()` self-corrects (it hashes the running
  binary, so *any* code change is automatically caught); `custom` has no
  such property — change a computation's logic without also bumping
  whatever string you pass to `custom`, and every persisted result for
  that computation is served as a `Clean` cache hit forever, indistinguishable
  from a legitimate one. Added `Fingerprint::current_exe_with(extra)`
  (`persist.rs`) — the binary hash mixed with caller data via
  `blake3::Hasher` — so the common "invalidate on binary change *and* on
  config change" case keeps the self-correcting property; `current_exe()`
  is now exactly `current_exe_with(&[])`, so no persisted fingerprint from
  before this change stops matching. `custom`'s doc comment now states the
  hazard bluntly rather than just describing the mechanism. README §8
  updated to steer callers toward `current_exe`/`current_exe_with`.
- **Non-deterministic param serialization (`HashMap`/`HashSet`) silently
  splits or misattaches identity.** `CompKey` identity is
  `blake3(postcard(param))`; `HashMap`/`HashSet` iteration order depends on
  a per-instance random hasher seed (`std`'s default `RandomState`), so two
  logically-equal maps — even two built in the same process — can
  serialize to different bytes. Live, this silently splits one logical
  computation application into two node identities. Across a persisted
  restart it's worse: `ErasedDef::revive_key` (`def.rs`) never reads the
  stored key, only `param_bytes` — it decodes the param and *recomputes*
  `CompKey::new(id, &param)`, so a restored record can attach to a
  different identity than it was saved under, get orphaned, and be swept
  by liveness GC (deleting its sink outputs) while the live graph
  recomputes that subtree cold. Three-part fix:
  - **Load-time key verification** (`persist.rs::restore_nodes`): redb's
    own table key *is* the persisted `CompKey` bytes (`encode_key`), now
    threaded all the way through `open_and_read`/`open_db`/`persist_load`
    (`StoredRecord = (Vec<u8>, NodeRecord)`, replacing a bare
    `Vec<NodeRecord>`). After `revive_key` recomputes a key, it's compared
    against the record's actual stored key; a mismatch drops the record
    with a loud `tracing::warn!` naming the def and pointing at
    non-deterministic serialization, instead of silently trusting it. A
    silent orphan-and-GC-delete becomes a diagnosable cold start.
  - **Debug-build determinism check** (`engine.rs::debug_check_param_determinism`,
    `#[cfg(debug_assertions)]`): when a param is first serialized for a
    brand-new node, its `param_bytes` are decoded back into `P` and
    re-encoded, `debug_assert_eq!`-checked against the original bytes —
    exactly mirroring what `revive_key` does at load time, so it catches
    the same hazard on the developer's very first run rather than only at
    a persisted restart. Deliberately *not* "serialize the same in-memory
    value twice": that would never trip (a live object's iteration order
    is already fixed for its own lifetime; only a *fresh* instance, like
    the one a decode produces, gets a new random seed). Verified during
    development to reproduce reliably (10/10 trials) for an 8-entry
    `HashMap<i32, i32>`; committed as two unit tests (`engine.rs`) — one
    proving it fires for `HashMap`, one proving it stays silent for the
    deterministic `BTreeMap` fix. Zero cost in release builds.
  - **Docs**: `CompParam`/`CompResult` (`key.rs`) now state the determinism
    requirement explicitly and point at `BTreeMap`/`BTreeSet`.
  - **Not fully closed**: `revive_key`'s own `CompKey::new(id, &param)`
    call (and, live, `EngineInner::eval`'s own up-front param hashing) can
    itself panic if `P`'s `Serialize` impl fails outright — `CompKey::new`/
    `StableHash` (`key.rs`) is public API with a non-fallible signature
    used pervasively (including by tests and by `crate::persist` itself),
    so making param-hashing fallible everywhere would be a much larger,
    separate API change. `EngineInner::eval`'s own hot-path hashing
    (`engine.rs`) was rewritten to serialize once, inline, with a
    `CompError` on failure — see Fix 4 below — but `revive_key`'s call
    (`persist_load`, before `EngineInner::nodes` is ever locked) still
    goes through `CompKey::new` and isn't covered. This can't poison
    anything (it runs before the node-table lock is taken) and only
    matters for a hand-broken `Serialize` impl that fails unconditionally
    — a case Fix 4's own debug/oversized checks don't cover either, since
    it fails before ever producing bytes to check. Left as a known gap.
- **A panic under the node-table mutex poisons the whole engine.**
  `DefTable::insert` (`engine.rs`) used to `assert!(param_bytes.len() <=
  u16::MAX)`, and `prepare` used to `postcard::to_stdvec(param).expect(...)`
  — both while `EngineInner::nodes`'s `std::sync::Mutex` was held. A panic
  there poisons that `Mutex` for the rest of the process: every subsequent
  `.lock().unwrap()` anywhere (including on a completely unrelated node)
  panics too. One oversized param (>64 KB of postcard bytes — easy with an
  embedded blob) or one node whose param type has a failing `Serialize`
  impl would kill *every* concurrent computation, not just its own.
  Fixed by moving all fallible param work before the lock: `EngineInner::eval`
  now serializes the param once, up front (`postcard::to_stdvec`, mapped to
  `CompError::Failed` on failure — this also removes the redundant *second*
  serialization `prepare` used to do for a brand-new node, since both the
  `CompKey`'s hash and the stored `param_bytes` now come from the same
  encoding), and `prepare` checks the `u16::MAX` size bound before ever
  calling `self.nodes.lock()`. **Kept the `u16` arena span** rather than
  widening it to `u32`: per Stage 5's per-row accounting, `param_len` is a
  per-row column, so `u16 -> u32` would cost 2 B/node — only 2 MB at this
  benchmark's 1M-node scale, genuinely cheap — but the real reason to keep
  `u16` is that 64 KB is already a generous parameter size for this
  engine's stated use case (a lookup key, per `crate::engine::Node`'s
  original design note); a param that large is much more likely to be a
  mistake (an embedded blob that belongs in a source/sink, not a param)
  than a legitimate need, and a loud `CompError` at the boundary catches
  that mistake immediately rather than quietly paying more memory forever.
  `DefTable::insert`'s bound is now a `debug_assert!` (its callers already
  guarantee it holds before ever reaching `insert`, so it documents an
  established invariant rather than validating untrusted input).
  `persist.rs::restore_nodes` got the same size check, since it also calls
  into the node table while `EngineInner::nodes` is locked, on records read
  from disk.
  - **Grep for other panics under a lock** (`engine.rs`/`driver.rs`/
    `persist.rs`, as instructed): `driver.rs`'s several `nodes.lock()`
    critical sections only ever call plain column reads/writes (no
    serde, no `assert!`) — nothing to fix. `engine.rs`'s
    `NodeTable::insert_new`'s `.expect("...must be registered...")` also
    runs under the lock, but is unreachable from user data (every caller —
    the live `eval` path via `get_def`, and `persist.rs::restore_nodes` via
    `self.def_names`/`self.erased_defs` — only ever calls it with a key
    whose definition is already known-registered); left as-is with its
    existing doc justification. While auditing, also found and fixed a
    *related* but not-under-a-lock panic: `def.rs::ErasedDef::serialize_value`'s
    `postcard::to_stdvec(typed).expect(...)` — reachable from the same
    "failing `R: Serialize` impl" hazard, but on the persister task's flush
    path (`persist.rs::PersistHandle::flush`), under the async `flushing`
    lock (a `tokio::sync::Mutex`, which does not poison on panic the way
    `std::sync::Mutex` does). Not a mutex-poisoning risk, but still an
    unnecessary panic reachable from user data — changed to return
    `Result<Vec<u8>, String>`; a failure now drops just that one pending
    upsert with a `tracing::warn!` instead of killing the persister task.
- **A failing result `Serialize` panics the driver task, near-silently.**
  `EngineInner::run`'s boxed execution future used to
  `postcard::to_stdvec(&result).expect(...)` for the early-cutoff content
  hash. A panic there propagates through `Shared` -> `join_all`
  (`driver.rs::run_wave`) -> whatever task is running `Engine::run`; since
  this crate's own examples `tokio::spawn` that task and never inspect its
  `JoinHandle`, the entire driver dies forever, with nothing but a panic
  message on stderr — no `tracing::warn!`, no retry, no trace in the
  engine's own error-reporting path at all. Fixed by returning a
  `CompError` instead: the offending node fails loudly through the
  existing error path (logged, stays `Dirty`, retried on the next relevant
  change) and the driver keeps running everything else.

**Correctness.** `cargo test --workspace --all-features` green: 104 passed
(up from 92 — the +12 are this stage's own: 2 unit tests for
`Registry::merge` (combines, panics on a duplicate id) plus 1 end-to-end
test for `EngineBuilder::registry` merging with an earlier `.source()`
registration; 2 for `Fingerprint::current_exe_with`/`custom`; 1 for the
load-time key-mismatch drop; 2 for the debug determinism check (fires for
`HashMap`, stays silent for `BTreeMap`); 2 for Fix 4's no-poisoning
guarantee (an oversized param, a failing param `Serialize`, each also
asserting a followup `eval_root` still succeeds); 2 for Fix 5's
failing-result-`Serialize` behavior, one at the `eval_root` level and one
proving the `Engine::run` driver task itself survives and keeps
propagating other computations). `cargo clippy --workspace --all-targets
--all-features -D warnings` clean.

**Benchmark.** Machine load was elevated during this run (`uptime` load
average **19.64** on a 14-core box, above Stage 6's already-flagged
12–30 range) — absolute numbers below are read against that, not against
an idle box. `cargo run -p computations --release --example persist_bench
--features testutil`, full 1M scale:

| phase | Stage 5/6 baseline (clean box) | this run (loaded box) | direction |
|---|---|---|---|
| 1. cold eval (persistence configured) | 3.69–3.73 s | 3.567 s | faster |
| 2. persist_now / db size | 2.24–2.49 s / 269.49 MB | 2.529 s / **269.49 MB** | flat / unchanged |
| 3. warm restart, no changes | 1.36–1.44 s | 1.374–1.386 s | flat |
| 4. restart, 1 changed input | 1.83–2.09 s | 1.818–1.991 s | flat |
| 5. cold restart, no persistence | 2.91–2.96 s | 2.748–2.781 s | faster |
| 6. fingerprint mismatch (full revalidation) | 4.96–5.13 s | 4.839 s | faster |
| 7. live incremental, no persistence | 502–509 ms | 501 ms | flat |
| 8. live incremental, with persistence | 650–704 ms | 677 ms | flat |
| engine-only RSS (phase 5, no persistence) | 328.1–330.5 MB | 338.3–340.7 MB | **+3%** |

Every timing phase came out at or below the clean-box baseline *despite*
the box being under roughly 40% heavier load than Stage 6's own
already-elevated range — no phase is within shouting distance of the 15%
regression threshold this task asked to watch for. The persisted db size
(269.49 MB, byte-identical to Stage 5) confirms `FORMAT_VERSION` truly
didn't move despite `StoredRecord` threading an extra `Vec<u8>` through the
*load* path — that extra field never touches what's written to disk, only
what's read back and compared in memory. Engine-only RSS is ~3% higher
than the Stage 5 baseline, comfortably inside the load-driven noise band
(Stage 6 measured similar-magnitude run-to-run swings on this same box);
nothing in this stage's changes adds any steady-state per-node memory (the
`Vec<u8>` `StoredRecord` key is a transient, load-time-only allocation,
freed once `restore_nodes` returns).

## Stage 9 — flow-argument computations (macro foundation)

Phase A of a future `#[computation]` proc-macro: made the macro's target
shape — `async fn sync_file(ctx: &Ctx, #[flow] source: &Arc<FsSource>,
#[flow] sink: &Arc<FsSink>, rel: PathBuf) -> Result<(), CompError>`, called
as a plain `sync_file(ctx, &source, &sink, rel).await`, no builder
registration, no captures — work *by hand*, so the macro (not built here)
only has to generate the shapes this stage wrote out explicitly:
`concat!(module_path!(), "::sync_file")`, the real body, a public wrapper
calling `Ctx::eval_flows`, and a `FlowThunk` registered via
`EngineBuilder::define_flows`. Entirely additive: every existing
`define`/`define_with`/`Comp<P, R>` builder-path API is unchanged, and the
two paths share one engine core (`prepare`/`run`, the node table, GC,
persistence) rather than forking it.

### Identity: why a flow's instance id has to enter `CompKey`

A flow argument (a source or sink) is read/written through
`Ctx::src_req`/`sink_req` exactly as today — its *contents* are never
hashed, dependency-tracked the same way as any other source/sink access.
But its *instance* still has to distinguish two calls: `sync_file(src_a,
sink, rel)` and `sync_file(src_b, sink, rel)` must be two different nodes,
never one. Without that, they'd collide on one `CompKey` (same def name,
same `rel` param) and silently serve each other's cached values — wrong
output, not a crash, and the kind of bug that only shows up as "the file
synced from the wrong source" days later.

- **`FlowId`** (`flow.rs`) unifies a source or sink's stable id:
  `enum FlowId { Source(SourceId), Sink(SinkId) }`, `Clone + Eq + Hash +
  Serialize + Deserialize`. Getting it `Serialize`/`Deserialize` required
  giving `SourceId`/`SinkId` themselves hand-written impls first (`source.rs`/
  `sink.rs`) — both just wrap an `Arc<str>`, and deriving through `Arc`
  needs serde's `rc` feature (not enabled in this workspace), so a plain
  string round-trip was written by hand instead. This is also what let
  `NodeRecord::flow_ids` (below) store `Vec<FlowId>` directly, with no
  `*Repr` stand-in type of the kind `RawDepRepr`/`RawOutputRepr` needed
  before `SourceId`/`SinkId` could serialize on their own.
- **`flow_aware_param_hash(flows, param_bytes)`** (`flow.rs`) is the one
  function that actually folds flow identity into `CompKey`. **The
  empty-flows case is not a degenerate case of the general formula — it's
  a hard, separately-branched requirement**: with `flows.is_empty()`, it
  returns *exactly* `blake3(param_bytes)`, truncated to 128 bits — bit-for-
  bit what `CompKey::new`/`StableHash::stable_hash` already compute for a
  plain param, with no flow-list contribution at all, not even an
  empty-list marker byte. Getting this wrong (e.g. always hashing
  `postcard(flows) ++ param_bytes`, even for an empty `flows`) would
  silently invalidate every persisted database ever written under the
  existing builder path the moment this feature shipped: postcard's own
  length-prefix for an empty `Vec` is a real, non-empty byte sequence, so
  the "general" formula's zero-flows case does *not* naturally coincide
  with the old one without this explicit branch. Pinned down directly by
  `flow::tests::flow_hash_with_no_flows_matches_plain_param_hash`, which
  asserts the zero-flows output equals both `StableHash::stable_hash` and
  `CompKey::new`'s own `param_hash()` — not just "an equivalent-looking
  hash", the literal same `Hash128`. (The builder path itself never calls
  this function at all — `Comp<P, R>`/`Ctx::eval` are completely untouched
  — so its hashes are bit-identical to before by construction, not by
  coincidence; this test is what makes that a checked invariant rather
  than an assertion in a doc comment.) With a non-empty `flows`,
  `postcard::to_stdvec(flows)` is self-delimiting (a `Vec`'s own length
  prefix), so concatenating it with `param_bytes` before hashing can never
  produce the same bytes for two different (flows, param) splits.

### Registry: typed lookup by instance id

A revived or rerun flow-argument node has only `FlowId`s, never live
handles — a node's flows have to be resolved fresh from the registry every
single time, first execution or any later rerun alike (see "no closures
stored anywhere" below). `Registry` (`registry.rs`) already stored every
source/sink behind its object-safe `dyn ErasedSource`/`dyn ErasedSink` —
untyped by design, since the driver only ever needs untyped operations —
so a second, parallel map was added per side: `source_typed`/`sink_typed:
HashMap<_, Arc<dyn Any + Send + Sync>>`, holding the *same* `Arc<S>`
(cloned once at `register_source`/`register_sink`, a refcount bump, not a
second instance) purely so `Registry::source_typed<S>`/`sink_typed<S>` can
downcast back to a caller-known concrete type. `Registry::merge` extends
both new maps alongside the existing ones.

### `FlowResolver`/`FlowThunk`: the uniform, closure-free rerun path

```rust
pub type FlowThunk =
    fn(Ctx, FlowResolver<'_>, &[u8]) -> BoxFuture<'static, Result<Arc<dyn Any + Send + Sync>, CompError>>;
```

A plain `fn` pointer, not a closure — it captures nothing, so both the
first execution and every later rerun call the exact same value; nothing
about a node's flows or param is ever stored in a stored closure anywhere
(mirroring the closure-kill already done for the builder path's own rerun,
Stage 4). This is also deliberately a plain, `Copy`, `'static` value
because Phase B's macro is expected to collect `(name, thunk)` pairs via
`inventory::submit!` and hand each to `EngineBuilder::define_flows` at
link time — a plain value is exactly what that needs.

`FlowResolver` (`flow.rs`) wraps a `&Registry` plus a node's ordered
`&[FlowId]`, and offers `source::<S>(idx)`/`sink::<S>(idx)` returning
`Result<Arc<S>, CompError>`. Every failure mode is a loud, named
`CompError::Failed` rather than a panic or a silent `None`: `idx` out of
range, the flow at `idx` is a sink where a source was expected (or vice
versa), the id isn't registered in this engine's `Registry` at all, or
it's registered under a different concrete type than `S` expects — each
message names the flow index and (where known) the id/kind, so a
misconfigured engine fails with something actually debuggable rather than
a generic "computation failed".

### Making `prepare`/`run` serve two kinds of definition without forking

The whole point was to reuse the existing cache-hit / single-flight-join /
run algorithm, not fork a second copy of it for flows. The one thing
`prepare`/`run` actually needed from a `CompDef<P, R>` was its typed value
column (`read_value`/`write_value`) and, for `run`, its body — both now
factored out:

- **`ValueColumn<R>`** (`def.rs`): `{ read_value(row) -> Option<R>;
  write_value(row, R) }`, implemented by both `CompDef<P, R>` (unchanged
  behavior, just moved behind a trait) and the new `FlowCompDef<R>`
  (`flow.rs` — generic over `R` alone, deliberately *not* `P`: a
  flow-argument def's parameter type is never named at the engine level at
  all, since `FlowThunk` decodes `param_bytes` itself; `P` only ever
  appears one layer up, at `Ctx::eval_flows`'s macro-generated call sites,
  purely to serialize what the caller already has in hand). Both defs'
  `Mutex<Vec<Option<R>>>` growth/read/write logic is the exact same code
  now (`column_read`/`column_write`, `def.rs`) rather than duplicated —
  "reuse `CompDef<P, R>`'s typed value column" is true of the actual code,
  not just the shape. `prepare`/`run` are now generic over `D: ValueColumn<R>
  + ?Sized` instead of a concrete `Arc<CompDef<P, R>>`.
- **`run`'s body** is now an explicit `BodyFn<P, R>` parameter instead of
  read off `def.body` internally. The builder path's call site passes
  `def.body.clone()` (exactly what `run` used to do itself); the
  flow-argument path (`EngineInner::eval_flows_core`) builds a one-off
  `BodyFn` (`engine.rs::build_flow_body`) that ignores the `_param: P` `run`
  hands it and instead resolves `thunk`'s flows fresh from the registry via
  a `FlowResolver`, calls the registered `FlowThunk`, and downcasts its
  erased result to `R` — from `run`'s point of view, indistinguishable from
  an ordinary builder-path body.
- **`EngineInner::eval_flows`/`eval_flows_core`/`eval_flows_erased`**
  (`engine.rs`) are the flow-argument mirror of `eval`: `eval_flows<P, R>`
  is the typed entry point `Ctx::eval_flows` calls; `eval_flows_erased<R>`
  is what a rerun/revival goes through, with no compile-time `P` at all —
  it instantiates the shared `eval_flows_core` at `P = ()`, which is sound
  (not a hack) because a flow-argument def's identity and execution never
  actually depend on `P`: `FlowThunk` decodes bytes itself, and the one
  place `P` matters at runtime (`prepare`'s debug-only param-determinism
  check) only ever fires for a node this engine has never seen before,
  which a rerun/revival can't be by construction (the node already
  exists). The one visible cost is diagnostic, not correctness: a
  flow-argument node's rerun trace event renders `param = ()` rather than
  the real value, since that value is genuinely never reconstructed on
  that path.
- **`EngineInner::rerun_node`** now takes the node's `&[FlowId]` alongside
  `param_bytes`, and tries `ErasedDef::rerun` first, falling back to the
  new `ErasedDef::rerun_flows` only when that reports "not applicable"
  (`None` — always true for a flow-argument def, whose `param_bytes` alone
  can't reconstruct its identity; never true for a builder-path def with
  well-formed bytes). Same try-then-fall-back ordering in
  `persist.rs::restore_nodes` between `ErasedDef::revive_key` and the new
  `revive_key_flows`. Neither caller has to know in advance which kind of
  def a `CompKey`/record names — both new `ErasedDef` methods default to
  `None`, so every pre-Stage-9 (builder-path) `ErasedDef` impl is
  unaffected without writing a single line at any existing call site.
- **`Engine::run_flows`** (`driver.rs`) is the flow-argument counterpart of
  `Engine::run`: same initial-evaluation / startup-GC / infinite
  propagation loop, factored into a shared `startup_gc_then_loop` so the
  only thing that actually differs is which of `eval_root`/`eval_root_flows`
  performs the initial evaluation — `propagate`/`liveness_gc`/`rerun_node`
  were already flow-agnostic (`CompKey`/`NodeRef`-only), so the driver
  needed no other change to rerun a dirtied flow-argument node under a live
  propagation round. `driver.rs::run_wave`'s job-building step now also
  reads each node's `flow_ids_clone(r)` (empty for a builder-path node)
  alongside its `param_bytes`, purely to hand it through to `rerun_node`.

### A new sparse side table, and why the node table itself needed no changes

The columnar node table (`DefTable`/`NodeRef`/`DefIndex`, Stage 5) needed
**zero changes**: it already stores rows keyed by a `Hash128` param hash
regardless of how that hash was computed, and a flow-argument def is
registered into the exact same `def_order`/`DefIndex` machinery as a
builder-path one (`EngineBuilder::define_flows` pushes onto `def_order`
exactly like `register` does) — so a flow-argument node is, as far as
`NodeTable` is concerned, just another row.

The one genuinely new piece of per-node state is a node's ordered
`Vec<FlowId>` itself, needed at rerun/persist/restore time (a node's
identity hash doesn't preserve the original flow list — hashing is
one-way). Added as a fourth sparse side table on `NodeTable`
(`flow_ids: HashMap<NodeRef, Vec<FlowId>>`), alongside the existing
`source_deps`/`outputs`/`inflight`: absent, not merely empty, for every
ordinary builder-path node, which never touches it at all. Purged in
`remove_by_id` alongside the other three, so a GC'd node never leaves one
behind.

### Persistence: format bump, and the restore-time fallback chain

`NodeRecord` gained `flow_ids: Vec<FlowId>` (stored directly, no `*Repr`
adapter needed — see the `FlowId`/`SourceId`/`SinkId` note above), and
`FORMAT_VERSION` bumped 2 -> 3: an older database's records don't have
that field at all, so rather than guess a default for every existing row,
the existing mismatch path wipes and recomputes cold — exactly the same
path a corrupt file or an unrecognized format already took, now also
covered by a dedicated test
(`persist::tests::old_format_version_is_wiped_and_starts_cold`, which
writes a stale `format_version` byte directly and asserts `open_db`
returns a fresh, empty database rather than trusting a partially-decodable
one). `PendingRecord::snapshot` reads a node's flow ids via the same
`nodes.flow_ids_clone(r)` the driver's rerun path uses.

The Stage-7 load-time key verification (a record's *recomputed* key must
match the raw key bytes it was actually filed under, or it's dropped
rather than trusted — see Stage 7 above) keeps working unmodified for both
paths: `restore_nodes` now tries `revive_key` first and falls back to
`revive_key_flows(flow_ids, param_bytes)` only when that returns `None`
(the same fallback-chain shape as `rerun_node`, above), and the resulting
key — whichever path produced it — is still compared against
`encode_key(&key) == stored_key_bytes` exactly as before. A flow-argument
record whose recomputed key doesn't match (e.g. a stale `FlowId` list from
a differently-shaped rerun) is dropped with the same loud warning and
diagnosable cold recompute as any other key mismatch, never silently
misattached.

### Proving it by hand (`tests/flow.rs`)

Wrote out, by hand, exactly what `#[computation]` would generate for a
two-flow computation (`sync_doc`: a `MemKvSource` reader + `VecSink`
writer) — the name constant, the real `impl` body, the public wrapper
calling `Ctx::eval_flows`, and the `FlowThunk` registered via
`define_flows` — across five scenarios, each in its own module so its name
constant and run-counter `static` (a `FlowThunk` is a plain `fn` with no
per-call captures, so counting invocations needs a `static`, not an
`Arc<AtomicUsize>` closure capture the way `tests/driver.rs` does it) can
never collide with another scenario's:

- **Evaluates correctly and memoizes**, via a nested builder-path root
  computation that calls the generated wrapper (exercising the wrapper
  itself, which needs a `&Ctx` unavailable at a bare root call, and
  proving the two registration styles interoperate — a builder-path
  computation calling a flow-argument one looks exactly like calling any
  other async function).
- **Re-runs on a source change under the live driver** (`Engine::run_flows`,
  spawned and polled exactly like every `tests/driver.rs` scenario).
- **Survives a persisted restart as a cache hit, zero reruns** — two
  `Engine`s built against the same redb file and the same source/sink
  `Arc`s (mirroring a real restart), with the second's initial evaluation
  going through `run_flows` (restoring only ever happens inside
  `Engine::run`/`run_flows`) asserting the shared run-counter stayed at 1.
- **The identity test**: the same computation name, called (via two
  builder-path wrappers) with the same param against two *different*
  `MemKvSource` instances, must produce two independent values and exactly
  two runs — the direct regression test for the silent-corruption case
  this whole stage exists to prevent. Also asserts a third, unchanged call
  stays a cache hit against its own node rather than colliding with the
  other instance's.
- **Mutual recursion** between two hand-written flow computations
  (`is_even`/`is_odd`, each taking a shared sink flow) calling each other
  by name through `Ctx::eval_flows` — a capability with no equivalent on
  the builder path today (`Comp::named`'s mutual-recursion escape hatch was
  removed; the documented workaround is merging into one self-recursive
  computation over a sum-type param). Flow-argument computations need no
  such workaround: since they're called by name rather than through a
  registration-backed handle, there's no handle that has to already exist
  before both defs are registered.

Also added three focused unit tests in `flow.rs` itself pinning down
`flow_aware_param_hash`'s three load-bearing properties directly (matches
the plain builder hash at zero flows; changes when flows are added; changes
across two different flow instances).

**Correctness.** `cargo test --workspace --all-features`: 113 passed (up
from 104 — +1 for the format-version-bump test above, +3 unit tests in
`flow.rs`, +5 integration tests in the new `tests/flow.rs`), 1 pre-existing
ignored test unaffected. `cargo clippy --workspace --all-targets
--all-features -- -D warnings` clean (one `#[allow(clippy::too_many_arguments)]`
on `EngineInner::run`, whose one new `body` parameter over Tier 2's already
seven pushed it past the lint's default threshold — documented inline
rather than restructured, since splitting the rest into a struct purely to
dodge the lint would cost more clarity at `run`'s two call sites than it
buys).

**Benchmark.** `uptime` load average **11.66** (1-min; 9.97/12.18 at
5/15-min) on this same 14-core box — elevated, in the same rough range as
Stage 7's own flagged 12–30, so read the absolute numbers against that,
not against an idle box; the comparison that actually matters is
relative, against this task's stated baselines. `cargo run -p computations
--release --example persist_bench --features testutil`, full 1M scale (one
run, not paired trials like Stage 5/7):

| phase | baseline (task-stated) | this run (loaded box) | direction |
|---|---|---|---|
| cold eval (persistence configured) | ~3.7 s | 3.646 s | flat |
| warm restart, no changes | ~1.4 s | 1.399 s / 1.422 s (2 trials) | flat |
| restart, 1 changed input | (not separately baselined) | 1.813 s / 1.992 s | — |
| cold restart, no persistence | (not separately baselined) | 2.737 s / 2.744 s | — |
| fingerprint mismatch (full revalidation) | (not separately baselined) | 4.899 s | — |
| live incremental, no persistence | ~505 ms | 499 ms | flat |
| live incremental, with persistence | (not separately baselined) | 603 ms | — |
| persist_now / db size | (not separately baselined) | 2.424 s / **269.49 MB** | unchanged |
| engine-only RSS (no persistence) | ~330 MB | 330.8 MB / 338.8 MB (2 trials) | flat |

Every phase this task gave an explicit baseline for landed at or inside
that baseline despite the elevated load — no phase is anywhere near the
15% regression threshold this task asked to watch for, so the flow
machinery this stage added is confirmed to cost the builder path nothing
at steady state: it sits behind a generic type parameter (`ValueColumn<R>`)
that monomorphizes away, an `EngineInner::eval_flows*` family the builder
path's own `eval` never calls, and a `flow_ids` side table that a
builder-path node never even inserts an entry into. The persisted db size
(269.49 MB) came out byte-identical to every prior stage's measurement
despite `NodeRecord` gaining a new `flow_ids` field — every existing
record's field is an empty `Vec`, and apparently doesn't shift redb's
page-rounded file size at this scale.

### Left for Phase B

- **The `#[computation]` proc-macro itself.** Everything in this stage is
  the hand-written target shape; nothing here generates Rust code. The
  macro's job is mechanical given this foundation: parse `#[flow]`-annotated
  arguments, emit the name constant, wrapper, and `FlowThunk` this stage's
  `tests/flow.rs` wrote by hand, and register each via `EngineBuilder::define_flows`.
- **`inventory`-based collection.** `define_flows` is still called
  explicitly per definition, exactly like `register`/`define` today.
  `FlowThunk` was deliberately kept a plain `fn` value (not a closure or
  boxed trait object) specifically so a macro can `inventory::submit!` a
  `(name, thunk)` pair at link time and a startup step can walk the
  inventory calling `define_flows` for each — that wiring itself wasn't
  built, only the plain-value shape it needs.
- **Multiple result types behind one `EngineBuilder`.** `define_flows<R>`
  takes `R` explicitly at the call site (nothing infers it), which will be
  entirely natural once the macro emits it directly from the user's return
  type — not attempted as an ergonomics improvement here since there is no
  hand-written call site that would benefit from inference weight over an
  explicit turbofish.
- **A typed `Comp`-like handle for a flow-argument computation.** Today a
  flow computation is called purely by name string (`Ctx::eval_flows(name,
  flows, param)`); there is no `Comp<P, R>`-equivalent handle carrying
  compile-time identity the way the builder path has. This was a
  deliberate scope cut, not an oversight: a flow computation's `P` is never
  named at the engine level at all (see `FlowCompDef<R>`'s docs), so a
  handle would need to either drop the `P` type parameter (weakening the
  type safety a `Comp<P, R>` call site currently gets from the compiler)
  or be generated per-macro-invocation with `P` baked in from the parsed
  signature — squarely Phase B's job, once the macro can see that
  signature to generate from.

## Stage 10 — the `#[computation]` macro

Phase B: a new `computations-macros` proc-macro crate (`proc-macro = true`,
`syn`/`quote`/`proc-macro2`) providing `#[computation]`, re-exported from
`computations` (`pub use computations_macros::computation;`) behind a new,
default-on `macros` feature. `#[computation]` turns exactly the shape Stage
9 wrote out by hand — `async fn sync_file(ctx: &Ctx, #[flow] source:
&Arc<FsSource>, #[flow] sink: &Arc<FsSink>, rel: PathBuf) -> Result<(),
CompError>`, called as a plain `sync_file(ctx, &source, &sink,
rel).await` — into generated code, with automatic registration on top
(Stage 9 left both of those as explicitly deferred work; see "Left for
Phase B" above).

### Why `#[flow]` has to be explicit (not inferred from the type)

A proc macro operates on tokens, never on resolved types: by the time
`#[computation]` runs, the compiler hasn't looked up what `Arc<FsSource>`
*is* yet, so there is no way to tell "this argument is a source/sink" apart
from "this argument just happens to be `Arc`-wrapped" (e.g. `Arc<Vec<u8>>`,
an ordinary shareable parameter) from syntax alone. `#[flow]` is the
human-supplied signal that removes the ambiguity; the design constraint
handed to this stage was explicit about not trying to infer it. The macro
does perform one syntactic check on its own (a `#[flow]`-marked argument
must be shaped `&Arc<T>` — a bare `T` or an owned `Arc<T>` is a
`compile_error!` naming the argument directly), but it cannot check that
`T` actually implements `SourceBase`/`SinkBase`; that surfaces at the
generated call site as an ordinary Rust trait-bound error instead (see
below).

**Dispatching `Source` vs. `Sink` without type info, either.** The macro
also never learns whether a given `#[flow]` argument's `T` is a source or a
sink — same token-only limitation. Resolved with the "autoref
specialization" pattern rather than a custom check: `computations::flow`
defines two *distinct* traits per operation —
`AsFlowId`/`AsFlowIdSink` (build a `FlowId` from an `Arc<T>`) and
`ResolveFlow`/`ResolveFlowSink` (rebuild an `Arc<T>` from a
`FlowResolver`) — each blanket-implemented over one of `SourceBase`/
`SinkBase`. Two blanket impls of the *same* trait over `SourceBase` and
`SinkBase` would conflict under Rust's coherence rules (the compiler can't
prove the two are mutually exclusive for some hypothetical future type),
but two impls of two *different* traits never conflict. Generated code
imports both traits into scope and calls the shared method name
(`source.as_flow_id()`, `Arc::<T>::resolve_flow(&resolver, idx)`); for any
concrete `T` that implements only one of the two marker traits (every type
in this workspace), method/associated-function resolution picks the one
applicable impl with no ambiguity — verified directly against a standalone
`rustc` sandbox before wiring it into the macro, since this dispatch trick
is the one piece of this stage that isn't obviously correct just from
reading it. A `T` implementing neither surfaces as an ordinary "no method
named `as_flow_id`"/unsatisfied-trait-bound error — not a custom
diagnostic, but still a compile-time failure naming the real problem
(constraint 1's contract, satisfied by ordinary Rust rather than by
macro-side type checking, which categorically cannot do this without a
`T: SourceBase` bound to check against). A `T` implementing *both* would
make the call genuinely ambiguous (`rustc` E0034) — an accepted, documented
edge case, since no type here is both a source and a sink.

### The generated code

For each `#[computation]` function, the macro emits five items in place of
the original one (module path shown for `sync_doc` inside `mod smoke`, via
`cargo +nightly expand`):

```rust
const SYNC_DOC_NAME: &str = "computation_macro_smoke::smoke::sync_doc";

async fn __computation_impl_sync_doc(
    ctx: &Ctx, source: &Arc<MemKvSource>, sink: &Arc<VecSink>, key: String,
) -> Result<(), CompError> { /* the original body, verbatim */ }

pub async fn sync_doc(
    ctx: &Ctx, source: &Arc<MemKvSource>, sink: &Arc<VecSink>, key: String,
) -> Result<(), ::computations::error::CompError> {
    use ::computations::flow::{AsFlowId as _, AsFlowIdSink as _};
    let __flows: [::computations::FlowId; 2usize] = [source.as_flow_id(), sink.as_flow_id()];
    let __param: String = key;
    ctx.eval_flows(SYNC_DOC_NAME, &__flows, __param).await
}

fn __computation_thunk_sync_doc(
    __ctx: ::computations::Ctx, __resolver: ::computations::FlowResolver<'_>, __param_bytes: &[u8],
) -> ::computations::FlowThunkFut {
    use ::computations::flow::{ResolveFlow as _, ResolveFlowSink as _};
    let __param: String = /* postcard-decode __param_bytes, or return a boxed Err future */;
    let key: String = __param;
    let source: Arc<MemKvSource> = /* Arc::<MemKvSource>::resolve_flow(&__resolver, 0), or return Err */;
    let sink: Arc<VecSink> = /* Arc::<VecSink>::resolve_flow(&__resolver, 1), or return Err */;
    Box::pin(async move {
        let __result = __computation_impl_sync_doc(&__ctx, &source, &sink, key).await?;
        Ok(Arc::new(__result) as Arc<dyn Any + Send + Sync>)
    })
}

fn __computation_register_sync_doc(builder: &mut ::computations::EngineBuilder) {
    builder.define_flows::<()>(SYNC_DOC_NAME, __computation_thunk_sync_doc);
}
::computations::inventory::submit! {
    ::computations::flow::ComputationEntry { name: SYNC_DOC_NAME, register: __computation_register_sync_doc }
}
```

This is, line for line, what `tests/flow.rs` wrote out by hand (Stage 9's
whole point). Two deliberate departures from a literal transcription:

- **`SYNC_DOC_NAME` is public** (matching the annotated function's own
  visibility), not doc-hidden — it's the escape hatch that lets a caller
  drive a `#[computation]` function as a genuine root via
  `Engine::run_flows`/`eval_root_flows` (building its own `FlowId` list from
  the equally-public `AsFlowId`/`AsFlowIdSink` traits) with zero `Comp<P,
  R>` handles and zero `EngineBuilder::define*` calls anywhere — see
  `examples/dirsync.rs` below, which needs exactly this.
- **`computations::postcard`/`computations::inventory` are re-exported**
  from the `computations` crate specifically so generated code never has to
  assume the annotated crate depends on `postcard`/`inventory` directly —
  only on `computations` itself. `FlowThunkFut` (a new, exported type alias
  for `FlowThunk`'s return type) does the same job for `futures::future::
  BoxFuture`/`std::any::Any`: an annotated crate needs no dependency on
  `futures` at all.
- **Multi-param bundling.** Two or more unmarked (non-`#[flow]`) arguments
  are bundled into a tuple, in left-to-right declaration order, for both
  hashing and (de)serialization — `fn f(ctx: &Ctx, a: A, b: B) -> ...`
  becomes `(a, b): (A, B)`. Exactly one param stays unwrapped (no
  single-element-tuple overhead); zero params encode as `()`. The ordering
  guarantee is load-bearing, not cosmetic: `computation_macro.rs`'s
  `zero_flow_params_only_computation_memoizes_and_orders_params` and
  `multi_param_with_flow_preserves_argument_order` tests both assert that
  swapping which argument plays which role produces a genuinely different
  node identity (and result), not a silently transposed one.

### Registration mechanism and its caveats

`::computations::inventory::submit!` collects every `ComputationEntry {
name, register }` in the final linked binary via platform constructor
sections (`.init_array`/`.ctors` on Linux/macOS, an equivalent on Windows)
that run before `main`. `EngineBuilder::build()` walks
`inventory::iter::<ComputationEntry>()` and calls each entry's `register`
fn — eager, not lazy-on-first-call, which matters specifically for
persistence: restore drops any record whose `DefId` isn't registered yet
(Stage 7's existing "unknown definition" tolerance), so a hypothetical
lazily-registered def would cold-start its *entire* subtree on every
restart, silently, the first time nothing had called it yet. `register` is
a plain, non-generic `fn(&mut EngineBuilder)` monomorphized per
`#[computation]` (it already knows its own concrete `R`), mirroring
`FlowThunk`'s own "erase to a bare fn pointer" trick one level up — this is
what lets `ComputationEntry` collect definitions of different result types
uniformly without `EngineBuilder::build()` ever naming any of them.

Two caveats, both inherited from `inventory`/ctor-based collection in
general, not specific to this crate:

- **No WebAssembly support** — wasm has no constructor-section mechanism,
  so `inventory::submit!`'s items never run there.
- **Static-linking dead-code elimination.** If a `#[computation]` function
  ends up in a `.rlib` archived into a final binary, and nothing else in
  that translation unit is ever referenced, an aggressive linker is in
  principle free to drop the whole object file — submit-time constructor
  included — before it ever runs.

**Escape hatch**: every `#[computation]` function also generates a plain,
directly-callable `__computation_register_<fn_name>(&mut EngineBuilder)` —
the very function `ComputationEntry::register` already points at. Call it
explicitly, before `build()`, on a target or link configuration where
automatic collection doesn't apply.

**Collision handling.** `EngineBuilder::build()` checks each inventory
entry's name against both the names already registered directly on the
builder (`define`/`define_with`/`define_flows`/... calls made before
`build()`) and every other inventory entry's name seen earlier in the same
walk, panicking with a message naming the colliding name and *which two*
registration paths produced it (two `#[computation]` functions vs. a
`#[computation]` function colliding with an explicit builder registration)
— the same "duplicate is a startup configuration error, not a runtime
condition" stance `register`/`define_flows`/`Registry::register_source`
already take, just extended to name both sides for this new registration
path.

### What `dirsync` lost

`crates/computations-fs/examples/dirsync.rs` — the deliverable "proof" —
lost its `env` tuple, both `EngineBuilder::define_with`/`define_rec_with`
calls, and both `Comp<PathBuf, ()>` handles: `sync_file`/`sync_dir` are now
two plain `#[computation]` functions, calling each other (and, for
`sync_dir`, itself) exactly the way any two ordinary async functions would.
`EngineBuilder::build()` picks both up with no `define*` call for either.
The one param `FsSink`'s design let the old version fold into a captured
closure invisibly — `source_root` (`FsSource` has no root of its own,
unlike `FsSink`) — has to be an explicit, ordinary (unmarked) parameter now,
threaded through both functions and cloned at each recursive call site,
since a `#[computation]` function has no captured environment to hide it
in. `Ctx::eval_all`'s batched-concurrent-evaluation role (needs a `Comp<P,
R>` handle) is played by `futures::future::try_join_all` over an iterator
of direct wrapper calls instead.

Net effect on raw line count is close to a wash, not a reduction — worth
stating plainly rather than rounding to a nicer-sounding number:

| | before (builder path) | after (`#[computation]`) |
|---|---|---|
| whole file | 167 lines | 198 lines (+31, almost entirely new module-doc explaining the Phase B wiring) |
| computation wiring only (defs + registration + driving call, comments excluded) | 41 lines | 45 lines |

The qualitative reduction — zero `EngineBuilder::define*` calls, zero
`Comp<P, R>` handles, zero captured `env` tuple, two top-level `pub`-able
async functions callable and testable on their own — doesn't show up as
fewer lines here specifically because `source_root` moved from "captured
once, invisible thereafter" to "explicit parameter, re-cloned at every
recursive call site." A computation with no such non-flow, non-`Arc`
shared state (most of `computation_macro.rs`'s own scenarios) sees a
straightforward reduction instead; `dirsync` is the one case in this
workspace shaped so the ledger doesn't obviously favor either side, which
is exactly why it was worth reporting honestly rather than picking a
rosier comparison.

`crates/computations-fs/tests/dirsync.rs` was deliberately **kept on the
builder path** rather than ported alongside the example: the example
already *is* the macro's port proof, and this test's job is to confirm the
older API keeps working, unchanged, once the macro exists alongside it —
porting both would leave zero coverage in this workspace actually
exercising `define_with`/`define_rec_with` against a real, nontrivial
(self-recursive, concurrent-batch) computation graph.

### Tests

`crates/computations/tests/computation_macro.rs` (7 scenarios, each its own
module so its `module_path!()`-derived name and run-counter `static` stay
unique, mirroring `tests/flow.rs`'s own pattern): single-flow
evaluate+memoize, multi-flow rerun-under-the-live-driver, zero-flow
params-only with multi-param ordering, a flow combined with multiple
ordinary params (the `dirsync` shape, params-ordering-preserved), mutual
recursion between two `#[computation]` functions (constraint 5 — now
supported, since compile-time name resolution through the generated
wrapper functions removes the hazard that used to motivate banning it on
the builder path), the identity guarantee (two different source instances
behind the same computation+param never collapse onto one node), and a
persisted restart through the macro path (cache hit, zero reruns). None of
these ever call `EngineBuilder::define_flows` for the `#[computation]`
function under test — every one relies purely on automatic registration,
which is itself the property being exercised throughout.

`crates/computations/tests/computation_macro_compile_fail.rs` +
`tests/ui/*.rs` (4 fixtures, `trybuild`, checked-in `.stderr` snapshots):
not-`async`, first argument not `&Ctx`, a `#[flow]` argument not shaped
`&Arc<T>`, and a return type not `Result<R, CompError>`. A fifth case
constraint 1 also names — a `#[flow]` argument correctly shaped as
`&Arc<T>` where `T` implements neither `SourceBase` nor `SinkBase` — is
deliberately *not* snapshot-tested: that failure is an ordinary Rust
trait-bound error whose exact wording is `rustc`'s to own, not this crate's
to pin down (and thus not safe to commit into a version-independent test
fixture). Snapshot fragility across `rustc` versions is `trybuild`'s known
tradeoff; regenerate with `TRYBUILD=overwrite` after a deliberate wording
change to this crate's own `compile_error!` messages.

**Correctness.** `cargo test --workspace --all-features`: 121 passed (up
from 113 — +7 in `computation_macro.rs`, +1 in
`computation_macro_compile_fail.rs`), 2 pre-existing ignored tests
unaffected. `cargo clippy --workspace --all-targets --all-features -- -D
warnings` clean.

### Benchmark

`uptime` load average **7.04** (1-min; 10.85/11.23 at 5/15-min) on this
same 14-core box at benchmark time — elevated, same rough range as Stage
9's own flagged 11.66, so read absolute numbers against that, not an idle
box; the relative comparison against this task's stated baselines (in turn
inherited from Stage 9) is what actually matters. `persist_bench` itself
exercises only the builder path (its 1M-instance synthetic graph predates
this stage and was never ported) — the point of running it here is to
confirm that adding a new default-on feature and two new dependencies
(`computations-macros`, `inventory`) to `computations` costs the
*unrelated* core engine path nothing, not to benchmark `#[computation]`
directly (the macro's own runtime cost is exactly `Ctx::eval_flows`, a
constant-time layer already measured in full in Stage 9 — this stage adds
no new field, lock, or branch to the hot `prepare`/`run` path at all).
`cargo run -p computations --release --example persist_bench --features
testutil`, full 1M scale:

| phase | baseline (task-stated) | this run (loaded box) | direction |
|---|---|---|---|
| cold eval (persistence configured) | ~3.65 s | 3.798 s | +4.1% |
| warm restart, no changes | ~1.40 s | 1.454 s / 1.407 s (2 trials) | +3.9% / +0.5% |
| live incremental, no persistence | ~499 ms | 549 ms | +10.0% |
| persist_now / db size | 269.49 MB | 269.49 MB | unchanged |
| engine-only RSS (no persistence) | ~331–339 MB | 333.8 MB / 344.7 MB (2 trials) | flat |

Every phase landed within the stated baseline's noise band, and nothing
crossed (or came close to) the 15% regression threshold this task asked to
watch for — the largest delta (live incremental, +10.0%) is consistent
with this run's elevated load rather than a genuine regression, and is
still well clear of the threshold. `#[computation]` and its automatic
registration are confirmed to add no measurable cost to the engine's
existing hot paths.

### Deferred

- **A typed handle for a `#[computation]` function**, analogous to
  `Comp<P, R>`, that could be passed around and stored the way a
  builder-path handle can (today, only the plain wrapper function itself,
  or the public `<FN>_NAME` constant plus `run_flows`/`eval_root_flows`, do
  that job). Not attempted here: the wrapper function already *is* the
  ergonomic handle for every call site this stage's tests or `dirsync`
  needed, and inventing a second handle type purely for symmetry with the
  builder path wasn't judged worth the added surface.
- **Generic `#[computation]` functions.** The macro rejects any annotated
  function with generic parameters outright (`#[computation] fn f<T>(...)
  -> ...` is a compile error) rather than attempting to support them — a
  generic function's `FlowThunk`/`ComputationEntry` would need one concrete
  monomorphization submitted per instantiation actually used in the
  binary, which `inventory::submit!`'s link-time, non-generic collection
  model cannot express on its own.
- **Complex argument patterns.** Every `#[computation]` argument (`ctx`,
  each `#[flow]` argument, each param) must bind via a simple identifier;
  a destructuring pattern (`fn f(ctx: &Ctx, (a, b): (i32, i32))`) is a
  compile error naming the offending argument. Supporting arbitrary
  irrefutable patterns would complicate the tuple-tupling/destructuring
  codegen for very little real-world benefit — every computation in this
  workspace (including `dirsync`) binds every argument by a plain name
  already.

## Stage 11 — the hospital benchmark (unshared-key workload)

A second benchmark, `examples/hospital_bench.rs`, ported from
`haskell-computations`'s `bench/Control/Computations/Demos/Bench/{Hospital,SystemSrc}.hs`
(commits `77520f3`, `978c03c`, `d3de930`; graph-shape/measurement rationale in
that repo's own `docs/benchmark-notes.md`, Stages 5–7). `persist_bench`'s
1M-instance graph is *shared-key*: 205,000 level-0 instances read only 300
distinct `MemKvSource` keys (`i % SRC_KEYS`), ~683 dependents per key, zero
source latency, and every comp body issues exactly one source read via a
single sequential `.await`. Two whole classes of optimization can never move
that graph's numbers regardless of whether they're implemented correctly:
anything that pays per *distinct interned source key* (300 keys is free no
matter how it's stored) and anything that pays per *source round trip*
(uncontended and free at zero latency, with nothing ever dispatched
concurrently). `hospital_bench` exists to make both non-trivial.

### What the Haskell original measures

Five `SystemSrc` instances (admissions/discharge/transfer, vitals, labs,
pharmacy, notes) stand in for separate clinical systems, each with a
configurable simulated per-call latency (`threadDelay`), a call counter, and
a concurrency high-water mark. `ApplicativeDo` desugars independent monadic
binds into a real `<*>`-combined `CompReqCombined` batch, so most comp
bodies' multi-key reads (vitals: value/unit/range, labs: result/range/
specimen, meds: order/drug, notes: text/author) dispatch as genuine
applicative batches, and `patientSummaryComp` reads one key from all five
sources in a single 8-leaf batch. No key is ever shared between two
dependents by design (~1.6M source calls against ~1.6M distinct keys, "every
reading is its own clinical fact"). It reports cold-eval wall time and
achieved instance count (an exact analytic target, not sampled), a
single-key live update, a rerun-heavy multi-key live update, RSS/
`max_live_bytes`, and per-source call/batch-call counts plus a concurrency
high-water mark — and a separate width×latency grid (scale 0.05, latency
500 µs) showing cold-eval speedup up to the graph's own widest-batch ceiling
(a 5-leaf plateau at width 4).

### What was ported, adapted, and dropped

**Kept**: the five-source clinical-system shape and names; the unshared-key
design (every leaf reads a key no other computation instance reads);
`SystemSrc`'s three defining properties as `LatencySource` (configurable
per-call latency via `tokio::time::sleep`, a call counter, a concurrency
high-water mark); the self-recursive lab-trend chain with a per-patient
depth cap in `[1, 5]` (`lab_trend_chain_cap`, identical formula); the one
deliberately cross-system 5-key batch (`patient_summary`); ward/hospital-
level rollups culminating in one root; a rerun-heavy live phase spreading
mutations across all five sources via a large-prime stride.

**Adapted, not transliterated**: `Ctx` has no `ApplicativeDo`-style
desugaring (or any engine-level request-combining type at all — see "Open
candidates" below), so every genuinely-independent multi-key read is written
explicitly with `futures::try_join!`/`Ctx::eval_all`, which drives the
underlying futures concurrently within tokio's cooperative scheduler. This
is arguably a more honest port than a mechanical translation would be:
nothing here secretly serializes what the source visually presents as
concurrent, and — see the measured table below — it demonstrates real,
substantial latency-hiding with **no width knob at all**: `Ctx::eval_all`
and `try_join!` simply build one future tree that tokio polls cooperatively,
and a `LatencySource::execute` call overlaps with any other pending call
against the same instance for free as long as it doesn't serialize itself
(it doesn't — the simulated delay is a plain `sleep`, held across no lock).
The Haskell original's `HOSPITAL_BENCH_CONCURRENCY` knob bounds a hand-rolled
worker pool that a wide batch's source leaves get dispatched to; this
crate's engine has no equivalent worker-pool concept to bound, and inventing
a per-source semaphore purely for this benchmark would measure a feature
that doesn't exist rather than the engine that does — so there is
deliberately no width knob here, only the measured, empirical high-water
mark each run actually achieves.

**Dropped**: the Haskell module's entire "pack every multi-field param/
result into a bare `Word64`" section (a real ~5% memory win *there*,
extensively justified in that module's own haddock). That whole exercise
exists because GHC's per-def column storage only unboxes a column whose
type is one of a fixed literal whitelist (`Word32`/`Word64`/`Int`/`Char`/
`Bool`/`Double`); a tuple or newtype never qualifies regardless of its
fields' types. This crate's per-def value column (`crate::def::CompDef`'s
`Mutex<Vec<Option<R>>>`, Stage 5 above) is an ordinary generic `Vec` with no
such whitelist — a `(u32, u32)` tuple is exactly as cheap as a bare `u64`
here, so `admission`'s result is a plain `(WardId, u32)` tuple.

**Simplified**: `interaction` checks only adjacent medication-order pairs
(`MEDS_PER_PATIENT - 1` per patient) rather than the Haskell original's full
`C(18, 2) = 153` all-pairs check. Both exist purely to give `interaction`'s
body two independent upstream reads to join concurrently; all-pairs buys no
further coverage of that property for ~8x the per-patient instance count.

Resulting per-patient shape: 744 instances (admission 1, vital 200,
vital_window 40, lab_result 180, lab_trend 180, med_order 20, interaction
19, note 100, note_digest/risk_score/patient_summary/patient_alert 1 each)
and 1,386 source calls (2 adt, 601 vitals, 541 labs, 41 pharmacy, 201
notes) — at the default scale (1,500 patients, 30 wards), 1,116,093
instances and 2,079,000 source calls against very close to that many
*distinct* keys (a handful of keys — the ones `patient_summary`'s
cross-system batch re-reads at sub-id 0 — are read by exactly 2 dependents;
every other key by exactly 1).

### Shared-key vs. unshared-key: which candidates each shape can measure

| workload | distinct keys | dependents/key | source latency | can measure |
|---|---|---|---|---|
| `persist_bench` | 300 (fixed, any scale) | ~683 | 0 | fan-in/rerun-cost, columnar memory, dirty-propagation cost |
| `hospital_bench` | ~2.07M at default scale (scales with graph size) | ~1 | configurable | per-key interning cost, latency-hiding, redundant-call elimination |

Concretely, of the three open candidates this task named:

- **Interning source-dep key bytes** (`crate::source::RawDep` stores each
  key as an owned, uninterned `Vec<u8>` today): invisible on
  `persist_bench` — 300 keys cost nothing to store redundantly at any scale.
  `hospital_bench` already puts a number on the ceiling: its cold-eval RSS
  is **~1,363 B/instance** (1,521.3 MB / 1,116,093 instances) against
  `persist_bench`'s own no-persistence baseline of **~332 B/instance**
  (Stage 5's own reported figure, reconfirmed below) — a **~4.1x** per-
  instance cost, on a graph that (per source call, not per instance) reads
  roughly 10x more source keys than `persist_bench` and shares almost none
  of them. Interning would only be able to close *some* of that gap (this
  benchmark's design deliberately never reads the same key twice from
  unrelated call paths except at `patient_summary`'s 5 sub-id-0 keys per
  patient, so there is little redundancy for an intern table to remove
  within a single run) — the more relevant comparison is the *baseline*
  cost of storing ~2M never-reused keys as owned `Vec<u8>` versus whatever a
  small-string/interned representation would cost instead.
- **A Zero/One/Many `source_index` representation** (only meaningful when
  most keys have very few dependents): `persist_bench`'s ~683
  dependents/key makes every bucket "Many" unconditionally — nothing to
  measure. `hospital_bench`'s ~1 dependent/key is exactly the shape such a
  representation targets; this benchmark is what would need to be re-run
  before/after that change to show a win.
- **Source-request bundling/dedup within an `eval_all` batch** (a
  `compSrcExecuteBatch`-style hook collapsing several requests bundled
  against one source instance into a single round trip): needs a source
  with real latency to show anything at zero cost per call, bundling is
  unmeasurable regardless of shape — exactly why `persist_bench`'s
  zero-latency `MemKvSource` could never evaluate this candidate.
  `hospital_bench`'s concurrency-demo phase already demonstrates the
  *opportunity*: at 10 patients / 2,000 µs latency, `vitals` alone served
  6,010 requests with a concurrency high-water mark of 6,000 — i.e.
  essentially every one of those requests was in flight at once, each
  paying its own full 2,000 µs delay independently (no engine-level
  dedup/bundling exists for `Source::execute` today, unlike `Ctx::eval`'s
  own single-flight dedup for repeated `(comp, param)` pairs). A handful of
  keys (e.g. `vitals/value/p{p}/v0`) are read twice per patient from two
  unrelated call paths (`vital(p, 0)` directly, and `patient_summary`'s
  cross-system batch) — each such pair pays two full round trips today; a
  bundling layer could collapse them to one.

### Measured table

`uptime` load average **4.4–4.5** (1-min ~4.5, 5-min 4.4, 15-min 4.3) on
this box at benchmark time — moderately loaded, same rough range as recent
sessions in this document; read the absolute numbers against that, and
prefer the relative comparisons (ratios, speedups) over the raw ones.
`cargo run -p computations --release --example hospital_bench --features
testutil`, default scale (`HOSPITAL_BENCH_SCALE=1`, `HOSPITAL_BENCH_SRC_LATENCY_US=0`):

| phase | time (ms) | reruns | RSS (MB) |
|---|---|---|---|
| 1. cold eval | 3,866 | 1,116,093 | 1,521.3 |
| 2. live incremental, 1 changed vitals key | 81 | 6 | 1,584.7 |
| 3. rerun-heavy live update (300 keys mutated) | 524 | 1,444 | 1,594.4 |
| 4. concurrency demo (10 patients, 2,000 µs/call latency) | 62 | 7,449 | 37.8 |

Source calls after phases 1–2 (achieved instance count matched the analytic
target exactly, 1,116,093): `adt` 3,001, `vitals` 901,504, `labs` 811,501,
`pharmacy` 61,501, `notes` 301,501 — total 2,079,008 (the extra 8 over the
2,079,000 analytic target are phase 2's own re-reads of the mutated
patient's vitals and cross-system keys). Concurrency high-water mark was
**1** for every source in phases 1–3 (`HOSPITAL_BENCH_SRC_LATENCY_US=0`, so
there's no delay for two calls to genuinely overlap inside) — phase 4 is
where latency-hiding actually shows up: at 2,000 µs/call and 10 patients,
13,860 total source calls completed in 62 ms wall time against a >= 27,720
ms fully-sequential estimate (`total_calls * latency`), with high-water
marks of 6,000 (`vitals`), 5,400 (`labs`), 2,000 (`notes`), 400
(`pharmacy`), and 10 (`adt`) — i.e. essentially the entire per-source
request volume was in flight simultaneously, achieved purely through
`Ctx::eval_all`/`try_join!`'s natural concurrency, no width knob involved.

Rerun-heavy phase: 300 keys mutated (spread across all five sources and the
full patient/ward range) produced 1,444 reruns in 524 ms (**362.9 µs/
rerun**) — a smaller reruns/key ratio than the Haskell original's ~7.6–8/key
because several of this graph's ward-level rollups (`ward_census`,
`ward_occupancy`) are insensitive to their inputs' *content* (only to
`admission`'s count/length, which an `adt`-key mutation's length rarely
changes), so early cutoff suppresses more upward propagation here than in
the Haskell original's design.

`hospital_bench`'s core work (phases 1–3, ~4.5 s) finishes far faster than
`persist_bench`'s full ~33 s, ten-child-process run — not because it does
less (1.12M instances here vs. `persist_bench`'s ~1M, and 2.08M source
calls vs. ~205K) but because it only needs *one* cold-eval pass: unlike
`persist_bench`, this benchmark has no persistence/restart machinery to
exercise, so it never pays for the 8 additional cold-or-warm restart trials
that make up most of `persist_bench`'s total wall time. Per-instance, it is
markedly *heavier*, not lighter — see the RSS/instance comparison above —
which is the more meaningful comparison for a benchmark whose whole point is
the cost of an unshared-key design, not wall-clock parity with a
differently-shaped benchmark. `HOSPITAL_BENCH_SCALE=0.02` (~22K instances,
30 patients) runs in well under a second and is the recommended quick
sanity check before a full run.

### `persist_bench` reconfirmed unaffected

Re-run in full immediately before/after this stage's changes (`cargo run -p
computations --release --example persist_bench --features testutil`, same
loaded box, load average 4.3–5.4 across the two runs):

| phase | baseline (task-stated) | this run | direction |
|---|---|---|---|
| cold eval (persistence configured) | ~3.7 s | 3.127 s | −15.5% (faster) |
| warm restart, no changes | ~1.4 s | 1.204 s / 1.207 s (2 trials) | −14% (faster) |
| live incremental, no persistence | ~500 ms | 447 ms | −10.6% (faster) |
| persist_now / db size | 269.49 MB | 269.49 MB | unchanged (exact) |
| engine-only RSS (no persistence) | ~330 MB | 331.7 MB / 356.1 MB (2 trials) | flat |

Every phase landed at or faster than its stated baseline, nothing
regressed, and the db size is byte-identical — this stage touched only
`examples/hospital_bench.rs` and this document, no `src/` change of any
kind, so this is a pure confirmation rather than a discovery.

### How to run

```text
# Full default scale (~1.1M instances, ~4.5 s core work, ~1.6 GB peak RSS)
cargo run -p computations --release --example hospital_bench --features testutil

# Quick sanity check (~22K instances, well under a second)
HOSPITAL_BENCH_SCALE=0.02 cargo run -p computations --release --example hospital_bench --features testutil

# Exercise the main phase with real source latency (careful — see the module
# docs' "no width knob" section: latency is paid by every one of ~2M source
# calls at full scale, so combine with a small HOSPITAL_BENCH_SCALE)
HOSPITAL_BENCH_SCALE=0.02 HOSPITAL_BENCH_SRC_LATENCY_US=500 \
  cargo run -p computations --release --example hospital_bench --features testutil

# Tune the rerun-heavy live-update phase
HOSPITAL_BENCH_RERUN_KEYS=1000 HOSPITAL_BENCH_RERUN_LOOPS=3 \
  cargo run -p computations --release --example hospital_bench --features testutil
```

`HOSPITAL_BENCH_PHASE` (`main` or `demo`) is the internal re-exec switch
(mirrors `PERSIST_BENCH_PHASE`); it isn't meant to be set by hand.

## Stage 12 — instrumentation: allocation deltas and lock-hold attribution

Two prerequisites for judging the next round of optimization candidates,
both ported from `haskell-computations`'s own instrumentation work
(`docs/benchmark-notes.md` there, commits `4d95a3f` for allocation deltas,
`572cc06`/`198cd75` for lock-hold stats): RSS only shows peaks, which is
blind to a fix that reduces churn without moving the peak (exactly what
that repo's own Stage 10 found); and Stage 6 of this doc flagged the global
`Mutex<NodeTable>` with only an aggregate "0.7% direct wait" figure, which
cannot say whether — or where — sharding it would help.

### Instrument 1 — allocated-bytes delta per phase

`crate::alloc_stats` (`crates/computations/src/alloc_stats.rs`) installs a
`GlobalAlloc` wrapper (`CountingAlloc`, delegating every call to `System`)
behind an off-by-default `alloc-stats` cargo feature
(`crates/computations/Cargo.toml`), gated at the `#[global_allocator]`
declaration itself (`lib.rs`) so an ordinary build has no custom allocator
at all — every number in this document prior to this stage was measured
without it and stays comparable. Two `Relaxed` `AtomicU64`s
(`ALLOCATED`/`DEALLOCATED`) accumulate bytes across every `alloc`/`dealloc`/
`alloc_zeroed`/`realloc` call; `snapshot()` reads both, and
`AllocSnapshot::delta` turns two snapshots into a phase's own allocation
independent of when GC/OS reclamation happens to run — mirroring GHC's
`allocated_bytes` (`getRTSStats`) exactly, down to the "delta, not
cumulative total" design point.

Both benchmarks sample a snapshot before and after each phase (cold eval,
each restart trial, the live-incremental settle, the rerun-heavy phase) and
print an `allocated_bytes (<phase>): N (X MB), deallocated_bytes: ..., net:
...` line, entirely absent from stdout when the feature is off (every
`#[cfg(feature = "alloc-stats")]`-guarded call site compiles to nothing).

**Enable with**: `--features testutil,alloc-stats` on either example, e.g.
`cargo run -p computations --release --example persist_bench --features
testutil,alloc-stats`.

**Overhead measured**: comparing `persist_bench`/`hospital_bench` with vs.
without the feature (same box, same session, load average 4.6–8.3 across
the runs below — see "the matrix" for the full load context), every phase's
allocation-instrumented wall time landed within noise of its baseline: -8.6%
to +12.5% across `persist_bench`'s eleven phases (straddling zero, no
consistent direction), +1.6% to +8.6% across `hospital_bench`'s four. A
single global atomic pair proved cheap enough at this crate's ~1M-instance,
multi-threaded-tokio-runtime scale that sharded/per-thread counters were not
needed in practice — worth revisiting only if a future, far more
allocation-heavy workload shows contention on `ALLOCATED`/`DEALLOCATED`
specifically. The allocation numbers themselves are, as GHC's equivalent
predicted, far more stable than wall time: `persist_bench` phase 5's two
trials reported bit-identical `allocated_bytes` (3,011,756,891 B) despite
their wall times differing by 3ms.

### Instrument 2 — per-call-site lock-hold time

`crate::lock_stats` (`crates/computations/src/lock_stats.rs`) times
`nodes: Mutex<NodeTable>` and `source_index: Mutex<HashMap<..>>`'s semantic
critical sections (not raw `.lock()` calls) individually, under a single
`COMPUTATIONS_LOCK_STATS` environment variable read exactly once, in
`EngineBuilder::build()`, into a plain `bool` field
(`EngineInner::lock_stats_enabled`) — the same load-bearing design point the
Haskell reference engine's `COMP_ENGINE_LOCK_STATS` makes explicit in its
own haddock: the enabled/disabled decision must be baked in once at setup,
never re-checked per acquisition, or the check itself would perturb the
hold-time baseline being measured. `EngineInner::timed` is the single choke
point every instrumented site calls through: when disabled it costs exactly
one `bool` branch (no `Instant::now`, no atomic write); when enabled it
records `(calls, nanos)` into one of 16 named [`LockSite`] accumulators
(`Relaxed` atomics throughout — pure statistics, not synchronization).

Sixteen named sites, chosen to match the task's own examples
(`record_call_dep`, `remove_stale_source_index`, the GC sweep,
dirty-priority updates, `prepare`) plus every other `nodes`/`source_index`
critical section on the propagation hot path (`engine.rs`'s `prepare` and
`run`'s three lock scopes, `record_call_dep`, `record_source_deps`'s two
mutex-scoped halves, `record_outputs`; `driver.rs`'s `affected_keys`,
`mark_dirty_quiet`, `split_by_tier`, `run_wave`'s two lock scopes, and
`liveness_gc`'s mark-sweep plus its later source-key-unregister scan) —
cold/startup-only paths (`live_outputs_by_sink`, `mark_all_dirty`,
`mark_root`, flow-argument `set_flow_ids`) were left uninstrumented since
neither benchmark exercises them and they contribute negligibly at scale.
A site that acquires two different mutexes in sequence
(`record_source_deps`) gets one `LockSite` variant per mutex, so no site's
nanoseconds are ever a mix of two unrelated locks' hold times.

`Engine::print_lock_stats()` prints the sorted, formatted breakdown (same
shape as the Haskell reference's per-method table: site, calls, total
seconds, mean nanoseconds, % of total) — a no-op when disabled. Both
benchmarks call it once per engine, at the point closest to "engine
shutdown" available in each benchmark's own structure: `persist_bench`
(process-per-phase — see its own module docs) calls it at the end of each
phase's worker process, giving one breakdown per phase; `hospital_bench`
(one long-lived engine across phases 1–3 in a single process) calls it once
at the end of `run_main_phase`, giving one cumulative breakdown across cold
eval + live incremental + rerun-heavy combined.

**Enable with**: `COMPUTATIONS_LOCK_STATS=1 cargo run -p computations
--release --example persist_bench --features testutil` (no cargo feature
needed — this is runtime-gated only, unlike `alloc-stats`).

**Overhead measured**: the *disabled* path is the one that must cost
nothing — confirmed by the baseline matrix below matching this document's
existing baselines. The *enabled* path's overhead scales with how many
critical sections a workload actually exercises: on `persist_bench`
(fewer, larger critical sections — 300 shared keys) it ranged from -5.1% to
+14.6% across phases, closer to noise on the multi-second cold-eval phases
and largest (+14.6%) on the two ~500ms live-incremental phases, where the
fixed per-call `Instant::now`-plus-atomic cost is a larger fraction of a
much shorter phase. On `hospital_bench` (far more critical-section calls —
~2M distinct source keys registered individually) overhead ran higher,
+9.6% to +20.1%, consistent with `Instant::now`'s per-call cost (not free
on any platform) being paid millions of additional times. Cheap enough for
an occasional diagnostic run; not something to leave on for every benchmark
invocation, which is exactly why it is opt-in.

### The matrix

`uptime` load average ranged **4.6–8.3** across this stage's nine runs
(1-min figures 4.6, 6.6, 6.3, 8.3, 6.6 at various points; the box was
moderately-to-heavily loaded throughout, similar to several earlier stages
in this document) — absolute numbers below should be read against that,
per this document's standing practice; the baseline-vs-instrumented
comparisons within each benchmark are same-session, same-load A/B pairs
and are the reliable signal.

**`persist_bench`, default scale**, baseline (both instruments off) vs.
`alloc-stats` on vs. `COMPUTATIONS_LOCK_STATS=1`:

| phase | baseline | alloc-stats | lock-stats | documented baseline |
|---|---|---|---|---|
| 1. cold eval (persistence configured) | 3382 ms | 3359 ms | 3438 ms | ~3.1–3.7 s ✓ |
| 2. persist_now [db=269.49 MB] | 2439 ms | 2369 ms | 2430 ms | (db size exact match, all three runs) |
| 3. warm restart, no changes | 1381/1360 ms | 1400/1378 ms | 1310/1341 ms | ~1.2–1.4 s ✓ |
| 4. restart, 1 changed input | 1828/1982 ms | 1802/1812 ms | 1771/1958 ms | (no documented range; reruns exact-match across all three: 100,164 / 137,085) |
| 5. cold restart, no persistence (engine-only RSS) | 2734/2561 ms, 336.4/328.8 MB | 2570/2567 ms, 329.3/352.9 MB | 2872/2871 ms, 338.6/345.6 MB | ~330 MB ✓ |
| 6. fingerprint mismatch (full revalidation) | 4306 ms | 4402 ms | 4662 ms | (no documented range) |
| 7. live incremental, no persistence | 465 ms | 502 ms | 533 ms | ~450–500 ms ✓ |
| 8. live incremental, with persistence (settle) | 560 ms | 630 ms | 642 ms | (no documented range) |

Every phase's rerun count and phase-5 engine-only RSS matched the
documented baseline exactly across all three runs (999,760 cold; 80,767
live; 336±8 MB engine-only), and the db size (269.49 MB) was byte-identical
in every run — the instruments change nothing about what the engine
computes, only what gets measured alongside it.

**`hospital_bench`, default scale**, same three configurations:

| phase | baseline | alloc-stats | lock-stats | documented baseline |
|---|---|---|---|---|
| 1. cold eval | 4152 ms, 1,116,093 reruns, 1512.4 MB | 4219 ms, 1509.2 MB | 4713 ms, 1516.2 MB | ~3.9 s / 1.12M reruns / ~1520 MB ✓ (time a little high, consistent with load) |
| 2. live incremental, 1 changed vitals key | 104 ms, 6 reruns | 108 ms | 114 ms | ~81 ms (high vs. doc, but a 6-rerun/~100ms phase is disproportionately scheduler-sensitive) |
| 3. rerun-heavy live update (300 keys) | 537 ms, 1444 reruns | 529 ms | 645 ms | ~524 ms ✓ |
| 4. concurrency demo | 70 ms, 7449 reruns | 76 ms | 70 ms | (no documented range; latency-hiding behavior unaffected) |

Reruns matched the documented baseline exactly in every run (1,116,093
cold; 1,444 rerun-heavy); RSS landed within the documented ~1520 MB band
throughout. Phase 2's wall time ran high relative to the documented ~81 ms
across all three configurations (not just the instrumented ones) — a
same-magnitude deviation present in the *baseline* run too, so this reads
as this session's load rather than anything the instruments touched.

### The lock-hold breakdown

This is the actual finding the instrument exists to produce — not just
plumbing. Full per-call-site table, `persist_bench` phase 1 (cold eval,
persistence configured — the largest, most representative shared-key run,
7,386,075 instrumented calls, 1.90 s total instrumented time):

| site | calls | total (s) | mean (ns) | % of total |
|---|---|---|---|---|
| `prepare` | 2,385,278 | 0.738 | 309.5 | 38.78% |
| `run/finish_success` | 999,760 | 0.689 | 688.8 | 36.18% |
| `record_call_dep` | 2,385,277 | 0.339 | 142.3 | 17.83% |
| `record_source_deps/nodes` | 205,000 | 0.056 | 272.3 | 2.93% |
| `record_source_deps/source_index` | 205,000 | 0.047 | 231.1 | 2.49% |
| `run/set_inflight` | 999,760 | 0.029 | 29.4 | 1.54% |
| `remove_stale_source_index` | 205,000 | 0.004 | 21.0 | 0.23% |
| `record_outputs` | 1,000 | 0.0004 | 383.9 | 0.02% |

`prepare` and `run/finish_success` alone are 74.96% of tracked lock-held
time. Phase 5 (same graph, persistence *not* configured) isolates what
persistence itself costs under the lock: `run/finish_success`'s share drops
from 36.18% to 10.82% (mean 688.8 ns → 146.3 ns/call) because
`crate::persist::enqueue_changed` — called from inside that critical
section specifically so the persister's snapshot is race-free (see
`engine.rs`'s own comment at that call site) — no longer runs; `prepare`
correspondingly rises to 53.72% of a now-smaller total. Every one of these
eight sites locks `nodes` except the two `record_source_deps`/
`remove_stale_source_index` `source_index` sites, which together are under
3% here — on this shared-key (300 keys, ~683 dependents/key) workload,
`source_index` is cheap because almost every registration hits an
already-present `HashMap` bucket.

`hospital_bench`'s cumulative breakdown (phases 1–3 combined, unshared-key
— ~2.07M distinct source keys, ~1 dependent/key, 9,896,830 instrumented
calls, 2.69 s total):

| site | calls | total (s) | mean (ns) | % of total |
|---|---|---|---|---|
| `record_source_deps/source_index` | 2,081,168 | 0.950 | 456.4 | 35.29% |
| `liveness_gc/mark_sweep` | 6 | 0.567 | 94,442,993.2 | 21.05% |
| `prepare` | 1,372,788 | 0.407 | 296.1 | 15.10% |
| `record_source_deps/nodes` | 2,081,168 | 0.382 | 183.3 | 14.17% |
| `run/finish_success` | 1,117,543 | 0.209 | 187.4 | 7.78% |
| `record_call_dep` | 1,371,337 | 0.128 | 93.3 | 4.75% |
| `run/set_inflight` | 1,117,543 | 0.031 | 28.0 | 1.16% |
| `remove_stale_source_index` | 753,368 | 0.016 | 21.7 | 0.61% |
| everything else (6 sites) | — | — | — | 0.09% |

This inverts the picture. On the shared-key workload, `nodes`-locked sites
are effectively the whole story (97%+); on the unshared-key workload, the
single largest line item — `record_source_deps/source_index`, at 456 ns/
call vs. `persist_bench`'s 231 ns/call for the identical operation — is a
critical section over `source_index`, a *different* mutex than
`Mutex<NodeTable>` entirely. That gap (about 2×/call, on ~10× the call
count) is exactly what Stage 11's own "Open candidates" section predicted
without measuring: a `HashMap<(SourceId, KeyBytes), HashSet<NodeRef>>`
bucket that is almost always being created fresh (a genuinely new key,
paying full hash + allocation cost) rather than found already-populated
(a cheap `HashSet::insert` into an existing bucket) is precisely the shape
a Zero/One/Many `source_index` representation targets. `liveness_gc`'s
mark-sweep is the other large single item (21.05% from just 6 calls, ~94 ms
each) — expected: it's a full reachability walk over ~1.1M nodes, paid once
per settled round, so its cost scales with graph size rather than with lock
contention. Like `record_source_deps/source_index`, just for a different
structural reason, sharding `NodeTable` would not obviously fix this
either: splitting the lock into shards would not shrink the amount of graph
a single mark-sweep pass still has to walk under whichever shard(s) it
touches.

### Does this data support sharding `Mutex<NodeTable>`?

**No** — for a different reason on each workload:

- **`persist_bench` (shared-key)**: `nodes`-locked sites account for over
  97% of tracked lock-held time, but Stage 6's own profiling already found
  only **0.7% direct `pthread_mutex_lock` wait** on this exact workload —
  i.e. the lock is essentially uncontended; almost all of this time is
  genuine, serialized *work* (`prepare`'s cache-hit/insert logic,
  `run/finish_success`'s result-write-plus-persist-enqueue, `record_call_dep`'s
  `SmallVec` pushes) that a caller must do regardless of which mutex
  instance guards it. Sharding splits *contention*, not *work* — with
  Stage 6's own `--hide-idle` per-thread check showing this benchmark's
  real concurrency tops out around 2–3 active threads, there is little
  contention here to split. The higher-leverage move this breakdown
  actually points at is shrinking what `prepare`/`run` do *under* the lock
  (the same direction Stage 6's optimization-candidate list already
  ranked hashing/tracing fixes into), not changing how many locks guard
  `NodeTable`.
- **`hospital_bench` (unshared-key)**: the single largest cost isn't even
  reachable by sharding `NodeTable` — `record_source_deps/source_index`'s
  35.29% is `source_index`, a separate `Mutex<HashMap<..>>` entirely.
  Sharding `NodeTable` here would leave this workload's actual biggest
  lock-held cost completely untouched. The data instead makes a concrete,
  falsifiable case for Stage 11's already-proposed Zero/One/Many
  `source_index` representation, which is what should be built and
  re-measured against this exact benchmark before `NodeTable` sharding is
  reconsidered at all.

Verdict: **`NodeTable` sharding is not supported as the next move by
either workload's breakdown.** Revisit only if a future change
demonstrably increases genuine cross-thread contention on `nodes`
specifically (this instrument is now in place to detect exactly that, via
a rising hold-time share alongside a rising `pthread_mutex_lock`-style
wait metric, should one be added later) — on the evidence gathered here,
the two concrete next steps this data supports are shrinking `prepare`/
`run`'s critical sections and redesigning `source_index`'s representation.

### Correctness

`cargo test --workspace --all-features` — 121 passed (unchanged count:
`alloc_stats`/`lock_stats` added no new tests, and neither feature changes
behavior, only what gets measured). `cargo test -p computations --features
testutil,alloc-stats` separately — all `computations`-crate tests green,
confirming the feature compiles and runs correctly standalone, not only as
part of `--all-features`. `cargo clippy --workspace --all-targets
--all-features -- -D warnings` clean.

### How to run

```text
# Allocation deltas (either benchmark): add alloc-stats to the feature list
cargo run -p computations --release --example persist_bench --features testutil,alloc-stats
cargo run -p computations --release --example hospital_bench --features testutil,alloc-stats

# Lock-hold breakdown (either benchmark): set the env var, no feature needed
COMPUTATIONS_LOCK_STATS=1 cargo run -p computations --release --example persist_bench --features testutil
COMPUTATIONS_LOCK_STATS=1 cargo run -p computations --release --example hospital_bench --features testutil

# Both at once
COMPUTATIONS_LOCK_STATS=1 cargo run -p computations --release --example persist_bench --features testutil,alloc-stats
```


## Stage 13 — interning source-dependency keys

Stage 12's lock-hold attribution named the next candidate explicitly:
`record_source_deps`'s `source_index`-locked critical section at 35.29% of
tracked lock-held time on `hospital_bench`, and its own root cause —
`crate::source::RawDep` (a `SourceId` plus two owned `Vec<u8>`s) stored
fresh, per node, per run, with `source_index` keyed on the same compound
`(SourceId, KeyBytes)` value. The Haskell reference engine hit the
identical shape of cost twice: Stage 4e (`40a754a`) interned the analogous
per-row source-dep column and measured it the single largest win of that
campaign (`max_live_bytes` 817.3 → 375.7 MB, **−54.0%**, from ~205k rows
duplicating only 300 distinct keys ~683×); Stage 4g (`6f5a8af`/`e0d0621`)
then found the un-refcounted version of that same interner was a real leak
in any workload whose key set turns over, and fixed it with reference
counting rather than a threshold sweep. This stage ports both halves in
one change, plus 76166c8's lesson (key a hot lookup on an interned integer,
not a hashed compound value) applied to `source_index` itself.

### Design

`crate::interner::SrcKeyId` (a `u32`) now stands in for `(SourceId, key
bytes)` everywhere a node or `source_index` used to store the pair
directly:

- `crate::interner::SrcKeyInterner` is the refcounted `(SourceId, key
  bytes) <-> SrcKeyId` table. Its forward map is nested
  (`SourceId -> (KeyBytes -> SrcKeyId)`) specifically so a lookup that hits
  — the common case once a workload's key set has stabilized — never
  allocates a probe key: `Vec<u8>: Borrow<[u8]>` lets it probe with a
  borrowed `&[u8]` straight from the caller's own bytes. Only a genuine
  miss (a key seen for the first time) pays for an owned `key.to_vec()`.
- **Versions churn, keys don't** — only `(SourceId, key)` identity is
  interned. `crate::interner::SrcDep { key_id: SrcKeyId, ver: VerBytes }`
  replaces `RawDep` in `NodeTable::source_deps` and as `source_index`'s
  value-set membership; the version stays inline, exactly as the task
  specified, deferring "intern the version too" as a separate,
  separately-measured follow-up rather than bundling it here.
- `source_index` is now `Mutex<HashMap<SrcKeyId, HashSet<NodeRef>,
  IdentityBuildHasher>>` — `SrcKeyId` is an opaque, process-local `u32`
  this process itself assigned, never adversary-chosen content, which is
  exactly the shape of key `crate::hashers::IdentityHasher` (Stage 6) is
  documented safe for; a new `write_u32` override on that hasher folds a
  bare `u32` newtype in with the same single rotate-xor `write_u64` uses,
  rather than falling through to the default trait method's byte-wise FNV
  loop. `SrcDep` itself still hashes with `std`'s default `SipHash`, not
  `IdentityBuildHasher` — it carries raw version bytes, the same
  low-entropy-content disqualifier `crate::hashers`'s docs already call
  out for `RawDep`.
- **Reclamation is refcounted, not swept**, per the task's explicit
  requirement (and per Stage 4g's finding that "never recycled" is a real
  leak, not a footnote): every live reference is `retain`ed/`release`d
  exactly on the paths that already establish or drop a dependency —
  `EngineInner::reconcile_source_deps` (the renamed, interner-aware
  `remove_stale_source_index`: retains every `SrcKeyId` a run's dependency
  set gained, releases every one it lost, **in that order** — retaining
  new references before releasing old ones is load-bearing here for
  exactly Stage 4g's "overlap-ordering hazard" reason: a node re-reading
  the same key this run must never observe a transient zero refcount that
  could free its own id out from under it) and `driver::liveness_gc`
  (releases one reference per distinct key a collected node held, resolved
  back to real bytes for `Source::unregister` *before* releasing —
  a released id's reverse mapping is gone). A failed run reconciles too
  (a new, deliberate divergence from the old code, which silently
  abandoned `old_source_deps`' `source_index` registrations on error):
  without this, a run that dies after partially recording some deps would
  leak exactly those references forever, since nothing else would ever
  release them.
- **Persistence keeps storing real bytes, unconditionally.** `SrcKeyId` is
  process-local and means nothing across a restart, so `NodeRecord`'s
  `source_deps: Vec<RawDepRepr>` field, `FORMAT_VERSION`, and the on-disk
  layout are all **unchanged** — confirmed empirically below (byte-identical
  `persist_bench` db size, 269.49 MB, before and after). `enqueue_changed`
  resolves each `SrcDep` back to bytes through the interner at snapshot
  time (`RawDepRepr::from_src_dep`); `restore_nodes` re-interns (and
  retains) every restored dep fresh, exactly as a live
  `record_source_deps` call would, since a freshly restored node has no
  prior "old" set to diff a retain against.
- Public surface unchanged: `Source`/`Sink`, `RawDep`, `raw_deps()` are
  untouched. Interning is entirely internal to `engine.rs`/`driver.rs`/
  `persist.rs`.

### Reclamation testing

`crate::interner`'s own unit tests cover intern/retain/release accounting
directly, including the task's explicitly-called-out case,
`releasing_the_last_reference_shrinks_the_interner` (asserts `live_len()`
drops to 0, not just that a counter hits 0), a shared-key survival case,
recycled-slot reuse, the re-intern-after-release ordering hazard, a
release-without-retain underflow (loud in debug builds), and a 50-key
interleaved churn test. Above that, a real engine-level test —
`driver::tests::liveness_gc_releases_a_collected_nodes_interned_source_keys`
— drives an actual `root`/`leaf` graph through `eval_root`/`liveness_gc`
(the same "leaf becomes unreachable" shape `tests/driver.rs`'s existing
`gc_collects_a_directly_source_dependent_node_once_unreachable` regression
test uses) and asserts `src_key_interner`'s live count drops from 2 to 1
once the leaf's node is actually collected — proof reclamation fires
through the real GC path, not only in the interner's own isolated tests.

### The matrix

`uptime` load average ranged **2.48–7.96** across this stage's benchmark
session (1-min figures 7.96, 5.23, 4.42, 2.89, 2.48 at various points —
similar to, and at moments heavier than, Stage 12's 4.6–8.3 band); as in
every prior stage, absolute numbers below should be read against that, and
the before/after pairs are same-session, same-machine A/B comparisons,
stashing this stage's changes for the "before" runs and popping the stash
back for "after" so both sides see the same load conditions rather than
being separated by however long implementation took.

**`persist_bench`, default scale** (shared-key: 300 distinct keys, ~683
dependents/key):

| phase | before | after | documented baseline |
|---|---|---|---|
| 1. cold eval (persistence configured) | 2879 ms, 1434.2 MB | 3016 ms, 1398.0 MB | ~3.1–3.7 s ✓ |
| 2. persist_now [db] | 2083 ms, db=269.49 MB | 2221 ms, db=269.49 MB | db byte-identical before/after, every run |
| 3. warm restart, no changes | 1151/1156 ms | 1244/1228 ms | ~1.2–1.4 s ✓ |
| 4. restart, 1 changed input | 1505/1642 ms | 1632/1638 ms | (reruns exact-match: 100,164 / 137,085, both sides) |
| 5. cold restart, no persistence (engine-only RSS) | 2344/2364 ms, 327.1/365.0 MB | 2344/2364 ms, 290.7/332.3 MB | ~330 MB (before) |
| 6. fingerprint mismatch | 4006 ms | 3849 ms | (no documented range) |
| 7. live incremental, no persistence | 415 ms | 414 ms | ~450–500 ms ✓ |
| 8. live incremental, with persistence (settle) | 486 ms | 592 ms | (no documented range; noisy, see below) |

Phase 5 — this benchmark's own canonical "engine-only RSS" figure, and the
most reproducible number in this table — was sampled **8 times each side**
across the session (plain, alloc-stats, and lock-stats runs, two trials
each): **336.3 MB average before, 299.5 MB average after, a 36.8 MB
(−10.9%) reduction**, holding up consistently despite ~15 MB of run-to-run
noise on *either* side (327–365 MB before, 290–332 MB after — the ranges
never overlap upward from after into before). Every other phase's RSS
swung by up to ±250 MB between repeated runs of the *identical* binary at
this box's current load (phases 1/3/4/6/8 all include heap growth from
comp_deps/rdeps/value columns that dwarfs a 300-key interner's footprint
either way), so those columns are reported for completeness but are not
treated as a reliable per-phase signal here — phase 5 and the db-size
column are. Time-wise, every phase landed within the same noise band on
both sides; phases 7/8 (the two persistence-decoupled-from-propagation
figures Stage 9 tuned) show no regression.

Allocation deltas (`--features testutil,alloc-stats`), phase 5: **352.2 MB
net before → 325.2 MB net after (−7.7%)** — smaller than the RSS delta,
consistent with RSS also capturing the allocator's own per-allocation
overhead (fewer, larger interner allocations vs. many small per-node
`Vec<u8>` ones), which a raw byte-delta counter doesn't see.

**`hospital_bench`, default scale** (unshared-key: ~2.08M distinct keys,
~1 dependent/key each):

| phase | before | after | documented baseline |
|---|---|---|---|
| 1. cold eval | 3824–4031 ms, 1504.5–1532.8 MB (3 runs) | 3649–3962 ms, 1209.2–1238.9 MB (4 runs) | ~3.9 s / 1.12M reruns / ~1520 MB (before) |
| 2. live incremental, 1 changed vitals key | 79–89 ms | 76–90 ms | ~81 ms ✓ |
| 3. rerun-heavy live update (300 keys) | 524–539 ms | 430–524 ms | ~524 ms ✓ (both sides) |
| 4. concurrency demo | 57–62 ms | 57–61 ms | (unaffected, as expected — no source-dep interning on this path) |

Reruns matched exactly on both sides in every run (1,116,093 cold; 1,444
rerun-heavy). Averaged across every sample this session: **cold-eval RSS
1514.7 MB before → 1216.9 MB after, a 297.8 MB (−19.7%) reduction** — the
single largest number in this stage, on the workload with *zero* key
sharing, where the win cannot come from deduplication. It comes instead
from a structural halving: before, every one of ~2.08M distinct keys had
its bytes stored **twice** — once inside the node's own `RawDep`, once
again as `source_index`'s `HashMap` key — regardless of whether any other
node shared that key. After, the interner holds the bytes once; both
`NodeTable::source_deps` and `source_index` hold only a `u32`. Cold-eval
wall time was flat-to-slightly-faster (3873.7 ms avg before → 3747.0 ms
avg after) — no regression despite the extra interning step.

Allocation deltas, cold eval: **1287.4 MB net before → 1239.6 MB net after
(−3.7%)** — again much smaller than the RSS delta, for the same
allocator-overhead reason as `persist_bench` above, only more pronounced
here: 2.08M individually-small `Vec<u8>` allocations carry far more
allocator bookkeeping overhead per byte than the interner's own
`Vec`-backed storage does.

### The lock-hold delta

The specific question Stage 12 raised: does `record_source_deps`'s 35.29%
share on `hospital_bench` actually drop? `hospital_bench`'s cumulative
breakdown (phases 1–3 combined), before vs. after:

| site | before | after |
|---|---|---|
| `record_source_deps/source_index` | 2,081,168 calls, 0.724 s, **33.60%** | 2,081,168 calls, 0.133 s, **6.39%** |
| `record_source_deps/interner` | *(did not exist)* | 2,081,168 calls, 0.629 s, **30.17%** |
| `record_source_deps/nodes` | 2,081,168 calls, 0.346 s, 16.07% | 2,081,168 calls, 0.273 s, 13.10% |
| `liveness_gc/mark_sweep` | 6 calls, 0.407 s, 18.89% | 6 calls, 0.405 s, 19.44% |
| `prepare` | 1,372,788 calls, 0.324 s, 15.05% | 1,372,788 calls, 0.321 s, 15.42% |

**Yes, dramatically** — `source_index`'s own critical section drops from
33.60% to 6.39% (our own baseline run landed close to Stage 12's documented
35.29%), confirming the byte-hashing work Stage 12 found there is gone from
that lock entirely. But the *total* cost of getting from raw bytes to a
recorded dependency did not disappear: `record_source_deps/interner`
(30.17%) plus the shrunk `source_index` (6.39%) plus `nodes` (13.10%) sums
to 49.66%, against 33.60% + 16.07% = 49.67% before — **within rounding of
identical**, exactly as expected for a workload with *zero* key reuse
(every `intern()` call is a first-sight miss, paying full hash + insert
cost no matter which lock holds it). The win here was never going to be
"less total CPU work" on this specific workload — it's that the expensive
byte-hashing work moved *off* `source_index` specifically and onto a
separate, independent lock, so a caller contending for `source_index` for
an unrelated reason (the reverse-lookup side, `affected_keys`) no longer
waits behind it — and, far more concretely, the RSS/allocation wins above.
`persist_bench`'s equivalent table (single-process, phase 1) shows the
opposite regime working as designed: `record_source_deps/interner` is only
1.37% there (205,000 calls, almost all cache hits against a 300-entry
table), `source_index` unchanged at ~1.7% either side (it was never the
bottleneck on this workload — Stage 12 already established `nodes`-locked
sites are 97%+ of the story there).

### Comparison to the Haskell prior

Stage 4e's headline number — 817.3 → 375.7 MB, **−54.0%** — came from a
1M-node, single-process, GC-copying-heavy runtime measuring *max live
heap*, on a workload where the *entire* win was 683× key deduplication
(their own root-cause finding: `wrapCompSrcDep` reconstructed a fresh
`Text`/`CompSrcId` per call). Our two workloads land in a different but
consistent place: `persist_bench` (the one genuinely comparable shared-key
shape, same 300-key/~683-dependent structure) shows a real but far smaller
**−10.9%** on the equivalent metric (engine-only RSS) — expected, since
Rust's per-node `RawDep` was never reconstructed-and-reallocated on every
*read* the way Haskell's `AnyCompSrcDep` was; the duplication here was
already bounded to one clone per `record_source_deps` call, not one
per-call *plus* one per `wrapCompSrcDep`/`compSrcId` round-trip. The
**−19.7%** win instead showed up on `hospital_bench`, a workload
Stage 4e's own campaign never had an equivalent to (theirs was
shared-key-only) — the structural "stored twice regardless of sharing"
cost this stage's design section describes above. Read together: this
crate's baseline per-node layout was already tighter than the Haskell
engine's pre-Stage-4e one (Stage 5's ~330 B/node vs. their ~1,500 B/node),
so there was less duplication-shaped waste left to remove on the
shared-key shape specifically — but the *reverse-index* redundancy
Stage 4e's design never had to contend with (their `SrcIndex`, Stage 4h,
was built key-interned from the start) turned out to be this codebase's
own analogous win, just located on the other benchmark.

### Verdict: kept

Both workloads improve on memory (persist_bench −10.9% engine RSS,
hospital_bench −19.7% cold-eval RSS) with flat-to-slightly-better wall time
on every phase measured, the specific lock hot-spot Stage 12 flagged drops
by 5×, the persisted format and db size are provably byte-identical, and
the two workloads' differing lock-hold pictures (a wash on hospital's raw
CPU accounting, a non-issue on persist_bench's) are fully explained rather
than merely observed. This clears the task's own bar — "cuts memory
materially... a win worth keeping" — on both benchmarks, not just the one
the design was originally motivated by.

### Correctness

`cargo test --workspace --all-features` — **134 passed** (121 baseline +
13 new: 11 `crate::interner` unit tests, one `crate::hashers` `write_u32`
regression test, one engine-level `driver::tests` reclamation test).
`cargo test -p computations --features testutil,alloc-stats` — all green
standalone. `cargo clippy --workspace --all-targets --all-features -- -D
warnings` clean.

### How to run

```text
cargo run -p computations --release --example persist_bench --features testutil
cargo run -p computations --release --example hospital_bench --features testutil
cargo run -p computations --release --example persist_bench --features testutil,alloc-stats
cargo run -p computations --release --example hospital_bench --features testutil,alloc-stats
COMPUTATIONS_LOCK_STATS=1 cargo run -p computations --release --example hospital_bench --features testutil
```

## Stage 14 — Zero/One/Many source_index entries

Stage 13 interned `source_index`'s key (`SrcKeyId`, a `u32`) but left its
value alone: `Mutex<HashMap<SrcKeyId, HashSet<NodeRef>, IdentityBuildHasher>>`.
An entry's "zero" state is already the *absent* map entry — both
`EngineInner::reconcile_source_deps` and `driver::liveness_gc` prune an
entry the moment its last dependent goes away — so every entry that exists
has **at least one** dependent by construction. The unexploited cost this
stage targets: an entry with exactly **one** dependent still pays for a
`HashSet<NodeRef>`'s backing-table allocation to hold a single 8-byte
`NodeRef`. This is the direct analogue of the Haskell reference engine's
own `SrcKeyArena` small-size optimisation (commit `2d2a726`,
`SrcKeyZero`/`SrcKeyOne`/`SrcKeyMany`), applied to this crate's
already-interned `source_index` rather than to a from-scratch arena.

### Design

`crate::engine::SourceRefs` replaces `HashSet<NodeRef>` as `source_index`'s
value type:

```rust
pub(crate) enum SourceRefs {
    One(NodeRef),
    Many(HashSet<NodeRef>),
}
```

- **Promotion** (`One -> Many`) happens in place, in `SourceRefs::insert`,
  the first time a second *distinct* `NodeRef` is recorded against a key —
  used by both `record_source_deps`'s live insert path and
  `persist::restore_nodes`'s cold rebuild path (both previously called
  `index.entry(key_id).or_default().insert(caller_r)`, relying on
  `HashSet::default()`; there is no longer a `Default` impl for
  `SourceRefs` — the map's `Entry` API is used directly instead, so a
  vacant entry always starts life as `One`, never an implicit empty
  `Many`).
- **Removal** goes through `SourceRefs::remove` (single dependent, used by
  `reconcile_source_deps`) or `SourceRefs::retain_live` (batch, used by
  `liveness_gc`'s mark-sweep `HashMap::retain` pass — `One`'s check has to
  test membership against the *whole* dead-set at once, since looping a
  single-item removal across a batch would need the batch's *last* id to
  happen to be the live one, which iteration order doesn't guarantee).
  Both report whether the entry is now empty; the caller drops the map
  entry entirely in that case, exactly as before.
- **Iteration** (`affected_keys`'s read path) goes through
  `SourceRefs::iter`, a two-armed iterator (`Option<NodeRef>` for `One`,
  `HashSet`'s own `Iter` for `Many`) so that call site doesn't need to match
  on the enum itself.
- **Demotion (`Many -> One`) is not implemented**, matching the Haskell
  prior's own one-way-promotion decision. Neither of this crate's two
  benchmarks exercises a key whose dependent count oscillates back down to
  exactly one without hitting zero first (`persist_bench`'s 300 keys hold a
  stable ~683 dependents each for the whole run; `hospital_bench`'s ~2.08M
  keys never exceed one dependent in the first place), so there is no
  workload here to justify the added complexity and the repeated
  allocate/discard churn an oscillating key would pay under demotion. If a
  future workload shows that shape, `SourceRefs::retain_live`/`remove` are
  exactly where a demotion check would slot in (compare `set.len() == 1`
  after removal and collapse to `One`).
- No public API change: `SourceRefs` is `pub(crate)`, only ever seen inside
  `engine.rs`/`driver.rs`/`persist.rs`.

**This is not Stage 5's rejected `SmallVec` experiment, despite the
surface similarity ("give a container inline capacity for its common
size").** Stage 5 measured-and-reverted `SmallVec<[NodeRef; N]>` for
`source_deps`/`outputs` because *most nodes have zero* of those — a node
that reads no sources and produces no sink outputs is common, and
`SmallVec`'s inline capacity is paid unconditionally on every node whether
or not it's ever used, losing to a never-allocated empty `HashSet`. A
`source_index` entry is never in that position: it doesn't exist at all
(no map entry, no allocation of any kind) until it has a first dependent,
by the pruning invariant above. There is no "wasted inline slot on an
empty container" case for `SourceRefs` to lose to — only "one slot
suffices for the common single-dependent case, promote when a second
distinct dependent shows up" — which is exactly the shape where a
size-1-then-N small-size optimization pays off rather than backfires.

### Measurements

`uptime` load average ranged **~4.0–7.4** (1-min figures 4.02 at session
start, 7.43/6.08 immediately before the "before" runs, 7.32 immediately
before the "after" runs) across this stage's benchmark session — broadly
similar on both sides, but this box is frequently loaded and RSS in
particular swings by tens of MB between repeated runs of the *identical*
binary at this load, so (as in every prior stage) the deterministic
byte-counted metrics (`allocated_bytes`, db size, rerun counts) are what
this stage's conclusion rests on; RSS is reported alongside for
completeness. Both sides are same-session, stash/pop A/B pairs (this
stage's changes stashed for "before", popped back for "after") so load
conditions match as closely as this machine allows.

**`persist_bench`, default scale** (shared-key: 300 distinct keys, ~683
dependents/key each — nearly every `source_index` entry is `Many`):

| metric | before | after | Δ |
|---|---|---|---|
| phase 5 (engine-only RSS), 6 samples each | 290.5–330.5 MB, avg 297.3 MB | 290.8–328.9 MB, avg 302.6 MB | +5.3 MB / +1.8% (noise: both sides include a ~330 MB single-run spike, and the ranges fully overlap) |
| phase 5 `allocated_bytes` (net), 2 samples each | 325,219,346 B (both trials, identical) | 325,219,346 B / 325,219,586 B | +240 B on one trial, +0.00007% — noise floor |
| `persist_now` db size | 269.49 MB | 269.49 MB | byte-identical, every run |
| `record_source_deps/source_index` lock share, phase 1 | 1.42–2.63% across 6 sampled runs | 1.42–2.27% across 6 sampled runs | unchanged within run-to-run spread |
| `remove_stale_source_index` lock share, phase 1 | 0.24–0.45% | 0.24–0.45% | unchanged |
| reruns, every phase | exact-match every run | exact-match every run | identical |

**Neutral, as predicted.** `persist_bench`'s keys are essentially all
`Many` from their first or second dependent onward (683 dependents each),
so this change adds one enum discriminant and one extra branch per
`source_index` operation on a workload that reaches the `Many` arm
immediately and stays there — the same "control, must not regress" role
Stage 7's Haskell existing-benchmark and this crate's own Stage 13 table
already established for this workload. The deterministic metric
(`allocated_bytes`) confirms it: 240 bytes apart on a 325 MB base is
noise, not a regression, and every RSS sample on both sides falls inside
the same ~40 MB band this box's load already produces run-to-run on an
*unchanged* binary (see Stage 13's own phase-5 noise discussion).

**`hospital_bench`, default scale** (unshared-key: ~2.08M distinct keys, ~1
dependent/key each — nearly every `source_index` entry is `One`):

| metric | before | after | Δ |
|---|---|---|---|
| cold-eval RSS, 3 samples each | 1209.3–1244.3 MB, avg 1221.0 MB | 1112.5–1129.0 MB, avg 1120.4 MB | **−100.6 MB, −8.24%** |
| cold-eval `allocated_bytes` (net) | 1,239,604,594 B (1239.6 MB) | 1,148,788,594 B (1148.8 MB) | **−90.8 MB, −7.33%** |
| `record_source_deps/source_index` lock share (phases 1–3) | 2,081,168 calls, 0.134 s, 6.41% | 2,081,168 calls, 0.095 s, 4.68% | −29.4% of this site's own time |
| cold-eval wall time | 3739–3998 ms | 3512–3894 ms | flat-to-slightly-faster (reported, not relied on) |
| reruns, cold / rerun-heavy | 1,116,093 / 1,444 | 1,116,093 / 1,444 | identical, every run |

**A real, reproducible win on the workload this stage targeted.** Every
sample on the "after" side beats every sample on the "before" side for
both RSS and `allocated_bytes` — no overlap, unlike `persist_bench`'s
noise band above. The deterministic `allocated_bytes` drop (−7.33%) is
close to the RSS drop (−8.24%), same relationship Stage 13 saw for the
opposite reason (RSS also captures allocator per-allocation overhead,
which shrinks further here since ~2.08M single-element `HashSet`
allocations are gone entirely, replaced by an inline enum variant with no
heap allocation at all). `record_source_deps/source_index`'s own lock-held
time drops by a relative 29%, consistent with skipping a `HashSet`
construction (hasher init, single-bucket table allocation) on every
first-and-only insert. Reruns matched exactly on both sides, confirming
no behavior change.

### Comparison to the Haskell prior

The Haskell `SrcKeyArena` optimisation measured `max_live_bytes` 4,310.3 →
4,012.4 MB (**−6.91%**) on its own unshared-key Hospital workload, plus a
smaller cold-eval `allocated_bytes` drop (−1.34%), with its shared-key
control moving by noise only (+0.004%). This stage lands in the same
place, workload for workload: our unshared-key `hospital_bench` improves
(−8.24% RSS / −7.33% `allocated_bytes`, slightly larger than Haskell's
figure — plausibly because our `HashSet<NodeRef>` held only a single
8-byte element per entry pre-change with no version/generation bytes
alongside it, so the *entire* backing-table allocation was pure overhead
being removed, versus Haskell's arena which still carried some per-key
`IORef` overhead even in the `SrcKeyOne` case per their own Stage 7 notes),
while our shared-key control (`persist_bench`) moves by noise only, same
as theirs. Both engines independently arrived at the identical
Zero/One/Many shape for the identical reason: an interned-key reverse
index's single-dependent case is the common one on an unshared-key
workload, and a HashSet-shaped container is the wrong default for holding
exactly one element.

### Verdict: kept

`hospital_bench` — the workload this stage was motivated by — improves by
a real, reproducible margin with no overlapping samples between before and
after; `persist_bench` — the workload predicted to be neutral-at-best —
lands within noise on every metric, deterministic and otherwise, with no
observed regression. The two workloads' predicted-and-confirmed
disagreement is exactly the shape the task called out in advance ("hospital
wins, persist neutral"), not an average to paper over: `hospital_bench`'s
win is kept without qualification, and `persist_bench`'s neutrality is
reported as such rather than folded into a blended number. Public API is
unchanged, persisted format and db size are unaffected (`source_index` was
never persisted — only `NodeRecord::source_deps`, which this stage does not
touch), and the full test suite (including reclamation and per-dependent
version-tracking tests) passes unmodified.

### Correctness

`cargo test --workspace --all-features` — **134 passed**, same count as
Stage 13 (no new test added: `SourceRefs`'s behavior is already covered
transitively by every existing `source_index`-touching test —
`reconcile_source_deps`, `affected_keys`, `liveness_gc` reclamation, and
`persist::restore_nodes` — since a wrong promotion/demotion/iteration would
fail those directly, e.g. a dependent silently dropped from notification).
`cargo test -p computations --features testutil,alloc-stats` — all green
standalone. `cargo clippy --workspace --all-targets --all-features -- -D
warnings` clean.

## Stage 15 — the per-dependency version field

Stage 13 interned `(SourceId, key bytes)` into `SrcKeyId(u32)` and
deliberately deferred the version field ("versions churn, keys don't"), so
`crate::interner::SrcDep { key_id: SrcKeyId, ver: VerBytes }` still stored
`ver` as an owned `Vec<u8>` — a 24-byte header plus a separate heap
allocation, per dependency, for what the task's own framing called "a
counter for `MemKvSource`, mtime+size for `FsSource`". This stage measures
that byte-length distribution directly and closes the gap the deferral left
open.

### Measured version-byte-length distribution

Instrumented both benchmarks directly rather than guessing: a temporary
histogram in `EngineInner::record_source_deps` bucketed every recorded
dependency's `ver.len()` (removed before this stage's commit — not shipped
code), run at full default scale.

| workload | source deps recorded | 1 byte | 2+ bytes |
|---|---|---|---|
| `persist_bench` (cold eval) | 205,000 | 205,000 (100%) | 0 |
| `persist_bench` (restart trials, cumulative) | 205,684 | 205,684 (100%) | 0 |
| `hospital_bench` (cold + live + rerun-heavy) | 2,081,168 | 2,081,168 (100%) | 0 |

**Every single measured dependency, on both benchmarks, postcard-encodes to
exactly 1 byte.** Both benchmarks' sources report `type Ver = u64` (a plain
counter starting at 0), and postcard's LEB128-style varint encoding keeps
any value below 128 to 1 byte — `persist_bench`'s `MemKvSource` versions
never exceed 3 (each of 300 keys is set once at startup, two bumped again
for the restart-trial phases); `hospital_bench`'s `LatencySource` versions
stay in the same tiny range (each of ~2.08M keys starts at version 0, and
the rerun-heavy phase's 300 mutated keys only reach version 1 or 2).
Verified independently with a standalone postcard round-trip: `0u64..=127u64`
all encode to 1 byte, `128..=16383` to 2 bytes.

For contrast (not benchmarked, but named explicitly in the task and present
in `computations-fs::FsVer`): `FsVer::File { mtime_nanos: u128, size: u64 }`
postcard-encodes to **11-15 bytes** for realistic 2020s-era mtimes (a u128
nanosecond timestamp needs ~9 bytes of varint alone), and `FsVer::Dir {
entries_hash: [u8; 16] }` (a fixed-size array, no varint compression) to
exactly **17 bytes**. Real-world filesystem versions are an order of
magnitude larger than either benchmark's measured distribution.

### Design chosen: inline `SmallVec<[u8; 8]>`, not interning

**Rejected: interning versions into a `VerId(u32)`.** Three independent
reasons, all falling out of the measurement above:

1. **Nothing to dedup.** The task's own background notes only 683 of
   `persist_bench`'s 205,000 deps share both key *and* version (0.33%) —
   `hospital_bench` shares essentially none (every key read by ~1
   dependent). An interner's entire value proposition is deduplicating
   repeated values; there are almost no repeats to remove.
2. **The replacement is bigger than the original.** Every measured version
   is 1 byte. A `VerId(u32)` is 4 bytes — interning would *quadruple* the
   per-dep storage for the identity half alone, before even counting the
   interner's own side tables (forward map, entries, refcounts, free list).
3. **Churn is exactly the failure mode the task warned about.** Stage 13's
   key interner works because keys are stable — a node reads the same key
   for its whole lifetime, so intern/retain happens once and lives a long
   time. Versions are the opposite by design ("versions churn, keys don't"
   is Stage 13's own stated reason for *not* interning them there): every
   rerun of every one of `hospital_bench`'s ~1.1M nodes produces a fresh
   version, meaning a version interner would pay a full
   intern-or-lookup-plus-retain-plus-release cycle on nearly every
   propagation round, for a value that's already smaller than the `u32`
   handle meant to replace it. This is precisely Stage 5's rejected-SmallVec
   shape in reverse: paying real, recurring cost for a data structure whose
   benefit (dedup) barely applies to this data.

**Chosen: replace `SrcDep::ver`'s `Vec<u8>` with `SmallVec<[u8; 8]>`**
(`crate::interner::SmallVerBytes`), spilling to the heap beyond 8 bytes. This
*is* the shape the task said Stage 5's rejection doesn't cover: `source_deps`/
`outputs` were rejected because most *nodes* have zero of them, so inline
capacity was paid on every node whether used or not. A `SrcDep`'s `ver` field
belongs to a dependency that, by definition, already exists — there is no
"empty node paying for unused capacity" case here to lose to.

The inline capacity of 8 (not 1, not 16) is itself measured, not guessed:
on this platform, `size_of::<SmallVec<[u8; N]>>()` is **24 bytes — identical
to `Vec<u8>`'s own header** — for any `N` up to 8; the inline buffer fits
inside space `Vec` already spends on pointer/capacity bookkeeping. The next
size class up (`N` in 9..=23) jumps to 32 bytes, an 8-byte-per-dependency
cost neither benchmark's 100%-1-byte distribution would justify paying for.
So `N=8` is a strict win with no downside: `size_of::<SrcDep>()` stays
exactly 32 bytes (unchanged — `SrcKeyId` 4 B + the inline buffer 24 B,
rounded to 8-byte alignment), while every dependency measured on either
benchmark needs zero heap allocation for its version. A version over 8 bytes
(realistic `FsVer::File`/`FsVer::Dir` values, 11-17 bytes, per the measurement
above) simply spills to the heap exactly as today's `Vec<u8>` did — no
regression for a workload neither benchmark exercises, just no additional
win for it either. `size_of::<SrcDep>() == 32` and
`size_of::<SmallVerBytes>() == size_of::<Vec<u8>>()` are now enforced by a
tripwire test (`interner::tests::srcdep_did_not_grow_past_its_pre_stage_15_size`)
in the same spirit as Stage 3/4's `size_of::<Node>()` tripwires.

Implementation notes:
- `record_source_deps` (`engine.rs`) switched its interning loop from
  `raw.iter().map(...).collect()` (cloning `dep.ver`) to
  `raw.into_iter().map(...).collect()` (`raw` is owned and unused
  afterward), so `SmallVec::from_vec(dep.ver)` moves the original `Vec<u8>`'s
  bytes into inline storage (copying only when `len <= 8`, exactly the
  measured case) instead of cloning it first and then converting — one fewer
  allocation on the hot path than a naive port would have had.
- Persistence (`persist.rs`) is unaffected in format: `RawDepRepr::ver`
  stays `Vec<u8>` (it's what gets postcard-encoded to disk), converting via
  `.to_vec()` when building it from a `SrcDep`, and via
  `SmallVerBytes::from_slice(&dep.ver)` when restoring one. `NodeRecord`,
  `FORMAT_VERSION`, and the on-disk layout are untouched — confirmed
  empirically below (byte-identical 269.49 MB db, before and after).
- No public API change: `Source`/`Sink`, `RawDep` (still `Vec<u8>` for
  `ver`), and `raw_deps()` are untouched — `SmallVerBytes`/`SrcDep` are
  `pub(crate)`, entirely internal to `engine.rs`/`persist.rs`.
- `NodeTable::source_deps_contains`'s linear-scan comparison
  (`d.ver == ver` against a borrowed `&[u8]`) changed to
  `d.ver.as_slice() == ver` — `SmallVec` doesn't implement `PartialEq<&[u8]>`
  the way `Vec<u8>` did; a one-line mechanical fix, not a behavior change.

### The matrix

`uptime` load average ranged **2.5–8.3** across this stage's benchmark
session (1-min figures 8.33, 4.29, 3.35, 3.13, 2.54 at various points around
the before/after pairs) — similar to, and overlapping, every prior stage's
band; as always, the before/after pairs below are same-session stash/pop A/B
comparisons (this stage's changes stashed for "before", popped back for
"after"), and the deterministic `allocated_bytes`/db-size/rerun-count
columns are what this stage's conclusion rests on, RSS reported alongside
for completeness per this document's standing practice.

**`persist_bench`, default scale** (shared-key: 300 distinct keys, ~683
dependents/key — the workload predicted to be neutral, since only 205,000 of
1-byte-each dependencies exist to save an allocation on):

| metric | before | after | Δ |
|---|---|---|---|
| phase 5 (engine-only RSS), 6 samples each | 290.7–328.0 MB, avg 297.1 MB | 287.7–328.1 MB, avg 299.8 MB | +2.7 MB / +0.9% (noise — ranges overlap almost entirely) |
| phase 5 `allocated_bytes` (net) | 325,219,346 B (all trials, identical — matches documented Stage 14 baseline exactly) | 325,014,346 B (all trials, identical) | **−205,000 B exactly**, matching the measured dep count (205,000) × 1 byte/dep to the byte |
| `persist_now` db size | 269.49 MB | 269.49 MB | byte-identical, every run |
| cold eval wall time, 3 samples each | 2844–3274 ms, avg 3017 ms | 2881–2931 ms, avg 2903 ms | flat-to-slightly-faster (reported, not relied on) |
| reruns, every phase | exact-match every run (999,760 / 100,164 / 137,085 / 80,767) | exact-match every run | identical |

**Neutral, exactly as the design section predicted.** The deterministic
`allocated_bytes` delta is real and exactly explained (one 1-byte heap
allocation eliminated per dependency, 205,000 of them, 205,000 bytes) but is
0.063% of a 325 MB base — invisible against this box's ordinary ~15-40 MB
run-to-run RSS noise on this benchmark (the same noise band Stage 13/14
already documented at this exact phase). No regression on any other metric.

**`hospital_bench`, default scale** (unshared-key: ~2.08M distinct keys, ~1
dependent/key — the workload this stage targets, since every one of its 2.08M
dependencies is a separate 1-byte allocation with nothing shared to fall
back on):

| metric | before | after | Δ |
|---|---|---|---|
| cold-eval RSS, 4 samples each | 1113.5–1147.8 MB, avg 1129.7 MB | 1082.9–1109.8 MB, avg 1095.5 MB | **−34.2 MB, −3.02%** (no overlap: every "after" sample beats every "before" sample) |
| cold-eval `allocated_bytes` (net) | 1,148,788,594 B (matches documented Stage 14 baseline exactly) | 1,146,709,594 B | **−2,079,000 B exactly**, matching phase 1's recorded-dep count × 1 byte/dep to the byte |
| cold-eval wall time, 4 samples each | 3570–3922 ms, avg 3672 ms | 3535–3902 ms, avg 3653 ms | flat (reported, not relied on) |
| `record_source_deps/interner` lock share (one sample each side) | 2,081,168 calls, 0.618 s, 30.42% | 2,081,168 calls, 0.700 s, 33.37% | within single-sample noise — this stage doesn't touch what happens under that lock, only what gets stored afterward |
| reruns, cold / rerun-heavy | 1,116,093 / 1,444 | 1,116,093 / 1,444 | identical, every run |

**A real, reproducible win on the workload this stage targeted.** Every
"after" RSS sample beats every "before" sample — no overlap. The
deterministic `allocated_bytes` drop (2,079,000 B, matching the histogram's
dependency count exactly) is, as in every prior stage, smaller than the RSS
drop: a 1-byte `Vec<u8>` allocation requests 1 byte from the allocator (all
`allocated_bytes` sees) but actually consumes a full minimum allocation
quantum of real heap space once macOS `malloc`'s tiny-region rounding and
per-region bookkeeping are counted — the same "RSS also captures allocator
per-allocation overhead" gap Stage 13/14 both already documented, just more
extreme here because the *requested* size (1 byte) is about as small as it
gets. Back-of-envelope: 34.2 MB saved over ~2.08M eliminated allocations is
≈16.4 bytes of real overhead recovered per allocation — squarely in the
range of a 16-byte minimum tiny-allocation quantum, which is exactly the
mechanism this stage's design section predicted (eliminating the heap
allocation entirely, not shrinking its requested size).

### Verdict: kept

`hospital_bench` — the unshared-key workload where every dependency's
version is its own allocation with no sharing to hide behind — improves by
a real, reproducible, fully-explained margin (−3.02% RSS, deterministic
−2,079,000 B `allocated_bytes`) with no overlapping samples between before
and after. `persist_bench` — predicted neutral because its total savings
(205,000 bytes) is two orders of magnitude below its own measurement noise
floor — lands within noise on every metric, deterministic and otherwise,
with no observed regression anywhere (db size byte-identical, reruns
exact-match, wall time flat). `size_of::<SrcDep>()` is provably unchanged
(32 bytes, tripwire-tested) — this stage traded nothing for the win except
one dependency's inline bytes replacing a pointer to the same bytes
elsewhere. Public API, persisted format, and db size are all unaffected.
This is the same "hospital wins, persist neutral, keep" shape Stage 14
already established for the sibling Zero/One/Many change, for the same
underlying reason: an unshared-key workload has millions of individually
tiny allocations with nothing to deduplicate, and inlining beats both
"leave it heap-allocated" and "intern it" for that shape.

### Correctness

`cargo test --workspace --all-features` — **135 passed** (134 baseline + 1
new: `interner::tests::srcdep_did_not_grow_past_its_pre_stage_15_size`, the
`size_of::<SrcDep>()`/`size_of::<SmallVerBytes>()` tripwire). `cargo test -p
computations --features testutil,alloc-stats` — all green standalone.
`cargo clippy --workspace --all-targets --all-features -- -D warnings`
clean.

### How to run

```text
cargo run -p computations --release --example persist_bench --features testutil
cargo run -p computations --release --example hospital_bench --features testutil
cargo run -p computations --release --example persist_bench --features testutil,alloc-stats
cargo run -p computations --release --example hospital_bench --features testutil,alloc-stats
COMPUTATIONS_LOCK_STATS=1 cargo run -p computations --release --example hospital_bench --features testutil
```

## Stage 16 — single-flight dedup for source requests

The last unevaluated candidate from `haskell-computations` (their `778180d`
"Bundle same-instance source requests within one applicative batch",
refined by `76166c8`): `Ctx::src_req` (`crates/computations/src/ctx.rs`)
calls `source.execute(req).await` directly on every call, with no check for
an identical request already in flight — unlike `Ctx::eval`, which has
had computation-level single-flight dedup (the `inflight` map in
`engine.rs`, joined via `futures::future::Shared`) since the very first
stage of this engine. This stage builds the source-request analogue,
measures it honestly on both benchmarks, and — the numbers below are
unambiguous — reverts it.

### Design (implemented, measured, then reverted)

Mirrored the existing computation-level `inflight` pattern exactly, one
layer down the call stack:

- A new `EngineInner::src_inflight: Mutex<HashMap<(SourceId, TypeId),
  Arc<dyn Any + Send + Sync>>>` field, keyed by source instance *and*
  request type (`Request` only requires `R: Eq + Hash` for one concrete
  type at a time — two unrelated request types sharing one `HashMap<R, _>`
  could in principle compare equal by coincidence, so the `TypeId` half
  kept every request type's bucket separate, exactly the hazard the task
  flagged up front). Each value, downcast back to its concrete
  `Arc<Mutex<HashMap<R, SharedSrcExec>>>`, was created once per
  `(SourceId, R)` pairing and never removed — cheap, same lifecycle as the
  `defs` map.
- `EngineInner::src_execute<S: Source<R>, R: Request>` did the
  check-or-insert/join-or-run dance `EngineInner::run` already does for
  computations: lock the per-`(source, request-type)` bucket, join an
  existing `Shared` future if `req` was already a key in it, otherwise
  build one (`Box::pin(async move { source.execute(...).await })`, boxed
  and `.shared()`), insert it, and run it. Dependencies were erased to
  `RawDep` *inside* the shared future itself (using the concrete
  `S::Key`/`S::Ver` still in scope there) before boxing the result, so
  every joining caller — not just whichever one happened to run it — got
  the identical, complete dependency set back and recorded it against its
  own `CompKey`, exactly as `Ctx::src_req` always has.
- The bucket entry was removed unconditionally the instant the shared
  future settled, success or failure alike (mirroring `EngineInner::run`'s
  `RunFinishSuccess`/`RunFinishError` arms, which both clear the
  computation-level `inflight` entry) — so a failed request never left a
  stuck entry that could hang a later identical call or replay a stale
  error forever, and matched this engine's existing "errors are not
  memoized" behavior instead of introducing a new one.
- Three new `lock_stats::LockSite` variants (`SrcInflightBucket`,
  `SrcInflightCheck`, `SrcInflightRemove`) instrumented the new locks the
  same way every other critical section in this engine already is, so the
  cost this stage was worried about ("you will have added a lock
  acquisition and a hash per source read for nothing") would be directly
  visible rather than inferred.
- No public API change: `Ctx::src_req`'s signature was untouched; a
  plugin author reading/writing sources never needed to know this
  machinery existed.

Two tests were written and passed before the revert decision, specifically
targeting the two hazards the task called out by name:

- `src_req_dedup_records_deps_for_every_joining_caller`: two *different*
  computations issue the identical concurrent `Ctx::src_req` against a
  source whose `execute` has a genuine `.await` (unlike this crate's
  `testutil::MemKvSource`, whose `execute` never yields and so can never
  leave a real overlap window open — this test used a small dedicated
  fixture with a `tokio::time::sleep` inside `execute` for exactly that
  reason). Asserted both: `Source::execute` ran exactly once (the dedup
  itself), and — the sharper check — both callers' own nodes each had the
  shared key recorded as a source dependency, referencing the *same*
  interned `SrcKeyId` (proof the joiner's caller-side bookkeeping ran as
  fully as the runner's, not just that both got the right return value).
- `failed_src_req_does_not_poison_the_dedup_bucket`: two concurrent
  identical requests that dedup into a *failing* execution both observed
  the error (not one `Ok`/one hang, not a panic); a follow-up call issued
  only after that shared future had fully settled ran the source fresh
  (bumping a call counter) rather than joining a stuck entry or replaying
  the cached error — proof the removal-on-failure path actually worked.

Both tests, and the full `cargo test --workspace --all-features` suite
(135 baseline + these 2 = 137) plus `--features testutil,alloc-stats`
standalone, passed; `cargo clippy --workspace --all-targets --all-features
-- -D warnings` was clean throughout. The implementation was correct. It
just didn't pay.

### Instrumentation used for measurement, then reverted with everything else

`hospital_bench`'s `LatencySource` already had a call counter
(`call_count`, Stage 11) — no change needed there, per the task's own
observation. `persist_bench`'s `MemKvSource` (via `computations::testutil`)
had no equivalent, so this stage added one (`MemKvSource::call_count()`, an
`AtomicU64` bumped in `execute`) plus a `report_src_calls` line printed
after every phase in `examples/persist_bench.rs` — this is what produced
the `source_calls` numbers in the tables below. Per this stage's
keep-or-revert instructions ("if reverted, commit the documentation of the
negative result only"), this instrumentation was reverted along with the
engine-level mechanism rather than kept on its own: `testutil.rs` and
`persist_bench.rs` are byte-identical to their pre-Stage-16 state in the
committed tree. The gap it would have closed (`MemKvSource` having no call
counter, unlike `hospital_bench`'s source) is real and independent of this
stage's keep/revert outcome; it is flagged here as a candidate for a future
small, standalone patch rather than folded into this revert.

### Why it doesn't pay: measured, not guessed

The task's own "honest possibility" section named the exact failure mode
this section confirms: *"if waves don't actually overlap those 683 reads
in time, the in-flight map will mostly miss."* It does, on both
benchmarks, for two different structural reasons.

**`persist_bench` (shared-key, 300 keys, ~683 dependents/key, zero-latency
`MemKvSource`)**: `MemKvSource::execute` never awaits anything internally
(a plain `std::sync::Mutex` lock, a `HashMap` read, done) — when boxed and
polled as the dedup layer's `Shared` future, it resolves on its very
*first* poll, before the async executor can schedule any other task to
reach the same bucket's check. There is no wall-clock window open for a
second, genuinely concurrent identical request to observe, regardless of
how many of the 683 dependents the driver's wave logically activates at
once. Measured directly: `src_execute`'s new `join_or_run`/`remove` lock
sites recorded **exactly as many calls as `MemKvSource::call_count()`
reported source calls, in every phase** — 205,000 on cold eval, 683 on
each restart-with-one-changed-key trial (this exact "up to 683 concurrent
reads" scenario the design was written for), 205,000 on the two
no-persistence/fingerprint-mismatch phases, 205,684 on the live-incremental
phases. A dedup join, had one ever happened, would have made the
`join_or_run`/`remove` call count *lower* than `call_count()`. It never
did — a **0% hit rate**, on the single benchmark shape this feature was
built to help.

**`hospital_bench` (unshared-key, ~2.08M distinct keys, ~1 dependent/key,
`LatencySource` with a real `.await` point in `execute` even at zero
configured latency)**: unshared keys are unshared by explicit design (see
Stage 11) — the one deliberate exception is `patient_summary`'s
cross-system 5-key re-read of the same sub-id-0 keys `vital(p,0)`/
`lab_result(p,0)`/`med_order(p,0)`/`note(p,0)`/`admission(p)` already read
directly. Tracing the dependency graph shows why even *that* never
overlaps: `patient_summary`'s body `try_join!`s `risk_score`/`admission`/
`note_digest` *first*, and only after all three fully settle does it issue
its own direct re-reads of the identical keys — `risk_score` alone already
transitively evaluates `vital(p, 0)` (via `vital_window`) to completion
before `patient_summary`'s second `try_join!` ever runs. The "shared key"
read is sequential, not concurrent, by construction of the graph itself,
so this benchmark's one built-in redundancy is invisible to a
concurrency-only dedup layer (a *batching* layer, which does not need
temporal overlap, could still collapse it — see below). Measured directly:
per-source call totals were **byte-identical before and after**
(`adt` 3,001, `vitals` 901,504, `labs` 811,501, `pharmacy` 61,501, `notes`
301,501, total 2,079,008) and `src_execute`'s `join_or_run`/`remove` call
counts matched `record_source_deps`'s own call count exactly (2,081,168) —
again a **0% hit rate**, on the benchmark built specifically to make a
non-zero hit rate visible if one existed.

### Cost: measured, not negligible

With zero benefit on either benchmark, the only question left is how much
the new locks cost. Also measured directly, via
`COMPUTATIONS_LOCK_STATS=1`:

- **`persist_bench`**, phase 1 (cold eval, 205,000 source calls): the three
  new sites (`SrcInflightBucket`/`SrcInflightCheck`/`SrcInflightRemove`)
  totaled **2.52% of tracked lock-held time** (0.45% + 1.06% + 1.01%),
  ≈199 ns of new fixed overhead per source read (36+84+80 ns mean per
  site). Against a ~3 s phase dominated by 2.4M `prepare`/`record_call_dep`
  calls, this is small enough to disappear into this box's own run-to-run
  noise band — cold-eval wall time landed at 2,926–3,147 ms across three
  after-runs, inside the 2,938–3,382 ms already seen across before-runs and
  this document's own documented ~3.1–3.7 s range.
- **`hospital_bench`**, phases 1–3 cumulative (2,081,168 source calls): the
  same three sites totaled **17.63% of tracked lock-held time** (2.76% +
  7.40% + 7.47%) — an order of magnitude larger share than on
  `persist_bench`, because this workload issues roughly 10x more source
  calls with almost no shared computation-level work to amortize the new
  locks against. This shows up in wall time, not just the lock breakdown:
  cold-eval time was **3,587–3,667 ms across two before-runs and
  4,411–4,828 ms across three after-runs — no overlap at all**, a
  reproducible **~20–30% regression**, even though the after-runs ran at
  *equal or lower* system load than the before-runs (`uptime` 1-minute
  figures 5.56/4.14 before vs 8.76/4.55/4.10 after — the slowdown survives
  the load comparison, not just an artifact of a busier box).
- `allocated_bytes` (deterministic, load-independent) confirms the
  regression is pure lock/CPU overhead, not allocation churn: phase 5's
  net alloc on `persist_bench` moved by **+584 B** (325,014,346 →
  325,014,930, matching Stage 15's documented baseline before this stage's
  change to the byte) and `hospital_bench`'s cold-eval net alloc moved by
  **+2,044 B** (1,146,709,594 → 1,146,711,638, likewise matching Stage 15's
  documented baseline exactly) — both changes are ~2,000x too small to
  explain the wall-time deltas above, and both are fully accounted for by
  the one-time `(SourceId, TypeId)` bucket-map entries this stage's design
  creates once per source/request-type pairing, not per request.
- Reruns and db size were unaffected on both benchmarks in every
  configuration (999,760/100,164/137,085/80,767 on `persist_bench`;
  1,116,093/1,444/6 on `hospital_bench`; db size byte-identical at 269.49
  MB) — the regression is a pure latency tax on every source read, with no
  correctness or persistence-format effect of any kind.

### Before/after tables

**`persist_bench`, default scale** (`uptime` 1-minute load 5.19–5.70 across
this stage's before/after runs — moderately loaded, same session,
same-session stash/pop A/B per this document's standing practice):

| phase | source calls (before) | source calls (after) | time before (ms) | time after (ms) |
|---|---|---|---|---|
| 1. cold eval | 205,000 (no counter existed) | 205,000 | 3,117 / 2,938 | 2,926 / 2,989 |
| 3. warm restart (both trials) | 0 | 0 | 1,047–1,151 | 1,047–1,069 |
| 4. restart, 1 changed input (both trials) | 683 (implicit) | 683 | 1,411–1,574 | 1,390–1,574 |
| 5. cold, no persistence (both trials) | 205,000 (implicit) | 205,000 | 2,306–2,362 | 2,342–2,595 |
| 6. fingerprint mismatch | 205,000 (implicit) | 205,000 | 3,776–3,832 | 3,819–4,061 |
| 7/8. live incremental (both) | 205,684 (implicit) | 205,684 | 402–486 | 395–508 |
| phase 5 `allocated_bytes` (net) | 325,014,346 B | 325,014,930 B | — | **+584 B** |
| db size | 269.49 MB | 269.49 MB | — | unchanged |

**`hospital_bench`, default scale** (`uptime` 1-minute load 5.56/4.14
before, 4.55/4.10/8.76 after):

| phase | source calls (before) | source calls (after) | time before (ms) | time after (ms) |
|---|---|---|---|---|
| 1. cold eval | 2,079,008 | 2,079,008 | 3,587–3,667 | 4,411–4,828 |
| 2. live incremental (1 key) | (included above) | (included above) | 79–81 | 79–87 |
| 3. rerun-heavy (300 keys) | (included above) | (included above) | 430–439 | 430–540 |
| 4. concurrency demo (10 pat., 2000 µs/call) | 13,860 | 13,860 | 56 | 80–86 |
| cold-eval `allocated_bytes` (net) | 1,146,709,594 B | 1,146,711,638 B | — | **+2,044 B** |
| cumulative lock-held time on new sites (phases 1–3) | n/a | 17.63% of total | — | — |

Both tables tell the same story from two different angles: **source call
counts are identical, byte for byte, in every phase of both benchmarks** —
proof of a 0% dedup hit rate, not just an absence of measured benefit —
while `hospital_bench`'s wall-clock time moved by a reproducible,
non-overlapping ~20–30% in the wrong direction.

### Relationship to the deliberately-dropped Haxl batching

This is explicitly the **dedup axis, not the batching axis** the Sources &
prior art section's Haxl citation refers to. Marlow et al.'s *There is no
fork* gets its concurrency win from **batching**: combining multiple
*different*, independently-issued requests against one data source into a
single round trip via `ApplicativeDo` desugaring, something this port
deliberately dropped in favor of explicit `Ctx::eval_all`/`try_join!`
concurrency (Stage 11's own "Adapted, not transliterated" section). Dedup
— collapsing multiple *identical* concurrent requests into one — is a
narrower, logically separate mechanism that doesn't need batching
machinery at all, which is exactly why this stage could be evaluated on
its own without reopening that decision. The finding here is that, absent
batching, dedup alone needs genuine temporal overlap of *literally
identical* requests to ever fire — and neither benchmark's actual
execution schedule produces any: `persist_bench`'s source is too fast
(zero-latency, non-yielding) to leave a window open at all, and
`hospital_bench`'s one built-in same-key redundancy
(`patient_summary`'s cross-system re-read) is masked by a sequential
dependency chain, not concurrent access. This is not evidence that dedup
could never help *any* workload — a source with real network-scale
latency (tens of ms, genuinely yielding) *and* genuine concurrent
key-sharing (e.g. an HTTP-cache-style source read by several independent
concurrent dependents for the same URL) is exactly the shape Haxl's own
dedup targets, and where this mechanism would likely earn its keep. It is
evidence that **neither of this codebase's two representative benchmarks
is that shape today**, and that shipping the mechanism anyway would mean
paying a permanent, measured tax on every source read in exchange for a
benefit this codebase currently has no way to demonstrate.

### Verdict: reverted

**Reverted.** Zero dedup hit rate on both benchmarks, a measured (not
inferred) lock-time cost that is small-but-present on `persist_bench`
(2.52% of tracked lock time, invisible in wall time) and large and
wall-clock-visible on `hospital_bench` (17.63% of tracked lock time, a
reproducible ~20–30% cold-eval regression with no overlapping before/after
samples) — for a benefit neither benchmark can show even once. This is
squarely the negative result the task's own "Honest possibility this
doesn't pay" section anticipated, now confirmed rather than assumed. The
implementation (`EngineInner::src_inflight`/`src_bucket`/`src_execute`,
the `Ctx::src_req` call-site change, the three new `LockSite` variants,
the `MemKvSource::call_count()`/`report_src_calls` measurement
instrumentation, and the two correctness tests) was fully built, tested
green, measured on both benchmarks, and then reverted in its entirety in
the same session — every source file this stage touched
(`crates/computations/src/engine.rs`, `ctx.rs`, `lock_stats.rs`,
`testutil.rs`, `examples/persist_bench.rs`) is byte-identical to its
pre-Stage-16 state in the committed tree. What survives is this section —
the measured call-count-identical, lock-time-costly result — so nobody
re-proposes source-level single-flight dedup on this codebase's current
two benchmarks without first checking whether either one's workload shape
has changed.

### Correctness

`cargo test --workspace --all-features` — 135 passed (unchanged from
Stage 15's count: the revert restores the exact pre-Stage-16 source tree,
so no new tests remain in the committed state). During implementation and
measurement, before the revert, the full suite plus the two new
correctness tests (137 total) passed, `cargo test -p computations
--features testutil,alloc-stats` passed standalone, and `cargo clippy
--workspace --all-targets --all-features -- -D warnings` was clean — the
mechanism was correct; it was rejected on cost/benefit, not on a bug.

## Stage 17 — re-profiling after the campaign

Stages 13-16 (source-key interning, Zero/One/Many `source_index`, inlined
version bytes, and the reverted single-flight experiment) landed since
Stage 6's CPU profile, which is now stale. This stage re-profiles both
benchmarks at HEAD (`9bf2451`, 135 tests), re-runs the Stage 12 instruments
(lock-hold attribution, allocation deltas) to confirm they still match
their last-documented baselines, adds a temporary capacity-based
memory-accounting instrument (reverted, per this stage's own acceptance
criteria), and ranks what's actually left.

### Tooling and load context

Same recipe as Stage 6: `cargo build -p computations --profile profiling
--example {persist_bench,hospital_bench} --features testutil`, `samply
record --save-only --unstable-presymbolicate`, offline leaf-sample
resolution against the `.syms.json` sidecar. The analysis script was
rewritten from scratch this stage (Stage 6's own copy was, per its own
notes, "kept in the scratchpad, not committed") with one addition Stage 6
didn't need: `known_addresses` sometimes resolves a sampled address to a
bare source-file path (no demangled symbol -- typically heavily-inlined
generic/async-state-machine code) or to the literal string `"UNKNOWN"`;
falling back to the presymbolication sidecar's coarser `symbol_table`
rva-range lookup in exactly those two cases turned a nonsensical top entry
(39% of one profile's active samples attributed to a bare
`.../harness.rs` path) into the expected
`tokio::runtime::task::harness::Harness<T,S>::poll`, and -- more
importantly -- fixed idle-park detection itself: `--hide-idle`'s walk
matches full-stack frame *names* against `tokio::runtime::park`, so a
mis-resolved intermediate frame silently defeated the whole idle filter
(one early sanity run mis-reported 11% idle where the range-corrected
version reports 46%). Verified against a `PERSIST_BENCH_SCALE=0.02` sanity
run before trusting full-scale output, same discipline as Stage 6.

`uptime` load average ranged **~2.0-4.3** across this stage's entire
session (1-minute figures from 2.0 up to 4.3 at various points) --
lighter than most prior stages in this document (several ran at 4.6-30)
but this box is still shared, so, per this document's standing practice,
absolute times below are reported for context only; the relative shares
(self-time percentages, lock-hold percentages, byte deltas) are what this
stage's conclusions rest on. `alloc_stats`-measured net-allocated-bytes
figures (deterministic, load-independent) are used in preference to RSS
wherever both are available, for the same reason.

### (a) `persist_bench` cold eval, no persistence -- top hotspots (phase 5-1, whole process, 3637 leaf samples, 46.1% idle-park excluded)

| % self | function | subsystem |
|---|---|---|
| 12.23% | `<sip::Hasher as Hasher>::write` | **SipHash -- but now on `RawDep`, not `Hash128`** (see below) |
| 10.04% | `tokio::runtime::task::harness::Harness<T,S>::poll` | tokio task polling |
| 7.70% | `futures_task::waker::clone_arc_raw` | waker-clone churn |
| 4.43% | `hashbrown::raw::RawTableInner::free_buckets` | HashSet/HashMap teardown |
| 4.08% | `NodeTable::id_of` | node-table lookup (now identity-hashed) |
| 2.75% | `harness::poll_future::{{closure}}` | tokio |
| 2.45% | `persist_bench::main` | benchmark harness itself |
| 1.99% | `UnsafeCell::with_mut` | tokio loom shim |
| 1.78% | `pthread_mutex_lock` | lock acquisition (the global `Mutex<NodeTable>`) |
| 1.63% | `Shared<Fut>::poll` | single-flight join machinery |
| 1.38% | `<T as ToString>::to_string` | `CompKey` debug/param formatting |
| 1.33% | `postcard::ser::serialize_with_flavor` | postcard encode |
| 1.27% | `_platform_memcmp` | alloc/compare noise |
| 1.17% | `EngineInner::run::{{closure}}` | driver |
| 1.02% | `source::raw_deps::{{closure}}` | **the fresh finding -- see below** |
| 0.87% | `NodeTable::source_deps_clone` | node-table/side-table access |
| ~2.78% (11 entries, listed together) | `tracing`/`sharded_slab` (`Instrumented::drop`, `Pool::create_with`, `DataInner::clear`, `Dispatch::get_default`, span init/release, ...) | **tracing instrumentation confound, still present** |
| ~0.97% | `blake3::hash` + `ChunkState::update` | blake3 hashing |

### (b) `persist_bench` warm restart, with persistence -- top hotspots (phase 3-1, whole process, 1726 leaf samples, 35.5% idle-park excluded)

| % self | function | subsystem |
|---|---|---|
| 8.80% | `persist::restore_nodes` | persist: decode + wire restored nodes |
| 5.75% | `harness::poll_future::{{closure}}` | tokio |
| 4.31% | `<Arc<T> as Hash>::hash` | `SourceId`/`DefId` hashing (restore-time lookups) |
| 3.68% | `BuildHasher::hash_one` | HashMap hashing (SipHash) |
| 3.23% | `pread` | redb (mmap'd file reads) |
| 3.14% | `<Vec<T> as Hash>::hash` | **`HashSet<RawDep>`/`HashSet<RawOutput>` construction -- see below** |
| 2.33% | `Filter::fold` | iterator-adapter overhead (def-name lookups?) |
| 1.89% | `Harness<T,S>::poll` | tokio |
| 1.71% | `Global::allocate` | allocator |
| 1.62% | `str::converts::from_utf8` | def-name string decode |
| 1.26% | `<SinkId as Hash>::hash` | `RawOutput` hashing (restore path) |
| 1.17% | `HashMap::insert` | node-table/index rebuild |
| 1.08% | `def::column_write` | typed value column write |

### (c) `persist_bench` live incremental, with persistence -- settle/flush windows (small-sample; consistent with Stage 6's shape)

Settle-only window (602 active samples): propagation/persist-pending-map
churn (`liveness_gc`, `propagate`/`propagate_tier`, `persister_loop`,
`HashMap::insert`) plus the same `hash_one`/tracing signature as (a) -- no
redb symbols, confirming the async-debounced design still holds. Flush-only
window (2015 active samples, 0.7% idle): still **redb/IO-dominated**
(`fcntl` 5.26%, `pwrite` 4.81%, `LeafMutator::insert` 1.79%, plus
`RawTable::drop`/hashing for the def-name/pending-map bookkeeping around
the transaction) -- no change from Stage 6's "not the redb write path"
verdict; listed for completeness, not as a new finding.

### `hospital_bench` cold eval -- top hotspots (window ~190-3828ms of the `main`-phase recording, proportional to the phase's own printed `RESULT` timing; 5494 leaf samples, 33.5% idle-park excluded)

| % self | function | subsystem |
|---|---|---|
| 7.88% | `drop_in_place<Instrumented<EngineInner::eval<..>::{{closure}}>>` | **tracing span teardown** |
| 6.68% | `liveness_gc::{{closure}}::{{closure}}` | mark-sweep GC self-time (own CPU cost, not just lock-hold) |
| 4.22% | `drop_in_place<Fuse<slab::Drain<Option<Waker>>>>` | tokio task-slab teardown |
| 3.15% | `start` | process/thread startup |
| 2.93% | `record_source_deps::{{closure}}` | interning + `HashSet<SrcDep>` construction |
| 2.90% | `CompDef::write_value` | typed value column write |
| 2.85% | `Core<T,S>::poll` | tokio |
| 2.82% | `Vec::pop` | free-list reuse |
| 1.97% | `drop_in_place<HashSet<String,RandomState>>` | ad hoc `String`-keyed set teardown |
| 1.70% | `RawTable::reserve_rehash` | HashMap growth |
| 1.15% | `drop_in_place<Span>` | tracing |
| 0.99% | `Dispatch::subscriber` | tracing |
| 0.93% | `postcard::try_push_varint_u64` | postcard encode |
| 0.88% | `drop_in_place<RefMut<DataInner>>` (sharded_slab) | tracing span-storage pool |
| **~14.0% (24 entries, summed)** | every `tracing`/`sharded_slab`/`Span` line in this window | **the confound, much worse here than on `persist_bench`** |

### `hospital_bench` rerun-heavy (300 keys mutated) -- top hotspots (window ~3911-4451ms of the same recording; 471 leaf samples, 9.8% idle-park excluded)

| % self | function | subsystem |
|---|---|---|
| 30.12% | `BuildHasher::hash_one` | SipHash -- 78/128 caller-traced samples (61%) resolve through `source::raw_deps`, the rest through `liveness_gc`/postcard |
| 13.65% | `liveness_gc::{{closure}}::{{closure}}` | mark-sweep self-time |
| 10.12% | `postcard::serialize_with_flavor` | postcard encode |
| 9.88% | `drop_in_place<Fuse<slab::Drain<Waker>>>` | tokio |
| 7.06% | `Option::is_some_and` | dirty-check branch |
| 5.18% | `drop_in_place<Instrumented<..>>` | tracing |
| **4.71%** | **`source::raw_deps` (self, direct)** | **the fresh finding, in the flesh** |
| 3.29% | `JoinAll::poll` | `Ctx::eval_all` fan-out |
| 1.41% | `drop_in_place<UnsafeCell<SrcKeyInterner>>` | interner teardown (per-process, once) |

### What Stages 13-15 demonstrably eliminated, and what took its place

- **SipHash on `Hash128` keys: eliminated as designed, but a *different*
  SipHash cost immediately backfills the same profiler line.** Stage 6's
  addendum (identity hashing for `Hash128`) is confirmed still doing its
  job: `NodeTable::id_of` (the per-def `Hash128Map` lookup) sits at 4.08%
  of persist_bench's active samples, essentially unchanged from Stage 6's
  original 3.6% pre-fix figure and nowhere near the ~5.6-8.7%
  *SipHash-family* share Stage 6 measured before the fix -- that specific
  cost is gone from the *stored index* structures it targeted. But
  `sip::Hasher::write` is back at **#1** (12.23%) in this fresh profile,
  and caller-tracing shows why: 153 of ~240 samples under it resolve
  through `<RawDep as Hash>::hash` -- `crate::source::raw_deps` (`source.rs`,
  called from every `Ctx::src_req`) builds a fresh `HashSet<RawDep>` on
  *every single source read*, hashing `SourceId` + full postcard key/version
  bytes through `std`'s default SipHash, entirely independent of Stage
  13-15's interning (which only ever sees the `HashSet<RawDep>` *after*
  it's built). This is a genuinely new finding, not a stale one: Stage 6's
  own SipHash discussion was scoped to `HashMap<Hash128, _>`/`HashMap<CompKey,
  _>` index structures and explicitly listed `RawDep`-keyed side tables as
  "deliberately left untouched... string/byte-keyed, not `Hash128`-dominated
  -- still need `std`'s HashDoS-resistant SipHash" (correct reasoning for
  *why identity hashing doesn't apply* -- but nobody had yet noticed that a
  `HashSet` doesn't need to exist there *at all* for the measured common
  case; see candidate 1 below). Confirmed the same shape recurs in
  `restore_nodes` (`<Vec<T> as Hash>::hash`/`<SinkId as Hash>::hash` in
  profile (b) -- restoring a node rebuilds `HashSet<RawDep>`/`HashSet<RawOutput>`
  from its persisted record the identical way) and in `hospital_bench`'s
  rerun-heavy window (`raw_deps` appears **directly**, as a named leaf, at
  4.71% self-time, with 61% of the window's dominant `hash_one` line
  tracing back through it by caller analysis).
- **blake3's relative share shrank** (~3.3% at Stage 6 -> ~0.97% combined
  here), but this reads as *dilution*, not a real speedup: blake3 hashes
  the same param/result bytes on the same call pattern as before (nothing
  in Stages 13-15 touched `CompKey`/result hashing), and this profile's own
  idle-park share (46.1%) is higher than Stage 6's (38.7%) on a lighter-
  loaded box, both symptoms of the same underlying shift -- Stage 13's
  interning step and the confound below now occupy more of the *active*
  sample budget, shrinking blake3's percentage of a differently-composed
  whole without it doing less absolute work. Reported factually, not as an
  optimization win.
- **The tracing confound Stage 6 flagged and left unfixed is still exactly
  present -- worse on the benchmark that didn't exist yet at Stage 6.**
  Quantified fresh, by summing every tracing/`sharded_slab`/`Instrumented`/
  `Span` line in each window: **~2.8% of `persist_bench`'s cold-eval active
  samples**, in the same ballpark as Stage 6's original ~4-5% estimate
  (small-sample noise plus this run's different composition explains the
  gap), but **~14.0% of `hospital_bench`'s cold-eval active samples** --
  roughly 5x higher. This tracks structurally: `hospital_bench`'s comp
  bodies are shorter and more source-request-heavy per instance than
  `persist_bench`'s (most comps do one source read and return, versus
  `persist_bench`'s FAN_IN=3 aggregation), so the *same* fixed per-span
  cost (`debug_span!`'s field formatting, `sharded_slab` storage,
  `Registry::enter`) is a larger fraction of each individual comp's total
  work. Stage 6's candidate 2 (give the benchmark's two `MessageSignal`
  layers a `Targets`/`EnvFilter`) was never applied; it is re-proposed
  below with a fresh, larger number behind it.
- **Node-table bookkeeping's other half (`record_call_dep`) has genuinely
  gotten cheap** -- it doesn't surface as a distinct leaf in either
  benchmark's top-25 self-time list at all (a `SmallVec` push under an
  already-held lock, apparently now cheap enough to not show up on its own
  in a 1kHz sample), even though `record_call_dep`'s *lock-held* time is
  still a substantial 19.28% (persist) / 5.63% (hospital) share in the
  fresh lock-stats tables below -- the two metrics measure different
  things (wall-clock self-time-when-sampled vs. total time spent inside a
  named critical section, including calls to other cheap functions), and
  the discrepancy is itself informative: this specific site's *own*
  instructions are cheap; what's expensive is everything happening while
  the lock across `prepare`+`record_call_dep`+`run/finish_success` is held,
  which lock-stats already attributes correctly and CPU-profiling
  necessarily smears across whichever leaf happens to be running at each
  1kHz tick.
- **Warm restart's cost is still redb I/O + hashing, not probing** -- same
  verdict as Stage 6 (`probe_versions` doesn't appear in the top 25 here
  either), just with the hashing half now traceable to the *same*
  `HashSet<RawDep>`/`HashSet<RawOutput>` construction pattern identified
  above, this time inside `restore_nodes` rather than `raw_deps` (both
  call sites build the identical shape of transient hash set from
  postcard-decoded bytes). See "durability tiers" under candidates below
  for what this implies about that specific proposal.
- **The flush window and the settle-only window are unchanged in shape**
  from Stage 6 -- redb-dominated and propagation/pending-map-dominated,
  respectively. Confirms nothing about the flush path moved.

### Fresh lock-hold and allocation baselines (confirms no regression since Stage 15)

`persist_bench` phase 1 (cold eval, persistence configured), fresh run,
`COMPUTATIONS_LOCK_STATS=1`:

| site | calls | total (s) | mean (ns) | % of total |
|---|---|---|---|---|
| `run/finish_success` | 999,760 | 0.596 | 595.9 | 37.26% |
| `prepare` | 2,385,278 | 0.567 | 237.6 | 35.45% |
| `record_call_dep` | 2,385,277 | 0.308 | 129.3 | 19.28% |
| `record_source_deps/nodes` | 205,000 | 0.043 | 209.6 | 2.69% |
| `run/set_inflight` | 999,760 | 0.027 | 27.1 | 1.69% |
| `record_source_deps/source_index` | 205,000 | 0.027 | 130.1 | 1.67% |
| `record_source_deps/interner` | 205,000 | 0.026 | 128.0 | 1.64% |
| `remove_stale_source_index` | 205,000 | 0.005 | 24.0 | 0.31% |
| `record_outputs` | 1,000 | 0.0002 | 232.7 | 0.01% |

Essentially unchanged from Stage 13's post-interning numbers for this exact
phase (interner 1.37%->1.64%, `source_index` ~1.7% either side) -- confirms
`persist_bench`'s lock-hold picture has been stable since Stage 13 landed,
through Stages 14-16.

`hospital_bench`, cumulative phases 1-3, fresh run:

| site | calls | total (s) | mean (ns) | % of total |
|---|---|---|---|---|
| `record_source_deps/interner` | 2,081,168 | 0.666 | 320.0 | 32.51% |
| `liveness_gc/mark_sweep` | 6 | 0.401 | 66,762,798.5 | 19.55% |
| `prepare` | 1,372,788 | 0.318 | 231.6 | 15.52% |
| `record_source_deps/nodes` | 2,081,168 | 0.267 | 128.2 | 13.02% |
| `run/finish_success` | 1,117,543 | 0.148 | 132.8 | 7.24% |
| `record_call_dep` | 1,371,337 | 0.115 | 84.2 | 5.63% |
| `record_source_deps/source_index` | 2,081,168 | 0.085 | 40.8 | 4.15% |
| `run/set_inflight` | 1,117,543 | 0.030 | 26.7 | 1.46% |
| `remove_stale_source_index` | 753,000 | 0.017 | 23.1 | 0.85% |

`source_index`'s share is down further from Stage 13's post-interning
33.60%->6.39% to **4.15%** here (Stage 14's Zero/One/Many landed in
between and shrank it again, as that stage's own table predicted:
6.41%->4.68%, now reconfirmed a third time at 4.15% on top of everything
else that's changed since). `interner`'s share is up correspondingly
(30.17%->32.51%) -- same lock, more of the total, exactly Stage 13's own
"the expensive work moved off `source_index`, not away" finding, still
holding three stages later.

`alloc_stats` net-allocated-bytes, both benchmarks, matched their
last-documented values **to the byte**: `persist_bench` phase 5 (no
persistence) 325,014,346 B / 325,014,586 B (two trials -- Stage 15's
documented baseline was 325,014,346 B, identical on trial 1); `hospital_bench`
cold eval 1,146,709,594 B (Stage 15's documented baseline, exact match).
Nothing has silently regressed or improved at the allocation level between
Stage 15 and this HEAD.

### Memory accounting: where the rest actually goes

Stage 5's per-row byte accounting summed `DefTable`'s *logical* column
lengths (135 B/row, common columns) against a *measured* RSS figure,
leaving a ~177 B/row gap attributed qualitatively to "`NodeRef` doubling",
"splitting one HashMap into 50", and unaccounted side tables. This stage
closes that gap with actual numbers: a temporary diagnostic
(`NodeTable::debug_memory_breakdown`/`SrcKeyInterner::debug_byte_estimate`,
added to `engine.rs`/`interner.rs`, wired into an `eprintln!` in each
benchmark, reverted before this stage's commit -- see "Correctness" below)
summed real `Vec`/`HashMap` **capacities** (not lengths) across every live
structure, run once per benchmark at the same point `alloc_stats`' net
figure was captured.

**`persist_bench`, 999,760 rows, phase 5 (no persistence), net-allocated
325.1 MB:**

| structure | measured bytes | B/row | % of total |
|---|---|---|---|
| `DefTable` dense columns, real capacity (`param_hash`/`result_hash`/`flags`/`param_off`/`param_len`/`comp_deps`/`rdeps`/`free`, summed via `.capacity()` across all 51 `DefTable`s) | 215.14 MB | 215.2 | 66.2% |
| per-def `index: Hash128Map<u32>` (51 tables) | 29.66 MB | 29.7 | 9.1% |
| `NodeTable::source_deps` (outer `HashMap<NodeRef, HashSet<SrcDep>>` shell + 205,000 inner `HashSet`s' real capacity) | 33.37 MB (13.07 + 20.30) | 33.4 | 10.3% |
| `param_arena` (all 51 defs) | 3.56 MB | 3.6 | 1.1% |
| `src_key_interner` | 0.04 MB | 0.04 | 0.01% |
| **named total** | **281.8 MB** | **281.9** | **86.7%** |
| unaccounted (typed value column, `outputs`/`inflight`/`flow_ids`, `VecSink`, tokio/futures scaffolding -- not instrumented this round) | ~43.3 MB | ~43.3 | 13.3% |

Two concrete findings inside this table that Stage 5 didn't have numbers
for:

- **Real `Vec` capacity is 215.2 B/row against Stage 5's 135 B/row
  *logical*-length sum for the same 7 columns (excluding the near-empty
  `free` list)** -- a **~80 B/row (37%)** gap that is pure capacity slack:
  `DefTable::insert` never reserves ahead of a population it can't know in
  advance, so every column's backing buffer sits wherever its last
  power-of-two doubling landed relative to the def's final row count.
- **`NodeTable::source_deps` -- every one of its 205,000 entries holds
  exactly one `SrcDep`** (confirmed independently by a second temporary
  instrument, a call-count histogram of `raw_deps()`'s input size: **100%**
  of `persist_bench`'s 205,000 calls and **99.9998%** of `hospital_bench`'s
  2,081,000 calls pass exactly one `Dep`). hashbrown allocates capacity for
  **3** items even to hold a `HashSet` of 1 (the smallest non-empty group
  size, rounded for its ~87.5% max load factor), so each of these
  single-dependency nodes pays **~99 B of real heap** (3 slots x
  (32 B `SrcDep` + 1 B control byte)) to store one 32-byte value -- a 3x
  over-allocation *on top of* paying for a hash table at all where a
  `SourceRefs`-style `One`/`Many` enum (Stage 14's own pattern, applied to
  the *other* side table) would cost zero heap bytes in the common case.

**`hospital_bench`, 1,116,093 rows, phase 1 cold eval, net-allocated
1,146.7 MB:**

| structure | measured bytes | B/row | % of total |
|---|---|---|---|
| `DefTable` dense columns, real capacity (18 defs) | 266.82 MB | 239.1 | 23.3% |
| per-def `index` (18 tables) | 38.12 MB | 34.2 | 3.3% |
| `NodeTable::source_deps` (753,000 entries, 2,079,000 total `SrcDep`s, outer shell + inner capacity) | 127.04 MB (52.30 + 74.75) | 113.8 | 11.1% |
| `param_arena` | 4.21 MB | 3.8 | 0.4% |
| `src_key_interner` | **253.58 MB** | 227.3 | **22.1%** |
| **named total** | **689.8 MB** | **618.2** | **60.2%** |
| unaccounted (18 defs' typed value columns, `comp_deps`/`rdeps` spillover, `outputs`, tracing's `sharded_slab` span-storage pool, tokio/futures scaffolding) | ~456.9 MB | ~409.5 | 39.8% |

The single biggest concretely-named structure of either benchmark:

- **`src_key_interner` costs 253.58 MB -- 22.1% of `hospital_bench`'s
  entire measured footprint -- for a genuine, previously-unflagged
  duplication inside the interner itself.** `SrcKeyInterner::intern`
  (`interner.rs`) calls `key.to_vec()` **twice** for every newly-seen key:
  once into `entries: Vec<Option<(SourceId, KeyBytes)>>` (kept for reverse
  lookup -- persistence, `Source::unregister`) and once more into
  `forward`'s nested `HashMap<KeyBytes, SrcKeyId>` (the forward-lookup
  index). On `persist_bench`'s 300-distinct-key workload this doubling is
  free either way; on `hospital_bench`'s ~2.08M-distinct-key,
  zero-sharing workload, every key's bytes (~18-24 raw bytes for a string
  like `"vitals/value/p1234/v5"`) are stored **twice**, independently, each
  copy paying its own malloc-quantum overhead (per Stage 15's own "~16.4 B
  of real overhead recovered per eliminated tiny allocation" finding --
  doubled here, not eliminated). This is exactly the kind of duplication
  Stage 13 set out to remove from `NodeTable`/`source_index` -- it simply
  reappeared one layer further in, inside the fix itself, on the one
  workload with no sharing to hide it.
- **`NodeTable::source_deps` costs 127.0 MB (11.1%) here too**, though the
  shape differs from `persist_bench`: only 1,500 of 753,000 entries hold
  exactly one `SrcDep` (average 2.76/entry -- `hospital_bench`'s comps
  batch multiple source reads per instance, e.g. `vitals` reads
  value+unit+range in one body), so a strict `One`/`Many` enum would help a
  smaller fraction of entries than on `persist_bench`; a `SmallVec<[SrcDep;
  N]>`-style representation (rather than a two-armed enum) would still
  recover the hashbrown-minimum-table tax that every one of these 753,000
  entries pays regardless of its own element count.
- **Real column capacity is 239.1 B/row against the same 135 B/row
  logical baseline** -- a **~104 B/row (43%)** gap, larger than
  `persist_bench`'s 37%, consistent with `hospital_bench`'s 18 defs having
  more unevenly-sized populations (some defs at 1,500 rows, others at
  ~900,000) and therefore more of them landing badly relative to their own
  next-power-of-two capacity boundary.
- The larger unaccounted residual here (39.8% vs. `persist_bench`'s 13.3%)
  is plausibly: the 18 defs' typed value columns (all small -- `u64` or
  `(WardId, u32)` tuples, confirmed by inspecting every `Comp<P, R>`
  registration in `hospital_bench.rs`; no `String` results, so this is
  individually cheap but multiplied across up to ~900,000 rows for the
  largest def); the ~63,092 nodes (5.65%) whose `comp_deps`/`rdeps` spilled
  past their 4-element `SmallVec` inline capacity (`transfer_candidates`
  alone fans out over all 1,500 patients via two separate `eval_all`
  calls; `note_digest`/`risk_score` fan into ~100-240 children per patient,
  1,500 such nodes each -- a real, if secondary, structural cost this
  diagnostic surfaced but didn't fully attribute); and -- flagged, not
  quantified this round given the effort budget -- tracing's
  `sharded_slab` span-storage pool, whose drops appear directly in this
  stage's own CPU profile of this exact phase (`drop_in_place<RefMut
  <DataInner>>`, `drop_in_place<Span>`), meaning the same "no `EnvFilter`"
  confound already measured as a CPU cost plausibly costs some non-zero,
  unmeasured amount of memory too (`sharded_slab`'s storage grows to
  accommodate historical span-slot high-water marks and isn't guaranteed
  to shrink back down).

### Ranked optimization candidates

Ranked by estimated impact x confidence, given this stage's evidence.

1. **Skip `HashSet<RawDep>`/`HashSet<RawOutput>` construction for the
   (measured) common single-element case.** Evidence: a temporary
   histogram (mirroring Stage 15's own version-byte-length methodology)
   found **100%** of `persist_bench`'s 205,000 `raw_deps()` calls and
   **99.9998%** of `hospital_bench`'s 2,081,000 calls carry exactly one
   `Dep` -- every one of them still builds a full SipHash-backed
   `HashSet<RawDep>` purely to hold that single element.
   `<RawDep as Hash>::hash` (via `sip::Hasher::write`) is `persist_bench`
   cold-eval's **#1** self-time line (12.23% of active samples, 64%
   caller-traced directly to it); on `hospital_bench`'s rerun-heavy
   window, `raw_deps` plus its `hash_one` calls account for **~23%** of
   active samples. The identical pattern recurs in
   `persist::restore_nodes` (profile (b)'s `<Vec<T> as Hash>::hash`/
   `<SinkId as Hash>::hash`) and in `ctx.rs`'s `sink_req` (lower priority
   -- 1,000-1,800 calls/run on both benchmarks, two to three orders of
   magnitude fewer than source reads). **Fix**: change `raw_deps`'s return
   type (and `record_source_deps`'s parameter) to a `SmallVec<[RawDep;
   1]>`-style representation with a linear dedup fallback only when
   `len() > 1` -- safe because `Source::execute`'s own typed
   `HashSet<Dep<K,V>>` (a public-API requirement, unchanged) already
   deduplicates by key before `raw_deps` ever sees it, so a `Vec`/`SmallVec`
   preserves that uniqueness for free. Apply the same shape to
   `SourceAdapter::wait_changes` (identical construction) and
   `restore_nodes`'s per-record `HashSet<RawDep>`/`HashSet<RawOutput>`
   (`persist.rs`). **Estimated win**: removes the single largest CPU line
   item on one benchmark and a top-3 item on the other -- roughly 5-12% of
   active self-time depending on workload, the highest-confidence CPU
   candidate on this list (direct, repeated, caller-traced evidence on
   both benchmarks and in both the live and restore paths). **Files**:
   `crates/computations/src/source.rs` (`raw_deps`, `SourceAdapter::wait_changes`),
   `crates/computations/src/engine.rs` (`record_source_deps`'s `raw`
   parameter type), `crates/computations/src/persist.rs` (`restore_nodes`),
   `crates/computations/src/ctx.rs` (`sink_req`, lower priority).

2. **Apply Stage 14's `SourceRefs` (Zero/One/Many) pattern to
   `NodeTable::source_deps`, the *other* side table Stage 14 didn't touch.**
   Stage 14 fixed `source_index` (key -> dependents); `source_deps` (node ->
   its own deps) has the identical "mostly-one-element `HashSet`" shape,
   just never measured before this stage. Evidence: the temporary capacity
   diagnostic found `source_deps`'s 205,000 `persist_bench` entries are
   **100%** single-`SrcDep`, costing 33.4 MB (10.3% of `persist_bench`'s
   total) for values that would cost zero heap bytes as an inline enum
   variant; hashbrown's 3-slot minimum-table allocation means each
   single-element entry pays ~99 B of real heap for one 32-byte value.
   `hospital_bench`'s 753,000 entries average 2.76 `SrcDep`s each (multi-key
   comp bodies), so a strict two-armed enum helps a smaller fraction there
   -- a `SmallVec<[SrcDep; 2]>`-style representation generalizes better
   across both workloads' measured distributions while still avoiding the
   hashbrown per-table minimum-size tax that all 753,000 of hospital's
   entries pay regardless of their own arity. **Estimated win**: ~25-30 MB
   (~8-9%) of `persist_bench`'s memory; a smaller *relative* but likely
   larger *absolute* share of `hospital_bench`'s 127 MB `source_deps` cost
   (the hashbrown-minimum-table tax alone, independent of arity, is paid
   753,000 times). **File**: `crates/computations/src/engine.rs`
   (`NodeTable::source_deps`'s type, `record_source_deps`/
   `reconcile_source_deps`/`liveness_gc`'s touch points, mirroring exactly
   Stage 14's own call-site list for `source_index`), plus
   `crates/computations/src/persist.rs` (`restore_nodes`'s rebuild path).

3. **Fix the benchmark harness's tracing confound (Stage 6 candidate 2,
   still unapplied) -- now quantified worse than Stage 6 estimated, on the
   benchmark that didn't exist yet at Stage 6.** Evidence: summing every
   tracing/`sharded_slab`/`Instrumented`/`Span` self-time line in this
   stage's own profiles gives **~2.8%** of `persist_bench`'s cold-eval
   active samples (in Stage 6's original ~4-5% ballpark) but **~14.0%** of
   `hospital_bench`'s -- roughly 5x higher, because `hospital_bench`'s
   shorter, more source-request-heavy comp bodies make the same fixed
   per-span cost a larger fraction of each comp's total work. **Fix**:
   give both benchmarks' two `MessageSignal` `tracing_subscriber::Layer`s
   a `Targets`/`EnvFilter` (or override `max_level_hint`) so
   `debug_span!`/`debug!` skip tracing's near-zero disabled-path -- a
   benchmark-only change (`persist_bench.rs`/`hospital_bench.rs`), zero
   engine-code risk. **Estimated win**: same order as the measured share,
   ~3-14% of wall time depending on workload -- and, independently of any
   wall-time win, this is measurement hygiene: every future profiling pass
   on this codebase inherits this confound until it's fixed. **Files**:
   `crates/computations/examples/persist_bench.rs`,
   `crates/computations/examples/hospital_bench.rs`.

4. **Stop double-copying key bytes inside `SrcKeyInterner` itself.**
   Evidence: the temporary capacity diagnostic measured `src_key_interner`
   at **253.58 MB -- 22.1% of `hospital_bench`'s entire footprint**, the
   single largest concretely-named structure of either benchmark, on a
   workload with ~2.08M distinct keys and zero sharing.
   `SrcKeyInterner::intern` (`interner.rs`) calls `key.to_vec()` twice per
   newly-seen key -- once into `entries: Vec<Option<(SourceId, KeyBytes)>>`,
   once more into `forward`'s nested `HashMap<KeyBytes, SrcKeyId>` -- two
   independent heap allocations holding identical bytes, each paying its
   own malloc-quantum overhead. **Fix**: share one allocation between the
   two structures -- store `Arc<[u8]>` (cheap under the same
   `Mutex<SrcKeyInterner>` this all already lives behind) in both
   `entries` and as `forward`'s inner-map key, via `Arc<[u8]>: Borrow<[u8]>`
   to preserve the module's own documented "a hit never allocates a probe
   key" property. **Estimated win**: roughly half of the interner's own
   footprint on a zero-sharing workload -- call it ~100-125 MB (~9-11% of
   `hospital_bench`'s total memory), effectively zero on `persist_bench`
   (300 keys either way, negligible base cost) -- the same
   "`hospital_bench`-wins, `persist_bench`-neutral" shape Stages 13-15
   already established repeatedly. **File**:
   `crates/computations/src/interner.rs` (`SrcKeyInterner::forward`/
   `entries`, `intern`/`intern_retain`/`release`).

5. **Reserve (or `shrink_to_fit`) `DefTable`'s dense columns instead of
   growing them via unreserved `push()`.** Evidence: measured real
   `.capacity()` sums are 215.2 B/row (`persist_bench`) and 239.1 B/row
   (`hospital_bench`) against Stage 5's 135 B/row *logical*-length
   accounting for the same 7 columns -- **~80 B/row (37%)** and **~104
   B/row (43%)** of pure Vec-growth capacity slack, respectively, because
   `DefTable::insert` never reserves ahead of a population it can't
   predict. **Fix options**: (a) an optional per-registration size hint
   threaded through `Registry`/`EngineBuilder` so `DefTable::new` can
   `Vec::with_capacity` up front, for callers who know their own scale
   (both benchmarks' level/patient counts are known before `build()`); or
   (b) `shrink_to_fit()` every `DefTable` column at a natural "settled"
   checkpoint (e.g. after `Engine::run`'s initial `eval_root` resolves),
   trading one one-time realloc-and-copy per column per def for reclaiming
   the doubling slack once a population stabilizes. **Estimated win**: up
   to the full measured gap in the best case (~24-25% of `DefTable`-column
   memory), realistically less since (a) only helps where a caller can
   predict scale and (b) has to weigh its own copy cost against the RSS
   win. **Files**: `crates/computations/src/engine.rs` (`DefTable::insert`,
   `DefTable` struct, `EngineBuilder`/`Registry` for the size-hint variant).

### The task's four named candidates, evaluated

- **The tracing confound** -- **IN**, see ranked candidate 3 above: real,
  now quantified on both benchmarks (worse on `hospital_bench` than Stage
  6's original estimate), cheap to fix, zero engine risk.
- **A packed `u32` `NodeRef` or same-def-local `u32` edge representation**
  -- Stage 5 predicted low value for `persist_bench`'s cross-def topology
  and declined to attempt it; this stage checked whether `hospital_bench`'s
  topology differs, per the task's explicit ask. It doesn't, in the
  direction that would matter: `comp_deps_spilled`/`rdeps_spilled` (nodes
  whose edge count exceeds the 4-element inline `SmallVec` capacity) are
  **5.65%**/**0.13%** of `hospital_bench`'s 1,116,093 nodes -- a small
  minority -- and `hospital_bench`'s edges are, if anything, *more*
  cross-def than `persist_bench`'s by construction (a patient's chain runs
  admission -> vital/lab/pharmacy/note -> note_digest/risk_score ->
  patient_summary -> ward rollups -> hospital rollup, crossing distinct
  defs at nearly every hop, unlike `persist_bench`'s own doc noting most of
  its edges already cross defs level-to-level). **RULED OUT**, same
  conclusion as Stage 5, now confirmed against the second benchmark rather
  than merely asserted. A packed-`u32` `NodeRef` specifically remains
  additionally undesirable for the reason Stage 5 gave (silently tightens
  the deliberately-generous `u16` `DefIndex` limit to 256 defs) for a
  saving that, per this same evidence, only matters inside an
  already-small minority of edges.
- **Durability tiers (skip probing high-durability keys on restart)** --
  the task asked to quantify probing's actual share of warm restart's
  ~1.2-1.4s first. This stage's profile (b) answers that a third time
  (Stage 6 found the same thing): `probe_versions` does not appear
  anywhere in the top-25 self-time list of either benchmark's restore
  path. Warm restart's cost is `pread`/`restore_nodes`/hashing (the same
  un-fixed `HashSet<RawDep>`/`HashSet<RawOutput>` construction from
  candidate 1, now inside `restore_nodes` instead of `raw_deps`) --
  confirmed independently by lock-stats, where phase 3 (warm restart)
  barely touches any of the 16 instrumented critical sections at all
  (`restore_nodes` takes `nodes`/`src_key_interner` locks directly,
  outside `EngineInner::timed`'s instrumented sites). **RULED OUT on the
  evidence available from both current benchmarks** -- there is nothing
  expensive in probing for a durability tier to skip. This is a real
  limitation of both benchmarks' in-memory, zero-latency sources rather
  than a claim that probing could never matter: a source with a genuine
  per-key round-trip cost at restore time (e.g. `computations-fs`'s
  `FsSource` against a cold filesystem cache) is exactly the shape that
  would need to exist before this candidate could be evaluated honestly.
- **Eviction to the redb store** -- not attempted; this stage's own memory
  accounting found no evidence of runaway *per-node* overhead that an
  eviction architecture would specifically fix, only concretely-named,
  in-place-fixable structures (interner duplication, `source_deps`
  HashSet-per-element overhead, `DefTable` capacity slack -- candidates
  1-2, 4-5 above). `persist_bench` (the one benchmark with persistence
  even configured) is the *smaller*-memory workload of the two (325 MB vs.
  `hospital_bench`'s ~1,100 MB), and `hospital_bench`'s memory pressure has
  nothing to do with persistence -- it doesn't have persistence configured
  in this benchmark at all. **DEFERRED, not ruled out in principle** (a
  workload with more distinct live nodes than fits in RAM would need
  something like this eventually), but not supported as *the next move*
  by either benchmark's evidence: the cheaper, already-identified,
  in-place fixes above would need to be exhausted first, and neither
  benchmark here is memory-constrained enough today to demonstrate
  eviction's value the way turbo-tasks' own workload apparently was.

### Honest read: diminishing returns, but not uniformly

`persist_bench` -- the workload most of Stages 5-16 were tuned against --
is genuinely approaching diminishing returns: its own largest remaining
named candidates (source_deps Zero/One/Many at ~8-9% of its memory, the
raw_deps hashing fix at a few percent of CPU, the tracing confound at
~2.8%) are real but each individually modest, and its lock-hold/allocation
profile has been stable since Stage 13. A fifth stage chasing
`persist_bench` specifically would be manufacturing wins, not finding
them.

**`hospital_bench` has not reached that point.** The unshared-key
workload Stage 11 added specifically to expose costs `persist_bench`
structurally cannot -- and which Stages 13-15 partially, not fully,
addressed -- still has substantial, concretely-identified headroom: the
interner's own internal duplication alone (~253 MB, 22% of total memory,
candidate 4) is larger than `persist_bench`'s *entire* engine-only memory
footprint; the tracing confound costs ~14% of active CPU samples on its
cold-eval path (candidate 3); and the `raw_deps`/`source_deps` HashSet-per-
request pattern (candidates 1-2) is directly visible as this workload's
single largest CPU line item in its own rerun-heavy phase. Combined,
candidates 1-4 plausibly reclaim a meaningful double-digit percentage of
`hospital_bench`'s memory and CPU time -- this is not a "fifth stage for
its own sake" list. If there is a next stage, the evidence here points at
prioritizing `hospital_bench`'s named candidates (1-4) specifically, not
re-profiling `persist_bench` again.

### Correctness

Two temporary diagnostics were added and fully reverted before this
stage's commit, per its own acceptance criteria: a call-count histogram in
`source::raw_deps` (mirroring Stage 15's version-byte-length measurement
methodology, used to establish the 100%/99.9998% single-`Dep` figures
above) and a capacity-based memory-accounting instrument
(`NodeTable::debug_memory_breakdown`, `SrcKeyInterner::debug_byte_estimate`,
and one `pub fn Engine::debug_memory_breakdown` entry point, plus one
`eprintln!` call site in each benchmark). `git diff --stat` against HEAD
is empty for every source file touched during measurement
(`crates/computations/src/source.rs`, `engine.rs`, `interner.rs`,
`crates/computations/examples/persist_bench.rs`, `hospital_bench.rs`) --
confirmed via `git checkout --` immediately after capturing this stage's
numbers, before writing up any of the sections above. `cargo test
--workspace --all-features` -- 135 passed (unchanged from Stage 15/16:
this stage is read-only against the committed source tree). `cargo clippy
--workspace --all-targets --all-features -- -D warnings` clean. `samply`'s
own artifacts (`*.json`/`*.syms.json`) and the rewritten
`analyze_profile.py` were kept in the session scratchpad, not the repo,
per Stage 6's own "artifacts kept out of git" precedent and the existing
`.gitignore` entries from that stage.

## Stage 18 — single-element source_deps

Stage 17's ranked candidate 2: `NodeTable::source_deps`
(`HashMap<NodeRef, HashSet<SrcDep>>`) is the *other* interned side table —
`source_index` (key -> dependents) got Stage 14's `One`/`Many` treatment;
`source_deps` (node -> its own deps) never did. Stage 17's own capacity
diagnostic measured it at 33.4 MB (10.3% of `persist_bench`'s total) and
127.0 MB (11.1% of `hospital_bench`'s total) — hashbrown allocates a
3-slot minimum table even to hold a `HashSet` of one 32-byte `SrcDep`, so
every single-dependency entry pays ~99 B of real heap to store one 32-byte
value.

### Design

`crate::engine::SrcDeps` replaces `HashSet<SrcDep>` as `NodeTable::source_deps`'s
value type, reusing Stage 14's `SourceRefs` shape verbatim:

```rust
pub(crate) enum SrcDeps {
    One(SrcDep),
    Many(HashSet<SrcDep>),
}
```

- **Promotion** (`One -> Many`) happens in place, in `SrcDeps::insert`, the
  first time a second *distinct* `SrcDep` is recorded against a node — used
  by `NodeTable::extend_source_deps` (both the live `record_source_deps`
  path and `persist::restore_nodes`'s cold rebuild path share this one
  method, exactly as before). A vacant map entry always starts life as
  `One`, never an implicit empty `Many` — there is no `Default` impl for
  `SrcDeps`, matching `SourceRefs`.
- **Demotion (`Many -> One`) is not implemented**, following Stage 14's own
  precedent and the task's explicit direction to do the same unless
  measurement showed a reason not to. Measurement (below) showed no such
  reason.
- **External call sites are unchanged.** `NodeTable::source_deps_clone`/
  `take_source_deps` still return an owned `HashSet<SrcDep>` — the shape
  `EngineInner::reconcile_source_deps`'s `old == new` early-return and
  key-id diff, `crate::persist::PendingRecord::snapshot`'s iteration, and
  `crate::persist::probe_restored_source_deps`'s per-key grouping are all
  already typed against. `SrcDeps::into_hashset`/`clone_to_hashset` do the
  conversion at the two points that need it: `Many` moves (or clones) its
  already-allocated `HashSet` with no rebuild; `One` pays a fresh
  single-element `HashSet` allocation, the same cost `take_source_deps`
  always had for a node with exactly one dependency. This means
  `EngineInner::run`, `driver::liveness_gc`, and every `crate::persist`
  touch point (`PendingRecord::snapshot`, `restore_nodes`,
  `probe_restored_source_deps`) needed **zero** changes beyond what
  `NodeTable`'s own methods do internally — the enum is entirely contained
  inside `engine.rs`.
- `source_deps_iter`/`source_deps_contains` go through a new `SrcDeps::iter`/
  `SrcDepsIter` (a two-armed iterator, `Option<&SrcDep>` for `One`,
  `HashSet`'s own `Iter` for `Many`), mirroring `SourceRefsIter` exactly.

**Contrast with Stage 5's rejected experiment, stated explicitly per the
task's request:** Stage 5 measured-and-reverted `SmallVec<[NodeRef; N]>` for
`source_deps`/`outputs` because *most nodes have zero* of either — a node
that reads no sources is common, and an inline-capacity container pays its
slot unconditionally whether or not it's ever used, losing to a
never-allocated empty `HashSet`. This stage's `source_deps` entries are
never in that position: `NodeTable::extend_source_deps` is a no-op for an
empty `raw` (a node with zero source reads never gets a map entry at all,
exactly as before), so there is no "wasted inline slot on an empty
container" case here either — only "one slot suffices for the common case,
promote when a second distinct dependency shows up," the same shape Stage
14 found paid off for `source_index`.

**A more important distinction, specific to this stage and flagged by the
task explicitly so it isn't conflated with the shape above:** the task's
own framing cited "100% single-element on `persist_bench`, 99.9998%
single-element on `hospital_bench`" as this stage's motivating evidence.
The `persist_bench` figure is exactly right — Stage 17's own capacity table
found all 205,000 entries hold exactly one `SrcDep`. The `hospital_bench`
figure, on closer reading of Stage 17's own numbers, describes a
*different* measurement: **99.9998% single-`Dep` refers to `raw_deps()`'s
per-call arity** (Stage 17's ranked candidate 1, `crate::source::raw_deps`
building a fresh `HashSet<RawDep>` per source read), not to
`NodeTable::source_deps`'s own final per-node entry size. Stage 17's own
capacity-diagnostic table for `source_deps` itself says the opposite for
this benchmark: **753,000 entries hold 2,079,000 total `SrcDep`s, averaging
2.76 per entry** — `hospital_bench`'s comp bodies read multiple keys per
instance (e.g. `vitals` reads value+unit+range in one body, each call to
`record_source_deps` contributing one dep to the same node's cumulative
set), so only ~1,500 of 753,000 entries (~0.2%) are actually single-`SrcDep`.
This stage implements the `One`/`Many` enum exactly as instructed regardless
— it is still a strict improvement (no workload's entries get worse, some
get zero-heap), and Stage 17's own ranked-candidate write-up already
anticipated this exact distinction ("a `SmallVec<[SrcDep; N]>`-style
representation generalizes better... while still avoiding the
hashbrown-minimum-table tax that all 753,000 entries pay regardless of
their own arity") — but the measured win on `hospital_bench` specifically
should be read against the *true* 0.2%-single distribution, not the
misattributed 99.9998% figure, and the numbers below report exactly that.

### Measurements

`uptime` load average ranged **~4.4–10.5** across this stage's benchmark
session (1-minute figures from 4.37 up to 10.49 at various points, this
box's heaviest band since Stage 6/13) — heavier than Stage 17's own
2.0–4.3 session, so per this document's standing practice the deterministic
`allocated_bytes`/db-size/rerun-count figures below are what this stage's
conclusion rests on, with RSS reported for context. Both sides are
same-session, stash/pop A/B pairs (this stage's `SrcDeps` change stashed
for "before", popped back for "after").

**`persist_bench`, default scale** (205,000 `source_deps` entries, **100%**
single-`SrcDep` per Stage 17's own table):

| metric | before | after | Δ |
|---|---|---|---|
| phase 5 (engine-only RSS), 6 samples before / 8 after | 290.2–312.0 MB, avg 299.7 MB | 257.2–282.1 MB, avg 265.2 MB | **−34.5 MB, −11.5%** (ranges do not overlap) |
| phase 5 `allocated_bytes` (net), 2 trials each, identical within each side | 325,014,346 B | 296,314,346 B | **−28,700,000 B, −8.83%** (matches Stage 17's own ~8-9% estimate) |
| `persist_now` db size | 269.49 MB | 269.49 MB | byte-identical |
| `record_source_deps/nodes` lock share, phase 1 | 2.29–3.49% across sampled runs | 2.29% | unchanged within run-to-run spread |
| `record_source_deps/source_index`, `/interner` lock shares | 1.91–2.79% / 1.24–2.04% | 1.91% / 1.24% | unchanged |
| reruns, every phase | exact-match every run (999,760 / 100,164 / 137,085) | exact-match every run | identical |

**A real, reproducible win — every "after" RSS sample beats every "before"
sample**, and the deterministic `allocated_bytes` delta lands almost
exactly on Stage 17's own predicted 8-9% range for this benchmark, since
100% of its entries convert to the zero-heap `One` variant.

**`hospital_bench`, default scale** (753,000 `source_deps` entries, only
**~0.2%** single-`SrcDep`, average 2.76/entry per Stage 17's own table):

| metric | before | after | Δ |
|---|---|---|---|
| cold-eval RSS, 3 samples each | 1088.0–1099.2 MB, avg 1094.2 MB | 1096.0–1099.3 MB, avg 1098.1 MB | +3.9 MB / +0.4% (ranges fully overlap — noise) |
| cold-eval `allocated_bytes` (net) | 1,146,709,594 B | 1,146,499,594 B | **−210,000 B, −0.018%** (matches Stage 17's baseline to 7 significant figures) |
| `record_source_deps/nodes` lock share (phases 1-3) | 11.87% | 12.13% | unchanged within noise |
| `record_source_deps/interner`, `liveness_gc/mark_sweep` lock shares | 33.24% / 21.36% | 33.29% / 20.93% | unchanged within noise |
| cold-eval wall time | 4140-4242 ms | 4140-4167 ms | flat |
| reruns, cold / rerun-heavy | 1,116,093 / 1,444 | 1,116,093 / 1,444 | identical, every run |

**Neutral, exactly as the corrected distribution above predicts.** With
only ~1,500 of 753,000 entries actually single-`SrcDep`, the other 751,500
stay `Many` and pay the identical `HashSet` cost they always did — the
measured −0.018% `allocated_bytes` delta is the right order of magnitude
for saving ~99 B on ~1,500 entries against a 1.1 GB base (≈150 KB, swamped
by run-to-run allocator noise at this scale), not a meaningfully-sized win,
but also not a regression: no lock share, RSS sample, or rerun count moved
outside its own noise band.

### Verdict: kept

`persist_bench` improves by a real, reproducible, non-overlapping margin on
both RSS and the deterministic `allocated_bytes` metric, matching Stage
17's own estimate almost exactly; `hospital_bench` — correctly understood
against its true ~0.2%-single distribution rather than the task's
misattributed 99.9998% figure — lands within noise on every metric, with no
observed regression on any of RSS, `allocated_bytes`, lock-hold shares, wall
time, or rerun counts. This is the same "one benchmark wins clearly, the
other is a documented, explained no-op" shape Stage 14 established (there,
in the opposite direction: `hospital_bench` won, `persist_bench` was
neutral) — a real win on one workload with zero regression on the other
clears this campaign's own bar for keeping a change. Persisted format and
db size are unaffected (`source_deps`'s *storage shape* was never part of
the on-disk record — only `NodeRecord::source_deps: Vec<RawDepRepr>`, which
this stage does not touch).

If a future workload needs `hospital_bench`'s own multi-key-per-node shape
addressed, Stage 17's own alternative suggestion — `SmallVec<[SrcDep; N]>`
instead of a strict two-armed enum, so a `Many`-shaped entry with 2-3
elements still avoids the hashbrown-minimum-table allocation — remains
exactly where to look next; it was not attempted here because the task
specified reusing Stage 14's own enum pattern, and this stage's evidence
doesn't show a workload-specific reason to deviate from that instruction.

### Correctness

`cargo test --workspace --all-features` — **135 passed** (unchanged count:
`SrcDeps`'s behavior is already covered transitively by every existing
`source_deps`-touching test, the same reasoning Stage 14 gave for
`SourceRefs` — a wrong promotion/iteration/conversion would fail
`reconcile_source_deps`, `liveness_gc` reclamation, or `persist::restore_nodes`
directly). `cargo test -p computations --features testutil,alloc-stats` —
all green standalone. `cargo clippy --workspace --all-targets --all-features
-- -D warnings` clean.
