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

## Not tried yet / open items

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

