//! The incremental engine: the dynamic dependency graph and change
//! propagation.
//!
//! [`Engine`] owns the def table (registered [`CompDef`]s, looked up by
//! [`DefId`]) and the node table: one row per evaluated [`CompKey`] (i.e.
//! per computation application), addressed by [`NodeRef`] and stored in a
//! per-definition, struct-of-arrays [`DefTable`] rather than one
//! heterogeneous slab — see [`NodeRef`]'s and [`DefTable`]'s docs for the
//! Tier-2 columnar layout this module implements. Evaluating a computation
//! walks the same algorithm whether it is a root call
//! ([`Engine::eval_root`]) or a nested call ([`crate::ctx::Ctx::eval`]):
//!
//! 1. A clean, cached node is returned immediately (memoization) — a typed
//!    clone straight out of [`crate::def::CompDef::values`], never an
//!    erased downcast.
//! 2. A node already being computed is joined via its shared, cloneable
//!    in-flight future (single-flight dedup) rather than recomputed.
//! 3. Otherwise the computation actually runs: its dependencies are
//!    collected fresh (recorded by the [`Ctx`] it is given), its result is
//!    content-hashed for early cutoff, and any sink outputs it stopped
//!    producing (relative to its previous run) are deleted.
//!
//! Change propagation itself (deciding *which* dirty nodes to re-run, and
//! in what order) lives in [`crate::driver`]; this module only provides the
//! primitive it drives: re-evaluating one node — see
//! [`EngineInner::rerun_node`], which decodes `(def, param_bytes)` and
//! re-runs the computation on demand rather than through any closure a node
//! keeps around.

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::future::{BoxFuture, FutureExt, Shared};
use smallvec::SmallVec;
use tokio::sync::{Mutex as AsyncMutex, mpsc};
use tracing::Instrument;

use crate::ctx::Ctx;
use crate::def::{
    Comp, CompDef, DefAdapter, ErasedDef, define_comp, define_comp_rec, define_comp_rec_with, define_comp_with,
};
use crate::error::CompError;
use crate::key::{CompKey, CompKeySet, CompParam, CompResult, DefId, Hash128, Hash128Map};
use crate::persist::{PersistHandle, PersistOptions};
use crate::registry::Registry;
use crate::sink::{OutBytes, RawOutput, SinkBase, SinkId};
use crate::source::{KeyBytes, RawDep, SourceBase, SourceId};

/// The result of one execution: the (erased) value, its content hash, and
/// how long the body itself took to run (for the `comp.eval` tracing
/// event). The postcard-encoded bytes needed to compute the hash are a
/// local of the execution future itself (see `EngineInner::run`) — never
/// carried any further, since neither `Node` nor this result type keeps a
/// serialized copy around; `crate::persist` re-serializes lazily, from the
/// erased value, only for a node that is actually about to be persisted
/// (see `crate::def::ErasedDef::serialize_value`).
type ExecResult = Result<(Arc<dyn Any + Send + Sync>, Hash128, Duration), CompError>;
/// The shared, joinable handle to an in-flight execution.
type SharedExec = Shared<BoxFuture<'static, ExecResult>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NodeState {
    /// Has a cached value that is up to date with its dependencies.
    Clean,
    /// Needs re-execution before its value can be trusted.
    Dirty,
    /// Currently executing (or joining an execution); see `inflight`.
    Running,
}

/// Priority of a node's pending dirty work.
///
/// [`DirtyPriority::Input`] marks a node dirtied because a genuine source
/// input changed. [`DirtyPriority::Revalidate`] marks a node dirtied for a
/// background reason that doesn't necessarily reflect a real input change —
/// e.g. (future work) a node restored from a persisted graph whose defining
/// code may have changed since it was last computed, which needs checking
/// but shouldn't hold up genuinely changed inputs.
///
/// [`crate::driver`] always drains all `Input`-tier dirty work to a
/// fixpoint before starting any `Revalidate`-tier work, and preempts
/// in-progress `Revalidate` work (between waves, never mid-wave) whenever
/// new `Input`-tier work arrives.
///
/// When a node that already has pending dirty work is marked dirty again,
/// the higher of the two priorities wins ("max priority wins", `Ord`
/// order): an `Input` mark is never downgraded back to `Revalidate` by a
/// later `Revalidate` mark, but a `Revalidate` mark is promoted to `Input`
/// by a later `Input` mark.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum DirtyPriority {
    Revalidate = 1,
    Input = 2,
}

/// A compact index into the engine's def table (see [`NodeTable`]),
/// assigned in registration order by [`EngineBuilder::build`].
///
/// This exists purely to shrink [`NodeRef`]'s identity down from a full
/// [`CompKey`] (32 bytes: a `DefId` is a 16-byte fat pointer) to 2 bytes. It
/// never replaces `CompKey`/`DefId` anywhere in the public API or on disk;
/// it is strictly an internal detail of how [`NodeTable`] indexes its
/// per-definition tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct DefIndex(u16);

/// The identity of one row in [`NodeTable`]'s columnar, per-definition
/// storage (see [`DefTable`]) — the Tier-2 replacement for Tier 1's
/// `NodeId` (a single global slab index).
///
/// Like the `NodeId` it replaces, a `NodeRef` is only a cheap (8-byte)
/// local handle into the *current* process's in-memory node table: it is
/// never persisted, never compared across restarts, and is only valid
/// until the node it names is collected by [`crate::driver`]'s liveness GC
/// — at which point its row may be reused for a completely unrelated node
/// of the *same* definition. Every long-lived reference to a computation (a
/// persisted record, `roots`) is keyed by `CompKey` instead; a `NodeRef` is
/// translated back to one, in O(1), via [`NodeTable::key_of`] — which needs
/// no per-node storage at all, since a `NodeRef` already carries both
/// halves of a `CompKey` (the definition, via `def`, and the parameter
/// hash, read from [`DefTable::param_hash`] at `row`) implicitly. This is
/// what lets Tier 2 drop the full `CompKey` Tier 1's `Node` kept purely so
/// `NodeId -> CompKey` translation had somewhere to read from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct NodeRef {
    pub(crate) def: DefIndex,
    pub(crate) row: u32,
}

/// The small set of mutually-exclusive/boolean bits packed into one byte
/// per row: `state` (2 bits, 3 values), `dirty_priority` (2 bits, `None`
/// plus 2 values), `last_changed` (1 bit), `has_result` (1 bit — Tier 2's
/// addition, see below), and `free` (1 bit — Tier 2's addition, see below).
/// 7 bits total, comfortably inside a `u8`.
///
/// `has_result` replaces Tier 1's `Node::value: Option<_>` / `result_hash:
/// Option<Hash128>` — a columnar `DefTable` stores dense
/// `Vec<Hash128>`/value columns (see `crate::def::CompDef::values`) with no
/// room for a per-row `Option` discriminant of their own, so "has this row
/// ever completed a successful run" has to live here instead, gating every
/// read of those columns.
///
/// `free` replaces Tier 1's `Vec<Option<Node>>` slab's own `None` sentinel:
/// a columnar `DefTable`'s per-column `Vec`s can't represent "this row
/// doesn't exist" by absence (every column must stay the same length), so a
/// freed row is instead marked here and skipped by every iteration method
/// ([`NodeTable::iter_refs`], [`DefTable::retain_rdeps`]) — its other
/// columns' bytes are left as stale garbage until the row is reused (see
/// [`DefTable::insert`]), exactly as `crate::def::CompDef`'s value column
/// and [`DefTable`]'s param arena also tolerate GC garbage between reuses.
#[derive(Debug, Clone, Copy, Default)]
struct NodeFlags(u8);

const STATE_MASK: u8 = 0b0000_0011;
const STATE_CLEAN: u8 = 0;
const STATE_DIRTY: u8 = 1;
const STATE_RUNNING: u8 = 2;

const PRIORITY_MASK: u8 = 0b0000_1100;
const PRIORITY_SHIFT: u8 = 2;
const PRIORITY_NONE: u8 = 0;
const PRIORITY_REVALIDATE: u8 = 1;
const PRIORITY_INPUT: u8 = 2;

const LAST_CHANGED_BIT: u8 = 0b0001_0000;
const HAS_RESULT_BIT: u8 = 0b0010_0000;
const FREE_BIT: u8 = 0b0100_0000;

impl NodeFlags {
    fn new(state: NodeState) -> Self {
        let mut flags = NodeFlags::default();
        flags.set_state(state);
        flags
    }

    fn state(self) -> NodeState {
        match self.0 & STATE_MASK {
            STATE_CLEAN => NodeState::Clean,
            STATE_DIRTY => NodeState::Dirty,
            STATE_RUNNING => NodeState::Running,
            other => unreachable!("NodeFlags: invalid packed state bits {other}"),
        }
    }

    fn set_state(&mut self, state: NodeState) {
        let bits = match state {
            NodeState::Clean => STATE_CLEAN,
            NodeState::Dirty => STATE_DIRTY,
            NodeState::Running => STATE_RUNNING,
        };
        self.0 = (self.0 & !STATE_MASK) | bits;
    }

    fn dirty_priority(self) -> Option<DirtyPriority> {
        match (self.0 & PRIORITY_MASK) >> PRIORITY_SHIFT {
            PRIORITY_NONE => None,
            PRIORITY_REVALIDATE => Some(DirtyPriority::Revalidate),
            PRIORITY_INPUT => Some(DirtyPriority::Input),
            other => unreachable!("NodeFlags: invalid packed dirty_priority bits {other}"),
        }
    }

    fn set_dirty_priority(&mut self, priority: Option<DirtyPriority>) {
        let bits = match priority {
            None => PRIORITY_NONE,
            Some(DirtyPriority::Revalidate) => PRIORITY_REVALIDATE,
            Some(DirtyPriority::Input) => PRIORITY_INPUT,
        };
        self.0 = (self.0 & !PRIORITY_MASK) | (bits << PRIORITY_SHIFT);
    }

    fn last_changed(self) -> bool {
        self.0 & LAST_CHANGED_BIT != 0
    }

    fn set_last_changed(&mut self, changed: bool) {
        if changed {
            self.0 |= LAST_CHANGED_BIT;
        } else {
            self.0 &= !LAST_CHANGED_BIT;
        }
    }

    fn has_result(self) -> bool {
        self.0 & HAS_RESULT_BIT != 0
    }

    fn set_has_result(&mut self, has_result: bool) {
        if has_result {
            self.0 |= HAS_RESULT_BIT;
        } else {
            self.0 &= !HAS_RESULT_BIT;
        }
    }

    fn is_free(self) -> bool {
        self.0 & FREE_BIT != 0
    }

    fn set_free(&mut self, free: bool) {
        if free {
            self.0 |= FREE_BIT;
        } else {
            self.0 &= !FREE_BIT;
        }
    }
}

/// One definition's columnar row storage: every `crate::engine`-generic
/// (non-typed) column for every computation application (row) of this
/// single definition, addressed by the row half of a [`NodeRef`].
///
/// This is the Tier-2 replacement for Tier 1's `Node` struct-of-node slab:
/// instead of one heterogeneous `Vec<Option<Node>>` shared by every
/// definition (an `Arc<dyn Any + Send + Sync>` value, a `CompKey`, and every
/// other field repeated per node regardless of def), each definition gets
/// its own struct-of-arrays, one allocation per column, so a `u8` flags byte
/// costs a `u8` per row rather than sharing a cache line with fields that
/// happen to belong to a completely different definition's nodes.
///
/// Two columns deliberately do *not* live here:
/// - `crate::def::CompDef::values` (the typed `R` result column) lives on
///   the definition's own `CompDef<P, R>` instead, since `DefTable` must
///   stay generic over every definition's `R` at once — see that field's
///   docs for why, and for the object-safe `ErasedDef` methods GC/persist
///   use to touch it without ever naming `R`.
/// - `source_deps`/`outputs`/`inflight` stay on [`NodeTable`] as sparse
///   side maps (unchanged from Tier 1): most nodes have none of the first
///   two, and essentially no node is ever `inflight` at rest, so a dense
///   per-row column for any of the three would mostly store nothing.
///
/// Row reuse (`Self::insert`, after `Self::remove` frees a row into
/// `free`) resets every column that could otherwise leak a stale value
/// across two unrelated `CompKey`s occupying the same row over time —
/// `comp_deps`/`rdeps`/`param_off`/`param_len`/`param_hash` — **except**
/// `result_hash`, whose staleness is instead gated by `flags.has_result()`
/// (cleared on every fresh/reused row), and the `param_arena` bytes a freed
/// row's slice pointed into, which are simply abandoned (never reclaimed;
/// see `Self::param_arena`'s docs) — both deliberate, documented garbage
/// tolerances rather than oversights, mirroring `crate::def::CompDef`'s own
/// value-column garbage tolerance.
#[derive(Default)]
struct DefTable {
    /// Row `i`'s parameter content hash — together with this table's
    /// `DefIndex` (implicit: `DefTable`s are indexed by `DefIndex` in
    /// [`NodeTable::defs`]), the two halves of the `CompKey` a `NodeRef{def,
    /// row: i}` names. This is also what lets [`NodeTable::key_of`]
    /// reconstruct a full `CompKey` from a bare `NodeRef` with no
    /// additional storage — see [`NodeRef`]'s docs.
    param_hash: Vec<Hash128>,
    /// Row `i`'s last successful result's content hash, valid only when
    /// `flags[i].has_result()` (see [`NodeFlags`]'s docs on why a dense
    /// column can't carry its own `Option` discriminant).
    result_hash: Vec<Hash128>,
    flags: Vec<NodeFlags>,
    /// Row `i`'s parameter bytes are `param_arena[param_off[i] ..
    /// param_off[i] + param_len[i] as usize]` — see `Self::param_arena`.
    param_off: Vec<u32>,
    param_len: Vec<u16>,
    /// An append-only byte arena backing every row's postcard-encoded
    /// parameter, replacing Tier 1's one-`Vec<u8>`-allocation-per-node with
    /// one allocation per definition. Parameters are small in practice
    /// (a single hashable lookup key, per `crate::engine::Node`'s original
    /// Tier-1 docs), so the append-only cost (never reclaiming a freed
    /// row's span) is a deliberate simplicity/memory tradeoff: `Self::remove`
    /// does not shrink or compact this arena, and nothing here ever will —
    /// a def whose instance population churns heavily under GC accumulates
    /// unreachable arena bytes for the lifetime of the process. This is
    /// judged acceptable for the same reason Tier 1's docs judged an
    /// always-allocated-even-when-empty per-node `Vec<u8>` unacceptable: the
    /// steady-state population (not the GC churn) dominates real workloads,
    /// and an arena is strictly smaller than one heap allocation per row
    /// even before any garbage is accounted for.
    param_arena: Vec<u8>,
    /// Row `i`'s outgoing call edges, by [`NodeRef`] (replaces Tier 1's
    /// `Node::comp_deps`, unchanged in spirit: small in practice, so a
    /// dedup-on-insert `SmallVec` beats a hash set at this size).
    comp_deps: Vec<SmallVec<[NodeRef; 4]>>,
    /// Row `i`'s incoming call edges — see `comp_deps`.
    rdeps: Vec<SmallVec<[NodeRef; 4]>>,
    /// Freed row indices available for `Self::insert` to reuse before
    /// growing any column.
    free: Vec<u32>,
    /// `param_hash -> row`, this definition's own share of what used to be
    /// `NodeTable`'s single `(DefIndex, Hash128) -> NodeId` index — split
    /// per-definition here for the same struct-of-arrays reason as every
    /// other column.
    ///
    /// Keyed by an already-uniform content hash, so this uses
    /// [`crate::hashers::IdentityBuildHasher`] instead of `std`'s default
    /// SipHash — see [`Hash128Map`] and `crate::hashers`' docs. This is the
    /// single hottest map this optimization targets: every `ctx.eval` call
    /// does a lookup and, on a cache miss, an insert here (Stage 6
    /// profiling, `docs/persistence-benchmark-notes.md`, found SipHash
    /// costing 5.6–8.7% of self-time, much of it this map).
    index: Hash128Map<u32>,
}

impl DefTable {
    /// Existing row for `param_hash`, if any.
    fn row_of(&self, param_hash: Hash128) -> Option<u32> {
        self.index.get(&param_hash).copied()
    }

    fn contains(&self, row: u32) -> bool {
        (row as usize) < self.flags.len() && !self.flags[row as usize].is_free()
    }

    /// Allocates a fresh `Dirty`, no-result row for `param_hash`/
    /// `param_bytes`, reusing a freed row if one is available, and indexes
    /// it. Callers must ensure `param_hash` is not already indexed (the
    /// coalescing "find-or-insert" case is [`NodeTable::get_or_insert`]).
    fn insert(&mut self, param_hash: Hash128, param_bytes: &[u8]) -> u32 {
        assert!(
            param_bytes.len() <= u16::MAX as usize,
            "param_bytes too large ({} bytes) for this definition's u16 arena span",
            param_bytes.len()
        );
        let off = self.param_arena.len() as u32;
        self.param_arena.extend_from_slice(param_bytes);
        let len = param_bytes.len() as u16;

        let row = match self.free.pop() {
            Some(row) => {
                let i = row as usize;
                self.param_hash[i] = param_hash;
                self.flags[i] = NodeFlags::new(NodeState::Dirty);
                self.param_off[i] = off;
                self.param_len[i] = len;
                self.comp_deps[i] = SmallVec::new();
                self.rdeps[i] = SmallVec::new();
                row
            }
            None => {
                self.param_hash.push(param_hash);
                self.result_hash.push(Hash128::from_bytes([0; 16]));
                self.flags.push(NodeFlags::new(NodeState::Dirty));
                self.param_off.push(off);
                self.param_len.push(len);
                self.comp_deps.push(SmallVec::new());
                self.rdeps.push(SmallVec::new());
                (self.param_hash.len() - 1) as u32
            }
        };
        self.index.insert(param_hash, row);
        row
    }

    /// Frees `row`: removes it from the param-hash index and returns it to
    /// the free list for a future [`Self::insert`] to reuse. Every other
    /// column's bytes are left in place (stale, gated by `flags.is_free()`
    /// — see [`DefTable`]'s docs) until that reuse overwrites them.
    fn remove(&mut self, row: u32) {
        let i = row as usize;
        self.index.remove(&self.param_hash[i]);
        self.flags[i].set_free(true);
        self.free.push(row);
    }
}

/// The engine's node table: [`NodeTable::defs`] holds one [`DefTable`] per
/// registered definition (indexed by [`DefIndex`]), addressable either by a
/// node's stable [`CompKey`] (via each `DefTable`'s own index) or by its
/// process-local [`NodeRef`] (a direct `(DefIndex, row)` pair) — see
/// [`NodeRef`]'s docs for why this Tier-2 columnar-per-definition layout
/// replaces Tier 1's single flat `Vec<Option<Node>>` slab.
///
/// `source_deps`/`outputs`/`inflight` stay exactly as they were in Tier 1:
/// sparse side maps, now keyed by [`NodeRef`] instead of `NodeId`, with an
/// entry only for a node that actually has one. [`Self::remove_by_id`]
/// purges all three whenever a node is collected, so a GC'd node never
/// leaves an orphaned side-table entry behind.
pub(crate) struct NodeTable {
    /// Indexed by [`DefIndex`].
    defs: Vec<DefTable>,
    /// `DefIndex -> DefId`, the inverse of `def_index`, needed to
    /// reconstruct a `CompKey` from a bare `NodeRef` (see
    /// [`Self::key_of`]).
    def_ids: Vec<DefId>,
    def_index: HashMap<DefId, DefIndex>,
    source_deps: HashMap<NodeRef, HashSet<RawDep>>,
    outputs: HashMap<NodeRef, HashSet<RawOutput>>,
    inflight: HashMap<NodeRef, SharedExec>,
}

impl NodeTable {
    /// Builds an empty table with one (empty) [`DefTable`] per entry of
    /// `def_order`, in registration order — the same order
    /// [`EngineBuilder::build`] uses to assign each definition its
    /// [`DefIndex`] (simply its position in `def_order`).
    fn new(def_order: Vec<DefId>) -> Self {
        let def_index: HashMap<DefId, DefIndex> =
            def_order.iter().enumerate().map(|(i, &id)| (id, DefIndex(i as u16))).collect();
        let defs = def_order.iter().map(|_| DefTable::default()).collect();
        NodeTable {
            defs,
            def_ids: def_order,
            def_index,
            source_deps: HashMap::new(),
            outputs: HashMap::new(),
            inflight: HashMap::new(),
        }
    }

    fn dt(&self, d: DefIndex) -> &DefTable {
        &self.defs[d.0 as usize]
    }

    fn dt_mut(&mut self, d: DefIndex) -> &mut DefTable {
        &mut self.defs[d.0 as usize]
    }

    pub(crate) fn id_of(&self, key: &CompKey) -> Option<NodeRef> {
        let def = *self.def_index.get(key.def())?;
        let row = self.dt(def).row_of(key.param_hash())?;
        Some(NodeRef { def, row })
    }

    /// Whether `r` still names a live (non-freed) row.
    pub(crate) fn contains(&self, r: NodeRef) -> bool {
        self.dt(r.def).contains(r.row)
    }

    /// Reconstructs `r`'s full `CompKey` — O(1), and needs no per-row
    /// storage beyond `r` itself and this def's `param_hash` column; see
    /// [`NodeRef`]'s docs.
    pub(crate) fn key_of(&self, r: NodeRef) -> CompKey {
        CompKey::from_parts(self.def_ids[r.def.0 as usize], self.dt(r.def).param_hash[r.row as usize])
    }

    pub(crate) fn state(&self, r: NodeRef) -> NodeState {
        self.dt(r.def).flags[r.row as usize].state()
    }

    pub(crate) fn set_state(&mut self, r: NodeRef, state: NodeState) {
        self.dt_mut(r.def).flags[r.row as usize].set_state(state);
    }

    pub(crate) fn dirty_priority(&self, r: NodeRef) -> Option<DirtyPriority> {
        self.dt(r.def).flags[r.row as usize].dirty_priority()
    }

    pub(crate) fn set_dirty_priority(&mut self, r: NodeRef, priority: Option<DirtyPriority>) {
        self.dt_mut(r.def).flags[r.row as usize].set_dirty_priority(priority);
    }

    pub(crate) fn last_changed(&self, r: NodeRef) -> bool {
        self.dt(r.def).flags[r.row as usize].last_changed()
    }

    pub(crate) fn set_last_changed(&mut self, r: NodeRef, changed: bool) {
        self.dt_mut(r.def).flags[r.row as usize].set_last_changed(changed);
    }

    /// `r`'s last successful result hash, `None` if it has never completed
    /// a run (gated by `NodeFlags::has_result`, not by a per-row `Option` —
    /// see [`DefTable::result_hash`]'s docs).
    pub(crate) fn result_hash(&self, r: NodeRef) -> Option<Hash128> {
        let t = self.dt(r.def);
        t.flags[r.row as usize].has_result().then(|| t.result_hash[r.row as usize])
    }

    /// Records `r`'s just-completed successful result: sets its hash,
    /// marks it `Clean` with no pending dirty priority, and sets
    /// `has_result`. Does *not* touch `r`'s typed value column
    /// (`crate::def::CompDef::write_value`) — callers write that
    /// separately, since `NodeTable` itself stays generic over every
    /// definition's `R`.
    pub(crate) fn set_result(&mut self, r: NodeRef, hash: Hash128) {
        let t = self.dt_mut(r.def);
        t.result_hash[r.row as usize] = hash;
        let flags = &mut t.flags[r.row as usize];
        flags.set_state(NodeState::Clean);
        flags.set_dirty_priority(None);
        flags.set_has_result(true);
    }

    pub(crate) fn param_bytes(&self, r: NodeRef) -> &[u8] {
        let t = self.dt(r.def);
        let off = t.param_off[r.row as usize] as usize;
        let len = t.param_len[r.row as usize] as usize;
        &t.param_arena[off..off + len]
    }

    pub(crate) fn comp_deps(&self, r: NodeRef) -> &[NodeRef] {
        &self.dt(r.def).comp_deps[r.row as usize]
    }

    pub(crate) fn rdeps(&self, r: NodeRef) -> &[NodeRef] {
        &self.dt(r.def).rdeps[r.row as usize]
    }

    pub(crate) fn clear_comp_deps(&mut self, r: NodeRef) {
        self.dt_mut(r.def).comp_deps[r.row as usize].clear();
    }

    /// Records a `r -> dep` call edge, deduping on insert (see
    /// `DefTable::comp_deps`'s docs on why a linear scan over a small
    /// `SmallVec` beats a hash set here).
    pub(crate) fn push_comp_dep(&mut self, r: NodeRef, dep: NodeRef) {
        let v = &mut self.dt_mut(r.def).comp_deps[r.row as usize];
        if !v.contains(&dep) {
            v.push(dep);
        }
    }

    /// Records a `dep -> r` (i.e. `r` is a caller of `dep`) reverse edge on
    /// `dep`'s own `rdeps` — see [`Self::push_comp_dep`].
    pub(crate) fn push_rdep(&mut self, dep: NodeRef, r: NodeRef) {
        let v = &mut self.dt_mut(dep.def).rdeps[dep.row as usize];
        if !v.contains(&r) {
            v.push(r);
        }
    }

    /// Removes every dead ref in `dead` from every *live* row's `rdeps`
    /// across every definition — the columnar equivalent of Tier 1's
    /// `nodes.values_mut().for_each(|n| n.rdeps.retain(...))`, run by
    /// `crate::driver`'s liveness GC once per pass, before any freed row
    /// can be reused. Freed rows are skipped (their stale `rdeps` bytes are
    /// abandoned, not scanned — see [`DefTable`]'s docs).
    pub(crate) fn retain_rdeps_not_in(&mut self, dead: &HashSet<NodeRef>) {
        for t in &mut self.defs {
            for i in 0..t.flags.len() {
                if t.flags[i].is_free() {
                    continue;
                }
                t.rdeps[i].retain(|x| !dead.contains(x));
            }
        }
    }

    /// Existing row for `key`, or a freshly inserted `Dirty` one (built from
    /// `param_bytes()`, called only on the insert path).
    pub(crate) fn get_or_insert(&mut self, key: &CompKey, param_bytes: impl FnOnce() -> Vec<u8>) -> NodeRef {
        if let Some(r) = self.id_of(key) {
            return r;
        }
        self.insert_new(key, &param_bytes())
    }

    /// Inserts a brand-new row for `key`. Callers must ensure `key` is not
    /// already present — [`Self::get_or_insert`] is the coalescing variant.
    pub(crate) fn insert_new(&mut self, key: &CompKey, param_bytes: &[u8]) -> NodeRef {
        let def = *self
            .def_index
            .get(key.def())
            .expect("insert_new: key's definition must be registered in this engine's def table");
        let row = self.dt_mut(def).insert(key.param_hash(), param_bytes);
        NodeRef { def, row }
    }

    /// Frees `r`'s row (see [`DefTable::remove`]) and purges every sparse
    /// side-table entry (`source_deps`/`outputs`/`inflight`) for it, so a
    /// collected node never leaves one behind.
    pub(crate) fn remove_by_id(&mut self, r: NodeRef) {
        self.dt_mut(r.def).remove(r.row);
        self.source_deps.remove(&r);
        self.outputs.remove(&r);
        self.inflight.remove(&r);
    }

    /// Every currently-live (non-freed) row, across every definition.
    pub(crate) fn iter_refs(&self) -> impl Iterator<Item = NodeRef> + '_ {
        self.defs.iter().enumerate().flat_map(|(di, t)| {
            let def = DefIndex(di as u16);
            t.flags
                .iter()
                .enumerate()
                .filter(|(_, f)| !f.is_free())
                .map(move |(row, _)| NodeRef { def, row: row as u32 })
        })
    }

    pub(crate) fn keys(&self) -> impl Iterator<Item = CompKey> + '_ {
        self.iter_refs().map(|r| self.key_of(r))
    }

    // -- sparse side tables (`source_deps`/`outputs`/`inflight`) --

    pub(crate) fn source_deps_iter(&self, r: NodeRef) -> impl Iterator<Item = &RawDep> {
        self.source_deps.get(&r).into_iter().flatten()
    }

    pub(crate) fn source_deps_contains(&self, r: NodeRef, dep: &RawDep) -> bool {
        self.source_deps.get(&r).is_some_and(|deps| deps.contains(dep))
    }

    pub(crate) fn source_deps_clone(&self, r: NodeRef) -> HashSet<RawDep> {
        self.source_deps.get(&r).cloned().unwrap_or_default()
    }

    /// Removes and returns `r`'s source deps (empty if it had none),
    /// leaving no entry behind — the side-table equivalent of clearing a
    /// plain field.
    pub(crate) fn take_source_deps(&mut self, r: NodeRef) -> HashSet<RawDep> {
        self.source_deps.remove(&r).unwrap_or_default()
    }

    /// Merges `raw` into `r`'s source-dep set, creating the entry if this
    /// is its first one. A no-op for an empty `raw`, so a node that never
    /// reads any source never gets an entry at all.
    pub(crate) fn extend_source_deps(&mut self, r: NodeRef, raw: &HashSet<RawDep>) {
        if raw.is_empty() {
            return;
        }
        self.source_deps.entry(r).or_default().extend(raw.iter().cloned());
    }

    pub(crate) fn outputs_iter(&self, r: NodeRef) -> impl Iterator<Item = &RawOutput> {
        self.outputs.get(&r).into_iter().flatten()
    }

    pub(crate) fn outputs_clone(&self, r: NodeRef) -> HashSet<RawOutput> {
        self.outputs.get(&r).cloned().unwrap_or_default()
    }

    /// Removes and returns `r`'s outputs (empty if it had none) — see
    /// [`Self::take_source_deps`].
    pub(crate) fn take_outputs(&mut self, r: NodeRef) -> HashSet<RawOutput> {
        self.outputs.remove(&r).unwrap_or_default()
    }

    /// Merges `raw` into `r`'s output set — see [`Self::extend_source_deps`].
    pub(crate) fn extend_outputs(&mut self, r: NodeRef, raw: &HashSet<RawOutput>) {
        if raw.is_empty() {
            return;
        }
        self.outputs.entry(r).or_default().extend(raw.iter().cloned());
    }

    pub(crate) fn inflight_get(&self, r: NodeRef) -> Option<SharedExec> {
        self.inflight.get(&r).cloned()
    }

    pub(crate) fn inflight_set(&mut self, r: NodeRef, shared: SharedExec) {
        self.inflight.insert(r, shared);
    }

    pub(crate) fn inflight_clear(&mut self, r: NodeRef) {
        self.inflight.remove(&r);
    }
}

/// The node's state just before a fresh run, snapshotted so the run can
/// diff against it afterwards (early cutoff, dropped outputs, stale source
/// index entries).
struct PreRunSnapshot {
    old_hash: Option<Hash128>,
    old_outputs: HashSet<RawOutput>,
    old_source_deps: HashSet<RawDep>,
}

/// What `prepare` decided to do about an evaluation request. Generic over
/// `R`: unlike Tier 1 (where a cache hit handed back an erased `Arc<dyn
/// Any>` for the caller to downcast), `prepare::<P, R>` already has `R` in
/// scope, so a cache hit clones the typed value straight out of
/// `crate::def::CompDef::values` (see that field's docs) with no erasure
/// round-trip at all.
enum Action<R> {
    CacheHit(R),
    Join(SharedExec),
    Run(NodeRef, PreRunSnapshot),
}

/// Shared engine state. Always accessed through `Arc<EngineInner>` (see
/// [`Engine`]); the node/def tables use plain `std::sync::Mutex` and are
/// never held locked across an `.await`.
pub(crate) struct EngineInner {
    defs: Mutex<HashMap<DefId, Arc<dyn Any + Send + Sync>>>,
    pub(crate) nodes: Mutex<NodeTable>,
    /// Maintained on every dependency (re-)collection so the driver can map
    /// a changed (source, key) pair back to the computations that read it,
    /// without scanning the whole node table. Stores [`NodeRef`]s rather
    /// than full [`CompKey`]s for the same reason `DefTable::comp_deps`/
    /// `rdeps` do — see [`NodeRef`]'s docs.
    pub(crate) source_index: Mutex<HashMap<(SourceId, KeyBytes), HashSet<NodeRef>>>,
    /// Root applications (evaluated via `Engine::eval_root`), so the
    /// driver's liveness GC knows which nodes are reachable from outside the
    /// graph and must not be collected even with no `rdeps`.
    /// Keyed by `CompKey`, whose hash is dominated by a `Hash128` — see
    /// [`CompKeySet`] and `crate::hashers`' docs.
    pub(crate) roots: Mutex<CompKeySet>,
    pub(crate) registry: Registry,
    /// Wakes the driver's `run` loop when dirty work is marked from outside
    /// an active propagation round (`EngineInner::mark_dirty`/
    /// `mark_all_dirty`, e.g. a test driving the engine directly) — every
    /// key such a call marks dirty is pushed here. See
    /// `crate::driver::EngineInner::recv_marked_dirty`. Persistence's own
    /// restore-time dirtying happens before this loop even starts (see
    /// `crate::persist`), so it never needs this channel.
    pub(crate) dirty_tx: mpsc::UnboundedSender<CompKey>,
    pub(crate) dirty_rx: AsyncMutex<mpsc::UnboundedReceiver<CompKey>>,
    /// Type-erased revival/rerun operations (see `crate::persist` and
    /// [`EngineInner::rerun_node`]) for every registered definition, keyed
    /// by `DefId`. Built once, alongside `defs`, at registration time; never
    /// mutated afterward.
    pub(crate) erased_defs: HashMap<DefId, Arc<dyn ErasedDef>>,
    /// Maps a definition's name back to its (real, `'static`) `DefId`, so
    /// `crate::persist` can turn a persisted record's owned `String` name
    /// into the `DefId` a live `CompKey` needs, without ever fabricating a
    /// `&'static str`. Built once, alongside `defs`; never mutated
    /// afterward.
    pub(crate) def_names: HashMap<String, DefId>,
    /// Persistence configuration set via `EngineBuilder::persistence`, if
    /// any. `None` means persistence was never opted into. Consulted only
    /// by `Engine::run`'s startup (`crate::persist::EngineInner::persist_load`)
    /// to decide whether to attempt a load at all.
    pub(crate) persist_opts: Option<PersistOptions>,
    /// The live persistence handle, populated by `persist_load` once the
    /// database has actually been opened (or disabled — `None` — if
    /// persistence was never configured, or opening it failed even after a
    /// wipe-and-retry). Behind a `Mutex` only because it starts empty and
    /// is filled in exactly once, asynchronously, after `EngineInner`
    /// itself already exists as an `Arc`; never contended in practice.
    pub(crate) persist: Mutex<Option<Arc<PersistHandle>>>,
}

impl EngineInner {
    fn get_def<P: CompParam, R: CompResult>(&self, id: &DefId) -> Result<Arc<CompDef<P, R>>, CompError> {
        let any = {
            let defs = self.defs.lock().unwrap();
            defs.get(id).cloned()
        };
        let any = any.ok_or_else(|| CompError::Failed(format!("no computation registered named `{id}`")))?;
        any.downcast::<CompDef<P, R>>().map_err(|_| {
            CompError::Failed(format!(
                "computation `{id}` was registered with parameter/result types that do not match this handle"
            ))
        })
    }

    /// Re-runs `key`'s computation via the normal `eval` path, decoding its
    /// typed parameter from `param_bytes` fresh on every call rather than
    /// through any closure a node keeps around, and
    /// [`crate::def::ErasedDef::rerun`] for the object-safe dispatch this
    /// needs (the driver, working only from a node's `CompKey`/
    /// `param_bytes`, has no static `P`/`R` to call `eval::<P, R>` with
    /// directly). This is exactly what persisted
    /// revival's `ErasedDef::revive_key` decodes at load time, applied on
    /// demand instead of once into a permanent closure — the two paths
    /// collapse into this one function precisely because neither needs a
    /// stored closure anymore.
    ///
    /// Always returns a future (never `None`/panics): a lookup or decode
    /// failure resolves to `Err(CompError::Failed(..))` inside the returned
    /// future rather than being reported synchronously, so every caller
    /// (in practice, only [`crate::driver`]'s wave propagation, joining many
    /// of these concurrently via `join_all`) can treat every job uniformly.
    pub(crate) fn rerun_node(self: &Arc<Self>, key: &CompKey, param_bytes: &[u8]) -> BoxFuture<'static, Result<(), CompError>> {
        let Some(erased_def) = self.erased_defs.get(key.def()) else {
            let key = key.clone();
            return Box::pin(async move { Err(CompError::Failed(format!("no computation registered named `{}`", key.def()))) });
        };
        match erased_def.rerun(self.clone(), param_bytes) {
            Some(fut) => fut,
            None => {
                let key = key.clone();
                Box::pin(async move {
                    Err(CompError::Failed(format!(
                        "computation `{}`: param bytes failed to decode during rerun",
                        key.def()
                    )))
                })
            }
        }
    }

    /// Decides whether `key` is a cache hit, an in-flight join, or needs a
    /// fresh run, creating its row on first sight. On the `Run` path this
    /// also resets the node's dynamic dependency/output collections and
    /// marks it `Running`, so the caller must actually run the computation
    /// after this returns.
    ///
    /// Takes `def` (already resolved by [`Self::eval`]) so a cache hit can
    /// clone the typed result straight out of `def`'s value column (see
    /// [`crate::def::CompDef::read_value`]) rather than downcasting an
    /// erased `Arc<dyn Any>` — the Tier-2 fast path [`Action`]'s docs
    /// describe.
    fn prepare<P: CompParam, R: CompResult>(
        self: &Arc<Self>,
        def: &Arc<CompDef<P, R>>,
        key: &CompKey,
        param: &P,
    ) -> Action<R> {
        let mut nodes = self.nodes.lock().unwrap();

        if let Some(r) = nodes.id_of(key) {
            if nodes.state(r) == NodeState::Clean
                && let Some(v) = def.read_value(r.row)
            {
                return Action::CacheHit(v);
            }
            if let Some(shared) = nodes.inflight_get(r) {
                return Action::Join(shared);
            }
        }

        let r = nodes.get_or_insert(key, || {
            postcard::to_stdvec(param).expect("postcard serialization of a well-formed value should not fail")
        });

        let old_hash = nodes.result_hash(r);
        nodes.set_state(r, NodeState::Running);
        nodes.clear_comp_deps(r);

        let old_outputs = nodes.take_outputs(r);
        let old_source_deps = nodes.take_source_deps(r);

        Action::Run(
            r,
            PreRunSnapshot {
                old_hash,
                old_outputs,
                old_source_deps,
            },
        )
    }

    /// Actually runs the computation for `key`/`node_ref`, building its
    /// child `Ctx`, awaiting the body, updating the node on success or
    /// failure, and reconciling `source_index` / dropped sink outputs.
    /// `def` is already resolved (by [`Self::eval`]), unlike Tier 1 where
    /// this method did its own `get_def` lookup — see [`Self::prepare`]'s
    /// docs for why `def` now needs to be in hand earlier.
    async fn run<P: CompParam, R: CompResult>(
        self: &Arc<Self>,
        def: &Arc<CompDef<P, R>>,
        key: &CompKey,
        node_ref: NodeRef,
        param: P,
        chain: Arc<Vec<CompKey>>,
        snapshot: PreRunSnapshot,
    ) -> Result<R, CompError> {
        let PreRunSnapshot {
            old_hash,
            old_outputs,
            old_source_deps,
        } = snapshot;

        let mut child_chain = (*chain).clone();
        child_chain.push(key.clone());
        let ctx = Ctx {
            engine: self.clone(),
            caller: Some(key.clone()),
            chain: Arc::new(child_chain),
        };

        // `param` is about to be moved into the execution future below, but
        // the "executed" completion event further down wants to render it
        // for diagnostics — never stored on the node, so this is the one
        // place left with the typed param in scope. Guarding the clone
        // itself behind `tracing::enabled!` means a disabled DEBUG level
        // pays neither the clone nor the eventual `format!`.
        let param_for_trace = if tracing::enabled!(tracing::Level::DEBUG) { Some(param.clone()) } else { None };

        let body = def.body.clone();
        let fut: BoxFuture<'static, ExecResult> = Box::pin(async move {
            let start = Instant::now();
            let result = (body)(ctx, param).await?;
            let elapsed = start.elapsed();
            // `value_bytes` here is purely a local scratch encoding for the
            // content hash below (early cutoff) — it is never returned or
            // stored anywhere; `crate::persist` re-derives its own copy,
            // lazily, only for a node that actually needs to be flushed.
            let value_bytes = postcard::to_stdvec(&result)
                .expect("postcard serialization of a well-formed value should not fail");
            let hash = Hash128::from_blake3(blake3::hash(&value_bytes));
            let value: Arc<dyn Any + Send + Sync> = Arc::new(result);
            Ok((value, hash, elapsed))
        });
        let shared: SharedExec = fut.shared();

        {
            let mut nodes = self.nodes.lock().unwrap();
            nodes.inflight_set(node_ref, shared.clone());
        }

        let outcome = shared.await;

        match outcome {
            Ok((value_any, hash, elapsed)) => {
                let changed = old_hash != Some(hash);
                let value: R = downcast_value::<R>(value_any, key)?;
                let (new_source_deps, new_outputs) = {
                    let mut nodes = self.nodes.lock().unwrap();
                    nodes.set_result(node_ref, hash);
                    nodes.set_last_changed(node_ref, changed);
                    // The one clone of `R` this Tier-2 design pays on the
                    // hot path — only on a genuine state change (not on
                    // every cache hit; see `crate::def::CompDef`'s docs).
                    def.write_value(node_ref.row, value.clone());
                    nodes.inflight_clear(node_ref);

                    // Only a genuinely changed result needs a fresh save: a
                    // recomputation that hit early cutoff already has its
                    // (still correct) record on disk. A brand-new node's
                    // first run always counts as changed (`old_hash` was
                    // `None`), so this naturally covers "just created" too,
                    // without a separate case. Enqueuing here, while `nodes`
                    // is still locked, is what makes `crate::persist`'s
                    // background flush race-free: the snapshot it takes is
                    // exactly this node's state at this instant, never a
                    // state some later (possibly concurrent) rerun has since
                    // overwritten.
                    if changed {
                        crate::persist::enqueue_changed(self, &nodes, key);
                    }

                    let new_source_deps = nodes.source_deps_clone(node_ref);
                    let new_outputs = nodes.outputs_clone(node_ref);
                    (new_source_deps, new_outputs)
                };

                self.remove_stale_source_index(node_ref, &old_source_deps, &new_source_deps);

                let dropped_outputs: HashSet<RawOutput> =
                    old_outputs.iter().filter(|o| !new_outputs.contains(o)).cloned().collect();
                if !dropped_outputs.is_empty() {
                    self.delete_dropped_outputs(dropped_outputs).await;
                }

                if let Some(param) = param_for_trace {
                    tracing::debug!(
                        outcome = "executed",
                        changed,
                        elapsed_ms = elapsed.as_millis() as u64,
                        param = format!("{param:?}"),
                        "comp.eval finished"
                    );
                }
                Ok(value)
            }
            Err(e) => {
                // Errors are not memoized: leave the node `Dirty` (not
                // `Clean`) so the next eval retries instead of reusing the
                // stale (or absent) value.
                let mut nodes = self.nodes.lock().unwrap();
                nodes.set_state(node_ref, NodeState::Dirty);
                nodes.inflight_clear(node_ref);
                tracing::debug!(outcome = "error", error = %e, "comp.eval finished");
                Err(e)
            }
        }
    }

    /// Drops `key` from `source_index`'s `(source, key-bytes)` bucket for
    /// every source dependency it read last run (`old`) but not this run
    /// (`new`) — so a node that has genuinely stopped reading some
    /// `(source, key-bytes)` pair no longer receives its future change
    /// notifications.
    ///
    /// Compares `old`/`new` by `(source, key-bytes)` identity, deliberately
    /// ignoring `ver`: `source_index` itself is keyed on identity alone (it
    /// has no notion of version), so a node that re-reads the *same*
    /// `(source, key-bytes)` pair at a newer version — the ordinary case
    /// for any node whose source input just changed — must stay registered
    /// for it. Diffing `RawDep`'s full equality (as this used to) instead
    /// treats every version bump as "the old dep vanished, a new one
    /// appeared", which deletes the node's own just-(re)inserted
    /// registration for that identical pair one statement later,
    /// orphaning it: the very next change to that key would then map to no
    /// node at all in `affected_keys`, permanently.
    fn remove_stale_source_index(&self, r: NodeRef, old: &HashSet<RawDep>, new: &HashSet<RawDep>) {
        if old == new {
            return;
        }
        let new_pairs: HashSet<(SourceId, KeyBytes)> =
            new.iter().map(|dep| (dep.source.clone(), dep.key.clone())).collect();
        let mut index = self.source_index.lock().unwrap();
        for dep in old {
            let idx_key = (dep.source.clone(), dep.key.clone());
            if new_pairs.contains(&idx_key) {
                continue;
            }
            if let Some(set) = index.get_mut(&idx_key) {
                set.remove(&r);
                if set.is_empty() {
                    index.remove(&idx_key);
                }
            }
        }
    }

    async fn delete_dropped_outputs(&self, dropped: HashSet<RawOutput>) {
        let mut by_sink: HashMap<SinkId, Vec<OutBytes>> = HashMap::new();
        for out in dropped {
            by_sink.entry(out.sink).or_default().push(out.out);
        }
        for (sink_id, outs) in by_sink {
            match self.registry.sink(&sink_id) {
                Some(sink) => {
                    if let Err(e) = sink.delete_outputs(outs).await {
                        tracing::debug!(sink = %sink_id, error = %e, "failed to delete dropped sink outputs");
                    }
                }
                None => {
                    tracing::debug!(sink = %sink_id, "dropped outputs reference an unregistered sink");
                }
            }
        }
    }

    /// The shared evaluation algorithm used by both `Engine::eval_root` and
    /// `Ctx::eval`: computes `def_id` applied to `param`, memoized and
    /// single-flight-deduplicated. Returns the value together with the
    /// `CompKey` it was computed under, so the caller can record a
    /// dependency against it without recomputing the key.
    ///
    /// The whole call runs inside a `comp.eval` tracing span (fields: `comp`
    /// = definition name, `param_hash` = short content hash of the
    /// parameter). Exactly one debug-level completion event is emitted
    /// before returning, tagged with `outcome` = `cache_hit` | `dedup_join`
    /// | `executed` (plus `changed` and `elapsed_ms`) | `error`.
    pub(crate) async fn eval<P: CompParam, R: CompResult>(
        self: &Arc<Self>,
        def_id: DefId,
        param: P,
        chain: Arc<Vec<CompKey>>,
    ) -> Result<(R, CompKey), CompError> {
        let key = CompKey::new(def_id, &param);
        let span = tracing::debug_span!(
            "comp.eval",
            comp = %def_id.name(),
            param_hash = %key.param_hash().short_hex()
        );

        async move {
            if chain.contains(&key) {
                let e = CompError::Cycle(render_chain(&chain, &key));
                tracing::debug!(outcome = "error", error = %e, "comp.eval finished");
                return Err(e);
            }

            // Resolved up front (rather than only on the `Run` path, as
            // Tier 1 did) because `prepare`'s cache-hit fast path needs it
            // too — see `Self::prepare`'s docs.
            let def = match self.get_def::<P, R>(&def_id) {
                Ok(def) => def,
                Err(e) => {
                    tracing::debug!(outcome = "error", error = %e, "comp.eval finished");
                    return Err(e);
                }
            };

            let action = self.prepare::<P, R>(&def, &key, &param);

            let value = match action {
                Action::CacheHit(v) => {
                    tracing::debug!(outcome = "cache_hit", "comp.eval finished");
                    v
                }
                Action::Join(shared) => match shared.await {
                    Ok((v, _hash, _elapsed)) => {
                        tracing::debug!(outcome = "dedup_join", "comp.eval finished");
                        downcast_value::<R>(v, &key)?
                    }
                    Err(e) => {
                        tracing::debug!(outcome = "error", error = %e, "comp.eval finished");
                        return Err(e);
                    }
                },
                Action::Run(node_ref, snapshot) => self.run::<P, R>(&def, &key, node_ref, param, chain, snapshot).await?,
            };

            Ok((value, key))
        }
        .instrument(span)
        .await
    }

    /// Records a `caller -> callee` call edge, deduping on insert: both
    /// `comp_deps`/`rdeps` are small `SmallVec`s (see [`DefTable`]'s docs),
    /// so a linear "already present?" scan before pushing is the
    /// deliberately cheap choice here, not an oversight — fan-in/fan-out
    /// stays small in practice, and a `SmallVec` has no hash table to check
    /// in O(1) anyway.
    pub(crate) fn record_call_dep(&self, caller: &CompKey, callee: &CompKey) {
        let mut nodes = self.nodes.lock().unwrap();
        if let (Some(caller_r), Some(callee_r)) = (nodes.id_of(caller), nodes.id_of(callee)) {
            nodes.push_comp_dep(caller_r, callee_r);
            nodes.push_rdep(callee_r, caller_r);
        }
    }

    pub(crate) fn mark_root(&self, key: CompKey) {
        self.roots.lock().unwrap().insert(key);
    }

    pub(crate) fn record_source_deps(&self, caller: &CompKey, raw: HashSet<RawDep>) {
        if raw.is_empty() {
            return;
        }
        let caller_r = {
            let mut nodes = self.nodes.lock().unwrap();
            let r = nodes.id_of(caller);
            if let Some(r) = r {
                nodes.extend_source_deps(r, &raw);
            }
            r
        };
        let Some(caller_r) = caller_r else { return };
        let mut index = self.source_index.lock().unwrap();
        for dep in raw {
            index.entry((dep.source, dep.key)).or_default().insert(caller_r);
        }
    }

    pub(crate) fn record_outputs(&self, caller: &CompKey, raw: HashSet<RawOutput>) {
        if raw.is_empty() {
            return;
        }
        let mut nodes = self.nodes.lock().unwrap();
        if let Some(r) = nodes.id_of(caller) {
            nodes.extend_outputs(r, &raw);
        }
    }
}

fn downcast_value<R: CompResult>(v: Arc<dyn Any + Send + Sync>, key: &CompKey) -> Result<R, CompError> {
    v.downcast::<R>().map(|arc| (*arc).clone()).map_err(|_| {
        CompError::Failed(format!(
            "computation {key:?}: cached value type mismatch (registered under a conflicting type)"
        ))
    })
}

/// Renders the ancestor chain plus the repeated key as `a#hash -> b#hash ->
/// a#hash`, for a readable `CompError::Cycle` message.
fn render_chain(chain: &[CompKey], repeated: &CompKey) -> String {
    let mut rendered = String::new();
    for k in chain {
        rendered.push_str(&format!("{k:?}"));
        rendered.push_str(" -> ");
    }
    rendered.push_str(&format!("{repeated:?}"));
    rendered
}

/// A handle to the incremental engine: the def table plus the memoized node
/// graph. Cheap to clone (an `Arc` bump); every clone shares the same state.
pub struct Engine {
    pub(crate) inner: Arc<EngineInner>,
}

impl Clone for Engine {
    fn clone(&self) -> Self {
        Engine {
            inner: self.inner.clone(),
        }
    }
}

impl Engine {
    /// Starts building an `Engine`.
    pub fn builder() -> EngineBuilder {
        EngineBuilder {
            defs: HashMap::new(),
            erased_defs: HashMap::new(),
            def_names: HashMap::new(),
            def_order: Vec::new(),
            registry: Registry::default(),
            persist_opts: None,
        }
    }

    /// Evaluates `comp` applied to `param` as a root application: the
    /// public entry point used by tests and by [`crate::driver`].
    ///
    /// The resulting node is marked as a root, so the driver's liveness
    /// garbage collection will not collect it even once nothing else
    /// depends on it.
    pub async fn eval_root<P: CompParam, R: CompResult>(&self, comp: &Comp<P, R>, param: P) -> Result<R, CompError> {
        let ctx = Ctx {
            engine: self.inner.clone(),
            caller: None,
            chain: Arc::new(Vec::new()),
        };
        ctx.eval(*comp, param).await
    }
}

// `pub(crate)`, not `pub`, because its only caller is this module's own
// `#[cfg(test)] mod tests` below: `pub(crate)` items aren't visible from the
// `tests/engine.rs` integration test crate, so this helper is exercised
// in-module instead.
#[cfg(test)]
impl Engine {
    /// Test-only: forces the node for `key` to be treated as dirty, so the
    /// next `eval`/`eval_root` against it re-executes instead of hitting
    /// the memoized cache. A stand-in for the driver's real invalidation,
    /// useful for unit-testing this module in isolation from `driver.rs`.
    pub(crate) fn mark_dirty_for_test(&self, key: &CompKey) {
        let mut nodes = self.inner.nodes.lock().unwrap();
        if let Some(r) = nodes.id_of(key) {
            nodes.set_state(r, NodeState::Dirty);
        }
    }
}

/// Builds an [`Engine`]: registers computation definitions and (optionally)
/// the [`Registry`] of sources/sinks the driver wires up.
pub struct EngineBuilder {
    defs: HashMap<DefId, Arc<dyn Any + Send + Sync>>,
    erased_defs: HashMap<DefId, Arc<dyn ErasedDef>>,
    def_names: HashMap<String, DefId>,
    /// Every registered `DefId`, in registration order — the source of
    /// truth [`Engine::build`] uses to assign each definition its
    /// [`DefIndex`] (simply its position here). See [`DefIndex`]'s docs.
    def_order: Vec<DefId>,
    registry: Registry,
    persist_opts: Option<PersistOptions>,
}

impl EngineBuilder {
    /// Registers a computation definition, returning a handle to it.
    ///
    /// # Panics
    /// Panics if a computation with the same name is already registered —
    /// this is a startup configuration error, not a runtime condition.
    pub fn register<P: CompParam, R: CompResult>(&mut self, def: CompDef<P, R>) -> Comp<P, R> {
        let id = def.id;
        let def = Arc::new(def);
        let prev = self.defs.insert(id, def.clone() as Arc<dyn Any + Send + Sync>);
        assert!(prev.is_none(), "duplicate computation name: {id}");
        self.erased_defs.insert(id, Arc::new(DefAdapter(def)) as Arc<dyn ErasedDef>);
        self.def_names.insert(id.name().to_string(), id);
        self.def_order.push(id);
        Comp::from_def_id(id)
    }

    /// Defines and registers a computation named `name` in one step.
    ///
    /// This is the preferred way to define a computation: it is exactly
    /// [`crate::def::define_comp`] followed by [`EngineBuilder::register`],
    /// collapsed into a single call. Reach for `define_comp` + `register`
    /// separately only when the [`CompDef`] needs to be built somewhere else
    /// (e.g. by a library handing back a `CompDef` for the caller to
    /// register).
    ///
    /// # Panics
    /// Panics if a computation with the same name is already registered.
    pub fn define<P, R, F, Fut>(&mut self, name: &'static str, body: F) -> Comp<P, R>
    where
        P: CompParam,
        R: CompResult,
        F: Fn(Ctx, P) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R, CompError>> + Send + 'static,
    {
        self.register(define_comp(name, body))
    }

    /// Defines and registers a self-recursive computation named `name` in
    /// one step.
    ///
    /// This is the preferred way to define a self-recursive computation: it
    /// is exactly [`crate::def::define_comp_rec`] followed by
    /// [`EngineBuilder::register`], collapsed into a single call. Reach for
    /// `define_comp_rec` + `register` separately only when the [`CompDef`]
    /// needs to be built somewhere else (e.g. by a library handing back a
    /// `CompDef` for the caller to register).
    ///
    /// # Panics
    /// Panics if a computation with the same name is already registered.
    pub fn define_rec<P, R, F, Fut>(&mut self, name: &'static str, body: F) -> Comp<P, R>
    where
        P: CompParam,
        R: CompResult,
        F: Fn(Comp<P, R>, Ctx, P) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R, CompError>> + Send + 'static,
    {
        self.register(define_comp_rec(name, body))
    }

    /// Defines and registers a computation named `name` whose body is
    /// threaded a shared environment `env` on every invocation, in addition
    /// to the usual `Ctx` and parameter.
    ///
    /// This is [`crate::def::define_comp_with`] followed by
    /// [`EngineBuilder::register`] — the environment-passing counterpart to
    /// [`EngineBuilder::define`]. Where `define`'s body is `Fn(Ctx, P) ->
    /// Fut`, `define_with`'s body is `Fn(E, Ctx, P) -> Fut`: it receives an
    /// owned clone of `env`, freshly made for every invocation, so there is
    /// no need to hand-write the two-layer clone dance (clone captured
    /// handles into the outer closure, then clone them again into the inner
    /// `async move` block for every call) that capturing `Arc` handles
    /// directly would otherwise require.
    ///
    /// `env` is typically a tuple of cheaply-cloneable handles (`Arc<...>`
    /// sources/sinks, config values, `PathBuf` roots, ...); destructure it
    /// directly in the closure's parameter list, e.g. `|(source, sink,
    /// root), ctx, rel| { ... }`.
    ///
    /// `env` is taken by reference (`&E`) rather than by value, which
    /// supports two call styles:
    ///
    /// - **Shared across several definitions**: build `let env = (...);`
    ///   once, then lend `&env` to every `define_with`/`define_rec_with` call
    ///   that needs it — no `.clone()` appears anywhere in the wiring code.
    /// - **Single-use, inline**: pass a temporary directly, e.g.
    ///   `builder.define_with("x", &(a, b, c), |...| ...)`. This *moves* `a`,
    ///   `b`, `c` into the anonymous temporary, so it only suits a value used
    ///   by exactly one definition.
    ///
    /// The per-invocation `env.clone()` performed internally is cheap for an
    /// `Arc`-based env (a refcount bump), not a deep copy.
    ///
    /// # Panics
    /// Panics if a computation with the same name is already registered.
    pub fn define_with<E, P, R, F, Fut>(&mut self, name: &'static str, env: &E, body: F) -> Comp<P, R>
    where
        E: Clone + Send + Sync + 'static,
        P: CompParam,
        R: CompResult,
        F: Fn(E, Ctx, P) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R, CompError>> + Send + 'static,
    {
        self.register(define_comp_with(name, env, body))
    }

    /// Defines and registers a self-recursive computation named `name` in
    /// one step, threading a shared environment `env` on every invocation
    /// (see [`Self::define_rec`] and [`Self::define_with`]).
    ///
    /// The body's signature is `Fn(E, Comp<P, R>, Ctx, P) -> Fut`: an owned clone
    /// of `env`, then the working handle to the computation's own definition,
    /// then the usual `Ctx`/param pair.
    ///
    /// # Panics
    /// Panics if a computation with the same name is already registered.
    pub fn define_rec_with<E, P, R, F, Fut>(&mut self, name: &'static str, env: &E, body: F) -> Comp<P, R>
    where
        E: Clone + Send + Sync + 'static,
        P: CompParam,
        R: CompResult,
        F: Fn(E, Comp<P, R>, Ctx, P) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R, CompError>> + Send + 'static,
    {
        self.register(define_comp_rec_with(name, env, body))
    }

    /// Registers a source instance directly on the builder, without having
    /// to construct a [`Registry`] by hand first.
    ///
    /// Equivalent to (and implemented via) [`Registry::register_source`] on
    /// the builder's internal registry; see that method for panic behavior.
    /// See [`EngineBuilder::registry`] for how this interacts with a
    /// wholesale `registry(...)` call.
    pub fn source<S: SourceBase>(&mut self, src: Arc<S>) -> &mut Self {
        self.registry.register_source(src);
        self
    }

    /// Registers a sink instance directly on the builder, without having to
    /// construct a [`Registry`] by hand first.
    ///
    /// Equivalent to (and implemented via) [`Registry::register_sink`] on
    /// the builder's internal registry; see that method for panic behavior.
    /// See [`EngineBuilder::registry`] for how this interacts with a
    /// wholesale `registry(...)` call.
    pub fn sink<S: SinkBase>(&mut self, sink: Arc<S>) -> &mut Self {
        self.registry.register_sink(sink);
        self
    }

    /// Attaches the sources/sinks [`crate::driver`] will use, *replacing*
    /// whatever registry the builder currently holds (including anything
    /// added via [`EngineBuilder::source`]/[`EngineBuilder::sink`] before
    /// this call). An engine built without ever calling `registry`,
    /// `source`, or `sink` has an empty registry, which is fine for tests
    /// that never write to a real sink.
    ///
    /// Prefer `source`/`sink` for new code — they merge into the existing
    /// registry rather than replacing it, so call order doesn't matter. This
    /// method still exists for callers that already build a [`Registry`]
    /// separately (or want to reset the builder's registry to a specific
    /// one); mixing it with `source`/`sink` is fine as long as you keep in
    /// mind that `registry(...)` wins over anything registered before it,
    /// while `source`/`sink` called after it add to what it set.
    pub fn registry(&mut self, registry: Registry) -> &mut Self {
        self.registry = registry;
        self
    }

    /// Opts this engine into persisting its dependency graph to a local
    /// redb file, so a restart can resume from cache instead of
    /// recomputing everything from scratch. See [`crate::persist`] for the
    /// full contract — in short, a corrupt database, a format mismatch, or
    /// a record that no longer matches a registered definition is always
    /// safe to drop and recompute; persistence never fails the engine.
    ///
    /// The actual load happens later, inside [`Engine::run`], before its
    /// initial evaluation. Not calling this at all (the default) leaves
    /// persistence disabled: the engine behaves exactly as it did before
    /// this feature existed.
    pub fn persistence(&mut self, opts: PersistOptions) -> &mut Self {
        self.persist_opts = Some(opts);
        self
    }

    /// Finishes building the `Engine`.
    ///
    /// # Panics
    /// Panics if more than `u16::MAX + 1` computations were registered — the
    /// capacity of the internal [`DefIndex`] the node table's lookup index
    /// uses (see [`DefIndex`]'s docs). Not a realistic limit for any
    /// hand-registered graph of definitions.
    pub fn build(self) -> Engine {
        let (dirty_tx, dirty_rx) = mpsc::unbounded_channel();
        assert!(
            self.def_order.len() <= u16::MAX as usize + 1,
            "engine has {} registered computations, exceeding the u16::MAX+1 DefIndex capacity",
            self.def_order.len()
        );
        Engine {
            inner: Arc::new(EngineInner {
                defs: Mutex::new(self.defs),
                nodes: Mutex::new(NodeTable::new(self.def_order)),
                source_index: Mutex::new(HashMap::new()),
                roots: Mutex::new(CompKeySet::default()),
                registry: self.registry,
                dirty_tx,
                dirty_rx: AsyncMutex::new(dirty_rx),
                erased_defs: self.erased_defs,
                def_names: self.def_names,
                persist_opts: self.persist_opts,
                persist: Mutex::new(None),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::def::define_comp;
    use crate::registry::Registry;
    use crate::testutil::{GetKey, MemKvSource, VecSink, WriteDoc};

    /// Tier-2 replacement for Tier 1's `size_of::<Node>()` tripwire — the
    /// `Node` struct that guarded no longer exists (Tier 2 replaced the
    /// single flat node slab with per-definition struct-of-arrays; see
    /// [`DefTable`]). Sums the per-row cost of every *common* (non-typed)
    /// column a live row costs in `DefTable`, excluding the append-only
    /// param arena, the per-definition `param_hash -> row` index map, and
    /// the typed value column on `crate::def::CompDef` (which varies with
    /// `R` by design — see that field's docs, and
    /// `typed_value_column_is_no_larger_than_a_boxed_any` below for a spot
    /// check of the win that split buys). A regression that widens any of
    /// these dense per-row columns (a bigger hash, a wider flags byte, a
    /// `NodeRef` that grows past its documented 8 bytes, a `SmallVec`
    /// inline capacity bump) should fail loudly here rather than only show
    /// up as a surprise in the 1M-instance `persist_bench` benchmark's RSS
    /// figure. The bound is deliberately generous — a coarse tripwire, not
    /// a precise layout contract.
    #[test]
    fn node_ref_and_row_stay_small() {
        assert_eq!(std::mem::size_of::<NodeRef>(), 8, "NodeRef grew past its documented 8 bytes");

        let per_row = std::mem::size_of::<Hash128>() * 2 // param_hash + result_hash
            + std::mem::size_of::<NodeFlags>()
            + std::mem::size_of::<u32>() // param_off
            + std::mem::size_of::<u16>() // param_len
            + std::mem::size_of::<SmallVec<[NodeRef; 4]>>() * 2; // comp_deps + rdeps
        assert!(
            per_row <= 150,
            "DefTable's per-row common-column cost grew to {per_row} bytes — see this test's doc comment"
        );
    }

    /// A `u64` result costs a `u64`-sized slot in `crate::def::CompDef`'s
    /// value column (see that field's docs), no worse than the size of the
    /// fat pointer alone that Tier 1's `Arc<dyn Any + Send + Sync>` cost
    /// regardless of `R`. Unlike that fat pointer, the typed column needs
    /// no separate heap allocation, which is the real Tier-2 saving: a
    /// difference in allocation count, not in `size_of` on its own.
    #[test]
    fn typed_value_column_is_no_larger_than_a_boxed_any() {
        let typed = std::mem::size_of::<Option<u64>>();
        let erased = std::mem::size_of::<Arc<dyn Any + Send + Sync>>();
        assert!(
            typed <= erased,
            "Option<u64> ({typed} B) should be no larger than a boxed Arc<dyn Any> handle ({erased} B) alone, \
             which additionally always costs a separate heap allocation this typed column never pays"
        );
    }

    /// Regression test for the `rerun`-closure -> `Arc<EngineInner>`
    /// reference cycle the Tier-1 memory redesign removed: before that
    /// change, any node that had ever executed via the
    /// driver's rerun path permanently captured `Arc<EngineInner>` inside
    /// its own closure, so an `Engine` was never actually freed even once
    /// every external handle to it was dropped. Exercises a source-driven
    /// rerun (the exact path that used to capture the cycle) before
    /// dropping every handle, including the driver task, then asserts a
    /// `Weak` reference taken up front can no longer be upgraded — proof
    /// that nothing still holds a hidden strong `Arc<EngineInner>`.
    #[tokio::test]
    async fn engine_is_droppable_after_a_rerun() {
        let kv = MemKvSource::new("kv");
        kv.set("a", "v0").await;
        let sink = VecSink::new("docs");

        let mut registry = Registry::default();
        registry.register_source(kv.clone());
        registry.register_sink(sink.clone());

        let mut builder = Engine::builder();
        builder.registry(registry);
        let comp: Comp<(), ()> = builder.define("droppable_probe", {
            let kv = kv.clone();
            let sink = sink.clone();
            move |ctx, _: ()| {
                let kv = kv.clone();
                let sink = sink.clone();
                async move {
                    let val = ctx.src_req(&kv, GetKey("a".to_string())).await?.unwrap_or_default();
                    ctx.sink_req(
                        &sink,
                        WriteDoc {
                            name: "doc_a".to_string(),
                            content: val,
                        },
                    )
                    .await?;
                    Ok(())
                }
            }
        });
        let engine = builder.build();
        let weak = Arc::downgrade(&engine.inner);

        let handle = {
            let e = engine.clone();
            tokio::spawn(async move { e.run(comp, ()).await })
        };

        wait_for(|| sink.get("doc_a").as_deref() == Some("v0")).await;

        // Trigger a genuine source-driven rerun: this is the exact path
        // that used to permanently mark a node's `rerun` closure (and thus
        // this whole engine) unreclaimable.
        kv.set("a", "v1").await;
        wait_for(|| sink.get("doc_a").as_deref() == Some("v1")).await;

        handle.abort();
        let _ = handle.await;
        drop(engine);

        assert!(
            weak.upgrade().is_none(),
            "EngineInner should be fully dropped once every handle (including the driver task) is \
             gone -- a surviving Weak upgrade means something still holds a hidden Arc<EngineInner>, \
             e.g. a reintroduced rerun-closure cycle"
        );
    }

    /// Polls `f` every 10ms until it returns `true`, panicking if 5s pass
    /// first — mirrors `tests/driver.rs`'s own `wait_until` helper (not
    /// reused directly since that lives in a separate integration-test
    /// crate this unit test can't depend on).
    async fn wait_for(f: impl Fn() -> bool) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if f() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("condition did not become true within 5s");
    }

    /// A body's second execution can produce fewer sink outputs than its
    /// first; the ones it stopped producing must be deleted from the sink.
    /// Re-execution is simulated with `mark_dirty_for_test` to unit-test
    /// this behavior without depending on `driver.rs`'s change propagation.
    #[tokio::test]
    async fn output_diffing_deletes_dropped_sink_outputs() {
        let kv = MemKvSource::new("kv");
        let sink = VecSink::new("docs");
        kv.set("names", "a,b").await;

        let mut registry = Registry::default();
        registry.register_sink(sink.clone());

        let mut builder = Engine::builder();
        builder.registry(registry);
        let comp = builder.register(define_comp::<(), (), _, _>("write_named_docs", {
            let kv = kv.clone();
            let sink = sink.clone();
            move |ctx, _: ()| {
                let kv = kv.clone();
                let sink = sink.clone();
                async move {
                    let names = ctx
                        .src_req(&kv, GetKey("names".to_string()))
                        .await?
                        .unwrap_or_default();
                    for name in names.split(',').filter(|s| !s.is_empty()) {
                        ctx.sink_req(
                            &sink,
                            WriteDoc {
                                name: name.to_string(),
                                content: "x".to_string(),
                            },
                        )
                        .await?;
                    }
                    Ok(())
                }
            }
        }));
        let engine = builder.build();

        engine.eval_root(&comp, ()).await.unwrap();
        let mut names = sink.names();
        names.sort();
        assert_eq!(names, vec!["a".to_string(), "b".to_string()]);

        kv.set("names", "a").await;
        let key = CompKey::new(*comp.def_id(), &());
        engine.mark_dirty_for_test(&key);

        engine.eval_root(&comp, ()).await.unwrap();
        let names = sink.names();
        assert_eq!(
            names,
            vec!["a".to_string()],
            "dropped output 'b' should have been deleted from the sink"
        );
    }
}
