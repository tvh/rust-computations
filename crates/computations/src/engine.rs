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
use std::collections::hash_map::Entry;
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
    BodyFn, Comp, CompDef, DefAdapter, ErasedDef, ValueColumn, define_comp, define_comp_rec, define_comp_rec_with,
    define_comp_with,
};
use crate::error::CompError;
use crate::flow::{FlowCompDef, FlowDefAdapter, FlowId, FlowResolver, FlowThunk, flow_aware_param_hash};
use crate::hashers::IdentityBuildHasher;
use crate::interner::{SmallVerBytes, SrcDep, SrcKeyId, SrcKeyInterner};
use crate::key::{CompKey, CompKeySet, CompParam, CompResult, DefId, Hash128, Hash128Map};
use crate::persist::{PersistHandle, PersistOptions};
use crate::registry::Registry;
use crate::sink::{OutBytes, RawOutput, SinkBase, SinkId};
use crate::source::{RawDep, SourceBase};

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

/// Small-size optimization for `source_index`'s value (Stage 14 — see
/// `docs/persistence-benchmark-notes.md`).
///
/// A `source_index` entry's "zero" state is already the *absent* map entry
/// (both `EngineInner::reconcile_source_deps` and `driver::liveness_gc`
/// prune an entry the moment its last dependent goes away — see below), so
/// every entry that exists has **at least one** dependent by construction.
/// `One` holds that common single dependent inline, with no backing-table
/// allocation at all; `Many` is the always-allocated `HashSet<NodeRef>` the
/// map used to store unconditionally, reached on a key's second *distinct*
/// dependent. This is the same shape as the Haskell reference engine's
/// `SrcKeyZero`/`SrcKeyOne`/`SrcKeyMany` (their commit `2d2a726`), applied
/// to this crate's already-interned `source_index`.
///
/// **Not the same reasoning as Stage 5's rejected `SmallVec` experiment**
/// for `source_deps`/`outputs`: those containers are legitimately empty on
/// *most nodes* (a node with no source reads has zero `source_deps`), so an
/// inline `SmallVec` slot was paid on every node whether or not it was ever
/// used, and lost to a never-allocated empty `HashSet`. A `source_index`
/// entry is never in that position — it doesn't exist at all until it has a
/// first dependent — so there is no "wasted inline capacity on an empty
/// container" case here, only "one slot suffices for the common case,
/// promote when a second distinct dependent shows up."
///
/// Promotion (`One` -> `Many`) is one-way, matching the Haskell prior:
/// `Many` never demotes back to `One` when a set shrinks to a single
/// element, only all the way to the absent-entry "zero" state when its last
/// dependent is removed (see [`Self::remove`]/[`Self::retain_live`]).
/// `persist_bench`'s workload keeps every key at ~683 stable dependents
/// (no key oscillates between `Many` and `One`), and `hospital_bench`'s
/// keys never grow past one dependent in the first place, so this crate has
/// the same "no oscillating-dependent-count workload to justify the
/// complexity" absence of a demonstrated payoff the Haskell haddock cites
/// for skipping demotion there.
#[derive(Debug)]
pub(crate) enum SourceRefs {
    One(NodeRef),
    Many(HashSet<NodeRef>),
}

impl SourceRefs {
    /// Inserts `r`, promoting `One -> Many` in place the first time a
    /// second *distinct* dependent is recorded against this key.
    pub(crate) fn insert(&mut self, r: NodeRef) {
        match self {
            SourceRefs::One(existing) => {
                if *existing != r {
                    let mut set = HashSet::with_capacity(2);
                    set.insert(*existing);
                    set.insert(r);
                    *self = SourceRefs::Many(set);
                }
            }
            SourceRefs::Many(set) => {
                set.insert(r);
            }
        }
    }

    /// Removes a single dependent `r`. Returns `true` once this entry has
    /// no dependents left, in which case the caller must drop the whole
    /// entry from the map — an absent entry, not an empty container, is
    /// this type's "zero" state.
    pub(crate) fn remove(&mut self, r: NodeRef) -> bool {
        match self {
            SourceRefs::One(existing) => *existing == r,
            SourceRefs::Many(set) => {
                set.remove(&r);
                set.is_empty()
            }
        }
    }

    /// Batch removal for `driver::liveness_gc`'s mark-sweep pass: removes
    /// every dependent in `dead` from this entry in one call. Returns `true`
    /// once no dependents remain (same "drop the whole entry" contract as
    /// [`Self::remove`]). Kept separate from `remove` (rather than looping
    /// `remove` once per dead id) because `One`'s check has to test
    /// membership in the whole `dead` set at once — looping a single-item
    /// `remove` across a batch would need the *last* dead id checked to
    /// happen to be the live one, which isn't guaranteed by iteration order.
    pub(crate) fn retain_live(&mut self, dead: &HashSet<NodeRef>) -> bool {
        match self {
            SourceRefs::One(existing) => dead.contains(existing),
            SourceRefs::Many(set) => {
                for r in dead {
                    set.remove(r);
                }
                set.is_empty()
            }
        }
    }

    pub(crate) fn iter(&self) -> SourceRefsIter<'_> {
        match self {
            SourceRefs::One(r) => SourceRefsIter::One(Some(*r)),
            SourceRefs::Many(set) => SourceRefsIter::Many(set.iter()),
        }
    }
}

pub(crate) enum SourceRefsIter<'a> {
    One(Option<NodeRef>),
    Many(std::collections::hash_set::Iter<'a, NodeRef>),
}

impl Iterator for SourceRefsIter<'_> {
    type Item = NodeRef;

    fn next(&mut self) -> Option<NodeRef> {
        match self {
            SourceRefsIter::One(opt) => opt.take(),
            SourceRefsIter::Many(it) => it.next().copied(),
        }
    }
}

/// Small-size optimization for `NodeTable::source_deps`'s value (Stage 18 —
/// see `docs/persistence-benchmark-notes.md`) — the same `One`/`Many` shape
/// [`SourceRefs`] (Stage 14) applies to `source_index`, this time on the
/// *other* side table Stage 14 didn't touch: a node's own recorded source
/// dependencies, keyed by [`NodeRef`] instead of [`SrcKeyId`].
///
/// A `source_deps` entry's "zero" state is already the absent map entry
/// (`NodeTable::extend_source_deps` never creates one for an empty `raw`,
/// and `NodeTable::remove_by_id` purges it outright), so every entry that
/// exists has **at least one** recorded dependency. Stage 17's own
/// measurement found `persist_bench`'s 205,000 entries **100%**
/// single-`SrcDep` — every one of them was still paying a `HashSet`'s
/// hashbrown-minimum (3-slot) backing-table allocation to hold one 32-byte
/// value. `hospital_bench`'s 753,000 entries are *not* the same shape —
/// only ~1,500 (0.2%) are single-`SrcDep`, averaging 2.76 each, because its
/// comp bodies read multiple keys per instance (e.g. `vitals` reads
/// value+unit+range in one body) — so this stage's own measurements below
/// report that difference honestly rather than assuming Stage 14's
/// hospital-favoring result repeats here; see this stage's writeup for the
/// actual numbers.
///
/// **Not Stage 5's rejected `SmallVec` experiment**, for the identical
/// reason [`SourceRefs`]'s own docs give: `source_deps` entries don't exist
/// at all until a node has a first source read, so there is no "wasted
/// inline slot on an empty container" case here — only "one slot suffices
/// for the common single-dependency case, promote when a second *distinct*
/// dependency shows up."
///
/// Promotion (`One -> Many`) is one-way, matching Stage 14's own choice —
/// see [`SourceRefs`]'s docs for why neither of this crate's two benchmarks
/// has a workload shape that would justify demotion's added complexity.
#[derive(Debug)]
pub(crate) enum SrcDeps {
    One(SrcDep),
    Many(HashSet<SrcDep>),
}

impl SrcDeps {
    /// Inserts `dep`, promoting `One -> Many` in place the first time a
    /// second *distinct* dependency is recorded against this node — mirrors
    /// [`SourceRefs::insert`].
    fn insert(&mut self, dep: SrcDep) {
        match self {
            SrcDeps::One(existing) => {
                if *existing != dep {
                    let mut set = HashSet::with_capacity(2);
                    set.insert(existing.clone());
                    set.insert(dep);
                    *self = SrcDeps::Many(set);
                }
            }
            SrcDeps::Many(set) => {
                set.insert(dep);
            }
        }
    }

    fn iter(&self) -> SrcDepsIter<'_> {
        match self {
            SrcDeps::One(dep) => SrcDepsIter::One(Some(dep)),
            SrcDeps::Many(set) => SrcDepsIter::Many(set.iter()),
        }
    }

    /// Converts into an owned `HashSet<SrcDep>` — the shape every existing
    /// caller (`EngineInner::reconcile_source_deps`'s `old == new` check and
    /// key-id diff, `crate::persist`'s restore/probe paths) is already typed
    /// against, so this stage doesn't need to touch any of those call sites.
    /// `Many` moves its already-allocated set out with no reallocation;
    /// `One` pays a fresh single-element `HashSet` allocation, same as
    /// `NodeTable::take_source_deps` always has for a node with exactly one
    /// dependency.
    fn into_hashset(self) -> HashSet<SrcDep> {
        match self {
            SrcDeps::One(dep) => HashSet::from([dep]),
            SrcDeps::Many(set) => set,
        }
    }

    /// The borrowing counterpart of [`Self::into_hashset`], used by
    /// `NodeTable::source_deps_clone`.
    fn clone_to_hashset(&self) -> HashSet<SrcDep> {
        match self {
            SrcDeps::One(dep) => HashSet::from([dep.clone()]),
            SrcDeps::Many(set) => set.clone(),
        }
    }
}

pub(crate) enum SrcDepsIter<'a> {
    One(Option<&'a SrcDep>),
    Many(std::collections::hash_set::Iter<'a, SrcDep>),
}

impl<'a> Iterator for SrcDepsIter<'a> {
    type Item = &'a SrcDep;

    fn next(&mut self) -> Option<&'a SrcDep> {
        match self {
            SrcDepsIter::One(opt) => opt.take(),
            SrcDepsIter::Many(it) => it.next(),
        }
    }
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
    ///
    /// # Panics (debug only)
    /// `debug_assert`s that `param_bytes` fits this definition's `u16`
    /// arena span. This used to be a runtime `assert!` reachable with the
    /// node-table lock held (a param over 64 KB of postcard bytes would
    /// panic mid-`prepare`, poisoning `EngineInner::nodes` for the rest of
    /// the process — see Stage 7 of `docs/persistence-benchmark-notes.md`).
    /// Both callers of this method now check the same bound *before* ever
    /// taking that lock (`EngineInner::prepare` on the live-eval path,
    /// `EngineInner::restore_nodes` on the persisted-load path) and turn a
    /// violation into a recoverable outcome (a returned `CompError`, or a
    /// dropped-with-a-warning record) instead of reaching this method at
    /// all — so by the time `insert` runs, the bound is already an
    /// established invariant, not user-facing input to validate.
    fn insert(&mut self, param_hash: Hash128, param_bytes: &[u8]) -> u32 {
        debug_assert!(
            param_bytes.len() <= u16::MAX as usize,
            "param_bytes too large ({} bytes) for this definition's u16 arena span -- caller should have \
             checked this before ever calling insert",
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
/// purges all four whenever a node is collected, so a GC'd node never
/// leaves an orphaned side-table entry behind.
pub(crate) struct NodeTable {
    /// Indexed by [`DefIndex`].
    defs: Vec<DefTable>,
    /// `DefIndex -> DefId`, the inverse of `def_index`, needed to
    /// reconstruct a `CompKey` from a bare `NodeRef` (see
    /// [`Self::key_of`]).
    def_ids: Vec<DefId>,
    def_index: HashMap<DefId, DefIndex>,
    /// Each node's recorded source dependencies, keyed on an interned
    /// [`SrcKeyId`] rather than a raw `(SourceId, KeyBytes)` pair (Stage
    /// 13 — see `docs/persistence-benchmark-notes.md`); still hashed with
    /// `std`'s default `SipHash`, not `IdentityBuildHasher`, because
    /// [`SrcDep`] carries raw version bytes (see `crate::hashers`'s docs on
    /// why that disqualifies it, same as `RawDep`). Value is [`SrcDeps`], a
    /// `One`/`Many` small-size optimization for the common single-dependency
    /// entry (Stage 18 — see `docs/persistence-benchmark-notes.md`).
    source_deps: HashMap<NodeRef, SrcDeps>,
    outputs: HashMap<NodeRef, HashSet<RawOutput>>,
    inflight: HashMap<NodeRef, SharedExec>,
    /// A flow-argument node's ordered [`FlowId`]s (Stage 9 — see
    /// `docs/persistence-benchmark-notes.md`), sparse like the three side
    /// maps above: absent (not merely empty) for every ordinary
    /// builder-path node, which never has any. Needed at three points a
    /// flow-argument node's identity alone can't supply: rerunning it
    /// (`crate::engine::EngineInner::rerun_node` resolves flows fresh from
    /// the registry on every rerun, never from a stored closure — see
    /// [`crate::flow`]'s module docs), persisting it
    /// (`crate::persist::PendingRecord::snapshot` copies this into the
    /// on-disk `NodeRecord`), and reviving it after a restart
    /// (`crate::persist::EngineInner::restore_nodes` writes the persisted
    /// list straight back in via [`Self::set_flow_ids`]).
    flow_ids: HashMap<NodeRef, Vec<FlowId>>,
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
            flow_ids: HashMap::new(),
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
    /// side-table entry (`source_deps`/`outputs`/`inflight`/`flow_ids`) for
    /// it, so a collected node never leaves one behind.
    pub(crate) fn remove_by_id(&mut self, r: NodeRef) {
        self.dt_mut(r.def).remove(r.row);
        self.source_deps.remove(&r);
        self.outputs.remove(&r);
        self.inflight.remove(&r);
        self.flow_ids.remove(&r);
    }

    /// Records `r`'s ordered flow ids (Stage 9 — see
    /// `docs/persistence-benchmark-notes.md`). A no-op for an empty `flows`,
    /// so an ordinary builder-path node never gets an entry at all — see
    /// this table's `flow_ids` field docs on why this stays sparse.
    pub(crate) fn set_flow_ids(&mut self, r: NodeRef, flows: Vec<FlowId>) {
        if flows.is_empty() {
            return;
        }
        self.flow_ids.insert(r, flows);
    }

    /// `r`'s recorded flow ids, empty if it has none (the ordinary
    /// builder-path case).
    pub(crate) fn flow_ids_clone(&self, r: NodeRef) -> Vec<FlowId> {
        self.flow_ids.get(&r).cloned().unwrap_or_default()
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

    pub(crate) fn source_deps_iter(&self, r: NodeRef) -> impl Iterator<Item = &SrcDep> {
        self.source_deps.get(&r).map(SrcDeps::iter).into_iter().flatten()
    }

    /// Whether `r` currently has a dependency on exactly `key_id` at
    /// exactly `ver`. A linear scan, not a `HashSet::contains` lookup: it
    /// would need an owned `SrcDep` (a `ver.to_vec()` allocation) to probe
    /// with, and a node's own distinct source-dep count stays small in
    /// practice — the same tradeoff `DefTable::comp_deps`'s docs make for
    /// `SmallVec` fan-in/fan-out lists.
    pub(crate) fn source_deps_contains(&self, r: NodeRef, key_id: SrcKeyId, ver: &[u8]) -> bool {
        self.source_deps
            .get(&r)
            .is_some_and(|deps| deps.iter().any(|d| d.key_id == key_id && d.ver.as_slice() == ver))
    }

    /// Clones `r`'s source deps into an owned `HashSet<SrcDep>` — the shape
    /// `EngineInner::reconcile_source_deps`'s equality/diff logic and
    /// `crate::persist`'s probe path are typed against; see
    /// [`SrcDeps::clone_to_hashset`].
    pub(crate) fn source_deps_clone(&self, r: NodeRef) -> HashSet<SrcDep> {
        self.source_deps.get(&r).map(SrcDeps::clone_to_hashset).unwrap_or_default()
    }

    /// Removes and returns `r`'s source deps as an owned `HashSet<SrcDep>`
    /// (empty if it had none), leaving no entry behind — the side-table
    /// equivalent of clearing a plain field. See [`SrcDeps::into_hashset`]
    /// for why the common `Many` case is a free move, not a reallocation.
    pub(crate) fn take_source_deps(&mut self, r: NodeRef) -> HashSet<SrcDep> {
        self.source_deps.remove(&r).map(SrcDeps::into_hashset).unwrap_or_default()
    }

    /// Merges `raw` into `r`'s source-dep set, creating the entry (starting
    /// life as [`SrcDeps::One`], never an implicit empty `Many` — see
    /// [`SourceRefs`]'s docs on why a vacant entry never starts as `Many`)
    /// if this is its first one. A no-op for an empty `raw`, so a node that
    /// never reads any source never gets an entry at all.
    pub(crate) fn extend_source_deps(&mut self, r: NodeRef, raw: &HashSet<SrcDep>) {
        if raw.is_empty() {
            return;
        }
        match self.source_deps.entry(r) {
            Entry::Vacant(v) => {
                let mut iter = raw.iter().cloned();
                let first = iter.next().expect("raw was checked non-empty above");
                let mut deps = SrcDeps::One(first);
                for dep in iter {
                    deps.insert(dep);
                }
                v.insert(deps);
            }
            Entry::Occupied(mut o) => {
                for dep in raw.iter().cloned() {
                    o.get_mut().insert(dep);
                }
            }
        }
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
    old_source_deps: HashSet<SrcDep>,
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

/// Debug-only determinism check for a brand-new node's parameter (see
/// [`crate::key::CompParam`]'s docs for the determinism requirement this
/// enforces, and `crate::persist::EngineInner::restore_nodes`'s load-time
/// key verification for the loud, load-time counterpart of the exact same
/// hazard).
///
/// Round-trips `param_bytes` through a decode-then-re-encode (mirroring
/// exactly what `crate::def::ErasedDef::revive_key` does at persisted-load
/// time) and `debug_assert_eq!`s the two encodings. This — not a naive
/// "serialize the same in-memory value twice" — is the check that actually
/// catches a `HashMap`/`HashSet` parameter: re-serializing the very same
/// `param` object twice in a row always produces identical bytes (its
/// bucket layout, and thus iteration order, is already fixed for that
/// object's lifetime), so it would never trip. A fresh decode builds a
/// genuinely new instance — a new `HashMap` gets its own `RandomState` seed
/// — so if the type's iteration order isn't actually deterministic, the
/// round-tripped re-encoding is very likely to disagree with the original,
/// right here, on the developer's very first run with such a parameter.
///
/// Never called for a cache hit or an already-existing dirty node — only
/// when `param_bytes` is about to be stored for a node this engine has
/// never seen before (see the one call site, in [`EngineInner::prepare`]).
#[cfg(debug_assertions)]
fn debug_check_param_determinism<P: CompParam>(key: &CompKey, param_bytes: &[u8]) {
    let Ok(decoded) = postcard::from_bytes::<P>(param_bytes) else {
        // A param this process just serialized should always decode back as
        // its own type; if it doesn't, that's a distinct serde bug this
        // check isn't meant to catch -- leave it to surface elsewhere
        // (e.g. the very next `eval` of the same param will hit the same
        // decode failure again, deterministically).
        return;
    };
    let re_encoded = postcard::to_stdvec(&decoded).expect("postcard serialization of a well-formed value should not fail");
    debug_assert_eq!(
        param_bytes,
        re_encoded.as_slice(),
        "computation `{key:?}`: this parameter re-serializes to different bytes after a decode/encode \
         round-trip -- almost always a HashMap/HashSet (or other iteration-order-dependent type) \
         somewhere in the parameter, whose in-memory order isn't stable across a fresh instance; use \
         BTreeMap/BTreeSet (or another deterministically-ordered container) for anything that feeds a \
         CompKey's identity. Left undetected, this can silently split one logical computation \
         application into multiple node identities, and orphan a persisted record across a restart \
         (see `crate::key::CompParam`'s docs)."
    );
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
    ///
    /// Keyed on [`SrcKeyId`] rather than a raw `(SourceId, KeyBytes)` pair
    /// (Stage 13 — see `docs/persistence-benchmark-notes.md`), and hashed
    /// with [`IdentityBuildHasher`]: an id is an opaque process-local `u32`
    /// this process itself assigned, never adversary-chosen byte content —
    /// see `crate::hashers`'s docs on why that's exactly the shape of key
    /// this hasher is safe for. Value is [`SourceRefs`], a `One`/`Many`
    /// small-size optimization over the single-dependent case (Stage 14 —
    /// see `docs/persistence-benchmark-notes.md`, and [`SourceRefs`]'s own
    /// docs for why this differs from Stage 5's rejected `SmallVec`
    /// experiment).
    pub(crate) source_index: Mutex<HashMap<SrcKeyId, SourceRefs, IdentityBuildHasher>>,
    /// Refcounted `(SourceId, key bytes) -> SrcKeyId` interner backing
    /// `source_deps`/`source_index` — see [`crate::interner`]'s module
    /// docs for the full design (including reclamation and why versions
    /// are deliberately left uninterned).
    pub(crate) src_key_interner: Mutex<SrcKeyInterner>,
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
    /// Whether `COMPUTATIONS_LOCK_STATS` was read as enabled at
    /// `EngineBuilder::build()` time -- read exactly once, there, and never
    /// again (see `crate::lock_stats`'s module docs for why a per-call
    /// re-check would itself perturb the measurement). [`Self::timed`]
    /// branches on this plain `bool`, never on a fresh env lookup.
    pub(crate) lock_stats_enabled: bool,
    /// Per-call-site lock-hold-time accumulators, written to only when
    /// `lock_stats_enabled` is set -- see [`Self::timed`] and
    /// `crate::lock_stats`.
    pub(crate) lock_stats: crate::lock_stats::LockStats,
}

impl EngineInner {
    /// Times `f` (expected to be one semantic critical section -- see
    /// `crate::lock_stats::LockSite`'s docs for the naming convention) and
    /// records its elapsed time under `site`, but only when
    /// `lock_stats_enabled` was set at build time. When disabled this costs
    /// exactly one predictable branch: no `Instant::now`, no atomic write.
    /// See `crate::lock_stats`'s module docs for why the enabled/disabled
    /// decision itself is never re-read here.
    #[inline]
    pub(crate) fn timed<T>(&self, site: crate::lock_stats::LockSite, f: impl FnOnce() -> T) -> T {
        if self.lock_stats_enabled {
            let t0 = Instant::now();
            let out = f();
            self.lock_stats.record(site, t0.elapsed());
            out
        } else {
            f()
        }
    }

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
    ///
    /// `flow_ids` is `r`'s recorded flow list (empty for an ordinary
    /// builder-path node — Stage 9, see
    /// `docs/persistence-benchmark-notes.md`). [`crate::def::ErasedDef::rerun`]
    /// is tried first; only when that reports "not applicable" (`None` —
    /// always the case for a flow-argument def, whose `param_bytes` alone
    /// can't reconstruct its identity, never for a builder-path def with
    /// well-formed `param_bytes`) does this fall back to
    /// [`crate::def::ErasedDef::rerun_flows`]. This ordering means a caller
    /// never has to know in advance which kind of def `key` names — it just
    /// always passes whatever flow list it has on hand, empty or not.
    pub(crate) fn rerun_node(
        self: &Arc<Self>,
        key: &CompKey,
        flow_ids: &[FlowId],
        param_bytes: &[u8],
    ) -> BoxFuture<'static, Result<(), CompError>> {
        let Some(erased_def) = self.erased_defs.get(key.def()) else {
            let key = key.clone();
            return Box::pin(async move { Err(CompError::Failed(format!("no computation registered named `{}`", key.def()))) });
        };
        if let Some(fut) = erased_def.rerun(self.clone(), param_bytes) {
            return fut;
        }
        if let Some(fut) = erased_def.rerun_flows(self.clone(), flow_ids, param_bytes) {
            return fut;
        }
        let key = key.clone();
        Box::pin(async move {
            Err(CompError::Failed(format!(
                "computation `{}`: param bytes failed to decode during rerun",
                key.def()
            )))
        })
    }

    /// Decides whether `key` is a cache hit, an in-flight join, or needs a
    /// fresh run, creating its row on first sight. On the `Run` path this
    /// also resets the node's dynamic dependency/output collections and
    /// marks it `Running`, so the caller must actually run the computation
    /// after this returns.
    ///
    /// Takes `def` (already resolved by [`Self::eval`]/[`Self::eval_flows`])
    /// so a cache hit can clone the typed result straight out of `def`'s
    /// value column (see [`crate::def::ValueColumn::read_value`]) rather
    /// than downcasting an erased `Arc<dyn Any>` — the Tier-2 fast path
    /// [`Action`]'s docs describe. Generic over `D:
    /// `[`ValueColumn`]`<R>` rather than a concrete `Arc<CompDef<P, R>>`
    /// (Stage 9 — see `docs/persistence-benchmark-notes.md`) precisely so
    /// this one implementation serves both the builder path
    /// (`D = CompDef<P, R>`) and the flow-argument path
    /// (`D = crate::flow::FlowCompDef<R>`) without forking it.
    ///
    /// Also takes `param_bytes` — [`Self::eval`]'s own postcard encoding of
    /// the parameter, computed once, before this call, entirely outside the
    /// node-table lock (see that method). This is what lets `prepare` itself
    /// never fallibly serialize anything while `self.nodes` is locked: the
    /// one remaining failure mode reachable from user-supplied data on the
    /// insert path — `param_bytes` not fitting this definition's `u16`
    /// arena span (see `DefTable::insert`) — is checked here too, still
    /// before the lock, and turned into a returned [`CompError`] instead of
    /// a panic. See Stage 7 of `docs/persistence-benchmark-notes.md` for the
    /// hazard this closes: a panic while `self.nodes` is locked poisons the
    /// `std::sync::Mutex` for the rest of the process, taking down every
    /// concurrent computation, not just the offending one.
    fn prepare<P: CompParam, R: CompResult, D: ValueColumn<R> + ?Sized>(
        self: &Arc<Self>,
        def: &D,
        key: &CompKey,
        param_bytes: &[u8],
    ) -> Result<Action<R>, CompError> {
        // Checked before the lock (and before any node for `key` is known
        // to exist or not) so a caller resubmitting an oversized param never
        // even reaches `self.nodes.lock()` — cheap enough (one length
        // comparison) to pay unconditionally rather than only on the
        // (unknown-until-locked) insert path.
        if param_bytes.len() > u16::MAX as usize {
            return Err(CompError::Failed(format!(
                "computation `{}`: parameter serializes to {} bytes, exceeding this engine's {}-byte \
                 per-node limit (see `crate::engine::DefTable`'s param arena)",
                key.def(),
                param_bytes.len(),
                u16::MAX
            )));
        }

        self.timed(crate::lock_stats::LockSite::Prepare, || {
            let mut nodes = self.nodes.lock().unwrap();

            if let Some(r) = nodes.id_of(key) {
                if nodes.state(r) == NodeState::Clean
                    && let Some(v) = def.read_value(r.row)
                {
                    return Ok(Action::CacheHit(v));
                }
                if let Some(shared) = nodes.inflight_get(r) {
                    return Ok(Action::Join(shared));
                }
            } else {
                // A genuinely new node: this is the one point `param_bytes`
                // was "first serialized" for it (`Self::eval` serializes on
                // every call, cache hit or not, but only a first-sight
                // `param` ever reaches this branch and actually gets
                // stored) — see `debug_check_param_determinism`'s docs for
                // what this checks and why. Debug-only, so a release build
                // pays nothing here.
                #[cfg(debug_assertions)]
                debug_check_param_determinism::<P>(key, param_bytes);
            }

            let r = nodes.get_or_insert(key, || param_bytes.to_vec());

            let old_hash = nodes.result_hash(r);
            nodes.set_state(r, NodeState::Running);
            nodes.clear_comp_deps(r);

            let old_outputs = nodes.take_outputs(r);
            let old_source_deps = nodes.take_source_deps(r);

            Ok(Action::Run(
                r,
                PreRunSnapshot {
                    old_hash,
                    old_outputs,
                    old_source_deps,
                },
            ))
        })
    }

    /// Actually runs the computation for `key`/`node_ref`, building its
    /// child `Ctx`, awaiting `body`, updating the node on success or
    /// failure, and reconciling `source_index` / dropped sink outputs.
    /// `def` is already resolved (by [`Self::eval`]/[`Self::eval_flows`]),
    /// unlike Tier 1 where this method did its own `get_def` lookup — see
    /// [`Self::prepare`]'s docs for why `def` now needs to be in hand
    /// earlier.
    ///
    /// `body` is taken as an explicit [`BodyFn`] rather than read off `def`
    /// (Stage 9 — see `docs/persistence-benchmark-notes.md`): the builder
    /// path's caller passes `def.body.clone()`, exactly as this method used
    /// to read internally, while the flow-argument path's caller builds a
    /// one-off `BodyFn` that resolves flows via a `crate::flow::FlowResolver`
    /// and calls the registered `crate::flow::FlowThunk` — letting `def`
    /// itself stay generic over just [`ValueColumn`]`<R>` (needed only for
    /// [`Self::prepare`]'s cache-hit read and this method's post-execution
    /// write) with no `body`/`thunk` field in common between the two def
    /// kinds at all.
    // `body` is the one parameter Stage 9 added on top of Tier 2's already
    // seven (see the doc comment above for why it can't just be read off
    // `def` anymore); splitting the rest into a struct purely to dodge this
    // lint would cost more clarity at each of `run`'s two call sites than it
    // would buy here.
    #[allow(clippy::too_many_arguments)]
    async fn run<P: CompParam, R: CompResult, D: ValueColumn<R> + ?Sized>(
        self: &Arc<Self>,
        def: &D,
        body: BodyFn<P, R>,
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

        // Cloned in for the error message below only: `key` itself isn't
        // moved into the future (`ctx`/`param` are), and this is cheap (a
        // `DefId` is a `&'static str`, `Hash128` is 16 bytes).
        let key_for_err = key.clone();
        let fut: BoxFuture<'static, ExecResult> = Box::pin(async move {
            let start = Instant::now();
            let result = (body)(ctx, param).await?;
            let elapsed = start.elapsed();
            // `value_bytes` here is purely a local scratch encoding for the
            // content hash below (early cutoff) — it is never returned or
            // stored anywhere; `crate::persist` re-derives its own copy,
            // lazily, only for a node that actually needs to be flushed.
            //
            // A failure here used to `.expect()` (panic): that panic
            // propagates through this `Shared` future's `join_all` in
            // `crate::driver::run_wave`, up into the task running
            // `Engine::run` — a caller that (per this crate's own examples)
            // `tokio::spawn`s that task and never inspects its `JoinHandle`
            // would simply lose the whole driver forever, silently (see
            // Stage 7 of `docs/persistence-benchmark-notes.md`). Returning a
            // `CompError` instead means only *this* node fails: it's logged
            // via the existing error path below, stays `Dirty`, and the
            // driver keeps running everything else.
            let value_bytes = match postcard::to_stdvec(&result) {
                Ok(bytes) => bytes,
                Err(e) => {
                    return Err(CompError::Failed(format!(
                        "computation {key_for_err:?}: result failed to serialize for its content hash: {e}"
                    )));
                }
            };
            let hash = Hash128::from_blake3(blake3::hash(&value_bytes));
            let value: Arc<dyn Any + Send + Sync> = Arc::new(result);
            Ok((value, hash, elapsed))
        });
        let shared: SharedExec = fut.shared();

        self.timed(crate::lock_stats::LockSite::RunSetInflight, || {
            let mut nodes = self.nodes.lock().unwrap();
            nodes.inflight_set(node_ref, shared.clone());
        });

        let outcome = shared.await;

        match outcome {
            Ok((value_any, hash, elapsed)) => {
                let changed = old_hash != Some(hash);
                let value: R = downcast_value::<R>(value_any, key)?;
                let (new_source_deps, new_outputs) = self.timed(crate::lock_stats::LockSite::RunFinishSuccess, || {
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
                });

                self.reconcile_source_deps(node_ref, &old_source_deps, &new_source_deps);

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
                let new_source_deps = self.timed(crate::lock_stats::LockSite::RunFinishError, || {
                    let mut nodes = self.nodes.lock().unwrap();
                    nodes.set_state(node_ref, NodeState::Dirty);
                    nodes.inflight_clear(node_ref);
                    nodes.source_deps_clone(node_ref)
                });
                // A failed run may still have recorded some deps via
                // `record_source_deps` before it errored out (whatever it
                // read before failing) -- reconciling against `old` here,
                // exactly as the success path does, is what keeps
                // `src_key_interner`'s refcounts correct: `old_source_deps`
                // was already taken out of the node table in `prepare`
                // (before this run started), so if this reconciliation
                // never ran, its references would simply never be
                // released, leaking one interned id per failed run's
                // stale dependency. See `crate::interner`'s module docs.
                self.reconcile_source_deps(node_ref, &old_source_deps, &new_source_deps);
                tracing::debug!(outcome = "error", error = %e, "comp.eval finished");
                Err(e)
            }
        }
    }

    /// Reconciles `r`'s recorded source dependencies after a run (Stage 13
    /// — see `docs/persistence-benchmark-notes.md` — this is the renamed,
    /// interner-aware `remove_stale_source_index`): retains a fresh
    /// `src_key_interner` reference for every dependency identity `r`
    /// gained (in `new` but not `old`), then drops `source_index`'s
    /// registration *and* the interner reference for every identity it
    /// lost (in `old` but not `new`) — so a node that has genuinely
    /// stopped reading some key no longer receives its future change
    /// notifications, and the key's interned id is reclaimed once nothing
    /// else references it either.
    ///
    /// Compares `old`/`new` by [`SrcKeyId`] identity, deliberately ignoring
    /// `ver`: `source_index` (and the interner's refcount) has no notion of
    /// version, so a node that re-reads the *same* key at a newer version —
    /// the ordinary case for any node whose source input just changed —
    /// must stay registered for it and must not transiently release its
    /// reference. Diffing by full equality (key *and* version) instead
    /// treats every version bump as "the old dep vanished, a new one
    /// appeared", which would delete the node's own just-(re)inserted
    /// registration one statement later, orphaning it — the very next
    /// change to that key would then map to no node at all in
    /// `affected_keys`, permanently (this was a real, fixed bug; see
    /// `successive_live_changes_to_the_same_key_each_trigger_a_rerun` in
    /// `tests/driver.rs`).
    ///
    /// Retains before releasing, in that order: see [`crate::interner`]'s
    /// module docs on why the ordering matters when the same key is read
    /// again this run.
    fn reconcile_source_deps(&self, r: NodeRef, old: &HashSet<SrcDep>, new: &HashSet<SrcDep>) {
        if old == new {
            return;
        }
        let old_ids: HashSet<SrcKeyId> = old.iter().map(|d| d.key_id).collect();
        let new_ids: HashSet<SrcKeyId> = new.iter().map(|d| d.key_id).collect();
        if old_ids == new_ids {
            // A pure version bump on every dependency: no identity actually
            // changed, so neither `source_index` nor the interner has
            // anything to do.
            return;
        }
        self.timed(crate::lock_stats::LockSite::RemoveStaleSourceIndex, || {
            {
                let mut interner = self.src_key_interner.lock().unwrap();
                for &id in new_ids.difference(&old_ids) {
                    interner.retain(id);
                }
            }
            let dropped: Vec<SrcKeyId> = old_ids.difference(&new_ids).copied().collect();
            if dropped.is_empty() {
                return;
            }
            {
                let mut index = self.source_index.lock().unwrap();
                for &id in &dropped {
                    if let Entry::Occupied(mut occ) = index.entry(id)
                        && occ.get_mut().remove(r)
                    {
                        occ.remove();
                    }
                }
            }
            let mut interner = self.src_key_interner.lock().unwrap();
            for id in dropped {
                interner.release(id);
            }
        });
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
        // Serialized once, up front, entirely outside the node-table lock:
        // this call's own `CompKey` (a content hash of these bytes, exactly
        // as `CompKey::new`/`StableHash` would compute it) and, on the
        // insert path, `prepare`'s stored `param_bytes` are both derived
        // from this single encoding — see `Self::prepare`'s docs on why
        // that matters (no fallible serialization ever happens while
        // `self.nodes` is locked; see Stage 7 of
        // `docs/persistence-benchmark-notes.md`).
        let param_bytes = match postcard::to_stdvec(&param) {
            Ok(bytes) => bytes,
            Err(e) => {
                return Err(CompError::Failed(format!("computation `{def_id}`: parameter failed to serialize: {e}")));
            }
        };
        let key = CompKey::from_parts(def_id, Hash128::from_blake3(blake3::hash(&param_bytes)));
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

            let action = match self.prepare::<P, R, CompDef<P, R>>(def.as_ref(), &key, &param_bytes) {
                Ok(action) => action,
                Err(e) => {
                    tracing::debug!(outcome = "error", error = %e, "comp.eval finished");
                    return Err(e);
                }
            };

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
                Action::Run(node_ref, snapshot) => {
                    let body = def.body.clone();
                    self.run::<P, R, CompDef<P, R>>(def.as_ref(), body, &key, node_ref, param, chain, snapshot).await?
                }
            };

            Ok((value, key))
        }
        .instrument(span)
        .await
    }

    /// Looks up `id`'s registered flow-argument definition (Stage 9 — see
    /// `docs/persistence-benchmark-notes.md`), downcast to the caller's
    /// concrete `R`. The flow-argument counterpart of [`Self::get_def`]: it
    /// shares that method's `defs` table (a flow-argument def and a
    /// builder-path def can never collide on the same name — whichever
    /// registers first wins the map slot, and the other panics at
    /// registration time, exactly as two builder-path defs sharing a name
    /// already do) but needs only `R`, never `P` — see
    /// [`crate::flow::FlowCompDef`]'s docs for why a flow-argument def's
    /// parameter type is never named at the engine level at all.
    fn get_flow_def<R: CompResult>(&self, id: &DefId) -> Result<Arc<FlowCompDef<R>>, CompError> {
        let any = {
            let defs = self.defs.lock().unwrap();
            defs.get(id).cloned()
        };
        let any = any.ok_or_else(|| CompError::Failed(format!("no flow computation registered named `{id}`")))?;
        any.downcast::<FlowCompDef<R>>().map_err(|_| {
            CompError::Failed(format!(
                "flow computation `{id}` was registered with a different result type than this call expects"
            ))
        })
    }

    /// The flow-argument counterpart of [`Self::eval`] (Stage 9 — see
    /// `docs/persistence-benchmark-notes.md`'s design rationale): looks up
    /// `name`'s registered flow definition, builds the flow-aware
    /// [`CompKey`] (via [`flow_aware_param_hash`], which folds `flows`' ids
    /// into the identity — see [`crate::flow`]'s module docs for why that's
    /// a correctness requirement, not an optimization), then runs through
    /// the exact same [`Self::prepare`]/[`Self::run`] cache-hit/
    /// single-flight-join/run algorithm [`Self::eval`] uses — never a
    /// forked copy of it. The public entry point for this is
    /// [`crate::ctx::Ctx::eval_flows`]; this method is also, in a sense,
    /// this crate's own hand-written stand-in for what a future
    /// `#[computation]` macro would generate a call to.
    pub(crate) async fn eval_flows<P: CompParam, R: CompResult>(
        self: &Arc<Self>,
        name: &'static str,
        flows: &[FlowId],
        param: P,
        chain: Arc<Vec<CompKey>>,
    ) -> Result<(R, CompKey), CompError> {
        let def_id = DefId::new(name);
        let param_bytes = match postcard::to_stdvec(&param) {
            Ok(bytes) => bytes,
            Err(e) => {
                return Err(CompError::Failed(format!("computation `{def_id}`: parameter failed to serialize: {e}")));
            }
        };
        let flow_def = self.get_flow_def::<R>(&def_id)?;
        self.eval_flows_core::<P, R>(def_id, flow_def, flows, param, param_bytes, chain).await
    }

    /// The erased rerun/revival entry point for a flow-argument node
    /// (Stage 9): unlike [`Self::eval_flows`], this has no compile-time `P`
    /// at all — only `param_bytes`, exactly what
    /// [`crate::persist::EngineInner::restore_nodes`]/
    /// [`Self::rerun_node`] have on hand for a node that has no live
    /// handle. It runs the identical [`Self::eval_flows_core`] machinery
    /// instantiated at `P = ()`; this is sound (not a hack) precisely
    /// because a flow-argument def's engine-visible identity and execution
    /// never depend on `P` in the first place — [`crate::flow::FlowThunk`]
    /// decodes `param_bytes` itself, and [`Self::prepare`]'s debug-only
    /// param-determinism check (the only place `P` is actually used at
    /// runtime) only ever fires for a node this engine has never seen
    /// before, which a rerun/revival can't be by construction (the node
    /// already exists — that's why it's being rerun). The one visible cost
    /// is diagnostic, not correctness: a flow-argument node's rerun trace
    /// event renders its `param` field as `()` rather than the real typed
    /// value, since that value was never reconstructed here at all.
    pub(crate) async fn eval_flows_erased<R: CompResult>(
        self: &Arc<Self>,
        flow_def: Arc<FlowCompDef<R>>,
        flows: &[FlowId],
        param_bytes: Vec<u8>,
        chain: Arc<Vec<CompKey>>,
    ) -> Result<(), CompError> {
        let def_id = flow_def.id;
        self.eval_flows_core::<(), R>(def_id, flow_def, flows, (), param_bytes, chain).await.map(|_| ())
    }

    /// The shared implementation behind [`Self::eval_flows`]/
    /// [`Self::eval_flows_erased`] — the flow-argument analogue of
    /// [`Self::eval`] itself. `param_bytes` is taken as an explicit,
    /// already-encoded argument (rather than re-derived from `param`,
    /// unlike `eval`) so [`Self::eval_flows_erased`] can supply the real,
    /// original bytes even though it has no real typed `param` to encode.
    async fn eval_flows_core<P: CompParam, R: CompResult>(
        self: &Arc<Self>,
        def_id: DefId,
        flow_def: Arc<FlowCompDef<R>>,
        flows: &[FlowId],
        param: P,
        param_bytes: Vec<u8>,
        chain: Arc<Vec<CompKey>>,
    ) -> Result<(R, CompKey), CompError> {
        let param_hash = flow_aware_param_hash(flows, &param_bytes);
        let key = CompKey::from_parts(def_id, param_hash);
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

            let action = match self.prepare::<P, R, FlowCompDef<R>>(flow_def.as_ref(), &key, &param_bytes) {
                Ok(action) => action,
                Err(e) => {
                    tracing::debug!(outcome = "error", error = %e, "comp.eval finished");
                    return Err(e);
                }
            };

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
                Action::Run(node_ref, snapshot) => {
                    // Recorded once, right here, rather than inside
                    // `prepare` (which stays entirely flow-agnostic): a
                    // fresh node's flows never change across its lifetime
                    // (a different flow set hashes to a different `CompKey`
                    // entirely -- see `flow_aware_param_hash`), so setting
                    // this unconditionally on every `Run` is a cheap,
                    // idempotent no-op after the first time.
                    {
                        let mut nodes = self.nodes.lock().unwrap();
                        nodes.set_flow_ids(node_ref, flows.to_vec());
                    }
                    let body = build_flow_body::<P, R>(flow_def.thunk, flows, param_bytes, key.clone());
                    self.run::<P, R, FlowCompDef<R>>(flow_def.as_ref(), body, &key, node_ref, param, chain, snapshot)
                        .await?
                }
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
        self.timed(crate::lock_stats::LockSite::RecordCallDep, || {
            let mut nodes = self.nodes.lock().unwrap();
            if let (Some(caller_r), Some(callee_r)) = (nodes.id_of(caller), nodes.id_of(callee)) {
                nodes.push_comp_dep(caller_r, callee_r);
                nodes.push_rdep(callee_r, caller_r);
            }
        });
    }

    pub(crate) fn mark_root(&self, key: CompKey) {
        self.roots.lock().unwrap().insert(key);
    }

    /// Records `raw` as `caller`'s source dependencies for the run
    /// currently in progress.
    ///
    /// Interns every dep's raw `(source, key)` bytes into a [`SrcKeyId`]
    /// *before* either lock below (Stage 13 — see
    /// `docs/persistence-benchmark-notes.md`), so the two critical sections
    /// that actually dominate this call's frequency — one per source read,
    /// across every node in the graph — never hash a byte vector: they only
    /// ever compare/insert a `u32`. Interning itself still hashes the raw
    /// bytes once per dep (unavoidable — it's the only way to recognize a
    /// key seen before), but against `src_key_interner`'s own map, sized by
    /// the number of *distinct* keys a workload has (300 on
    /// `persist_bench`), not by the number of dependents reading them
    /// (205,000) — see [`crate::interner`]'s module docs.
    ///
    /// Does *not* retain a reference for any newly-interned id: interning
    /// alone never implies a live reference (see
    /// [`crate::interner::SrcKeyInterner::intern`]'s docs) — that happens
    /// only once this run settles, in [`Self::reconcile_source_deps`],
    /// which is the one place that actually knows whether a given identity
    /// is new to `caller` or one it already held.
    pub(crate) fn record_source_deps(&self, caller: &CompKey, raw: HashSet<RawDep>) {
        if raw.is_empty() {
            return;
        }
        let interned: HashSet<SrcDep> = self.timed(crate::lock_stats::LockSite::RecordSourceDepsIntern, || {
            let mut interner = self.src_key_interner.lock().unwrap();
            // `into_iter` (not `iter`) so `dep.ver`'s `Vec<u8>` moves straight
            // into `SmallVec::from_vec` instead of being cloned -- `raw` is
            // owned by this call and unused afterward, so there is nothing to
            // preserve it for.
            raw.into_iter()
                .map(|dep| {
                    let key_id = interner.intern(&dep.source, &dep.key);
                    SrcDep { key_id, ver: SmallVerBytes::from_vec(dep.ver) }
                })
                .collect()
        });
        let caller_r = self.timed(crate::lock_stats::LockSite::RecordSourceDepsNodes, || {
            let mut nodes = self.nodes.lock().unwrap();
            let r = nodes.id_of(caller);
            if let Some(r) = r {
                nodes.extend_source_deps(r, &interned);
            }
            r
        });
        let Some(caller_r) = caller_r else { return };
        self.timed(crate::lock_stats::LockSite::RecordSourceDepsIndex, || {
            let mut index = self.source_index.lock().unwrap();
            for dep in interned {
                match index.entry(dep.key_id) {
                    Entry::Vacant(v) => {
                        v.insert(SourceRefs::One(caller_r));
                    }
                    Entry::Occupied(mut occ) => occ.get_mut().insert(caller_r),
                }
            }
        });
    }

    pub(crate) fn record_outputs(&self, caller: &CompKey, raw: HashSet<RawOutput>) {
        if raw.is_empty() {
            return;
        }
        self.timed(crate::lock_stats::LockSite::RecordOutputs, || {
            let mut nodes = self.nodes.lock().unwrap();
            if let Some(r) = nodes.id_of(caller) {
                nodes.extend_outputs(r, &raw);
            }
        });
    }
}

fn downcast_value<R: CompResult>(v: Arc<dyn Any + Send + Sync>, key: &CompKey) -> Result<R, CompError> {
    v.downcast::<R>().map(|arc| (*arc).clone()).map_err(|_| {
        CompError::Failed(format!(
            "computation {key:?}: cached value type mismatch (registered under a conflicting type)"
        ))
    })
}

/// Builds a one-off [`BodyFn`] that wraps a flow-argument definition's
/// [`FlowThunk`] for exactly one call to [`EngineInner::run`] (Stage 9 — see
/// `docs/persistence-benchmark-notes.md`). This is what lets `run` stay
/// completely unaware of flows: from its point of view this is just an
/// ordinary `BodyFn<P, R>`, identical in shape to what the builder path
/// passes (`def.body.clone()`), except it ignores the `_param: P` argument
/// `run` hands it (already folded into `param_bytes`, captured below) and
/// instead resolves `thunk`'s flows fresh, from the registry, on every call
/// — never from a stored handle. `flows`/`param_bytes` are cloned once per
/// `Run` (never per cache hit or single-flight join, since only
/// [`EngineInner::eval_flows_core`]'s `Action::Run` arm ever calls this).
fn build_flow_body<P: CompParam, R: CompResult>(
    thunk: FlowThunk,
    flows: &[FlowId],
    param_bytes: Vec<u8>,
    key: CompKey,
) -> BodyFn<P, R> {
    let flows: Arc<[FlowId]> = Arc::from(flows.to_vec());
    Arc::new(move |ctx: Ctx, _param: P| {
        let flows = flows.clone();
        let param_bytes = param_bytes.clone();
        let key = key.clone();
        Box::pin(async move {
            let engine = ctx.engine.clone();
            let resolver = FlowResolver::new(&engine.registry, &flows);
            let value_any = (thunk)(ctx, resolver, &param_bytes).await?;
            downcast_value::<R>(value_any, &key)
        })
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

    /// The flow-argument counterpart of [`Self::eval_root`] (Stage 9 — see
    /// `docs/persistence-benchmark-notes.md`): evaluates the flow-argument
    /// computation named `name`, applied to `flows`/`param`, as a root
    /// application — what a macro-generated top-level call (outside any
    /// other computation) would use, and what this crate's own hand-written
    /// flow tests use directly. See [`crate::ctx::Ctx::eval_flows`] for the
    /// nested-call counterpart (a computation calling another
    /// flow-argument computation).
    pub async fn eval_root_flows<P: CompParam, R: CompResult>(
        &self,
        name: &'static str,
        flows: &[FlowId],
        param: P,
    ) -> Result<R, CompError> {
        let ctx = Ctx {
            engine: self.inner.clone(),
            caller: None,
            chain: Arc::new(Vec::new()),
        };
        ctx.eval_flows(name, flows, param).await
    }

    /// Whether `COMPUTATIONS_LOCK_STATS` was read as enabled when this
    /// engine was built (see [`EngineBuilder::build`] and
    /// `crate::lock_stats`'s module docs). Lets a caller (typically a
    /// benchmark) decide whether it's worth calling [`Self::print_lock_stats`]
    /// at all.
    pub fn lock_stats_enabled(&self) -> bool {
        self.inner.lock_stats_enabled
    }

    /// Prints the per-call-site lock-hold-time breakdown collected so far
    /// (see `crate::lock_stats`) to stdout, sorted by total hold time
    /// descending. A no-op -- prints nothing -- if `COMPUTATIONS_LOCK_STATS`
    /// wasn't enabled at build time. Safe to call at any point in the
    /// engine's lifetime, including mid-run; a benchmark typically calls
    /// this once, near the end of a phase or its own run, as its "engine
    /// shutdown" report (mirroring the Haskell reference engine's
    /// `COMP_ENGINE_LOCK_STATS`, printed once at engine close).
    pub fn print_lock_stats(&self) {
        if self.inner.lock_stats_enabled {
            print!("{}", self.inner.lock_stats.report());
        }
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

    /// Registers a flow-argument computation named `name`, whose body is
    /// `thunk` (Stage 9 — see `docs/persistence-benchmark-notes.md`'s design
    /// rationale in full, and [`crate::flow`]'s module docs for what this
    /// makes possible: a computation taking source/sink arguments directly,
    /// with no builder-captured closure).
    ///
    /// Unlike [`Self::register`]/[`Self::define`], this returns no handle:
    /// a flow-argument computation is called by name, through
    /// [`crate::ctx::Ctx::eval_flows`]/[`crate::engine::Engine::eval_root_flows`],
    /// not through a `Comp<P, R>` — there is no single `P` to attach to a
    /// handle in the first place (see [`crate::flow::FlowCompDef`]'s docs).
    /// This is also deliberately explicit, plain-value registration data
    /// rather than anything closure-shaped: Phase B's `#[computation]` macro
    /// is expected to collect `(name, thunk)` pairs via `inventory::submit!`
    /// and call this once per collected pair, so keeping `thunk` a bare
    /// [`FlowThunk`] `fn` pointer here is what makes that later step
    /// possible at all.
    ///
    /// `R` must be given explicitly at the call site (e.g.
    /// `builder.define_flows::<Result<(), CompError>>(name, thunk)`) since
    /// nothing else here can infer it.
    ///
    /// # Panics
    /// Panics if a computation (flow-argument or builder-path alike) with
    /// the same name is already registered — this is a startup
    /// configuration error, not a runtime condition (see [`Self::register`]).
    pub fn define_flows<R: CompResult>(&mut self, name: &'static str, thunk: FlowThunk) -> &mut Self {
        let id = DefId::new(name);
        let def = Arc::new(FlowCompDef::<R>::new(id, thunk));
        let prev = self.defs.insert(id, def.clone() as Arc<dyn Any + Send + Sync>);
        assert!(prev.is_none(), "duplicate computation name: {id}");
        self.erased_defs.insert(id, Arc::new(FlowDefAdapter(def)) as Arc<dyn ErasedDef>);
        self.def_names.insert(id.name().to_string(), id);
        self.def_order.push(id);
        self
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
    /// Composes freely with [`EngineBuilder::registry`] in any order — see
    /// that method's docs.
    pub fn source<S: SourceBase>(&mut self, src: Arc<S>) -> &mut Self {
        self.registry.register_source(src);
        self
    }

    /// Registers a sink instance directly on the builder, without having to
    /// construct a [`Registry`] by hand first.
    ///
    /// Equivalent to (and implemented via) [`Registry::register_sink`] on
    /// the builder's internal registry; see that method for panic behavior.
    /// Composes freely with [`EngineBuilder::registry`] in any order — see
    /// that method's docs.
    pub fn sink<S: SinkBase>(&mut self, sink: Arc<S>) -> &mut Self {
        self.registry.register_sink(sink);
        self
    }

    /// Merges `registry`'s sources/sinks into the builder's own registry
    /// (via [`Registry::merge`]), on top of anything already registered via
    /// [`EngineBuilder::source`]/[`EngineBuilder::sink`]/an earlier call to
    /// this same method. Call order across `source`/`sink`/`registry` never
    /// matters: every registration this builder ever sees ends up attached,
    /// regardless of which of the three methods added it or in what order.
    ///
    /// An engine built without ever calling `registry`, `source`, or `sink`
    /// has an empty registry, which is fine for tests that never write to a
    /// real sink.
    ///
    /// This used to *replace* the builder's registry outright, which meant a
    /// `registry(...)` call silently discarded any `source`/`sink`
    /// registrations that preceded it — a real hazard: `Ctx::src_req`/
    /// `sink_req` execute directly against the caller's `Arc` regardless of
    /// the registry, so a dropped registration produced *correct* results
    /// while the driver simply never learned to watch that source or GC that
    /// sink's outputs, both silently. Merging instead makes call order
    /// irrelevant.
    ///
    /// # Panics
    /// Panics if `registry` registers a source or sink id already present in
    /// the builder's registry — see [`Registry::merge`].
    pub fn registry(&mut self, registry: Registry) -> &mut Self {
        self.registry.merge(registry);
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
    /// Before doing anything else, this also consumes every `#[computation]`
    /// function's automatic registration (Phase B — see
    /// `docs/persistence-benchmark-notes.md`'s Stage 10, and
    /// [`crate::flow::ComputationEntry`]'s docs for the `inventory`-based
    /// collection mechanism and its platform caveats), so a
    /// `#[computation]`-defined computation is available with no explicit
    /// `define*`/`register` call at all. Gated behind this crate's `macros`
    /// feature, a no-op with it disabled.
    ///
    /// # Panics
    /// - Panics if more than `u16::MAX + 1` computations were registered —
    ///   the capacity of the internal [`DefIndex`] the node table's lookup
    ///   index uses (see [`DefIndex`]'s docs). Not a realistic limit for any
    ///   hand-registered graph of definitions.
    /// - Panics if a `#[computation]` function's name collides with another
    ///   `#[computation]` function's name, or with a name already
    ///   registered directly on this builder (`define`/`define_flows`/
    ///   `register`/...) before `build()` was called — naming the
    ///   colliding name and which two registration paths produced it, the
    ///   same "duplicate is a startup configuration error" stance every
    ///   other registration method in this crate already takes.
    // `mut` is only needed by the `#[cfg(feature = "macros")]` block below
    // (each collected registration is applied via `&mut self`); with that
    // feature disabled the binding is never mutated, hence the conditional
    // `allow` rather than an unconditional one that would mask a genuine
    // future unused-mut regression on the "macros"-enabled build.
    #[cfg_attr(not(feature = "macros"), allow(unused_mut))]
    pub fn build(mut self) -> Engine {
        #[cfg(feature = "macros")]
        {
            let mut macro_registered: HashSet<&'static str> = HashSet::new();
            for entry in inventory::iter::<crate::flow::ComputationEntry> {
                if macro_registered.contains(entry.name) {
                    panic!(
                        "duplicate computation name `{}`: registered by more than one #[computation] function \
                         (two functions producing the same `concat!(module_path!(), \"::\", ...)` name) -- \
                         rename one of them",
                        entry.name
                    );
                }
                if self.def_names.contains_key(entry.name) {
                    panic!(
                        "duplicate computation name `{}`: registered both by a #[computation] function and by \
                         an explicit EngineBuilder registration (define/define_flows/register/define_with/...) \
                         made before build() -- rename one of them, or remove the explicit registration",
                        entry.name
                    );
                }
                macro_registered.insert(entry.name);
                (entry.register)(&mut self);
            }
        }

        let (dirty_tx, dirty_rx) = mpsc::unbounded_channel();
        assert!(
            self.def_order.len() <= u16::MAX as usize + 1,
            "engine has {} registered computations, exceeding the u16::MAX+1 DefIndex capacity",
            self.def_order.len()
        );
        // Read once, here, rather than per critical section: `EngineInner::timed`
        // is on the hottest path in the engine (every `nodes`/`source_index`
        // critical section, millions of times at 1M-instance scale), so the
        // enabled/disabled decision has to be made once, at build time, and
        // baked into this plain `bool` -- not re-checked on every call. See
        // `crate::lock_stats`'s module docs (this mirrors the Haskell
        // reference engine's `COMP_ENGINE_LOCK_STATS` design point exactly).
        let lock_stats_enabled = match std::env::var_os("COMPUTATIONS_LOCK_STATS") {
            None => false,
            Some(v) => !v.is_empty() && v != "0",
        };
        Engine {
            inner: Arc::new(EngineInner {
                defs: Mutex::new(self.defs),
                nodes: Mutex::new(NodeTable::new(self.def_order)),
                source_index: Mutex::new(HashMap::default()),
                src_key_interner: Mutex::new(SrcKeyInterner::new()),
                roots: Mutex::new(CompKeySet::default()),
                registry: self.registry,
                dirty_tx,
                dirty_rx: AsyncMutex::new(dirty_rx),
                erased_defs: self.erased_defs,
                def_names: self.def_names,
                persist_opts: self.persist_opts,
                persist: Mutex::new(None),
                lock_stats_enabled,
                lock_stats: crate::lock_stats::LockStats::new(),
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

    /// Fix 3(b)'s debug-only round-trip check must actually trip for a
    /// `HashMap` parameter — the case it exists to catch. Not a naive
    /// "serialize the same value twice" (see `debug_check_param_determinism`'s
    /// docs on why that would never catch anything): this builds one
    /// `HashMap`, serializes it once, decodes that back into a *fresh*
    /// `HashMap` (a new instance, with its own random hasher seed — exactly
    /// what `crate::def::ErasedDef::revive_key` does at persisted-load
    /// time), and calls the checker with the original bytes. Verified during
    /// development to reproduce reliably (10/10 trials) for an 8-entry
    /// `HashMap<i32, i32>` — `std`'s default `RandomState` draws fresh keys
    /// per instance, so two differently-seeded maps holding the same
    /// entries almost never share a bucket layout.
    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "re-serializes to different bytes")]
    fn debug_check_param_determinism_panics_on_a_hashmap_param() {
        let mut map: HashMap<i32, i32> = HashMap::new();
        for i in 0..8 {
            map.insert(i, i * 10);
        }
        let param_bytes = postcard::to_stdvec(&map).expect("serializes fine");
        let key = CompKey::new(DefId::new("det_check_probe"), &map);
        debug_check_param_determinism::<HashMap<i32, i32>>(&key, &param_bytes);
    }

    /// The same check must be a silent no-op for a deterministic parameter
    /// type (a `BTreeMap`, the documented fix for the `HashMap` case above) —
    /// proof this isn't a check that just always fires.
    #[cfg(debug_assertions)]
    #[test]
    fn debug_check_param_determinism_accepts_a_btreemap_param() {
        use std::collections::BTreeMap;
        let mut map: BTreeMap<i32, i32> = BTreeMap::new();
        for i in 0..8 {
            map.insert(i, i * 10);
        }
        let param_bytes = postcard::to_stdvec(&map).expect("serializes fine");
        let key = CompKey::new(DefId::new("det_check_probe"), &map);
        debug_check_param_determinism::<BTreeMap<i32, i32>>(&key, &param_bytes);
    }

    /// Fix 4: an oversized param (over this engine's `u16::MAX`-byte
    /// per-node arena span) must return a `CompError`, never panic — and,
    /// critically, the engine must keep working afterward. Before the fix,
    /// this panicked inside `DefTable::insert` while `EngineInner::nodes`
    /// was locked, poisoning that `std::sync::Mutex` for the rest of the
    /// process; the real assertion here is the *second* `eval_root`
    /// succeeding, not just the first one erroring.
    #[tokio::test]
    async fn oversized_param_errors_without_poisoning_the_engine() {
        let mut builder = Engine::builder();
        let big: Comp<Vec<u8>, usize> =
            builder.define("oversized_param_probe", |_ctx, p: Vec<u8>| async move { Ok(p.len()) });
        let followup: Comp<(), ()> = builder.define("oversized_param_followup", |_ctx, _: ()| async move { Ok(()) });
        let engine = builder.build();

        let oversized = vec![0u8; 100_000]; // postcard-encodes past u16::MAX
        let result = engine.eval_root(&big, oversized).await;
        assert!(
            matches!(result, Err(CompError::Failed(_))),
            "expected a Failed error for an oversized param, got {result:?}"
        );

        let ok = engine.eval_root(&followup, ()).await;
        assert!(
            ok.is_ok(),
            "the node-table mutex must not be poisoned by an oversized-param error: {ok:?}"
        );
    }

    /// A param type whose `Serialize` impl unconditionally fails must also
    /// return a `CompError` rather than panicking (this one never even
    /// reaches `prepare`/the node-table lock — it fails inside `eval`'s own
    /// up-front serialization), and the engine must likewise keep working
    /// afterward.
    #[derive(Debug, Clone)]
    struct UnserializableParam;

    impl serde::Serialize for UnserializableParam {
        fn serialize<S: serde::Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom("deliberately broken Serialize for testing"))
        }
    }

    impl<'de> serde::Deserialize<'de> for UnserializableParam {
        fn deserialize<D: serde::Deserializer<'de>>(_deserializer: D) -> Result<Self, D::Error> {
            Ok(UnserializableParam)
        }
    }

    #[tokio::test]
    async fn failing_param_serialize_errors_without_poisoning_the_engine() {
        let mut builder = Engine::builder();
        let broken: Comp<UnserializableParam, ()> =
            builder.define("broken_param_probe", |_ctx, _: UnserializableParam| async move { Ok(()) });
        let followup: Comp<(), ()> = builder.define("broken_param_followup", |_ctx, _: ()| async move { Ok(()) });
        let engine = builder.build();

        let result = engine.eval_root(&broken, UnserializableParam).await;
        assert!(
            matches!(result, Err(CompError::Failed(_))),
            "expected a Failed error for a param that fails to serialize, got {result:?}"
        );

        let ok = engine.eval_root(&followup, ()).await;
        assert!(ok.is_ok(), "the engine must keep working after a param-serialize failure: {ok:?}");
    }

    /// Fix 5: a result type whose `Serialize` impl fails must fail just that
    /// node (`CompError`, stays dirty) rather than panicking inside the
    /// shared execution future — a panic there used to propagate through
    /// `Shared`/`join_all` all the way up into whatever task ran
    /// `Engine::run`. Exercised here directly via `eval_root` (the
    /// `Engine::run`-level, driver-survives version of this same fix lives
    /// in `tests/driver.rs`).
    #[derive(Debug, Clone, Default)]
    struct UnserializableResult;

    impl serde::Serialize for UnserializableResult {
        fn serialize<S: serde::Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom("deliberately broken Serialize for testing"))
        }
    }

    impl<'de> serde::Deserialize<'de> for UnserializableResult {
        fn deserialize<D: serde::Deserializer<'de>>(_deserializer: D) -> Result<Self, D::Error> {
            Ok(UnserializableResult)
        }
    }

    #[tokio::test]
    async fn failing_result_serialize_errors_without_panicking() {
        let mut builder = Engine::builder();
        let broken: Comp<(), UnserializableResult> =
            builder.define("broken_result_probe", |_ctx, _: ()| async move { Ok(UnserializableResult) });
        let followup: Comp<(), ()> = builder.define("broken_result_followup", |_ctx, _: ()| async move { Ok(()) });
        let engine = builder.build();

        let result = engine.eval_root(&broken, ()).await;
        assert!(
            matches!(result, Err(CompError::Failed(_))),
            "expected a Failed error for a result that fails to serialize, got {result:?}"
        );

        let ok = engine.eval_root(&followup, ()).await;
        assert!(ok.is_ok(), "the engine must keep working after a result-serialize failure: {ok:?}");
    }
}
