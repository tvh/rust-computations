//! The incremental engine: the dynamic dependency graph and change
//! propagation.
//!
//! [`Engine`] owns the def table (registered [`CompDef`]s, looked up by
//! [`DefId`]) and the node table (one [`Node`] per evaluated [`CompKey`],
//! i.e. per computation application). Evaluating a computation walks the
//! same algorithm whether it is a root call ([`Engine::eval_root`]) or a
//! nested call ([`crate::ctx::Ctx::eval`]):
//!
//! 1. A clean, cached node is returned immediately (memoization).
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
//! keeps around (see [`Node`]'s docs for why there is no such closure).

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
use crate::key::{CompKey, CompParam, CompResult, DefId, Hash128};
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

/// The identity of a node's slot in [`EngineInner`]'s slab (`NodeTable`).
///
/// Unlike [`CompKey`] (a computation's stable, content-addressed identity),
/// a `NodeId` is only a cheap (4-byte) local handle into the *current*
/// process's in-memory node table: it is never persisted, never compared
/// across restarts, and is only valid until the node it names is collected
/// by [`crate::driver`]'s liveness GC — at which point its slot may be
/// reused for a completely unrelated node. Every long-lived reference to a
/// computation (a persisted record, `roots`) is keyed by `CompKey` instead,
/// precisely so it can never be invalidated by GC reusing a `NodeId`;
/// `NodeId` is used purely as an edge (`comp_deps`, `rdeps`, `source_index`)
/// representation, and as the key of the node table's sparse side tables
/// (`source_deps`/`outputs`/`inflight` — see [`NodeTable`]), inside a single
/// process's live node table, translated back to a `CompKey` (via
/// [`Node::key`]) whenever an edge needs to leave that table (persistence,
/// driver-level dirty bookkeeping).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct NodeId(u32);

/// A compact index into the engine's def table (see [`NodeTable`]),
/// assigned in registration order by [`EngineBuilder::build`].
///
/// This exists purely to shrink [`NodeTable`]'s `CompKey`-keyed index: a
/// `(DefIndex, Hash128)` pair is 18 bytes versus a full [`CompKey`]'s 32
/// (a `DefId` is a 16-byte fat pointer; a `DefIndex` is 2). It never
/// replaces `CompKey`/`DefId` anywhere in the public API, on disk, or on
/// [`Node`] itself (which still keeps its own full `CompKey` — see
/// [`Node::key`] — so comp names stay trivially loggable everywhere without
/// needing to thread a def-table lookup through call sites that only ever
/// had a `Node` in hand); it is strictly an internal detail of how
/// [`NodeTable`] indexes its slab.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct DefIndex(u16);

/// The small set of mutually-exclusive/boolean bits that used to be three
/// separate [`Node`] fields (`state`, `dirty_priority`, `last_changed`),
/// packed into one byte.
///
/// `state` takes 2 bits (3 values), `dirty_priority` takes 2 bits (`None`
/// plus 2 values), `last_changed` takes 1 bit — 5 bits total, comfortably
/// inside a `u8`. Accessed only through [`Node`]'s own `state`/`set_state`/
/// `dirty_priority`/`set_dirty_priority`/`last_changed`/`set_last_changed`
/// methods, which read exactly like the plain fields they replaced at every
/// call site.
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
}

/// One computation application's memoized state.
///
/// Deliberately holds no closure of any kind (see [`EngineInner::rerun_node`]
/// for how the driver re-runs a dirtied node instead): an earlier design
/// gave every node a boxed `rerun: RerunFn` closure capturing a permanent
/// `Arc<EngineInner>` plus its typed, owned parameter, which meant an
/// `Engine` was never actually freed even once every external handle to it
/// was dropped (any node that had ever executed kept the whole engine alive
/// via that closure's self-reference). Re-deriving the execution future on
/// demand from `(def, param_bytes)` — the same mechanism persistence's
/// revival path already used — removes that cycle entirely, at the cost of
/// one postcard param decode per rerun (see `EngineInner::rerun_node`),
/// negligible next to a node's actual body cost.
///
/// Two other collections that used to live directly on every `Node` —
/// `source_deps`/`outputs` (a `HashSet` each, reserved even for the many
/// nodes that have none) and `inflight` (relevant only to a node currently
/// running) — now live in sparse side tables on [`NodeTable`], keyed by
/// [`NodeId`], present only for nodes that actually have an entry.
pub(crate) struct Node {
    flags: NodeFlags,
    /// This node's own stable identity — the `NodeTable`'s index entry,
    /// duplicated here so a `NodeId` can be translated back to its
    /// `CompKey` in O(1) (needed by GC, persistence snapshotting, and the
    /// driver's rdeps-to-dirty-keys translation) without a second, reverse
    /// index. Deliberately kept as a full `CompKey` (not the compact
    /// `(DefIndex, Hash128)` pair `NodeTable`'s own index uses internally —
    /// see [`DefIndex`]'s docs) so every one of those call sites keeps
    /// working, and comp names stay trivially loggable, with no def-table
    /// lookup required.
    pub(crate) key: CompKey,
    /// The cached result, erased. `Some` even when `state() == Dirty` (a
    /// stale-but-not-yet-superseded value), `None` only before the first
    /// successful execution.
    ///
    /// Deliberately *not* accompanied by a pre-serialized `value_bytes`
    /// field: `crate::persist` re-derives postcard bytes lazily, from this
    /// `Arc`, only for a node actually about to be flushed (see
    /// `crate::def::ErasedDef::serialize_value`), rather than every node
    /// permanently carrying a redundant serialized copy of its own value in
    /// memory.
    pub(crate) value: Option<Arc<dyn Any + Send + Sync>>,
    pub(crate) result_hash: Option<Hash128>,
    /// Postcard-encoded bytes of this node's parameter, computed once when
    /// the node is first created (a `CompKey`'s param never changes across
    /// reruns of that same key). Read by `crate::persist` to save a node
    /// without needing to know its concrete parameter type, and by
    /// `EngineInner::rerun_node` to decode the typed param again on every
    /// rerun (see [`Node`]'s docs on why there is no cached rerun closure).
    ///
    /// Kept eagerly serialized (unlike `value`) rather than lazily derived
    /// from a stored erased param: a param is written exactly once per node
    /// (never re-serialized on rerun, so it never duplicates work the way an
    /// eager `value_bytes` used to on every changed rerun) and is typically
    /// far smaller than a result value (a single hashable lookup key vs. a
    /// potentially large aggregated/computed value) — storing it erased
    /// (`Arc<dyn Any>` plus a deferred serializer call) would cost at least
    /// as much memory as the bytes themselves, for no reruns-avoided upside.
    pub(crate) param_bytes: Vec<u8>,
    /// Computations this node called during its last execution, by
    /// [`NodeId`] rather than the full [`CompKey`] each edge would otherwise
    /// have to repeat.
    ///
    /// A `SmallVec` rather than a `HashSet`: fan-out is small in practice, so
    /// a linear dedup-on-insert scan (see `EngineInner::record_call_dep`) is
    /// cheaper in both time and space than a hash table's per-entry
    /// overhead at this size — a deliberate small-set tradeoff, not an
    /// oversight.
    pub(crate) comp_deps: SmallVec<[NodeId; 4]>,
    /// Computations that called this node during their last execution, by
    /// [`NodeId`] (see `comp_deps`'s docs for why `SmallVec` and why by id).
    pub(crate) rdeps: SmallVec<[NodeId; 4]>,
}

impl Node {
    pub(crate) fn state(&self) -> NodeState {
        self.flags.state()
    }

    pub(crate) fn set_state(&mut self, state: NodeState) {
        self.flags.set_state(state);
    }

    pub(crate) fn dirty_priority(&self) -> Option<DirtyPriority> {
        self.flags.dirty_priority()
    }

    pub(crate) fn set_dirty_priority(&mut self, priority: Option<DirtyPriority>) {
        self.flags.set_dirty_priority(priority);
    }

    pub(crate) fn last_changed(&self) -> bool {
        self.flags.last_changed()
    }

    pub(crate) fn set_last_changed(&mut self, changed: bool) {
        self.flags.set_last_changed(changed);
    }

    /// Constructs a `Node` directly in the `Clean` state from a persisted
    /// record's revived pieces (see `crate::persist`), without ever having
    /// executed the computation in this process.
    ///
    /// `comp_deps`/`rdeps` both start empty: `crate::persist`'s loader wires
    /// them up itself afterward, once every record in the batch has a
    /// [`NodeId`] (a dependency edge can only be expressed once both of its
    /// endpoints are already in the table), mirroring how a live node's
    /// edges are built incrementally by `EngineInner::record_call_dep` as it
    /// actually runs. Likewise, this node's `source_deps`/`outputs` are not
    /// set here: `crate::persist::restore_nodes` populates `NodeTable`'s
    /// side tables for them directly, once this node has a [`NodeId`].
    pub(crate) fn from_persisted(key: CompKey, revived: RevivedNode) -> Node {
        Node {
            flags: NodeFlags::new(NodeState::Clean),
            key,
            value: Some(revived.value),
            result_hash: Some(revived.result_hash),
            param_bytes: revived.param_bytes,
            comp_deps: SmallVec::new(),
            rdeps: SmallVec::new(),
        }
    }
}

/// Every piece [`Node::from_persisted`] needs to revive a `Clean` node from
/// a persisted record, bundled into one struct purely to keep that
/// constructor's argument list manageable (see `crate::persist`, which
/// builds one of these per decodable, still-registered record). Excludes
/// `comp_deps` (and the node's own `CompKey`, passed to `from_persisted`
/// separately) for the same reason `crate::persist::restore_nodes` also
/// keeps `source_deps`/`outputs` out of this struct: all three need either
/// every record in the batch to already have a [`NodeId`] (`comp_deps`) or
/// this node's own not-yet-assigned [`NodeId`] (`source_deps`/`outputs`,
/// which land in `NodeTable`'s side tables, not on `Node` itself) before
/// they can be resolved — `restore_nodes` handles all of that itself, after
/// every record has been turned into a `Node`.
pub(crate) struct RevivedNode {
    pub(crate) param_bytes: Vec<u8>,
    pub(crate) value: Arc<dyn Any + Send + Sync>,
    pub(crate) result_hash: Hash128,
}

/// The engine's node table: a slab (`Vec<Option<Node>>` plus a free list) of
/// every live node, addressable either by its stable [`CompKey`] (via an
/// index) or by its process-local [`NodeId`] (a direct slab slot).
///
/// This is the in-memory analogue of what used to be a plain
/// `HashMap<CompKey, Node>`: most of the methods that existed on that map
/// (`get`/`get_mut`/`keys`/`values_mut`) are preserved here with the same
/// `CompKey`-keyed signature, so most call sites needed no changes at all
/// when this replaced it — only code that stores or walks *edges*
/// (`comp_deps`/`rdeps`/`source_index`) needed to switch to the `NodeId`-based
/// methods (`get_by_id`/`get_mut_by_id`/`id_of`) to get the point of this
/// type: an edge only needs 4 bytes (`NodeId`), not 32 (`CompKey`).
///
/// Two independent memory-diet moves live here rather than on `Node` itself:
///
/// - The `CompKey`-keyed lookup index is keyed on `(DefIndex, Hash128)`
///   internally (18 bytes) rather than the full `CompKey` (32 bytes) — see
///   [`DefIndex`]'s docs. `def_index` is this table's own private copy of
///   "`DefId` -> `DefIndex`", built once (from registration order) by
///   [`EngineBuilder::build`] and never mutated afterward.
/// - `source_deps`/`outputs`/`inflight` — fields that used to live directly
///   on every `Node` even though most nodes have zero source deps/outputs
///   and are essentially never `inflight` at rest — are sparse side maps
///   here instead, keyed by [`NodeId`], with an entry only for a node that
///   actually has one. [`Self::remove_by_id`] purges all three whenever a
///   node is collected, so a GC'd node never leaves an orphaned side-table
///   entry behind.
pub(crate) struct NodeTable {
    slots: Vec<Option<Node>>,
    free: Vec<u32>,
    index: HashMap<(DefIndex, Hash128), NodeId>,
    def_index: HashMap<DefId, DefIndex>,
    source_deps: HashMap<NodeId, HashSet<RawDep>>,
    outputs: HashMap<NodeId, HashSet<RawOutput>>,
    inflight: HashMap<NodeId, SharedExec>,
}

impl NodeTable {
    fn new(def_index: HashMap<DefId, DefIndex>) -> Self {
        NodeTable {
            slots: Vec::new(),
            free: Vec::new(),
            index: HashMap::new(),
            def_index,
            source_deps: HashMap::new(),
            outputs: HashMap::new(),
            inflight: HashMap::new(),
        }
    }

    /// Translates a `CompKey` into this table's compact internal index key,
    /// `None` if `key`'s definition isn't registered on this engine (can't
    /// happen for any `CompKey` that legitimately reached this table, since
    /// every one is built from a registered `DefId` — see [`DefIndex`]'s
    /// docs — but returning `Option` here rather than panicking keeps every
    /// caller's existing "no node for this key" handling exactly as it was).
    fn index_key(&self, key: &CompKey) -> Option<(DefIndex, Hash128)> {
        Some((*self.def_index.get(key.def())?, key.param_hash()))
    }

    pub(crate) fn id_of(&self, key: &CompKey) -> Option<NodeId> {
        let ik = self.index_key(key)?;
        self.index.get(&ik).copied()
    }

    pub(crate) fn get(&self, key: &CompKey) -> Option<&Node> {
        let id = self.id_of(key)?;
        self.slots[id.0 as usize].as_ref()
    }

    pub(crate) fn get_mut(&mut self, key: &CompKey) -> Option<&mut Node> {
        let id = self.id_of(key)?;
        self.slots[id.0 as usize].as_mut()
    }

    pub(crate) fn get_by_id(&self, id: NodeId) -> Option<&Node> {
        self.slots.get(id.0 as usize)?.as_ref()
    }

    pub(crate) fn get_mut_by_id(&mut self, id: NodeId) -> Option<&mut Node> {
        self.slots.get_mut(id.0 as usize)?.as_mut()
    }

    /// Inserts a brand-new node under `key` (which must equal `node.key`;
    /// enforced here rather than trusted, since a caller mismatch would
    /// otherwise silently corrupt the id<->key mapping), returning its
    /// freshly assigned `NodeId`. Reuses a GC-freed slab slot if one is
    /// available (see `Self::remove_by_id`), otherwise grows the slab.
    ///
    /// Callers must ensure `key` is not already present — [`Self::get_or_insert_with`]
    /// is the coalescing variant used by the ordinary "cache miss, create a
    /// node" path.
    pub(crate) fn insert_new(&mut self, key: CompKey, mut node: Node) -> NodeId {
        debug_assert_eq!(node.key, key, "Node::key must match the key it is inserted under");
        node.key = key.clone();
        let id = match self.free.pop() {
            Some(slot) => {
                self.slots[slot as usize] = Some(node);
                NodeId(slot)
            }
            None => {
                let idx = self.slots.len() as u32;
                self.slots.push(Some(node));
                NodeId(idx)
            }
        };
        let ik = self
            .index_key(&key)
            .expect("insert_new: key's definition must be registered in this engine's def table");
        self.index.insert(ik, id);
        id
    }

    /// Returns the existing node for `key` if present, otherwise builds one
    /// via `make` and inserts it — the id/node pair either way. `make`'s
    /// result's `key` field is overwritten with `key` regardless of what it
    /// sets, so callers may leave it at any placeholder value.
    pub(crate) fn get_or_insert_with(&mut self, key: &CompKey, make: impl FnOnce() -> Node) -> (NodeId, &mut Node) {
        if let Some(id) = self.id_of(key) {
            (id, self.slots[id.0 as usize].as_mut().expect("slab slot present for an indexed key"))
        } else {
            let id = self.insert_new(key.clone(), make());
            (id, self.slots[id.0 as usize].as_mut().expect("just inserted"))
        }
    }

    /// Removes and returns the node at `id` (if any), freeing its slab slot
    /// for reuse by a future [`Self::insert_new`]/[`Self::get_or_insert_with`]
    /// call — which is exactly why a [`NodeId`] must never be treated as
    /// stable across a GC pass (see [`NodeId`]'s docs): the slot this
    /// returns can be handed out again, for an entirely unrelated `CompKey`,
    /// the very next time a node is created.
    ///
    /// Also purges every sparse side-table entry (`source_deps`/`outputs`/
    /// `inflight`) for `id`, if any, so a collected node never leaves one
    /// behind — the single place that guarantees this regardless of which
    /// caller triggered the removal.
    pub(crate) fn remove_by_id(&mut self, id: NodeId) -> Option<Node> {
        let node = self.slots.get_mut(id.0 as usize)?.take()?;
        if let Some(ik) = self.index_key(&node.key) {
            self.index.remove(&ik);
        }
        self.source_deps.remove(&id);
        self.outputs.remove(&id);
        self.inflight.remove(&id);
        self.free.push(id.0);
        Some(node)
    }

    pub(crate) fn keys(&self) -> impl Iterator<Item = &CompKey> {
        self.slots.iter().filter_map(|s| s.as_ref().map(|n| &n.key))
    }

    pub(crate) fn values_mut(&mut self) -> impl Iterator<Item = &mut Node> {
        self.slots.iter_mut().filter_map(|s| s.as_mut())
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (NodeId, &Node)> {
        self.slots.iter().enumerate().filter_map(|(i, s)| s.as_ref().map(|n| (NodeId(i as u32), n)))
    }

    // -- sparse side tables (`source_deps`/`outputs`/`inflight`) --

    pub(crate) fn source_deps_iter(&self, id: NodeId) -> impl Iterator<Item = &RawDep> {
        self.source_deps.get(&id).into_iter().flatten()
    }

    pub(crate) fn source_deps_contains(&self, id: NodeId, dep: &RawDep) -> bool {
        self.source_deps.get(&id).is_some_and(|deps| deps.contains(dep))
    }

    pub(crate) fn source_deps_clone(&self, id: NodeId) -> HashSet<RawDep> {
        self.source_deps.get(&id).cloned().unwrap_or_default()
    }

    /// Removes and returns `id`'s source deps (empty if it had none),
    /// leaving no entry behind — the side-table equivalent of clearing a
    /// plain field.
    pub(crate) fn take_source_deps(&mut self, id: NodeId) -> HashSet<RawDep> {
        self.source_deps.remove(&id).unwrap_or_default()
    }

    /// Merges `raw` into `id`'s source-dep set, creating the entry if this
    /// is its first one. A no-op for an empty `raw`, so a node that never
    /// reads any source never gets an entry at all.
    pub(crate) fn extend_source_deps(&mut self, id: NodeId, raw: &HashSet<RawDep>) {
        if raw.is_empty() {
            return;
        }
        self.source_deps.entry(id).or_default().extend(raw.iter().cloned());
    }

    pub(crate) fn outputs_iter(&self, id: NodeId) -> impl Iterator<Item = &RawOutput> {
        self.outputs.get(&id).into_iter().flatten()
    }

    pub(crate) fn outputs_clone(&self, id: NodeId) -> HashSet<RawOutput> {
        self.outputs.get(&id).cloned().unwrap_or_default()
    }

    /// Removes and returns `id`'s outputs (empty if it had none) — see
    /// [`Self::take_source_deps`].
    pub(crate) fn take_outputs(&mut self, id: NodeId) -> HashSet<RawOutput> {
        self.outputs.remove(&id).unwrap_or_default()
    }

    /// Merges `raw` into `id`'s output set — see [`Self::extend_source_deps`].
    pub(crate) fn extend_outputs(&mut self, id: NodeId, raw: &HashSet<RawOutput>) {
        if raw.is_empty() {
            return;
        }
        self.outputs.entry(id).or_default().extend(raw.iter().cloned());
    }

    pub(crate) fn inflight_get(&self, id: NodeId) -> Option<SharedExec> {
        self.inflight.get(&id).cloned()
    }

    pub(crate) fn inflight_set(&mut self, id: NodeId, shared: SharedExec) {
        self.inflight.insert(id, shared);
    }

    pub(crate) fn inflight_clear(&mut self, id: NodeId) {
        self.inflight.remove(&id);
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

/// What `prepare` decided to do about an evaluation request.
enum Action {
    CacheHit(Arc<dyn Any + Send + Sync>),
    Join(SharedExec),
    Run(PreRunSnapshot),
}

/// Shared engine state. Always accessed through `Arc<EngineInner>` (see
/// [`Engine`]); the node/def tables use plain `std::sync::Mutex` and are
/// never held locked across an `.await`.
pub(crate) struct EngineInner {
    defs: Mutex<HashMap<DefId, Arc<dyn Any + Send + Sync>>>,
    pub(crate) nodes: Mutex<NodeTable>,
    /// Maintained on every dependency (re-)collection so the driver can map
    /// a changed (source, key) pair back to the computations that read it,
    /// without scanning the whole node table. Stores [`NodeId`]s rather than
    /// full [`CompKey`]s for the same reason `Node::comp_deps`/`rdeps` do —
    /// see [`NodeId`]'s docs.
    pub(crate) source_index: Mutex<HashMap<(SourceId, KeyBytes), HashSet<NodeId>>>,
    /// Root applications (evaluated via `Engine::eval_root`), so the
    /// driver's liveness GC knows which nodes are reachable from outside the
    /// graph and must not be collected even with no `rdeps`.
    pub(crate) roots: Mutex<HashSet<CompKey>>,
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
    /// through any closure a node keeps around — see [`Node`]'s docs for why
    /// there is no such closure anymore, and [`crate::def::ErasedDef::rerun`]
    /// for the object-safe dispatch this needs (the driver, working only
    /// from a `Node`'s `CompKey`/`param_bytes`, has no static `P`/`R` to
    /// call `eval::<P, R>` with directly). This is exactly what persisted
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
    /// fresh run, creating its `Node` on first sight. On the `Run` path this
    /// also resets the node's dynamic dependency/output collections and
    /// marks it `Running`, so the caller must actually run the computation
    /// after this returns.
    fn prepare<P: CompParam>(self: &Arc<Self>, key: &CompKey, param: &P) -> Action {
        let mut nodes = self.nodes.lock().unwrap();

        if let Some(id) = nodes.id_of(key) {
            if let Some(node) = nodes.get_by_id(id)
                && node.state() == NodeState::Clean
                && let Some(v) = &node.value
            {
                return Action::CacheHit(v.clone());
            }
            if let Some(shared) = nodes.inflight_get(id) {
                return Action::Join(shared);
            }
        }

        let (id, node) = nodes.get_or_insert_with(key, || Node {
            flags: NodeFlags::new(NodeState::Dirty),
            key: key.clone(),
            value: None,
            result_hash: None,
            param_bytes: postcard::to_stdvec(param)
                .expect("postcard serialization of a well-formed value should not fail"),
            comp_deps: SmallVec::new(),
            rdeps: SmallVec::new(),
        });

        let old_hash = node.result_hash;
        node.set_state(NodeState::Running);
        node.comp_deps.clear();

        let old_outputs = nodes.take_outputs(id);
        let old_source_deps = nodes.take_source_deps(id);

        Action::Run(PreRunSnapshot {
            old_hash,
            old_outputs,
            old_source_deps,
        })
    }

    /// Actually runs the computation for `key`, building its child `Ctx`,
    /// awaiting the body, updating the node on success or failure, and
    /// reconciling `source_index` / dropped sink outputs.
    async fn run<P: CompParam, R: CompResult>(
        self: &Arc<Self>,
        key: &CompKey,
        def_id: &DefId,
        param: P,
        chain: Arc<Vec<CompKey>>,
        snapshot: PreRunSnapshot,
    ) -> Result<R, CompError> {
        let PreRunSnapshot {
            old_hash,
            old_outputs,
            old_source_deps,
        } = snapshot;
        let def = match self.get_def::<P, R>(def_id) {
            Ok(def) => def,
            Err(e) => {
                // Def lookup failed before we ever started executing: leave
                // the node dirty (not stuck `Running`) so the next attempt
                // retries cleanly.
                let mut nodes = self.nodes.lock().unwrap();
                if let Some(node) = nodes.get_mut(key) {
                    node.set_state(NodeState::Dirty);
                }
                tracing::debug!(outcome = "error", error = %e, "comp.eval finished");
                return Err(e);
            }
        };

        let mut child_chain = (*chain).clone();
        child_chain.push(key.clone());
        let ctx = Ctx {
            engine: self.clone(),
            caller: Some(key.clone()),
            chain: Arc::new(child_chain),
        };

        // `param` is about to be moved into the execution future below, but
        // the "executed" completion event further down wants to render it
        // for diagnostics — never stored on `Node` (see its docs), so this
        // is the one place left with the typed param in scope. Guarding the
        // clone itself behind `tracing::enabled!` means a disabled DEBUG
        // level pays neither the clone nor the eventual `format!`.
        let param_for_trace = if tracing::enabled!(tracing::Level::DEBUG) { Some(param.clone()) } else { None };

        let fut: BoxFuture<'static, ExecResult> = Box::pin(async move {
            let start = Instant::now();
            let result = (def.body)(ctx, param).await?;
            let elapsed = start.elapsed();
            // `value_bytes` here is purely a local scratch encoding for the
            // content hash below (early cutoff) — it is never returned or
            // stored on the node (see `Node::value`'s docs); `crate::persist`
            // re-derives its own copy, lazily, only for a node that actually
            // needs to be flushed.
            let value_bytes = postcard::to_stdvec(&result)
                .expect("postcard serialization of a well-formed value should not fail");
            let hash = Hash128::from_blake3(blake3::hash(&value_bytes));
            let value: Arc<dyn Any + Send + Sync> = Arc::new(result);
            Ok((value, hash, elapsed))
        });
        let shared: SharedExec = fut.shared();

        {
            let mut nodes = self.nodes.lock().unwrap();
            if let Some(id) = nodes.id_of(key) {
                nodes.inflight_set(id, shared.clone());
            }
        }

        let outcome = shared.await;

        match outcome {
            Ok((value_any, hash, elapsed)) => {
                let changed = old_hash != Some(hash);
                let (id, new_source_deps, new_outputs) = {
                    let mut nodes = self.nodes.lock().unwrap();
                    let id = nodes.id_of(key).expect("node present: created by prepare() before run() is called");
                    {
                        let node = nodes.get_mut_by_id(id).expect("node present: just looked up its id above");
                        node.set_state(NodeState::Clean);
                        node.set_dirty_priority(None);
                        node.value = Some(value_any.clone());
                        node.result_hash = Some(hash);
                        node.set_last_changed(changed);
                    }
                    nodes.inflight_clear(id);

                    // Only a genuinely changed result needs a fresh save: a
                    // recomputation that hit early cutoff already has its
                    // (still correct) record on disk. A brand-new node's
                    // first run always counts as changed (`old_hash` was
                    // `None`), so this naturally covers "just created" too,
                    // without a separate case. Snapshotting and enqueuing
                    // here, while `nodes` is still locked, is what makes
                    // `crate::persist`'s background flush race-free: the
                    // record it eventually writes is exactly this node's
                    // state at this instant, never a state some later
                    // (possibly concurrent) rerun has since overwritten. Only
                    // the node's *value* `Arc` is cloned under this lock
                    // (cheap, a refcount bump); the postcard bytes
                    // `crate::persist` actually writes are serialized later,
                    // outside this lock entirely (see
                    // `crate::persist::enqueue_changed`).
                    if changed {
                        crate::persist::enqueue_changed(self, &nodes, key);
                    }

                    let new_source_deps = nodes.source_deps_clone(id);
                    let new_outputs = nodes.outputs_clone(id);
                    (id, new_source_deps, new_outputs)
                };

                self.remove_stale_source_index(id, &old_source_deps, &new_source_deps);

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
                downcast_value::<R>(value_any, key)
            }
            Err(e) => {
                // Errors are not memoized: leave the node `Dirty` (not
                // `Clean`) so the next eval retries instead of reusing the
                // stale (or absent) value.
                let mut nodes = self.nodes.lock().unwrap();
                if let Some(id) = nodes.id_of(key) {
                    if let Some(node) = nodes.get_mut_by_id(id) {
                        node.set_state(NodeState::Dirty);
                    }
                    nodes.inflight_clear(id);
                }
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
    fn remove_stale_source_index(&self, id: NodeId, old: &HashSet<RawDep>, new: &HashSet<RawDep>) {
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
                set.remove(&id);
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

            let action = self.prepare::<P>(&key, &param);

            let value = match action {
                Action::CacheHit(v) => {
                    tracing::debug!(outcome = "cache_hit", "comp.eval finished");
                    downcast_value::<R>(v, &key)?
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
                Action::Run(snapshot) => self.run::<P, R>(&key, &def_id, param, chain, snapshot).await?,
            };

            Ok((value, key))
        }
        .instrument(span)
        .await
    }

    /// Records a `caller -> callee` call edge, deduping on insert: both
    /// `comp_deps`/`rdeps` are small `SmallVec`s (see [`Node`]'s docs), so a
    /// linear "already present?" scan before pushing is the deliberately
    /// cheap choice here, not an oversight — fan-in/fan-out stays small in
    /// practice, and a `SmallVec` has no hash table to check in O(1) anyway.
    pub(crate) fn record_call_dep(&self, caller: &CompKey, callee: &CompKey) {
        let mut nodes = self.nodes.lock().unwrap();
        let callee_id = nodes.id_of(callee);
        if let (Some(callee_id), Some(node)) = (callee_id, nodes.get_mut(caller))
            && !node.comp_deps.contains(&callee_id)
        {
            node.comp_deps.push(callee_id);
        }
        let caller_id = nodes.id_of(caller);
        if let (Some(caller_id), Some(node)) = (caller_id, nodes.get_mut(callee))
            && !node.rdeps.contains(&caller_id)
        {
            node.rdeps.push(caller_id);
        }
    }

    pub(crate) fn mark_root(&self, key: CompKey) {
        self.roots.lock().unwrap().insert(key);
    }

    pub(crate) fn record_source_deps(&self, caller: &CompKey, raw: HashSet<RawDep>) {
        if raw.is_empty() {
            return;
        }
        let caller_id = {
            let mut nodes = self.nodes.lock().unwrap();
            let id = nodes.id_of(caller);
            if let Some(id) = id {
                nodes.extend_source_deps(id, &raw);
            }
            id
        };
        let Some(caller_id) = caller_id else { return };
        let mut index = self.source_index.lock().unwrap();
        for dep in raw {
            index.entry((dep.source, dep.key)).or_default().insert(caller_id);
        }
    }

    pub(crate) fn record_outputs(&self, caller: &CompKey, raw: HashSet<RawOutput>) {
        if raw.is_empty() {
            return;
        }
        let mut nodes = self.nodes.lock().unwrap();
        if let Some(id) = nodes.id_of(caller) {
            nodes.extend_outputs(id, &raw);
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
        if let Some(node) = nodes.get_mut(key) {
            node.set_state(NodeState::Dirty);
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
        let def_index: HashMap<DefId, DefIndex> =
            self.def_order.iter().enumerate().map(|(i, &id)| (id, DefIndex(i as u16))).collect();
        Engine {
            inner: Arc::new(EngineInner {
                defs: Mutex::new(self.defs),
                nodes: Mutex::new(NodeTable::new(def_index)),
                source_index: Mutex::new(HashMap::new()),
                roots: Mutex::new(HashSet::new()),
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

    /// Guards the point of the "node memory diet": a regression that
    /// accidentally reintroduces a wide field (a full `CompKey` per edge, a
    /// pre-serialized `value_bytes`/`param_debug`, a 256-bit hash, a
    /// per-node rerun closure, or an always-allocated `source_deps`/
    /// `outputs`/`inflight` field) should fail loudly here rather than only
    /// show up as a surprise in the 1M-instance `persist_bench` benchmark's
    /// RSS figure. The bound is deliberately generous (comfortably above
    /// the measured size on the platform this was tuned on) — this is a
    /// coarse tripwire, not a precise layout contract; exact field layout is
    /// not part of any public API.
    ///
    /// Measured at 296 bytes before the Tier-1 memory redesign (closure-kill
    /// plus `u16` `DefIndex` plus sparse side tables plus the
    /// `state`/`dirty_priority`/`last_changed` bitfield packing), 160 bytes
    /// after — the tripwire bound below is set with headroom above that
    /// 160, not tight to it.
    #[test]
    fn node_stays_small() {
        let size = std::mem::size_of::<Node>();
        assert!(size <= 192, "Node grew to {size} bytes — see this test's doc comment");
    }

    /// Regression test for the `rerun`-closure -> `Arc<EngineInner>`
    /// reference cycle this Tier-1 memory redesign removes (see [`Node`]'s
    /// docs): before this change, any node that had ever executed via the
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
