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
//! primitive it drives: re-evaluating one node.

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
/// A closure that re-executes a node's computation via the normal eval path.
///
/// Captures the node's typed `Comp<P, R>` handle, its param, and the engine.
/// Called by the driver's change propagation on dirtied nodes.
pub(crate) type RerunFn = Arc<dyn Fn() -> BoxFuture<'static, Result<(), CompError>> + Send + Sync>;

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
/// computation (a [`RerunFn`] closure, a persisted record, `roots`) is keyed
/// by `CompKey` instead, precisely so it can never be invalidated by GC
/// reusing a `NodeId`; `NodeId` is used purely as an edge (`comp_deps`,
/// `rdeps`, `source_index`) representation inside a single process's live
/// node table, translated back to a `CompKey` (via [`Node::key`]) whenever
/// an edge needs to leave that table (persistence, driver-level dirty
/// bookkeeping).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct NodeId(u32);

/// One computation application's memoized state.
pub(crate) struct Node {
    pub(crate) state: NodeState,
    /// The priority of this node's pending dirty work; `None` exactly when
    /// `state == Clean` (cleared on every successful run, left untouched on
    /// a failed one so the node stays dirty at the priority it was being
    /// processed at). Set — and merged via max, "max priority wins" — by
    /// [`crate::driver`]'s dirtying paths; see [`DirtyPriority`].
    pub(crate) dirty_priority: Option<DirtyPriority>,
    /// This node's own stable identity — the `NodeTable`'s `CompKey` index
    /// entry, duplicated here so a `NodeId` can be translated back to its
    /// `CompKey` in O(1) (needed by GC, persistence snapshotting, and the
    /// driver's rdeps-to-dirty-keys translation) without a second, reverse
    /// index.
    pub(crate) key: CompKey,
    /// The cached result, erased. `Some` even when `state == Dirty` (a
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
    /// without needing to know its concrete parameter type.
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
    /// Source reads this node made during its last execution.
    ///
    /// Deliberately still a `HashSet`, unlike `comp_deps`/`rdeps`: measured
    /// on the 1M-node persistence benchmark, switching this to a small
    /// inline `SmallVec` was a net *loss*, not a win — most nodes in that
    /// benchmark (and, more importantly, in general: any node that doesn't
    /// read a source at all, e.g. one whose only job is combining other
    /// computations) have *zero* source deps, and a `SmallVec`'s inline
    /// capacity is reserved unconditionally even when empty. With
    /// `RawDep`'s size (source id + two byte buffers), reserving even a
    /// 1-2 element inline array on every node costs more than a never-
    /// touched `HashSet` (no allocation at all until the first insert) —
    /// the opposite of `comp_deps`/`rdeps`, whose `NodeId` elements are
    /// small enough, and whose typical count is non-zero often enough, for
    /// the inline reservation to pay for itself.
    pub(crate) source_deps: HashSet<RawDep>,
    /// Computations that called this node during their last execution, by
    /// [`NodeId`] (see `comp_deps`'s docs for why `SmallVec` and why by id).
    pub(crate) rdeps: SmallVec<[NodeId; 4]>,
    /// Sink outputs this node produced during its last execution. Still a
    /// `HashSet`, for the same reason as `source_deps`.
    pub(crate) outputs: HashSet<RawOutput>,
    /// Whether the last successful execution's result hash differed from
    /// the one before it (early cutoff signal).
    ///
    /// Read by the driver to decide whether a clean recomputation should
    /// propagate further to this node's rdeps, or stop here (early cutoff).
    pub(crate) last_changed: bool,
    inflight: Option<SharedExec>,
    /// Called by the driver during change propagation to re-execute a
    /// dirtied node through the normal eval path. Captures only `DefId`
    /// (`Copy`) and the typed, owned parameter — never a `CompKey` or
    /// `NodeId` — recomputing the current `CompKey` itself on every call
    /// (see `EngineInner::make_rerun`); this is what makes it safe for a
    /// `rerun` closure to long-outlive the `NodeId` (never `Copy`-captured
    /// here in the first place) of whatever node currently holds it, across
    /// any number of GC slot-reuse cycles.
    pub(crate) rerun: RerunFn,
}

impl Node {
    /// Constructs a `Node` directly in the `Clean` state from a persisted
    /// record's revived pieces (see `crate::persist`), without ever having
    /// executed the computation in this process.
    ///
    /// `comp_deps`/`rdeps` both start empty: `crate::persist`'s loader wires
    /// them up itself afterward, once every record in the batch has a
    /// [`NodeId`] (a dependency edge can only be expressed once both of its
    /// endpoints are already in the table), mirroring how a live node's
    /// edges are built incrementally by `EngineInner::record_call_dep` as it
    /// actually runs.
    pub(crate) fn from_persisted(key: CompKey, revived: RevivedNode) -> Node {
        Node {
            state: NodeState::Clean,
            dirty_priority: None,
            key,
            value: Some(revived.value),
            result_hash: Some(revived.result_hash),
            param_bytes: revived.param_bytes,
            comp_deps: SmallVec::new(),
            source_deps: revived.source_deps,
            rdeps: SmallVec::new(),
            outputs: revived.outputs,
            last_changed: false,
            inflight: None,
            rerun: revived.rerun,
        }
    }
}

/// Every piece [`Node::from_persisted`] needs to revive a `Clean` node from
/// a persisted record, bundled into one struct purely to keep that
/// constructor's argument list manageable (see `crate::persist`, which
/// builds one of these per decodable, still-registered record). Excludes
/// `comp_deps` (and the node's own `CompKey`, passed to `from_persisted`
/// separately): both need every record in the batch to already have a
/// [`NodeId`] before they can be resolved, which `crate::persist::restore_nodes`
/// therefore does itself, after every record has been turned into a `Node`.
pub(crate) struct RevivedNode {
    pub(crate) param_bytes: Vec<u8>,
    pub(crate) value: Arc<dyn Any + Send + Sync>,
    pub(crate) result_hash: Hash128,
    pub(crate) source_deps: HashSet<RawDep>,
    pub(crate) outputs: HashSet<RawOutput>,
    pub(crate) rerun: RerunFn,
}

/// The engine's node table: a slab (`Vec<Option<Node>>` plus a free list) of
/// every live node, addressable either by its stable [`CompKey`] (via an
/// index) or by its process-local [`NodeId`] (a direct slab slot).
///
/// This is the in-memory analogue of what used to be a plain
/// `HashMap<CompKey, Node>`: every method that existed on that map
/// (`get`/`get_mut`/`keys`/`values`/`values_mut`) is preserved here with the
/// same `CompKey`-keyed signature, so most call sites needed no changes at
/// all when this replaced it — only code that stores or walks *edges*
/// (`comp_deps`/`rdeps`/`source_index`) needed to switch to the `NodeId`-based
/// methods (`get_by_id`/`get_mut_by_id`/`id_of`) to get the point of this
/// type: an edge only needs 4 bytes (`NodeId`), not 32 (`CompKey`).
#[derive(Default)]
pub(crate) struct NodeTable {
    slots: Vec<Option<Node>>,
    free: Vec<u32>,
    index: HashMap<CompKey, NodeId>,
}

impl NodeTable {
    fn new() -> Self {
        NodeTable::default()
    }

    pub(crate) fn id_of(&self, key: &CompKey) -> Option<NodeId> {
        self.index.get(key).copied()
    }

    pub(crate) fn get(&self, key: &CompKey) -> Option<&Node> {
        let id = *self.index.get(key)?;
        self.slots[id.0 as usize].as_ref()
    }

    pub(crate) fn get_mut(&mut self, key: &CompKey) -> Option<&mut Node> {
        let id = *self.index.get(key)?;
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
        self.index.insert(key, id);
        id
    }

    /// Returns the existing node for `key` if present, otherwise builds one
    /// via `make` and inserts it — the id/node pair either way. `make`'s
    /// result's `key` field is overwritten with `key` regardless of what it
    /// sets, so callers may leave it at any placeholder value.
    pub(crate) fn get_or_insert_with(&mut self, key: &CompKey, make: impl FnOnce() -> Node) -> (NodeId, &mut Node) {
        if let Some(&id) = self.index.get(key) {
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
    pub(crate) fn remove_by_id(&mut self, id: NodeId) -> Option<Node> {
        let node = self.slots.get_mut(id.0 as usize)?.take()?;
        self.index.remove(&node.key);
        self.free.push(id.0);
        Some(node)
    }

    pub(crate) fn keys(&self) -> impl Iterator<Item = &CompKey> {
        self.index.keys()
    }

    pub(crate) fn values(&self) -> impl Iterator<Item = &Node> {
        self.slots.iter().filter_map(|s| s.as_ref())
    }

    pub(crate) fn values_mut(&mut self) -> impl Iterator<Item = &mut Node> {
        self.slots.iter_mut().filter_map(|s| s.as_mut())
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (NodeId, &Node)> {
        self.slots.iter().enumerate().filter_map(|(i, s)| s.as_ref().map(|n| (NodeId(i as u32), n)))
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
    /// Type-erased revival operations (see `crate::persist`) for every
    /// registered definition, keyed by `DefId`. Built once, alongside
    /// `defs`, at registration time; never mutated afterward.
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

    /// Builds the `rerun` closure for `def_id` applied to `param`:
    /// re-evaluates it via the normal `eval_root` path when called.
    ///
    /// Shared by [`Self::prepare`] (building a brand-new live node) and
    /// [`crate::def::DefAdapter::revive_param`] (reviving a node restored
    /// from a persisted record — see `crate::persist`), so a restored
    /// node's `rerun` is exactly the same closure shape a live one gets,
    /// with no separate revival-only code path to drift out of sync.
    pub(crate) fn make_rerun<P: CompParam, R: CompResult>(self: &Arc<Self>, def_id: DefId, param: P) -> RerunFn {
        let engine_for_rerun = self.clone();
        Arc::new(move || {
            let engine = engine_for_rerun.clone();
            let param = param.clone();
            // Deliberately calls `Self::eval` directly rather than going
            // through `Engine::eval_root` (which this used to do): the
            // computation body itself gets exactly the same `Ctx` either
            // way (`Self::run` always builds it fresh with
            // `caller: Some(key)`, regardless of how the evaluation was
            // reached), but `eval_root` additionally marks its argument a
            // GC root as a side effect of its own outer, `caller: None`
            // `Ctx::eval` call — appropriate for a *genuine* root
            // evaluation (the one `crate::driver::Engine::run` performs
            // once, up front), but wrong here: this closure re-evaluates
            // an *existing* node, dirtied for any reason (its own source
            // input changed, a revalidation sweep, ...), not a fresh root.
            // Routing it through `eval_root` would permanently mark every
            // node that ever directly serves a propagation wave as a root
            // — since `roots` only ever grows — which would make liveness
            // GC never collect it again even after every real caller stops
            // referencing it.
            Box::pin(async move { engine.eval::<P, R>(def_id, param, Arc::new(Vec::new())).await.map(|_| ()) })
        })
    }

    /// Decides whether `key` is a cache hit, an in-flight join, or needs a
    /// fresh run, creating its `Node` (with a `rerun` closure) on first
    /// sight. On the `Run` path this also resets the node's dynamic
    /// dependency/output collections and marks it `Running`, so the caller
    /// must actually run the computation after this returns.
    fn prepare<P: CompParam, R: CompResult>(
        self: &Arc<Self>,
        key: &CompKey,
        def_id: &DefId,
        param: &P,
    ) -> Action {
        let mut nodes = self.nodes.lock().unwrap();

        if let Some(node) = nodes.get(key) {
            if node.state == NodeState::Clean
                && let Some(v) = &node.value
            {
                return Action::CacheHit(v.clone());
            }
            if let Some(shared) = &node.inflight {
                return Action::Join(shared.clone());
            }
        }

        let rerun: RerunFn = self.make_rerun::<P, R>(*def_id, param.clone());
        let (_id, node) = nodes.get_or_insert_with(key, || Node {
            state: NodeState::Dirty,
            dirty_priority: None,
            key: key.clone(),
            value: None,
            result_hash: None,
            param_bytes: postcard::to_stdvec(param)
                .expect("postcard serialization of a well-formed value should not fail"),
            comp_deps: SmallVec::new(),
            source_deps: HashSet::new(),
            rdeps: SmallVec::new(),
            outputs: HashSet::new(),
            last_changed: false,
            inflight: None,
            rerun,
        });

        let old_hash = node.result_hash;
        let old_outputs = node.outputs.clone();
        let old_source_deps = node.source_deps.clone();
        node.state = NodeState::Running;
        node.comp_deps.clear();
        node.source_deps.clear();
        node.outputs.clear();

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
                    node.state = NodeState::Dirty;
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
            if let Some(node) = nodes.get_mut(key) {
                node.inflight = Some(shared.clone());
            }
        }

        let outcome = shared.await;

        match outcome {
            Ok((value_any, hash, elapsed)) => {
                let changed = old_hash != Some(hash);
                let (id, new_source_deps, new_outputs) = {
                    let mut nodes = self.nodes.lock().unwrap();
                    {
                        let node = nodes
                            .get_mut(key)
                            .expect("node present: created by prepare() before run() is called");
                        node.state = NodeState::Clean;
                        node.dirty_priority = None;
                        node.value = Some(value_any.clone());
                        node.result_hash = Some(hash);
                        node.last_changed = changed;
                        node.inflight = None;
                    }

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

                    let node = nodes.get(key).expect("node present: just updated above");
                    let id = nodes.id_of(key).expect("id present: just looked up its node above");
                    (id, node.source_deps.clone(), node.outputs.clone())
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
                if let Some(node) = nodes.get_mut(key) {
                    node.state = NodeState::Dirty;
                    node.inflight = None;
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

            let action = self.prepare::<P, R>(&key, &def_id, &param);

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
            if let Some(node) = nodes.get_mut(caller) {
                node.source_deps.extend(raw.iter().cloned());
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
        if let Some(node) = nodes.get_mut(caller) {
            node.outputs.extend(raw);
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
            node.state = NodeState::Dirty;
        }
    }
}

/// Builds an [`Engine`]: registers computation definitions and (optionally)
/// the [`Registry`] of sources/sinks the driver wires up.
pub struct EngineBuilder {
    defs: HashMap<DefId, Arc<dyn Any + Send + Sync>>,
    erased_defs: HashMap<DefId, Arc<dyn ErasedDef>>,
    def_names: HashMap<String, DefId>,
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

    /// Defines and registers a computation named `name` in one step,
    /// threading a shared environment `env` into the body on every
    /// invocation.
    ///
    /// This is exactly [`crate::def::define_comp_with`] followed by
    /// [`EngineBuilder::register`], collapsed into a single call — the
    /// environment-passing counterpart to [`EngineBuilder::define`]. Where
    /// `define`'s body is `Fn(Ctx, P) -> Fut`, `define_with`'s body is `Fn(E,
    /// Ctx, P) -> Fut`: it receives an owned clone of `env`, freshly made for
    /// every invocation, so there is no need to hand-write the two-layer
    /// clone dance (clone into the outer closure, clone again into the inner
    /// `async move`) that capturing `Arc` handles directly would otherwise
    /// require.
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
    /// one step, threading a shared environment `env` into the body on every
    /// invocation.
    ///
    /// This is exactly [`crate::def::define_comp_rec_with`] followed by
    /// [`EngineBuilder::register`] — the environment-passing counterpart to
    /// [`EngineBuilder::define_rec`]. The body's signature is `Fn(E,
    /// Comp<P, R>, Ctx, P) -> Fut`: an owned clone of `env`, then the working
    /// handle to the computation's own definition (`this`), then the usual
    /// `Ctx`/param pair. See [`EngineBuilder::define_with`] for the two
    /// `&env` call styles (shared-across-definitions vs. single-use inline)
    /// and why `env` is cloned once per invocation.
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
    pub fn build(self) -> Engine {
        let (dirty_tx, dirty_rx) = mpsc::unbounded_channel();
        Engine {
            inner: Arc::new(EngineInner {
                defs: Mutex::new(self.defs),
                nodes: Mutex::new(NodeTable::new()),
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
    /// pre-serialized `value_bytes`/`param_debug`, a 256-bit hash) should
    /// fail loudly here rather than only show up as a surprise in the
    /// 1M-instance `persist_bench` benchmark's RSS figure. The bound is
    /// deliberately generous (comfortably above the measured size on the
    /// platform this was tuned on) — this is a coarse tripwire, not a
    /// precise layout contract; exact field layout is not part of any
    /// public API.
    #[test]
    fn node_stays_small() {
        let size = std::mem::size_of::<Node>();
        assert!(size <= 320, "Node grew to {size} bytes — see this test's doc comment");
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
