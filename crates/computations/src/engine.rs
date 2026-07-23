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
use std::sync::{Arc, Mutex};

use futures::future::{BoxFuture, FutureExt, Shared};

use crate::ctx::Ctx;
use crate::def::{Comp, CompDef};
use crate::error::CompError;
use crate::key::{CompKey, CompParam, CompResult, DefId, Hash256, StableHash};
use crate::registry::Registry;
use crate::sink::{OutBytes, RawOutput, SinkId};
use crate::source::{KeyBytes, RawDep, SourceId};

/// The result of one execution: the (erased) value plus its content hash.
type ExecResult = Result<(Arc<dyn Any + Send + Sync>, Hash256), CompError>;
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

/// One computation application's memoized state.
pub(crate) struct Node {
    pub(crate) state: NodeState,
    /// The cached result, erased. `Some` even when `state == Dirty` (a
    /// stale-but-not-yet-superseded value), `None` only before the first
    /// successful execution.
    value: Option<Arc<dyn Any + Send + Sync>>,
    result_hash: Option<Hash256>,
    /// Computations this node called during its last execution.
    pub(crate) comp_deps: HashSet<CompKey>,
    /// Source reads this node made during its last execution.
    pub(crate) source_deps: HashSet<RawDep>,
    /// Computations that called this node during their last execution.
    pub(crate) rdeps: HashSet<CompKey>,
    /// Sink outputs this node produced during its last execution.
    pub(crate) outputs: HashSet<RawOutput>,
    /// Whether the last successful execution's result hash differed from
    /// the one before it (early cutoff signal).
    ///
    /// Read by the driver to decide whether a clean recomputation should
    /// propagate further to this node's rdeps, or stop here (early cutoff).
    pub(crate) last_changed: bool,
    inflight: Option<SharedExec>,
    /// Called by the driver during change propagation to re-execute a
    /// dirtied node through the normal eval path.
    pub(crate) rerun: RerunFn,
    def: DefId,
    /// `Debug`-rendered param, for diagnostics (tracing, panic messages)
    /// without needing `Node` itself to be generic over `P`.
    param_debug: String,
}

/// The node's state just before a fresh run, snapshotted so the run can
/// diff against it afterwards (early cutoff, dropped outputs, stale source
/// index entries).
struct PreRunSnapshot {
    old_hash: Option<Hash256>,
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
    pub(crate) nodes: Mutex<HashMap<CompKey, Node>>,
    /// Maintained on every dependency (re-)collection so the driver can map
    /// a changed (source, key) pair back to the computations that read it,
    /// without scanning the whole node table.
    pub(crate) source_index: Mutex<HashMap<(SourceId, KeyBytes), HashSet<CompKey>>>,
    /// Root applications (evaluated via `Engine::eval_root`), so the
    /// driver's liveness GC knows which nodes are reachable from outside the
    /// graph and must not be collected even with no `rdeps`.
    pub(crate) roots: Mutex<HashSet<CompKey>>,
    pub(crate) registry: Registry,
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

        let engine_for_rerun = self.clone();
        let def_id_for_rerun = def_id.clone();
        let param_for_rerun = param.clone();
        let node = nodes.entry(key.clone()).or_insert_with(|| {
            let rerun: RerunFn = Arc::new(move || {
                let engine = Engine {
                    inner: engine_for_rerun.clone(),
                };
                let comp = Comp::<P, R>::from_def_id(def_id_for_rerun.clone());
                let param = param_for_rerun.clone();
                Box::pin(async move { engine.eval_root(&comp, param).await.map(|_| ()) })
            });
            Node {
                state: NodeState::Dirty,
                value: None,
                result_hash: None,
                comp_deps: HashSet::new(),
                source_deps: HashSet::new(),
                rdeps: HashSet::new(),
                outputs: HashSet::new(),
                last_changed: false,
                inflight: None,
                rerun,
                def: def_id.clone(),
                param_debug: format!("{param:?}"),
            }
        });

        tracing::debug!(comp = ?key, def = %node.def, param = %node.param_debug, "begin execution");

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

        let key_for_fut = key.clone();
        let fut: BoxFuture<'static, ExecResult> = Box::pin(async move {
            tracing::debug!(comp = ?key_for_fut, "execute start");
            let result = (def.body)(ctx, param).await?;
            let hash = result.stable_hash();
            let value: Arc<dyn Any + Send + Sync> = Arc::new(result);
            Ok((value, hash))
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
            Ok((value_any, hash)) => {
                let changed = old_hash != Some(hash);
                let (new_source_deps, new_outputs) = {
                    let mut nodes = self.nodes.lock().unwrap();
                    let node = nodes
                        .get_mut(key)
                        .expect("node present: created by prepare() before run() is called");
                    node.state = NodeState::Clean;
                    node.value = Some(value_any.clone());
                    node.result_hash = Some(hash);
                    node.last_changed = changed;
                    node.inflight = None;
                    (node.source_deps.clone(), node.outputs.clone())
                };

                self.remove_stale_source_index(key, &old_source_deps, &new_source_deps);

                let dropped_outputs: HashSet<RawOutput> =
                    old_outputs.difference(&new_outputs).cloned().collect();
                if !dropped_outputs.is_empty() {
                    self.delete_dropped_outputs(dropped_outputs).await;
                }

                tracing::debug!(comp = ?key, changed, "execute finished");
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
                tracing::debug!(comp = ?key, error = %e, "execute failed");
                Err(e)
            }
        }
    }

    fn remove_stale_source_index(&self, key: &CompKey, old: &HashSet<RawDep>, new: &HashSet<RawDep>) {
        if old == new {
            return;
        }
        let mut index = self.source_index.lock().unwrap();
        for dep in old.difference(new) {
            let idx_key = (dep.source.clone(), dep.key.clone());
            if let Some(set) = index.get_mut(&idx_key) {
                set.remove(key);
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
    pub(crate) async fn eval<P: CompParam, R: CompResult>(
        self: &Arc<Self>,
        def_id: DefId,
        param: P,
        chain: Arc<Vec<CompKey>>,
    ) -> Result<(R, CompKey), CompError> {
        let key = CompKey::new(def_id.clone(), &param);

        if chain.contains(&key) {
            return Err(CompError::Cycle(render_chain(&chain, &key)));
        }

        let action = self.prepare::<P, R>(&key, &def_id, &param);

        let value = match action {
            Action::CacheHit(v) => {
                tracing::debug!(comp = ?key, "cache hit");
                downcast_value::<R>(v, &key)?
            }
            Action::Join(shared) => {
                tracing::debug!(comp = ?key, "dedup join on inflight execution");
                let (v, _hash) = shared.await?;
                downcast_value::<R>(v, &key)?
            }
            Action::Run(snapshot) => self.run::<P, R>(&key, &def_id, param, chain, snapshot).await?,
        };

        Ok((value, key))
    }

    pub(crate) fn record_call_dep(&self, caller: &CompKey, callee: &CompKey) {
        let mut nodes = self.nodes.lock().unwrap();
        if let Some(node) = nodes.get_mut(caller) {
            node.comp_deps.insert(callee.clone());
        }
        if let Some(node) = nodes.get_mut(callee) {
            node.rdeps.insert(caller.clone());
        }
    }

    pub(crate) fn mark_root(&self, key: CompKey) {
        self.roots.lock().unwrap().insert(key);
    }

    pub(crate) fn record_source_deps(&self, caller: &CompKey, raw: HashSet<RawDep>) {
        if raw.is_empty() {
            return;
        }
        {
            let mut nodes = self.nodes.lock().unwrap();
            if let Some(node) = nodes.get_mut(caller) {
                node.source_deps.extend(raw.iter().cloned());
            }
        }
        let mut index = self.source_index.lock().unwrap();
        for dep in raw {
            index
                .entry((dep.source, dep.key))
                .or_default()
                .insert(caller.clone());
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
            registry: Registry::default(),
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
        ctx.eval(comp, param).await
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
    registry: Registry,
}

impl EngineBuilder {
    /// Registers a computation definition, returning a handle to it.
    ///
    /// # Panics
    /// Panics if a computation with the same name is already registered —
    /// this is a startup configuration error, not a runtime condition.
    pub fn register<P: CompParam, R: CompResult>(&mut self, def: CompDef<P, R>) -> Comp<P, R> {
        let id = def.id.clone();
        let prev = self.defs.insert(id.clone(), Arc::new(def) as Arc<dyn Any + Send + Sync>);
        assert!(prev.is_none(), "duplicate computation name: {id}");
        Comp::from_def_id(id)
    }

    /// Attaches the sources/sinks [`crate::driver`] will use. An engine
    /// built without calling this has an empty registry, which is fine for
    /// tests that never write to a real sink.
    pub fn registry(&mut self, registry: Registry) -> &mut Self {
        self.registry = registry;
        self
    }

    /// Finishes building the `Engine`.
    pub fn build(self) -> Engine {
        Engine {
            inner: Arc::new(EngineInner {
                defs: Mutex::new(self.defs),
                nodes: Mutex::new(HashMap::new()),
                source_index: Mutex::new(HashMap::new()),
                roots: Mutex::new(HashSet::new()),
                registry: self.registry,
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
        let key = CompKey::new(comp.def_id().clone(), &());
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
