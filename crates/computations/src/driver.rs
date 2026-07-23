//! The top-level driver that wires sources, computations, and sinks together.
//!
//! [`Engine::run`] is the driver's entry point: it performs the initial
//! evaluation of the application, a startup garbage-collection pass over
//! every sink's existing outputs, and then loops forever reacting to
//! upstream source changes — dirtying the affected nodes, re-running them in
//! waves along the dependency graph (stopping early wherever a
//! recomputation's result hash didn't change), and running a liveness GC
//! pass after every round settles.
//!
//! Cancel the loop by aborting or dropping the task it runs in
//! (e.g. `tokio::spawn(async move { engine.run(comp, param).await })`);
//! `run` never returns on its own once the initial evaluation succeeds.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use futures::future::{BoxFuture, join_all, select_all};
use tracing::Instrument;

use crate::def::Comp;
use crate::engine::{Engine, EngineInner, NodeState, RerunFn};
use crate::error::CompError;
use crate::key::{CompKey, CompParam, CompResult};
use crate::sink::{OutBytes, RawOutput, SinkId};
use crate::source::{KeyBytes, RawDep, SourceId};

/// Summary of one `propagate` round, for the `driver.propagate` span's
/// round-summary event.
struct PropagateStats {
    waves: usize,
    total_reran: usize,
}

/// Summary of one `liveness_gc` pass, for its own consolidated event and for
/// the enclosing round-summary event.
#[derive(Default)]
struct GcStats {
    nodes_collected: usize,
    outputs_deleted: usize,
    keys_unregistered: usize,
}

impl Engine {
    /// Runs the system: an initial evaluation of `comp` applied to `param`,
    /// a startup GC pass, then an infinite loop reacting to source changes.
    ///
    /// Returns an error only if the *initial* evaluation fails. Once the
    /// system is up, propagation errors are logged (`tracing::warn`) and the
    /// loop continues; the offending node stays dirty and is retried the
    /// next time a relevant source change comes in.
    ///
    /// This future never resolves on the happy path: cancel it by aborting
    /// or dropping the task it runs in.
    pub async fn run<P: CompParam, R: CompResult>(&self, comp: Comp<P, R>, param: P) -> Result<(), CompError> {
        let start = Instant::now();
        self.eval_root(&comp, param).await?;
        tracing::info!(elapsed_ms = start.elapsed().as_millis() as u64, "initial evaluation complete");

        let startup_outputs_deleted = self.inner.startup_gc().await;
        if startup_outputs_deleted > 0 {
            tracing::info!(outputs_deleted = startup_outputs_deleted, "startup GC complete");
        } else {
            tracing::debug!("startup GC complete: nothing to collect");
        }

        loop {
            let changed = self.inner.wait_for_any_change().await;
            if changed.is_empty() {
                continue;
            }
            let dirtied = self.inner.affected_keys(&changed);
            if dirtied.is_empty() {
                continue;
            }

            let span = tracing::debug_span!(
                "driver.propagate",
                triggering_deps = changed.len(),
                dirtied = dirtied.len()
            );
            async {
                let prop_stats = self.inner.propagate(dirtied).await;
                let gc_stats = self.inner.liveness_gc().await;
                tracing::debug!(
                    waves = prop_stats.waves,
                    total_reran = prop_stats.total_reran,
                    nodes_gcd = gc_stats.nodes_collected,
                    outputs_deleted = gc_stats.outputs_deleted,
                    keys_unregistered = gc_stats.keys_unregistered,
                    "propagation round complete"
                );
            }
            .instrument(span)
            .await;
        }
    }
}

impl EngineInner {
    /// Startup GC: for every sink that can list its existing outputs,
    /// deletes whatever it reports that no live node currently produces.
    /// Returns the total number of outputs deleted across all sinks, so the
    /// caller can log a single summary.
    async fn startup_gc(&self) -> usize {
        let live = self.live_outputs_by_sink();
        let sinks: Vec<Arc<dyn crate::sink::ErasedSink>> = self.registry.sinks().cloned().collect();

        let mut total_deleted = 0usize;
        for sink in sinks {
            let id = sink.instance_id();
            match sink.list_existing_outputs().await {
                Ok(Some(existing)) => {
                    let live_for_sink = live.get(&id);
                    let stale: Vec<OutBytes> = existing
                        .into_iter()
                        .filter(|out| !live_for_sink.is_some_and(|l| l.contains(out)))
                        .collect();
                    if stale.is_empty() {
                        continue;
                    }
                    let count = stale.len();
                    match sink.delete_outputs(stale).await {
                        Ok(()) => {
                            total_deleted += count;
                            tracing::debug!(sink = %id, outputs_deleted = count, "startup GC: sink pass complete");
                        }
                        Err(e) => {
                            tracing::warn!(sink = %id, error = %e, "startup GC: failed to delete stale outputs");
                        }
                    }
                }
                Ok(None) => {}
                Err(e) => {
                    tracing::warn!(sink = %id, error = %e, "startup GC: failed to list existing outputs");
                }
            }
        }
        total_deleted
    }

    /// The union of every node's currently recorded outputs, grouped by sink.
    fn live_outputs_by_sink(&self) -> HashMap<SinkId, HashSet<OutBytes>> {
        let nodes = self.nodes.lock().unwrap();
        let mut live: HashMap<SinkId, HashSet<OutBytes>> = HashMap::new();
        for node in nodes.values() {
            for out in &node.outputs {
                live.entry(out.sink.clone()).or_default().insert(out.out.clone());
            }
        }
        live
    }

    /// Awaits the next batch of changes from ANY registered source.
    ///
    /// Races every source's `wait_changes()` future (cancel-safe per the
    /// `SourceBase` contract) and returns as soon as the first resolves,
    /// dropping the rest of that round's futures — a fresh set is built on
    /// every call, so nothing is lost by discarding the unfinished ones.
    /// If no sources are registered, this never resolves (there is nothing
    /// to wait for).
    async fn wait_for_any_change(&self) -> HashSet<RawDep> {
        let sources: Vec<Arc<dyn crate::source::ErasedSource>> = self.registry.sources().cloned().collect();
        if sources.is_empty() {
            return std::future::pending().await;
        }
        tracing::trace!(
            sources = ?sources.iter().map(|s| s.instance_id()).collect::<Vec<_>>(),
            "awaiting source changes"
        );
        let futs: Vec<BoxFuture<'_, HashSet<RawDep>>> = sources.iter().map(|s| s.wait_changes()).collect();
        let (changed, _idx, _rest) = select_all(futs).await;
        changed
    }

    /// Maps a batch of changed deps through `source_index` to the
    /// computations that depend on them, skipping any dep whose reported
    /// version is already the one recorded on the node (a spurious wake:
    /// the node's last run already observed this exact version).
    fn affected_keys(&self, changed: &HashSet<RawDep>) -> HashSet<CompKey> {
        let index = self.source_index.lock().unwrap();
        let nodes = self.nodes.lock().unwrap();
        let mut affected = HashSet::new();
        for dep in changed {
            let Some(keys) = index.get(&(dep.source.clone(), dep.key.clone())) else {
                continue;
            };
            for key in keys {
                if let Some(node) = nodes.get(key)
                    && node.source_deps.contains(dep)
                {
                    continue;
                }
                affected.insert(key.clone());
            }
        }
        affected
    }

    /// Wave propagation: repeatedly re-runs the current dirty frontier
    /// concurrently, then dirties the rdeps of every node whose result hash
    /// changed (nodes whose hash didn't change stop propagation there —
    /// early cutoff), until no dirty frontier remains. Each node re-runs at
    /// most once per round, tracked via `done`.
    ///
    /// A node that becomes unreachable mid-round (e.g. its last live caller
    /// was just cut off) may still be re-run once here rather than being
    /// skipped; the liveness GC pass that follows the round cleans it up
    /// regardless, so this is a harmless wasted rerun rather than a
    /// correctness issue.
    async fn propagate(&self, initial: HashSet<CompKey>) -> PropagateStats {
        let mut frontier = initial;
        let mut done: HashSet<CompKey> = HashSet::new();
        let mut waves = 0usize;
        let mut total_reran = 0usize;

        while !frontier.is_empty() {
            let batch: Vec<CompKey> = frontier.into_iter().filter(|k| done.insert(k.clone())).collect();
            if batch.is_empty() {
                break;
            }
            waves += 1;
            let dirty_count = batch.len();

            let jobs: Vec<(CompKey, RerunFn)> = {
                let mut nodes = self.nodes.lock().unwrap();
                batch
                    .iter()
                    .filter_map(|key| {
                        let node = nodes.get_mut(key)?;
                        node.state = NodeState::Dirty;
                        Some((key.clone(), node.rerun.clone()))
                    })
                    .collect()
            };

            let results = join_all(jobs.iter().map(|(_, rerun)| rerun())).await;

            let mut next_frontier = HashSet::new();
            let mut reran = 0usize;
            let mut cutoffs = 0usize;
            {
                let nodes = self.nodes.lock().unwrap();
                for ((key, _), result) in jobs.iter().zip(results.iter()) {
                    match result {
                        Ok(()) => {
                            reran += 1;
                            let Some(node) = nodes.get(key) else {
                                continue;
                            };
                            if node.last_changed {
                                for rdep in &node.rdeps {
                                    if !done.contains(rdep) {
                                        next_frontier.insert(rdep.clone());
                                    }
                                }
                            } else {
                                cutoffs += 1;
                            }
                        }
                        Err(e) => {
                            // `comp` (the def name alone) mirrors the
                            // `comp.eval` span's field convention so this
                            // standalone event (outside that span — the
                            // rerun's own nested span has already closed by
                            // the time we get here) can be filtered/grepped
                            // on the same field name; `key` keeps the full
                            // name#hash identity for disambiguating between
                            // applications of the same computation.
                            tracing::warn!(
                                comp = %key.def().name(),
                                key = ?key,
                                error = %e,
                                "change propagation: computation failed; it stays dirty and will retry on the next relevant change"
                            );
                        }
                    }
                }
            }
            total_reran += reran;
            tracing::debug!(wave = waves, dirty = dirty_count, reran, cutoffs, "propagation wave complete");
            frontier = next_frontier;
        }

        PropagateStats { waves, total_reran }
    }

    /// Liveness GC: mark-sweep from `roots` over `comp_deps`. Any node not
    /// reached is collected: its sink outputs are deleted (grouped by
    /// sink), its source-key dependencies are unregistered from their
    /// source *if* no surviving node still depends on that (source, key),
    /// and it is removed from the node table, `source_index`, and every
    /// surviving node's `rdeps`.
    async fn liveness_gc(&self) -> GcStats {
        let (dead_keys, outputs_by_sink, dead_source_deps) = {
            let mut nodes = self.nodes.lock().unwrap();

            let mut reachable: HashSet<CompKey> = HashSet::new();
            let mut stack: Vec<CompKey> = {
                let roots = self.roots.lock().unwrap();
                roots.iter().cloned().collect()
            };
            while let Some(key) = stack.pop() {
                if !reachable.insert(key.clone()) {
                    continue;
                }
                if let Some(node) = nodes.get(&key) {
                    for dep in &node.comp_deps {
                        if !reachable.contains(dep) {
                            stack.push(dep.clone());
                        }
                    }
                }
            }

            let dead_keys: Vec<CompKey> = nodes.keys().filter(|k| !reachable.contains(k)).cloned().collect();
            if dead_keys.is_empty() {
                return GcStats::default();
            }

            let mut outputs_by_sink: HashMap<SinkId, Vec<OutBytes>> = HashMap::new();
            let mut dead_source_deps: HashMap<SourceId, HashSet<KeyBytes>> = HashMap::new();
            for key in &dead_keys {
                if let Some(node) = nodes.remove(key) {
                    for RawOutput { sink, out } in node.outputs {
                        outputs_by_sink.entry(sink).or_default().push(out);
                    }
                    for dep in node.source_deps {
                        dead_source_deps.entry(dep.source).or_default().insert(dep.key);
                    }
                }
            }

            {
                let mut index = self.source_index.lock().unwrap();
                index.retain(|_, callers| {
                    for key in &dead_keys {
                        callers.remove(key);
                    }
                    !callers.is_empty()
                });
            }
            for node in nodes.values_mut() {
                for key in &dead_keys {
                    node.rdeps.remove(key);
                }
            }

            (dead_keys, outputs_by_sink, dead_source_deps)
        };

        // Unregister source keys that no surviving node depends on anymore.
        let mut to_unregister: HashMap<SourceId, Vec<KeyBytes>> = HashMap::new();
        {
            let index = self.source_index.lock().unwrap();
            for (source_id, keys) in &dead_source_deps {
                for key in keys {
                    if !index.contains_key(&(source_id.clone(), key.clone())) {
                        to_unregister.entry(source_id.clone()).or_default().push(key.clone());
                    }
                }
            }
        }
        let mut keys_unregistered = 0usize;
        for (source_id, keys) in to_unregister {
            keys_unregistered += keys.len();
            if let Some(source) = self.registry.source(&source_id) {
                source.unregister(&keys);
            }
        }

        let mut outputs_deleted = 0usize;
        for (sink_id, outs) in outputs_by_sink {
            outputs_deleted += outs.len();
            if let Some(sink) = self.registry.sink(&sink_id)
                && let Err(e) = sink.delete_outputs(outs).await
            {
                tracing::warn!(sink = %sink_id, error = %e, "liveness GC: failed to delete outputs of a collected node");
            }
        }

        let nodes_collected = dead_keys.len();
        tracing::debug!(nodes_collected, outputs_deleted, keys_unregistered, "liveness GC pass");
        GcStats {
            nodes_collected,
            outputs_deleted,
            keys_unregistered,
        }
    }
}
