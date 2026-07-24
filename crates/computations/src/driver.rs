//! The top-level driver that wires sources, computations, and sinks together.
//!
//! [`Engine::run`] is the driver's entry point: it performs the initial
//! evaluation of the application, a startup garbage-collection pass over
//! every sink's existing outputs, and then loops forever reacting to either
//! upstream source changes or dirty work marked directly via
//! [`Engine::mark_dirty`]/[`Engine::mark_all_dirty`] — dirtying the affected
//! nodes, re-running them in waves along the dependency graph (stopping
//! early wherever a recomputation's result hash didn't change), and running
//! a liveness GC pass after every round settles.
//!
//! Dirty work carries a [`crate::engine::DirtyPriority`]: `Input`-tier work
//! (genuine source changes) always drains to a fixpoint before any
//! `Revalidate`-tier work starts, and an in-progress Revalidate sweep is
//! preempted — between waves, never mid-wave — by newly arrived Input-tier
//! work. See this module's `EngineInner::propagate` for the tier-aware wave
//! loop.
//!
//! Cancel the loop by aborting or dropping the task it runs in
//! (e.g. `tokio::spawn(async move { engine.run(comp, param).await })`);
//! `run` never returns on its own once the initial evaluation succeeds.
//!
//! If persistence is configured (see `crate::persist`), saving happens
//! entirely off this loop: a background persister task flushes on its own
//! debounced schedule, so no round here ever awaits a write. Use
//! [`Engine::persist_now`] for a deterministic "everything outstanding is
//! now durable" point.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use futures::future::{BoxFuture, FutureExt, join_all, select_all};
use tracing::Instrument;

use crate::def::Comp;
use crate::engine::{DirtyPriority, Engine, EngineInner, NodeId, NodeState};
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

        // Restore a persisted graph, if configured (see `crate::persist`),
        // before the initial evaluation: restored `Clean` nodes are cache
        // hits below, and anything a fingerprint mismatch or a source's
        // `probe_versions` couldn't vouch for is already marked dirty by
        // the time we get here, so it re-executes as part of this very
        // evaluation rather than waiting for the loop's first iteration.
        self.inner.persist_load().await;

        self.eval_root(&comp, param).await?;
        tracing::info!(elapsed_ms = start.elapsed().as_millis() as u64, "initial evaluation complete");

        let startup_outputs_deleted = self.inner.startup_gc().await;
        if startup_outputs_deleted > 0 {
            tracing::info!(outputs_deleted = startup_outputs_deleted, "startup GC complete");
        } else {
            tracing::debug!("startup GC complete: nothing to collect");
        }

        loop {
            // Wait for either a genuine source change, or dirty work marked
            // from outside this loop entirely (`Engine::mark_dirty`/
            // `mark_all_dirty`, e.g. a test driving the engine directly) —
            // the latter is how the loop wakes up even with no source
            // change in sight. Persistence's own restore-time dirtying
            // (`crate::persist::mark_dirty_transitive`) happens earlier,
            // before this loop even starts (see `persist_load` above), so
            // the affected nodes are already handled by the initial
            // evaluation rather than needing to wake this loop up.
            let (dirtied, triggering_deps) = tokio::select! {
                changed = self.inner.wait_for_any_change() => {
                    if changed.is_empty() {
                        continue;
                    }
                    let dirtied = self.inner.affected_keys(&changed);
                    if dirtied.is_empty() {
                        continue;
                    }
                    let triggering_deps = changed.len();
                    self.inner.mark_dirty_quiet(&dirtied, DirtyPriority::Input);
                    (dirtied, triggering_deps)
                }
                dirtied = self.inner.recv_marked_dirty() => {
                    if dirtied.is_empty() {
                        continue;
                    }
                    (dirtied, 0)
                }
            };

            let span = tracing::debug_span!(
                "driver.propagate",
                triggering_deps,
                dirtied = dirtied.len()
            );
            async {
                let prop_stats = self.inner.propagate(dirtied).await;
                let gc_stats = self.inner.liveness_gc().await;
                // Persistence is no longer awaited here: every node's
                // record (and every GC'd node's removal) was already
                // enqueued synchronously as `propagate`/`liveness_gc` ran
                // (see `crate::persist::enqueue_changed`/`mark_removed`); a
                // background persister task decides on its own timing when
                // to actually flush that to disk (see `crate::persist`'s
                // module docs). A round is "done" the moment its results
                // are enqueued, not once they're durable.
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

    /// Forces an immediate flush of everything currently pending — every
    /// node change and GC removal already enqueued (synchronously, as it
    /// happened) since the last flush — and awaits its completion, if
    /// [`crate::EngineBuilder::persistence`] was configured (a no-op
    /// otherwise). The background persister task (see `crate::persist`)
    /// flushes on its own debounced schedule regardless; this is for a
    /// caller (typically a test simulating a restart, or a graceful
    /// shutdown path) that wants a deterministic "everything settled is now
    /// durable" point without waiting for that timing.
    pub async fn persist_now(&self) {
        self.inner.persist_force_flush().await;
    }

    /// Closes the persisted database file, if persistence is configured,
    /// releasing redb's exclusive lock on it without needing this
    /// `Engine`'s last reference to be dropped first. Also lets the
    /// background persister task (see `crate::persist`) notice its handle
    /// is gone and exit.
    ///
    /// Does **not** itself flush: call [`Self::persist_now`] first if
    /// anything enqueued since the last flush needs to survive the close
    /// (every call site in this crate's own tests does exactly that).
    ///
    /// An earlier version of the engine needed this method for a second
    /// reason beyond promptness: any node that had ever executed carried a
    /// `rerun` closure with a permanent `Arc<EngineInner>` self-reference,
    /// so an `Engine` was never, in general, fully dropped merely by
    /// dropping every handle to it — `EngineInner` effectively lived for the
    /// process's whole lifetime regardless. That reference cycle no longer
    /// exists (see `crate::engine::EngineInner::rerun_node`: a rerun now
    /// decodes `(def, param_bytes)` on demand instead of going through a
    /// stored closure), so an `Engine` genuinely drops once every handle to
    /// it — including its still-running `run` task — is gone. This method
    /// still matters, though: it closes redb's exclusive file lock
    /// *promptly*, without waiting on that drop to happen on its own, which
    /// is what a test simulating a restart against the same file within a
    /// single process needs (the previous engine's database must be closed
    /// before a new one can open the same file — redb allows only one open
    /// `Database` per file at a time).
    pub fn persist_close(&self) {
        self.inner.persist.lock().unwrap().take();
    }

    /// Test-only: how many entries are currently enqueued but not yet
    /// flushed to disk (`0` if persistence isn't configured/loaded, or
    /// nothing is pending). Lets a test observe "a round settled but
    /// nothing has been written yet" without reaching into a second redb
    /// handle on the same file.
    #[cfg(any(test, feature = "testutil"))]
    pub fn persist_pending_count(&self) -> usize {
        self.inner.persist_pending_count()
    }

    /// Test-only: how many flushes have completed successfully so far
    /// (`0` if persistence isn't configured/loaded).
    #[cfg(any(test, feature = "testutil"))]
    pub fn persist_flush_count(&self) -> u64 {
        self.inner.persist_flush_count()
    }

    /// Marks every currently-known node dirty at `priority`, waking the
    /// `run` loop so it picks the work up even if no source change ever
    /// arrives. See [`DirtyPriority`] for the max-wins merge rule applied
    /// when a node already has pending dirty work.
    ///
    /// Used by `crate::persist`'s load step (mark every restored node
    /// `Revalidate` on a fingerprint mismatch, so it gets checked against
    /// the current code in the background without holding up genuinely
    /// changed inputs) and by tests driving the engine directly. Ordinary
    /// application code never needs this — nodes are dirtied indirectly,
    /// through source changes.
    pub fn mark_all_dirty(&self, priority: DirtyPriority) {
        self.inner.mark_all_dirty(priority);
    }

    /// Marks `keys` dirty at `priority`. See [`Engine::mark_all_dirty`].
    pub fn mark_dirty(&self, keys: &HashSet<CompKey>, priority: DirtyPriority) {
        self.inner.mark_dirty(keys, priority);
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
        for (id, _node) in nodes.iter() {
            for out in nodes.outputs_iter(id) {
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
            let Some(ids) = index.get(&(dep.source.clone(), dep.key.clone())) else {
                continue;
            };
            for &id in ids {
                let Some(node) = nodes.get_by_id(id) else { continue };
                if nodes.source_deps_contains(id, dep) {
                    continue;
                }
                affected.insert(node.key.clone());
            }
        }
        affected
    }

    /// Marks every key in `keys` dirty at `priority`, taking the max of any
    /// existing pending priority for that key ("max priority wins"; see
    /// [`DirtyPriority`]). A key with no node is ignored — nothing to mark.
    ///
    /// Does not wake the `run` loop; use this only for marking that happens
    /// *from inside* an already-running propagation round, where the loop
    /// is already awake and driving things (initial source-change
    /// dirtying, rdep propagation, preemption polling), or before the loop
    /// has even started (persistence's load step — see
    /// `crate::persist::mark_dirty_transitive` — marks up-front, before the
    /// initial evaluation ever runs). Marking from outside an active round,
    /// once the loop is already running, must go through [`Self::mark_dirty`]
    /// instead so the loop actually wakes up to service it.
    pub(crate) fn mark_dirty_quiet(&self, keys: &HashSet<CompKey>, priority: DirtyPriority) {
        let mut nodes = self.nodes.lock().unwrap();
        for key in keys {
            if let Some(node) = nodes.get_mut(key) {
                let merged = match node.dirty_priority() {
                    Some(existing) => existing.max(priority),
                    None => priority,
                };
                node.set_dirty_priority(Some(merged));
                node.set_state(NodeState::Dirty);
            }
        }
    }

    /// Marks `keys` dirty at `priority` (see [`Self::mark_dirty_quiet`])
    /// and wakes the `run` loop so it services the work even if it is
    /// currently blocked waiting for a source change. The entry point for
    /// [`Engine::mark_dirty`].
    pub(crate) fn mark_dirty(&self, keys: &HashSet<CompKey>, priority: DirtyPriority) {
        self.mark_dirty_quiet(keys, priority);
        for key in keys {
            let _ = self.dirty_tx.send(key.clone());
        }
    }

    /// Marks every currently-known node dirty at `priority`. See
    /// [`Self::mark_dirty`]. (At rest, between propagation rounds, the node
    /// table only ever holds live nodes — liveness GC sweeps dead ones away
    /// at every round boundary — so "every node" and "every live node"
    /// coincide here.)
    pub(crate) fn mark_all_dirty(&self, priority: DirtyPriority) {
        let keys: HashSet<CompKey> = {
            let nodes = self.nodes.lock().unwrap();
            nodes.keys().cloned().collect()
        };
        self.mark_dirty(&keys, priority);
    }

    /// Awaits the next batch of dirty keys marked from outside an active
    /// propagation round (via [`Self::mark_dirty`]/[`Self::mark_all_dirty`]),
    /// draining whatever else is queued without blocking further — mirrors
    /// `wait_for_any_change`'s draining of a source's change channel.
    async fn recv_marked_dirty(&self) -> HashSet<CompKey> {
        let mut rx = self.dirty_rx.lock().await;
        let mut batch = HashSet::new();
        match rx.recv().await {
            Some(key) => {
                batch.insert(key);
            }
            None => return batch,
        }
        while let Ok(key) = rx.try_recv() {
            batch.insert(key);
        }
        batch
    }

    /// Non-blocking check for source changes that arrived while
    /// Revalidate-tier work was in progress, used to preempt it: polls
    /// every registered source once (never waits — see
    /// [`futures::future::FutureExt::now_or_never`]) and, if anything
    /// reports a change, marks the affected computations dirty at
    /// [`DirtyPriority::Input`] and returns their keys (empty if nothing
    /// new arrived).
    fn poll_pending_input_changes(&self) -> HashSet<CompKey> {
        let sources: Vec<Arc<dyn crate::source::ErasedSource>> = self.registry.sources().cloned().collect();
        let mut changed: HashSet<RawDep> = HashSet::new();
        for source in &sources {
            if let Some(deps) = source.wait_changes().now_or_never() {
                changed.extend(deps);
            }
        }
        if changed.is_empty() {
            return HashSet::new();
        }
        let dirtied = self.affected_keys(&changed);
        if dirtied.is_empty() {
            return HashSet::new();
        }
        self.mark_dirty_quiet(&dirtied, DirtyPriority::Input);
        dirtied
    }

    /// Splits `keys` into (Input frontier, Revalidate frontier) by reading
    /// each key's currently recorded `dirty_priority`. A key with no node,
    /// or no pending priority (nothing to do — already settled), is
    /// dropped from both.
    fn split_by_tier(&self, keys: HashSet<CompKey>) -> (HashSet<CompKey>, HashSet<CompKey>) {
        let nodes = self.nodes.lock().unwrap();
        let mut input = HashSet::new();
        let mut revalidate = HashSet::new();
        for key in keys {
            match nodes.get(&key).and_then(|n| n.dirty_priority()) {
                Some(DirtyPriority::Input) => {
                    input.insert(key);
                }
                Some(DirtyPriority::Revalidate) => {
                    revalidate.insert(key);
                }
                None => {}
            }
        }
        (input, revalidate)
    }

    /// Wave propagation, tier-aware: `Input`-tier dirty work (genuine
    /// source changes) always runs to a fixpoint before any
    /// `Revalidate`-tier work even starts, and Revalidate work is preempted
    /// — checked between waves, never mid-wave, so running futures always
    /// complete — by any Input-tier work that arrives while it runs. See
    /// [`DirtyPriority`].
    ///
    /// Within a tier this is exactly the propagation algorithm this method
    /// used before tiers existed: repeatedly re-run the current dirty
    /// frontier concurrently, then dirty the rdeps of every node whose
    /// result hash changed (nodes whose hash didn't change stop propagation
    /// there — early cutoff), until no dirty frontier remains for that
    /// tier. Each node re-runs at most once per round — across every tier
    /// and every preemption interleaving — tracked via `done`.
    ///
    /// Every key in `initial` must already have been marked dirty (with its
    /// intended priority) by the caller; this only reads that state to
    /// route each key into its tier's frontier.
    ///
    /// A node that becomes unreachable mid-round (e.g. its last live caller
    /// was just cut off) may still be re-run once here rather than being
    /// skipped; the liveness GC pass that follows the round (after every
    /// tier has settled) cleans it up regardless, so this is a harmless
    /// wasted rerun rather than a correctness issue.
    async fn propagate(self: &Arc<Self>, initial: HashSet<CompKey>) -> PropagateStats {
        let mut done: HashSet<CompKey> = HashSet::new();
        let mut waves = 0usize;
        let mut total_reran = 0usize;

        let (input_frontier, revalidate_frontier) = self.split_by_tier(initial);

        total_reran += self
            .propagate_tier(DirtyPriority::Input, input_frontier, &mut done, &mut waves)
            .await;

        let mut frontier = revalidate_frontier;
        while !frontier.is_empty() {
            let batch: Vec<CompKey> = frontier.into_iter().filter(|k| done.insert(k.clone())).collect();
            if batch.is_empty() {
                break;
            }
            waves += 1;
            let (reran, next) = self.run_wave(DirtyPriority::Revalidate, batch, &done, waves).await;
            total_reran += reran;
            frontier = next;

            // Background-ish: yield between waves so this stays low
            // priority under load, then check whether real input work
            // arrived while the wave ran; if so, drain it to a full
            // fixpoint before resuming Revalidate work.
            tokio::task::yield_now().await;
            let preempting_input = self.poll_pending_input_changes();
            if !preempting_input.is_empty() {
                total_reran += self
                    .propagate_tier(DirtyPriority::Input, preempting_input, &mut done, &mut waves)
                    .await;
            }
        }

        PropagateStats { waves, total_reran }
    }

    /// Runs `tier`'s frontier to a fixpoint (waves until its frontier is
    /// empty), threading the round's `done` set and wave counter through.
    /// Returns the number of nodes that actually reran.
    async fn propagate_tier(
        self: &Arc<Self>,
        tier: DirtyPriority,
        initial: HashSet<CompKey>,
        done: &mut HashSet<CompKey>,
        waves: &mut usize,
    ) -> usize {
        let mut frontier = initial;
        let mut total_reran = 0usize;
        while !frontier.is_empty() {
            let batch: Vec<CompKey> = frontier.into_iter().filter(|k| done.insert(k.clone())).collect();
            if batch.is_empty() {
                break;
            }
            *waves += 1;
            let (reran, next) = self.run_wave(tier, batch, done, *waves).await;
            total_reran += reran;
            frontier = next;
        }
        total_reran
    }

    /// Runs one wave: re-executes `batch` concurrently, then computes the
    /// next frontier — the rdeps of every node whose result hash changed
    /// (early cutoff otherwise) that isn't already `done` this round —
    /// marking each of them dirty at `tier` (dirtiness propagates at the
    /// priority of the node that changed). Returns the number of nodes that
    /// actually reran.
    async fn run_wave(
        self: &Arc<Self>,
        tier: DirtyPriority,
        batch: Vec<CompKey>,
        done: &HashSet<CompKey>,
        wave: usize,
    ) -> (usize, HashSet<CompKey>) {
        let dirty_count = batch.len();

        let jobs: Vec<(CompKey, Vec<u8>)> = {
            let mut nodes = self.nodes.lock().unwrap();
            batch
                .iter()
                .filter_map(|key| {
                    let node = nodes.get_mut(key)?;
                    node.set_state(NodeState::Dirty);
                    Some((key.clone(), node.param_bytes.clone()))
                })
                .collect()
        };

        let results = join_all(jobs.iter().map(|(key, param_bytes)| self.rerun_node(key, param_bytes))).await;

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
                        if node.last_changed() {
                            for &rdep_id in &node.rdeps {
                                let Some(rdep_node) = nodes.get_by_id(rdep_id) else { continue };
                                if !done.contains(&rdep_node.key) {
                                    next_frontier.insert(rdep_node.key.clone());
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
        tracing::debug!(
            wave,
            tier = ?tier,
            dirty = dirty_count,
            reran,
            cutoffs,
            "propagation wave complete"
        );

        if !next_frontier.is_empty() {
            self.mark_dirty_quiet(&next_frontier, tier);
        }

        (reran, next_frontier)
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

            let mut reachable: HashSet<NodeId> = HashSet::new();
            let mut stack: Vec<NodeId> = {
                let roots = self.roots.lock().unwrap();
                roots.iter().filter_map(|key| nodes.id_of(key)).collect()
            };
            while let Some(id) = stack.pop() {
                if !reachable.insert(id) {
                    continue;
                }
                if let Some(node) = nodes.get_by_id(id) {
                    for &dep in &node.comp_deps {
                        if !reachable.contains(&dep) {
                            stack.push(dep);
                        }
                    }
                }
            }

            let dead_ids: HashSet<NodeId> =
                nodes.iter().map(|(id, _)| id).filter(|id| !reachable.contains(id)).collect();
            if dead_ids.is_empty() {
                return GcStats::default();
            }

            let mut dead_keys: Vec<CompKey> = Vec::with_capacity(dead_ids.len());
            let mut outputs_by_sink: HashMap<SinkId, Vec<OutBytes>> = HashMap::new();
            let mut dead_source_deps: HashMap<SourceId, HashSet<KeyBytes>> = HashMap::new();
            for &id in &dead_ids {
                // Captured before `remove_by_id`, which purges every
                // side-table entry (`source_deps`/`outputs`/`inflight`) for
                // `id` as part of collecting the node itself — see its docs.
                let outputs = nodes.outputs_clone(id);
                let source_deps = nodes.source_deps_clone(id);
                if let Some(node) = nodes.remove_by_id(id) {
                    dead_keys.push(node.key);
                    for RawOutput { sink, out } in outputs {
                        outputs_by_sink.entry(sink).or_default().push(out);
                    }
                    for dep in source_deps {
                        dead_source_deps.entry(dep.source).or_default().insert(dep.key);
                    }
                }
            }

            {
                let mut index = self.source_index.lock().unwrap();
                index.retain(|_, callers| {
                    for id in &dead_ids {
                        callers.remove(id);
                    }
                    !callers.is_empty()
                });
            }
            for node in nodes.values_mut() {
                node.rdeps.retain(|id| !dead_ids.contains(id));
            }

            (dead_keys, outputs_by_sink, dead_source_deps)
        };

        for key in &dead_keys {
            crate::persist::mark_removed(self, key.clone());
        }

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

// `pub(crate)`, not `pub`, so `mark_dirty`/`mark_dirty_quiet`/`nodes` aren't
// visible from the `tests/driver.rs` integration test crate: the max-wins
// merge rule is exercised directly here instead, deterministically and
// without depending on any real propagation timing (see
// `tests/driver.rs`'s `input_priority_preempts_in_progress_revalidate_sweep`
// for the timing-sensitive, end-to-end version of tiered dirtying).
#[cfg(test)]
mod tests {
    use super::*;
    use crate::def::define_comp;

    /// Marking a node Revalidate then Input must leave it at Input (the
    /// higher priority); marking it Revalidate again afterwards must not
    /// downgrade it back down. "Max priority wins."
    #[tokio::test]
    async fn mark_dirty_max_priority_wins() {
        let mut builder = Engine::builder();
        let comp: Comp<(), ()> = builder.register(define_comp::<(), (), _, _>("max_wins_probe", |_ctx, _: ()| async {
            Ok(())
        }));
        let engine = builder.build();
        engine.eval_root(&comp, ()).await.unwrap();

        let key = CompKey::new(*comp.def_id(), &());
        let mut keys = HashSet::new();
        keys.insert(key.clone());

        engine.inner.mark_dirty_quiet(&keys, DirtyPriority::Revalidate);
        assert_eq!(
            engine.inner.nodes.lock().unwrap().get(&key).unwrap().dirty_priority(),
            Some(DirtyPriority::Revalidate)
        );
        assert_eq!(
            engine.inner.nodes.lock().unwrap().get(&key).unwrap().state(),
            NodeState::Dirty
        );

        engine.inner.mark_dirty_quiet(&keys, DirtyPriority::Input);
        assert_eq!(
            engine.inner.nodes.lock().unwrap().get(&key).unwrap().dirty_priority(),
            Some(DirtyPriority::Input),
            "Input must win over a prior Revalidate mark"
        );

        engine.inner.mark_dirty_quiet(&keys, DirtyPriority::Revalidate);
        assert_eq!(
            engine.inner.nodes.lock().unwrap().get(&key).unwrap().dirty_priority(),
            Some(DirtyPriority::Input),
            "a later Revalidate mark must not downgrade an already-Input pending priority"
        );
    }

    /// `mark_all_dirty` must reach every node currently in the table, not
    /// just ones explicitly named, and must apply the same max-wins rule.
    #[tokio::test]
    async fn mark_all_dirty_reaches_every_node() {
        let mut builder = Engine::builder();
        let a: Comp<(), ()> =
            builder.register(define_comp::<(), (), _, _>("all_dirty_a", |_ctx, _: ()| async { Ok(()) }));
        let b: Comp<(), ()> =
            builder.register(define_comp::<(), (), _, _>("all_dirty_b", |_ctx, _: ()| async { Ok(()) }));
        let engine = builder.build();
        engine.eval_root(&a, ()).await.unwrap();
        engine.eval_root(&b, ()).await.unwrap();

        let key_a = CompKey::new(*a.def_id(), &());
        let key_b = CompKey::new(*b.def_id(), &());

        engine.inner.mark_all_dirty(DirtyPriority::Revalidate);

        let nodes = engine.inner.nodes.lock().unwrap();
        assert_eq!(nodes.get(&key_a).unwrap().dirty_priority(), Some(DirtyPriority::Revalidate));
        assert_eq!(nodes.get(&key_b).unwrap().dirty_priority(), Some(DirtyPriority::Revalidate));
    }
}
