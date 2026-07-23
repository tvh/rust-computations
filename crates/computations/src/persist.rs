//! Opt-in, cache-only persistence of the dependency graph to a local redb
//! file.
//!
//! Persistence exists purely as a warm-start optimization: nothing here is
//! a source of truth. A corrupt database file, an on-disk format the
//! current binary doesn't recognize, or an individual record that no
//! longer matches a registered definition or decodes cleanly is always
//! safe to drop (the whole database, or just that one record) and
//! recompute from scratch — persistence never fails the engine, it only
//! ever warns and keeps running cold for the affected part of the graph.
//!
//! ## What gets persisted, and when
//!
//! After every settled propagation round, and once after the very first
//! (startup) evaluation, [`EngineInner::persist_flush`] writes exactly the
//! nodes whose last run actually produced a new result (early cutoff means
//! most reruns don't), plus removes whichever nodes liveness GC just
//! collected — never a wholesale rewrite of the graph. [`Engine::persist_now`]
//! exposes the same flush publicly, for a caller (typically a test
//! simulating a restart) that wants a deterministic "everything settled is
//! now safely on disk" point instead of waiting for the driver's own
//! timing.
//!
//! ## What gets loaded, and how it's trusted
//!
//! [`EngineInner::persist_load`] runs once, at the very start of
//! [`crate::driver`]'s `Engine::run`, before the initial evaluation. Every
//! record whose definition is still registered is revived into a `Clean`
//! node (see [`crate::def::ErasedDef`]); everything else is dropped. Two
//! independent checks then decide how much to *trust* what was revived,
//! each capable of forcing part (or all) of the graph to be re-verified in
//! the background rather than replayed blindly:
//!
//! - [`Fingerprint`]: a stable identity for "the code that produced these
//!   results". A mismatch (the binary has changed since the graph was last
//!   saved) marks *every* restored node [`crate::DirtyPriority::Revalidate`]
//!   — cheap to check, correct if wrong, and never blocks on it (Revalidate
//!   work always yields to genuine input changes).
//! - [`SourceBase::probe_versions`](crate::SourceBase::probe_versions):
//!   every restored source dependency is re-checked against its source's
//!   *current* version. Anything that changed (or that a source can't or
//!   won't confirm) marks its dependents, and every transitive ancestor of
//!   them, [`crate::DirtyPriority::Input`] — indistinguishable, by the time
//!   the initial evaluation runs, from a genuine input change having just
//!   arrived.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex as AsyncMutex;

use crate::engine::{DirtyPriority, EngineInner, Node, RevivedNode};
use crate::key::{CompKey, DefId, Hash256};
use crate::sink::{OutBytes, RawOutput, SinkId};
use crate::source::{KeyBytes, RawDep, SourceId, VerBytes};

/// The on-disk record format version.
///
/// Bumping this forces every existing persisted database to be treated as
/// unreadable (wiped, cold start) on next load — the escape hatch for a
/// breaking change to the record encoding itself. This is independent of
/// [`Fingerprint`], which is about the *code that produced the values*, not
/// the *encoding of the store*: a format bump always wipes, a fingerprint
/// mismatch only ever revalidates in the background.
const FORMAT_VERSION: u8 = 1;

const META_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");
const NODES_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("nodes");
const META_KEY: &str = "meta";

/// A stable identity for "the code that computed these persisted results".
///
/// Compared against the fingerprint stored alongside a persisted graph at
/// load time: a mismatch means the binary (or whatever the caller chose to
/// fingerprint) has changed since the graph was saved, so every restored
/// node is marked [`crate::DirtyPriority::Revalidate`] rather than trusted
/// outright — cheap to double-check, and never blocks genuinely changed
/// inputs (see [`crate::DirtyPriority`]'s tiering).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Fingerprint([u8; 32]);

impl Fingerprint {
    /// Fingerprints the current process's own executable: the common case
    /// of "has the binary changed since this graph was last saved".
    ///
    /// If the executable can't be located or read (an unusual environment —
    /// containers that delete their own binary, some sandboxes, ...), this
    /// falls back to a random fingerprint (logged via `tracing::warn`),
    /// which can never match a previously stored one: every restored node
    /// is always marked Revalidate in that case, which is always safe
    /// (just occasionally more conservative than necessary), never
    /// incorrect.
    pub fn current_exe() -> Self {
        match std::env::current_exe().and_then(std::fs::read) {
            Ok(bytes) => Fingerprint::custom(bytes),
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "Fingerprint::current_exe: failed to read the current executable; \
                     using a random fingerprint (always mismatches, so the restored graph \
                     will always be revalidated in the background)"
                );
                random_fingerprint()
            }
        }
    }

    /// Fingerprints arbitrary caller-chosen data — a build id, a version
    /// string, a config hash, or anything else that should invalidate the
    /// trust placed in a persisted graph when it changes.
    pub fn custom(data: impl AsRef<[u8]>) -> Self {
        Fingerprint(*blake3::hash(data.as_ref()).as_bytes())
    }
}

/// A fingerprint that can never equal any real one, used when
/// [`Fingerprint::current_exe`] can't read its own executable. Mixes in
/// enough ambient, per-process, per-call entropy (wall-clock time, PID, a
/// process-wide call counter, and a stack address, which ASLR randomizes)
/// that it is unique for all practical purposes — it does not need to be
/// cryptographically random, only vanishingly unlikely to collide, since a
/// spurious *match* (not a mismatch) is the only outcome that could ever
/// matter here, and this value is never persisted or compared against
/// itself across processes.
fn random_fingerprint() -> Fingerprint {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let mut hasher = blake3::Hasher::new();
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos();
    hasher.update(&nanos.to_le_bytes());
    hasher.update(&(std::process::id() as u64).to_le_bytes());
    hasher.update(&COUNTER.fetch_add(1, Ordering::Relaxed).to_le_bytes());
    let stack_marker = 0u8;
    hasher.update(&(std::ptr::from_ref(&stack_marker) as usize).to_le_bytes());
    Fingerprint(*hasher.finalize().as_bytes())
}

/// Options enabling persistence on an [`crate::EngineBuilder`] via
/// [`crate::EngineBuilder::persistence`].
#[derive(Debug, Clone)]
pub struct PersistOptions {
    /// The redb file to persist the dependency graph to. Created if it
    /// doesn't exist; wiped and recreated if it's unreadable or in an
    /// unrecognized format.
    pub path: PathBuf,
    /// Identifies "the code that computed these results" — see
    /// [`Fingerprint`].
    pub fingerprint: Fingerprint,
}

/// The persisted `meta` table's sole row.
#[derive(Serialize, Deserialize)]
struct MetaRecord {
    format_version: u8,
    fingerprint: [u8; 32],
}

/// A postcard-friendly stand-in for a [`CompKey`], identifying a dependency
/// edge (or the primary key of a `nodes` row) by definition name and param
/// hash — never the full parameter value, which is only ever stored for a
/// node's own primary record (see [`NodeRecord::param_bytes`]).
#[derive(Serialize, Deserialize, Clone)]
struct CompKeyRepr {
    def_name: String,
    param_hash: [u8; 32],
}

impl CompKeyRepr {
    fn from_key(key: &CompKey) -> Self {
        CompKeyRepr {
            def_name: key.def().name().to_string(),
            param_hash: key.param_hash().as_bytes(),
        }
    }

    /// Reconstructs the `CompKey` this identifies, if its definition is
    /// still registered (looked up by name in `def_names`). Returns `None`
    /// (rather than fabricating a `DefId`, which would require leaking a
    /// `&'static str`) if it isn't — the caller simply drops this
    /// dependency edge, exactly as it would drop a whole record for an
    /// unregistered definition.
    fn to_key(&self, def_names: &HashMap<String, DefId>) -> Option<CompKey> {
        let def_id = *def_names.get(&self.def_name)?;
        Some(CompKey::from_parts(def_id, Hash256::from_bytes(self.param_hash)))
    }
}

/// A postcard-friendly stand-in for a [`RawDep`] (whose `source: SourceId`
/// doesn't itself implement `Serialize`).
#[derive(Serialize, Deserialize, Clone)]
struct RawDepRepr {
    source: String,
    key: KeyBytes,
    ver: VerBytes,
}

impl RawDepRepr {
    fn from_dep(dep: &RawDep) -> Self {
        RawDepRepr {
            source: dep.source.to_string(),
            key: dep.key.clone(),
            ver: dep.ver.clone(),
        }
    }

    fn to_dep(&self) -> RawDep {
        RawDep {
            source: SourceId::new(&self.source),
            key: self.key.clone(),
            ver: self.ver.clone(),
        }
    }
}

/// A postcard-friendly stand-in for a [`RawOutput`] (whose `sink: SinkId`
/// doesn't itself implement `Serialize`).
#[derive(Serialize, Deserialize, Clone)]
struct RawOutputRepr {
    sink: String,
    out: OutBytes,
}

impl RawOutputRepr {
    fn from_output(out: &RawOutput) -> Self {
        RawOutputRepr {
            sink: out.sink.to_string(),
            out: out.out.clone(),
        }
    }

    fn to_output(&self) -> RawOutput {
        RawOutput {
            sink: SinkId::new(&self.sink),
            out: self.out.clone(),
        }
    }
}

/// One `nodes` table row: everything needed to revive a `Clean` [`Node`]
/// without ever re-executing its computation.
#[derive(Serialize, Deserialize)]
struct NodeRecord {
    def_name: String,
    param_bytes: Vec<u8>,
    comp_deps: Vec<CompKeyRepr>,
    source_deps: Vec<RawDepRepr>,
    result_hash: [u8; 32],
    value_bytes: Vec<u8>,
    outputs: Vec<RawOutputRepr>,
}

/// Encodes `key` into the same bytes used both as a `nodes` table primary
/// key and as a `comp_deps` entry — a pure function of `key`'s def name and
/// param hash, so it round-trips through [`CompKeyRepr::to_key`] without
/// needing the original parameter value.
fn encode_key(key: &CompKey) -> Vec<u8> {
    postcard::to_stdvec(&CompKeyRepr::from_key(key))
        .expect("postcard serialization of a well-formed value should not fail")
}

/// Keys changed or removed since the last successful save, accumulated as
/// the engine runs and drained by the next [`EngineInner::persist_flush`].
#[derive(Default)]
struct PendingDiff {
    changed: HashSet<CompKey>,
    removed: HashSet<CompKey>,
}

/// The live persistence handle: the open database plus whatever has
/// changed since the last save. Held behind an `Arc` in
/// `EngineInner::persist` once [`EngineInner::persist_load`] has
/// established it.
pub(crate) struct PersistHandle {
    db: Database,
    fingerprint: Fingerprint,
    pending: Mutex<PendingDiff>,
    /// Serializes concurrent flush attempts (the driver's own after-round
    /// save racing a test's manual `persist_now()`, say) so a flush always
    /// observes — and fully drains — whatever was pending by the time it
    /// was called, rather than two flushes each grabbing half the diff.
    flushing: AsyncMutex<()>,
}

impl PersistHandle {
    fn new(db: Database, fingerprint: Fingerprint) -> Self {
        PersistHandle {
            db,
            fingerprint,
            pending: Mutex::new(PendingDiff::default()),
            flushing: AsyncMutex::new(()),
        }
    }

    /// Records that `key`'s node just produced a genuinely new result and
    /// should be (re-)saved at the next flush.
    pub(crate) fn mark_changed(&self, key: CompKey) {
        let mut pending = self.pending.lock().unwrap();
        pending.removed.remove(&key);
        pending.changed.insert(key);
    }

    /// Records that `key`'s node was just collected by liveness GC and its
    /// record should be deleted at the next flush.
    pub(crate) fn mark_removed(&self, key: CompKey) {
        let mut pending = self.pending.lock().unwrap();
        pending.changed.remove(&key);
        pending.removed.insert(key);
    }
}

impl EngineInner {
    /// Loads the persisted graph, if [`crate::EngineBuilder::persistence`]
    /// was ever called — a no-op otherwise. Called once, by
    /// [`crate::driver`]'s `Engine::run`, before the initial evaluation.
    ///
    /// See the [module docs](self) for what "loading" restores and how much
    /// it's trusted.
    pub(crate) async fn persist_load(self: &Arc<Self>) {
        let Some(opts) = self.persist_opts.clone() else { return };

        let path = opts.path.clone();
        let (db, stored_fingerprint, records) = match tokio::task::spawn_blocking(move || open_db(&path)).await {
            Ok(outcome) => outcome,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "persistence: load task panicked; persistence disabled for this run, starting cold"
                );
                return;
            }
        };
        let Some(db) = db else {
            // Already warned inside `open_db`: even a fresh database
            // couldn't be created (bad path, permissions, ...).
            return;
        };

        let fingerprint_mismatch = stored_fingerprint != Some(opts.fingerprint);
        let restored_count = self.restore_nodes(records);

        if restored_count > 0 {
            if fingerprint_mismatch {
                tracing::info!(
                    "persistence: fingerprint mismatch, revalidating the restored graph in the background"
                );
                self.mark_all_dirty(DirtyPriority::Revalidate);
            }

            let changed = probe_restored_source_deps(self).await;
            if !changed.is_empty() {
                tracing::info!(
                    count = changed.len(),
                    "persistence: some restored source dependencies changed since last run"
                );
                mark_dirty_transitive(self, changed, DirtyPriority::Input);
            }
        }

        tracing::info!(nodes_restored = restored_count, "persistence: load complete");
        *self.persist.lock().unwrap() = Some(Arc::new(PersistHandle::new(db, opts.fingerprint)));
    }

    /// Turns every decodable, still-registered record into a `Clean`
    /// [`Node`], inserts it into `self.nodes`, then rebuilds `rdeps` and
    /// `source_index` from the whole restored batch. Returns the number of
    /// nodes actually restored.
    fn restore_nodes(self: &Arc<Self>, records: Vec<NodeRecord>) -> usize {
        let mut restored: HashMap<CompKey, Node> = HashMap::new();

        for record in records {
            let Some(&def_id) = self.def_names.get(&record.def_name) else {
                tracing::debug!(
                    def = %record.def_name,
                    "persistence: dropping a record whose definition is no longer registered"
                );
                continue;
            };
            let Some(erased_def) = self.erased_defs.get(&def_id) else {
                continue;
            };
            let Some((key, rerun, param_debug)) = erased_def.revive_param(self, &record.param_bytes) else {
                tracing::debug!(def = %record.def_name, "persistence: dropping a record whose param bytes failed to decode");
                continue;
            };
            let Some(value) = erased_def.revive_value(&record.value_bytes) else {
                tracing::debug!(def = %record.def_name, "persistence: dropping a record whose value bytes failed to decode");
                continue;
            };

            let comp_deps: HashSet<CompKey> =
                record.comp_deps.iter().filter_map(|dep| dep.to_key(&self.def_names)).collect();
            let source_deps: HashSet<RawDep> = record.source_deps.iter().map(RawDepRepr::to_dep).collect();
            let outputs: HashSet<RawOutput> = record.outputs.iter().map(RawOutputRepr::to_output).collect();

            let node = Node::from_persisted(RevivedNode {
                param_bytes: record.param_bytes,
                param_debug,
                value,
                value_bytes: record.value_bytes,
                result_hash: Hash256::from_bytes(record.result_hash),
                comp_deps,
                source_deps,
                outputs,
                rerun,
            });
            restored.insert(key, node);
        }

        let restored_count = restored.len();
        if restored_count == 0 {
            return 0;
        }

        // Rebuild `rdeps`: every restored node's `comp_deps` implies a
        // reverse edge on the callee, mirroring what `record_call_dep`
        // builds incrementally as a live node actually runs.
        let rdep_edges: Vec<(CompKey, CompKey)> = restored
            .iter()
            .flat_map(|(caller, node)| node.comp_deps.iter().map(|callee| (callee.clone(), caller.clone())))
            .collect();
        for (callee, caller) in rdep_edges {
            if let Some(node) = restored.get_mut(&callee) {
                node.rdeps.insert(caller);
            }
        }

        // Rebuild `source_index` the same way `record_source_deps` builds
        // it incrementally for a live node.
        {
            let mut source_index = self.source_index.lock().unwrap();
            for (key, node) in &restored {
                for dep in &node.source_deps {
                    source_index
                        .entry((dep.source.clone(), dep.key.clone()))
                        .or_default()
                        .insert(key.clone());
                }
            }
        }

        self.nodes.lock().unwrap().extend(restored);
        restored_count
    }
}

/// Probes every distinct (source, key) pair among the just-restored nodes'
/// source deps, grouped by source (one `probe_versions` call per source,
/// regardless of how many restored nodes/keys reference it — this also
/// (re)subscribes every probed key for future change notifications, per
/// [`SourceBase::probe_versions`](crate::SourceBase::probe_versions)'s
/// contract, which is the *only* way a restored dependency gets back into
/// a source's watch set). Returns every `CompKey` whose own recorded
/// version for some dep no longer matches what the source currently
/// reports — including a source no longer registered, one that doesn't
/// support probing at all, or a key the source can no longer observe.
async fn probe_restored_source_deps(engine: &EngineInner) -> HashSet<CompKey> {
    // Snapshot what's needed and release the lock before awaiting anything
    // (holding a `std::sync::Mutex` guard across an `.await` is unsound to
    // rely on and easy to deadlock).
    let deps_by_key: HashMap<CompKey, HashSet<RawDep>> = {
        let nodes = engine.nodes.lock().unwrap();
        nodes.iter().map(|(k, n)| (k.clone(), n.source_deps.clone())).collect()
    };

    let mut by_source: HashMap<SourceId, HashSet<KeyBytes>> = HashMap::new();
    for deps in deps_by_key.values() {
        for dep in deps {
            by_source.entry(dep.source.clone()).or_default().insert(dep.key.clone());
        }
    }

    let mut probed: HashMap<SourceId, Option<HashMap<KeyBytes, Option<VerBytes>>>> = HashMap::new();
    for (source_id, keys) in by_source {
        let result = match engine.registry.source(&source_id) {
            Some(source) => {
                let keys: Vec<KeyBytes> = keys.into_iter().collect();
                source.probe_versions(&keys).await
            }
            None => None,
        };
        probed.insert(source_id, result);
    }

    let mut changed = HashSet::new();
    for (key, deps) in &deps_by_key {
        for dep in deps {
            let unchanged = matches!(
                probed.get(&dep.source),
                Some(Some(map)) if matches!(map.get(&dep.key), Some(Some(ver)) if *ver == dep.ver)
            );
            if !unchanged {
                changed.insert(key.clone());
                break;
            }
        }
    }
    changed
}

/// Marks every key in `initial` dirty at `priority`, then marks every
/// transitive ancestor (via `rdeps`) dirty too — necessary because a plain
/// evaluation walk (as the initial evaluation following a load is) only
/// re-runs a node it actually visits whose own state is `Dirty`; a
/// cache-hit ancestor would otherwise never visit (and so never re-run) a
/// dirty descendant reached only through it. This re-establishes, before
/// the initial evaluation, the same "Clean implies transitively up to
/// date" invariant that `crate::driver`'s wave propagation maintains
/// continuously while the engine is running live.
fn mark_dirty_transitive(engine: &EngineInner, initial: HashSet<CompKey>, priority: DirtyPriority) {
    let mut seen: HashSet<CompKey> = HashSet::new();
    let mut frontier = initial;
    while !frontier.is_empty() {
        engine.mark_dirty_quiet(&frontier, priority);
        seen.extend(frontier.iter().cloned());

        let nodes = engine.nodes.lock().unwrap();
        let mut next = HashSet::new();
        for key in &frontier {
            if let Some(node) = nodes.get(key) {
                for rdep in &node.rdeps {
                    if !seen.contains(rdep) {
                        next.insert(rdep.clone());
                    }
                }
            }
        }
        drop(nodes);
        frontier = next;
    }
}

impl EngineInner {
    /// Saves whatever has changed or been removed since the last
    /// successful save, if persistence is configured and there is anything
    /// to write — a cheap no-op otherwise. Called automatically after every
    /// settled propagation round and after the initial evaluation (see
    /// [`crate::driver`]); also exposed publicly as [`crate::Engine::persist_now`]
    /// for a caller that wants a deterministic flush point.
    ///
    /// Failures (a disk error, a poisoned lock, ...) are logged via
    /// `tracing::warn` and never propagated: the pending diff is put back
    /// so the next flush retries it, and the engine keeps running exactly
    /// as if persistence were disabled.
    pub(crate) async fn persist_flush(&self) {
        let Some(handle) = self.persist.lock().unwrap().clone() else { return };
        let _guard = handle.flushing.lock().await;

        let (changed, removed) = {
            let mut pending = handle.pending.lock().unwrap();
            if pending.changed.is_empty() && pending.removed.is_empty() {
                return;
            }
            (std::mem::take(&mut pending.changed), std::mem::take(&mut pending.removed))
        };

        let mut upserts: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        {
            let nodes = self.nodes.lock().unwrap();
            for key in &changed {
                // Changed-then-removed within the same window, or dirtied
                // again since it last succeeded: either way, nothing safe
                // to save for it right now.
                let Some(node) = nodes.get(key) else { continue };
                if node.state != crate::engine::NodeState::Clean {
                    continue;
                }
                let (Some(value_bytes), Some(result_hash)) = (node.value_bytes.clone(), node.result_hash) else {
                    continue;
                };
                let record = NodeRecord {
                    def_name: key.def().name().to_string(),
                    param_bytes: node.param_bytes.clone(),
                    comp_deps: node.comp_deps.iter().map(CompKeyRepr::from_key).collect(),
                    source_deps: node.source_deps.iter().map(RawDepRepr::from_dep).collect(),
                    result_hash: result_hash.as_bytes(),
                    value_bytes,
                    outputs: node.outputs.iter().map(RawOutputRepr::from_output).collect(),
                };
                let bytes = postcard::to_stdvec(&record)
                    .expect("postcard serialization of a well-formed value should not fail");
                upserts.push((encode_key(key), bytes));
            }
        }
        let deletes: Vec<Vec<u8>> = removed.iter().map(encode_key).collect();

        if upserts.is_empty() && deletes.is_empty() {
            return;
        }
        let upsert_count = upserts.len();
        let delete_count = deletes.len();

        let write_result = {
            let handle = handle.clone();
            tokio::task::spawn_blocking(move || write_batch(&handle.db, handle.fingerprint, &upserts, &deletes)).await
        };

        match write_result {
            Ok(Ok(())) => {
                tracing::debug!(upserts = upsert_count, deletes = delete_count, "persistence: saved");
            }
            Ok(Err(e)) => {
                tracing::warn!(error = %e, "persistence: failed to save; will retry at the next save point");
                let mut pending = handle.pending.lock().unwrap();
                pending.changed.extend(changed);
                pending.removed.extend(removed);
            }
            Err(e) => {
                tracing::warn!(error = %e, "persistence: save task panicked; will retry at the next save point");
                let mut pending = handle.pending.lock().unwrap();
                pending.changed.extend(changed);
                pending.removed.extend(removed);
            }
        }
    }
}

/// Marks `key`'s node changed (for the next [`EngineInner::persist_flush`]),
/// if persistence is configured. Called by `EngineInner::run` right after a
/// successful execution whose result hash actually changed.
pub(crate) fn mark_changed(engine: &EngineInner, key: CompKey) {
    if let Some(handle) = engine.persist.lock().unwrap().clone() {
        handle.mark_changed(key);
    }
}

/// Marks `key`'s node removed (for the next [`EngineInner::persist_flush`]),
/// if persistence is configured. Called by `crate::driver`'s liveness GC for
/// every node it collects.
pub(crate) fn mark_removed(engine: &EngineInner, key: CompKey) {
    if let Some(handle) = engine.persist.lock().unwrap().clone() {
        handle.mark_removed(key);
    }
}

/// Opens (or creates) the database at `path`, reading back its meta row and
/// every `nodes` row. On any failure — corrupt file, unrecognized format,
/// or anything else redb reports — logs a warning, deletes the file, and
/// tries once more against a fresh database; if even that fails,
/// persistence is disabled for this run (`None`) rather than the engine
/// ever failing to start.
fn open_db(path: &Path) -> (Option<Database>, Option<Fingerprint>, Vec<NodeRecord>) {
    match open_and_read(path) {
        Ok(outcome) => outcome,
        Err(e) => {
            tracing::warn!(
                error = %e,
                path = %path.display(),
                "persistence: existing database unreadable or in an unrecognized format, wiping and starting cold"
            );
            let _ = std::fs::remove_file(path);
            match open_and_read(path) {
                Ok(outcome) => outcome,
                Err(e2) => {
                    tracing::warn!(
                        error = %e2,
                        path = %path.display(),
                        "persistence: failed to open a fresh database after wiping; disabling persistence for this run"
                    );
                    (None, None, Vec::new())
                }
            }
        }
    }
}

type OpenOutcome = (Option<Database>, Option<Fingerprint>, Vec<NodeRecord>);

fn open_and_read(path: &Path) -> Result<OpenOutcome, redb::Error> {
    let db = Database::create(path)?;
    let read_txn = db.begin_read()?;

    let fingerprint = {
        let meta_table = match read_txn.open_table(META_TABLE) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok((Some(db), None, Vec::new())),
            Err(e) => return Err(e.into()),
        };
        let Some(meta_bytes) = meta_table.get(META_KEY)? else {
            return Ok((Some(db), None, Vec::new()));
        };
        let meta: MetaRecord = postcard::from_bytes(meta_bytes.value())
            .map_err(|e| redb::Error::Corrupted(format!("meta record: {e}")))?;
        if meta.format_version != FORMAT_VERSION {
            return Err(redb::Error::Corrupted(format!(
                "persisted format version {} != current {FORMAT_VERSION}",
                meta.format_version
            )));
        }
        Fingerprint(meta.fingerprint)
    };

    let records = {
        let nodes_table = match read_txn.open_table(NODES_TABLE) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok((Some(db), Some(fingerprint), Vec::new())),
            Err(e) => return Err(e.into()),
        };
        let mut records = Vec::new();
        for entry in nodes_table.iter()? {
            let (_key, value) = entry?;
            match postcard::from_bytes::<NodeRecord>(value.value()) {
                Ok(record) => records.push(record),
                Err(e) => tracing::debug!(error = %e, "persistence: dropping a node record that failed to decode"),
            }
        }
        records
    };

    Ok((Some(db), Some(fingerprint), records))
}

/// Writes one batch of upserts and deletes, plus the current meta row, in a
/// single transaction.
fn write_batch(
    db: &Database,
    fingerprint: Fingerprint,
    upserts: &[(Vec<u8>, Vec<u8>)],
    deletes: &[Vec<u8>],
) -> Result<(), redb::Error> {
    let write_txn = db.begin_write()?;
    {
        let mut meta_table = write_txn.open_table(META_TABLE)?;
        let meta = MetaRecord {
            format_version: FORMAT_VERSION,
            fingerprint: fingerprint.0,
        };
        let meta_bytes =
            postcard::to_stdvec(&meta).expect("postcard serialization of a well-formed value should not fail");
        meta_table.insert(META_KEY, meta_bytes.as_slice())?;
    }
    {
        let mut nodes_table = write_txn.open_table(NODES_TABLE)?;
        for (key, value) in upserts {
            nodes_table.insert(key.as_slice(), value.as_slice())?;
        }
        for key in deletes {
            nodes_table.remove(key.as_slice())?;
        }
    }
    write_txn.commit()?;
    Ok(())
}
