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
//! Saving is decoupled from propagation entirely. The moment a node
//! produces a genuinely new result (or liveness GC collects one), its
//! record is snapshotted — under the same node-table lock that just
//! updated it, so the snapshot can never race a later mutation of that
//! same node — and dropped into a [`PendingMap`] owned by a background
//! *persister task* (spawned once, by [`EngineInner::persist_load`], for
//! the lifetime of the engine). That task alone decides when to actually
//! write: a debounced quiet period, a size cap on the backlog, or a
//! max-staleness cap on the oldest unflushed entry, whichever comes first
//! (see [`PersistOptions`]). [`crate::driver::Engine::run`]'s propagation
//! loop never awaits a write — a round is "done" the moment its results
//! are enqueued, not once they're durable.
//!
//! This means a crash can lose whatever sits in the pending map at the
//! moment it happens — at most [`PersistOptions::flush_max_staleness`]
//! worth of updates, usually far less. That's fine: persistence is a
//! cache, so anything lost is simply recomputed on the next run (a
//! restored node whose source deps have since moved on is caught by
//! [`SourceBase::probe_versions`](crate::SourceBase::probe_versions) at
//! load time exactly as if the change had arrived after a cold start).
//! [`Engine::persist_now`] exposes a deterministic "everything outstanding
//! is now safely on disk" flush point for a caller (typically a test
//! simulating a restart, or a shutdown path) that needs one.
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
//!   arrived. This is also what makes the crash-window loss above safe:
//!   whatever a lost flush would have recorded, `probe_versions` still
//!   sees the real, current source state on the next load.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex as AsyncMutex, Notify};

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

/// How long the background persister task idles between checks for "has
/// persistence been closed?" while the pending map is empty — bounds how
/// long the task can linger after [`Engine::persist_close`] drops the last
/// strong reference to its [`PersistHandle`] with nothing left enqueuing
/// work to wake it via [`PersistHandle::notify`].
const IDLE_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// How many consecutive failed flush attempts a single pending entry
/// tolerates before it is dropped rather than retried again. Persistence is
/// a cache: dropping a stubbornly-unwritable entry just means that key is
/// recomputed (or serves a slightly staler cached value) on the next load,
/// never a correctness problem — but retrying forever would let a
/// persistently broken disk grow the pending map without bound.
const MAX_FLUSH_ATTEMPTS: u32 = 5;

/// Minimum gap between repeated "flush failed, will retry" warnings, so a
/// sustained failure (e.g. a full disk) logs at a sane rate instead of once
/// per debounce interval.
const WARN_THROTTLE: Duration = Duration::from_secs(5);

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
    use std::sync::atomic::AtomicU64 as CounterU64;
    use std::time::{SystemTime, UNIX_EPOCH};

    static COUNTER: CounterU64 = CounterU64::new(0);

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
///
/// Build with [`PersistOptions::new`], which fills in the flush-timing
/// knobs with their defaults; override any of them with the builder methods
/// ([`Self::flush_debounce`], [`Self::flush_max_pending`],
/// [`Self::flush_max_staleness`]) if the defaults don't suit a particular
/// deployment (a test wanting deterministic control over flush timing,
/// say).
#[derive(Debug, Clone)]
pub struct PersistOptions {
    /// The redb file to persist the dependency graph to. Created if it
    /// doesn't exist; wiped and recreated if it's unreadable or in an
    /// unrecognized format.
    pub path: PathBuf,
    /// Identifies "the code that computed these results" — see
    /// [`Fingerprint`].
    pub fingerprint: Fingerprint,
    /// The background persister task flushes once this much time has
    /// passed with nothing new enqueued (a "quiet period"). Every new
    /// enqueue restarts the clock. Default: 500ms.
    pub flush_debounce: Duration,
    /// The persister task flushes immediately, bypassing the debounce
    /// timer, once the pending (unflushed) map holds this many entries.
    /// Default: 100,000.
    pub flush_max_pending: usize,
    /// The persister task flushes immediately, bypassing the debounce
    /// timer, once the oldest currently-pending entry has waited this
    /// long — an upper bound on how much a crash can lose, independent of
    /// how continuously new work keeps resetting the debounce clock.
    /// Default: 5s.
    pub flush_max_staleness: Duration,
}

impl PersistOptions {
    /// Default quiet-period debounce (see [`Self::flush_debounce`]).
    pub const DEFAULT_FLUSH_DEBOUNCE: Duration = Duration::from_millis(500);
    /// Default pending-map size cap (see [`Self::flush_max_pending`]).
    pub const DEFAULT_FLUSH_MAX_PENDING: usize = 100_000;
    /// Default max-staleness cap (see [`Self::flush_max_staleness`]).
    pub const DEFAULT_FLUSH_MAX_STALENESS: Duration = Duration::from_secs(5);

    /// Builds `PersistOptions` for `path`/`fingerprint` with every
    /// flush-timing knob at its default. See the builder methods to
    /// override individual knobs.
    pub fn new(path: impl Into<PathBuf>, fingerprint: Fingerprint) -> Self {
        PersistOptions {
            path: path.into(),
            fingerprint,
            flush_debounce: Self::DEFAULT_FLUSH_DEBOUNCE,
            flush_max_pending: Self::DEFAULT_FLUSH_MAX_PENDING,
            flush_max_staleness: Self::DEFAULT_FLUSH_MAX_STALENESS,
        }
    }

    /// Overrides the quiet-period debounce. See [`Self::flush_debounce`].
    pub fn flush_debounce(mut self, debounce: Duration) -> Self {
        self.flush_debounce = debounce;
        self
    }

    /// Overrides the pending-map size cap. See [`Self::flush_max_pending`].
    pub fn flush_max_pending(mut self, max_pending: usize) -> Self {
        self.flush_max_pending = max_pending;
        self
    }

    /// Overrides the max-staleness cap. See [`Self::flush_max_staleness`].
    pub fn flush_max_staleness(mut self, max_staleness: Duration) -> Self {
        self.flush_max_staleness = max_staleness;
        self
    }
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

impl NodeRecord {
    /// Snapshots everything needed to persist `key`'s node, cloning its
    /// param/value/deps/hash bytes. Callers hold `EngineInner::nodes`'s
    /// lock while calling this (see [`enqueue_changed`]) so the snapshot
    /// can never observe a torn write from a concurrent rerun of the same
    /// node.
    ///
    /// Returns `None` for a node with no successful run yet (no
    /// `value_bytes`/`result_hash`) — nothing coherent to persist.
    fn snapshot(key: &CompKey, node: &Node) -> Option<NodeRecord> {
        let value_bytes = node.value_bytes.clone()?;
        let result_hash = node.result_hash?;
        Some(NodeRecord {
            def_name: key.def().name().to_string(),
            param_bytes: node.param_bytes.clone(),
            comp_deps: node.comp_deps.iter().map(CompKeyRepr::from_key).collect(),
            source_deps: node.source_deps.iter().map(RawDepRepr::from_dep).collect(),
            result_hash: result_hash.as_bytes(),
            value_bytes,
            outputs: node.outputs.iter().map(RawOutputRepr::from_output).collect(),
        })
    }
}

/// Encodes `key` into the same bytes used both as a `nodes` table primary
/// key and as a `comp_deps` entry — a pure function of `key`'s def name and
/// param hash, so it round-trips through [`CompKeyRepr::to_key`] without
/// needing the original parameter value.
fn encode_key(key: &CompKey) -> Vec<u8> {
    postcard::to_stdvec(&CompKeyRepr::from_key(key))
        .expect("postcard serialization of a well-formed value should not fail")
}

/// What a [`PendingMap`] entry says should happen to a key at the next
/// flush: write `record` (an update entry overwrites a prior update for the
/// same key), or delete whatever's on disk (a removal supersedes a pending
/// update for the same key — see [`PendingMap::upsert`]/[`PendingMap::remove`],
/// both of which are a plain `HashMap::insert` and so can never leave a key
/// present in "both" an update and a removal set, unlike a two-collection
/// design would risk).
enum EntryKind {
    Upsert(NodeRecord),
    Remove,
}

/// One [`PendingMap`] entry: what to do, plus how many times a flush has
/// already failed to write it (see [`MAX_FLUSH_ATTEMPTS`]).
struct PendingEntry {
    kind: EntryKind,
    attempts: u32,
}

/// Keys changed or removed since the last successful flush, accumulated as
/// the engine runs and drained by [`PersistHandle::flush`]. Record bytes
/// are snapshotted at enqueue time (see [`NodeRecord::snapshot`]), so the
/// background persister task that eventually flushes this never needs to
/// touch live nodes, and a round can never race an in-progress flush.
#[derive(Default)]
struct PendingMap {
    entries: HashMap<CompKey, PendingEntry>,
    /// When the currently-pending batch started accumulating: set on the
    /// first enqueue after the map was last empty (i.e. after the last
    /// flush drained it), cleared back to `None` by that drain. Read by the
    /// persister task to enforce [`PersistOptions::flush_max_staleness`].
    oldest_enqueue: Option<Instant>,
}

impl PendingMap {
    fn upsert(&mut self, key: CompKey, record: NodeRecord) {
        self.insert(key, PendingEntry { kind: EntryKind::Upsert(record), attempts: 0 });
    }

    fn remove(&mut self, key: CompKey) {
        self.insert(key, PendingEntry { kind: EntryKind::Remove, attempts: 0 });
    }

    fn insert(&mut self, key: CompKey, entry: PendingEntry) {
        if self.entries.is_empty() {
            self.oldest_enqueue = Some(Instant::now());
        }
        self.entries.insert(key, entry);
    }
}

/// The live persistence handle: the open database, flush-timing
/// configuration, and whatever has changed since the last flush. Held
/// behind an `Arc` in `EngineInner::persist` once
/// [`EngineInner::persist_load`] has established it; the background
/// persister task spawned alongside it holds only a [`Weak`] reference, so
/// dropping this `Arc` (in [`crate::driver::Engine::persist_close`]) is
/// enough to let that task notice and exit — see [`persister_loop`].
pub(crate) struct PersistHandle {
    db: Database,
    fingerprint: Fingerprint,
    pending: Mutex<PendingMap>,
    /// Wakes the persister task when something new is enqueued, so it can
    /// restart its debounce clock (or notice the pending map went from
    /// empty to non-empty) without polling tightly.
    ///
    /// A standalone `Arc`, not owned outright by this handle: the persister
    /// task holds its own clone of the *same* `Arc` independent of this
    /// handle's own lifetime (see [`persister_loop`]), so it can wait on it
    /// without needing to keep `db` — and the OS-level file lock that comes
    /// with it — open for the duration of that wait. If it held a strong
    /// `Arc<PersistHandle>` for that instead, [`crate::driver::Engine::persist_close`]
    /// would have no reliable way to actually close `db` promptly: it could
    /// drop its own reference yet still lose the race against however long
    /// the persister task's current debounce/staleness sleep had left to
    /// run.
    notify: Arc<Notify>,
    /// Serializes concurrent flush attempts (the persister task's own
    /// timer-driven flush racing a caller's manual `persist_now()`, say) so
    /// a flush always observes — and fully drains — whatever was pending by
    /// the time it was called, rather than two flushes each grabbing half
    /// the batch.
    flushing: AsyncMutex<()>,
    flush_debounce: Duration,
    flush_max_pending: usize,
    flush_max_staleness: Duration,
    last_warn: Mutex<Option<Instant>>,
    /// Number of flushes that have completed successfully. Test-only
    /// visibility into "has a flush happened yet" without reaching into a
    /// second redb handle on the same file.
    flush_count: AtomicU64,
}

impl PersistHandle {
    fn new(db: Database, fingerprint: Fingerprint, opts: &PersistOptions, notify: Arc<Notify>) -> Self {
        PersistHandle {
            db,
            fingerprint,
            pending: Mutex::new(PendingMap::default()),
            notify,
            flushing: AsyncMutex::new(()),
            flush_debounce: opts.flush_debounce,
            flush_max_pending: opts.flush_max_pending,
            flush_max_staleness: opts.flush_max_staleness,
            last_warn: Mutex::new(None),
            flush_count: AtomicU64::new(0),
        }
    }

    /// Records that `key`'s node just produced a genuinely new result and
    /// should be (re-)saved at the next flush. `record` must already
    /// reflect that node's state at the moment of the call (see
    /// [`NodeRecord::snapshot`]).
    fn enqueue_upsert(&self, key: CompKey, record: NodeRecord) {
        self.pending.lock().unwrap().upsert(key, record);
        self.notify.notify_one();
    }

    /// Records that `key`'s node was just collected by liveness GC and its
    /// record should be deleted at the next flush.
    fn enqueue_remove(&self, key: CompKey) {
        self.pending.lock().unwrap().remove(key);
        self.notify.notify_one();
    }

    /// Drains whatever is currently pending and writes it in one redb
    /// write transaction, off the calling task via `spawn_blocking`. A
    /// no-op if nothing is pending. On failure, re-enqueues every entry
    /// from this batch that hasn't yet exhausted [`MAX_FLUSH_ATTEMPTS`] (see
    /// [`Self::requeue_after_failure`]) so the next flush retries it.
    async fn flush(self: &Arc<Self>) {
        let _guard = self.flushing.lock().await;

        let taken = {
            let mut pending = self.pending.lock().unwrap();
            if pending.entries.is_empty() {
                return;
            }
            std::mem::take(&mut *pending)
        };

        let mut upserts: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(taken.entries.len());
        let mut deletes: Vec<Vec<u8>> = Vec::new();
        for (key, entry) in &taken.entries {
            match &entry.kind {
                EntryKind::Upsert(record) => {
                    let bytes = postcard::to_stdvec(record)
                        .expect("postcard serialization of a well-formed value should not fail");
                    upserts.push((encode_key(key), bytes));
                }
                EntryKind::Remove => deletes.push(encode_key(key)),
            }
        }
        let upsert_count = upserts.len();
        let delete_count = deletes.len();

        let start = Instant::now();
        let write_result = {
            let handle = self.clone();
            tokio::task::spawn_blocking(move || write_batch(&handle.db, handle.fingerprint, &upserts, &deletes)).await
        };
        let elapsed_ms = start.elapsed().as_millis() as u64;

        match write_result {
            Ok(Ok(())) => {
                self.flush_count.fetch_add(1, Ordering::SeqCst);
                tracing::debug!(
                    upserts = upsert_count,
                    deletes = delete_count,
                    elapsed_ms,
                    "persistence: flush complete"
                );
            }
            Ok(Err(e)) => self.requeue_after_failure(taken.entries, &e.to_string()),
            Err(e) => self.requeue_after_failure(taken.entries, &format!("flush task panicked: {e}")),
        }
    }

    /// After a failed write, bumps every affected entry's attempt count,
    /// permanently drops (with a loud warning) any that just exceeded
    /// [`MAX_FLUSH_ATTEMPTS`], and merges the rest back into the live
    /// pending map — but only where a fresher enqueue hasn't already
    /// replaced that key while the failed flush was in flight (that fresher
    /// entry already supersedes the one that just failed to write).
    fn requeue_after_failure(&self, mut entries: HashMap<CompKey, PendingEntry>, error: &str) {
        let mut dropped = 0usize;
        entries.retain(|_, entry| {
            entry.attempts += 1;
            let keep = entry.attempts <= MAX_FLUSH_ATTEMPTS;
            if !keep {
                dropped += 1;
            }
            keep
        });

        let should_warn = {
            let mut last_warn = self.last_warn.lock().unwrap();
            let now = Instant::now();
            let should = match *last_warn {
                None => true,
                Some(t) => now.duration_since(t) >= WARN_THROTTLE,
            };
            if should {
                *last_warn = Some(now);
            }
            should
        };
        if should_warn {
            tracing::warn!(
                error = %error,
                pending_retry = entries.len(),
                "persistence: flush failed; will retry at the next flush"
            );
        }
        if dropped > 0 {
            tracing::warn!(
                dropped,
                "persistence: dropping pending entries after {MAX_FLUSH_ATTEMPTS} failed flush attempts; \
                 affected keys will simply be recomputed (or serve their last-persisted value) on the \
                 next load — persistence is a cache, never a source of truth"
            );
        }

        if entries.is_empty() {
            return;
        }
        let mut pending = self.pending.lock().unwrap();
        let was_empty = pending.entries.is_empty();
        for (key, entry) in entries {
            pending.entries.entry(key).or_insert(entry);
        }
        if was_empty && !pending.entries.is_empty() {
            pending.oldest_enqueue = Some(Instant::now());
        }
    }

    /// Test-only: how many entries are currently unflushed.
    #[cfg(any(test, feature = "testutil"))]
    pub(crate) fn pending_count(&self) -> usize {
        self.pending.lock().unwrap().entries.len()
    }

    /// Test-only: how many flushes have completed successfully so far.
    #[cfg(any(test, feature = "testutil"))]
    pub(crate) fn flush_count(&self) -> u64 {
        self.flush_count.load(Ordering::SeqCst)
    }
}

/// The background persister task's whole life: loop forever, waiting for
/// something to be pending and then for the earliest of the debounce quiet
/// period, the size cap, or the max-staleness cap, flushing whenever one
/// fires.
///
/// Deliberately holds the [`PersistHandle`] itself only via a [`Weak`]
/// reference, upgraded momentarily just to peek at pending state or to
/// flush, and never across a wait: waiting instead goes through `notify`, a
/// standalone `Arc<Notify>` this task holds independent of the handle's own
/// lifetime. This is what lets [`crate::driver::Engine::persist_close`]
/// close the underlying database *promptly* — if this task instead held a
/// strong `Arc<PersistHandle>` for the whole debounce/staleness sleep (up
/// to [`PersistOptions::flush_max_staleness`], which a caller can configure
/// arbitrarily long), `persist_close` dropping its own reference wouldn't
/// be enough: the database would stay open, and its OS-level file lock
/// held, until whichever sleep this task happened to be in the middle of
/// finally elapsed.
async fn persister_loop(weak: Weak<PersistHandle>, notify: Arc<Notify>) {
    loop {
        // Phase 1: idle until something is pending (or the handle is gone).
        loop {
            let has_pending = match weak.upgrade() {
                Some(handle) => !handle.pending.lock().unwrap().entries.is_empty(),
                None => return,
            };
            if has_pending {
                break;
            }
            tokio::select! {
                _ = notify.notified() => {}
                _ = tokio::time::sleep(IDLE_POLL_INTERVAL) => {}
            }
        }

        // Phase 2: something is pending — wait for the earliest of quiet
        // debounce / size cap / max staleness, then flush. Every new
        // enqueue (`notify`) restarts this wait from scratch, which is
        // exactly what "debounce" means; the size/staleness checks at the
        // top of each iteration ensure a steady trickle of enqueues can
        // never postpone a flush past `flush_max_staleness`.
        'wait: loop {
            let outcome = match weak.upgrade() {
                Some(handle) => {
                    let (len, oldest, debounce, max_pending, max_staleness) = {
                        let pending = handle.pending.lock().unwrap();
                        (
                            pending.entries.len(),
                            pending.oldest_enqueue,
                            handle.flush_debounce,
                            handle.flush_max_pending,
                            handle.flush_max_staleness,
                        )
                    };
                    let staleness_remaining =
                        oldest.map(|t| max_staleness.saturating_sub(t.elapsed())).unwrap_or(max_staleness);
                    if len >= max_pending || staleness_remaining.is_zero() {
                        WaitOutcome::FlushNow
                    } else {
                        WaitOutcome::Wait { debounce, staleness_remaining }
                    }
                }
                None => return,
            };

            match outcome {
                WaitOutcome::FlushNow => {
                    let Some(handle) = weak.upgrade() else { return };
                    handle.flush().await;
                    break 'wait;
                }
                WaitOutcome::Wait { debounce, staleness_remaining } => {
                    tokio::select! {
                        _ = tokio::time::sleep(debounce) => {}
                        _ = tokio::time::sleep(staleness_remaining) => {}
                        _ = notify.notified() => {
                            // Something new arrived: loop back around to
                            // recompute len/oldest and restart the wait.
                            continue 'wait;
                        }
                    }
                    let Some(handle) = weak.upgrade() else { return };
                    handle.flush().await;
                    break 'wait;
                }
            }
        }
    }
}

/// What [`persister_loop`]'s phase-2 wait decided to do, computed while
/// briefly holding an upgraded [`PersistHandle`] and then acted on after
/// dropping it — so the actual wait (or flush) never needs the handle held
/// any longer than each individual step requires.
enum WaitOutcome {
    FlushNow,
    Wait { debounce: Duration, staleness_remaining: Duration },
}

impl EngineInner {
    /// Loads the persisted graph, if [`crate::EngineBuilder::persistence`]
    /// was ever called — a no-op otherwise. Called once, by
    /// [`crate::driver`]'s `Engine::run`, before the initial evaluation.
    /// Also spawns the background persister task that owns all future
    /// flushing for this engine's lifetime (or until
    /// [`crate::driver::Engine::persist_close`]).
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

        let notify = Arc::new(Notify::new());
        let handle = Arc::new(PersistHandle::new(db, opts.fingerprint, &opts, notify.clone()));
        *self.persist.lock().unwrap() = Some(handle.clone());
        tokio::spawn(persister_loop(Arc::downgrade(&handle), notify));
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
    /// Forces an immediate flush of everything currently pending and awaits
    /// its completion — the implementation behind
    /// [`crate::driver::Engine::persist_now`]. A no-op if persistence isn't
    /// configured, or if nothing is pending.
    ///
    /// Note this does *not* itself enqueue anything: by the time it's
    /// called, every settled node's record (and every GC'd node's removal)
    /// has already been enqueued synchronously — see [`enqueue_changed`]
    /// and [`mark_removed`] — so "force an immediate flush" is the whole
    /// job.
    pub(crate) async fn persist_force_flush(&self) {
        let handle = self.persist.lock().unwrap().clone();
        if let Some(handle) = handle {
            handle.flush().await;
        }
    }

    /// Test-only: how many entries are currently unflushed, or `0` if
    /// persistence isn't configured/loaded yet.
    #[cfg(any(test, feature = "testutil"))]
    pub(crate) fn persist_pending_count(&self) -> usize {
        self.persist.lock().unwrap().as_ref().map(|h| h.pending_count()).unwrap_or(0)
    }

    /// Test-only: how many flushes have completed successfully so far, or
    /// `0` if persistence isn't configured/loaded yet.
    #[cfg(any(test, feature = "testutil"))]
    pub(crate) fn persist_flush_count(&self) -> u64 {
        self.persist.lock().unwrap().as_ref().map(|h| h.flush_count()).unwrap_or(0)
    }
}

/// Snapshots `key`'s node (see [`NodeRecord::snapshot`]) and enqueues it for
/// the next flush, if persistence is configured. Called by
/// `EngineInner::run` right after a successful execution whose result hash
/// actually changed, while `node`'s entry in `EngineInner::nodes` is still
/// locked — so the snapshot can never race a subsequent rerun of the same
/// node starting before this enqueue completes.
pub(crate) fn enqueue_changed(engine: &EngineInner, key: &CompKey, node: &Node) {
    let Some(handle) = engine.persist.lock().unwrap().clone() else { return };
    let Some(record) = NodeRecord::snapshot(key, node) else { return };
    handle.enqueue_upsert(key.clone(), record);
}

/// Marks `key`'s node removed (for the next flush), if persistence is
/// configured. Called by `crate::driver`'s liveness GC for every node it
/// collects.
pub(crate) fn mark_removed(engine: &EngineInner, key: CompKey) {
    if let Some(handle) = engine.persist.lock().unwrap().clone() {
        handle.enqueue_remove(key);
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
