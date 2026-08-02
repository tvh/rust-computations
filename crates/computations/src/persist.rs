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

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use tokio::sync::{Mutex as AsyncMutex, Notify};

use crate::def::ErasedDef;
use crate::engine::{DirtyPriority, EngineInner, NodeRef, NodeTable, SourceRefs};
use crate::flow::FlowId;
use crate::interner::{SmallVerBytes, SrcDep, SrcKeyId, SrcKeyInterner};
use crate::key::{CompKey, CompKeyMap, CompKeySet, DefId, Hash128};
use crate::sink::{OutBytes, RawOutput, RawOutputs, SinkId};
use crate::source::{KeyBytes, RawDep, RawDeps, SourceId, VerBytes};

/// The on-disk record format version.
///
/// Bumping this forces every existing persisted database to be treated as
/// unreadable (wiped, cold start) on next load — the escape hatch for a
/// breaking change to the record encoding itself. This is independent of
/// [`Fingerprint`], which is about the *code that produced the values*, not
/// the *encoding of the store*: a format bump always wipes, a fingerprint
/// mismatch only ever revalidates in the background.
///
/// Bumped 2 -> 3 for Stage 9 (see `docs/persistence-benchmark-notes.md`):
/// [`NodeRecord`] gained `flow_ids`, so an older v2 database's records don't
/// have that field at all — rather than guess a default for every existing
/// row, the mismatch path above wipes and recomputes cold, exactly as it
/// already does for any other encoding change.
const FORMAT_VERSION: u8 = 3;

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
        Self::current_exe_with([])
    }

    /// Fingerprints the current process's own executable, mixed with
    /// caller-chosen `extra` data (a config hash, a version string, a
    /// feature-flag set, ...) — the common "I want to invalidate on a
    /// binary change *and* on a config change" case, without giving up
    /// [`Self::current_exe`]'s self-correcting property (see
    /// [`Self::custom`]'s docs for what that property is and why giving it
    /// up is a real hazard).
    ///
    /// Falls back to [`Self::current_exe`]'s same random-fingerprint
    /// behavior (and the same warning) if the executable can't be read;
    /// `extra` is simply not mixed in in that case, since a random
    /// fingerprint already can never match a previously stored one.
    pub fn current_exe_with(extra: impl AsRef<[u8]>) -> Self {
        match std::env::current_exe().and_then(std::fs::read) {
            Ok(bytes) => {
                let mut hasher = blake3::Hasher::new();
                hasher.update(&bytes);
                hasher.update(extra.as_ref());
                Fingerprint(*hasher.finalize().as_bytes())
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "Fingerprint::current_exe_with: failed to read the current executable; \
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
    ///
    /// **Hazard: this is only as trustworthy as `data` itself.**
    /// [`Self::current_exe`] is self-correcting — it hashes the actual
    /// running binary, so *any* code change automatically produces a
    /// different fingerprint, with nothing for the caller to remember to
    /// update. `custom` has no such guarantee: it trusts `data` completely.
    /// Change a computation body's logic without also bumping whatever
    /// string or hash you pass here, and every persisted result for that
    /// computation is silently served as a `Clean` cache hit forever —
    /// indistinguishable, at load time, from a legitimate one (see
    /// `crate::persist`'s module docs on how little a fingerprint match is
    /// actually trusted to mean). There is no detection for this: a stale
    /// `custom` fingerprint is not corruption, not a decode failure, not
    /// anything this module's "always safe to drop and recompute" story
    /// covers — it looks exactly like a correct cache hit.
    ///
    /// Prefer [`Self::current_exe`] (or [`Self::current_exe_with`] if a
    /// config value should *also* invalidate the cache) whenever possible;
    /// reach for `custom` alone only when the running binary genuinely
    /// isn't a meaningful proxy for "the code that produced these results"
    /// (e.g. a stable long-running host process that reloads computation
    /// logic without restarting).
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
    param_hash: [u8; 16],
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
        Some(CompKey::from_parts(def_id, Hash128::from_bytes(self.param_hash)))
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
    /// The persisted counterpart of a node's *interned* [`SrcDep`]:
    /// resolves `dep.key_id` back to real `(source, key)` bytes through
    /// `interner`, since the on-disk record must never store a
    /// process-local id — see `crate::interner`'s module docs on why
    /// persistence stays byte-based. `None` if `dep.key_id` has already
    /// been released (should not happen while the owning node still holds
    /// it — see `crate::engine::EngineInner::reconcile_source_deps` — but
    /// this is a snapshot read racing nothing under the same lock, so it's
    /// handled rather than assumed).
    fn from_src_dep(interner: &SrcKeyInterner, dep: &SrcDep) -> Option<Self> {
        let (source, key) = interner.resolve(dep.key_id)?;
        Some(RawDepRepr {
            source: source.to_string(),
            key: key.to_vec(),
            ver: dep.ver.to_vec(),
        })
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

/// One `nodes` table row: everything needed to revive a `Clean` node
/// without ever re-executing its computation. The on-disk shape — always
/// carries fully-serialized `value_bytes`, satisfying the standing
/// requirement that the *database* keep every byte of information it always
/// has, even though the in-memory node this is built from no longer keeps a
/// permanent serialized copy of its own value (see [`PendingRecord`],
/// which is what actually gets built at enqueue time; `value_bytes` is only
/// ever filled in lazily, at flush time, from [`PendingRecord::value`]).
#[derive(Serialize, Deserialize)]
struct NodeRecord {
    def_name: String,
    param_bytes: Vec<u8>,
    comp_deps: Vec<CompKeyRepr>,
    source_deps: Vec<RawDepRepr>,
    result_hash: [u8; 16],
    value_bytes: Vec<u8>,
    outputs: Vec<RawOutputRepr>,
    /// This node's ordered flow ids (Stage 9, format v3 — see
    /// `docs/persistence-benchmark-notes.md`), empty for an ordinary
    /// builder-path node. Stored directly as `Vec<FlowId>` rather than
    /// through a `*Repr` stand-in type (contrast `RawDepRepr`/
    /// `RawOutputRepr`, both written before `SourceId`/`SinkId` gained
    /// hand-written `Serialize`/`Deserialize` impls): `FlowId` embeds those
    /// ids directly and is itself postcard-friendly now, so no adapter is
    /// needed here.
    flow_ids: Vec<FlowId>,
}

/// The in-memory, not-yet-written counterpart of [`NodeRecord`]: everything
/// a changed node's [`PendingMap`] entry needs, except `value_bytes` — which
/// is deliberately *not* computed yet. `enqueue_changed` builds one of these
/// while `EngineInner::nodes` is locked, cloning `value` (an `Arc`, so this
/// is a cheap refcount bump, not a deep copy); the actual postcard encoding
/// of that value happens later, in [`PersistHandle::flush`], entirely
/// outside that lock — see this module's docs for why that split matters
/// (a value can be arbitrarily larger than the rest of a node's metadata,
/// so serializing it is the one part of "record a change" worth deferring
/// off the hot path, and worth skipping entirely for an entry that gets
/// coalesced by a fresher enqueue before the next flush ever happens).
struct PendingRecord {
    def_name: String,
    param_bytes: Vec<u8>,
    comp_deps: Vec<CompKeyRepr>,
    source_deps: Vec<RawDepRepr>,
    result_hash: [u8; 16],
    value: Arc<dyn Any + Send + Sync>,
    outputs: Vec<RawOutputRepr>,
    /// See [`NodeRecord::flow_ids`].
    flow_ids: Vec<FlowId>,
}

impl PendingRecord {
    /// Snapshots everything needed to persist `r`'s node, given its already
    /// resolved `value`/`result_hash` (see [`enqueue_changed`], the only
    /// caller: it reads those two off `r`'s typed value column /
    /// `result_hash` column before calling this, since `NodeTable` itself
    /// stays generic over every definition's `R` — see
    /// `crate::def::CompDef::values`'s docs). Callers hold
    /// `EngineInner::nodes`'s lock while calling this so the snapshot can
    /// never observe a torn write from a concurrent rerun of the same node.
    /// `nodes` is needed to translate `r`'s `comp_deps` [`NodeRef`]s back
    /// into the `CompKey`s persistence actually stores, and to read `r`'s
    /// `source_deps`/`outputs` out of `nodes`'s sparse side tables.
    ///
    /// `interner` resolves each of `r`'s interned `source_deps` back to the
    /// real `(source, key)` bytes the on-disk record stores (Stage 13 —
    /// see `docs/persistence-benchmark-notes.md`): the persisted format is
    /// unchanged by interning, only how the in-memory side reaches it. A
    /// dep whose id has already been released (should not happen while `r`
    /// still holds it) is simply dropped rather than trusted — see
    /// [`RawDepRepr::from_src_dep`].
    fn snapshot(
        nodes: &NodeTable,
        interner: &SrcKeyInterner,
        r: NodeRef,
        key: &CompKey,
        value: Arc<dyn Any + Send + Sync>,
        result_hash: Hash128,
    ) -> PendingRecord {
        let comp_deps: Vec<CompKeyRepr> =
            nodes.comp_deps(r).iter().map(|&dep| CompKeyRepr::from_key(&nodes.key_of(dep))).collect();
        PendingRecord {
            def_name: key.def().name().to_string(),
            param_bytes: nodes.param_bytes(r).to_vec(),
            comp_deps,
            source_deps: nodes.source_deps_iter(r).filter_map(|dep| RawDepRepr::from_src_dep(interner, dep)).collect(),
            result_hash: result_hash.as_bytes(),
            value,
            outputs: nodes.outputs_iter(r).map(RawOutputRepr::from_output).collect(),
            flow_ids: nodes.flow_ids_clone(r),
        }
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
    Upsert(PendingRecord),
    Remove,
}

/// One [`PendingMap`] entry: what to do, plus how many times a flush has
/// already failed to write it (see [`MAX_FLUSH_ATTEMPTS`]).
struct PendingEntry {
    kind: EntryKind,
    attempts: u32,
}

/// Keys changed or removed since the last successful flush, accumulated as
/// the engine runs and drained by [`PersistHandle::flush`]. Every field
/// except the eventual value bytes is snapshotted at enqueue time (see
/// [`PendingRecord::snapshot`]), so the background persister task that
/// eventually flushes this never needs to touch live nodes, and a round can
/// never race an in-progress flush; the value itself is serialized lazily,
/// at flush time (see [`PendingRecord`]'s docs).
#[derive(Default)]
struct PendingMap {
    /// Keyed by `CompKey`, whose hash is dominated by a `Hash128` — see
    /// [`CompKeyMap`] and `crate::hashers`' docs.
    entries: CompKeyMap<PendingEntry>,
    /// When the currently-pending batch started accumulating: set on the
    /// first enqueue after the map was last empty (i.e. after the last
    /// flush drained it), cleared back to `None` by that drain. Read by the
    /// persister task to enforce [`PersistOptions::flush_max_staleness`].
    oldest_enqueue: Option<Instant>,
}

impl PendingMap {
    fn upsert(&mut self, key: CompKey, record: PendingRecord) {
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
    /// A snapshot of `EngineInner::erased_defs`, cloned once at construction
    /// (cheap: it's a map of `Arc`s, and the set of registered definitions
    /// never changes after an engine is built). Lets [`Self::flush`] encode
    /// each pending entry's erased value into bytes (see
    /// [`crate::def::ErasedDef::serialize_value`]) without needing a live
    /// reference back to `EngineInner` — keeping the persister task's
    /// footprint limited to exactly what flushing needs (see
    /// [`persister_loop`]'s docs on why it only ever holds a `Weak`
    /// reference to this handle, let alone the whole engine).
    erased_defs: HashMap<DefId, Arc<dyn ErasedDef>>,
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
    fn new(
        db: Database,
        fingerprint: Fingerprint,
        opts: &PersistOptions,
        notify: Arc<Notify>,
        erased_defs: HashMap<DefId, Arc<dyn ErasedDef>>,
    ) -> Self {
        PersistHandle {
            db,
            fingerprint,
            erased_defs,
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
    /// [`PendingRecord::snapshot`]).
    fn enqueue_upsert(&self, key: CompKey, record: PendingRecord) {
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
                // `value_bytes` is derived here, not at enqueue time: this
                // is the "serialize outside the lock" step (see
                // `PendingRecord`'s docs) — `EngineInner::nodes` was never
                // held while any of this ran, and an entry coalesced away by
                // a fresher enqueue before reaching this point (see
                // `PendingMap::upsert`) never pays this cost at all.
                EntryKind::Upsert(pending) => {
                    let Some(erased_def) = self.erased_defs.get(key.def()) else {
                        // Can't happen for an entry this handle itself
                        // enqueued (its def was registered when the owning
                        // node was created) — defensive, not a real path.
                        tracing::debug!(
                            def = %key.def().name(),
                            "persistence: flush: no erased def for a pending upsert's definition; dropping this entry"
                        );
                        continue;
                    };
                    let value_bytes = match erased_def.serialize_value(&pending.value) {
                        Ok(bytes) => bytes,
                        Err(e) => {
                            // A failing `R: Serialize` impl, not an engine
                            // bug (see `ErasedDef::serialize_value`'s docs)
                            // -- drop just this one pending upsert rather
                            // than letting it panic the persister task.
                            tracing::warn!(
                                def = %key.def().name(),
                                error = %e,
                                "persistence: flush: value failed to serialize; dropping this pending upsert"
                            );
                            continue;
                        }
                    };
                    let record = NodeRecord {
                        def_name: pending.def_name.clone(),
                        param_bytes: pending.param_bytes.clone(),
                        comp_deps: pending.comp_deps.clone(),
                        source_deps: pending.source_deps.clone(),
                        result_hash: pending.result_hash,
                        value_bytes,
                        outputs: pending.outputs.clone(),
                        flow_ids: pending.flow_ids.clone(),
                    };
                    let bytes = postcard::to_stdvec(&record)
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
    fn requeue_after_failure(&self, mut entries: CompKeyMap<PendingEntry>, error: &str) {
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
        let handle = Arc::new(PersistHandle::new(
            db,
            opts.fingerprint,
            &opts,
            notify.clone(),
            self.erased_defs.clone(),
        ));
        *self.persist.lock().unwrap() = Some(handle.clone());
        tokio::spawn(persister_loop(Arc::downgrade(&handle), notify));
    }

    /// Turns every decodable, still-registered record into a `Clean` row,
    /// inserting every one of them into `self.nodes` (assigning each a
    /// fresh [`NodeRef`]), then — now that every restored node has a ref —
    /// wires up `comp_deps`/`rdeps` and rebuilds `source_index` from the
    /// whole restored batch. Returns the number of nodes actually restored.
    ///
    /// The two-pass shape (decode-and-insert, then wire up edges) is
    /// required by [`NodeRef`]-based edges: a dependency edge can only be
    /// expressed once *both* of its endpoints already have a ref, which
    /// isn't true of any single record in isolation (it may name a
    /// dependency that itself appears later in the same batch, or not at
    /// all — see the second pass's handling of a dangling `comp_deps`
    /// entry).
    ///
    /// ## Load-time key verification
    ///
    /// `record.param_bytes` was decoded, then re-serialized by
    /// [`crate::def::ErasedDef::revive_key`] to recompute this record's
    /// `CompKey` (the on-disk record itself never stores the key directly —
    /// only the definition name and the param bytes). If that recomputed
    /// key doesn't match `stored_key_bytes` (the actual primary key this
    /// record was filed under — redb's own table key, threaded through from
    /// [`open_and_read`]), the record is dropped with a loud warning rather
    /// than trusted.
    ///
    /// This is not a defensive nicety: a mismatch here means
    /// `record.param_bytes` re-serializes to different bytes than it did
    /// when this record was written — in practice, almost always a
    /// non-deterministic param `Serialize` impl (see [`CompParam`]'s docs).
    /// Trusting the recomputed key anyway would silently attach this
    /// record's value/deps/outputs to the *wrong* live identity: the record
    /// can never be found again under the key it actually holds, so it
    /// looks unreachable to the driver's liveness GC, which then deletes
    /// its sink outputs — while the real, still-live identity recomputes
    /// cold, having lost nothing from *its* perspective. Dropping the
    /// record here instead means exactly one node recomputes cold on this
    /// restart, with a warning naming the culprit definition — a diagnosable
    /// cold start instead of a silent orphan-and-delete.
    fn restore_nodes(&self, records: Vec<StoredRecord>) -> usize {
        struct PendingNode {
            key: CompKey,
            param_bytes: Vec<u8>,
            result_hash: Hash128,
            value_bytes: Vec<u8>,
            erased_def: Arc<dyn ErasedDef>,
            comp_dep_keys: Vec<CompKey>,
            source_deps: RawDeps,
            outputs: RawOutputs,
            flow_ids: Vec<FlowId>,
        }

        let mut pending: Vec<PendingNode> = Vec::with_capacity(records.len());

        for (stored_key_bytes, record) in records {
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
            // Try the flow-less revival first (the ordinary builder-path
            // case), falling back to the flow-aware one only when that
            // reports "not applicable" (`None` -- always the case for a
            // flow-argument def, whose `param_bytes` alone can't
            // reconstruct its identity; see `crate::def::ErasedDef::revive_key`'s
            // docs). Mirrors `EngineInner::rerun_node`'s identical
            // try-`rerun`-then-`rerun_flows` ordering, for the same reason:
            // neither side has to know in advance which kind of def a
            // record names.
            let key = erased_def
                .revive_key(&record.param_bytes)
                .or_else(|| erased_def.revive_key_flows(&record.flow_ids, &record.param_bytes));
            let Some(key) = key else {
                tracing::debug!(def = %record.def_name, "persistence: dropping a record whose param bytes failed to decode");
                continue;
            };

            // Load-time key verification -- see this method's docs.
            if encode_key(&key) != stored_key_bytes {
                tracing::warn!(
                    def = %record.def_name,
                    "persistence: dropping a record whose recomputed key does not match the key it was \
                     stored under -- almost always a non-deterministic parameter Serialize impl (e.g. a \
                     HashMap/HashSet field; see `computations::CompParam`'s docs). This node will simply \
                     recompute cold rather than risk attaching to the wrong live identity."
                );
                continue;
            }

            // The engine's node table caps a single row's param span at
            // `u16::MAX` bytes (see `engine::DefTable::insert`); checked
            // here, before this record ever reaches that table, for the
            // same reason `EngineInner::prepare` checks it before taking
            // the node-table lock on the live-eval path -- see Stage 7 of
            // `docs/persistence-benchmark-notes.md`.
            if record.param_bytes.len() > u16::MAX as usize {
                tracing::warn!(
                    def = %record.def_name,
                    len = record.param_bytes.len(),
                    "persistence: dropping a record whose param bytes exceed this engine's per-node size limit"
                );
                continue;
            }

            let comp_dep_keys: Vec<CompKey> =
                record.comp_deps.iter().filter_map(|dep| dep.to_key(&self.def_names)).collect();
            // `RawDeps`/`RawOutputs`, not `HashSet`s (Stage 20 — see
            // `docs/persistence-benchmark-notes.md`): `record.source_deps`/
            // `record.outputs` were themselves snapshotted from an
            // already-deduplicated set (`PendingRecord::snapshot`), so this
            // is another instance of the identical "1:1 map preserves
            // uniqueness for free" reasoning `raw_deps` relies on, this time
            // on the warm-restart path Stage 17's profile (b) caught paying
            // the same `HashSet<RawDep>`/`HashSet<RawOutput>` construction
            // cost.
            let source_deps: RawDeps = record.source_deps.iter().map(RawDepRepr::to_dep).collect();
            let outputs: RawOutputs = record.outputs.iter().map(RawOutputRepr::to_output).collect();

            pending.push(PendingNode {
                key,
                param_bytes: record.param_bytes,
                result_hash: Hash128::from_bytes(record.result_hash),
                value_bytes: record.value_bytes,
                erased_def: erased_def.clone(),
                comp_dep_keys,
                source_deps,
                outputs,
                flow_ids: record.flow_ids,
            });
        }

        if pending.is_empty() {
            return 0;
        }

        let mut nodes = self.nodes.lock().unwrap();
        // Held for the whole two-pass restore below, nested inside `nodes`
        // exactly like `enqueue_changed`'s ordering (Stage 13 — see
        // `docs/persistence-benchmark-notes.md`): every restored dep is a
        // brand-new reference (a freshly restored node has no prior "old"
        // set to diff against), so each one is interned *and* retained in
        // one call, mirroring `crate::interner::SrcKeyInterner::intern_retain`'s
        // own doc comment.
        let mut interner = self.src_key_interner.lock().unwrap();

        // Pass 1: every restored node gets a `NodeRef`, its value is decoded
        // straight into its definition's typed value column (see
        // `crate::def::ErasedDef::revive_and_store` — a record whose value
        // bytes fail to decode here has its just-inserted row un-inserted
        // again rather than left half-formed), and — now that it has a ref
        // — its `source_deps`/`outputs` land in `NodeTable`'s sparse side
        // tables directly.
        let mut edge_work: Vec<(NodeRef, Vec<CompKey>)> = Vec::with_capacity(pending.len());
        for p in pending {
            let r = nodes.insert_new(&p.key, &p.param_bytes);
            if !p.erased_def.revive_and_store(r.row, &p.value_bytes) {
                tracing::debug!(
                    def = %p.key.def().name(),
                    "persistence: dropping a record whose value bytes failed to decode"
                );
                nodes.remove_by_id(r);
                continue;
            }
            nodes.set_result(r, p.result_hash);
            let interned_deps: SmallVec<[SrcDep; 1]> = p
                .source_deps
                .iter()
                .map(|dep| SrcDep {
                    key_id: interner.intern_retain(&dep.source, &dep.key),
                    ver: SmallVerBytes::from_slice(&dep.ver),
                })
                .collect();
            nodes.extend_source_deps(r, &interned_deps);
            nodes.extend_outputs(r, &p.outputs);
            nodes.set_flow_ids(r, p.flow_ids);
            edge_work.push((r, p.comp_dep_keys));
        }
        drop(interner);
        let restored_count = edge_work.len();

        // Pass 2: wire up `comp_deps`/`rdeps` now that every ref exists. A
        // dependency key with no matching ref (its own record failed to
        // decode, or named an unregistered definition) is simply dropped —
        // exactly as silently as `crate::driver`'s liveness GC already
        // tolerates a `comp_deps` entry pointing nowhere.
        for (caller_r, dep_keys) in edge_work {
            for dep_key in dep_keys {
                let Some(callee_r) = nodes.id_of(&dep_key) else { continue };
                nodes.push_comp_dep(caller_r, callee_r);
                nodes.push_rdep(callee_r, caller_r);
            }
        }

        // Rebuild `source_index` the same way `record_source_deps` builds it
        // incrementally for a live node.
        let source_index_entries: Vec<(SrcKeyId, NodeRef)> =
            nodes.iter_refs().flat_map(|r| nodes.source_deps_iter(r).map(move |dep| (dep.key_id, r))).collect();
        drop(nodes);

        {
            let mut source_index = self.source_index.lock().unwrap();
            for (key_id, r) in source_index_entries {
                match source_index.entry(key_id) {
                    std::collections::hash_map::Entry::Vacant(v) => {
                        v.insert(SourceRefs::One(r));
                    }
                    std::collections::hash_map::Entry::Occupied(mut occ) => occ.get_mut().insert(r),
                }
            }
        }

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
async fn probe_restored_source_deps(engine: &EngineInner) -> CompKeySet {
    // Snapshot what's needed and release the lock before awaiting anything
    // (holding a `std::sync::Mutex` guard across an `.await` is unsound to
    // rely on and easy to deadlock).
    let deps_by_key: CompKeyMap<HashSet<SrcDep>> = {
        let nodes = engine.nodes.lock().unwrap();
        nodes.iter_refs().map(|r| (nodes.key_of(r), nodes.source_deps_clone(r))).collect()
    };

    // Resolve every interned key back to real `(source, key)` bytes up
    // front, through a single `src_key_interner` lock -- a one-time,
    // startup-only cost (Stage 13 — see
    // `docs/persistence-benchmark-notes.md`), unlike the per-dependent hot
    // path interning targets. A dep whose id has already been released
    // (should not happen for a node whose `source_deps` snapshot above
    // still lists it) is simply dropped rather than trusted.
    let resolved: CompKeyMap<Vec<(SourceId, KeyBytes, VerBytes)>> = {
        let interner = engine.src_key_interner.lock().unwrap();
        deps_by_key
            .iter()
            .map(|(key, deps)| {
                let deps = deps
                    .iter()
                    .filter_map(|dep| {
                        let (source, key_bytes) = interner.resolve(dep.key_id)?;
                        Some((source.clone(), key_bytes.to_vec(), dep.ver.to_vec()))
                    })
                    .collect();
                (key.clone(), deps)
            })
            .collect()
    };

    let mut by_source: HashMap<SourceId, HashSet<KeyBytes>> = HashMap::new();
    for deps in resolved.values() {
        for (source, key, _ver) in deps {
            by_source.entry(source.clone()).or_default().insert(key.clone());
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

    let mut changed = CompKeySet::default();
    for (key, deps) in &resolved {
        for (source, key_bytes, ver) in deps {
            let unchanged = matches!(
                probed.get(source),
                Some(Some(map)) if matches!(map.get(key_bytes), Some(Some(v)) if v == ver)
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
fn mark_dirty_transitive(engine: &EngineInner, initial: CompKeySet, priority: DirtyPriority) {
    let mut seen: CompKeySet = CompKeySet::default();
    let mut frontier = initial;
    while !frontier.is_empty() {
        engine.mark_dirty_quiet(&frontier, priority);
        seen.extend(frontier.iter().cloned());

        let nodes = engine.nodes.lock().unwrap();
        let mut next = CompKeySet::default();
        for key in &frontier {
            if let Some(r) = nodes.id_of(key) {
                for &rdep_r in nodes.rdeps(r) {
                    if !nodes.contains(rdep_r) {
                        continue;
                    }
                    let rdep_key = nodes.key_of(rdep_r);
                    if !seen.contains(&rdep_key) {
                        next.insert(rdep_key);
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

/// Snapshots `key`'s node (see [`PendingRecord::snapshot`]) and enqueues it
/// for the next flush, if persistence is configured. Called by
/// `EngineInner::run` right after a successful execution whose result hash
/// actually changed, while `EngineInner::nodes` is still locked — so the
/// snapshot can never race a subsequent rerun of the same node starting
/// before this enqueue completes. Only the node's value `Arc` is cloned
/// under that lock; its postcard bytes are derived later, at flush time,
/// entirely outside it (see [`PendingRecord`]'s docs).
pub(crate) fn enqueue_changed(engine: &EngineInner, nodes: &NodeTable, key: &CompKey) {
    let Some(handle) = engine.persist.lock().unwrap().clone() else { return };
    let Some(r) = nodes.id_of(key) else { return };
    let Some(erased_def) = engine.erased_defs.get(key.def()) else { return };
    let Some(value) = erased_def.value_any(r.row) else { return };
    let Some(result_hash) = nodes.result_hash(r) else { return };
    // `src_key_interner` is a separate lock from `nodes` (held by the
    // caller already), acquired and dropped fully within this one
    // statement -- never nested with anything else, matching every other
    // `EngineInner` call site's ordering (see `crate::lock_stats`'s
    // `LockSite` docs).
    let interner = engine.src_key_interner.lock().unwrap();
    let record = PendingRecord::snapshot(nodes, &interner, r, key, value, result_hash);
    drop(interner);
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
fn open_db(path: &Path) -> OpenOutcome {
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

/// A raw `nodes` table row as read off disk: its primary key bytes (see
/// [`encode_key`]) alongside the decoded record — [`EngineInner::restore_nodes`]
/// needs the raw key bytes too, not just the record, to verify a revived
/// record's *recomputed* key actually matches the key it was stored under
/// (see that method's docs on why: a non-deterministic param `Serialize`
/// impl can make them disagree).
type StoredRecord = (Vec<u8>, NodeRecord);

type OpenOutcome = (Option<Database>, Option<Fingerprint>, Vec<StoredRecord>);

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
            let (key, value) = entry?;
            let key_bytes = key.value().to_vec();
            match postcard::from_bytes::<NodeRecord>(value.value()) {
                Ok(record) => records.push((key_bytes, record)),
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::*;
    use crate::def::{Comp, define_comp};
    use crate::engine::Engine;

    #[test]
    fn current_exe_with_mixes_in_extra_data() {
        // `extra` changes the fingerprint even though the binary hash half
        // is identical both times.
        let a = Fingerprint::current_exe_with("config-v1");
        let b = Fingerprint::current_exe_with("config-v2");
        assert_ne!(a, b, "different `extra` must produce different fingerprints");

        // Same binary, same `extra` -> stable (deterministic) fingerprint.
        let a_again = Fingerprint::current_exe_with("config-v1");
        assert_eq!(a, a_again, "same binary + same extra must reproduce the same fingerprint");

        // `current_exe()` is exactly `current_exe_with(&[])`.
        assert_eq!(Fingerprint::current_exe(), Fingerprint::current_exe_with([]));
    }

    #[test]
    fn custom_ignores_the_binary_entirely() {
        // `custom` never mixes in the running binary at all, unlike
        // `current_exe_with` -- two different `custom` payloads that happen
        // to collide with `current_exe`'s output would be a documentation
        // bug, not something to assert on; what matters here is that
        // `custom` is a pure function of its input.
        assert_eq!(Fingerprint::custom("same"), Fingerprint::custom("same"));
        assert_ne!(Fingerprint::custom("a"), Fingerprint::custom("b"));
    }

    /// Stage 9 bumped `FORMAT_VERSION` 2 -> 3 (for `NodeRecord::flow_ids`,
    /// see `docs/persistence-benchmark-notes.md`): an older database's meta
    /// row still claims the old version, and must hit the existing
    /// "unreadable/unrecognized format" path -- wiped and reopened fresh,
    /// never panicking, never trusting a v2 record's absent `flow_ids` as
    /// if it decoded to an empty `Vec`.
    #[test]
    fn old_format_version_is_wiped_and_starts_cold() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("graph.redb");

        // A meta row alone is enough: `open_and_read` rejects on the
        // version check before ever opening `NODES_TABLE`.
        {
            let db = Database::create(&db_path).unwrap();
            let write_txn = db.begin_write().unwrap();
            {
                let mut meta_table = write_txn.open_table(META_TABLE).unwrap();
                let meta = MetaRecord {
                    format_version: FORMAT_VERSION - 1,
                    fingerprint: [0; 32],
                };
                let meta_bytes = postcard::to_stdvec(&meta).unwrap();
                meta_table.insert(META_KEY, meta_bytes.as_slice()).unwrap();
            }
            write_txn.commit().unwrap();
        }

        let (db, fingerprint, records) = open_db(&db_path);
        assert!(db.is_some(), "an unrecognized format must still yield a usable (freshly wiped) database");
        assert_eq!(fingerprint, None, "a freshly wiped database has no stored fingerprint yet");
        assert!(records.is_empty(), "a freshly wiped database has no restorable records");
    }

    /// Writes a single raw `nodes` table row directly (bypassing
    /// [`PersistHandle`] entirely) under `key_bytes`, plus a matching meta
    /// row — the low-level tool the key-verification test below uses to
    /// construct a record stored under a key that doesn't match what it
    /// actually decodes to.
    fn write_raw_record(path: &Path, fingerprint: Fingerprint, key_bytes: &[u8], record: &NodeRecord) {
        let db = Database::create(path).unwrap();
        let write_txn = db.begin_write().unwrap();
        {
            let mut meta_table = write_txn.open_table(META_TABLE).unwrap();
            let meta = MetaRecord {
                format_version: FORMAT_VERSION,
                fingerprint: fingerprint.0,
            };
            let meta_bytes = postcard::to_stdvec(&meta).unwrap();
            meta_table.insert(META_KEY, meta_bytes.as_slice()).unwrap();
        }
        {
            let mut nodes_table = write_txn.open_table(NODES_TABLE).unwrap();
            let bytes = postcard::to_stdvec(record).unwrap();
            nodes_table.insert(key_bytes, bytes.as_slice()).unwrap();
        }
        write_txn.commit().unwrap();
    }

    /// Fix 3(a): a record whose param bytes decode to (and therefore
    /// recompute) a *different* `CompKey` than the raw key it was actually
    /// filed under — exactly what a non-deterministic param `Serialize`
    /// impl produces across two instances — must be dropped at load time
    /// rather than trusted, with no panic. Constructed directly (rather
    /// than via a real non-deterministic type) by writing a record whose
    /// param decodes to `5` under the raw key for `999`: `revive_key`
    /// (which only ever looks at `param_bytes`, never the stored key) would
    /// recompute the key for `5` regardless of what it was filed under, so
    /// this exercises exactly the mismatch the load-time check exists to
    /// catch.
    ///
    /// Proven via the computation's own run counter: if the mismatched
    /// record were wrongly trusted, it would be restored as a `Clean` cache
    /// hit for key `5` and the body would never run at all (and the
    /// engine would return the record's stale, deliberately-wrong stored
    /// value `5` instead of the correct `10`). With the check in place, the
    /// record is dropped, the node starts absent, and the initial
    /// evaluation runs the body fresh.
    #[tokio::test]
    async fn restore_drops_a_record_whose_recomputed_key_does_not_match_its_stored_key() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("graph.redb");

        let def_id = DefId::new("key_mismatch_probe");
        let fingerprint = Fingerprint::custom("fp-key-mismatch");

        let real_param_bytes = postcard::to_stdvec(&5i32).unwrap();
        let wrong_key = CompKey::new(def_id, &999i32);
        let wrong_key_bytes = encode_key(&wrong_key);

        let record = NodeRecord {
            def_name: "key_mismatch_probe".to_string(),
            param_bytes: real_param_bytes,
            comp_deps: Vec::new(),
            source_deps: Vec::new(),
            result_hash: [0; 16],
            // Deliberately the *wrong* answer (`5`, not `5 * 2 = 10`) so a
            // wrongly-trusted restore is observable through the returned
            // value, not just through the run counter.
            value_bytes: postcard::to_stdvec(&5i32).unwrap(),
            outputs: Vec::new(),
            flow_ids: Vec::new(),
        };
        write_raw_record(&db_path, fingerprint, &wrong_key_bytes, &record);

        let run_count = Arc::new(AtomicUsize::new(0));
        let mut builder = Engine::builder();
        builder.persistence(PersistOptions::new(db_path.clone(), fingerprint));
        let comp: Comp<i32, i32> = builder.register(define_comp("key_mismatch_probe", {
            let run_count = run_count.clone();
            move |_ctx, n: i32| {
                let run_count = run_count.clone();
                async move {
                    run_count.fetch_add(1, Ordering::SeqCst);
                    Ok(n * 2)
                }
            }
        }));
        let engine = builder.build();

        let handle = {
            let engine = engine.clone();
            tokio::spawn(async move { engine.run(comp, 5).await })
        };

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if run_count.load(Ordering::SeqCst) >= 1 {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the body must actually run -- the mismatched-key record must not be trusted as a cache hit");

        assert!(!handle.is_finished(), "a mismatched-key record must not crash the engine");
        handle.abort();
    }
}
