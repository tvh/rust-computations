//! A filesystem-watching data source built on `notify`.
//!
//! [`FsSource`] serves two [`Request`] kinds — [`ReadFile`] and [`ListDir`] —
//! and pushes change notifications for every path it has been asked to read
//! or list, until [`SourceBase::unregister`] is called for that path.
//!
//! ## Watcher strategy
//!
//! `FsSource` watches with [`notify::PollWatcher`] (100ms poll interval)
//! rather than the platform-native [`notify::RecommendedWatcher`]. The
//! native backends (FSEvents on macOS, inotify on Linux) are the better
//! choice in a normal desktop/server process, but their delivery depends on
//! kernel/OS event APIs that can be throttled, denied, or simply unavailable
//! under sandboxing or in CI containers, which makes test timing flaky in
//! exactly those environments. Polling trades a small, bounded latency
//! (bounded by the poll interval) for behavior that is identical on every
//! platform and every sandbox, which matters more for this crate's test
//! suite than shaving milliseconds off change delivery.
//!
//! ## Key canonicalization
//!
//! Keys are canonicalized paths ([`canonical_key`]) so that the same file
//! always maps to the same key regardless of how it was spelled (relative
//! vs. absolute, `.`/`..` segments, symlinks). Plain `std::fs::canonicalize`
//! requires the path to exist, but a core use case here is watching a file
//! that does *not* exist yet (so its later creation can be observed) —
//! so we canonicalize the deepest existing ancestor and re-append the
//! (non-existent) remainder of the path.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, UNIX_EPOCH};

use computations::error::{CompError, SourceError};
use computations::{Ctx, Dep, Request, Source, SourceBase, SourceId, StableHash};
use notify::{Config, RecursiveMode, Watcher};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex as AsyncMutex, mpsc};

/// Reads the full contents of a file. `Output = Vec<u8>`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReadFile(pub PathBuf);

impl Request for ReadFile {
    type Output = Vec<u8>;
}

/// Lists the entries of a directory. `Output = Vec<DirEntry>` (sorted).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ListDir(pub PathBuf);

impl Request for ListDir {
    type Output = Vec<DirEntry>;
}

/// One entry of a directory listing.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct DirEntry {
    pub name: String,
    pub kind: EntryKind,
}

/// The kind of a [`DirEntry`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum EntryKind {
    File,
    Dir,
}

/// The version [`FsSource`] reports for a watched path.
///
/// `Missing` is a first-class version (not an error) so that a computation
/// depending on a not-yet-existing file can still record a dependency on it,
/// and gets woken up once the file is created.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FsVer {
    /// The path did not exist when last observed.
    Missing,
    /// A regular file, identified by modification time and size. Cheap to
    /// compute and, for our purposes (detecting change, not verifying
    /// content), sufficiently precise.
    File { mtime_nanos: u128, size: u64 },
    /// A directory, identified by a stable hash of its sorted entry list.
    Dir { entries_hash: [u8; 16] },
}

/// Canonicalizes `path` into a stable source key, even if `path` does not
/// currently exist.
///
/// Tries plain [`std::fs::canonicalize`] first. If that fails (typically
/// because the path, or some ancestor of it, does not exist),
/// lexically-absolutizes the path, canonicalizes the deepest ancestor that
/// *does* exist, and re-appends the remaining (non-existent) components
/// unchanged. This keeps the key stable across process runs and independent
/// of the working directory, while still allowing a source to watch and
/// later observe the creation of a path that does not exist yet.
pub fn canonical_key(path: &Path) -> std::io::Result<PathBuf> {
    if let Ok(canon) = std::fs::canonicalize(path) {
        return Ok(canon);
    }
    let abs = std::path::absolute(path)?;
    let mut remainder: Vec<std::ffi::OsString> = Vec::new();
    let mut cur: &Path = &abs;
    let ancestor: PathBuf = loop {
        if cur.exists() {
            break cur.to_path_buf();
        }
        match (cur.file_name(), cur.parent()) {
            (Some(name), Some(parent)) => {
                remainder.push(name.to_os_string());
                cur = parent;
            }
            // Reached the root (or an otherwise component-less path) without
            // finding an existing ancestor; use it as-is.
            _ => break cur.to_path_buf(),
        }
    };
    let mut result = std::fs::canonicalize(&ancestor).unwrap_or(ancestor);
    for comp in remainder.into_iter().rev() {
        result.push(comp);
    }
    Ok(result)
}

fn list_dir_entries(path: &Path) -> std::io::Result<Vec<DirEntry>> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let kind = if entry.file_type()?.is_dir() {
            EntryKind::Dir
        } else {
            EntryKind::File
        };
        out.push(DirEntry {
            name: entry.file_name().to_string_lossy().into_owned(),
            kind,
        });
    }
    out.sort();
    Ok(out)
}

fn compute_file_ver(path: &Path) -> FsVer {
    match std::fs::metadata(path) {
        Ok(meta) => {
            let mtime_nanos = meta
                .modified()
                .ok()
                .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            FsVer::File {
                mtime_nanos,
                size: meta.len(),
            }
        }
        Err(_) => FsVer::Missing,
    }
}

fn compute_dir_ver(path: &Path) -> FsVer {
    match list_dir_entries(path) {
        Ok(entries) => FsVer::Dir {
            entries_hash: entries.stable_hash().as_bytes(),
        },
        Err(_) => FsVer::Missing,
    }
}

fn compute_ver(path: &Path, is_dir: bool) -> FsVer {
    if is_dir {
        compute_dir_ver(path)
    } else {
        compute_file_ver(path)
    }
}

struct WatchEntry {
    is_dir: bool,
    last_ver: FsVer,
}

/// State shared between [`FsSource`] and the notify event-handler closure
/// (which runs on notify's own background thread).
struct Shared {
    watched: Mutex<HashMap<PathBuf, WatchEntry>>,
}

/// A short, stable label for an [`FsVer`]'s variant, for tracing fields (the
/// full value, especially `Dir`'s entry hash, is not worth logging).
fn ver_kind(ver: &FsVer) -> &'static str {
    match ver {
        FsVer::Missing => "missing",
        FsVer::File { .. } => "file",
        FsVer::Dir { .. } => "dir",
    }
}

fn handle_event(shared: &Shared, tx: &mpsc::UnboundedSender<Dep<PathBuf, FsVer>>, event: notify::Event) {
    let mut watched = shared.watched.lock().unwrap();
    for path in &event.paths {
        // A path affects a watched key K if it *is* K (K itself was
        // created/removed/renamed/modified) or if it is a direct child of K
        // (K is a watched directory and one of its entries changed).
        let affected: Vec<PathBuf> = watched
            .keys()
            .filter(|key| path == *key || path.parent() == Some(key.as_path()))
            .cloned()
            .collect();
        for key in affected {
            let entry = watched.get_mut(&key).expect("key came from this map");
            let new_ver = compute_ver(&key, entry.is_dir);
            if new_ver != entry.last_ver {
                entry.last_ver = new_ver.clone();
                tracing::debug!(key = %key.display(), ver_kind = ver_kind(&new_ver), "fs source: change notification");
                let _ = tx.send(Dep {
                    key: key.clone(),
                    ver: new_ver,
                });
            }
        }
    }
}

/// A source that reads files and lists directories, pushing change
/// notifications via a polling filesystem watcher.
///
/// See the [module docs](self) for the watcher and key-canonicalization
/// strategy.
pub struct FsSource {
    id: SourceId,
    shared: Arc<Shared>,
    /// Refcounts for paths actually passed to `watcher.watch(..)`, so that
    /// multiple registered keys sharing a parent directory only need one
    /// underlying watch on it.
    watch_refs: Mutex<HashMap<PathBuf, usize>>,
    watcher: Mutex<notify::PollWatcher>,
    changes_rx: AsyncMutex<mpsc::UnboundedReceiver<Dep<PathBuf, FsVer>>>,
}

impl FsSource {
    /// Creates a new `FsSource` with the given instance id.
    pub fn new(id: &str) -> std::io::Result<Arc<Self>> {
        let shared = Arc::new(Shared {
            watched: Mutex::new(HashMap::new()),
        });
        // Only the watcher callback ever sends on this channel; the sending
        // half lives inside the watcher (moved into its event-handler
        // closure below), which in turn lives as long as `FsSource` does,
        // so the channel stays open for `FsSource`'s whole lifetime without
        // `FsSource` needing to hold a sender itself.
        let (tx, rx) = mpsc::unbounded_channel();

        let cb_shared = shared.clone();
        // `with_compare_contents(true)`: notify's `PollWatcher` otherwise
        // only tracks a file's modification time at *second* granularity
        // (see notify's `poll.rs`), so two writes within the same wall-clock
        // second would go undetected even though their content (and our own
        // `FsVer`, which uses nanosecond mtimes plus size) differs. Content
        // comparison catches those changes too, at the cost of hashing file
        // contents once per poll interval.
        let config = Config::default()
            .with_poll_interval(Duration::from_millis(100))
            .with_compare_contents(true);
        let watcher = notify::PollWatcher::new(
            move |res: notify::Result<notify::Event>| match res {
                Ok(event) => handle_event(&cb_shared, &tx, event),
                Err(err) => tracing::warn!(error = %err, "filesystem watch error"),
            },
            config,
        )
        .map_err(|e| std::io::Error::other(e.to_string()))?;

        Ok(Arc::new(FsSource {
            id: SourceId::new(id),
            shared,
            watch_refs: Mutex::new(HashMap::new()),
            watcher: Mutex::new(watcher),
            changes_rx: AsyncMutex::new(rx),
        }))
    }

    /// Records `key` as watched (computing and returning its current
    /// version), setting up (or adjusting) the underlying filesystem watches
    /// as needed.
    fn register(&self, key: &Path, is_dir: bool) -> FsVer {
        let ver = compute_ver(key, is_dir);
        let prev_is_dir = {
            let mut watched = self.shared.watched.lock().unwrap();
            watched
                .insert(
                    key.to_path_buf(),
                    WatchEntry {
                        is_dir,
                        last_ver: ver.clone(),
                    },
                )
                .map(|old| old.is_dir)
        };
        match prev_is_dir {
            None => self.add_watch(key, is_dir),
            Some(old_is_dir) if old_is_dir != is_dir => {
                self.remove_watch(key, old_is_dir);
                self.add_watch(key, is_dir);
            }
            Some(_) => {}
        }
        ver
    }

    fn add_watch(&self, key: &Path, is_dir: bool) {
        let parent = key.parent().map(Path::to_path_buf).unwrap_or_else(|| key.to_path_buf());
        self.bump_watch(&parent);
        if is_dir {
            self.bump_watch(key);
        }
    }

    fn remove_watch(&self, key: &Path, is_dir: bool) {
        let parent = key.parent().map(Path::to_path_buf).unwrap_or_else(|| key.to_path_buf());
        self.drop_watch(&parent);
        if is_dir {
            self.drop_watch(key);
        }
    }

    fn bump_watch(&self, path: &Path) {
        let mut refs = self.watch_refs.lock().unwrap();
        let count = refs.entry(path.to_path_buf()).or_insert(0);
        *count += 1;
        if *count == 1 {
            let mut watcher = self.watcher.lock().unwrap();
            if let Err(e) = watcher.watch(path, RecursiveMode::NonRecursive) {
                tracing::warn!(?path, error = %e, "failed to watch path");
            }
        }
    }

    fn drop_watch(&self, path: &Path) {
        let mut refs = self.watch_refs.lock().unwrap();
        if let Some(count) = refs.get_mut(path) {
            *count -= 1;
            if *count == 0 {
                refs.remove(path);
                let mut watcher = self.watcher.lock().unwrap();
                let _ = watcher.unwatch(path);
            }
        }
    }

    /// Reads the full contents of `path`, recording the read (and its
    /// eventual changes) as a dependency of the currently executing
    /// computation. A typed convenience over `ctx.src_req(source, ReadFile(path))`.
    pub async fn read_file(self: &Arc<Self>, ctx: &Ctx, path: impl Into<PathBuf>) -> Result<Vec<u8>, CompError> {
        ctx.src_req(self, ReadFile(path.into())).await
    }

    /// Lists the entries of the directory at `path`, recording the listing
    /// (and its eventual changes) as a dependency of the currently executing
    /// computation. A typed convenience over `ctx.src_req(source, ListDir(path))`.
    pub async fn list_dir(self: &Arc<Self>, ctx: &Ctx, path: impl Into<PathBuf>) -> Result<Vec<DirEntry>, CompError> {
        ctx.src_req(self, ListDir(path.into())).await
    }
}

impl SourceBase for FsSource {
    type Key = PathBuf;
    type Ver = FsVer;

    fn instance_id(&self) -> SourceId {
        self.id.clone()
    }

    /// Reports each path's current [`FsVer`] and (re)watches it — exactly
    /// [`FsSource::register`], driven for a batch of keys instead of a
    /// single request.
    ///
    /// A key still tracked from a previous `register` (i.e. this same
    /// `FsSource` instance already watches it) reuses its known `is_dir`.
    /// Otherwise (typically: a key restored from a persisted graph, in a
    /// fresh process that has never watched it) `is_dir` is determined by
    /// asking the filesystem directly; a path that doesn't currently exist
    /// is treated as a file, which is harmless — `compute_file_ver` and
    /// `compute_dir_ver` agree on `FsVer::Missing` for a nonexistent path
    /// either way, and the file/dir distinction only starts to matter once
    /// something exists there again.
    async fn probe_versions(&self, keys: &HashSet<PathBuf>) -> Option<HashMap<PathBuf, Option<FsVer>>> {
        let mut out = HashMap::new();
        for key in keys {
            let is_dir = {
                let watched = self.shared.watched.lock().unwrap();
                watched.get(key).map(|e| e.is_dir)
            }
            .unwrap_or_else(|| key.is_dir());
            let ver = self.register(key, is_dir);
            out.insert(key.clone(), Some(ver));
        }
        Some(out)
    }

    async fn wait_changes(&self) -> HashSet<Dep<PathBuf, FsVer>> {
        let mut rx = self.changes_rx.lock().await;
        let mut batch = HashSet::new();
        // Await the first item (cancel-safe: tokio's mpsc recv is
        // cancel-safe), then opportunistically drain any additional queued
        // items into the same batch without blocking further.
        match rx.recv().await {
            Some(dep) => {
                batch.insert(dep);
            }
            None => return batch,
        }
        while let Ok(dep) = rx.try_recv() {
            batch.insert(dep);
        }
        batch
    }

    fn unregister(&self, keys: &HashSet<PathBuf>) {
        let removed: Vec<(PathBuf, bool)> = {
            let mut watched = self.shared.watched.lock().unwrap();
            keys.iter()
                .filter_map(|k| watched.remove(k).map(|e| (k.clone(), e.is_dir)))
                .collect()
        };
        for (key, is_dir) in removed {
            self.remove_watch(&key, is_dir);
        }
    }
}

impl Source<ReadFile> for FsSource {
    async fn execute(
        &self,
        req: ReadFile,
    ) -> (Result<Vec<u8>, SourceError>, HashSet<Dep<PathBuf, FsVer>>) {
        let ReadFile(path) = req;
        let key = match canonical_key(&path) {
            Ok(k) => k,
            Err(e) => return (Err(SourceError::from(e)), HashSet::new()),
        };
        let ver = self.register(&key, false);
        // Missing files are a legitimate, first-class state (see `FsVer`):
        // the dep above is still recorded with `FsVer::Missing` so that the
        // computation is retried when the file appears, even though the
        // read itself fails here.
        let result = tokio::fs::read(&key).await.map_err(SourceError::from);
        let mut deps = HashSet::new();
        deps.insert(Dep { key, ver });
        (result, deps)
    }
}

impl Source<ListDir> for FsSource {
    async fn execute(
        &self,
        req: ListDir,
    ) -> (Result<Vec<DirEntry>, SourceError>, HashSet<Dep<PathBuf, FsVer>>) {
        let ListDir(path) = req;
        let key = match canonical_key(&path) {
            Ok(k) => k,
            Err(e) => return (Err(SourceError::from(e)), HashSet::new()),
        };
        let ver = self.register(&key, true);
        let result = {
            let key = key.clone();
            tokio::task::spawn_blocking(move || list_dir_entries(&key))
                .await
                .unwrap_or_else(|_| {
                    Err(std::io::Error::other("list_dir_entries task panicked"))
                })
                .map_err(SourceError::from)
        };
        let mut deps = HashSet::new();
        deps.insert(Dep { key, ver });
        (result, deps)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration as StdDuration;

    const TIMEOUT: StdDuration = StdDuration::from_secs(10);
    // Give the poll watcher (100ms interval) a couple of cycles to settle
    // before asserting "no notification arrived".
    const SETTLE: StdDuration = StdDuration::from_millis(500);

    async fn wait_changes_timeout(
        src: &FsSource,
    ) -> Result<HashSet<Dep<PathBuf, FsVer>>, tokio::time::error::Elapsed> {
        tokio::time::timeout(TIMEOUT, src.wait_changes()).await
    }

    #[tokio::test]
    async fn read_file_roundtrip_and_change_notification() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("a.txt");
        std::fs::write(&file_path, b"hello").unwrap();

        let src = FsSource::new("fs").unwrap();
        let (result, deps) = src.execute(ReadFile(file_path.clone())).await;
        assert_eq!(result.unwrap(), b"hello".to_vec());
        assert_eq!(deps.len(), 1);
        let dep = deps.into_iter().next().unwrap();
        assert!(matches!(dep.ver, FsVer::File { .. }));

        // Ensure the mtime actually changes on some filesystems with coarse
        // mtime resolution.
        tokio::time::sleep(StdDuration::from_millis(20)).await;
        std::fs::write(&file_path, b"hello world").unwrap();

        let changes = wait_changes_timeout(&src).await.expect("should notify");
        assert_eq!(changes.len(), 1);
        let changed = changes.into_iter().next().unwrap();
        assert_eq!(changed.key, canonical_key(&file_path).unwrap());
        assert_ne!(changed.ver, dep.ver);
    }

    #[tokio::test]
    async fn missing_file_read_errs_but_records_dep_and_notifies_on_creation() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("missing.txt");

        let src = FsSource::new("fs").unwrap();
        let (result, deps) = src.execute(ReadFile(file_path.clone())).await;
        assert!(result.is_err());
        assert_eq!(deps.len(), 1);
        let dep = deps.into_iter().next().unwrap();
        assert_eq!(dep.ver, FsVer::Missing);

        std::fs::write(&file_path, b"now it exists").unwrap();

        let changes = wait_changes_timeout(&src).await.expect("should notify");
        assert_eq!(changes.len(), 1);
        let changed = changes.into_iter().next().unwrap();
        assert!(matches!(changed.ver, FsVer::File { .. }));
    }

    #[tokio::test]
    async fn list_dir_roundtrip_and_change_on_add_and_remove() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a"), b"1").unwrap();
        std::fs::write(dir.path().join("b"), b"2").unwrap();

        let src = FsSource::new("fs").unwrap();
        let (result, deps) = src.execute(ListDir(dir.path().to_path_buf())).await;
        let entries = result.unwrap();
        assert_eq!(entries.len(), 2);
        assert!(entries.windows(2).all(|w| w[0] <= w[1]), "entries sorted");
        let dep = deps.into_iter().next().unwrap();
        let ver0 = dep.ver;

        std::fs::write(dir.path().join("c"), b"3").unwrap();
        let changes = wait_changes_timeout(&src).await.expect("should notify on add");
        let changed = changes.into_iter().next().unwrap();
        assert_ne!(changed.ver, ver0);
        let ver1 = changed.ver;

        std::fs::remove_file(dir.path().join("a")).unwrap();
        let changes = wait_changes_timeout(&src)
            .await
            .expect("should notify on remove");
        let changed = changes.into_iter().next().unwrap();
        assert_ne!(changed.ver, ver1);
    }

    #[tokio::test]
    async fn unregister_stops_notifications() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("a.txt");
        std::fs::write(&file_path, b"hello").unwrap();

        let src = FsSource::new("fs").unwrap();
        let _ = src.execute(ReadFile(file_path.clone())).await;

        let key = canonical_key(&file_path).unwrap();
        let mut keys = HashSet::new();
        keys.insert(key);
        src.unregister(&keys);

        // Let any in-flight poll cycle settle before mutating.
        tokio::time::sleep(SETTLE).await;
        std::fs::write(&file_path, b"changed after unregister").unwrap();

        let timed_out = tokio::time::timeout(SETTLE, src.wait_changes()).await;
        assert!(
            timed_out.is_err(),
            "unregistered key should not notify, got {timed_out:?}"
        );
    }

    #[tokio::test]
    async fn probe_versions_reports_current_version_for_file_and_dir() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("a.txt");
        std::fs::write(&file_path, b"hello").unwrap();
        let file_key = canonical_key(&file_path).unwrap();
        let dir_key = canonical_key(dir.path()).unwrap();

        // A fresh source, never having executed a request against either
        // key — as if these keys were restored from a persisted graph.
        let src = FsSource::new("fs").unwrap();
        let mut keys = HashSet::new();
        keys.insert(file_key.clone());
        keys.insert(dir_key.clone());

        let probed = src.probe_versions(&keys).await.expect("probing supported");
        assert!(matches!(probed.get(&file_key), Some(Some(FsVer::File { .. }))));
        assert!(matches!(probed.get(&dir_key), Some(Some(FsVer::Dir { .. }))));
    }

    #[tokio::test]
    async fn probe_versions_resubscribes_for_future_notifications() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("a.txt");
        std::fs::write(&file_path, b"hello").unwrap();
        let file_key = canonical_key(&file_path).unwrap();

        let src = FsSource::new("fs").unwrap();
        // Never read via `execute` — only via `probe_versions`.
        let mut keys = HashSet::new();
        keys.insert(file_key.clone());
        let _ = src.probe_versions(&keys).await.expect("probing supported");

        tokio::time::sleep(StdDuration::from_millis(20)).await;
        std::fs::write(&file_path, b"hello world").unwrap();

        let changes = wait_changes_timeout(&src)
            .await
            .expect("probed key should have been (re)subscribed for change notifications");
        assert_eq!(changes.len(), 1);
        let changed = changes.into_iter().next().unwrap();
        assert_eq!(changed.key, file_key);
    }

    #[tokio::test]
    async fn canonical_key_stable_for_missing_paths() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("nested").join("missing.txt");

        let key_a = canonical_key(&missing).unwrap();
        let key_b = canonical_key(&missing).unwrap();
        assert_eq!(key_a, key_b);
        assert!(key_a.starts_with(std::fs::canonicalize(dir.path()).unwrap()));
    }
}
