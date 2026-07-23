//! A filesystem-writing sink for computation results.
//!
//! [`FsSink`] writes files and creates directories under a fixed root,
//! reporting each write's path (relative to the root) as its output so the
//! engine can garbage-collect outputs whose producing computation died.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use computations::error::{CompError, SinkError};
use computations::{Ctx, Request, Sink, SinkBase, SinkId};

/// Writes `contents` to `rel_path` (relative to the sink's root), creating
/// parent directories as needed. `Output = ()`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WriteFile {
    pub rel_path: PathBuf,
    pub contents: Vec<u8>,
}

impl Request for WriteFile {
    type Output = ();
}

/// Creates `rel_path` (relative to the sink's root) and any missing parent
/// directories. `Output = ()`.
///
/// An empty `rel_path` (the root itself) is a no-op that reports no output:
/// the root always exists already (`FsSink::new` creates it), so there is
/// nothing to do and nothing to track.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MakeDirs {
    pub rel_path: PathBuf,
}

impl Request for MakeDirs {
    type Output = ();
}

/// Rejects a candidate output path unless it is relative and free of `..`
/// (or any other non-normal) components, so a sink operation can never
/// touch anything outside its root.
fn validate_rel_path(rel: &Path) -> Result<(), SinkError> {
    if rel.as_os_str().is_empty() {
        return Err(SinkError::Other("rel_path must not be empty".to_string()));
    }
    if rel.is_absolute() {
        return Err(SinkError::Other(format!(
            "rel_path must be relative, got {}",
            rel.display()
        )));
    }
    for comp in rel.components() {
        if !matches!(comp, std::path::Component::Normal(_)) {
            return Err(SinkError::Other(format!(
                "rel_path must contain only normal path segments, found {comp:?} in {}",
                rel.display()
            )));
        }
    }
    Ok(())
}

/// A sink that writes files and directories under a fixed root directory.
pub struct FsSink {
    id: SinkId,
    root: PathBuf,
}

impl FsSink {
    /// Creates a new `FsSink` rooted at `root`, creating `root` if it does
    /// not already exist.
    ///
    /// Returns `Arc<Self>` directly (matching [`super::FsSource::new`]'s
    /// signature): every real use of a sink needs it behind an `Arc` anyway
    /// (the typed helpers like [`FsSink::write_file`] take `self: &Arc<Self>`),
    /// so callers no longer need to wrap the result themselves.
    ///
    /// This constructor is infallible by design: if creating the root
    /// directory fails here, the error simply resurfaces on the first actual
    /// write, which already has to handle I/O errors.
    pub fn new(id: &str, root: impl Into<PathBuf>) -> Arc<Self> {
        let root = root.into();
        let _ = std::fs::create_dir_all(&root);
        Arc::new(FsSink {
            id: SinkId::new(id),
            root,
        })
    }

    /// Test-only inspection: the root directory this sink writes under.
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn full_path(&self, rel: &Path) -> PathBuf {
        self.root.join(rel)
    }

    /// Writes `contents` to `rel_path` (relative to the sink's root),
    /// reporting the write as an output of the currently executing
    /// computation. A typed convenience over
    /// `ctx.sink_req(sink, WriteFile { rel_path, contents })`.
    pub async fn write_file(
        self: &Arc<Self>,
        ctx: &Ctx,
        rel_path: impl Into<PathBuf>,
        contents: Vec<u8>,
    ) -> Result<(), CompError> {
        ctx.sink_req(
            self,
            WriteFile {
                rel_path: rel_path.into(),
                contents,
            },
        )
        .await
    }

    /// Creates `rel_path` (relative to the sink's root), reporting it as an
    /// output of the currently executing computation. A typed convenience
    /// over `ctx.sink_req(sink, MakeDirs { rel_path })`.
    pub async fn make_dirs(self: &Arc<Self>, ctx: &Ctx, rel_path: impl Into<PathBuf>) -> Result<(), CompError> {
        ctx.sink_req(
            self,
            MakeDirs {
                rel_path: rel_path.into(),
            },
        )
        .await
    }

    /// Removes now-empty directories, walking upward from `start` and
    /// stopping at (not including) the root, or at the first non-empty /
    /// otherwise unremovable directory.
    async fn prune_empty_ancestors(&self, start: Option<&Path>) {
        let mut cur = start.map(Path::to_path_buf);
        while let Some(dir) = cur {
            if dir == self.root || !dir.starts_with(&self.root) {
                break;
            }
            if tokio::fs::remove_dir(&dir).await.is_err() {
                // Either not empty, or some other issue (already gone,
                // permissions, ...) — either way, stop pruning upward.
                break;
            }
            cur = dir.parent().map(Path::to_path_buf);
        }
    }

    async fn write_atomic(&self, full: &Path, contents: &[u8]) -> Result<(), SinkError> {
        if let Some(parent) = full.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let tmp_name = {
            let mut name = full.file_name().unwrap_or_default().to_os_string();
            name.push(".tmp");
            name
        };
        let tmp = full.with_file_name(tmp_name);
        tokio::fs::write(&tmp, contents).await?;
        tokio::fs::rename(&tmp, full).await?;
        Ok(())
    }
}

impl SinkBase for FsSink {
    type Out = PathBuf;

    fn instance_id(&self) -> SinkId {
        self.id.clone()
    }

    async fn delete_outputs(&self, outs: HashSet<PathBuf>) -> Result<(), SinkError> {
        tracing::debug!(sink = %self.id, count = outs.len(), "fs sink: deleting outputs");
        // Process deepest paths first: within one batch this guarantees a
        // file is removed before any directory that contains it, so a
        // directory that this very batch empties out is itself removable by
        // the time we reach it (directories are only ever removed if
        // empty). Sorting purely by component count is sufficient because a
        // path nested under another always has strictly more components.
        let mut outs: Vec<PathBuf> = outs.into_iter().collect();
        outs.sort_by_key(|p| std::cmp::Reverse(p.components().count()));
        for out in outs {
            validate_rel_path(&out)?;
            let full = self.full_path(&out);
            match tokio::fs::symlink_metadata(&full).await {
                Ok(meta) if meta.is_dir() => {
                    // Remove only if empty; leave non-empty directories
                    // alone (they may still be in active use).
                    let _ = tokio::fs::remove_dir(&full).await;
                }
                Ok(_) => {
                    if let Err(e) = tokio::fs::remove_file(&full).await
                        && e.kind() != std::io::ErrorKind::NotFound
                    {
                        return Err(SinkError::from(e));
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    // Already gone: deletion is idempotent.
                }
                Err(e) => return Err(SinkError::from(e)),
            }
            self.prune_empty_ancestors(full.parent()).await;
        }
        Ok(())
    }

    async fn list_existing_outputs(&self) -> Result<Option<HashSet<PathBuf>>, SinkError> {
        let root = self.root.clone();
        let files = tokio::task::spawn_blocking(move || -> std::io::Result<Vec<PathBuf>> {
            let mut out = Vec::new();
            walk_files(&root, &root, &mut out)?;
            Ok(out)
        })
        .await
        .map_err(|_| SinkError::Other("list_existing_outputs task panicked".to_string()))?
        .map_err(SinkError::from)?;
        Ok(Some(files.into_iter().collect()))
    }
}

/// Recursively collects every entry under `dir` (which starts at `root` and
/// descends), reporting each as a path relative to `root`. The root itself
/// is never reported (callers only ever `read_dir` its contents).
///
/// Every entry is reported, not just regular files:
///
/// - Directories are reported too, one output per directory, in the same
///   rel-path shape [`MakeDirs`] reports for its `rel_path`. A directory
///   that no longer has a live producer (e.g. its source vanished between
///   runs) would otherwise be invisible to startup GC's `existing − live`
///   diff — an already-empty stale directory never triggers the ancestor
///   pruning in [`FsSink::delete_outputs`], since that only fires when a
///   file *inside* it is deleted. Listing the directory itself closes that
///   gap. A directory that is still in active use (an ancestor of live
///   output, or a live `MakeDirs` output itself) is unaffected: subtracting
///   the live set removes it from the deletion batch, and even if some
///   ancestor slips through, `delete_outputs` only removes directories that
///   are empty, so a directory holding live content is a silent no-op.
/// - Symlinks and any other non-file, non-dir entries (`file_type()` does
///   not follow links, so a symlink is neither `is_file()` nor `is_dir()`)
///   are reported as file-kind outputs: they are simple deletable entries,
///   and `delete_outputs` removes them with `remove_file`, which unlinks a
///   symlink without following it.
fn walk_files(root: &Path, dir: &Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        let rel = path
            .strip_prefix(root)
            .expect("walked path is always under root")
            .to_path_buf();
        if file_type.is_dir() {
            out.push(rel);
            walk_files(root, &path, out)?;
        } else {
            // Regular file, symlink, or any other non-dir entry type.
            out.push(rel);
        }
    }
    Ok(())
}

impl Sink<WriteFile> for FsSink {
    async fn execute(&self, req: WriteFile) -> (HashSet<PathBuf>, Result<(), SinkError>) {
        let WriteFile { rel_path, contents } = req;
        if let Err(e) = validate_rel_path(&rel_path) {
            return (HashSet::new(), Err(e));
        }
        let full = self.full_path(&rel_path);
        match self.write_atomic(&full, &contents).await {
            Ok(()) => {
                let mut outs = HashSet::new();
                outs.insert(rel_path);
                (outs, Ok(()))
            }
            Err(e) => (HashSet::new(), Err(e)),
        }
    }
}

impl Sink<MakeDirs> for FsSink {
    async fn execute(&self, req: MakeDirs) -> (HashSet<PathBuf>, Result<(), SinkError>) {
        let MakeDirs { rel_path } = req;
        if rel_path.as_os_str().is_empty() {
            // The root itself: already exists (`FsSink::new` creates it), so
            // this is a successful no-op with no output to report.
            return (HashSet::new(), Ok(()));
        }
        if let Err(e) = validate_rel_path(&rel_path) {
            return (HashSet::new(), Err(e));
        }
        let full = self.full_path(&rel_path);
        match tokio::fs::create_dir_all(&full).await {
            Ok(()) => {
                let mut outs = HashSet::new();
                outs.insert(rel_path);
                (outs, Ok(()))
            }
            Err(e) => (HashSet::new(), Err(SinkError::from(e))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn write_file_creates_file_and_reports_output() {
        let dir = tempfile::tempdir().unwrap();
        let sink = FsSink::new("fs", dir.path());

        let rel = PathBuf::from("a/b/c.txt");
        let (outs, result) = sink
            .execute(WriteFile {
                rel_path: rel.clone(),
                contents: b"hello".to_vec(),
            })
            .await;
        assert!(result.is_ok());
        assert_eq!(outs, HashSet::from([rel.clone()]));

        let full = dir.path().join(&rel);
        assert_eq!(tokio::fs::read(&full).await.unwrap(), b"hello");
    }

    #[tokio::test]
    async fn delete_outputs_removes_file_and_prunes_empty_parents_but_keeps_root() {
        let dir = tempfile::tempdir().unwrap();
        let sink = FsSink::new("fs", dir.path());

        let rel = PathBuf::from("a/b/c.txt");
        let _ = sink
            .execute(WriteFile {
                rel_path: rel.clone(),
                contents: b"hello".to_vec(),
            })
            .await;

        sink.delete_outputs(HashSet::from([rel.clone()]))
            .await
            .unwrap();

        assert!(!dir.path().join(&rel).exists());
        assert!(!dir.path().join("a/b").exists());
        assert!(!dir.path().join("a").exists());
        assert!(dir.path().exists(), "root must survive pruning");
    }

    #[tokio::test]
    async fn delete_outputs_is_idempotent_for_missing_files() {
        let dir = tempfile::tempdir().unwrap();
        let sink = FsSink::new("fs", dir.path());

        let result = sink
            .delete_outputs(HashSet::from([PathBuf::from("never-written.txt")]))
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn list_existing_outputs_sees_preexisting_files() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("sub")).unwrap();
        std::fs::write(dir.path().join("sub/x.txt"), b"1").unwrap();
        std::fs::write(dir.path().join("y.txt"), b"2").unwrap();

        let sink = FsSink::new("fs", dir.path());
        let existing = sink.list_existing_outputs().await.unwrap().unwrap();
        assert_eq!(
            existing,
            HashSet::from([
                PathBuf::from("sub"),
                PathBuf::from("sub/x.txt"),
                PathBuf::from("y.txt")
            ])
        );
    }

    #[tokio::test]
    async fn make_dirs_on_empty_rel_path_is_a_no_op() {
        let dir = tempfile::tempdir().unwrap();
        let sink = FsSink::new("fs", dir.path());

        let (outs, result) = sink
            .execute(MakeDirs {
                rel_path: PathBuf::new(),
            })
            .await;
        assert!(result.is_ok());
        assert!(outs.is_empty());
        assert!(dir.path().exists(), "root must still exist");
    }

    #[tokio::test]
    async fn list_existing_outputs_sees_preexisting_empty_dir() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("stale_empty_dir")).unwrap();

        let sink = FsSink::new("fs", dir.path());
        let existing = sink.list_existing_outputs().await.unwrap().unwrap();
        assert_eq!(existing, HashSet::from([PathBuf::from("stale_empty_dir")]));
    }

    #[tokio::test]
    async fn startup_gc_removes_a_stale_preexisting_empty_dir() {
        // Simulates the engine's startup GC: list what exists, subtract the
        // (here, empty) live set, delete the remainder. A directory that no
        // live computation produces must not survive this, or it would be
        // immortal (an already-empty directory never triggers
        // `delete_outputs`'s ancestor-pruning, which only fires when a file
        // *inside* it is removed).
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("stale_empty_dir")).unwrap();

        let sink = FsSink::new("fs", dir.path());
        let existing = sink.list_existing_outputs().await.unwrap().unwrap();
        let live: HashSet<PathBuf> = HashSet::new();
        let stale: HashSet<PathBuf> = existing.difference(&live).cloned().collect();

        sink.delete_outputs(stale).await.unwrap();

        assert!(!dir.path().join("stale_empty_dir").exists());
        assert!(dir.path().exists(), "root must survive");
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn startup_gc_removes_a_stale_preexisting_symlink() {
        let dir = tempfile::tempdir().unwrap();
        // A symlink whose target need not even exist: it's just an alien
        // entry that startup GC should be able to see and remove, the same
        // as it would an alien regular file.
        std::os::unix::fs::symlink(dir.path().join("nonexistent-target"), dir.path().join("stale_link")).unwrap();

        let sink = FsSink::new("fs", dir.path());
        let existing = sink.list_existing_outputs().await.unwrap().unwrap();
        assert_eq!(existing, HashSet::from([PathBuf::from("stale_link")]));

        let live: HashSet<PathBuf> = HashSet::new();
        let stale: HashSet<PathBuf> = existing.difference(&live).cloned().collect();
        sink.delete_outputs(stale).await.unwrap();

        // `symlink_metadata` (not `exists`, which follows links and would
        // report `false` for a dangling link regardless of whether the link
        // itself is still there) confirms the link entry itself is gone.
        assert!(tokio::fs::symlink_metadata(dir.path().join("stale_link")).await.is_err());
    }

    #[tokio::test]
    async fn delete_outputs_removes_a_dir_and_its_stale_files_in_one_batch() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("stale_dir")).unwrap();
        std::fs::write(dir.path().join("stale_dir/a.txt"), b"1").unwrap();
        std::fs::write(dir.path().join("stale_dir/b.txt"), b"2").unwrap();

        let sink = FsSink::new("fs", dir.path());
        let outs = HashSet::from([
            PathBuf::from("stale_dir"),
            PathBuf::from("stale_dir/a.txt"),
            PathBuf::from("stale_dir/b.txt"),
        ]);
        sink.delete_outputs(outs).await.unwrap();

        assert!(!dir.path().join("stale_dir").exists());
    }

    #[tokio::test]
    async fn delete_outputs_leaves_a_live_dir_in_place_while_removing_a_stale_file_inside_it() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("live_dir")).unwrap();
        std::fs::write(dir.path().join("live_dir/live.txt"), b"keep").unwrap();
        std::fs::write(dir.path().join("live_dir/stale.txt"), b"drop").unwrap();

        let sink = FsSink::new("fs", dir.path());
        let existing = sink.list_existing_outputs().await.unwrap().unwrap();
        assert_eq!(
            existing,
            HashSet::from([
                PathBuf::from("live_dir"),
                PathBuf::from("live_dir/live.txt"),
                PathBuf::from("live_dir/stale.txt"),
            ])
        );

        // The dir and the live file are both still-live outputs; only the
        // stale file is in `existing - live`.
        let live = HashSet::from([PathBuf::from("live_dir"), PathBuf::from("live_dir/live.txt")]);
        let stale: HashSet<PathBuf> = existing.difference(&live).cloned().collect();
        assert_eq!(stale, HashSet::from([PathBuf::from("live_dir/stale.txt")]));

        sink.delete_outputs(stale).await.unwrap();

        assert!(!dir.path().join("live_dir/stale.txt").exists());
        assert!(dir.path().join("live_dir/live.txt").exists());
        assert!(dir.path().join("live_dir").exists(), "live dir must survive");
    }

    #[tokio::test]
    async fn path_escape_attempts_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let sink = FsSink::new("fs", dir.path());

        let (outs, result) = sink
            .execute(WriteFile {
                rel_path: PathBuf::from("../escape.txt"),
                contents: b"nope".to_vec(),
            })
            .await;
        assert!(result.is_err());
        assert!(outs.is_empty());
        assert!(!dir.path().parent().unwrap().join("escape.txt").exists());

        let absolute = dir.path().join("also_escaped.txt");
        let (outs, result) = sink
            .execute(WriteFile {
                rel_path: absolute,
                contents: b"nope".to_vec(),
            })
            .await;
        assert!(result.is_err());
        assert!(outs.is_empty());

        let (outs, result) = sink
            .execute(MakeDirs {
                rel_path: PathBuf::from("a/../../escape"),
            })
            .await;
        assert!(result.is_err());
        assert!(outs.is_empty());
    }
}
