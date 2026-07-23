//! A filesystem-writing sink for computation results.
//!
//! [`FsSink`] writes files and creates directories under a fixed root,
//! reporting each write's path (relative to the root) as its output so the
//! engine can garbage-collect outputs whose producing computation died.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use computations::error::SinkError;
use computations::{Request, Sink, SinkBase, SinkId};

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
    /// This constructor is infallible by design (matching the signature
    /// used to build sources/sinks elsewhere in this crate): if creating the
    /// root directory fails here, the error simply resurfaces on the first
    /// actual write, which already has to handle I/O errors.
    pub fn new(id: &str, root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        let _ = std::fs::create_dir_all(&root);
        FsSink {
            id: SinkId::new(id),
            root,
        }
    }

    /// Test-only inspection: the root directory this sink writes under.
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn full_path(&self, rel: &Path) -> PathBuf {
        self.root.join(rel)
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

/// Recursively collects every regular file under `dir` (which starts at
/// `root` and descends), reporting each as a path relative to `root`.
///
/// Directories are not reported as outputs themselves (see module docs):
/// [`MakeDirs`]-produced directories are still pruned once empty by
/// [`FsSink::delete_outputs`]'s ancestor-pruning, they are just not listed
/// here as pre-existing outputs to protect on startup GC.
fn walk_files(root: &Path, dir: &Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            walk_files(root, &path, out)?;
        } else if file_type.is_file() {
            let rel = path
                .strip_prefix(root)
                .expect("walked path is always under root")
                .to_path_buf();
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
            HashSet::from([PathBuf::from("sub/x.txt"), PathBuf::from("y.txt")])
        );
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
