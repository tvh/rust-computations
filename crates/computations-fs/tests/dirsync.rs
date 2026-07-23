//! End-to-end convergence test for the `dirsync` demo's wiring.
//!
//! Duplicates the computation wiring from
//! `examples/dirsync.rs` (examples aren't importable from an integration
//! test crate) and drives it against real temp directories via
//! `Engine::run`, asserting that the target directory converges to mirror
//! the source directory through a sequence of live edits: initial sync
//! (plus startup GC of a stale pre-existing target file), content
//! modification, addition, file deletion (liveness GC), and whole-subtree
//! deletion (liveness GC).
//!
//! `FsSource` polls every 100ms (see `computations-fs::source` docs), so
//! every wait uses a generous (15s) timeout to stay robust in slower CI
//! environments while still failing fast on an actual regression.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use computations::{Comp, Engine};
use computations_fs::{DirEntry, EntryKind, FsSink, FsSource};

/// Polls `f` every 20ms until it returns `true`, panicking with `msg` if 15s
/// pass first. Generous relative to the source's 100ms poll interval so
/// propagation delay never causes a flaky failure.
async fn wait_until(msg: &str, f: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if f() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("condition did not become true within 15s: {msg}"));
}

/// Recursively reads every regular file under `root` into a `path -> bytes`
/// map, keyed by path relative to `root` (with forward-slash-joined
/// components so it's platform-independent to compare/print).
fn snapshot_files(root: &Path) -> BTreeMap<String, Vec<u8>> {
    fn walk(root: &Path, dir: &Path, out: &mut BTreeMap<String, Vec<u8>>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(root, &path, out);
            } else if path.is_file() {
                let rel = path.strip_prefix(root).unwrap();
                let rel_str = rel.components().map(|c| c.as_os_str().to_string_lossy()).collect::<Vec<_>>().join("/");
                out.insert(rel_str, std::fs::read(&path).unwrap());
            }
        }
    }
    let mut out = BTreeMap::new();
    walk(root, root, &mut out);
    out
}

/// Asserts that `target` contains exactly the same set of files, with the
/// same contents, as `source` — the definition of "mirrors exactly" used
/// throughout this test.
fn assert_mirrors(source: &Path, target: &Path) {
    let src_snapshot = snapshot_files(source);
    let tgt_snapshot = snapshot_files(target);
    assert_eq!(
        src_snapshot, tgt_snapshot,
        "target does not mirror source\nsource: {source:?}\ntarget: {target:?}"
    );
}

/// Builds the same `sync_file` / `sync_dir` computation pair as
/// `examples/dirsync.rs`, registered against `source_root`/the given sink,
/// and returns the root `sync_dir` handle to drive with `Engine::run`.
fn build_engine(source_root: PathBuf, sink: Arc<FsSink>) -> (Engine, Comp<PathBuf, ()>) {
    let source = FsSource::new("test-source").unwrap();

    let mut builder = Engine::builder();
    builder.source(source.clone());
    builder.sink(sink.clone());

    let env = (source, sink, source_root);
    let sync_file = builder.define_with("sync_file", &env, |(source, sink, source_root), ctx, rel: PathBuf| async move {
        let full = source_root.join(&rel);
        match source.read_file(&ctx, full).await {
            Ok(contents) => {
                sink.write_file(&ctx, rel, contents).await?;
            }
            Err(e) => {
                tracing::warn!(rel = %rel.display(), error = %e, "read failed; skipping this round");
            }
        }
        Ok(())
    });

    let sync_dir = builder.define_rec_with(
        "sync_dir",
        &env,
        move |(source, sink, source_root), sync_dir, ctx, rel: PathBuf| async move {
            if !rel.as_os_str().is_empty() {
                sink.make_dirs(&ctx, rel.clone()).await?;
            }

            let entries = source.list_dir(&ctx, source_root.join(&rel)).await?;
            let mut file_rels = Vec::new();
            let mut dir_rels = Vec::new();
            for DirEntry { name, kind } in entries {
                let child_rel = rel.join(&name);
                match kind {
                    EntryKind::File => file_rels.push(child_rel),
                    EntryKind::Dir => dir_rels.push(child_rel),
                }
            }

            let files_fut = ctx.eval_all(&sync_file, file_rels);
            let dirs_fut = ctx.eval_all(&sync_dir, dir_rels);
            tokio::try_join!(files_fut, dirs_fut)?;

            Ok(())
        },
    );

    let engine = builder.build();
    (engine, sync_dir)
}

/// Drives `sync_dir(root)` end to end through a sequence of live edits to
/// the source directory, asserting the target converges to mirror it after
/// each one. Kept as a single test (rather than several) because each step
/// depends on the driver state left by the previous one.
#[tokio::test(flavor = "multi_thread")]
async fn dirsync_converges_through_a_sequence_of_edits() {
    let src_dir = tempfile::tempdir().unwrap();
    let tgt_dir = tempfile::tempdir().unwrap();
    let src = src_dir.path();
    let tgt = tgt_dir.path();

    // --- Set up initial source tree, plus a stale file already present in
    // the target that no live computation will produce. ---
    std::fs::write(src.join("root.txt"), b"root contents").unwrap();
    std::fs::create_dir_all(src.join("sub")).unwrap();
    std::fs::write(src.join("sub/nested.txt"), b"nested contents").unwrap();

    std::fs::write(tgt.join("stale.txt"), b"should be removed by startup GC").unwrap();

    let sink = Arc::new(FsSink::new("test-sink", tgt));
    let (engine, root) = build_engine(src.to_path_buf(), sink);

    let handle = {
        let engine = engine.clone();
        tokio::spawn(async move { engine.run(&root, PathBuf::new()).await })
    };

    // 1. Initial sync + startup GC.
    wait_until("initial sync should mirror source and remove the stale file", || {
        !tgt.join("stale.txt").exists() && tgt.join("root.txt").exists() && tgt.join("sub/nested.txt").exists()
    })
    .await;
    assert_mirrors(src, tgt);

    // 2. Modify a file's contents in the source.
    std::fs::write(src.join("root.txt"), b"root contents v2").unwrap();
    wait_until("modified root.txt content should propagate", || {
        std::fs::read(tgt.join("root.txt")).ok().as_deref() == Some(b"root contents v2".as_slice())
    })
    .await;
    assert_mirrors(src, tgt);

    // 3. Add a new file in the source's subdirectory.
    std::fs::write(src.join("sub/new.txt"), b"new file contents").unwrap();
    wait_until("new file in subdir should appear in target", || tgt.join("sub/new.txt").exists()).await;
    assert_mirrors(src, tgt);

    // 4. Delete a file in the source (liveness GC of its output).
    std::fs::remove_file(src.join("sub/new.txt")).unwrap();
    wait_until("deleted file should disappear from target", || !tgt.join("sub/new.txt").exists()).await;
    assert_mirrors(src, tgt);

    // 5. Delete the whole subdirectory in the source (subtree liveness GC).
    std::fs::remove_dir_all(src.join("sub")).unwrap();
    wait_until("deleted subdir's subtree should disappear from target", || !tgt.join("sub").exists()).await;
    assert_mirrors(src, tgt);

    handle.abort();
}
