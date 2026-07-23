//! A continuously-running directory mirror built on `computations-fs`.
//!
//! ```text
//! cargo run -p computations-fs --example dirsync -- --source <A> --target <B> [--log-level <level>]
//! ```
//!
//! Watches `<A>` and keeps `<B>` an exact mirror of it: files are copied
//! over, directories are recreated, and anything removed from `<A>` (or
//! already present in `<B>` before startup but not produced by any live
//! computation) is deleted from `<B>`. Runs forever; stop it with Ctrl-C.
//!
//! ## Wiring
//!
//! Two mutually-referential computations, keyed by paths *relative* to the
//! source/target roots (so their identity — and thus memoization — survives
//! the roots themselves being arbitrary absolute paths):
//!
//! - `sync_file(rel)`: reads `source_root/rel` and writes it to `target_root/rel`.
//! - `sync_dir(rel)`: creates `target_root/rel` (skipped for the root itself,
//!   since the sink refuses an empty relative path and the target root
//!   already exists), lists `source_root/rel`, then evaluates `sync_file`
//!   over every file entry and `sync_dir` over every subdirectory entry
//!   concurrently (Haxl-style: both waves of child evaluations run at once,
//!   each wave itself deduplicated/parallelized via `Ctx::eval_all`).
//!
//! `sync_dir` calling itself is the standard recursion pattern for this
//! engine: [`Comp::named`] creates a handle from the definition's name alone,
//! with no body attached, so a definition's own body can close over a
//! handle to itself (created before the definition exists) and the two are
//! reconciled once `EngineBuilder::register` is called with that same name.
use std::path::PathBuf;
use std::sync::Arc;

use computations::{Comp, Engine, Registry, define_comp};
use computations_fs::{DirEntry, EntryKind, FsSink, FsSource, ListDir, MakeDirs, ReadFile, WriteFile};

struct Args {
    source: PathBuf,
    target: PathBuf,
    log_level: String,
}

fn parse_args() -> Args {
    let mut source = None;
    let mut target = None;
    let mut log_level = "info".to_string();

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--source" => {
                source = Some(args.next().unwrap_or_else(|| usage_error("--source requires a value")));
            }
            "--target" => {
                target = Some(args.next().unwrap_or_else(|| usage_error("--target requires a value")));
            }
            "--log-level" => {
                log_level = args.next().unwrap_or_else(|| usage_error("--log-level requires a value"));
            }
            other => usage_error(&format!("unrecognized argument: {other}")),
        }
    }

    let source = source.unwrap_or_else(|| usage_error("missing required --source <path>"));
    let target = target.unwrap_or_else(|| usage_error("missing required --target <path>"));

    Args {
        source: PathBuf::from(source),
        target: PathBuf::from(target),
        log_level,
    }
}

fn usage_error(msg: &str) -> ! {
    eprintln!(
        "error: {msg}\n\nusage: dirsync --source <path> --target <path> [--log-level <level>]"
    );
    std::process::exit(2);
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let args = parse_args();

    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&args.log_level));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    println!("dirsync: mirroring {} -> {}", args.source.display(), args.target.display());

    let source = FsSource::new("dirsync-source")?;
    let sink = Arc::new(FsSink::new("dirsync-sink", &args.target));

    let mut registry = Registry::default();
    registry.register_source(source.clone());
    registry.register_sink(sink.clone());

    let mut builder = Engine::builder();
    builder.registry(registry);

    // Pre-created handle so `sync_dir`'s own body can call itself
    // recursively; see the module docs for why this works (`Comp::named`
    // names a computation without requiring its `CompDef` to exist yet).
    let sync_dir: Comp<PathBuf, ()> = Comp::named("sync_dir");

    let source_root = args.source.clone();
    let sync_file: Comp<PathBuf, ()> = builder.register(define_comp("sync_file", {
        let source = source.clone();
        let sink = sink.clone();
        let source_root = source_root.clone();
        move |ctx, rel: PathBuf| {
            let source = source.clone();
            let sink = sink.clone();
            let source_root = source_root.clone();
            async move {
                let full = source_root.join(&rel);
                match ctx.src_req(&source, ReadFile(full)).await {
                    Ok(contents) => {
                        ctx.sink_req(&sink, WriteFile { rel_path: rel, contents }).await?;
                    }
                    Err(e) => {
                        // The file may have vanished (or been replaced by a
                        // directory, etc.) between its parent directory's
                        // listing and this read. Rather than failing the
                        // node (which would leave a stale write behind and
                        // spam retries), we log and succeed with no output:
                        // the parent directory's `ListDir` dependency will
                        // itself have changed (the entry disappeared), which
                        // re-triggers `sync_dir` and, transitively, drops
                        // this `sync_file` application if it's no longer
                        // listed — or re-attempts it if the file reappears.
                        tracing::warn!(rel = %rel.display(), error = %e, "read failed; skipping this round");
                    }
                }
                Ok(())
            }
        }
    }));

    let sync_dir_def: Comp<PathBuf, ()> = builder.register(define_comp("sync_dir", {
        let source = source.clone();
        let sink = sink.clone();
        let source_root = source_root.clone();
        let sync_file = sync_file.clone();
        let sync_dir = sync_dir.clone();
        move |ctx, rel: PathBuf| {
            let source = source.clone();
            let sink = sink.clone();
            let source_root = source_root.clone();
            let sync_file = sync_file.clone();
            let sync_dir = sync_dir.clone();
            async move {
                // The target root itself always exists (`FsSink::new`
                // creates it); `MakeDirs` also rejects an empty relative
                // path, so only non-root directories need it created.
                if !rel.as_os_str().is_empty() {
                    ctx.sink_req(&sink, MakeDirs { rel_path: rel.clone() }).await?;
                }

                let entries = ctx.src_req(&source, ListDir(source_root.join(&rel))).await?;
                let mut file_rels = Vec::new();
                let mut dir_rels = Vec::new();
                for DirEntry { name, kind } in entries {
                    let child_rel = rel.join(&name);
                    match kind {
                        EntryKind::File => file_rels.push(child_rel),
                        EntryKind::Dir => dir_rels.push(child_rel),
                    }
                }

                // Haxl-style: both waves of children run concurrently, each
                // wave itself batched via `eval_all`.
                let files_fut = ctx.eval_all(&sync_file, file_rels);
                let dirs_fut = ctx.eval_all(&sync_dir, dir_rels);
                tokio::try_join!(files_fut, dirs_fut)?;

                Ok(())
            }
        }
    }));

    let engine = builder.build();
    engine.run(&sync_dir_def, PathBuf::new()).await?;
    Ok(())
}
