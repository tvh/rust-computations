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
//! - `sync_dir(rel)`: creates `target_root/rel` (a no-op for the root itself,
//!   since the sink's root already exists), lists `source_root/rel`, then
//!   evaluates `sync_file` over every file entry and `sync_dir` over every
//!   subdirectory entry concurrently (Haxl-style: both waves of child
//!   evaluations run at once, each wave itself deduplicated/parallelized via
//!   `Ctx::eval_all`).
//!
//! `sync_dir` calling itself is the standard recursion pattern for this
//! engine, handled here by `EngineBuilder::define_rec_with`: it hands the
//! body a working handle to its own computation (`sync_dir` below), so there
//! is no need to separately write `Comp::named("sync_dir")` and
//! `define_comp("sync_dir", ...)` with the name repeated (and possibly
//! drifting) between the two.
//!
//! Both definitions are built with the `_with` variants
//! (`EngineBuilder::define_with` / `EngineBuilder::define_rec_with`): `let
//! env = (source, sink, source_root);` is built once, after registering
//! `source`/`sink` on the builder (which each need one clone of their own,
//! since the builder keeps its own `Arc` alongside the one moved into
//! `env`), and lent by reference to both `define_with`/`define_rec_with`
//! calls. Each body destructures its `(source, sink, source_root)` env
//! parameter directly — those two registration clones are the only clones
//! left in this wiring for capturing shared state; the one remaining
//! `rel.clone()` in `sync_dir`'s body is an ordinary value clone (`rel` is
//! consumed by `make_dirs` and then read again below), unrelated to the env
//! pattern.
use std::path::PathBuf;

use computations::Engine;
use computations_fs::{DirEntry, EntryKind, FsSink, FsSource};

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
    let sink = FsSink::new("dirsync-sink", &args.target);

    let mut builder = Engine::builder();
    builder.source(source.clone());
    builder.sink(sink.clone());

    let env = (source, sink, args.source);
    let sync_file = builder.define_with("sync_file", &env, |(source, sink, source_root), ctx, rel: PathBuf| async move {
        let full = source_root.join(&rel);
        match source.read_file(&ctx, full).await {
            Ok(contents) => {
                sink.write_file(&ctx, rel, contents).await?;
            }
            Err(e) => {
                // The file may have vanished (or been replaced by a
                // directory, etc.) between its parent directory's listing
                // and this read. Rather than failing the node (which would
                // leave a stale write behind and spam retries), we log and
                // succeed with no output: the parent directory's `ListDir`
                // dependency will itself have changed (the entry
                // disappeared), which re-triggers `sync_dir` and,
                // transitively, drops this `sync_file` application if it's
                // no longer listed — or re-attempts it if the file
                // reappears.
                tracing::warn!(rel = %rel.display(), error = %e, "read failed; skipping this round");
            }
        }
        Ok(())
    });

    let sync_dir = builder.define_rec_with(
        "sync_dir",
        &env,
        move |(source, sink, source_root), sync_dir, ctx, rel: PathBuf| async move {
            // A no-op for the root itself (empty `rel`): the target root
            // already exists (`FsSink::new` creates it).
            sink.make_dirs(&ctx, rel.clone()).await?;

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

            // Haxl-style: both waves of children run concurrently, each
            // wave itself batched via `eval_all`.
            let files_fut = ctx.eval_all(sync_file, file_rels);
            let dirs_fut = ctx.eval_all(sync_dir, dir_rels);
            tokio::try_join!(files_fut, dirs_fut)?;

            Ok(())
        },
    );

    let engine = builder.build();
    engine.run(sync_dir, PathBuf::new()).await?;
    Ok(())
}
