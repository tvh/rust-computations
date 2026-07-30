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
//! Two mutually-referential `#[computation]` functions (Phase B — see
//! `docs/persistence-benchmark-notes.md`'s Stage 10), keyed by paths
//! *relative* to the source/target roots (so their identity — and thus
//! memoization — survives the roots themselves being arbitrary absolute
//! paths):
//!
//! - `sync_file(source_root, rel)`: reads `source_root/rel` and writes it to
//!   `target_root/rel` (`target_root` lives inside the `#[flow] sink`
//!   itself — see [`FsSink`] — so it never needs to be threaded as a
//!   parameter).
//! - `sync_dir(source_root, rel)`: creates `target_root/rel` (a no-op for
//!   the root itself, since the sink's root already exists), lists
//!   `source_root/rel`, then evaluates `sync_file` over every file entry
//!   and `sync_dir` over every subdirectory entry concurrently (Haxl-style:
//!   both waves of child evaluations run at once, each wave itself batched
//!   via `futures::future::try_join_all`).
//!
//! `sync_dir` calling itself is ordinary self-recursion: the call inside
//! its own body (`sync_dir(ctx, source, sink, ...)`) resolves to the very
//! same `#[computation]`-generated wrapper function this whole module
//! defines, exactly as any other function call would — there is no special
//! "recursive" registration step to opt into (contrast the builder path's
//! own `EngineBuilder::define_rec_with`, which exists only because a
//! builder-registered computation has no name of its own to refer to until
//! it is registered).
//!
//! `source`/`sink` are `#[flow]` arguments (their *instance* identity joins
//! every node's identity — see `computations::flow`'s module docs); the
//! filesystem root each one is anchored at is different for the two:
//! `FsSink::new` bakes the target root into the sink itself, but `FsSource`
//! has no root of its own (it watches whatever absolute paths it is asked
//! to read/list), so `source_root` is instead an ordinary (hashed,
//! persisted) parameter, threaded through both computations alongside
//! `rel` — this crate's `#[computation]` proof of multi-param bundling
//! (`computations/tests/computation_macro.rs` has a synthetic version of
//! the same shape).
//!
//! There is no `EngineBuilder::define*` call, no `Comp<P, R>` handle, and no
//! captured `env` tuple anywhere in this file: `EngineBuilder::build()`
//! collects both computations automatically (Phase B's `inventory`-based
//! registration — see `computations::flow::ComputationEntry`'s docs), and
//! the initial root evaluation drives `sync_dir` directly by name via
//! `Engine::run_flows`, building its `FlowId` list from the same public
//! `computations::flow::AsFlowId`/`AsFlowIdSink` traits the macro's own
//! generated code uses internally.
use std::path::PathBuf;
use std::sync::Arc;

use computations::error::CompError;
use computations::flow::{AsFlowId as _, AsFlowIdSink as _};
use computations::{Ctx, Engine, computation};
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

#[computation]
async fn sync_file(
    ctx: &Ctx,
    #[flow] source: &Arc<FsSource>,
    #[flow] sink: &Arc<FsSink>,
    source_root: PathBuf,
    rel: PathBuf,
) -> Result<(), CompError> {
    let full = source_root.join(&rel);
    match source.read_file(ctx, full).await {
        Ok(contents) => {
            sink.write_file(ctx, rel, contents).await?;
        }
        Err(e) => {
            // The file may have vanished (or been replaced by a directory,
            // etc.) between its parent directory's listing and this read.
            // Rather than failing the node (which would leave a stale write
            // behind and spam retries), we log and succeed with no output:
            // the parent directory's `ListDir` dependency will itself have
            // changed (the entry disappeared), which re-triggers `sync_dir`
            // and, transitively, drops this `sync_file` application if it's
            // no longer listed — or re-attempts it if the file reappears.
            tracing::warn!(rel = %rel.display(), error = %e, "read failed; skipping this round");
        }
    }
    Ok(())
}

#[computation]
async fn sync_dir(
    ctx: &Ctx,
    #[flow] source: &Arc<FsSource>,
    #[flow] sink: &Arc<FsSink>,
    source_root: PathBuf,
    rel: PathBuf,
) -> Result<(), CompError> {
    // A no-op for the root itself (empty `rel`): the target root already
    // exists (`FsSink::new` creates it).
    sink.make_dirs(ctx, rel.clone()).await?;

    let entries = source.list_dir(ctx, source_root.join(&rel)).await?;
    let mut file_rels = Vec::new();
    let mut dir_rels = Vec::new();
    for DirEntry { name, kind } in entries {
        let child_rel = rel.join(&name);
        match kind {
            EntryKind::File => file_rels.push(child_rel),
            EntryKind::Dir => dir_rels.push(child_rel),
        }
    }

    // Haxl-style: both waves of children run concurrently, each wave itself
    // batched via `try_join_all` (the `#[computation]`-generated wrapper
    // functions are plain `async fn`s, not `Comp<P, R>` handles, so this
    // plays the role `Ctx::eval_all` plays for the builder path).
    let files_fut =
        futures::future::try_join_all(file_rels.into_iter().map(|r| sync_file(ctx, source, sink, source_root.clone(), r)));
    let dirs_fut =
        futures::future::try_join_all(dir_rels.into_iter().map(|r| sync_dir(ctx, source, sink, source_root.clone(), r)));
    tokio::try_join!(files_fut, dirs_fut)?;

    Ok(())
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
    let engine = builder.build();

    let flows = [source.as_flow_id(), sink.as_flow_id()];
    engine
        .run_flows::<(PathBuf, PathBuf), ()>(SYNC_DIR_NAME, &flows, (args.source, PathBuf::new()))
        .await?;
    Ok(())
}
