# computations

A coarse-grained, push-based self-adjusting computation (SAC) engine for Rust.

## 1. What this is

Named computations are memoized async functions from a parameter to a
result. Calling one computation from another (via a shared `Ctx`) is
recorded as a dependency, so the set of computation applications actually
evaluated at runtime forms a dynamic dependency graph — dynamic because it
is discovered by tracing real calls as they happen, not declared up front.
External data (files, key-value stores, HTTP responses, ...) enters the
graph through pluggable `Source`s, and computation results leave it through
pluggable `Sink`s.

The engine is *push-based*: sources report changes as they occur (a file
was modified, a key was set), the driver maps each change back to the
computation applications that read it, and only those — and their
transitive dependents — are re-run. Recomputation stops early wherever a
node's result hash is unchanged from before (early cutoff): if
`parse_config` re-reads a file whose formatting changed but whose parsed
value didn't, everything downstream of `parse_config` is left alone. When a
node stops being reachable from a root (because its caller no longer calls
it, or the whole call was cut off), it becomes dead; a liveness GC pass
deletes any sink outputs it had produced and unregisters any source keys it
was the last watcher of.

This is a Rust implementation of the architecture described in Stefan
Wehr's FUNARCH'23 paper, [*A Software Architecture Based on Coarse-Grained
Self-Adjusting Computations*](https://doi.org/10.1145/3609025.3609481), and
is modeled directly on the reference Haskell implementation at
[github.com/skogsbaer/computations](https://github.com/skogsbaer/computations).
See [§5](#5-api-mapping) for how the two map onto each other name-for-name.

Deliberate simplifications relative to the paper (this is a from-scratch
Rust port, not a full port of every feature):

- **One caching strategy.** The paper offers a choice of caching
  strategies (full memoization, hash-based cutoff only, no caching); this
  engine always does both — memoize the value *and* cut off propagation on
  an unchanged result hash (`fullCaching`, roughly).
- **No request batching.** The Haskell driver batches independent
  requests behind the scenes (Haxl-style). Here, concurrency is explicit:
  call `Ctx::eval_all` (or hand-rolled `join!`/`try_join_all`) to run
  several evaluations concurrently. The engine still deduplicates
  concurrent calls to the *same* application via single-flight joining, so
  accidental duplicate work is avoided even without batching.
- **No incremental fold computations.** The paper's `defineIncComp` (an
  incremental fold over a changing list) has no analogue here; every
  computation re-runs its whole body from scratch on invalidation.
- **Persistence is opt-in and its own module, not a core engine
  responsibility.** Unlike the reference implementation's on-disk registry
  (always on), this port keeps the dependency graph in memory by default;
  calling `EngineBuilder::persistence` opts a given `Engine` into saving it
  to a local file and restoring it on the next run. See [§8](#8-persistence).

Two `Source` backends ship in this workspace: `computations-fs`
(filesystem — §3 below) and `computations-time`, a wall-clock time source
with no polling loop of its own. `computations_time::TimeSource` answers
two requests — `RoundedTime(bucket)` (current time rounded down to a
granularity like `Bucket::MINUTE` or `Bucket::FIVE_MINUTES`) and
`IsAfter(t)` (has wall-clock time passed `t`, flipping `false` to `true`
exactly once) — via one background task that sleeps until the next bucket
boundary or deadline actually due, recomputed fresh from the wall clock on
every wakeup. This is the analogue of the paper's built-in `compGetTime`
source (see [§5](#5-api-mapping)).

## 2. Quick example

```rust,ignore
use computations::Engine;
use computations_fs::{FsSink, FsSource};

# async fn example() -> anyhow::Result<()> {
let source = FsSource::new("fs")?;
let sink = FsSink::new("fs", "/tmp/out");

let mut builder = Engine::builder();
builder.source(source.clone());
builder.sink(sink.clone());

// `uppercase_file`: reads a file and writes its upper-cased contents.
// `EngineBuilder::define_with` is the environment-passing counterpart to
// `define_comp` + `register`: build `env` once, and the body gets an owned
// clone of it on every invocation — no per-call clone dance to write out
// by hand.
let env = (source, sink);
let uppercase_file = builder.define_with("uppercase_file", &env, |(source, sink), ctx, path: std::path::PathBuf| async move {
    let bytes = source.read_file(&ctx, path.clone()).await?;
    let upper = String::from_utf8_lossy(&bytes).to_uppercase();
    sink.write_file(&ctx, "out.txt", upper.into_bytes()).await?;
    Ok(())
});

let engine = builder.build();
// One-shot: `eval_root` runs it once and returns.
engine.eval_root(&uppercase_file, "/tmp/in.txt".into()).await?;
// Continuous: `run` evaluates once, then reacts to source changes forever.
// engine.run(uppercase_file, "/tmp/in.txt".into()).await?;
# Ok(())
# }
```

This is illustrative (marked `ignore` since it depends on real paths at
`/tmp`) rather than doc-tested; see `crates/computations-fs/examples/dirsync.rs`
for a complete, runnable version with two mutually-recursive computations.

`define_with`/`define_rec_with` (this section's flagship pattern) are the
environment-passing variants of `define`/`define_rec`: reach for the plain
`define`/`define_rec` instead when a computation's body needs no captured
environment at all (see the `sum`/`store` computations in
`crates/computations/tests/engine.rs` for that no-env case).

## 3. The dirsync demo

`computations-fs` ships a small standing demo: a directory mirror that
keeps a target directory an exact, live copy of a source directory.

```sh
cargo run -p computations-fs --example dirsync -- --source <A> --target <B>
```

It watches `<A>` and keeps `<B>` in sync: files are copied over,
directories are recreated, and anything removed from `<A>` — or present in
`<B>` before startup but not produced by any live computation — is deleted.
It runs forever; stop it with Ctrl-C.

Things worth trying while it runs:

- **Add/modify a file** in `<A>`: only that file's `sync_file` application
  (and, if it's new, its parent `sync_dir`) re-runs. Siblings are untouched.
- **Touch a file without changing its content** (e.g. `touch a.txt`): the
  filesystem source still wakes up (mtime changed), but the read's
  content, and hence `sync_file`'s result hash, are unchanged — early
  cutoff stops propagation right there, and `sync_dir` never re-runs.
- **Delete a file**: the parent directory's listing changes, `sync_dir`
  re-runs, and the now-unreachable `sync_file` application is collected by
  the next liveness GC pass, which deletes its `sync_file`-produced output.

Run it with tracing on to see all of this directly:

```sh
RUST_LOG=computations=debug cargo run -p computations-fs --example dirsync -- --source <A> --target <B>
```

## 4. Architecture

- **Engine / eval** (`engine.rs`): owns the definition table and the node
  table (one `Node` per evaluated `CompKey`, i.e. per computation
  application). `eval` is the single shared algorithm behind both a root
  call (`Engine::eval_root`) and a nested call (`Ctx::eval`): return a
  clean cached value, join an already-running execution (single-flight
  dedup), or actually run the body, content-hash its result, and reconcile
  dependencies/outputs against the previous run.
- **Ctx** (`ctx.rs`): the handle passed into a running computation body
  (the paper's `CompM` monad). Every effect a computation can have —
  calling another computation, reading a source, writing a sink — goes
  through `Ctx`, which is what lets the engine record it as a dependency of
  whichever application is currently executing. A computation that needs to
  call itself recursively should be built with `define_comp_rec` rather than
  `define_comp`: it hands the body a working `Comp` handle to its own
  definition, so there's no separate `Comp::named("x")` / `define_comp("x",
  ...)` pair with the name `"x"` repeated (and possibly drifting) between
  the two. Mutual recursion between two or more definitions still goes
  through `Comp::named` directly (see the `cycle_a`/`cycle_b` test in
  `crates/computations/tests/engine.rs`).
- **Sources** (`source.rs`): a plugin trait pair (`SourceBase` + `Source<R>`
  per request type `R`) for external inputs. Each read reports the
  `Dep`s (key + version) it touched; the version is opaque to the engine,
  just comparable. A source also exposes a push channel
  (`wait_changes`) the driver awaits for change notifications, and
  `unregister` so it can stop watching a key nothing depends on anymore.
- **Sinks** (`sink.rs`): the write-side counterpart (`SinkBase` +
  `Sink<R>`). Each write reports the outputs it produced, tracked per node
  so that outputs a node stops producing — or that belonged to a node that
  died entirely — can be deleted (garbage collection, not left to rot).
- **Registry** (`registry.rs`): the startup-time table of source/sink
  instances the driver polls and writes through. `EngineBuilder::source`/
  `EngineBuilder::sink` register directly into the builder's own registry
  (as seen in the quick example above); `EngineBuilder::registry` remains
  for callers that already assemble a `Registry` value separately (it
  *replaces* the builder's registry outright, so call it before any
  `source`/`sink` calls whose registrations should survive).
- **Driver** (`driver.rs`): the top-level loop (`Engine::run`). Initial
  evaluation, a startup GC pass (deleting any sink outputs nothing live
  produces), then forever: wait for a source-change batch, map it to
  affected nodes, propagate the resulting dirty frontier in waves
  (re-running each wave concurrently, stopping a branch's propagation at
  the first unchanged result hash — early cutoff), and run a liveness GC
  pass after each round settles.
- **Identity** (`key.rs`): a computation application (`CompKey`) is
  identified by its definition's name plus a stable content hash
  (`postcard` serialization, `blake3` digest) of its parameter — the
  `StableHash`/`LargeHashable` analogue. This is what makes memoization and
  dependency-graph node identity independent of object addresses or
  registration order.

## 5. API mapping

| Rust (this crate) | Paper / Haskell (`skogsbaer/computations`) |
|---|---|
| `define_comp` (or `EngineBuilder::define`, which is `define_comp` + `register` in one step — the preferred way to define a computation) | `defineComp` |
| `define_comp_rec` (or `EngineBuilder::define_rec`, the one-step equivalent) | *(not in the paper; a convenience over `define_comp` + `Comp::named` for self-recursive bodies)* |
| `Comp<P, R>` | `Comp` |
| `Ctx::eval` | `evalComp` |
| `Ctx::src_req` | `compSrcReq` |
| `Ctx::sink_req` | `compSinkReq` |
| `Engine::run` | `compDriver` |
| `Registry` | `CompFlowRegistry` |
| `StableHash` | `LargeHashable` |
| memoize + hash cutoff (always on) | `fullCaching` |
| `Source` / `SourceBase` | `CompSrc` |
| `Sink` / `SinkBase` | `CompSink` |
| `computations_time::TimeSource` (`RoundedTime`, `IsAfter`) | `compGetTime` |
| *(not implemented)* | `hashCaching` (cutoff without memoizing the value) |
| *(not implemented)* | `defineIncComp` (incremental fold computations) |

## 6. Tracing

Every evaluation runs inside a `comp.eval` span (`comp` = definition name,
`param_hash` = a short content hash of the parameter); every propagation
round runs inside a `driver.propagate` span. Turn tracing on with
`RUST_LOG`:

```sh
RUST_LOG=computations=info cargo run -p computations-fs --example dirsync -- --source A --target B
```

At `info`, you'll see two milestones: the initial evaluation completing
(with its elapsed time) and the startup GC summary. At `debug`, you get
the full picture: one completion event per computation application
(`outcome` = `cache_hit` | `dedup_join` | `executed` [+ `changed`,
`elapsed_ms`] | `error`), one event per propagation wave (dirty count,
reran, cutoffs), a round summary (waves, total reran, nodes GC'd, outputs
deleted), and a per-pass liveness/startup GC summary. `computations_fs`
adds its own light debug events: a change notification per watched path
(with the version's kind — file, dir, or missing) and a note before a sink
deletes a batch of outputs.

```sh
RUST_LOG=computations=debug,computations_fs=debug cargo run -p computations-fs --example dirsync -- --source A --target B
```

## 7. Status and limitations

This is a working, tested implementation of the paper's core model — dynamic
dependency tracking, memoization with hash-based early cutoff, single-flight
concurrency dedup, cycle detection, and liveness-driven output GC — plus two
real plugin backends (the filesystem, and wall-clock time), plus opt-in
dependency-graph persistence across restarts (see [§8](#8-persistence)). It
does not implement request batching or incremental fold computations (see
[§1](#1-what-this-is) for why). The `computations-fs` crate's `notify::PollWatcher`-based source
trades a little latency (bounded by a 100ms poll interval) for
filesystem-change delivery that behaves identically across platforms and
sandboxes, which matters more for its test suite than shaving milliseconds
off change detection. The `computations-time` crate's source has no such
tradeoff to make: it drives every change notification off a single
background task sleeping until the next bucket boundary or deadline
actually due, with no polling loop at all.

Run the test suite (unit and integration tests across all three crates,
plus a tracing smoke test and a doc test):

```sh
cargo test --workspace --all-features
```

## 8. Persistence

Persistence is opt-in, per-`Engine`, and exists purely as a warm-start
cache — nothing about it is a source of truth, and nothing about it can
fail the engine.

```rust
use computations::{Engine, Fingerprint, PersistOptions};

builder.persistence(PersistOptions {
    path: "graph.redb".into(),
    fingerprint: Fingerprint::current_exe(),
});
```

**Storage.** The graph is saved to a local [redb](https://www.redb.org)
file: a `meta` row (format version + fingerprint) and one `nodes` row per
memoized computation application (its parameter, result, dependency edges,
and sink outputs — all postcard-encoded). After every settled propagation
round, and once after the initial evaluation, only the nodes that actually
changed or were garbage-collected are written — never a wholesale rewrite.
`Engine::persist_now` forces an immediate save (useful for a caller that
wants a deterministic "everything settled is now on disk" point, e.g. a
test simulating a restart).

**Fingerprint.** A `Fingerprint` identifies "the code that produced these
results" — `Fingerprint::current_exe()` hashes the running binary (the
common case: did the executable change since this graph was saved);
`Fingerprint::custom(data)` lets a caller fingerprint anything else that
should invalidate trust in a persisted graph (a version string, a config
hash, ...). A mismatch at load time never discards the graph — it marks
every restored node for background revalidation instead (see below).

**Load-anyway, then verify.** On restart, every persisted node whose
definition is still registered is restored as a cache hit — no
recomputation — and then two independent checks decide how much that cache
hit should actually be trusted:

- A fingerprint mismatch marks the *entire* restored graph
  `DirtyPriority::Revalidate`: cheap to double-check in the background,
  and (per the two-tier dirty priority scheme) never allowed to block a
  genuinely changed input.
- Every restored source dependency is re-checked via
  `SourceBase::probe_versions` against its source's current value.
  Anything that changed (or that a source can't or won't confirm) marks
  its dependents `DirtyPriority::Input`, indistinguishable from a change
  that had just arrived.

**Cache, not source of truth.** A missing file, an unreadable or
wrong-format database, or a record referencing a definition/type that no
longer exists is always safe to drop — the affected node (or, in the
worst case, the whole database) is simply treated as absent and
recomputed. Persistence failures are logged (`tracing::warn`) and never
propagated: the engine always starts, cold if it has to.
