//! Integration tests for the `#[computation]` proc-macro (Phase B — see
//! `docs/persistence-benchmark-notes.md`'s Stage 10, and `tests/flow.rs`,
//! whose hand-written computations are exactly what this macro expands
//! `#[computation]`-annotated functions into).
//!
//! Every scenario below defines its `#[computation]` function(s) in their
//! own module (module-path-derived names must stay unique across
//! scenarios, exactly as `tests/flow.rs`'s own module docs explain) and
//! never calls `EngineBuilder::define_flows`/`define` for the
//! `#[computation]`-generated computation itself — the whole point being
//! demonstrated here is that `EngineBuilder::build()` finds and registers
//! it automatically, via the `inventory`-based collection
//! `computations::flow::ComputationEntry` describes. Where a scenario also
//! defines an ordinary builder-registered "root" computation, that's purely
//! a driving harness (`Engine::eval_root`/`Engine::run` need a
//! registration-backed `Comp<P, R>` handle to start from) and doubles as
//! coexistence evidence, mirroring `tests/flow.rs`'s own pattern.
//!
//! Run with `cargo test -p computations --features testutil`. Gated on the
//! (default-on) `macros` feature at the whole-file level: with
//! `--no-default-features`, `#[computation]` doesn't exist at all, so this
//! entire test binary is skipped rather than failing to compile.
#![cfg(feature = "macros")]

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use computations::error::CompError;
use computations::testutil::{GetKey, MemKvSource, VecSink, WriteDoc};
use computations::{Comp, Ctx, Engine, Fingerprint, PersistOptions, Registry, computation};

/// Polls `f` every 10ms until it returns `true`, panicking if 5s pass first
/// — mirrors `tests/flow.rs`'s own helper.
async fn wait_until(f: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if f() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("condition did not become true within 5s");
}

// =====================================================================
// Scenario 1: a single-flow computation (one source, no sink) — evaluates
// correctly and memoizes, with zero explicit `define_flows` call anywhere.
// =====================================================================
mod single_flow {
    use super::*;

    pub static RUNS: AtomicUsize = AtomicUsize::new(0);

    #[computation]
    pub async fn read_key(ctx: &Ctx, #[flow] source: &Arc<MemKvSource>, key: String) -> Result<String, CompError> {
        RUNS.fetch_add(1, Ordering::SeqCst);
        Ok(ctx.src_req(source, GetKey(key)).await?.unwrap_or_default())
    }
}

#[tokio::test]
async fn single_flow_computation_evaluates_and_memoizes() {
    let kv = MemKvSource::new("single_flow_kv");
    kv.set("a", "hello").await;

    let mut builder = Engine::builder();
    builder.source(kv.clone());
    let root: Comp<String, String> = builder.define("single_flow_root", {
        let kv = kv.clone();
        move |ctx, key: String| {
            let kv = kv.clone();
            async move { single_flow::read_key(&ctx, &kv, key).await }
        }
    });
    let engine = builder.build();

    let result = engine.eval_root(&root, "a".to_string()).await.unwrap();
    assert_eq!(result, "hello");
    assert_eq!(single_flow::RUNS.load(Ordering::SeqCst), 1);

    // Same param -- a cache hit, not a second run.
    let result = engine.eval_root(&root, "a".to_string()).await.unwrap();
    assert_eq!(result, "hello");
    assert_eq!(single_flow::RUNS.load(Ordering::SeqCst), 1, "unchanged re-evaluation must be a cache hit");
}

// =====================================================================
// Scenario 2: a multi-flow computation (source + sink) — evaluates,
// memoizes, and re-runs under the live driver on a genuine source change.
// =====================================================================
mod multi_flow {
    use super::*;

    pub static RUNS: AtomicUsize = AtomicUsize::new(0);

    #[computation]
    pub async fn sync_doc(ctx: &Ctx, #[flow] source: &Arc<MemKvSource>, #[flow] sink: &Arc<VecSink>, key: String) -> Result<(), CompError> {
        RUNS.fetch_add(1, Ordering::SeqCst);
        let value = ctx.src_req(source, GetKey(key.clone())).await?.unwrap_or_default();
        ctx.sink_req(sink, WriteDoc { name: key, content: value }).await?;
        Ok(())
    }
}

#[tokio::test]
async fn multi_flow_computation_reruns_on_source_change_under_the_driver() {
    let kv = MemKvSource::new("multi_flow_kv");
    let sink = VecSink::new("multi_flow_docs");
    kv.set("a", "v0").await;

    let mut builder = Engine::builder();
    builder.source(kv.clone());
    builder.sink(sink.clone());
    let root: Comp<String, ()> = builder.define("multi_flow_root", {
        let kv = kv.clone();
        let sink = sink.clone();
        move |ctx, key: String| {
            let kv = kv.clone();
            let sink = sink.clone();
            async move { multi_flow::sync_doc(&ctx, &kv, &sink, key).await }
        }
    });
    let engine = builder.build();

    let handle = {
        let engine = engine.clone();
        tokio::spawn(async move { engine.run(root, "a".to_string()).await })
    };

    wait_until(|| sink.get("a").as_deref() == Some("v0")).await;
    assert_eq!(multi_flow::RUNS.load(Ordering::SeqCst), 1);

    kv.set("a", "v1").await;
    wait_until(|| sink.get("a").as_deref() == Some("v1")).await;
    assert_eq!(multi_flow::RUNS.load(Ordering::SeqCst), 2, "a genuine source change must cause exactly one rerun");

    handle.abort();
}

// =====================================================================
// Scenario 3: a zero-flow (params-only) computation — still routes through
// `Ctx::eval_flows` (an empty flow list), still gets automatic
// registration, and its identity is driven entirely by its (here,
// multi-param, tuple-bundled) parameters.
// =====================================================================
mod params_only {
    use super::*;

    pub static RUNS: AtomicUsize = AtomicUsize::new(0);

    #[computation]
    pub async fn combine(_ctx: &Ctx, a: i64, b: i64) -> Result<i64, CompError> {
        RUNS.fetch_add(1, Ordering::SeqCst);
        Ok(a * 10 + b)
    }
}

#[tokio::test]
async fn zero_flow_params_only_computation_memoizes_and_orders_params() {
    let mut builder = Engine::builder();
    let root: Comp<(i64, i64), i64> = builder.define("params_only_root", |ctx, (a, b): (i64, i64)| async move {
        params_only::combine(&ctx, a, b).await
    });
    let engine = builder.build();

    let result = engine.eval_root(&root, (2, 3)).await.unwrap();
    assert_eq!(result, 23);
    assert_eq!(params_only::RUNS.load(Ordering::SeqCst), 1);

    // Same params -- cache hit.
    engine.eval_root(&root, (2, 3)).await.unwrap();
    assert_eq!(params_only::RUNS.load(Ordering::SeqCst), 1);

    // Swapped params -- a distinct identity (proves the params tuple's
    // ordering is preserved, not e.g. sorted or otherwise normalized), and
    // a distinct result.
    let swapped = engine.eval_root(&root, (3, 2)).await.unwrap();
    assert_eq!(swapped, 32);
    assert_eq!(params_only::RUNS.load(Ordering::SeqCst), 2, "a different param ordering must be a fresh run");
}

// =====================================================================
// Scenario 4: a flow argument combined with multiple ordinary params (the
// `dirsync`-style shape) — proves the params tuple's declared left-to-right
// ordering guarantee even in the presence of a `#[flow]` argument
// interleaved before them.
// =====================================================================
mod multi_param_with_flow {
    use super::*;

    pub static RUNS: AtomicUsize = AtomicUsize::new(0);

    #[computation]
    pub async fn write_pair(ctx: &Ctx, #[flow] sink: &Arc<VecSink>, name: String, content: String) -> Result<(), CompError> {
        RUNS.fetch_add(1, Ordering::SeqCst);
        ctx.sink_req(sink, WriteDoc { name, content }).await?;
        Ok(())
    }
}

#[tokio::test]
async fn multi_param_with_flow_preserves_argument_order() {
    let sink = VecSink::new("multi_param_with_flow_docs");

    let mut builder = Engine::builder();
    builder.sink(sink.clone());
    let root: Comp<(String, String), ()> = builder.define("multi_param_with_flow_root", {
        let sink = sink.clone();
        move |ctx, (name, content): (String, String)| {
            let sink = sink.clone();
            async move { multi_param_with_flow::write_pair(&ctx, &sink, name, content).await }
        }
    });
    let engine = builder.build();

    engine.eval_root(&root, ("doc1".to_string(), "hello".to_string())).await.unwrap();
    assert_eq!(sink.get("doc1"), Some("hello".to_string()));
    assert_eq!(multi_param_with_flow::RUNS.load(Ordering::SeqCst), 1);

    // Swap which string plays which role -- a distinct node (and it writes
    // under a different name), proving `name`/`content` were never
    // silently transposed by the tuple-bundling step.
    engine.eval_root(&root, ("hello".to_string(), "doc1".to_string())).await.unwrap();
    assert_eq!(sink.get("hello"), Some("doc1".to_string()));
    assert_eq!(multi_param_with_flow::RUNS.load(Ordering::SeqCst), 2);
}

// =====================================================================
// Scenario 5: mutual recursion between two `#[computation]` functions
// (constraint 5 — now explicitly supported, since compile-time name
// resolution through the generated wrapper functions removes the hazard
// that used to motivate banning it).
// =====================================================================
mod mutual_recursion {
    use super::*;

    pub static EVEN_RUNS: AtomicUsize = AtomicUsize::new(0);
    pub static ODD_RUNS: AtomicUsize = AtomicUsize::new(0);

    #[computation]
    pub async fn is_even(ctx: &Ctx, #[flow] sink: &Arc<VecSink>, n: u32) -> Result<bool, CompError> {
        EVEN_RUNS.fetch_add(1, Ordering::SeqCst);
        if n == 0 { Ok(true) } else { is_odd(ctx, sink, n - 1).await }
    }

    #[computation]
    pub async fn is_odd(ctx: &Ctx, #[flow] sink: &Arc<VecSink>, n: u32) -> Result<bool, CompError> {
        ODD_RUNS.fetch_add(1, Ordering::SeqCst);
        if n == 0 { Ok(false) } else { is_even(ctx, sink, n - 1).await }
    }
}

#[tokio::test]
async fn mutual_recursion_between_two_computation_functions() {
    let sink = VecSink::new("mutual_recursion_docs");

    let mut builder = Engine::builder();
    builder.sink(sink.clone());
    let root: Comp<u32, bool> = builder.define("mutual_recursion_root", {
        let sink = sink.clone();
        move |ctx, n: u32| {
            let sink = sink.clone();
            async move { mutual_recursion::is_even(&ctx, &sink, n).await }
        }
    });
    let engine = builder.build();

    assert!(engine.eval_root(&root, 10).await.unwrap());
    assert!(!engine.eval_root(&root, 7).await.unwrap());

    assert!(
        mutual_recursion::EVEN_RUNS.load(Ordering::SeqCst) > 0 && mutual_recursion::ODD_RUNS.load(Ordering::SeqCst) > 0,
        "both mutually-recursive computations must actually have run"
    );
}

// =====================================================================
// Scenario 6: the identity guarantee — the SAME `#[computation]` called
// with two DIFFERENT source instances must produce two distinct nodes with
// independent values, never a cross-contaminated cache hit.
// =====================================================================
mod identity {
    use super::*;

    pub static RUNS: AtomicUsize = AtomicUsize::new(0);

    #[computation]
    pub async fn read_key(ctx: &Ctx, #[flow] source: &Arc<MemKvSource>, key: String) -> Result<String, CompError> {
        RUNS.fetch_add(1, Ordering::SeqCst);
        Ok(ctx.src_req(source, GetKey(key)).await?.unwrap_or_default())
    }
}

#[tokio::test]
async fn same_computation_different_source_instances_are_distinct_nodes() {
    let src_a = MemKvSource::new("identity_macro_a");
    let src_b = MemKvSource::new("identity_macro_b");
    src_a.set("k", "value-from-a").await;
    src_b.set("k", "value-from-b").await;

    let mut builder = Engine::builder();
    builder.source(src_a.clone());
    builder.source(src_b.clone());
    let root_a: Comp<String, String> = builder.define("identity_macro_root_a", {
        let src_a = src_a.clone();
        move |ctx, key: String| {
            let src_a = src_a.clone();
            async move { identity::read_key(&ctx, &src_a, key).await }
        }
    });
    let root_b: Comp<String, String> = builder.define("identity_macro_root_b", {
        let src_b = src_b.clone();
        move |ctx, key: String| {
            let src_b = src_b.clone();
            async move { identity::read_key(&ctx, &src_b, key).await }
        }
    });
    let engine = builder.build();

    let result_a = engine.eval_root(&root_a, "k".to_string()).await.unwrap();
    let result_b = engine.eval_root(&root_b, "k".to_string()).await.unwrap();

    assert_eq!(result_a, "value-from-a");
    assert_eq!(result_b, "value-from-b");
    assert_ne!(result_a, result_b, "two different source instances must never collapse onto one cached value");
    assert_eq!(identity::RUNS.load(Ordering::SeqCst), 2, "each distinct identity must run its own body exactly once");

    let result_a_again = engine.eval_root(&root_a, "k".to_string()).await.unwrap();
    assert_eq!(result_a_again, "value-from-a");
    assert_eq!(identity::RUNS.load(Ordering::SeqCst), 2, "re-evaluating an unchanged identity is a cache hit");
}

// =====================================================================
// Scenario 7: a persisted restart through the macro path — restore is a
// cache hit, zero reruns.
// =====================================================================
mod persisted_restart {
    use super::*;

    pub static RUNS: AtomicUsize = AtomicUsize::new(0);

    #[computation]
    pub async fn sync_doc(ctx: &Ctx, #[flow] source: &Arc<MemKvSource>, #[flow] sink: &Arc<VecSink>, key: String) -> Result<(), CompError> {
        RUNS.fetch_add(1, Ordering::SeqCst);
        let value = ctx.src_req(source, GetKey(key.clone())).await?.unwrap_or_default();
        ctx.sink_req(sink, WriteDoc { name: key, content: value }).await?;
        Ok(())
    }
}

fn build_persisted_engine(kv: Arc<MemKvSource>, sink: Arc<VecSink>, db_path: PathBuf, fingerprint: Fingerprint) -> (Engine, Comp<String, ()>) {
    let mut registry = Registry::default();
    registry.register_source(kv.clone());
    registry.register_sink(sink.clone());

    let mut builder = Engine::builder();
    builder.registry(registry);
    builder.persistence(PersistOptions::new(db_path, fingerprint));
    let root: Comp<String, ()> = builder.define("persisted_restart_root", move |ctx, key: String| {
        let kv = kv.clone();
        let sink = sink.clone();
        async move { persisted_restart::sync_doc(&ctx, &kv, &sink, key).await }
    });
    let engine = builder.build();
    (engine, root)
}

#[tokio::test]
async fn macro_computation_survives_a_persisted_restart_as_a_cache_hit() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("graph.redb");
    let fingerprint = Fingerprint::custom("computation-macro-persist-test");

    let kv = MemKvSource::new("persisted_restart_kv");
    let sink = VecSink::new("persisted_restart_docs");
    kv.set("a", "hello").await;

    // First engine: runs the computation for real, then flushes and closes.
    {
        let (engine, root) = build_persisted_engine(kv.clone(), sink.clone(), db_path.clone(), fingerprint);
        let handle = {
            let engine = engine.clone();
            tokio::spawn(async move { engine.run(root, "a".to_string()).await })
        };
        wait_until(|| sink.get("a").as_deref() == Some("hello")).await;
        assert_eq!(persisted_restart::RUNS.load(Ordering::SeqCst), 1);

        engine.persist_now().await;
        engine.persist_close();
        handle.abort();
    }

    // Second engine, same db path/fingerprint, same source/sink `Arc`s --
    // the initial evaluation must be a pure cache hit, zero reruns.
    {
        let (engine, root) = build_persisted_engine(kv.clone(), sink.clone(), db_path.clone(), fingerprint);
        let handle = {
            let engine = engine.clone();
            tokio::spawn(async move { engine.run(root, "a".to_string()).await })
        };

        tokio::time::sleep(Duration::from_millis(300)).await;

        assert_eq!(
            persisted_restart::RUNS.load(Ordering::SeqCst),
            1,
            "restoring from a persisted, unchanged graph must be a cache hit -- zero reruns"
        );
        assert_eq!(sink.get("a"), Some("hello".to_string()));
        assert!(!handle.is_finished(), "engine2's driver task must still be running");

        handle.abort();
    }
}
