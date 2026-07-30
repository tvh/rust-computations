//! Integration tests for flow-argument computations (Stage 9 — see
//! `docs/persistence-benchmark-notes.md`'s design rationale in full). Run
//! with `cargo test -p computations --features testutil`.
//!
//! Everything below is written out by hand exactly as a future
//! `#[computation]` proc-macro would generate it for
//!
//! ```ignore
//! #[computation]
//! async fn sync_doc(ctx: &Ctx, #[flow] source: &Arc<MemKvSource>, #[flow] sink: &Arc<VecSink>, key: String)
//!     -> Result<(), CompError> { ... }
//! ```
//!
//! i.e. for each hand-written computation below: a name constant
//! (`concat!(module_path!(), "::<fn name>")`), the real `impl` body, a
//! public wrapper with the user's original signature that calls
//! `Ctx::eval_flows` (no builder registration, no captured handles), and a
//! `FlowThunk` registered via `EngineBuilder::define_flows`.
//!
//! Each scenario gets its own module so its name constant and run-counter
//! `static` are unique to that scenario — `cargo test` runs test functions
//! concurrently within one process, and a `FlowThunk` is a plain `fn` with
//! no per-call captures, so counting invocations needs a `static`, not an
//! `Arc<AtomicUsize>` closure capture (contrast the builder-path pattern in
//! `tests/driver.rs`). Distinct modules (and therefore distinct
//! `module_path!()`-derived names) keep every scenario's counter and node
//! identity from ever colliding with another's.

use std::any::Any;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use computations::error::CompError;
use computations::testutil::{GetKey, MemKvSource, VecSink, WriteDoc};
use computations::{Comp, Ctx, Engine, Fingerprint, FlowId, FlowResolver, PersistOptions, Registry, SinkBase, SourceBase};
use futures::future::BoxFuture;

/// Polls `f` every 10ms until it returns `true`, panicking if 5s pass first
/// — mirrors `tests/driver.rs`'s/`tests/persist.rs`'s own helper.
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
// Scenario 1: evaluates correctly, and memoizes.
// =====================================================================
mod doc_sync {
    use super::*;

    pub const NAME: &str = concat!(module_path!(), "::sync_doc");
    pub static RUNS: AtomicUsize = AtomicUsize::new(0);

    /// The real body, exactly as a user would write it: no builder, no
    /// registration, just a plain async fn over a `Ctx` and two flow
    /// handles.
    async fn body(ctx: &Ctx, source: &Arc<MemKvSource>, sink: &Arc<VecSink>, key: String) -> Result<(), CompError> {
        RUNS.fetch_add(1, Ordering::SeqCst);
        let value = ctx.src_req(source, GetKey(key.clone())).await?.unwrap_or_default();
        ctx.sink_req(
            sink,
            WriteDoc {
                name: key,
                content: value,
            },
        )
        .await?;
        Ok(())
    }

    /// The generated public wrapper: callable as `sync_doc(ctx, &source,
    /// &sink, key).await`, with no builder registration or capture visible
    /// at the call site.
    pub async fn sync_doc(ctx: &Ctx, source: &Arc<MemKvSource>, sink: &Arc<VecSink>, key: String) -> Result<(), CompError> {
        let flows = [FlowId::Source(source.instance_id()), FlowId::Sink(sink.instance_id())];
        ctx.eval_flows(NAME, &flows, key).await
    }

    /// The generated `FlowThunk`: decodes `param_bytes`, resolves both
    /// flows by the `#[flow]` argument's position (0 = source, 1 = sink),
    /// and calls the real body.
    pub fn thunk(
        ctx: Ctx,
        resolver: FlowResolver<'_>,
        param_bytes: &[u8],
    ) -> BoxFuture<'static, Result<Arc<dyn Any + Send + Sync>, CompError>> {
        let key: String = match postcard::from_bytes(param_bytes) {
            Ok(k) => k,
            Err(e) => return Box::pin(async move { Err(CompError::Failed(format!("sync_doc: param decode failed: {e}"))) }),
        };
        let source = match resolver.source::<MemKvSource>(0) {
            Ok(s) => s,
            Err(e) => return Box::pin(async move { Err(e) }),
        };
        let sink = match resolver.sink::<VecSink>(1) {
            Ok(s) => s,
            Err(e) => return Box::pin(async move { Err(e) }),
        };
        Box::pin(async move {
            body(&ctx, &source, &sink, key).await?;
            Ok(Arc::new(()) as Arc<dyn Any + Send + Sync>)
        })
    }
}

#[tokio::test]
async fn evaluates_and_memoizes() {
    let kv = MemKvSource::new("doc_sync_kv");
    let sink = VecSink::new("doc_sync_docs");
    kv.set("a", "hello").await;

    let mut registry = Registry::default();
    registry.register_source(kv.clone());
    registry.register_sink(sink.clone());

    let mut builder = Engine::builder();
    builder.registry(registry);
    builder.define_flows::<()>(doc_sync::NAME, doc_sync::thunk);
    // A plain builder-path root computation that calls the flow-argument
    // computation's generated *wrapper* (`doc_sync::sync_doc`) as a nested
    // call -- exercising the wrapper itself (it needs a `&Ctx`, which only
    // exists inside a running computation, never at a bare root call) and
    // demonstrating that the two registration styles interoperate freely:
    // a builder-path computation calling a flow-argument one looks exactly
    // like calling any other async function.
    let root: Comp<String, ()> = builder.define("evaluates_and_memoizes_root", {
        let kv = kv.clone();
        let sink = sink.clone();
        move |ctx, key: String| {
            let kv = kv.clone();
            let sink = sink.clone();
            async move { doc_sync::sync_doc(&ctx, &kv, &sink, key).await }
        }
    });
    let engine = builder.build();

    engine.eval_root(&root, "a".to_string()).await.unwrap();
    assert_eq!(sink.get("a"), Some("hello".to_string()));
    assert_eq!(doc_sync::RUNS.load(Ordering::SeqCst), 1, "the body must actually run once");

    // Same name, same flows, same param -- a cache hit, not a second run.
    engine.eval_root(&root, "a".to_string()).await.unwrap();
    assert_eq!(
        doc_sync::RUNS.load(Ordering::SeqCst),
        1,
        "an unchanged re-evaluation must be a memoized cache hit, not a second run"
    );
}

// =====================================================================
// Scenario 2: re-runs on a source change, under the live driver.
// =====================================================================
mod doc_sync_rerun {
    use super::*;

    pub const NAME: &str = concat!(module_path!(), "::sync_doc");
    pub static RUNS: AtomicUsize = AtomicUsize::new(0);

    async fn body(ctx: &Ctx, source: &Arc<MemKvSource>, sink: &Arc<VecSink>, key: String) -> Result<(), CompError> {
        RUNS.fetch_add(1, Ordering::SeqCst);
        let value = ctx.src_req(source, GetKey(key.clone())).await?.unwrap_or_default();
        ctx.sink_req(
            sink,
            WriteDoc {
                name: key,
                content: value,
            },
        )
        .await?;
        Ok(())
    }

    pub fn thunk(
        ctx: Ctx,
        resolver: FlowResolver<'_>,
        param_bytes: &[u8],
    ) -> BoxFuture<'static, Result<Arc<dyn Any + Send + Sync>, CompError>> {
        let key: String = match postcard::from_bytes(param_bytes) {
            Ok(k) => k,
            Err(e) => return Box::pin(async move { Err(CompError::Failed(format!("sync_doc: param decode failed: {e}"))) }),
        };
        let source = match resolver.source::<MemKvSource>(0) {
            Ok(s) => s,
            Err(e) => return Box::pin(async move { Err(e) }),
        };
        let sink = match resolver.sink::<VecSink>(1) {
            Ok(s) => s,
            Err(e) => return Box::pin(async move { Err(e) }),
        };
        Box::pin(async move {
            body(&ctx, &source, &sink, key).await?;
            Ok(Arc::new(()) as Arc<dyn Any + Send + Sync>)
        })
    }
}

#[tokio::test]
async fn reruns_on_source_change_under_the_driver() {
    let kv = MemKvSource::new("doc_sync_rerun_kv");
    let sink = VecSink::new("doc_sync_rerun_docs");
    kv.set("a", "v0").await;

    let mut registry = Registry::default();
    registry.register_source(kv.clone());
    registry.register_sink(sink.clone());

    let mut builder = Engine::builder();
    builder.registry(registry);
    builder.define_flows::<()>(doc_sync_rerun::NAME, doc_sync_rerun::thunk);
    let engine = builder.build();

    let flows = [FlowId::Source(kv.instance_id()), FlowId::Sink(sink.instance_id())];

    let handle = {
        let engine = engine.clone();
        let flows = flows.clone();
        tokio::spawn(async move { engine.run_flows::<String, ()>(doc_sync_rerun::NAME, &flows, "a".to_string()).await })
    };

    wait_until(|| sink.get("a").as_deref() == Some("v0")).await;
    assert_eq!(doc_sync_rerun::RUNS.load(Ordering::SeqCst), 1);

    kv.set("a", "v1").await;
    wait_until(|| sink.get("a").as_deref() == Some("v1")).await;
    assert_eq!(
        doc_sync_rerun::RUNS.load(Ordering::SeqCst),
        2,
        "a genuine source change must cause exactly one rerun"
    );

    handle.abort();
}

// =====================================================================
// Scenario 3: survives a persisted restart -- restore is a cache hit,
// zero reruns.
// =====================================================================
mod doc_sync_persist {
    use super::*;

    pub const NAME: &str = concat!(module_path!(), "::sync_doc");
    pub static RUNS: AtomicUsize = AtomicUsize::new(0);

    async fn body(ctx: &Ctx, source: &Arc<MemKvSource>, sink: &Arc<VecSink>, key: String) -> Result<(), CompError> {
        RUNS.fetch_add(1, Ordering::SeqCst);
        let value = ctx.src_req(source, GetKey(key.clone())).await?.unwrap_or_default();
        ctx.sink_req(
            sink,
            WriteDoc {
                name: key,
                content: value,
            },
        )
        .await?;
        Ok(())
    }

    pub fn thunk(
        ctx: Ctx,
        resolver: FlowResolver<'_>,
        param_bytes: &[u8],
    ) -> BoxFuture<'static, Result<Arc<dyn Any + Send + Sync>, CompError>> {
        let key: String = match postcard::from_bytes(param_bytes) {
            Ok(k) => k,
            Err(e) => return Box::pin(async move { Err(CompError::Failed(format!("sync_doc: param decode failed: {e}"))) }),
        };
        let source = match resolver.source::<MemKvSource>(0) {
            Ok(s) => s,
            Err(e) => return Box::pin(async move { Err(e) }),
        };
        let sink = match resolver.sink::<VecSink>(1) {
            Ok(s) => s,
            Err(e) => return Box::pin(async move { Err(e) }),
        };
        Box::pin(async move {
            body(&ctx, &source, &sink, key).await?;
            Ok(Arc::new(()) as Arc<dyn Any + Send + Sync>)
        })
    }
}

#[tokio::test]
async fn survives_a_persisted_restart_as_a_cache_hit() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("graph.redb");
    let fingerprint = Fingerprint::custom("flow-persist-test");

    let kv = MemKvSource::new("doc_sync_persist_kv");
    let sink = VecSink::new("doc_sync_persist_docs");
    kv.set("a", "hello").await;

    let flows = [FlowId::Source(kv.instance_id()), FlowId::Sink(sink.instance_id())];

    // First engine: runs the computation for real, then flushes and closes.
    {
        let mut registry = Registry::default();
        registry.register_source(kv.clone());
        registry.register_sink(sink.clone());

        let mut builder = Engine::builder();
        builder.registry(registry);
        builder.define_flows::<()>(doc_sync_persist::NAME, doc_sync_persist::thunk);
        builder.persistence(PersistOptions::new(db_path.clone(), fingerprint));
        let engine = builder.build();

        let handle = {
            let engine = engine.clone();
            let flows = flows.clone();
            tokio::spawn(async move {
                engine.run_flows::<String, ()>(doc_sync_persist::NAME, &flows, "a".to_string()).await
            })
        };
        wait_until(|| sink.get("a").as_deref() == Some("hello")).await;
        assert_eq!(doc_sync_persist::RUNS.load(Ordering::SeqCst), 1);

        engine.persist_now().await;
        engine.persist_close();
        handle.abort();
    }

    // Second engine, same db path/fingerprint, same source/sink `Arc`s
    // (mirroring how a real restart keeps the same external world, just a
    // fresh process) -- the initial evaluation must be a pure cache hit.
    // Goes through `run_flows` (not a bare `eval_root_flows`) because
    // restoring from disk only ever happens inside `Engine::run`/`run_flows`
    // (`EngineInner::persist_load`, called once at driver startup) -- see
    // `crate::persist`'s module docs.
    {
        let mut registry = Registry::default();
        registry.register_source(kv.clone());
        registry.register_sink(sink.clone());

        let mut builder = Engine::builder();
        builder.registry(registry);
        builder.define_flows::<()>(doc_sync_persist::NAME, doc_sync_persist::thunk);
        builder.persistence(PersistOptions::new(db_path.clone(), fingerprint));
        let engine = builder.build();

        let handle = {
            let engine = engine.clone();
            let flows = flows.clone();
            tokio::spawn(async move {
                engine.run_flows::<String, ()>(doc_sync_persist::NAME, &flows, "a".to_string()).await
            })
        };

        // Give the restart's initial evaluation + startup GC a moment to run.
        tokio::time::sleep(Duration::from_millis(300)).await;

        assert_eq!(
            doc_sync_persist::RUNS.load(Ordering::SeqCst),
            1,
            "restoring from a persisted, unchanged graph must be a cache hit -- zero reruns \
             (RUNS is shared across both engine instances in this test, so still-1 IS the proof)"
        );
        assert_eq!(sink.get("a"), Some("hello".to_string()));
        assert!(!handle.is_finished(), "engine2's driver task must still be running");

        handle.abort();
    }
}

// =====================================================================
// Scenario 4: the identity test -- the SAME computation name called with
// two DIFFERENT source instances must produce two distinct nodes with
// independent values. Getting this wrong is the silent-corruption case
// flow identity exists to prevent (see `crate::flow`'s module docs).
// =====================================================================
mod doc_sync_identity {
    use super::*;

    pub const NAME: &str = concat!(module_path!(), "::sync_doc");
    pub static RUNS: AtomicUsize = AtomicUsize::new(0);

    async fn body(ctx: &Ctx, source: &Arc<MemKvSource>, key: String) -> Result<String, CompError> {
        RUNS.fetch_add(1, Ordering::SeqCst);
        let value = ctx.src_req(source, GetKey(key)).await?.unwrap_or_default();
        Ok(value)
    }

    pub async fn sync_doc(ctx: &Ctx, source: &Arc<MemKvSource>, key: String) -> Result<String, CompError> {
        let flows = [FlowId::Source(source.instance_id())];
        ctx.eval_flows(NAME, &flows, key).await
    }

    pub fn thunk(
        ctx: Ctx,
        resolver: FlowResolver<'_>,
        param_bytes: &[u8],
    ) -> BoxFuture<'static, Result<Arc<dyn Any + Send + Sync>, CompError>> {
        let key: String = match postcard::from_bytes(param_bytes) {
            Ok(k) => k,
            Err(e) => return Box::pin(async move { Err(CompError::Failed(format!("sync_doc: param decode failed: {e}"))) }),
        };
        let source = match resolver.source::<MemKvSource>(0) {
            Ok(s) => s,
            Err(e) => return Box::pin(async move { Err(e) }),
        };
        Box::pin(async move {
            let value = body(&ctx, &source, key).await?;
            Ok(Arc::new(value) as Arc<dyn Any + Send + Sync>)
        })
    }
}

#[tokio::test]
async fn same_name_different_source_instances_are_distinct_nodes() {
    let src_a = MemKvSource::new("doc_sync_identity_a");
    let src_b = MemKvSource::new("doc_sync_identity_b");
    src_a.set("k", "value-from-a").await;
    src_b.set("k", "value-from-b").await;

    let mut registry = Registry::default();
    registry.register_source(src_a.clone());
    registry.register_source(src_b.clone());

    let mut builder = Engine::builder();
    builder.registry(registry);
    builder.define_flows::<String>(doc_sync_identity::NAME, doc_sync_identity::thunk);
    // Two builder-path root computations, each closing over a DIFFERENT
    // source instance and calling the flow-argument computation's
    // generated wrapper (`doc_sync_identity::sync_doc`) as a nested call --
    // exercising the wrapper itself, exactly as `evaluates_and_memoizes`
    // does above. Both ultimately call the very same flow-argument
    // computation name; only the flow instance passed to it differs.
    let root_a: Comp<String, String> = builder.define("identity_root_a", {
        let src_a = src_a.clone();
        move |ctx, key: String| {
            let src_a = src_a.clone();
            async move { doc_sync_identity::sync_doc(&ctx, &src_a, key).await }
        }
    });
    let root_b: Comp<String, String> = builder.define("identity_root_b", {
        let src_b = src_b.clone();
        move |ctx, key: String| {
            let src_b = src_b.clone();
            async move { doc_sync_identity::sync_doc(&ctx, &src_b, key).await }
        }
    });
    let engine = builder.build();

    // Same underlying flow computation name, same param ("k"), DIFFERENT
    // source instance behind each wrapper call.
    let result_a = engine.eval_root(&root_a, "k".to_string()).await.unwrap();
    let result_b = engine.eval_root(&root_b, "k".to_string()).await.unwrap();

    assert_eq!(result_a, "value-from-a");
    assert_eq!(result_b, "value-from-b");
    assert_ne!(
        result_a, result_b,
        "two different source instances behind the same computation name and param must never collapse \
         onto one cached value -- that would be exactly the silent-corruption bug flow-id-in-identity \
         exists to prevent"
    );
    assert_eq!(
        doc_sync_identity::RUNS.load(Ordering::SeqCst),
        2,
        "each distinct (name, flows, param) identity must run its own body exactly once -- 2 runs, not 1 \
         shared cache hit"
    );

    // Re-evaluating either one again must still be a cache hit against its
    // OWN node, not a fresh run and not a collision with the other.
    let result_a_again = engine.eval_root(&root_a, "k".to_string()).await.unwrap();
    assert_eq!(result_a_again, "value-from-a");
    assert_eq!(doc_sync_identity::RUNS.load(Ordering::SeqCst), 2, "re-evaluating an unchanged identity is a cache hit");
}

// =====================================================================
// Scenario 5: mutual recursion between two hand-written flow computations
// -- now expressible without `Comp::named`, since a flow computation is
// called by name (through `Ctx::eval_flows`) rather than through a
// registration-backed handle.
// =====================================================================
mod mutual_flows {
    use super::*;

    pub const IS_EVEN: &str = concat!(module_path!(), "::is_even");
    pub const IS_ODD: &str = concat!(module_path!(), "::is_odd");
    pub static EVEN_RUNS: AtomicUsize = AtomicUsize::new(0);
    pub static ODD_RUNS: AtomicUsize = AtomicUsize::new(0);

    fn flows_of(sink: &Arc<VecSink>) -> [FlowId; 1] {
        [FlowId::Sink(sink.instance_id())]
    }

    async fn is_even_body(ctx: &Ctx, sink: &Arc<VecSink>, n: u32) -> Result<bool, CompError> {
        EVEN_RUNS.fetch_add(1, Ordering::SeqCst);
        if n == 0 {
            return Ok(true);
        }
        is_odd(ctx, sink, n - 1).await
    }

    async fn is_odd_body(ctx: &Ctx, sink: &Arc<VecSink>, n: u32) -> Result<bool, CompError> {
        ODD_RUNS.fetch_add(1, Ordering::SeqCst);
        if n == 0 {
            return Ok(false);
        }
        is_even(ctx, sink, n - 1).await
    }

    pub async fn is_even(ctx: &Ctx, sink: &Arc<VecSink>, n: u32) -> Result<bool, CompError> {
        ctx.eval_flows(IS_EVEN, &flows_of(sink), n).await
    }

    pub async fn is_odd(ctx: &Ctx, sink: &Arc<VecSink>, n: u32) -> Result<bool, CompError> {
        ctx.eval_flows(IS_ODD, &flows_of(sink), n).await
    }

    pub fn is_even_thunk(
        ctx: Ctx,
        resolver: FlowResolver<'_>,
        param_bytes: &[u8],
    ) -> BoxFuture<'static, Result<Arc<dyn Any + Send + Sync>, CompError>> {
        let n: u32 = match postcard::from_bytes(param_bytes) {
            Ok(n) => n,
            Err(e) => return Box::pin(async move { Err(CompError::Failed(format!("is_even: {e}"))) }),
        };
        let sink = match resolver.sink::<VecSink>(0) {
            Ok(s) => s,
            Err(e) => return Box::pin(async move { Err(e) }),
        };
        Box::pin(async move {
            let result = is_even_body(&ctx, &sink, n).await?;
            Ok(Arc::new(result) as Arc<dyn Any + Send + Sync>)
        })
    }

    pub fn is_odd_thunk(
        ctx: Ctx,
        resolver: FlowResolver<'_>,
        param_bytes: &[u8],
    ) -> BoxFuture<'static, Result<Arc<dyn Any + Send + Sync>, CompError>> {
        let n: u32 = match postcard::from_bytes(param_bytes) {
            Ok(n) => n,
            Err(e) => return Box::pin(async move { Err(CompError::Failed(format!("is_odd: {e}"))) }),
        };
        let sink = match resolver.sink::<VecSink>(0) {
            Ok(s) => s,
            Err(e) => return Box::pin(async move { Err(e) }),
        };
        Box::pin(async move {
            let result = is_odd_body(&ctx, &sink, n).await?;
            Ok(Arc::new(result) as Arc<dyn Any + Send + Sync>)
        })
    }
}

#[tokio::test]
async fn mutual_recursion_between_two_flow_computations() {
    let sink = VecSink::new("mutual_flows_docs");

    let mut registry = Registry::default();
    registry.register_sink(sink.clone());

    let mut builder = Engine::builder();
    builder.registry(registry);
    builder.define_flows::<bool>(mutual_flows::IS_EVEN, mutual_flows::is_even_thunk);
    builder.define_flows::<bool>(mutual_flows::IS_ODD, mutual_flows::is_odd_thunk);
    let engine = builder.build();

    let flows = [FlowId::Sink(sink.instance_id())];

    let ten_is_even = engine
        .eval_root_flows::<u32, bool>(mutual_flows::IS_EVEN, &flows, 10)
        .await
        .unwrap();
    assert!(ten_is_even);

    let seven_is_even = engine
        .eval_root_flows::<u32, bool>(mutual_flows::IS_EVEN, &flows, 7)
        .await
        .unwrap();
    assert!(!seven_is_even);

    assert!(
        mutual_flows::EVEN_RUNS.load(Ordering::SeqCst) > 0 && mutual_flows::ODD_RUNS.load(Ordering::SeqCst) > 0,
        "both mutually-recursive computations must actually have run"
    );
}
