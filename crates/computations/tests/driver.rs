//! Integration tests for the driver: initial evaluation, startup GC,
//! push-based change propagation with early cutoff, liveness GC, and
//! failure resilience. Run with `cargo test -p computations --features
//! testutil`.
//!
//! `Engine::run` never returns on the happy path, so every test spawns it
//! via `tokio::spawn`, polls assertions through `wait_until` (bounded by a
//! 5s timeout so a propagation bug can never hang `cargo test`), and aborts
//! the task before returning.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use computations::error::CompError;
use computations::testutil::{GetKey, MemKvSource, VecSink, WriteDoc};
use computations::{Comp, Engine, Registry, Sink};

/// Polls `f` every 10ms until it returns `true`, panicking if 5s pass first.
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

/// Two independent chains, each reading its own key and writing its own
/// doc. Changing one key's value must only recompute (and rewrite) its own
/// chain, leaving the other chain's run count and output untouched.
#[tokio::test]
async fn minimal_recomputation_two_independent_chains() {
    let kv = MemKvSource::new("kv");
    let sink = VecSink::new("docs");

    let mut registry = Registry::default();
    registry.register_source(kv.clone());
    registry.register_sink(sink.clone());

    let counter1 = Arc::new(AtomicUsize::new(0));
    let counter2 = Arc::new(AtomicUsize::new(0));

    let mut builder = Engine::builder();
    builder.registry(registry);

    let chain1: Comp<(), ()> = builder.define("chain1", {
        let kv = kv.clone();
        let sink = sink.clone();
        let counter1 = counter1.clone();
        move |ctx, _: ()| {
            let kv = kv.clone();
            let sink = sink.clone();
            let counter1 = counter1.clone();
            async move {
                counter1.fetch_add(1, Ordering::SeqCst);
                let val = ctx.src_req(&kv, GetKey("a".to_string())).await?.unwrap_or_default();
                ctx.sink_req(
                    &sink,
                    WriteDoc {
                        name: "doc_a".to_string(),
                        content: val,
                    },
                )
                .await?;
                Ok(())
            }
        }
    });

    let chain2: Comp<(), ()> = builder.define("chain2", {
        let kv = kv.clone();
        let sink = sink.clone();
        let counter2 = counter2.clone();
        move |ctx, _: ()| {
            let kv = kv.clone();
            let sink = sink.clone();
            let counter2 = counter2.clone();
            async move {
                counter2.fetch_add(1, Ordering::SeqCst);
                let val = ctx.src_req(&kv, GetKey("b".to_string())).await?.unwrap_or_default();
                ctx.sink_req(
                    &sink,
                    WriteDoc {
                        name: "doc_b".to_string(),
                        content: val,
                    },
                )
                .await?;
                Ok(())
            }
        }
    });

    let root: Comp<(), ()> = builder.define("two_chains_root", {
        let chain1 = chain1.clone();
        let chain2 = chain2.clone();
        move |ctx, _: ()| {
            let chain1 = chain1.clone();
            let chain2 = chain2.clone();
            async move {
                ctx.eval(&chain1, ()).await?;
                ctx.eval(&chain2, ()).await?;
                Ok(())
            }
        }
    });

    let engine = builder.build();
    let handle = {
        let engine = engine.clone();
        let root = root.clone();
        tokio::spawn(async move { engine.run(&root, ()).await })
    };

    wait_until(|| counter1.load(Ordering::SeqCst) >= 1 && counter2.load(Ordering::SeqCst) >= 1).await;
    assert_eq!(sink.get("doc_a"), Some(String::new()));
    assert_eq!(sink.get("doc_b"), Some(String::new()));

    let c1_before = counter1.load(Ordering::SeqCst);
    let c2_before = counter2.load(Ordering::SeqCst);

    kv.set("a", "hello").await;
    wait_until(|| sink.get("doc_a").as_deref() == Some("hello")).await;

    assert!(counter1.load(Ordering::SeqCst) > c1_before, "chain1 should have rerun");
    assert_eq!(
        counter2.load(Ordering::SeqCst),
        c2_before,
        "chain2 must not rerun on a change to a key it never reads"
    );

    handle.abort();
}

/// The child reads a key but always returns the same constant, so its
/// result hash never changes. Changing the key must rerun the child (its
/// counter bumps) but must NOT propagate to the parent (early cutoff).
#[tokio::test]
async fn early_cutoff_stops_propagation_on_unchanged_hash() {
    let kv = MemKvSource::new("kv");
    let sink = VecSink::new("docs");

    let mut registry = Registry::default();
    registry.register_source(kv.clone());
    registry.register_sink(sink.clone());

    let child_runs = Arc::new(AtomicUsize::new(0));
    let parent_runs = Arc::new(AtomicUsize::new(0));

    let mut builder = Engine::builder();
    builder.registry(registry);

    let child: Comp<(), i32> = builder.define("cutoff_child", {
        let kv = kv.clone();
        let runs = child_runs.clone();
        move |ctx, _: ()| {
            let kv = kv.clone();
            let runs = runs.clone();
            async move {
                runs.fetch_add(1, Ordering::SeqCst);
                let _ = ctx.src_req(&kv, GetKey("k".to_string())).await?;
                Ok(42) // constant regardless of the key's value
            }
        }
    });

    let parent: Comp<(), ()> = builder.define("cutoff_parent", {
        let sink = sink.clone();
        let child = child.clone();
        let runs = parent_runs.clone();
        move |ctx, _: ()| {
            let sink = sink.clone();
            let child = child.clone();
            let runs = runs.clone();
            async move {
                runs.fetch_add(1, Ordering::SeqCst);
                let v = ctx.eval(&child, ()).await?;
                ctx.sink_req(
                    &sink,
                    WriteDoc {
                        name: "doc".to_string(),
                        content: v.to_string(),
                    },
                )
                .await?;
                Ok(())
            }
        }
    });

    let engine = builder.build();
    let handle = {
        let engine = engine.clone();
        let parent = parent.clone();
        tokio::spawn(async move { engine.run(&parent, ()).await })
    };

    wait_until(|| sink.get("doc") == Some("42".to_string())).await;
    let child_before = child_runs.load(Ordering::SeqCst);
    let parent_before = parent_runs.load(Ordering::SeqCst);

    kv.set("k", "new-value").await;
    wait_until(|| child_runs.load(Ordering::SeqCst) > child_before).await;
    // Give any (incorrect) further propagation a moment to happen before
    // asserting it didn't.
    tokio::time::sleep(Duration::from_millis(150)).await;

    assert_eq!(
        parent_runs.load(Ordering::SeqCst),
        parent_before,
        "parent must not rerun: the child's result hash didn't change"
    );

    handle.abort();
}

/// The root reads a comma-separated `file_list` key and evaluates one child
/// per name, each writing its own doc. Removing a name from the list must,
/// after the round settles, delete that name's doc via liveness GC while
/// leaving the surviving docs alone.
#[tokio::test]
async fn deletion_gc_removes_output_for_removed_name() {
    let kv = MemKvSource::new("kv");
    let sink = VecSink::new("docs");
    kv.set("file_list", "x,y").await;

    let mut registry = Registry::default();
    registry.register_source(kv.clone());
    registry.register_sink(sink.clone());

    let mut builder = Engine::builder();
    builder.registry(registry);

    let child: Comp<String, ()> = builder.define("write_named_doc", {
        let sink = sink.clone();
        move |ctx, name: String| {
            let sink = sink.clone();
            async move {
                ctx.sink_req(
                    &sink,
                    WriteDoc {
                        name: format!("out/{name}"),
                        content: "x".to_string(),
                    },
                )
                .await?;
                Ok(())
            }
        }
    });

    let root: Comp<(), ()> = builder.define("file_list_root", {
        let kv = kv.clone();
        let child = child.clone();
        move |ctx, _: ()| {
            let kv = kv.clone();
            let child = child.clone();
            async move {
                let list = ctx
                    .src_req(&kv, GetKey("file_list".to_string()))
                    .await?
                    .unwrap_or_default();
                let names: Vec<String> = list.split(',').filter(|s| !s.is_empty()).map(str::to_string).collect();
                ctx.eval_all(&child, names).await?;
                Ok(())
            }
        }
    });

    let engine = builder.build();
    let handle = {
        let engine = engine.clone();
        let root = root.clone();
        tokio::spawn(async move { engine.run(&root, ()).await })
    };

    wait_until(|| sink.names().len() == 2).await;
    assert!(sink.get("out/x").is_some());
    assert!(sink.get("out/y").is_some());

    kv.set("file_list", "x").await;
    wait_until(|| sink.names().len() == 1).await;

    assert!(sink.get("out/x").is_some(), "surviving name's doc should remain");
    assert!(sink.get("out/y").is_none(), "removed name's doc should be collected");

    handle.abort();
}

/// A doc pre-existing in the sink before the driver ever runs, which no
/// computation produces, must be deleted by the startup GC pass.
#[tokio::test]
async fn startup_gc_removes_stale_pre_existing_output() {
    let sink = VecSink::new("docs");
    let _ = sink
        .execute(WriteDoc {
            name: "stale".to_string(),
            content: "old".to_string(),
        })
        .await;

    let mut registry = Registry::default();
    registry.register_sink(sink.clone());

    let mut builder = Engine::builder();
    builder.registry(registry);

    let root: Comp<(), ()> = builder.define("startup_gc_root", {
        let sink = sink.clone();
        move |ctx, _: ()| {
            let sink = sink.clone();
            async move {
                ctx.sink_req(
                    &sink,
                    WriteDoc {
                        name: "kept".to_string(),
                        content: "new".to_string(),
                    },
                )
                .await?;
                Ok(())
            }
        }
    });

    let engine = builder.build();
    let handle = {
        let engine = engine.clone();
        let root = root.clone();
        tokio::spawn(async move { engine.run(&root, ()).await })
    };

    wait_until(|| sink.get("stale").is_none() && sink.get("kept").is_some()).await;

    handle.abort();
}

/// A body that errors on a "poisoned" value must not crash the driver: the
/// node stays dirty, the doc keeps its last good value, and the next good
/// value recovers it.
#[tokio::test]
async fn failure_resilience_recovers_after_value_is_fixed() {
    let kv = MemKvSource::new("kv");
    let sink = VecSink::new("docs");
    kv.set("flag", "ok1").await;

    let mut registry = Registry::default();
    registry.register_source(kv.clone());
    registry.register_sink(sink.clone());

    let mut builder = Engine::builder();
    builder.registry(registry);

    let root: Comp<(), ()> = builder.define("resilience_root", {
        let kv = kv.clone();
        let sink = sink.clone();
        move |ctx, _: ()| {
            let kv = kv.clone();
            let sink = sink.clone();
            async move {
                let val = ctx.src_req(&kv, GetKey("flag".to_string())).await?.unwrap_or_default();
                if val == "poison" {
                    return Err(CompError::Failed("poisoned value".to_string()));
                }
                ctx.sink_req(
                    &sink,
                    WriteDoc {
                        name: "doc".to_string(),
                        content: val,
                    },
                )
                .await?;
                Ok(())
            }
        }
    });

    let engine = builder.build();
    let handle = {
        let engine = engine.clone();
        let root = root.clone();
        tokio::spawn(async move { engine.run(&root, ()).await })
    };

    wait_until(|| sink.get("doc") == Some("ok1".to_string())).await;

    kv.set("flag", "poison").await;
    // Give the driver time to observe and fail on the bad value.
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(!handle.is_finished(), "driver task must survive a propagation error");
    assert_eq!(
        sink.get("doc"),
        Some("ok1".to_string()),
        "doc keeps its last good value while the node is dirty"
    );

    kv.set("flag", "ok2").await;
    wait_until(|| sink.get("doc") == Some("ok2".to_string())).await;

    handle.abort();
}
