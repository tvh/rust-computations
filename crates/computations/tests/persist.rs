//! Integration tests for opt-in persistence (`crate::persist`): saving the
//! dependency graph to a redb file and restoring it across a simulated
//! restart. Run with `cargo test -p computations --features testutil`.
//!
//! A "restart" is simulated by building a *second*, independent `Engine`
//! against the same persisted file, sharing the same `MemKvSource`/
//! `VecSink` `Arc`s across both engine instances (mirroring how a real
//! restart keeps the same external world, just a fresh process) — see
//! `tests/driver.rs`'s module docs for the same `wait_until`/abort pattern
//! this file reuses.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use computations::testutil::{GetKey, MemKvSource, VecSink, WriteDoc};
use computations::{Comp, Engine, Fingerprint, PersistOptions, Registry};

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

fn persist_opts(path: &std::path::Path, fingerprint: &str) -> PersistOptions {
    PersistOptions {
        path: path.to_path_buf(),
        fingerprint: Fingerprint::custom(fingerprint),
    }
}

/// Round trip: a settled two-chain graph, persisted, then restored in a
/// fresh engine (same defs, same source/sink `Arc`s, same path and
/// fingerprint) must not re-run either chain — the initial evaluation is a
/// pure cache hit — while the sink's outputs remain intact (startup GC must
/// see the restored `outputs` as live and not delete them).
#[tokio::test]
async fn round_trip_unchanged_chains_do_not_rerun() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("graph.redb");

    let kv = MemKvSource::new("kv");
    let sink = VecSink::new("docs");
    kv.set("a", "1").await;
    kv.set("b", "2").await;

    fn build(
        kv: &Arc<MemKvSource>,
        sink: &Arc<VecSink>,
        db_path: &std::path::Path,
        counter1: &Arc<AtomicUsize>,
        counter2: &Arc<AtomicUsize>,
    ) -> (Engine, Comp<(), ()>) {
        let mut registry = Registry::default();
        registry.register_source(kv.clone());
        registry.register_sink(sink.clone());

        let mut builder = Engine::builder();
        builder.registry(registry);
        builder.persistence(persist_opts(db_path, "fp-v1"));

        let chain1: Comp<(), ()> = builder.define("rt_chain1", {
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

        let chain2: Comp<(), ()> = builder.define("rt_chain2", {
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

        let root: Comp<(), ()> = builder.define("rt_root", move |ctx, _: ()| async move {
            ctx.eval(chain1, ()).await?;
            ctx.eval(chain2, ()).await?;
            Ok(())
        });

        (builder.build(), root)
    }

    let counter1_e1 = Arc::new(AtomicUsize::new(0));
    let counter2_e1 = Arc::new(AtomicUsize::new(0));
    let (engine1, root1) = build(&kv, &sink, &db_path, &counter1_e1, &counter2_e1);
    let handle1 = {
        let engine1 = engine1.clone();
        tokio::spawn(async move { engine1.run(root1, ()).await })
    };

    wait_until(|| sink.get("doc_a") == Some("1".to_string()) && sink.get("doc_b") == Some("2".to_string())).await;
    engine1.persist_now().await;
    engine1.persist_close();
    handle1.abort();
    let _ = handle1.await;
    drop(engine1);

    let counter1_e2 = Arc::new(AtomicUsize::new(0));
    let counter2_e2 = Arc::new(AtomicUsize::new(0));
    let (engine2, root2) = build(&kv, &sink, &db_path, &counter1_e2, &counter2_e2);
    let handle2 = {
        let engine2 = engine2.clone();
        tokio::spawn(async move { engine2.run(root2, ()).await })
    };

    // Give the restart's initial evaluation + startup GC a moment to run.
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert_eq!(counter1_e2.load(Ordering::SeqCst), 0, "chain1 must be a pure cache hit after restart");
    assert_eq!(counter2_e2.load(Ordering::SeqCst), 0, "chain2 must be a pure cache hit after restart");
    assert_eq!(sink.get("doc_a"), Some("1".to_string()), "restored output must survive startup GC");
    assert_eq!(sink.get("doc_b"), Some("2".to_string()), "restored output must survive startup GC");
    assert!(!handle2.is_finished(), "engine2's driver task must still be running");

    handle2.abort();
}

/// An input key changed while the "process" was down: after restart, only
/// the chain that reads that key must re-run; the untouched chain stays a
/// cache hit.
#[tokio::test]
async fn changed_input_across_restart_reruns_only_dependent_chain() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("graph.redb");

    let kv = MemKvSource::new("kv");
    let sink = VecSink::new("docs");
    kv.set("a", "before").await;
    kv.set("b", "stable").await;

    fn build(
        kv: &Arc<MemKvSource>,
        sink: &Arc<VecSink>,
        db_path: &std::path::Path,
        counter1: &Arc<AtomicUsize>,
        counter2: &Arc<AtomicUsize>,
    ) -> (Engine, Comp<(), ()>) {
        let mut registry = Registry::default();
        registry.register_source(kv.clone());
        registry.register_sink(sink.clone());

        let mut builder = Engine::builder();
        builder.registry(registry);
        builder.persistence(persist_opts(db_path, "fp-v1"));

        let chain1: Comp<(), ()> = builder.define("ci_chain1", {
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

        let chain2: Comp<(), ()> = builder.define("ci_chain2", {
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

        let root: Comp<(), ()> = builder.define("ci_root", move |ctx, _: ()| async move {
            ctx.eval(chain1, ()).await?;
            ctx.eval(chain2, ()).await?;
            Ok(())
        });

        (builder.build(), root)
    }

    let counter1_e1 = Arc::new(AtomicUsize::new(0));
    let counter2_e1 = Arc::new(AtomicUsize::new(0));
    let (engine1, root1) = build(&kv, &sink, &db_path, &counter1_e1, &counter2_e1);
    let handle1 = {
        let engine1 = engine1.clone();
        tokio::spawn(async move { engine1.run(root1, ()).await })
    };
    wait_until(|| sink.get("doc_a") == Some("before".to_string()) && sink.get("doc_b") == Some("stable".to_string()))
        .await;
    engine1.persist_now().await;
    engine1.persist_close();
    handle1.abort();
    let _ = handle1.await;
    drop(engine1);

    // The "process" is down: the input changes.
    kv.set("a", "after").await;

    let counter1_e2 = Arc::new(AtomicUsize::new(0));
    let counter2_e2 = Arc::new(AtomicUsize::new(0));
    let (engine2, root2) = build(&kv, &sink, &db_path, &counter1_e2, &counter2_e2);
    let handle2 = {
        let engine2 = engine2.clone();
        tokio::spawn(async move { engine2.run(root2, ()).await })
    };

    wait_until(|| sink.get("doc_a") == Some("after".to_string())).await;
    // Give any (incorrect) further work a moment to happen before asserting
    // chain2 never touched anything.
    tokio::time::sleep(Duration::from_millis(150)).await;

    assert!(counter1_e2.load(Ordering::SeqCst) >= 1, "chain1 must re-run: its input changed");
    assert_eq!(counter2_e2.load(Ordering::SeqCst), 0, "chain2 must stay a cache hit: its input never changed");
    assert_eq!(sink.get("doc_b"), Some("stable".to_string()), "chain2's output must be untouched");

    handle2.abort();
}

/// A shared, ordered event log — see `tests/driver.rs`'s identical helper —
/// so `changed_fingerprint_and_input_reruns_everything_in_order` can assert
/// *relative* completion order between two concurrently-evaluated chains
/// without depending on wall-clock timing.
#[derive(Clone, Default)]
struct SeqLog {
    next: Arc<AtomicUsize>,
    events: Arc<std::sync::Mutex<Vec<(&'static str, usize)>>>,
}

impl SeqLog {
    fn record(&self, label: &'static str) {
        let seq = self.next.fetch_add(1, Ordering::SeqCst);
        self.events.lock().unwrap().push((label, seq));
    }

    fn last_seq(&self, label: &str) -> Option<usize> {
        self.events.lock().unwrap().iter().filter(|(l, _)| *l == label).map(|(_, seq)| *seq).max()
    }
}

/// A different fingerprint (simulating a binary change) plus a genuinely
/// changed input: everything eventually re-runs with correct outputs. The
/// fast, input-driven chain's completion must precede the deliberately slow,
/// fingerprint-only-revalidated chain's completion (the same `SeqLog`
/// pattern `tests/driver.rs`'s `input_priority_preempts_in_progress_revalidate_sweep`
/// uses, here evaluated concurrently within the restart's initial
/// evaluation via `futures::future::join`).
#[tokio::test]
async fn changed_fingerprint_and_input_reruns_everything_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("graph.redb");

    let kv = MemKvSource::new("kv");
    let sink = VecSink::new("docs");
    kv.set("x", "before").await;

    let log1 = SeqLog::default();

    let mut registry1 = Registry::default();
    registry1.register_source(kv.clone());
    registry1.register_sink(sink.clone());
    let mut builder1 = Engine::builder();
    builder1.registry(registry1);
    builder1.persistence(persist_opts(&db_path, "fp-A"));

    let input_runs1 = Arc::new(AtomicUsize::new(0));
    let slow_runs1 = Arc::new(AtomicUsize::new(0));

    let input1: Comp<(), ()> = builder1.define("fp_input", {
        let kv = kv.clone();
        let sink = sink.clone();
        let runs = input_runs1.clone();
        let log = log1.clone();
        move |ctx, _: ()| {
            let kv = kv.clone();
            let sink = sink.clone();
            let runs = runs.clone();
            let log = log.clone();
            async move {
                runs.fetch_add(1, Ordering::SeqCst);
                let val = ctx.src_req(&kv, GetKey("x".to_string())).await?.unwrap_or_default();
                ctx.sink_req(
                    &sink,
                    WriteDoc {
                        name: "doc_input".to_string(),
                        content: val,
                    },
                )
                .await?;
                log.record("input");
                Ok(())
            }
        }
    });

    let slow1: Comp<(), usize> = builder1.define("fp_slow", {
        let runs = slow_runs1.clone();
        let log = log1.clone();
        move |_ctx, _: ()| {
            let runs = runs.clone();
            let log = log.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(150)).await;
                let n = runs.fetch_add(1, Ordering::SeqCst) + 1;
                log.record("revalidate");
                Ok(n)
            }
        }
    });

    let root1: Comp<(), ()> = builder1.define("fp_root", move |ctx, _: ()| async move {
        let ctx2 = ctx.clone();
        let (r1, r2) = futures::future::join(ctx.eval(input1, ()), ctx2.eval(slow1, ())).await;
        r1?;
        r2?;
        Ok(())
    });

    let engine1 = builder1.build();
    let handle1 = {
        let engine1 = engine1.clone();
        tokio::spawn(async move { engine1.run(root1, ()).await })
    };
    wait_until(|| sink.get("doc_input") == Some("before".to_string())).await;
    engine1.persist_now().await;
    engine1.persist_close();
    handle1.abort();
    let _ = handle1.await;
    drop(engine1);

    // The "process" is down: the input changes.
    kv.set("x", "after").await;

    let log2 = SeqLog::default();
    let mut registry2 = Registry::default();
    registry2.register_source(kv.clone());
    registry2.register_sink(sink.clone());
    let mut builder2 = Engine::builder();
    builder2.registry(registry2);
    // A different fingerprint: simulates the binary having changed.
    builder2.persistence(persist_opts(&db_path, "fp-B"));

    let input_runs2 = Arc::new(AtomicUsize::new(0));
    let slow_runs2 = Arc::new(AtomicUsize::new(0));

    let input2: Comp<(), ()> = builder2.define("fp_input", {
        let kv = kv.clone();
        let sink = sink.clone();
        let runs = input_runs2.clone();
        let log = log2.clone();
        move |ctx, _: ()| {
            let kv = kv.clone();
            let sink = sink.clone();
            let runs = runs.clone();
            let log = log.clone();
            async move {
                runs.fetch_add(1, Ordering::SeqCst);
                let val = ctx.src_req(&kv, GetKey("x".to_string())).await?.unwrap_or_default();
                ctx.sink_req(
                    &sink,
                    WriteDoc {
                        name: "doc_input".to_string(),
                        content: val,
                    },
                )
                .await?;
                log.record("input");
                Ok(())
            }
        }
    });

    let slow2: Comp<(), usize> = builder2.define("fp_slow", {
        let runs = slow_runs2.clone();
        let log = log2.clone();
        move |_ctx, _: ()| {
            let runs = runs.clone();
            let log = log.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(150)).await;
                let n = runs.fetch_add(1, Ordering::SeqCst) + 1;
                log.record("revalidate");
                Ok(n)
            }
        }
    });

    let root2: Comp<(), ()> = builder2.define("fp_root", move |ctx, _: ()| async move {
        let ctx2 = ctx.clone();
        let (r1, r2) = futures::future::join(ctx.eval(input2, ()), ctx2.eval(slow2, ())).await;
        r1?;
        r2?;
        Ok(())
    });

    let engine2 = builder2.build();
    let handle2 = {
        let engine2 = engine2.clone();
        tokio::spawn(async move { engine2.run(root2, ()).await })
    };

    wait_until(|| sink.get("doc_input") == Some("after".to_string())).await;
    wait_until(|| slow_runs2.load(Ordering::SeqCst) >= 1).await;

    assert!(input_runs2.load(Ordering::SeqCst) >= 1, "the input chain must re-run");
    assert!(slow_runs2.load(Ordering::SeqCst) >= 1, "the fingerprint-mismatched chain must re-run too");

    let input_seq = log2.last_seq("input").expect("input chain recorded a completion");
    let revalidate_seq = log2.last_seq("revalidate").expect("revalidate chain recorded a completion");
    assert!(
        input_seq < revalidate_seq,
        "the fast input-driven rerun (seq {input_seq}) should complete before the slow \
         fingerprint-revalidated rerun (seq {revalidate_seq})"
    );

    handle2.abort();
}

/// A record whose definition is no longer registered in this process (the
/// def was removed from the binary) must simply be dropped at load — the
/// rest of the graph loads and runs fine, with no error.
#[tokio::test]
async fn unknown_def_on_restart_is_dropped_without_error() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("graph.redb");

    let kv = MemKvSource::new("kv");
    let sink = VecSink::new("docs");
    kv.set("a", "1").await;
    kv.set("b", "2").await;

    // Engine 1: root calls both chain_a and (a soon-to-be-removed) chain_b.
    let mut registry1 = Registry::default();
    registry1.register_source(kv.clone());
    registry1.register_sink(sink.clone());
    let mut builder1 = Engine::builder();
    builder1.registry(registry1);
    builder1.persistence(persist_opts(&db_path, "fp-A"));

    let chain_a1: Comp<(), ()> = builder1.define("ud_chain_a", {
        let kv = kv.clone();
        let sink = sink.clone();
        move |ctx, _: ()| {
            let kv = kv.clone();
            let sink = sink.clone();
            async move {
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
    let chain_b1: Comp<(), ()> = builder1.define("ud_chain_b", {
        let kv = kv.clone();
        let sink = sink.clone();
        move |ctx, _: ()| {
            let kv = kv.clone();
            let sink = sink.clone();
            async move {
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
    let root1: Comp<(), ()> = builder1.define("ud_root", move |ctx, _: ()| async move {
        ctx.eval(chain_a1, ()).await?;
        ctx.eval(chain_b1, ()).await?;
        Ok(())
    });

    let engine1 = builder1.build();
    let handle1 = {
        let engine1 = engine1.clone();
        tokio::spawn(async move { engine1.run(root1, ()).await })
    };
    wait_until(|| sink.get("doc_a").is_some() && sink.get("doc_b").is_some()).await;
    engine1.persist_now().await;
    engine1.persist_close();
    handle1.abort();
    let _ = handle1.await;
    drop(engine1);

    // Engine 2: "ud_chain_b" no longer exists in the code, and root no
    // longer calls it either (the realistic shape of a removed def: the
    // caller was updated too). A different fingerprint forces a full
    // revalidation, so root and chain_a actually re-run against the new
    // code rather than staying a stale cache hit.
    let mut registry2 = Registry::default();
    registry2.register_source(kv.clone());
    registry2.register_sink(sink.clone());
    let mut builder2 = Engine::builder();
    builder2.registry(registry2);
    builder2.persistence(persist_opts(&db_path, "fp-B"));

    let chain_a2: Comp<(), ()> = builder2.define("ud_chain_a", {
        let kv = kv.clone();
        let sink = sink.clone();
        move |ctx, _: ()| {
            let kv = kv.clone();
            let sink = sink.clone();
            async move {
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
    let root2: Comp<(), ()> = builder2.define("ud_root", move |ctx, _: ()| async move {
        ctx.eval(chain_a2, ()).await?;
        Ok(())
    });

    let engine2 = builder2.build();
    let handle2 = {
        let engine2 = engine2.clone();
        tokio::spawn(async move { engine2.run(root2, ()).await })
    };

    // `run` must complete its initial evaluation (i.e. the task is still
    // alive, looping, rather than having returned an error) and chain_a's
    // output must be present and correct.
    wait_until(|| sink.get("doc_a").is_some()).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!handle2.is_finished(), "engine must start successfully despite the orphaned chain_b record");
    assert_eq!(sink.get("doc_a"), Some("1".to_string()));

    handle2.abort();
}

/// A corrupted (non-redb) database file must never panic: the engine simply
/// treats it as unreadable, wipes it, and starts cold.
#[tokio::test]
async fn corrupted_db_file_starts_cold_without_panic() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("graph.redb");
    std::fs::write(&db_path, b"not a redb file, just garbage bytes").unwrap();

    let kv = MemKvSource::new("kv");
    let sink = VecSink::new("docs");
    kv.set("a", "1").await;

    let mut registry = Registry::default();
    registry.register_source(kv.clone());
    registry.register_sink(sink.clone());
    let mut builder = Engine::builder();
    builder.registry(registry);
    builder.persistence(persist_opts(&db_path, "fp-A"));

    let root: Comp<(), ()> = builder.define("corrupt_root", {
        let kv = kv.clone();
        let sink = sink.clone();
        move |ctx, _: ()| {
            let kv = kv.clone();
            let sink = sink.clone();
            async move {
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

    let engine = builder.build();
    let handle = {
        let engine = engine.clone();
        tokio::spawn(async move { engine.run(root, ()).await })
    };

    wait_until(|| sink.get("doc_a") == Some("1".to_string())).await;
    assert!(!handle.is_finished(), "a corrupted db must not crash the engine");

    handle.abort();
}
