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
use computations::{Comp, CompKey, DirtyPriority, Engine, Fingerprint, PersistOptions, Registry};

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
    PersistOptions::new(path.to_path_buf(), Fingerprint::custom(fingerprint))
}

/// Like [`persist_opts`], but with flush timing pushed out far beyond any
/// test's runtime, so the background persister task never flushes on its
/// own — only an explicit `persist_now()` ever writes anything. Used by
/// tests that need to observe "settled but not yet durable" as a distinct,
/// stable state.
fn persist_opts_no_auto_flush(path: &std::path::Path, fingerprint: &str) -> PersistOptions {
    persist_opts(path, fingerprint)
        .flush_debounce(Duration::from_secs(3600))
        .flush_max_pending(usize::MAX)
        .flush_max_staleness(Duration::from_secs(3600))
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

/// A settled round must not wait for a flush: with the debounce/staleness
/// knobs pushed far out (see [`persist_opts_no_auto_flush`]), a mutation's
/// round can visibly settle (the sink reflects the new value) while
/// nothing has actually been written to disk yet — proven via
/// `persist_flush_count` staying put and `persist_pending_count` becoming
/// nonzero — and only `persist_now()` actually makes it durable.
#[tokio::test]
async fn rounds_do_not_block_on_flush() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("graph.redb");

    let kv = MemKvSource::new("kv");
    let sink = VecSink::new("docs");
    kv.set("a", "1").await;

    let mut registry = Registry::default();
    registry.register_source(kv.clone());
    registry.register_sink(sink.clone());
    let mut builder = Engine::builder();
    builder.registry(registry);
    builder.persistence(persist_opts_no_auto_flush(&db_path, "fp-noblock"));

    // Returns the value it read (rather than `()`) so its result hash
    // actually changes across reruns with different source content — a
    // `Comp<_, ()>` would hash the same constant `()` every time and, after
    // its first run, would never again look "changed" to persistence (its
    // sink write is a side effect the result hash doesn't capture), which
    // would make this test vacuous.
    let chain: Comp<(), String> = builder.define("nb_chain", {
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
                        content: val.clone(),
                    },
                )
                .await?;
                Ok(val)
            }
        }
    });

    let engine = builder.build();
    let handle = {
        let engine = engine.clone();
        tokio::spawn(async move { engine.run(chain, ()).await })
    };

    wait_until(|| sink.get("doc_a") == Some("1".to_string())).await;
    engine.persist_now().await;
    let baseline_flushes = engine.persist_flush_count();
    assert_eq!(baseline_flushes, 1, "the startup evaluation's result must be durable after an explicit flush");
    assert_eq!(engine.persist_pending_count(), 0);

    kv.set("a", "2").await;
    wait_until(|| sink.get("doc_a") == Some("2".to_string())).await;

    // The round has visibly settled, yet nothing has been auto-flushed:
    // the round did not wait for that.
    assert_eq!(
        engine.persist_flush_count(),
        baseline_flushes,
        "a settled round must not itself trigger (or wait for) a flush"
    );
    assert!(
        engine.persist_pending_count() > 0,
        "the new result must be enqueued for persistence even though it hasn't been flushed yet"
    );

    engine.persist_now().await;
    assert_eq!(engine.persist_flush_count(), baseline_flushes + 1, "persist_now must force exactly one more flush");
    assert_eq!(engine.persist_pending_count(), 0, "persist_now must drain everything outstanding");

    engine.persist_close();
    handle.abort();
    let _ = handle.await;
}

/// Two mutations to the same key before any flush must coalesce into one
/// final pending entry (a later update overwrites an earlier one for the
/// same key), and the record actually written reflects only the final
/// value — checked via restart correctness (a fresh engine must see the
/// second, not the first, mutation's value as a cache hit) rather than by
/// reaching into flush internals.
#[tokio::test]
async fn coalesced_updates_persist_only_the_final_value() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("graph.redb");

    let kv = MemKvSource::new("kv");
    let sink = VecSink::new("docs");
    kv.set("a", "v0").await;

    fn build(
        kv: &Arc<MemKvSource>,
        sink: &Arc<VecSink>,
        db_path: &std::path::Path,
        counter: &Arc<AtomicUsize>,
    ) -> (Engine, Comp<(), String>) {
        let mut registry = Registry::default();
        registry.register_source(kv.clone());
        registry.register_sink(sink.clone());
        let mut builder = Engine::builder();
        builder.registry(registry);
        builder.persistence(persist_opts_no_auto_flush(db_path, "fp-coalesce"));

        // Returns the value read (see `rounds_do_not_block_on_flush` for why
        // a `Comp<_, ()>` would make this test vacuous).
        let chain: Comp<(), String> = builder.define("coalesce_chain", {
            let kv = kv.clone();
            let sink = sink.clone();
            let counter = counter.clone();
            move |ctx, _: ()| {
                let kv = kv.clone();
                let sink = sink.clone();
                let counter = counter.clone();
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    let val = ctx.src_req(&kv, GetKey("a".to_string())).await?.unwrap_or_default();
                    ctx.sink_req(
                        &sink,
                        WriteDoc {
                            name: "doc_a".to_string(),
                            content: val.clone(),
                        },
                    )
                    .await?;
                    Ok(val)
                }
            }
        });
        (builder.build(), chain)
    }

    let counter1 = Arc::new(AtomicUsize::new(0));
    let (engine1, root1) = build(&kv, &sink, &db_path, &counter1);
    let handle1 = {
        let engine1 = engine1.clone();
        tokio::spawn(async move { engine1.run(root1, ()).await })
    };
    wait_until(|| sink.get("doc_a") == Some("v0".to_string())).await;
    engine1.persist_now().await;

    // Two mutations, back to back, with no flush in between (the debounce
    // knobs make sure of that).
    kv.set("a", "v1").await;
    wait_until(|| sink.get("doc_a") == Some("v1".to_string())).await;
    kv.set("a", "v2").await;
    wait_until(|| sink.get("doc_a") == Some("v2".to_string())).await;

    assert_eq!(
        engine1.persist_pending_count(),
        1,
        "two updates to the same key before any flush must coalesce into one pending entry"
    );

    engine1.persist_now().await;
    assert_eq!(engine1.persist_pending_count(), 0);

    engine1.persist_close();
    handle1.abort();
    let _ = handle1.await;
    drop(engine1);

    let counter2 = Arc::new(AtomicUsize::new(0));
    let (engine2, root2) = build(&kv, &sink, &db_path, &counter2);
    let handle2 = {
        let engine2 = engine2.clone();
        tokio::spawn(async move { engine2.run(root2, ()).await })
    };

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(counter2.load(Ordering::SeqCst), 0, "the restored record must be a pure cache hit");
    assert_eq!(sink.get("doc_a"), Some("v2".to_string()), "only the final coalesced value must have persisted");

    handle2.abort();
}

/// A removal enqueued after an update to the *same* key (a node updated,
/// then collected by liveness GC once it becomes unreachable, all before
/// any flush) must supersede that update: nothing survives to disk for
/// that key. Checked via restart correctness — if the removal had lost to
/// the stale update, the key would wrongly come back as a cache hit
/// instead of running fresh.
#[tokio::test]
async fn removal_supersedes_pending_update_across_gc() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("graph.redb");

    let kv = MemKvSource::new("kv");
    let sink = VecSink::new("docs");
    kv.set("mode", "on").await;
    kv.set("val", "v1").await;

    fn build(
        kv: &Arc<MemKvSource>,
        sink: &Arc<VecSink>,
        db_path: &std::path::Path,
        leaf_runs: &Arc<AtomicUsize>,
    ) -> (Engine, Comp<(), ()>) {
        let mut registry = Registry::default();
        registry.register_source(kv.clone());
        registry.register_sink(sink.clone());
        let mut builder = Engine::builder();
        builder.registry(registry);
        builder.persistence(persist_opts_no_auto_flush(db_path, "fp-gc"));

        // Returns the value read (rather than `()`) so its second run
        // (after `val` changes to "v2") actually enqueues a fresh pending
        // update for persistence — the update this test needs a later
        // removal to supersede. A `Comp<_, ()>` would hash the same
        // constant `()` on every run, so after its first run it would
        // never look "changed" to persistence again (its sink write is a
        // side effect the result hash doesn't capture), leaving no pending
        // update for the removal to actually supersede.
        let leaf: Comp<(), String> = builder.define("gc_leaf", {
            let kv = kv.clone();
            let sink = sink.clone();
            let leaf_runs = leaf_runs.clone();
            move |ctx, _: ()| {
                let kv = kv.clone();
                let sink = sink.clone();
                let leaf_runs = leaf_runs.clone();
                async move {
                    leaf_runs.fetch_add(1, Ordering::SeqCst);
                    let val = ctx.src_req(&kv, GetKey("val".to_string())).await?.unwrap_or_default();
                    ctx.sink_req(
                        &sink,
                        WriteDoc {
                            name: "leaf_doc".to_string(),
                            content: val.clone(),
                        },
                    )
                    .await?;
                    Ok(val)
                }
            }
        });

        let root: Comp<(), ()> = builder.define("gc_root", {
            let kv = kv.clone();
            move |ctx, _: ()| {
                let kv = kv.clone();
                async move {
                    let mode = ctx.src_req(&kv, GetKey("mode".to_string())).await?.unwrap_or_default();
                    if mode == "on" {
                        ctx.eval(leaf, ()).await?;
                    }
                    Ok(())
                }
            }
        });

        (builder.build(), root)
    }

    let leaf_runs1 = Arc::new(AtomicUsize::new(0));
    let (engine1, root1) = build(&kv, &sink, &db_path, &leaf_runs1);
    let handle1 = {
        let engine1 = engine1.clone();
        tokio::spawn(async move { engine1.run(root1, ()).await })
    };
    wait_until(|| sink.get("leaf_doc") == Some("v1".to_string())).await;
    engine1.persist_now().await;
    assert_eq!(engine1.persist_flush_count(), 1);

    // Update leaf's value (queues an update for its key), then -- before
    // any flush -- make it unreachable (queues a removal for that very
    // same key via liveness GC).
    kv.set("val", "v2").await;
    wait_until(|| sink.get("leaf_doc") == Some("v2".to_string())).await;
    kv.set("mode", "off").await;
    wait_until(|| sink.get("leaf_doc").is_none()).await;

    assert_eq!(engine1.persist_flush_count(), 1, "no auto-flush should have happened yet");
    assert_eq!(
        engine1.persist_pending_count(),
        1,
        "the GC removal must have coalesced with (superseded) the still-pending update for the same key"
    );

    engine1.persist_now().await;
    assert_eq!(engine1.persist_flush_count(), 2);
    assert_eq!(engine1.persist_pending_count(), 0);

    engine1.persist_close();
    handle1.abort();
    let _ = handle1.await;
    drop(engine1);

    // "Restart" with mode flipped back on: root will call leaf again. If
    // leaf's record had wrongly survived (update beating removal), this
    // would be a pure cache hit; it must instead run fresh.
    kv.set("mode", "on").await;

    let leaf_runs2 = Arc::new(AtomicUsize::new(0));
    let (engine2, root2) = build(&kv, &sink, &db_path, &leaf_runs2);
    let handle2 = {
        let engine2 = engine2.clone();
        tokio::spawn(async move { engine2.run(root2, ()).await })
    };

    wait_until(|| sink.get("leaf_doc") == Some("v2".to_string())).await;
    assert!(
        leaf_runs2.load(Ordering::SeqCst) >= 1,
        "leaf's persisted record must have been removed by GC, forcing a fresh run rather than a stale cache hit"
    );

    handle2.abort();
}

/// A crash after a *known* flush point (forced via `persist_now`) but
/// before any later change is flushed must lose only that later,
/// still-pending change — not the whole graph, and not silently: on
/// restart, `probe_versions` must catch that the live source has moved on
/// since the persisted snapshot and recompute forward to the current,
/// correct value.
#[tokio::test]
async fn crash_after_known_flush_point_loses_only_unflushed_tail() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("graph.redb");

    let kv = MemKvSource::new("kv");
    let sink = VecSink::new("docs");
    kv.set("a", "before").await;

    fn build(
        kv: &Arc<MemKvSource>,
        sink: &Arc<VecSink>,
        db_path: &std::path::Path,
        counter: &Arc<AtomicUsize>,
    ) -> (Engine, Comp<(), ()>) {
        let mut registry = Registry::default();
        registry.register_source(kv.clone());
        registry.register_sink(sink.clone());
        let mut builder = Engine::builder();
        builder.registry(registry);
        builder.persistence(persist_opts_no_auto_flush(db_path, "fp-crash"));

        let chain: Comp<(), ()> = builder.define("crash_chain", {
            let kv = kv.clone();
            let sink = sink.clone();
            let counter = counter.clone();
            move |ctx, _: ()| {
                let kv = kv.clone();
                let sink = sink.clone();
                let counter = counter.clone();
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
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
        (builder.build(), chain)
    }

    let counter1 = Arc::new(AtomicUsize::new(0));
    let (engine1, root1) = build(&kv, &sink, &db_path, &counter1);
    let handle1 = {
        let engine1 = engine1.clone();
        tokio::spawn(async move { engine1.run(root1, ()).await })
    };
    wait_until(|| sink.get("doc_a") == Some("before".to_string())).await;

    // A known-durable point.
    engine1.persist_now().await;
    assert_eq!(engine1.persist_flush_count(), 1);

    // A further change settles live but is never flushed -- simulating a
    // crash landing inside the debounce window.
    kv.set("a", "after").await;
    wait_until(|| sink.get("doc_a") == Some("after".to_string())).await;
    assert_eq!(engine1.persist_flush_count(), 1, "the post-flush-point change must still be unflushed at crash time");

    // "Crash": no persist_now, no persist_close.
    handle1.abort();
    let _ = handle1.await;
    drop(engine1);

    let counter2 = Arc::new(AtomicUsize::new(0));
    let (engine2, root2) = build(&kv, &sink, &db_path, &counter2);
    let handle2 = {
        let engine2 = engine2.clone();
        tokio::spawn(async move { engine2.run(root2, ()).await })
    };

    // `sink` is the same `Arc` engine1 already wrote "after" into live,
    // before the crash -- so its content alone can't prove engine2 did
    // anything (it would already read "after" even if engine2 wrongly
    // treated the node as a stale cache hit and never recomputed at all).
    // Waiting on `counter2` instead proves a genuine fresh execution
    // happened on restart.
    wait_until(|| counter2.load(Ordering::SeqCst) >= 1).await;
    assert_eq!(
        sink.get("doc_a"),
        Some("after".to_string()),
        "the recompute must reproduce the current, correct value"
    );

    handle2.abort();
}

/// A restarted engine's persisted records must carry enough to rebuild the
/// full dependency graph, not merely enough for the root itself to be a
/// cache hit (which alone proves nothing about `comp_deps`: a `Clean` root
/// short-circuits `prepare()` without ever visiting its dependencies at
/// all). This test specifically exercises `comp_deps`/`rdeps` restoration
/// (in-memory, keyed by `NodeId` — see `crate::engine::NodeId`) by forcing a
/// liveness GC pass after restart and confirming a dependency reachable
/// only via `root`'s restored `comp_deps` survives it, rather than being
/// incorrectly collected as unreachable — which is exactly what would
/// happen if a record's persisted `def_name` + `param_hash` pairs failed to
/// resolve back into a live edge on restart.
#[tokio::test]
async fn restart_restores_comp_deps_sufficient_for_liveness_gc() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("graph.redb");

    let kv = MemKvSource::new("kv");
    let sink = VecSink::new("docs");
    kv.set("a", "1").await;

    fn build(
        kv: &Arc<MemKvSource>,
        sink: &Arc<VecSink>,
        db_path: &std::path::Path,
        counter: &Arc<AtomicUsize>,
    ) -> (Engine, Comp<(), ()>) {
        let mut registry = Registry::default();
        registry.register_source(kv.clone());
        registry.register_sink(sink.clone());

        let mut builder = Engine::builder();
        builder.registry(registry);
        builder.persistence(persist_opts(db_path, "fp-v1"));

        let leaf: Comp<(), ()> = builder.define("gc_leaf", {
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
                            name: "doc_leaf".to_string(),
                            content: val,
                        },
                    )
                    .await?;
                    Ok(())
                }
            }
        });

        let root: Comp<(), ()> = builder.define("gc_root", {
            let counter = counter.clone();
            move |ctx, _: ()| {
                let counter = counter.clone();
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    ctx.eval(leaf, ()).await?;
                    Ok(())
                }
            }
        });

        (builder.build(), root)
    }

    let counter1 = Arc::new(AtomicUsize::new(0));
    let (engine1, root1) = build(&kv, &sink, &db_path, &counter1);
    let handle1 = {
        let engine1 = engine1.clone();
        tokio::spawn(async move { engine1.run(root1, ()).await })
    };
    wait_until(|| sink.get("doc_leaf") == Some("1".to_string())).await;
    engine1.persist_now().await;
    engine1.persist_close();
    handle1.abort();
    let _ = handle1.await;
    drop(engine1);

    let counter2 = Arc::new(AtomicUsize::new(0));
    let (engine2, root2) = build(&kv, &sink, &db_path, &counter2);
    let handle2 = {
        let engine2 = engine2.clone();
        tokio::spawn(async move { engine2.run(root2, ()).await })
    };

    // Give the restart's initial evaluation + startup GC a moment to settle
    // as a pure cache hit (proves nothing about `comp_deps` on its own —
    // see this test's doc comment — but establishes the baseline).
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(counter2.load(Ordering::SeqCst), 0, "root must be a pure cache hit after restart");
    assert_eq!(sink.get("doc_leaf"), Some("1".to_string()), "restored output must survive startup GC");

    // Now force a real propagation round (and the liveness GC pass that
    // unconditionally follows it, even an empty round — see
    // `crate::driver::Engine::run`) *without* touching `root` or `leaf`:
    // mark a bogus key with no corresponding node dirty. `mark_dirty` wakes
    // the driver loop regardless of whether a node exists for the key (see
    // `EngineInner::mark_dirty`), so this triggers `propagate` (a no-op:
    // `split_by_tier` drops a key with no node) immediately followed by
    // `liveness_gc` — whose mark-sweep walks `root`'s *restored* `comp_deps`
    // (never re-established live, since `root` itself never reran) to
    // decide what's still reachable. If those edges hadn't been correctly
    // rebuilt from the persisted `def_name`/`param_hash` pairs on restore,
    // `leaf` would look unreachable and get collected: its sink output
    // deleted.
    let bogus_key = CompKey::new(computations::DefId::new("no_such_computation"), &());
    let mut keys = std::collections::HashSet::new();
    keys.insert(bogus_key);
    engine2.mark_dirty(&keys, DirtyPriority::Input);

    // No observable "round complete" signal is exposed publicly; a fixed
    // settle window is the same pattern this file already uses elsewhere
    // (e.g. the plain cache-hit check above).
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert_eq!(counter2.load(Ordering::SeqCst), 0, "root must still never have rerun");
    assert_eq!(
        sink.get("doc_leaf"),
        Some("1".to_string()),
        "leaf's output must survive liveness GC: it is only reachable via root's restored comp_deps edge"
    );

    handle2.abort();
}
