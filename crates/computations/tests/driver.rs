//! Integration tests for the driver: initial evaluation, startup GC,
//! push-based change propagation with early cutoff, liveness GC, two-tier
//! dirty priorities, and failure resilience. Run with `cargo test -p
//! computations --features testutil`.
//!
//! `Engine::run` never returns on the happy path, so every test spawns it
//! via `tokio::spawn`, polls assertions through `wait_until` (bounded by a
//! 5s timeout so a propagation bug can never hang `cargo test`), and aborts
//! the task before returning.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use computations::error::CompError;
use computations::testutil::{GetKey, MemKvSource, VecSink, WriteDoc};
use computations::{Comp, CompKey, DirtyPriority, Engine, Registry, Sink};

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

    let root: Comp<(), ()> = builder.define("two_chains_root", move |ctx, _: ()| async move {
        ctx.eval(chain1, ()).await?;
        ctx.eval(chain2, ()).await?;
        Ok(())
    });

    let engine = builder.build();
    let handle = {
        let engine = engine.clone();
        tokio::spawn(async move { engine.run(root, ()).await })
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
        let runs = parent_runs.clone();
        move |ctx, _: ()| {
            let sink = sink.clone();
            let runs = runs.clone();
            async move {
                runs.fetch_add(1, Ordering::SeqCst);
                let v = ctx.eval(child, ()).await?;
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
        tokio::spawn(async move { engine.run(parent, ()).await })
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
        move |ctx, _: ()| {
            let kv = kv.clone();
            async move {
                let list = ctx
                    .src_req(&kv, GetKey("file_list".to_string()))
                    .await?
                    .unwrap_or_default();
                let names: Vec<String> = list.split(',').filter(|s| !s.is_empty()).map(str::to_string).collect();
                ctx.eval_all(child, names).await?;
                Ok(())
            }
        }
    });

    let engine = builder.build();
    let handle = {
        let engine = engine.clone();
        tokio::spawn(async move { engine.run(root, ()).await })
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
        tokio::spawn(async move { engine.run(root, ()).await })
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
        tokio::spawn(async move { engine.run(root, ()).await })
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

/// A shared, ordered event log: each `record` call appends `label` tagged
/// with a fresh, monotonically increasing sequence number, so tests can
/// assert *relative* ordering between events from independent tasks
/// (running comp bodies) without depending on wall-clock timing.
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

    /// The highest sequence number recorded under `label`, if any.
    fn last_seq(&self, label: &str) -> Option<usize> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter(|(l, _)| *l == label)
            .map(|(_, seq)| *seq)
            .max()
    }
}

/// `mark_all_dirty(Revalidate)` on a settled, two-chain graph must
/// eventually re-run every node (run counters advance for both chains), but
/// since neither chain's computed value actually changes, the sink content
/// stays exactly as it was — early cutoff-worthy behavior at the
/// Revalidate tier too: nothing downstream should observe a change, and
/// nothing here rewrites a doc to a new value.
#[tokio::test]
async fn mark_all_dirty_revalidate_reruns_everything_without_changing_output() {
    let kv = MemKvSource::new("kv");
    let sink = VecSink::new("docs");

    let mut registry = Registry::default();
    registry.register_source(kv.clone());
    registry.register_sink(sink.clone());

    let counter1 = Arc::new(AtomicUsize::new(0));
    let counter2 = Arc::new(AtomicUsize::new(0));

    let mut builder = Engine::builder();
    builder.registry(registry);

    let chain1: Comp<(), ()> = builder.define("revalidate_chain1", {
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

    let chain2: Comp<(), ()> = builder.define("revalidate_chain2", {
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

    let root: Comp<(), ()> = builder.define("revalidate_root", move |ctx, _: ()| async move {
        ctx.eval(chain1, ()).await?;
        ctx.eval(chain2, ()).await?;
        Ok(())
    });

    let engine = builder.build();
    let handle = {
        let engine = engine.clone();
        tokio::spawn(async move { engine.run(root, ()).await })
    };

    wait_until(|| counter1.load(Ordering::SeqCst) >= 1 && counter2.load(Ordering::SeqCst) >= 1).await;
    assert_eq!(sink.get("doc_a"), Some(String::new()));
    assert_eq!(sink.get("doc_b"), Some(String::new()));

    let c1_before = counter1.load(Ordering::SeqCst);
    let c2_before = counter2.load(Ordering::SeqCst);

    engine.mark_all_dirty(DirtyPriority::Revalidate);

    wait_until(|| counter1.load(Ordering::SeqCst) > c1_before && counter2.load(Ordering::SeqCst) > c2_before).await;
    // Give the round a moment to fully settle before checking nothing else
    // changes.
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert_eq!(
        sink.get("doc_a"),
        Some(String::new()),
        "revalidation must not rewrite doc_a's content: the recomputed value is unchanged"
    );
    assert_eq!(
        sink.get("doc_b"),
        Some(String::new()),
        "revalidation must not rewrite doc_b's content: the recomputed value is unchanged"
    );

    handle.abort();
}

/// Input-tier work must not be starved by an in-progress Revalidate-tier
/// sweep: mark a slow, two-stage Revalidate chain dirty, then immediately
/// change a key feeding an unrelated (fast) Input chain. The Input chain's
/// rerun must complete — and its doc must be updated — strictly before the
/// slow Revalidate sweep's last rerun, proving the driver preempts between
/// Revalidate waves rather than draining the whole sweep first.
///
/// Ordering is asserted via `SeqLog`'s monotonic sequence numbers (not
/// wall-clock timestamps), so this only depends on relative event order,
/// not on any particular timing margin.
#[tokio::test]
async fn input_priority_preempts_in_progress_revalidate_sweep() {
    let kv = MemKvSource::new("kv");
    let sink = VecSink::new("docs");
    kv.set("a", "before").await;

    let mut registry = Registry::default();
    registry.register_source(kv.clone());
    registry.register_sink(sink.clone());

    let log = SeqLog::default();
    let chain1_runs = Arc::new(AtomicUsize::new(0));
    let slow_a_runs = Arc::new(AtomicUsize::new(0));
    let slow_b_runs = Arc::new(AtomicUsize::new(0));

    let mut builder = Engine::builder();
    builder.registry(registry);

    // Fast Input-tier chain: reads "a", writes doc_a, no artificial delay.
    let chain1: Comp<(), ()> = builder.define("priority_chain1", {
        let kv = kv.clone();
        let sink = sink.clone();
        let runs = chain1_runs.clone();
        let log = log.clone();
        move |ctx, _: ()| {
            let kv = kv.clone();
            let sink = sink.clone();
            let runs = runs.clone();
            let log = log.clone();
            async move {
                runs.fetch_add(1, Ordering::SeqCst);
                let val = ctx.src_req(&kv, GetKey("a".to_string())).await?.unwrap_or_default();
                ctx.sink_req(
                    &sink,
                    WriteDoc {
                        name: "doc_a".to_string(),
                        content: val,
                    },
                )
                .await?;
                log.record("input");
                Ok(())
            }
        }
    });

    // Slow, two-stage Revalidate-tier chain: no source deps, each stage
    // sleeps and returns an ever-incrementing value (so its hash always
    // changes and propagation to the next stage is never cut off).
    let slow_a: Comp<(), usize> = builder.define("priority_slow_a", {
        let runs = slow_a_runs.clone();
        let log = log.clone();
        move |_ctx, _: ()| {
            let runs = runs.clone();
            let log = log.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(120)).await;
                let n = runs.fetch_add(1, Ordering::SeqCst) + 1;
                log.record("revalidate");
                Ok(n)
            }
        }
    });

    let slow_b: Comp<(), usize> = builder.define("priority_slow_b", {
        let runs = slow_b_runs.clone();
        let log = log.clone();
        move |ctx, _: ()| {
            let runs = runs.clone();
            let log = log.clone();
            async move {
                let a = ctx.eval(slow_a, ()).await?;
                tokio::time::sleep(Duration::from_millis(120)).await;
                let n = runs.fetch_add(1, Ordering::SeqCst) + 1;
                log.record("revalidate");
                Ok(a + n)
            }
        }
    });

    let root: Comp<(), ()> = builder.define("priority_root", move |ctx, _: ()| async move {
        ctx.eval(chain1, ()).await?;
        ctx.eval(slow_b, ()).await?;
        Ok(())
    });

    let engine = builder.build();
    let handle = {
        let engine = engine.clone();
        tokio::spawn(async move { engine.run(root, ()).await })
    };

    wait_until(|| chain1_runs.load(Ordering::SeqCst) >= 1 && slow_b_runs.load(Ordering::SeqCst) >= 1).await;
    assert_eq!(sink.get("doc_a"), Some("before".to_string()));

    let c1_before = chain1_runs.load(Ordering::SeqCst);
    let slow_b_before = slow_b_runs.load(Ordering::SeqCst);

    // Mark only the head of the slow chain dirty at Revalidate priority;
    // slow_b is pulled in afterwards, purely by rdep propagation, so the
    // sweep genuinely spans two waves.
    let slow_a_key = CompKey::new(*slow_a.def_id(), &());
    let mut revalidate_keys = HashSet::new();
    revalidate_keys.insert(slow_a_key);
    engine.mark_dirty(&revalidate_keys, DirtyPriority::Revalidate);

    // Immediately (no delay): a genuine input change on an unrelated chain.
    kv.set("a", "after").await;

    wait_until(|| chain1_runs.load(Ordering::SeqCst) > c1_before).await;
    wait_until(|| slow_b_runs.load(Ordering::SeqCst) > slow_b_before).await;

    let input_seq = log.last_seq("input").expect("chain1 recorded an input-tier rerun");
    let revalidate_seq = log
        .last_seq("revalidate")
        .expect("the slow chain recorded at least one revalidate-tier rerun");
    assert!(
        input_seq < revalidate_seq,
        "chain1's input-tier rerun (seq {input_seq}) should complete before the slow \
         revalidate sweep's last rerun (seq {revalidate_seq})"
    );

    assert_eq!(
        sink.get("doc_a"),
        Some("after".to_string()),
        "chain1's doc should reflect the input change"
    );

    handle.abort();
}

/// A chain must keep reacting to *every* live change to the source key it
/// reads directly, not just the first one after startup.
///
/// Regression test: `EngineInner::remove_stale_source_index` used to diff
/// `old`/`new` source deps by full `RawDep` equality (including `ver`), so
/// a node re-reading the very same `(source, key)` pair at a newer version
/// looked like "the old dep vanished, a new one appeared" — deleting the
/// node's own just-(re)inserted `source_index` registration for that pair
/// one statement later. The very next live change to that key then mapped
/// to no node at all, and the chain would silently stop updating forever.
#[tokio::test]
async fn successive_live_changes_to_the_same_key_each_trigger_a_rerun() {
    let kv = MemKvSource::new("kv");
    let sink = VecSink::new("docs");
    kv.set("a", "v0").await;

    let mut registry = Registry::default();
    registry.register_source(kv.clone());
    registry.register_sink(sink.clone());

    let mut builder = Engine::builder();
    builder.registry(registry);

    let root: Comp<(), ()> = builder.define("repeat_change_root", {
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

    wait_until(|| sink.get("doc_a") == Some("v0".to_string())).await;

    kv.set("a", "v1").await;
    wait_until(|| sink.get("doc_a") == Some("v1".to_string())).await;

    // Before the fix, this second live change would never be observed:
    // `source_index` had already lost the node's registration for `a`
    // after the first change settled.
    kv.set("a", "v2").await;
    wait_until(|| sink.get("doc_a") == Some("v2".to_string())).await;

    kv.set("a", "v3").await;
    wait_until(|| sink.get("doc_a") == Some("v3".to_string())).await;

    handle.abort();
}

/// A node with its own *direct* source dependency (so a source change
/// dirties it directly, and it is re-run via its own `rerun` closure rather
/// than only ever being reached through a parent's `ctx.eval`) must still
/// be collected by liveness GC once it becomes unreachable.
///
/// Regression test: `EngineInner::make_rerun`'s closure used to re-evaluate
/// a dirtied node via `Engine::eval_root`, whose outer `Ctx { caller: None
/// }` unconditionally marks its argument a GC root as a side effect. Since
/// `roots` only ever grows, the first time change propagation ever
/// re-ran *any* node directly (which is the ordinary case for a node that
/// reads a source itself, not only via a parent), that node became a
/// permanent root — liveness GC would then never collect it again, even
/// after its real (structural) caller stopped referencing it entirely.
#[tokio::test]
async fn gc_collects_a_directly_source_dependent_node_once_unreachable() {
    let kv = MemKvSource::new("kv");
    let sink = VecSink::new("docs");
    kv.set("mode", "on").await;
    kv.set("val", "v1").await;

    let mut registry = Registry::default();
    registry.register_source(kv.clone());
    registry.register_sink(sink.clone());

    let mut builder = Engine::builder();
    builder.registry(registry);

    // `leaf` reads `val` *directly*: a change to `val` dirties `leaf`
    // itself (not `root`), so `leaf` is re-run via its own `rerun` closure
    // during propagation -- the exact path that used to wrongly mark it a
    // permanent root.
    let leaf: Comp<(), ()> = builder.define("direct_gc_leaf", {
        let kv = kv.clone();
        let sink = sink.clone();
        move |ctx, _: ()| {
            let kv = kv.clone();
            let sink = sink.clone();
            async move {
                let val = ctx.src_req(&kv, GetKey("val".to_string())).await?.unwrap_or_default();
                ctx.sink_req(
                    &sink,
                    WriteDoc {
                        name: "leaf_doc".to_string(),
                        content: val,
                    },
                )
                .await?;
                Ok(())
            }
        }
    });

    let root: Comp<(), ()> = builder.define("direct_gc_root", {
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

    let engine = builder.build();
    let handle = {
        let engine = engine.clone();
        tokio::spawn(async move { engine.run(root, ()).await })
    };

    wait_until(|| sink.get("leaf_doc") == Some("v1".to_string())).await;

    // Dirty `leaf` directly (not via `root`) -- this is what used to mark
    // it a permanent root.
    kv.set("val", "v2").await;
    wait_until(|| sink.get("leaf_doc") == Some("v2".to_string())).await;

    // Now make `leaf` unreachable: `root` stops calling it.
    kv.set("mode", "off").await;
    wait_until(|| sink.get("leaf_doc").is_none()).await;

    handle.abort();
}
