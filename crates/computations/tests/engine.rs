//! Integration tests for the engine core: memoization, dependency
//! collection, single-flight dedup, cycle detection, and concurrent
//! evaluation. Run with `cargo test -p computations --features testutil`.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use computations::error::CompError;
use computations::testutil::{GetKey, MemKvSource, VecSink, WriteDoc};
use computations::{Comp, Engine, define_comp, define_comp_rec};

/// The paper's running example: `store (sum (number_of_lines <$> file_list))`
/// over a `MemKvSource` of file contents and a `VecSink` of written docs.
#[tokio::test]
async fn paper_pipeline_computes_and_stores_line_count_sum() {
    let kv = MemKvSource::new("kv");
    let sink = VecSink::new("docs");

    kv.set("file_list", "a,b,c").await;
    kv.set("a", "line1\nline2").await; // 2 lines
    kv.set("b", "line1").await; // 1 line
    kv.set("c", "line1\nline2\nline3").await; // 3 lines

    let nol_runs = Arc::new(AtomicUsize::new(0));
    let sum_runs = Arc::new(AtomicUsize::new(0));
    let store_runs = Arc::new(AtomicUsize::new(0));

    let mut builder = Engine::builder();

    let number_of_lines: Comp<String, usize> = builder.define("number_of_lines", {
        let kv = kv.clone();
        let runs = nol_runs.clone();
        move |ctx, key: String| {
            let kv = kv.clone();
            let runs = runs.clone();
            async move {
                runs.fetch_add(1, Ordering::SeqCst);
                let content = ctx.src_req(&kv, GetKey(key)).await?.unwrap_or_default();
                Ok(content.lines().count())
            }
        }
    });

    let sum: Comp<(), usize> = builder.define("sum", {
        let kv = kv.clone();
        let runs = sum_runs.clone();
        move |ctx, _: ()| {
            let kv = kv.clone();
            let runs = runs.clone();
            async move {
                runs.fetch_add(1, Ordering::SeqCst);
                let list = ctx
                    .src_req(&kv, GetKey("file_list".to_string()))
                    .await?
                    .unwrap_or_default();
                let names: Vec<String> = list
                    .split(',')
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_string())
                    .collect();
                let counts = ctx.eval_all(&number_of_lines, names).await?;
                Ok(counts.into_iter().sum::<usize>())
            }
        }
    });

    let store: Comp<(), ()> = builder.define("store", {
        let sink = sink.clone();
        let runs = store_runs.clone();
        move |ctx, _: ()| {
            let sink = sink.clone();
            let runs = runs.clone();
            async move {
                runs.fetch_add(1, Ordering::SeqCst);
                let total = ctx.eval(&sum, ()).await?;
                ctx.sink_req(
                    &sink,
                    WriteDoc {
                        name: "sum".to_string(),
                        content: total.to_string(),
                    },
                )
                .await?;
                Ok(())
            }
        }
    });

    let engine = builder.build();
    engine.eval_root(&store, ()).await.unwrap();

    assert_eq!(sink.get("sum"), Some("6".to_string()));
    assert_eq!(nol_runs.load(Ordering::SeqCst), 3, "one run per file key");
    assert_eq!(sum_runs.load(Ordering::SeqCst), 1);
    assert_eq!(store_runs.load(Ordering::SeqCst), 1);
}

// Deliberately exercises the low-level `register(define_comp(...))` path
// (rather than the `builder.define(...)` sugar used elsewhere in this file)
// to keep that path under test.
#[tokio::test]
async fn eval_root_memoizes_repeated_calls() {
    let runs = Arc::new(AtomicUsize::new(0));
    let mut builder = Engine::builder();
    let comp: Comp<i32, i32> = builder.register(define_comp("double", {
        let runs = runs.clone();
        move |_ctx, n: i32| {
            let runs = runs.clone();
            async move {
                runs.fetch_add(1, Ordering::SeqCst);
                Ok(n * 2)
            }
        }
    }));
    let engine = builder.build();

    let a = engine.eval_root(&comp, 21).await.unwrap();
    let b = engine.eval_root(&comp, 21).await.unwrap();

    assert_eq!(a, 42);
    assert_eq!(b, 42);
    assert_eq!(runs.load(Ordering::SeqCst), 1, "second eval_root should hit the cache");
}

/// A evals both B and C; B and C both eval D with the same param. D must
/// run exactly once even though two different callers depend on it.
#[tokio::test]
async fn diamond_shared_dependency_runs_once() {
    let d_runs = Arc::new(AtomicUsize::new(0));
    let mut builder = Engine::builder();

    let d: Comp<i32, i32> = builder.define("diamond_d", {
        let runs = d_runs.clone();
        move |_ctx, n: i32| {
            let runs = runs.clone();
            async move {
                runs.fetch_add(1, Ordering::SeqCst);
                Ok(n + 1)
            }
        }
    });

    let b: Comp<i32, i32> = builder.define("diamond_b", move |ctx, n: i32| async move { ctx.eval(&d, n).await });

    let c: Comp<i32, i32> = builder.define("diamond_c", move |ctx, n: i32| async move { ctx.eval(&d, n).await });

    let a: Comp<i32, i32> = builder.define("diamond_a", move |ctx, n: i32| async move {
        let (bv, cv) = futures::try_join!(ctx.eval(&b, n), ctx.eval(&c, n))?;
        Ok(bv + cv)
    });

    let engine = builder.build();
    let result = engine.eval_root(&a, 10).await.unwrap();

    assert_eq!(result, 22); // (10+1) + (10+1)
    assert_eq!(d_runs.load(Ordering::SeqCst), 1, "D must run exactly once");
}

/// Two concurrent `eval_root` calls on the same slow computation must share
/// one execution (single-flight dedup), not run the body twice.
#[tokio::test]
async fn concurrent_eval_root_deduplicates_single_flight() {
    let runs = Arc::new(AtomicUsize::new(0));
    let mut builder = Engine::builder();
    let comp: Comp<i32, i32> = builder.define("slow", {
        let runs = runs.clone();
        move |_ctx, n: i32| {
            let runs = runs.clone();
            async move {
                runs.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(50)).await;
                Ok(n * 10)
            }
        }
    });
    let engine = builder.build();

    let (a, b) = tokio::join!(engine.eval_root(&comp, 4), engine.eval_root(&comp, 4));

    assert_eq!(a.unwrap(), 40);
    assert_eq!(b.unwrap(), 40);
    assert_eq!(runs.load(Ordering::SeqCst), 1, "body should run exactly once");
}

/// A computation that (via a second computation) evals itself with the SAME
/// param must fail fast with `CompError::Cycle`, never hang.
#[tokio::test]
async fn cycle_with_same_param_is_detected_promptly() {
    let a: Comp<i32, i32> = Comp::named("cycle_a");
    let b: Comp<i32, i32> = Comp::named("cycle_b");

    let mut builder = Engine::builder();
    builder.define("cycle_a", move |ctx, n: i32| async move { ctx.eval(&b, n).await });
    builder.define("cycle_b", move |ctx, n: i32| async move { ctx.eval(&a, n).await });
    let engine = builder.build();

    let result = tokio::time::timeout(Duration::from_millis(500), engine.eval_root(&a, 1))
        .await
        .expect("cycle detection must not hang");

    assert!(
        matches!(result, Err(CompError::Cycle(_))),
        "expected a Cycle error, got {result:?}"
    );
}

/// Self-recursion is fine as long as each recursive call uses a different
/// param (no cycle).
#[tokio::test]
async fn self_recursion_with_different_params_works() {
    let countdown: Comp<i64, i64> = Comp::named("countdown_sum");

    let mut builder = Engine::builder();
    builder.define("countdown_sum", move |ctx, n: i64| async move {
        if n <= 0 {
            Ok(0)
        } else {
            let rest = ctx.eval(&countdown, n - 1).await?;
            Ok(n + rest)
        }
    });
    let engine = builder.build();

    let result = tokio::time::timeout(Duration::from_millis(500), engine.eval_root(&countdown, 5))
        .await
        .expect("self-recursion over distinct params must not hang")
        .unwrap();

    assert_eq!(result, 15); // 5+4+3+2+1+0
}

/// `Ctx::eval_all` runs its calls concurrently: three 50ms-sleeping
/// evaluations should take well under 150ms total.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn eval_all_runs_concurrently() {
    let mut builder = Engine::builder();
    let sleepy: Comp<i32, i32> = builder.define("sleepy", |_ctx, n: i32| async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(n * 2)
    });
    let root: Comp<(), Vec<i32>> =
        builder.define("root_eval_all", move |ctx, _: ()| async move { ctx.eval_all(&sleepy, [1, 2, 3]).await });
    let engine = builder.build();

    let start = tokio::time::Instant::now();
    let result = engine.eval_root(&root, ()).await.unwrap();
    let elapsed = start.elapsed();

    assert_eq!(result, vec![2, 4, 6]);
    assert!(
        elapsed < Duration::from_millis(120),
        "eval_all should run concurrently, took {elapsed:?}"
    );
}

/// `define_comp_rec` hands the body a working handle to its own
/// computation, so self-recursion needs no separate `Comp::named` +
/// `define_comp` pair with a duplicated name string (contrast with
/// `self_recursion_with_different_params_works` above).
///
/// Deliberately exercises the low-level `register(define_comp_rec(...))`
/// path (rather than the `builder.define_rec(...)` sugar) to keep that path
/// under test.
#[tokio::test]
async fn define_comp_rec_supports_self_recursion() {
    let mut builder = Engine::builder();
    let countdown: Comp<i64, i64> = builder.register(define_comp_rec(
        "countdown_sum_rec",
        |this, ctx, n: i64| async move {
            if n <= 0 {
                Ok(0)
            } else {
                let rest = ctx.eval(&this, n - 1).await?;
                Ok(n + rest)
            }
        },
    ));
    let engine = builder.build();

    let result = tokio::time::timeout(Duration::from_millis(500), engine.eval_root(&countdown, 5))
        .await
        .expect("self-recursion over distinct params must not hang")
        .unwrap();

    assert_eq!(result, 15); // 5+4+3+2+1+0
}

/// `EngineBuilder::source`/`EngineBuilder::sink` register directly on the
/// builder's internal registry, without the caller building a `Registry` by
/// hand — the erased source/sink must actually reach the driver's wiring
/// (proven here via the same output-GC path exercised by the
/// `Registry`-based test in `engine.rs`'s unit test module).
#[tokio::test]
async fn builder_source_and_sink_register_without_explicit_registry() {
    let kv = MemKvSource::new("kv");
    let sink = VecSink::new("docs");
    kv.set("name", "a").await;

    let mut builder = Engine::builder();
    builder.source(kv.clone());
    builder.sink(sink.clone());

    let comp: Comp<(), ()> = builder.define("write_named_doc", {
        let kv = kv.clone();
        let sink = sink.clone();
        move |ctx, _: ()| {
            let kv = kv.clone();
            let sink = sink.clone();
            async move {
                let name = ctx.src_req(&kv, GetKey("name".to_string())).await?.unwrap_or_default();
                ctx.sink_req(
                    &sink,
                    WriteDoc {
                        name,
                        content: "x".to_string(),
                    },
                )
                .await?;
                Ok(())
            }
        }
    });
    let engine = builder.build();

    engine.eval_root(&comp, ()).await.unwrap();
    assert_eq!(sink.get("a"), Some("x".to_string()));
}

/// `define_with` clones its shared env into the body on every invocation;
/// destructuring the env tuple directly in the closure's parameter list
/// gives access to its pieces (here a log and a string prefix) with no
/// `.clone()` needed anywhere in the call site beyond the env tuple's own
/// construction.
#[tokio::test]
async fn define_with_threads_shared_env_into_every_invocation() {
    let log: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let prefix = "n=".to_string();

    let mut builder = Engine::builder();
    let env = (log.clone(), prefix);
    let record: Comp<i32, i32> = builder.define_with("record", &env, |(log, prefix), _ctx, n: i32| async move {
        log.lock().unwrap().push(format!("{prefix}{n}"));
        Ok(n * 2)
    });
    let engine = builder.build();

    assert_eq!(engine.eval_root(&record, 1).await.unwrap(), 2);
    assert_eq!(engine.eval_root(&record, 2).await.unwrap(), 4);
    assert_eq!(engine.eval_root(&record, 3).await.unwrap(), 6);

    assert_eq!(
        *log.lock().unwrap(),
        vec!["n=1".to_string(), "n=2".to_string(), "n=3".to_string()],
        "every invocation should observe the same shared env"
    );
}

/// `define_rec_with` combines self-recursion (a working `this` handle,
/// passed between the env and `Ctx`) with a shared env: each recursive step
/// appends to an env-held `Arc<Mutex<Vec<i64>>>`, proving both the
/// recursive handle and the env are threaded correctly on every call,
/// including the recursive ones.
#[tokio::test]
async fn define_rec_with_supports_recursion_with_shared_env() {
    let steps: Arc<Mutex<Vec<i64>>> = Arc::new(Mutex::new(Vec::new()));

    let mut builder = Engine::builder();
    let env = steps.clone();
    let countdown: Comp<i64, i64> =
        builder.define_rec_with("countdown_with_env", &env, |steps, this, ctx, n: i64| async move {
            steps.lock().unwrap().push(n);
            if n <= 0 {
                Ok(0)
            } else {
                let rest = ctx.eval(&this, n - 1).await?;
                Ok(n + rest)
            }
        });
    let engine = builder.build();

    let result = engine.eval_root(&countdown, 5).await.unwrap();

    assert_eq!(result, 15); // 5+4+3+2+1+0
    assert_eq!(*steps.lock().unwrap(), vec![5, 4, 3, 2, 1, 0]);
}
