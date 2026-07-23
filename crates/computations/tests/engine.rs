//! Integration tests for the engine core: memoization, dependency
//! collection, single-flight dedup, cycle detection, and concurrent
//! evaluation. Run with `cargo test -p computations --features testutil`.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use computations::error::CompError;
use computations::testutil::{GetKey, MemKvSource, VecSink, WriteDoc};
use computations::{Comp, Engine, define_comp};

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

    let number_of_lines: Comp<String, usize> = builder.register(define_comp("number_of_lines", {
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
    }));

    let sum: Comp<(), usize> = builder.register(define_comp("sum", {
        let kv = kv.clone();
        let number_of_lines = number_of_lines.clone();
        let runs = sum_runs.clone();
        move |ctx, _: ()| {
            let kv = kv.clone();
            let number_of_lines = number_of_lines.clone();
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
    }));

    let store: Comp<(), ()> = builder.register(define_comp("store", {
        let sink = sink.clone();
        let sum = sum.clone();
        let runs = store_runs.clone();
        move |ctx, _: ()| {
            let sink = sink.clone();
            let sum = sum.clone();
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
    }));

    let engine = builder.build();
    engine.eval_root(&store, ()).await.unwrap();

    assert_eq!(sink.get("sum"), Some("6".to_string()));
    assert_eq!(nol_runs.load(Ordering::SeqCst), 3, "one run per file key");
    assert_eq!(sum_runs.load(Ordering::SeqCst), 1);
    assert_eq!(store_runs.load(Ordering::SeqCst), 1);
}

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

    let d: Comp<i32, i32> = builder.register(define_comp("diamond_d", {
        let runs = d_runs.clone();
        move |_ctx, n: i32| {
            let runs = runs.clone();
            async move {
                runs.fetch_add(1, Ordering::SeqCst);
                Ok(n + 1)
            }
        }
    }));

    let b: Comp<i32, i32> = builder.register(define_comp("diamond_b", {
        let d = d.clone();
        move |ctx, n: i32| {
            let d = d.clone();
            async move { ctx.eval(&d, n).await }
        }
    }));

    let c: Comp<i32, i32> = builder.register(define_comp("diamond_c", {
        let d = d.clone();
        move |ctx, n: i32| {
            let d = d.clone();
            async move { ctx.eval(&d, n).await }
        }
    }));

    let a: Comp<i32, i32> = builder.register(define_comp("diamond_a", {
        let b = b.clone();
        let c = c.clone();
        move |ctx, n: i32| {
            let b = b.clone();
            let c = c.clone();
            async move {
                let (bv, cv) = futures::try_join!(ctx.eval(&b, n), ctx.eval(&c, n))?;
                Ok(bv + cv)
            }
        }
    }));

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
    let comp: Comp<i32, i32> = builder.register(define_comp("slow", {
        let runs = runs.clone();
        move |_ctx, n: i32| {
            let runs = runs.clone();
            async move {
                runs.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(50)).await;
                Ok(n * 10)
            }
        }
    }));
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
    builder.register(define_comp("cycle_a", {
        let b = b.clone();
        move |ctx, n: i32| {
            let b = b.clone();
            async move { ctx.eval(&b, n).await }
        }
    }));
    builder.register(define_comp("cycle_b", {
        let a = a.clone();
        move |ctx, n: i32| {
            let a = a.clone();
            async move { ctx.eval(&a, n).await }
        }
    }));
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
    builder.register(define_comp("countdown_sum", {
        let countdown = countdown.clone();
        move |ctx, n: i64| {
            let countdown = countdown.clone();
            async move {
                if n <= 0 {
                    Ok(0)
                } else {
                    let rest = ctx.eval(&countdown, n - 1).await?;
                    Ok(n + rest)
                }
            }
        }
    }));
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
    let sleepy: Comp<i32, i32> = builder.register(define_comp("sleepy", |_ctx, n: i32| async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(n * 2)
    }));
    let root: Comp<(), Vec<i32>> = builder.register(define_comp("root_eval_all", {
        let sleepy = sleepy.clone();
        move |ctx, _: ()| {
            let sleepy = sleepy.clone();
            async move { ctx.eval_all(&sleepy, [1, 2, 3]).await }
        }
    }));
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
