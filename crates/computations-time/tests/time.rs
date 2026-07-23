//! Integration tests for `TimeSource`: rounding correctness, bucket-rollover
//! recomputation and one-shot `IsAfter` flips under `Engine::run`, and an
//! efficiency guard proving change notifications are boundary-driven rather
//! than polled.
//!
//! Real-time based: buckets/deadlines are kept short (100-500ms) and
//! timeouts generous, and every test is hang-proof via the `wait_until`
//! pattern from `crates/computations/tests/driver.rs`.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use computations::testutil::{VecSink, WriteDoc};
use computations::{Comp, Engine, Source, SourceBase};
use computations_time::{Bucket, RoundedTime, TimeKey, TimeSource};

/// Polls `f` every 10ms until it returns `true`, panicking if 10s pass
/// first — bounds every test against a propagation bug hanging `cargo test`.
async fn wait_until(f: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if f() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("condition did not become true within 10s");
}

/// Rounding correctness, exercised directly against `TimeSource::execute`
/// (no `Engine` involved): the rounded value is a multiple of the bucket
/// and `rounded <= now < rounded + bucket`; different buckets are different
/// keys.
#[tokio::test]
async fn rounding_correctness() {
    let src = TimeSource::new("time");
    let bucket_a = Bucket::new(Duration::from_millis(300)).unwrap();
    let bucket_b = Bucket::new(Duration::from_millis(700)).unwrap();

    let before = SystemTime::now();
    let (result, deps) = src.execute(RoundedTime(bucket_a)).await;
    let rounded = result.unwrap();
    let after = SystemTime::now();

    assert!(rounded <= before || rounded <= after, "rounded value must not be in the future");
    assert!(after < rounded + bucket_a.duration(), "rounded value must be within one bucket of now");
    let nanos = rounded.duration_since(UNIX_EPOCH).unwrap().as_nanos();
    assert_eq!(
        nanos % bucket_a.duration().as_nanos(),
        0,
        "rounded value must be an exact multiple of the bucket"
    );

    let dep = deps.into_iter().next().unwrap();
    assert_eq!(dep.key, TimeKey::Bucket(bucket_a));

    let (result_b, deps_b) = src.execute(RoundedTime(bucket_b)).await;
    result_b.unwrap();
    let dep_b = deps_b.into_iter().next().unwrap();
    assert_ne!(dep.key, dep_b.key, "different buckets must produce different keys");
}

/// A computation reading `RoundedTime(Bucket::new(300ms))` under
/// `Engine::run` must be re-run at every bucket rollover: driven entirely by
/// the timer task waking on boundary crossings, not by polling.
#[tokio::test]
async fn bucket_rollover_drives_recomputation() {
    let time_src = TimeSource::new("time");
    let sink = VecSink::new("docs");

    let mut builder = Engine::builder();
    builder.source(time_src.clone());
    builder.sink(sink.clone());

    let counter = Arc::new(AtomicUsize::new(0));
    let bucket = Bucket::new(Duration::from_millis(300)).unwrap();

    let env = (time_src.clone(), sink.clone(), counter.clone(), bucket);
    let root: Comp<(), ()> = builder.define_with("bucket_root", &env, |(time_src, sink, counter, bucket), ctx, _: ()| async move {
        counter.fetch_add(1, Ordering::SeqCst);
        let t = time_src.rounded(&ctx, bucket).await?;
        let millis = t.duration_since(UNIX_EPOCH).unwrap().as_millis();
        ctx.sink_req(
            &sink,
            WriteDoc {
                name: "doc".to_string(),
                content: millis.to_string(),
            },
        )
        .await?;
        Ok(())
    });

    let engine = builder.build();
    let handle = {
        let engine = engine.clone();
        tokio::spawn(async move { engine.run(root, ()).await })
    };

    wait_until(|| counter.load(Ordering::SeqCst) >= 1).await;

    // Allow generous slack: across ~2s with a 300ms bucket, at least 2
    // rollovers must have driven a rerun (not exact counts).
    tokio::time::sleep(Duration::from_millis(2000)).await;
    let runs = counter.load(Ordering::SeqCst);
    assert!(runs >= 2, "expected at least 2 runs within ~2s of 300ms bucket rollovers, got {runs}");

    handle.abort();
}

/// An `IsAfter(now + 300ms)` computation flips its written value from
/// `false` to `true` exactly once, then stays stable (no further reruns —
/// the one-shot property, and proof the fired deadline was dropped from the
/// schedule rather than kept around).
#[tokio::test]
async fn is_after_flips_once_then_stays_stable() {
    let time_src = TimeSource::new("time");
    let sink = VecSink::new("docs");

    let mut builder = Engine::builder();
    builder.source(time_src.clone());
    builder.sink(sink.clone());

    let counter = Arc::new(AtomicUsize::new(0));
    let deadline = SystemTime::now() + Duration::from_millis(300);

    let env = (time_src.clone(), sink.clone(), counter.clone(), deadline);
    let root: Comp<(), ()> = builder.define_with("is_after_root", &env, |(time_src, sink, counter, deadline), ctx, _: ()| async move {
        counter.fetch_add(1, Ordering::SeqCst);
        let after = time_src.is_after(&ctx, deadline).await?;
        ctx.sink_req(
            &sink,
            WriteDoc {
                name: "doc".to_string(),
                content: after.to_string(),
            },
        )
        .await?;
        Ok(())
    });

    let engine = builder.build();
    let handle = {
        let engine = engine.clone();
        tokio::spawn(async move { engine.run(root, ()).await })
    };

    wait_until(|| sink.get("doc").is_some()).await;
    assert_eq!(sink.get("doc"), Some("false".to_string()), "must start false: the deadline hasn't passed yet");

    wait_until(|| sink.get("doc").as_deref() == Some("true")).await;

    let stable_count = counter.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        counter.load(Ordering::SeqCst),
        stable_count,
        "no further reruns after the one-shot flip to true"
    );

    handle.abort();
}

/// An `IsAfter` deadline that is already in the past when first executed
/// must report `true` immediately and never enter the schedule (asserted
/// via run-counter stability: nothing should ever wake this computation up
/// again).
#[tokio::test]
async fn already_past_deadline_is_true_immediately_and_stable() {
    let time_src = TimeSource::new("time");
    let sink = VecSink::new("docs");

    let mut builder = Engine::builder();
    builder.source(time_src.clone());
    builder.sink(sink.clone());

    let counter = Arc::new(AtomicUsize::new(0));
    let deadline = SystemTime::now() - Duration::from_secs(3600);

    let env = (time_src.clone(), sink.clone(), counter.clone(), deadline);
    let root: Comp<(), ()> = builder.define_with(
        "past_deadline_root",
        &env,
        |(time_src, sink, counter, deadline), ctx, _: ()| async move {
            counter.fetch_add(1, Ordering::SeqCst);
            let after = time_src.is_after(&ctx, deadline).await?;
            ctx.sink_req(
                &sink,
                WriteDoc {
                    name: "doc".to_string(),
                    content: after.to_string(),
                },
            )
            .await?;
            Ok(())
        },
    );

    let engine = builder.build();
    let handle = {
        let engine = engine.clone();
        tokio::spawn(async move { engine.run(root, ()).await })
    };

    wait_until(|| sink.get("doc") == Some("true".to_string())).await;

    let stable_count = counter.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        counter.load(Ordering::SeqCst),
        stable_count,
        "an already-past deadline must never enter the schedule, so nothing should wake this up again"
    );

    handle.abort();
}

/// Efficiency guard: with one 200ms bucket watched, `wait_changes` must
/// receive roughly N ≈ 5 notifications over ~1s (one per boundary crossed),
/// not hundreds — proof the timer is boundary-driven, not a busy poll.
#[tokio::test]
async fn change_notifications_are_boundary_driven_not_polled() {
    let time_src = TimeSource::new("time");
    let bucket = Bucket::new(Duration::from_millis(200)).unwrap();

    let (result, _deps) = time_src.execute(RoundedTime(bucket)).await;
    result.unwrap();

    let window = Duration::from_millis(1000);
    let start = std::time::Instant::now();
    let mut count = 0usize;
    loop {
        let elapsed = start.elapsed();
        if elapsed >= window {
            break;
        }
        let remaining = window - elapsed;
        match tokio::time::timeout(remaining, time_src.wait_changes()).await {
            Ok(_changes) => count += 1,
            Err(_) => break,
        }
    }

    assert!(
        (3..=8).contains(&count),
        "expected ~5 boundary-driven notifications over 1s of a 200ms bucket, got {count}"
    );
}
