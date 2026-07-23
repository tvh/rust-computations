//! Smoke test for the `comp.eval` tracing instrumentation: installs a
//! capturing subscriber around a couple of evaluations and checks that the
//! rendered output actually mentions the computation's name and an
//! `executed`/`cache_hit` outcome. Not a format-stability test — just proof
//! that the events fire on the expected paths.

use std::sync::{Arc, Mutex};

use computations::{Comp, Engine, define_comp};
use tracing_subscriber::fmt::MakeWriter;

/// A `Write` sink that appends into a shared, lockable buffer, so the test
/// can inspect what the `fmt` subscriber rendered after the fact.
#[derive(Clone, Default)]
struct SharedBuf(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for SharedBuf {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for SharedBuf {
    type Writer = SharedBuf;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

#[test]
fn comp_eval_events_mention_comp_name_and_outcome() {
    let buf = SharedBuf::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(buf.clone())
        .with_max_level(tracing::Level::DEBUG)
        .with_ansi(false)
        .finish();

    tracing::subscriber::with_default(subscriber, || {
        // A fresh current-thread runtime, not `#[tokio::test]`: the whole
        // point is to run the evaluations *inside* this closure so
        // `with_default` is actually the active subscriber while they run,
        // and nesting a tokio runtime inside another isn't allowed.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut builder = Engine::builder();
            let answer: Comp<(), i32> = builder.register(define_comp("smoke_answer", |_ctx, _: ()| async {
                Ok(42)
            }));
            let engine = builder.build();

            // First call: no cached value yet, so it executes.
            assert_eq!(engine.eval_root(&answer, ()).await.unwrap(), 42);
            // Second call: the node is clean, so this is a cache hit.
            assert_eq!(engine.eval_root(&answer, ()).await.unwrap(), 42);
        });
    });

    let output = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
    assert!(
        output.contains("smoke_answer"),
        "tracing output should mention the computation's name, got:\n{output}"
    );
    assert!(
        output.contains("outcome=executed") || output.contains("outcome=\"executed\""),
        "tracing output should show an `executed` outcome, got:\n{output}"
    );
    assert!(
        output.contains("outcome=cache_hit") || output.contains("outcome=\"cache_hit\""),
        "tracing output should show a `cache_hit` outcome, got:\n{output}"
    );
}
