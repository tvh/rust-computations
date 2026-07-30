//! The evaluation context handed to a running computation (the paper's
//! `CompM` monad).
//!
//! A [`Ctx`] is how a computation body calls other computations, reads
//! sources, and writes sinks. Every such call is recorded as a dependency of
//! the computation currently executing (`self.caller`), which is what lets
//! the engine rebuild its dynamic dependency graph on every run.

use std::collections::HashSet;
use std::sync::Arc;

use crate::def::Comp;
use crate::engine::EngineInner;
use crate::error::CompError;
use crate::flow::FlowId;
use crate::key::{CompKey, CompParam, CompResult};
use crate::sink::{RawOutput, Sink};
use crate::source::{Request, Source, raw_deps};

/// The context passed to a running computation body.
///
/// Cloning a `Ctx` is cheap: an `Arc` bump on the engine handle plus an
/// `Arc` bump on the shared ancestor chain (used for cycle detection).
#[derive(Clone)]
pub struct Ctx {
    pub(crate) engine: Arc<EngineInner>,
    /// The key of the computation currently executing on whose behalf this
    /// `Ctx`'s calls are recorded. `None` for a root evaluation (see
    /// [`crate::engine::Engine::eval_root`]): such calls still execute, but
    /// record no dependency (there is no computation to record it against).
    pub(crate) caller: Option<CompKey>,
    /// Every ancestor `CompKey` on the call stack that led here, including
    /// `caller` itself. Used to detect dependency cycles before they can
    /// deadlock the single-flight machinery.
    pub(crate) chain: Arc<Vec<CompKey>>,
}

impl Ctx {
    /// Calls another computation, applying `param`.
    ///
    /// The call is memoized (a clean, previously-computed result is reused)
    /// and single-flight-deduplicated (concurrent calls to the same
    /// application share one execution). A dependency from the currently
    /// executing computation to `comp` applied to `param` is recorded.
    ///
    /// Returns [`CompError::Cycle`] promptly, without awaiting anything
    /// that could deadlock, if `comp` applied to `param` is already an
    /// ancestor of this call.
    pub async fn eval<P: CompParam, R: CompResult>(
        &self,
        comp: Comp<P, R>,
        param: P,
    ) -> Result<R, CompError> {
        let (value, callee_key) = self
            .engine
            .eval::<P, R>(*comp.def_id(), param, self.chain.clone())
            .await?;

        match &self.caller {
            Some(caller_key) => self.engine.record_call_dep(caller_key, &callee_key),
            None => self.engine.mark_root(callee_key),
        }

        Ok(value)
    }

    /// Calls the flow-argument computation named `name`, applied to
    /// `flows`/`param` (Stage 9 — see `docs/persistence-benchmark-notes.md`'s
    /// design rationale in full). This is what a `#[computation]`-generated
    /// wrapper function will call internally, and what this crate's own
    /// hand-written flow tests call directly to exercise the same shape by
    /// hand.
    ///
    /// Identical memoization/single-flight/dependency-recording contract as
    /// [`Self::eval`] — same cache-hit/join/run algorithm, same call-dep
    /// recording against the currently executing computation (or
    /// [`crate::engine::EngineInner::mark_root`] for a root call) — the only
    /// difference is that identity here is named by string plus an ordered
    /// [`FlowId`] list rather than by a registration-backed [`Comp<P, R>`]
    /// handle, since a flow-argument computation's parameter type is never
    /// attached to a handle in the first place (see
    /// [`crate::flow::FlowCompDef`]'s docs). `flows`' ids (not their
    /// contents) participate in this call's identity — see
    /// [`crate::flow`]'s module docs for why that is a correctness
    /// requirement, not an optimization.
    pub async fn eval_flows<P: CompParam, R: CompResult>(
        &self,
        name: &'static str,
        flows: &[FlowId],
        param: P,
    ) -> Result<R, CompError> {
        let (value, callee_key) = self.engine.eval_flows::<P, R>(name, flows, param, self.chain.clone()).await?;

        match &self.caller {
            Some(caller_key) => self.engine.record_call_dep(caller_key, &callee_key),
            None => self.engine.mark_root(callee_key),
        }

        Ok(value)
    }

    /// Evaluates `comp` concurrently over every param in `params`,
    /// preserving their order in the returned `Vec`.
    pub async fn eval_all<P: CompParam, R: CompResult>(
        &self,
        comp: Comp<P, R>,
        params: impl IntoIterator<Item = P>,
    ) -> Result<Vec<R>, CompError> {
        let futs = params.into_iter().map(|p| self.eval(comp, p));
        futures::future::try_join_all(futs).await
    }

    /// Reads from a source by executing `req`, recording the deps it
    /// reports against the currently executing computation.
    pub async fn src_req<S, R>(&self, source: &Arc<S>, req: R) -> Result<R::Output, CompError>
    where
        S: Source<R>,
        R: Request,
    {
        let (result, deps) = source.execute(req).await;
        let result = result?;
        if let Some(caller_key) = &self.caller {
            let raw = raw_deps(&source.instance_id(), &deps);
            self.engine.record_source_deps(caller_key, raw);
        }
        Ok(result)
    }

    /// Writes to a sink by executing `req`, recording the outputs it
    /// reports against the currently executing computation.
    pub async fn sink_req<S, R>(&self, sink: &Arc<S>, req: R) -> Result<R::Output, CompError>
    where
        S: Sink<R>,
        R: Request,
    {
        let (outs, result) = sink.execute(req).await;
        let result = result?;
        if let Some(caller_key) = &self.caller {
            let raw: HashSet<RawOutput> = outs
                .iter()
                .map(|out| RawOutput {
                    sink: sink.instance_id(),
                    out: postcard::to_stdvec(out)
                        .expect("postcard serialization of a well-formed value should not fail"),
                })
                .collect();
            self.engine.record_outputs(caller_key, raw);
        }
        Ok(result)
    }
}
