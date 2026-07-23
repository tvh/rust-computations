//! Definitions of memoized computations and their dependency declarations.
//!
//! A [`CompDef`] is a globally-named async function `Ctx -> P -> R` (the
//! paper's `c :: P -> CompM R`, given a name). Registering it with an
//! [`crate::engine::EngineBuilder`] yields a [`Comp`]: a cheap, cloneable
//! handle carrying only the definition's [`DefId`] plus the phantom `P`/`R`
//! types, with no body attached.
//!
//! Because a `Comp<P, R>` is just an id, it can be constructed independently
//! of (and before) the `CompDef` it names being built or registered. That is
//! what makes recursive and mutually-recursive definitions straightforward:
//! a body can close over a handle to its own computation, or to one defined
//! later, and only the name needs to be known up front. Registration order
//! never affects the correctness of a handle.

use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;

use futures::future::BoxFuture;

use crate::ctx::Ctx;
use crate::error::CompError;
use crate::key::{CompParam, CompResult, DefId};

/// The type-erased body of a computation definition.
pub(crate) type BodyFn<P, R> =
    Arc<dyn Fn(Ctx, P) -> BoxFuture<'static, Result<R, CompError>> + Send + Sync>;

/// A computation definition: a globally-named async function from `P` to
/// `R`, run with a [`Ctx`] that records its dependencies as it executes.
pub struct CompDef<P, R> {
    pub(crate) id: DefId,
    pub(crate) body: BodyFn<P, R>,
}

/// Defines a computation named `name` with the given body.
///
/// `name` must be globally unique among computations registered with the
/// same [`crate::engine::Engine`]; `EngineBuilder::register` panics on a
/// duplicate name.
pub fn define_comp<P, R, F, Fut>(name: &str, body: F) -> CompDef<P, R>
where
    P: CompParam,
    R: CompResult,
    F: Fn(Ctx, P) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<R, CompError>> + Send + 'static,
{
    CompDef {
        id: DefId::new(name),
        body: Arc::new(move |ctx, param| Box::pin(body(ctx, param))),
    }
}

/// A cheap, cloneable handle referring to a computation by name only.
///
/// A `Comp<P, R>` carries no body: cloning it is an `Arc` bump on the
/// underlying [`DefId`]. Using a handle whose `DefId` has no registered
/// (or type-matching) definition at evaluation time returns
/// [`CompError::Failed`] with a descriptive message, rather than panicking.
pub struct Comp<P, R> {
    def: DefId,
    _marker: PhantomData<fn(P) -> R>,
}

impl<P, R> Comp<P, R> {
    /// Creates a handle referring to the computation named `name`.
    ///
    /// This does not require a matching `CompDef` to exist yet, which is
    /// what allows (mutually) recursive definitions: build the handle(s)
    /// first, close over them in the bodies, then register the defs.
    pub fn named(name: &str) -> Self {
        Comp::from_def_id(DefId::new(name))
    }

    /// Creates a handle directly from an existing [`DefId`].
    pub(crate) fn from_def_id(def: DefId) -> Self {
        Comp {
            def,
            _marker: PhantomData,
        }
    }

    /// Returns the identity of the computation this handle refers to.
    pub fn def_id(&self) -> &DefId {
        &self.def
    }
}

// Hand-written rather than `#[derive(Clone)]`/`#[derive(Debug)]`: the
// derive macros would add spurious `P: Clone`/`R: Debug` bounds, but a
// `Comp` never actually stores a `P` or `R` (only a `PhantomData<fn(P) ->
// R>`, which is `Clone`/`Send`/`Sync` unconditionally).
impl<P, R> Clone for Comp<P, R> {
    fn clone(&self) -> Self {
        Comp {
            def: self.def.clone(),
            _marker: PhantomData,
        }
    }
}

impl<P, R> fmt::Debug for Comp<P, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Comp").field(&self.def).finish()
    }
}
