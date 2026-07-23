//! Definitions of memoized computations and their dependency declarations.
//!
//! A [`CompDef`] is a globally-named async function `Ctx -> P -> R` (the
//! paper's `c :: P -> CompM R`, given a name). Registering it with an
//! [`crate::engine::EngineBuilder`] yields a [`Comp`]: a cheap, `Copy`
//! handle carrying only the definition's [`DefId`] plus the phantom `P`/`R`
//! types, with no body attached.
//!
//! Because a `Comp<P, R>` is just an id, it can be constructed independently
//! of (and before) the `CompDef` it names being built or registered. That is
//! what makes recursive and mutually-recursive definitions straightforward:
//! a body can close over a handle to its own computation, or to one defined
//! later, and only the name needs to be known up front. Registration order
//! never affects the correctness of a handle.

use std::any::Any;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;

use futures::future::BoxFuture;

use crate::ctx::Ctx;
use crate::engine::{EngineInner, RerunFn};
use crate::error::CompError;
use crate::key::{CompKey, CompParam, CompResult, DefId};

/// The type-erased body of a computation definition.
pub(crate) type BodyFn<P, R> =
    Arc<dyn Fn(Ctx, P) -> BoxFuture<'static, Result<R, CompError>> + Send + Sync>;

/// A computation definition: a globally-named async function from `P` to
/// `R`, run with a [`Ctx`] that records its dependencies as it executes.
pub struct CompDef<P, R> {
    pub(crate) id: DefId,
    pub(crate) body: BodyFn<P, R>,
}

/// Object-safe, byte-erased revival operations for a registered
/// [`CompDef`], used by `crate::persist` to restore a node from a persisted
/// record without knowing its concrete `P`/`R` types.
///
/// The def.rs analogue of [`crate::source::ErasedSource`] /
/// [`crate::source::SourceAdapter`]: [`DefAdapter`] adapts a concrete
/// `CompDef<P, R>` to this object-safe interface, postcard-decoding bytes at
/// the boundary. Built once per definition, alongside `defs`, by
/// `EngineBuilder::register`.
pub(crate) trait ErasedDef: Send + Sync {
    /// The "param reviver": decodes `param_bytes` as this definition's
    /// param type and, on success, returns the `CompKey` it identifies
    /// (recomputed by hashing the decoded param, exactly as a live
    /// [`CompKey::new`] would), a `rerun` closure for it (byte-for-byte the
    /// same closure shape [`EngineInner::make_rerun`] builds for a live
    /// node), and its `Debug`-rendered param string (mirroring the
    /// `param_debug` a live node's `prepare()` computes).
    ///
    /// Returns `None` if `param_bytes` fails to decode as this definition's
    /// param type — a corrupt or stale record, dropped by the caller rather
    /// than treated as fatal.
    fn revive_param(&self, engine: &Arc<EngineInner>, param_bytes: &[u8]) -> Option<(CompKey, RerunFn, String)>;

    /// The "value reviver": decodes `value_bytes` as this definition's
    /// result type, erased. Returns `None` if the bytes fail to decode (a
    /// corrupt or stale record).
    fn revive_value(&self, value_bytes: &[u8]) -> Option<Arc<dyn Any + Send + Sync>>;
}

/// Adapts a concrete `CompDef<P, R>` to the erased [`ErasedDef`] interface.
///
/// Constructed by `EngineBuilder::register`.
pub(crate) struct DefAdapter<P, R>(pub Arc<CompDef<P, R>>);

impl<P: CompParam, R: CompResult> ErasedDef for DefAdapter<P, R> {
    fn revive_param(&self, engine: &Arc<EngineInner>, param_bytes: &[u8]) -> Option<(CompKey, RerunFn, String)> {
        let param: P = postcard::from_bytes(param_bytes).ok()?;
        let key = CompKey::new(self.0.id, &param);
        let param_debug = format!("{param:?}");
        let rerun = engine.make_rerun::<P, R>(self.0.id, param);
        Some((key, rerun, param_debug))
    }

    fn revive_value(&self, value_bytes: &[u8]) -> Option<Arc<dyn Any + Send + Sync>> {
        let value: R = postcard::from_bytes(value_bytes).ok()?;
        Some(Arc::new(value) as Arc<dyn Any + Send + Sync>)
    }
}

/// Defines a computation named `name` with the given body.
///
/// `name` must be globally unique among computations registered with the
/// same [`crate::engine::Engine`]; `EngineBuilder::register` panics on a
/// duplicate name.
pub fn define_comp<P, R, F, Fut>(name: &'static str, body: F) -> CompDef<P, R>
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

/// Defines a computation named `name` whose body receives a handle to
/// itself (`this`) as its first argument.
///
/// This is [`define_comp`] plus the recursion boilerplate: instead of
/// writing `Comp::named("x")` once and `define_comp("x", ...)` separately —
/// two places that must repeat the same string literal, silently drifting
/// apart if one is ever renamed without the other — `define_comp_rec` builds
/// the `Comp::named(name)` handle itself and hands a copy of it to `body`
/// on every invocation. `Comp::named` (and thus mutual, as opposed to
/// self, recursion) is still available directly for the rarer case of two
/// or more computations that call each other.
///
/// # Example
///
/// ```
/// use computations::{Ctx, Engine, define_comp_rec};
///
/// # async fn example() -> Result<(), computations::error::CompError> {
/// let mut builder = Engine::builder();
/// let countdown_sum = builder.register(define_comp_rec(
///     "countdown_sum",
///     |this, ctx: Ctx, n: i64| async move {
///         if n <= 0 {
///             Ok(0)
///         } else {
///             let rest = ctx.eval(this, n - 1).await?;
///             Ok(n + rest)
///         }
///     },
/// ));
/// let engine = builder.build();
/// assert_eq!(engine.eval_root(&countdown_sum, 5).await?, 15); // 5+4+3+2+1+0
/// # Ok(())
/// # }
/// ```
pub fn define_comp_rec<P, R, F, Fut>(name: &'static str, body: F) -> CompDef<P, R>
where
    P: CompParam,
    R: CompResult,
    F: Fn(Comp<P, R>, Ctx, P) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<R, CompError>> + Send + 'static,
{
    let this = Comp::named(name);
    let id = *this.def_id();
    CompDef {
        id,
        body: Arc::new(move |ctx, param| Box::pin(body(this, ctx, param))),
    }
}

/// Defines a computation named `name` whose body is threaded a shared
/// environment `env` on every invocation, in addition to the usual `Ctx`
/// and parameter.
///
/// This is [`define_comp`] plus one extra argument: instead of `Fn(Ctx, P)
/// -> Fut`, the body is `Fn(E, Ctx, P) -> Fut`, and `env` is cloned once per
/// call so the body always gets an owned value rather than a reference. It
/// exists because a `CompDef`'s body must be `'static` and callable
/// repeatedly, which otherwise forces callers to hand-write a two-layer
/// clone dance: clone captured handles into the outer closure, then clone
/// them again into the inner `async move` block for every call. Taking
/// `env` once here and cloning it internally removes that boilerplate from
/// the call site.
///
/// `env` is typically a tuple of cheaply-cloneable handles (`Arc<...>`
/// sources/sinks, config values, `PathBuf` roots, ...); destructure it
/// directly in the closure's parameter list, e.g. `|(source, sink, root),
/// ctx, rel| { ... }`.
///
/// `define_comp_with` takes `env` by reference (`&E`, cloned once here to
/// build the stored closure) rather than by value, which supports two call
/// styles:
///
/// - **Shared across several definitions**: build `let env = (...);` once,
///   then pass `&env` to each `define_comp_with` call that needs it — no
///   `.clone()` appears anywhere in the call site.
/// - **Single-use, inline**: pass a temporary directly, e.g.
///   `define_comp_with("x", &(a, b, c), |...| ...)`. This moves `a`, `b`,
///   `c` into the anonymous temporary, so it only makes sense when no other
///   definition needs those values afterward.
///
/// The per-invocation `env.clone()` this performs internally is cheap for an
/// `Arc`-based env (a refcount bump), not a deep copy.
///
/// # Example
///
/// ```
/// use computations::{Ctx, Engine, define_comp_with};
///
/// # async fn example() -> Result<(), computations::error::CompError> {
/// let prefix = "hello, ".to_string();
/// let mut builder = Engine::builder();
/// let greet = builder.register(define_comp_with(
///     "greet",
///     &prefix,
///     |prefix: String, _ctx: Ctx, name: String| async move { Ok(format!("{prefix}{name}")) },
/// ));
/// let engine = builder.build();
/// assert_eq!(engine.eval_root(&greet, "world".to_string()).await?, "hello, world");
/// # Ok(())
/// # }
/// ```
pub fn define_comp_with<E, P, R, F, Fut>(name: &'static str, env: &E, body: F) -> CompDef<P, R>
where
    E: Clone + Send + Sync + 'static,
    P: CompParam,
    R: CompResult,
    F: Fn(E, Ctx, P) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<R, CompError>> + Send + 'static,
{
    let env = env.clone();
    CompDef {
        id: DefId::new(name),
        body: Arc::new(move |ctx, param| {
            let env = env.clone();
            Box::pin(body(env, ctx, param))
        }),
    }
}

/// Defines a self-recursive computation named `name` (see [`define_comp_rec`])
/// whose body is additionally threaded a shared environment `env` on every
/// invocation (see [`define_comp_with`]).
///
/// The body's signature is `Fn(E, Comp<P, R>, Ctx, P) -> Fut`: an owned clone
/// of `env`, then the working handle to the computation's own definition,
/// then the usual `Ctx`/param pair. See [`define_comp_with`] for the two
/// `&env` call styles (shared-across-definitions vs. single-use inline) and
/// why `env` is cloned once per invocation rather than passed by reference.
pub fn define_comp_rec_with<E, P, R, F, Fut>(name: &'static str, env: &E, body: F) -> CompDef<P, R>
where
    E: Clone + Send + Sync + 'static,
    P: CompParam,
    R: CompResult,
    F: Fn(E, Comp<P, R>, Ctx, P) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<R, CompError>> + Send + 'static,
{
    let this = Comp::named(name);
    let id = *this.def_id();
    let env = env.clone();
    CompDef {
        id,
        body: Arc::new(move |ctx, param| {
            let env = env.clone();
            Box::pin(body(env, this, ctx, param))
        }),
    }
}

/// A cheap, `Copy` handle referring to a computation by name only.
///
/// A `Comp<P, R>` carries no body: copying it is just copying its
/// underlying [`DefId`] (a `&'static str`). Using a handle whose `DefId` has
/// no registered (or type-matching) definition at evaluation time returns
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
    pub fn named(name: &'static str) -> Self {
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

// Hand-written rather than `#[derive(Clone, Copy)]`/`#[derive(Debug)]`: the
// derive macros would add spurious `P: Clone`/`P: Copy`/`R: Debug` bounds,
// but a `Comp` never actually stores a `P` or `R` (only a `PhantomData<fn(P)
// -> R>`, which is `Clone`/`Copy`/`Send`/`Sync` unconditionally).
impl<P, R> Clone for Comp<P, R> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<P, R> Copy for Comp<P, R> {}

impl<P, R> fmt::Debug for Comp<P, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Comp").field(&self.def).finish()
    }
}
