//! Flow-argument computations: the hand-written foundation a future
//! `#[computation]` proc-macro will generate into (Phase A of that work —
//! see `docs/persistence-benchmark-notes.md`'s Stage 9 for the design
//! rationale in full).
//!
//! Today, a computation is registered via a builder closure that *captures*
//! its source/sink handles (`EngineBuilder::define_with`); the macro's target
//! shape instead takes flow arguments (sources/sinks) directly in the
//! function signature, with no builder registration and no captures:
//!
//! ```ignore
//! #[computation]
//! async fn sync_file(ctx: &Ctx, #[flow] source: &Arc<FsSource>, #[flow] sink: &Arc<FsSink>, rel: PathBuf)
//!     -> Result<(), CompError> { /* ... */ }
//! ```
//!
//! Two things make that shape work without forking the engine:
//!
//! - **[`FlowId`] joins a flow argument's *instance* identity to the node's
//!   [`crate::key::CompKey`].** A flow (source or sink) is never hashed as
//!   an ordinary parameter — its *contents* are read/written through
//!   `Ctx::src_req`/`sink_req` exactly as today, dependency-tracked the same
//!   way — but its *instance id* still has to distinguish `sync_file(src_a,
//!   sink, rel)` from `sync_file(src_b, sink, rel)`. Without that, the two
//!   calls would collide on one node identity and silently serve each
//!   other's cached values (wrong output, not a crash) — see
//!   [`flow_aware_param_hash`].
//! - **The rerun path resolves flows from the registry, not from a stored
//!   closure.** A node revived from disk (or simply dirtied and re-run by
//!   `crate::driver`) has only ids, never live handles — a persisted
//!   `FlowId` is looked back up through [`crate::registry::Registry`] by
//!   [`FlowResolver`], typed and downcast on demand. [`FlowThunk`] is the
//!   uniform entry point both first execution and every rerun go through:
//!   a plain `fn` pointer (no captures at all), so a future
//!   `inventory::submit!`-based macro can hand one to
//!   [`crate::engine::EngineBuilder::define_flows`] as a plain value.

use std::any::Any;
use std::sync::{Arc, Mutex};

use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};

use crate::def::{ErasedDef, ValueColumn, column_read, column_write};
use crate::engine::EngineInner;
use crate::error::CompError;
use crate::key::{CompKey, CompResult, DefId, Hash128};
use crate::registry::Registry;
use crate::sink::{SinkBase, SinkId};
use crate::source::{SourceBase, SourceId};

/// The identity of one flow argument: a source or a sink instance, by its
/// stable [`SourceId`]/[`SinkId`] — never by its contents. See the [module
/// docs](self) for why this has to feed a flow-argument computation's
/// [`CompKey`] even though the flow's *contents* are never hashed.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FlowId {
    Source(SourceId),
    Sink(SinkId),
}

impl std::fmt::Display for FlowId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FlowId::Source(id) => write!(f, "source `{id}`"),
            FlowId::Sink(id) => write!(f, "sink `{id}`"),
        }
    }
}

/// Computes a flow-argument computation's parameter-hash contribution:
/// `def_name` itself is folded in separately (by [`CompKey`], exactly as it
/// already is for the builder path), so this only needs to combine the
/// node's ordered `flows` with its ordinary `param_bytes`.
///
/// **The empty-flows case is not a degenerate case of the general one — it
/// is a hard requirement.** When `flows` is empty this returns *exactly*
/// what [`crate::key::StableHash::stable_hash`]/[`CompKey::new`] already
/// compute for `param_bytes` alone: `blake3(param_bytes)`, truncated to 128
/// bits, with no flow-list contribution at all — not even an empty-list
/// marker byte. This is what keeps every builder-path (`define`/`define_with`)
/// hash bit-for-bit unchanged by this feature's existence: the builder path
/// never calls this function (it still goes through `CompKey::new`
/// directly), but *if* it ever needed to be expressed in terms of this one
/// unified formula, the zero-flows branch below proves it would compute the
/// identical bytes. See `tests::flow_hash_with_no_flows_matches_plain_param_hash`
/// for the test that pins this down directly against `CompKey::new`'s own
/// output. Getting this wrong (e.g. always encoding `postcard(flows)` even
/// when `flows` is empty) would silently invalidate every persisted record
/// ever written under the plain builder path, since postcard's own
/// length-prefix for an empty `Vec` is a real, non-empty byte sequence.
pub(crate) fn flow_aware_param_hash(flows: &[FlowId], param_bytes: &[u8]) -> Hash128 {
    if flows.is_empty() {
        return Hash128::from_blake3(blake3::hash(param_bytes));
    }
    // `postcard::to_stdvec(flows)` is itself self-delimiting (a `Vec`
    // serializes with a length prefix), so concatenating it with
    // `param_bytes` before hashing can never produce the same bytes for two
    // different (flows, param) splits -- no extra separator needed.
    let flows_bytes =
        postcard::to_stdvec(flows).expect("postcard serialization of a well-formed FlowId list should not fail");
    let mut hasher = blake3::Hasher::new();
    hasher.update(&flows_bytes);
    hasher.update(param_bytes);
    Hash128::from_blake3(hasher.finalize())
}

/// Resolves a flow-argument computation's ordered [`FlowId`]s against a live
/// [`Registry`], typed and downcast on demand.
///
/// Built fresh for every execution (first run or rerun alike) from whatever
/// `flows` a node's identity carries — never stored anywhere itself, so a
/// [`FlowThunk`] never captures anything beyond the plain bytes/ids it's
/// handed.
pub struct FlowResolver<'a> {
    registry: &'a Registry,
    flows: &'a [FlowId],
}

impl<'a> FlowResolver<'a> {
    pub(crate) fn new(registry: &'a Registry, flows: &'a [FlowId]) -> Self {
        FlowResolver { registry, flows }
    }

    /// Resolves flow argument `idx` as a source of concrete type `S`.
    ///
    /// # Errors
    /// A loud, diagnosable [`CompError::Failed`] — naming both the flow
    /// index and, where known, the offending id/kind — for every failure
    /// mode: `idx` out of range, the flow at `idx` is a sink rather than a
    /// source, the id isn't registered in this engine's [`Registry`] at
    /// all, or it's registered under a different concrete type than `S`.
    /// Never panics: a stale or mismatched flow id is exactly the kind of
    /// "the world moved on since this was persisted" condition this crate
    /// otherwise always turns into a recoverable error (see
    /// `crate::persist`'s module docs).
    pub fn source<S: SourceBase>(&self, idx: usize) -> Result<Arc<S>, CompError> {
        let flow = self.flows.get(idx).ok_or_else(|| {
            CompError::Failed(format!(
                "flow argument {idx}: this computation was called with only {} flow(s)",
                self.flows.len()
            ))
        })?;
        let FlowId::Source(id) = flow else {
            return Err(CompError::Failed(format!(
                "flow argument {idx}: expected a source, but this node's recorded flow is a {flow}"
            )));
        };
        self.registry.source_typed::<S>(id).ok_or_else(|| {
            CompError::Failed(format!(
                "flow argument {idx}: source `{id}` is not registered in this engine, or is registered \
                 under a different concrete type than this computation expects"
            ))
        })
    }

    /// Resolves flow argument `idx` as a sink of concrete type `S` — see
    /// [`Self::source`] for the error contract.
    pub fn sink<S: SinkBase>(&self, idx: usize) -> Result<Arc<S>, CompError> {
        let flow = self.flows.get(idx).ok_or_else(|| {
            CompError::Failed(format!(
                "flow argument {idx}: this computation was called with only {} flow(s)",
                self.flows.len()
            ))
        })?;
        let FlowId::Sink(id) = flow else {
            return Err(CompError::Failed(format!(
                "flow argument {idx}: expected a sink, but this node's recorded flow is a {flow}"
            )));
        };
        self.registry.sink_typed::<S>(id).ok_or_else(|| {
            CompError::Failed(format!(
                "flow argument {idx}: sink `{id}` is not registered in this engine, or is registered \
                 under a different concrete type than this computation expects"
            ))
        })
    }
}

/// The type-erased body of a flow-argument computation: decodes
/// `param_bytes` as its own (statically known only to itself, never to the
/// engine) parameter type, resolves whatever flows it needs via the given
/// [`FlowResolver`], runs the real body, and boxes the (also statically
/// known only to itself) result as an erased `Arc<dyn Any>`.
///
/// A plain `fn` pointer, not a closure: it captures nothing, so both first
/// execution and every rerun call the exact same value — nothing is ever
/// stored per-node beyond the bytes/ids [`crate::engine::EngineInner::eval_flows`]
/// already keeps track of. This is also deliberately a plain value (`fn`
/// pointers are `Copy` and `'static`) rather than a boxed trait object,
/// since Phase B's `#[computation]` macro will hand one to
/// [`crate::engine::EngineBuilder::define_flows`] via `inventory::submit!` —
/// a plain value is exactly what that needs to collect at link time.
pub type FlowThunk =
    fn(crate::ctx::Ctx, FlowResolver<'_>, &[u8]) -> BoxFuture<'static, Result<Arc<dyn Any + Send + Sync>, CompError>>;

/// A flow-argument computation definition: a globally-named [`FlowThunk`]
/// plus its own typed value column (see [`crate::def::CompDef`]'s docs for
/// why the value column lives where `R` is statically known, and
/// `column_read`/`column_write` for the storage the two share).
///
/// Deliberately generic over `R` alone, not `P`: unlike [`crate::def::CompDef<P,
/// R>`], a flow-argument def's parameter type is never named at the engine
/// level at all — [`FlowThunk`] takes and decodes raw `param_bytes` itself,
/// so the engine only ever needs to know how to hash/store/rerun a node,
/// never what its parameter's Rust type actually is. `P` only appears one
/// layer up, at [`crate::ctx::Ctx::eval_flows`]'s public call sites (the
/// macro-generated wrapper function), purely to serialize the argument the
/// caller already has in hand.
pub(crate) struct FlowCompDef<R> {
    pub(crate) id: DefId,
    pub(crate) thunk: FlowThunk,
    values: Mutex<Vec<Option<R>>>,
}

impl<R> FlowCompDef<R> {
    pub(crate) fn new(id: DefId, thunk: FlowThunk) -> Self {
        FlowCompDef {
            id,
            thunk,
            values: Mutex::new(Vec::new()),
        }
    }
}

impl<R: Clone> FlowCompDef<R> {
    fn read_value(&self, row: u32) -> Option<R> {
        column_read(&self.values, row)
    }

    fn write_value(&self, row: u32, value: R) {
        column_write(&self.values, row, value);
    }
}

impl<R: Clone + Send + Sync> ValueColumn<R> for FlowCompDef<R> {
    fn read_value(&self, row: u32) -> Option<R> {
        FlowCompDef::read_value(self, row)
    }

    fn write_value(&self, row: u32, value: R) {
        FlowCompDef::write_value(self, row, value);
    }
}

/// Adapts a concrete `FlowCompDef<R>` to the object-safe [`ErasedDef`]
/// interface — the flow-argument counterpart of `crate::def::DefAdapter`.
///
/// Constructed by `crate::engine::EngineBuilder::define_flows`.
pub(crate) struct FlowDefAdapter<R>(pub Arc<FlowCompDef<R>>);

impl<R: CompResult> ErasedDef for FlowDefAdapter<R> {
    fn revive_key(&self, _param_bytes: &[u8]) -> Option<CompKey> {
        // A flow-argument record is never revived through the flow-less
        // path: `param_bytes` alone can't reconstruct its key (the flow ids
        // matter too -- see `flow_aware_param_hash`). Returning `None`
        // unconditionally routes every caller (`crate::persist::restore_nodes`,
        // `crate::engine::EngineInner::rerun_node`) to `Self::revive_key_flows`
        // instead, regardless of whether this particular node happens to
        // have zero flows -- so that edge case is never mistaken for an
        // ordinary builder-path record.
        None
    }

    fn value_any(&self, row: u32) -> Option<Arc<dyn Any + Send + Sync>> {
        self.0.read_value(row).map(|v| Arc::new(v) as Arc<dyn Any + Send + Sync>)
    }

    fn revive_and_store(&self, row: u32, value_bytes: &[u8]) -> bool {
        match postcard::from_bytes::<R>(value_bytes) {
            Ok(value) => {
                self.0.write_value(row, value);
                true
            }
            Err(_) => false,
        }
    }

    fn serialize_value(&self, value: &Arc<dyn Any + Send + Sync>) -> Result<Vec<u8>, String> {
        let typed: &R = value
            .downcast_ref::<R>()
            .expect("serialize_value: value's concrete type must match this definition's result type");
        postcard::to_stdvec(typed).map_err(|e| e.to_string())
    }

    fn rerun(&self, _engine: Arc<EngineInner>, _param_bytes: &[u8]) -> Option<BoxFuture<'static, Result<(), CompError>>> {
        // See `Self::revive_key`: the flow-less rerun path never applies to
        // a flow-argument def -- `Self::rerun_flows` is the one that does.
        None
    }

    fn revive_key_flows(&self, flows: &[FlowId], param_bytes: &[u8]) -> Option<CompKey> {
        Some(CompKey::from_parts(self.0.id, flow_aware_param_hash(flows, param_bytes)))
    }

    fn rerun_flows(
        &self,
        engine: Arc<EngineInner>,
        flows: &[FlowId],
        param_bytes: &[u8],
    ) -> Option<BoxFuture<'static, Result<(), CompError>>> {
        let def = self.0.clone();
        let flows = flows.to_vec();
        let param_bytes = param_bytes.to_vec();
        Some(Box::pin(async move {
            engine.eval_flows_erased::<R>(def, &flows, param_bytes, Arc::new(Vec::new())).await
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key::{CompKey, DefId, StableHash};

    /// The identity requirement Stage 9 exists to protect (see the module
    /// docs and [`flow_aware_param_hash`]'s own docs): with zero flows, the
    /// unified flow-aware hash formula must produce *exactly* the same bytes
    /// as the plain builder path's own [`CompKey::new`] / `StableHash`
    /// hashing already does — not merely an equivalent-looking hash, the
    /// literal same `Hash128`. If this test ever fails, every existing
    /// persisted database silently stops matching its own records the
    /// moment this crate is rebuilt.
    #[test]
    fn flow_hash_with_no_flows_matches_plain_param_hash() {
        #[derive(serde::Serialize)]
        struct Param {
            a: i32,
            b: String,
        }
        let param = Param { a: 42, b: "hello".to_string() };
        let param_bytes = postcard::to_stdvec(&param).unwrap();

        let via_flow_path = flow_aware_param_hash(&[], &param_bytes);
        let via_stable_hash = param.stable_hash();
        assert_eq!(via_flow_path, via_stable_hash, "zero-flows hash must equal StableHash::stable_hash bit-for-bit");

        let via_comp_key = CompKey::new(DefId::new("flow_hash_probe"), &param).param_hash();
        assert_eq!(via_flow_path, via_comp_key, "zero-flows hash must equal CompKey::new's own param hash bit-for-bit");
    }

    /// A non-empty flow list must change the hash relative to the same
    /// param with no flows at all — otherwise two differently-flowed calls
    /// to the same computation would collide on one node identity (the
    /// silent-corruption case this whole feature exists to prevent).
    #[test]
    fn flow_hash_with_flows_differs_from_no_flows() {
        let param_bytes = postcard::to_stdvec(&"same param").unwrap();
        let no_flows = flow_aware_param_hash(&[], &param_bytes);
        let one_flow = flow_aware_param_hash(&[FlowId::Source(SourceId::new("a"))], &param_bytes);
        assert_ne!(no_flows, one_flow);
    }

    /// Two different flow instances (same computation, same param) must
    /// hash to two different identities — this is the direct silent-
    /// corruption regression the module docs describe:
    /// `sync_file(src_a, sink, rel)` vs. `sync_file(src_b, sink, rel)`.
    #[test]
    fn flow_hash_differs_across_flow_instances() {
        let param_bytes = postcard::to_stdvec(&"rel/path").unwrap();
        let with_a = flow_aware_param_hash(&[FlowId::Source(SourceId::new("src_a"))], &param_bytes);
        let with_b = flow_aware_param_hash(&[FlowId::Source(SourceId::new("src_b"))], &param_bytes);
        assert_ne!(with_a, with_b);
    }
}
