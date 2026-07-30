//! A startup-time registry of pluggable sources and sinks.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use crate::sink::{ErasedSink, SinkAdapter, SinkBase, SinkId};
use crate::source::{ErasedSource, SourceAdapter, SourceBase, SourceId};

/// Holds every source and sink instance wired into a driver at startup.
///
/// Each source/sink is stored twice, under the same id: once behind its
/// object-safe erased trait (`sources`/`sinks`, unchanged since Stage 0 —
/// what the driver and `Ctx::src_req`/`sink_req` use, since they only ever
/// need untyped operations) and once as a plain `Arc<dyn Any + Send +
/// Sync>` holding the *original* `Arc<S>` (`source_typed`/`sink_typed`,
/// added for flow-argument computations — see [`crate::flow`]). The second
/// copy exists because a revived flow-argument node has only a
/// [`crate::flow::FlowId`] to go on (no live handle survives a rerun or a
/// restart), so [`crate::flow::FlowResolver`] must be able to look a
/// concrete `Arc<S>` back up by id and downcast to the caller's expected
/// `S` — something the erased `dyn ErasedSource`/`dyn ErasedSink` objects
/// can't do (they were never `Any`). Both copies are the same `Arc`
/// underneath (a refcount bump each, not a clone of the source/sink
/// itself), so this doubles pointer bookkeeping only, not real state.
#[derive(Default)]
pub struct Registry {
    sources: HashMap<SourceId, Arc<dyn ErasedSource>>,
    source_typed: HashMap<SourceId, Arc<dyn Any + Send + Sync>>,
    sinks: HashMap<SinkId, Arc<dyn ErasedSink>>,
    sink_typed: HashMap<SinkId, Arc<dyn Any + Send + Sync>>,
}

impl Registry {
    /// Registers a source instance under its [`SourceBase::instance_id`].
    ///
    /// # Panics
    /// Panics if a source with the same id is already registered — this is
    /// a startup configuration error, not a runtime condition to recover from.
    pub fn register_source<S: SourceBase>(&mut self, src: Arc<S>) {
        let id = src.instance_id();
        let prev = self
            .sources
            .insert(id.clone(), Arc::new(SourceAdapter(src.clone())));
        assert!(prev.is_none(), "duplicate source id: {id}");
        self.source_typed.insert(id, src as Arc<dyn Any + Send + Sync>);
    }

    /// Registers a sink instance under its [`SinkBase::instance_id`].
    ///
    /// # Panics
    /// Panics if a sink with the same id is already registered — this is a
    /// startup configuration error, not a runtime condition to recover from.
    pub fn register_sink<S: SinkBase>(&mut self, sink: Arc<S>) {
        let id = sink.instance_id();
        let prev = self.sinks.insert(id.clone(), Arc::new(SinkAdapter(sink.clone())));
        assert!(prev.is_none(), "duplicate sink id: {id}");
        self.sink_typed.insert(id, sink as Arc<dyn Any + Send + Sync>);
    }

    /// Iterates over every registered source, erased.
    pub(crate) fn sources(&self) -> impl Iterator<Item = &Arc<dyn ErasedSource>> {
        self.sources.values()
    }

    /// Iterates over every registered sink, erased.
    pub(crate) fn sinks(&self) -> impl Iterator<Item = &Arc<dyn ErasedSink>> {
        self.sinks.values()
    }

    /// Looks up a registered source by id.
    pub(crate) fn source(&self, id: &SourceId) -> Option<&Arc<dyn ErasedSource>> {
        self.sources.get(id)
    }

    /// Looks up a registered sink by id.
    pub(crate) fn sink(&self, id: &SinkId) -> Option<&Arc<dyn ErasedSink>> {
        self.sinks.get(id)
    }

    /// Looks up a registered source by id, downcast to the concrete `S` a
    /// caller expects. `None` covers both "no source registered under this
    /// id" and "registered, but as a different concrete type" — see
    /// [`crate::flow::FlowResolver::source`], the one caller, for how it
    /// turns either case into a named, loud [`crate::error::CompError`]
    /// rather than leaving `None` to propagate as a generic failure.
    pub(crate) fn source_typed<S: SourceBase>(&self, id: &SourceId) -> Option<Arc<S>> {
        self.source_typed.get(id)?.clone().downcast::<S>().ok()
    }

    /// Looks up a registered sink by id, downcast to the concrete `S` a
    /// caller expects — see [`Self::source_typed`].
    pub(crate) fn sink_typed<S: SinkBase>(&self, id: &SinkId) -> Option<Arc<S>> {
        self.sink_typed.get(id)?.clone().downcast::<S>().ok()
    }

    /// Merges `other`'s sources and sinks into `self`, used by
    /// [`crate::EngineBuilder::registry`] so that call can *merge* into the
    /// builder's existing registrations instead of replacing them wholesale.
    ///
    /// # Panics
    /// Panics if `other` registers a source or sink id already present in
    /// `self` — the same "duplicate is a startup configuration error" stance
    /// [`Self::register_source`]/[`Self::register_sink`] already take,
    /// applied uniformly here regardless of whether a given id got into
    /// `self` via `register_source`/`register_sink` directly or via an
    /// earlier `merge`. A silent "last write wins" here would let a
    /// `.source(a)` call followed by a `.registry(other)` that happens to
    /// also register `a` silently shadow the first registration — exactly
    /// the class of surprise `EngineBuilder::registry`'s old "replaces
    /// everything" behavior caused, just at a different granularity.
    pub(crate) fn merge(&mut self, other: Registry) {
        for (id, src) in other.sources {
            let prev = self.sources.insert(id.clone(), src);
            assert!(prev.is_none(), "duplicate source id: {id}");
        }
        for (id, src) in other.source_typed {
            self.source_typed.insert(id, src);
        }
        for (id, sink) in other.sinks {
            let prev = self.sinks.insert(id.clone(), sink);
            assert!(prev.is_none(), "duplicate sink id: {id}");
        }
        for (id, sink) in other.sink_typed {
            self.sink_typed.insert(id, sink);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{MemKvSource, VecSink};

    #[test]
    fn merge_combines_distinct_ids_from_both_registries() {
        let mut base = Registry::default();
        base.register_source(MemKvSource::new("kv_a"));

        let mut other = Registry::default();
        other.register_source(MemKvSource::new("kv_b"));
        other.register_sink(VecSink::new("docs"));

        base.merge(other);

        assert!(base.source(&crate::source::SourceId::new("kv_a")).is_some());
        assert!(base.source(&crate::source::SourceId::new("kv_b")).is_some());
        assert!(base.sink(&crate::sink::SinkId::new("docs")).is_some());
    }

    #[test]
    #[should_panic(expected = "duplicate source id")]
    fn merge_panics_on_a_source_id_present_in_both() {
        let mut base = Registry::default();
        base.register_source(MemKvSource::new("kv_a"));

        let mut other = Registry::default();
        other.register_source(MemKvSource::new("kv_a"));

        base.merge(other);
    }
}
