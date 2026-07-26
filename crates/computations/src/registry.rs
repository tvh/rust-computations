//! A startup-time registry of pluggable sources and sinks.

use std::collections::HashMap;
use std::sync::Arc;

use crate::sink::{ErasedSink, SinkAdapter, SinkBase, SinkId};
use crate::source::{ErasedSource, SourceAdapter, SourceBase, SourceId};

/// Holds every source and sink instance wired into a driver at startup.
#[derive(Default)]
pub struct Registry {
    sources: HashMap<SourceId, Arc<dyn ErasedSource>>,
    sinks: HashMap<SinkId, Arc<dyn ErasedSink>>,
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
            .insert(id.clone(), Arc::new(SourceAdapter(src)));
        assert!(prev.is_none(), "duplicate source id: {id}");
    }

    /// Registers a sink instance under its [`SinkBase::instance_id`].
    ///
    /// # Panics
    /// Panics if a sink with the same id is already registered — this is a
    /// startup configuration error, not a runtime condition to recover from.
    pub fn register_sink<S: SinkBase>(&mut self, sink: Arc<S>) {
        let id = sink.instance_id();
        let prev = self.sinks.insert(id.clone(), Arc::new(SinkAdapter(sink)));
        assert!(prev.is_none(), "duplicate sink id: {id}");
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
        for (id, sink) in other.sinks {
            let prev = self.sinks.insert(id.clone(), sink);
            assert!(prev.is_none(), "duplicate sink id: {id}");
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
