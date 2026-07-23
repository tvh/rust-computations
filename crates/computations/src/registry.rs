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
    // Consumed by the engine/driver (steps 4-5): not called yet in this crate
    // outside tests.
    #[allow(dead_code)]
    pub(crate) fn sources(&self) -> impl Iterator<Item = &Arc<dyn ErasedSource>> {
        self.sources.values()
    }

    /// Looks up a registered source by id.
    // Consumed by the engine/driver (steps 4-5): not called yet in this crate
    // outside tests.
    #[allow(dead_code)]
    pub(crate) fn source(&self, id: &SourceId) -> Option<&Arc<dyn ErasedSource>> {
        self.sources.get(id)
    }

    /// Looks up a registered sink by id.
    // Consumed by the engine/driver (steps 4-5): not called yet in this crate
    // outside tests.
    #[allow(dead_code)]
    pub(crate) fn sink(&self, id: &SinkId) -> Option<&Arc<dyn ErasedSink>> {
        self.sinks.get(id)
    }
}
