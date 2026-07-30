//! The trait implemented by outputs that consume computation results.
//!
//! A sink performs typed write [`Request`]s and reports which outputs it
//! produced, so the engine can garbage-collect outputs whose producing
//! computation died (paper appendix A).

use std::collections::HashSet;
use std::fmt;
use std::future::Future;
use std::sync::Arc;

use futures::future::BoxFuture;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::SinkError;
use crate::source::Request;

/// The stable identity of a [`SinkBase`] instance (e.g. `"fs"`, `"db-cache"`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SinkId(Arc<str>);

impl SinkId {
    /// Creates a `SinkId` for the sink instance named `name`.
    pub fn new(name: &str) -> Self {
        SinkId(Arc::from(name))
    }
}

impl fmt::Display for SinkId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

// See `SourceId`'s identical hand-written impls for why this exists and why
// it's hand-written rather than derived through the wrapped `Arc<str>`.
impl serde::Serialize for SinkId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> serde::Deserialize<'de> for SinkId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer).map(|s| SinkId::new(&s))
    }
}

/// The untyped operations every sink supports, independent of any particular
/// [`Request`] type it can execute.
pub trait SinkBase: Send + Sync + 'static {
    /// The type of outputs this sink produces (e.g. a file path, a row id).
    type Out: Clone + Eq + std::hash::Hash + Serialize + DeserializeOwned + Send + Sync + 'static;

    /// This sink instance's stable identity.
    fn instance_id(&self) -> SinkId;

    /// Deletes outputs whose producing computation died (GC).
    fn delete_outputs(
        &self,
        outs: HashSet<Self::Out>,
    ) -> impl Future<Output = Result<(), SinkError>> + Send;

    /// Lists outputs existing on the external system (startup GC).
    ///
    /// `Ok(None)` means listing is unsupported by this sink.
    fn list_existing_outputs(
        &self,
    ) -> impl Future<Output = Result<Option<HashSet<Self::Out>>, SinkError>> + Send;
}

/// A sink that can execute requests of type `R`.
pub trait Sink<R: Request>: SinkBase {
    /// Executes one write request, reporting the outputs it produced.
    fn execute(
        &self,
        req: R,
    ) -> impl Future<Output = (HashSet<Self::Out>, Result<R::Output, SinkError>)> + Send;
}

/// Postcard-encoded bytes of a sink output, erased of their concrete type.
pub type OutBytes = Vec<u8>;

/// An erased output: which sink produced it, plus postcard-encoded bytes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RawOutput {
    pub sink: SinkId,
    pub out: OutBytes,
}

/// Object-safe, byte-erased view of a [`SinkBase`].
///
/// The engine and driver store sinks heterogeneously behind this trait and
/// only need untyped operations.
pub(crate) trait ErasedSink: Send + Sync {
    fn instance_id(&self) -> SinkId;
    fn delete_outputs(&self, outs: Vec<OutBytes>) -> BoxFuture<'_, Result<(), SinkError>>;
    fn list_existing_outputs(&self) -> BoxFuture<'_, Result<Option<HashSet<OutBytes>>, SinkError>>;
}

/// Adapts any concrete `S: SinkBase` to the erased [`ErasedSink`] interface,
/// postcard-(de)serializing outputs at the boundary.
///
/// Constructed by `Registry::register_sink`.
pub(crate) struct SinkAdapter<S>(pub Arc<S>);

impl<S: SinkBase> ErasedSink for SinkAdapter<S> {
    fn instance_id(&self) -> SinkId {
        self.0.instance_id()
    }

    fn delete_outputs(&self, outs: Vec<OutBytes>) -> BoxFuture<'_, Result<(), SinkError>> {
        Box::pin(async move {
            // An output that fails to deserialize as `S::Out` cannot belong
            // to this sink, so it is simply skipped.
            let outs: HashSet<S::Out> = outs
                .iter()
                .filter_map(|bytes| postcard::from_bytes(bytes).ok())
                .collect();
            self.0.delete_outputs(outs).await
        })
    }

    fn list_existing_outputs(&self) -> BoxFuture<'_, Result<Option<HashSet<OutBytes>>, SinkError>> {
        Box::pin(async move {
            let outs = self.0.list_existing_outputs().await?;
            Ok(outs.map(|set| {
                set.iter()
                    .map(|out| {
                        postcard::to_stdvec(out).expect("postcard serialization should not fail")
                    })
                    .collect()
            }))
        })
    }
}
