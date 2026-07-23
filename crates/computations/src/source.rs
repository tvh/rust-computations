//! The trait implemented by external data sources feeding the engine.
//!
//! A source answers typed read [`Request`]s and reports which input
//! dependencies (key + version pairs) each request touched. It can also push
//! change notifications for previously touched keys, and stop watching keys
//! on request via [`SourceBase::unregister`] (paper appendix A).

use std::collections::HashSet;
use std::fmt;
use std::future::Future;
use std::sync::Arc;

use futures::future::BoxFuture;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::SourceError;

/// A typed request against a source or sink.
pub trait Request: Clone + Eq + std::hash::Hash + Send + Sync + 'static {
    /// The typed result of executing this request.
    type Output: Clone + Send + Sync + 'static;
}

/// A dependency: an input key together with the version observed when it
/// was read (the paper's `Dep k v`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct Dep<K, V> {
    pub key: K,
    pub ver: V,
}

/// The stable identity of a [`SourceBase`] instance (e.g. `"fs"`, `"http-cache"`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SourceId(Arc<str>);

impl SourceId {
    /// Creates a `SourceId` for the source instance named `name`.
    pub fn new(name: &str) -> Self {
        SourceId(Arc::from(name))
    }
}

impl fmt::Display for SourceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The untyped operations every source supports, independent of any
/// particular [`Request`] type it can execute.
pub trait SourceBase: Send + Sync + 'static {
    /// The type of keys this source reads.
    type Key: Clone + Eq + std::hash::Hash + Serialize + DeserializeOwned + Send + Sync + 'static;
    /// The type of versions this source reports for a key.
    type Ver: Clone + Eq + Serialize + Send + Sync + 'static;

    /// This source instance's stable identity.
    fn instance_id(&self) -> SourceId;

    /// Awaits the next batch of changed deps (push channel).
    ///
    /// Must be cancel-safe: dropping the returned future before it resolves
    /// must not lose any change that has not yet been reported.
    fn wait_changes(&self) -> impl Future<Output = HashSet<Dep<Self::Key, Self::Ver>>> + Send;

    /// Stops watching `keys` (their watchers died).
    fn unregister(&self, keys: &HashSet<Self::Key>);
}

/// A source that can execute requests of type `R`.
pub trait Source<R: Request>: SourceBase {
    /// Executes one request, reporting the deps it touched along the way.
    #[allow(clippy::type_complexity)] // shape mandated by the source/sink API design
    fn execute(
        &self,
        req: R,
    ) -> impl Future<Output = (Result<R::Output, SourceError>, HashSet<Dep<Self::Key, Self::Ver>>)>
    + Send;
}

/// Postcard-encoded bytes of a source key, erased of their concrete type.
pub type KeyBytes = Vec<u8>;
/// Postcard-encoded bytes of a source version, erased of their concrete type.
pub type VerBytes = Vec<u8>;

/// An erased [`Dep`]: which source it came from, plus postcard-encoded key
/// and version bytes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RawDep {
    pub source: SourceId,
    pub key: KeyBytes,
    pub ver: VerBytes,
}

/// Object-safe, byte-erased view of a [`SourceBase`].
///
/// The engine and driver store sources heterogeneously behind this trait and
/// only need untyped operations.
pub(crate) trait ErasedSource: Send + Sync {
    fn instance_id(&self) -> SourceId;
    fn wait_changes(&self) -> BoxFuture<'_, HashSet<RawDep>>;
    fn unregister(&self, keys: &[KeyBytes]);
}

/// Adapts any concrete `S: SourceBase` to the erased [`ErasedSource`]
/// interface, postcard-(de)serializing keys/versions at the boundary.
///
/// Constructed by `Registry::register_source`.
pub(crate) struct SourceAdapter<S>(pub Arc<S>);

impl<S: SourceBase> ErasedSource for SourceAdapter<S> {
    fn instance_id(&self) -> SourceId {
        self.0.instance_id()
    }

    fn wait_changes(&self) -> BoxFuture<'_, HashSet<RawDep>> {
        Box::pin(async move {
            let deps = self.0.wait_changes().await;
            raw_deps(&self.0.instance_id(), &deps)
        })
    }

    fn unregister(&self, keys: &[KeyBytes]) {
        // A key that fails to deserialize as `S::Key` cannot belong to this
        // source, so it is simply skipped rather than treated as an error.
        let keys: HashSet<S::Key> = keys
            .iter()
            .filter_map(|bytes| postcard::from_bytes(bytes).ok())
            .collect();
        self.0.unregister(&keys);
    }
}

/// Erases a set of typed deps into their postcard-encoded, source-tagged
/// form, for the engine to store and match heterogeneously.
pub(crate) fn raw_deps<K, V>(source: &SourceId, deps: &HashSet<Dep<K, V>>) -> HashSet<RawDep>
where
    K: Serialize,
    V: Serialize,
{
    deps.iter()
        .map(|dep| RawDep {
            source: source.clone(),
            key: postcard::to_stdvec(&dep.key).expect("postcard serialization should not fail"),
            ver: postcard::to_stdvec(&dep.ver).expect("postcard serialization should not fail"),
        })
        .collect()
}
