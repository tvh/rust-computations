//! Test-only helpers for building fixtures against the engine.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use tokio::sync::{Mutex as AsyncMutex, mpsc};

use crate::error::{SinkError, SourceError};
use crate::sink::{Sink, SinkBase, SinkId};
use crate::source::{Dep, Request, Source, SourceBase, SourceId};

/// A `GetKey` request against [`MemKvSource`]: reads the current value of a
/// key, if any.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GetKey(pub String);

impl Request for GetKey {
    type Output = Option<String>;
}

struct MemKvState {
    values: HashMap<String, String>,
    versions: HashMap<String, u64>,
    watched: HashSet<String>,
}

/// An in-memory string key/value store usable as a test [`Source`].
///
/// Each key has a version counter starting at 0 (so a key that does not yet
/// exist reads as version 0, and later creation is detectable as a change).
/// `set` bumps the version and, if the key is currently watched, pushes the
/// change onto an internal channel that [`SourceBase::wait_changes`] drains.
pub struct MemKvSource {
    id: SourceId,
    state: Mutex<MemKvState>,
    changes_tx: mpsc::UnboundedSender<Dep<String, u64>>,
    changes_rx: AsyncMutex<mpsc::UnboundedReceiver<Dep<String, u64>>>,
}

impl MemKvSource {
    /// Creates a new, empty `MemKvSource` with the given instance id.
    pub fn new(id: &str) -> Arc<Self> {
        let (changes_tx, changes_rx) = mpsc::unbounded_channel();
        Arc::new(MemKvSource {
            id: SourceId::new(id),
            state: Mutex::new(MemKvState {
                values: HashMap::new(),
                versions: HashMap::new(),
                watched: HashSet::new(),
            }),
            changes_tx,
            changes_rx: AsyncMutex::new(changes_rx),
        })
    }

    /// Sets `key` to `value`, bumping its version. If `key` is currently
    /// watched, pushes the resulting `Dep` onto the change channel.
    pub async fn set(&self, key: &str, value: &str) {
        let (new_ver, watched) = {
            let mut state = self.state.lock().unwrap();
            state.values.insert(key.to_string(), value.to_string());
            let ver = state.versions.entry(key.to_string()).or_insert(0);
            *ver += 1;
            (*ver, state.watched.contains(key))
        };
        if watched {
            let _ = self.changes_tx.send(Dep {
                key: key.to_string(),
                ver: new_ver,
            });
        }
    }
}

impl SourceBase for MemKvSource {
    type Key = String;
    type Ver = u64;

    fn instance_id(&self) -> SourceId {
        self.id.clone()
    }

    async fn wait_changes(&self) -> HashSet<Dep<String, u64>> {
        let mut rx = self.changes_rx.lock().await;
        let mut batch = HashSet::new();
        // Await the first item (cancel-safe: mpsc recv is cancel-safe), then
        // opportunistically drain any additional queued items into the same
        // batch without blocking further.
        match rx.recv().await {
            Some(dep) => {
                batch.insert(dep);
            }
            None => return batch,
        }
        while let Ok(dep) = rx.try_recv() {
            batch.insert(dep);
        }
        batch
    }

    fn unregister(&self, keys: &HashSet<String>) {
        let mut state = self.state.lock().unwrap();
        for key in keys {
            state.watched.remove(key);
        }
    }
}

impl Source<GetKey> for MemKvSource {
    async fn execute(
        &self,
        req: GetKey,
    ) -> (Result<Option<String>, SourceError>, HashSet<Dep<String, u64>>) {
        let GetKey(key) = req;
        let mut state = self.state.lock().unwrap();
        state.watched.insert(key.clone());
        let value = state.values.get(&key).cloned();
        let ver = *state.versions.get(&key).unwrap_or(&0);
        let mut deps = HashSet::new();
        deps.insert(Dep { key, ver });
        (Ok(value), deps)
    }
}

/// A `WriteDoc` request against [`VecSink`]: stores `content` under `name`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WriteDoc {
    pub name: String,
    pub content: String,
}

impl Request for WriteDoc {
    type Output = ();
}

/// An in-memory name -> content store usable as a test [`Sink`].
pub struct VecSink {
    id: SinkId,
    docs: Mutex<HashMap<String, String>>,
}

impl VecSink {
    /// Creates a new, empty `VecSink` with the given instance id.
    pub fn new(id: &str) -> Arc<Self> {
        Arc::new(VecSink {
            id: SinkId::new(id),
            docs: Mutex::new(HashMap::new()),
        })
    }

    /// Test-only inspection: the content stored under `name`, if any.
    pub fn get(&self, name: &str) -> Option<String> {
        self.docs.lock().unwrap().get(name).cloned()
    }

    /// Test-only inspection: every name currently stored.
    pub fn names(&self) -> Vec<String> {
        self.docs.lock().unwrap().keys().cloned().collect()
    }
}

impl SinkBase for VecSink {
    type Out = String;

    fn instance_id(&self) -> SinkId {
        self.id.clone()
    }

    async fn delete_outputs(&self, outs: HashSet<String>) -> Result<(), SinkError> {
        let mut docs = self.docs.lock().unwrap();
        for name in outs {
            docs.remove(&name);
        }
        Ok(())
    }

    async fn list_existing_outputs(&self) -> Result<Option<HashSet<String>>, SinkError> {
        let docs = self.docs.lock().unwrap();
        Ok(Some(docs.keys().cloned().collect()))
    }
}

impl Sink<WriteDoc> for VecSink {
    async fn execute(&self, req: WriteDoc) -> (HashSet<String>, Result<(), SinkError>) {
        let WriteDoc { name, content } = req;
        self.docs.lock().unwrap().insert(name.clone(), content);
        let mut outs = HashSet::new();
        outs.insert(name);
        (outs, Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::Registry;

    #[tokio::test]
    async fn mem_kv_execute_returns_value_and_versioned_dep() {
        let src = MemKvSource::new("kv");
        src.set("a", "1").await;

        let (result, deps) = src.execute(GetKey("a".to_string())).await;
        assert_eq!(result.unwrap(), Some("1".to_string()));
        assert_eq!(deps.len(), 1);
        let dep = deps.into_iter().next().unwrap();
        assert_eq!(dep.key, "a");
        assert_eq!(dep.ver, 1);
    }

    #[tokio::test]
    async fn mem_kv_execute_on_missing_key_reports_version_zero() {
        let src = MemKvSource::new("kv");
        let (result, deps) = src.execute(GetKey("missing".to_string())).await;
        assert_eq!(result.unwrap(), None);
        let dep = deps.into_iter().next().unwrap();
        assert_eq!(dep.ver, 0);
    }

    #[tokio::test]
    async fn mem_kv_set_bumps_version_and_notifies_watchers() {
        let src = MemKvSource::new("kv");
        // Reading the key marks it watched.
        let _ = src.execute(GetKey("a".to_string())).await;

        src.set("a", "1").await;
        let changes = src.wait_changes().await;
        assert_eq!(changes.len(), 1);
        let dep = changes.into_iter().next().unwrap();
        assert_eq!(dep.key, "a");
        assert_eq!(dep.ver, 1);
    }

    #[tokio::test]
    async fn mem_kv_unregister_stops_notifications() {
        let src = MemKvSource::new("kv");
        let _ = src.execute(GetKey("a".to_string())).await;

        let mut watched = HashSet::new();
        watched.insert("a".to_string());
        src.unregister(&watched);

        src.set("a", "1").await;
        let timed_out = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            src.wait_changes(),
        )
        .await;
        assert!(timed_out.is_err(), "unregistered key should not notify");
    }

    #[tokio::test]
    async fn mem_kv_set_on_unwatched_key_emits_nothing() {
        let src = MemKvSource::new("kv");
        // Never read/watched "a".
        src.set("a", "1").await;

        let timed_out = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            src.wait_changes(),
        )
        .await;
        assert!(timed_out.is_err(), "unwatched key should not notify");
    }

    #[tokio::test]
    async fn vec_sink_execute_stores_and_reports_output() {
        let sink = VecSink::new("docs");
        let (outs, result) = sink
            .execute(WriteDoc {
                name: "doc1".to_string(),
                content: "hello".to_string(),
            })
            .await;
        assert!(result.is_ok());
        assert_eq!(outs.len(), 1);
        assert!(outs.contains("doc1"));
        assert_eq!(sink.get("doc1"), Some("hello".to_string()));
    }

    #[tokio::test]
    async fn vec_sink_delete_outputs_removes_entries() {
        let sink = VecSink::new("docs");
        let _ = sink
            .execute(WriteDoc {
                name: "doc1".to_string(),
                content: "hello".to_string(),
            })
            .await;

        let mut to_delete = HashSet::new();
        to_delete.insert("doc1".to_string());
        sink.delete_outputs(to_delete).await.unwrap();
        assert_eq!(sink.get("doc1"), None);
    }

    #[tokio::test]
    async fn vec_sink_list_existing_outputs_reflects_state() {
        let sink = VecSink::new("docs");
        let _ = sink
            .execute(WriteDoc {
                name: "doc1".to_string(),
                content: "hello".to_string(),
            })
            .await;
        let _ = sink
            .execute(WriteDoc {
                name: "doc2".to_string(),
                content: "world".to_string(),
            })
            .await;

        let existing = sink.list_existing_outputs().await.unwrap().unwrap();
        assert_eq!(existing.len(), 2);
        assert!(existing.contains("doc1"));
        assert!(existing.contains("doc2"));

        let mut names = sink.names();
        names.sort();
        assert_eq!(names, vec!["doc1".to_string(), "doc2".to_string()]);
    }

    #[tokio::test]
    async fn erasure_roundtrip_source_and_sink() {
        let src = MemKvSource::new("kv");
        let sink = VecSink::new("docs");

        let mut registry = Registry::default();
        registry.register_source(src.clone());
        registry.register_sink(sink.clone());

        let src_id = SourceId::new("kv");
        let sink_id = SinkId::new("docs");

        // Reading "a" marks it watched.
        let _ = src.execute(GetKey("a".to_string())).await;
        src.set("a", "1").await;

        let erased_src = registry.source(&src_id).expect("source registered");
        let raw = erased_src.wait_changes().await;
        assert_eq!(raw.len(), 1);
        let raw_dep = raw.into_iter().next().unwrap();
        assert_eq!(raw_dep.source, src_id);
        let key: String = postcard::from_bytes(&raw_dep.key).unwrap();
        assert_eq!(key, "a");
        let ver: u64 = postcard::from_bytes(&raw_dep.ver).unwrap();
        assert_eq!(ver, 1);

        // Unregister via bytes stops further notifications for that key.
        let key_bytes = postcard::to_stdvec(&"a".to_string()).unwrap();
        erased_src.unregister(&[key_bytes]);
        src.set("a", "2").await;
        let timed_out =
            tokio::time::timeout(std::time::Duration::from_millis(50), erased_src.wait_changes())
                .await;
        assert!(timed_out.is_err(), "unregistered key should not notify");

        // Sink round trip via the erased interface.
        let _ = sink
            .execute(WriteDoc {
                name: "doc1".to_string(),
                content: "hello".to_string(),
            })
            .await;

        let erased_sink = registry.sink(&sink_id).expect("sink registered");
        let existing = erased_sink
            .list_existing_outputs()
            .await
            .unwrap()
            .expect("VecSink supports listing");
        let doc1_bytes = postcard::to_stdvec(&"doc1".to_string()).unwrap();
        assert!(existing.contains(&doc1_bytes));

        erased_sink.delete_outputs(vec![doc1_bytes]).await.unwrap();
        assert_eq!(sink.get("doc1"), None);
    }
}
