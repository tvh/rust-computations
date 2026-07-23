//! Error types shared across the engine, sources, and sinks.

/// An error reported by a data source.
#[derive(Debug, Clone, thiserror::Error)]
pub enum SourceError {
    #[error("io error: {0}")]
    Io(String),
    #[error("{0}")]
    Other(String),
}

/// An error reported by a data sink.
#[derive(Debug, Clone, thiserror::Error)]
pub enum SinkError {
    #[error("io error: {0}")]
    Io(String),
    #[error("{0}")]
    Other(String),
}

/// An error produced while running or scheduling a computation.
#[derive(Debug, Clone, thiserror::Error)]
pub enum CompError {
    #[error("computation failed: {0}")]
    Failed(String),
    #[error("dependency cycle detected on computation {0}")]
    Cycle(String),
    #[error("source error: {0}")]
    Source(#[from] SourceError),
    #[error("sink error: {0}")]
    Sink(#[from] SinkError),
}

impl From<std::io::Error> for SourceError {
    fn from(e: std::io::Error) -> Self {
        SourceError::Io(e.to_string())
    }
}

impl From<std::io::Error> for SinkError {
    fn from(e: std::io::Error) -> Self {
        SinkError::Io(e.to_string())
    }
}
