use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error, Clone)]
pub enum DbError {
    #[error("IO error: {0}")]
    Io(Arc<std::io::Error>),

    #[error("Key not found: {0}")]
    KeyNotFound(String),

    #[error("Directory not found: {0}")]
    DirectoryNotFound(String),

    #[error("Manifest not found")]
    ManifestNotFound,

    #[error("Manifest read error: {0}")]
    ManifestReadError(String),

    #[error("Manifest corrupted")]
    ManifestCorrupted,

    #[error("Writer panic. Operation aborted.")]
    WriterPanic,

    #[error("Data corrupted: {0}")]
    DataCorrupted(String),
}

impl From<std::io::Error> for DbError {
    fn from(err: std::io::Error) -> Self {
        DbError::Io(std::sync::Arc::new(err))
    }
}
