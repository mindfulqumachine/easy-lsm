use thiserror::Error;

#[derive(Debug, Error)]
pub enum LsmError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Key not found: {0}")]
    KeyNotFound(String),
}
