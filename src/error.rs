#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("IO error: {0}")]
    IO(#[from] std::io::Error),

    #[error("Vlog entry corrupted: {0}")]
    InvalidVlogEntry(String),

    #[error("Vlog file corrupted: {0}")]
    VLogFileCorrupted(String),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
