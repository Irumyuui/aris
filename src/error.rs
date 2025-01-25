#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("IO error: {0}")]
    IO(#[from] std::io::Error),

    #[error("Value-Log file not found: {0}")]
    ValueLogFileNotFound(String),

    #[error("Value Log File Corrupted: {0}")]
    ValueLogFileCorrupted(String),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
