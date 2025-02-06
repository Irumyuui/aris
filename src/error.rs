#[derive(Debug, thiserror::Error)]
pub enum DBError {
    #[error("IO: {0}")]
    IO(#[from] std::io::Error),
}

pub type DBResult<T, E = DBError> = std::result::Result<T, E>;
