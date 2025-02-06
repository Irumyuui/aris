#[derive(Debug, thiserror::Error)]
pub enum DBError {
    #[error("IO: {0}")]
    IO(#[from] std::io::Error),

    #[error("VarInt: {0}")]
    VarInt(#[from] VarIntError),
}

pub type DBResult<T, E = DBError> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum VarIntError {
    #[error("Insufficient bytes")]
    InsufficientBytes,

    #[error("Overflow")]
    Overflow,
}
