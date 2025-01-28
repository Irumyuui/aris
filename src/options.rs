use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct Options {
    pub(crate) vlog_path: Option<PathBuf>,
}

#[derive(Default)]
pub struct ReadOptions {
    pub snapshot: Option<u64>,
}
