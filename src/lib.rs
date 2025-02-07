pub(crate) mod config;
pub(crate) mod mem;
pub(crate) mod redo_log;
pub(crate) mod table;

pub mod comparator;
pub mod error;
pub mod iterator;
pub mod utils;
pub mod filter;

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL_ALLOCATOR: MiMalloc = MiMalloc;

#[ctor::ctor]
fn __init() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_target(true)
        .with_file(true)
        .with_level(true)
        .without_time()
        .with_thread_ids(true)
        .init();
}
