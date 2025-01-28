pub mod error;
pub mod utils;
pub mod vlog;

pub mod write_batch;
mod memtable;
mod db_impl;

pub mod options;

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL_ALLOCATOR: MiMalloc = MiMalloc;

#[ctor::ctor]
fn init() {
    color_backtrace::install();

    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_target(true)
        .with_file(true)
        .with_level(true)
        .without_time()
        .with_thread_ids(true)
        .init();
}
