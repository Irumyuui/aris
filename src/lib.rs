use mimalloc::MiMalloc;

mod memtable;

mod db;
mod utils;

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
        .with_thread_ids(true)
        .init();
}
