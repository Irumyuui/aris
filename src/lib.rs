#![allow(unused)]

use mimalloc::MiMalloc;

pub mod utils;

#[global_allocator]
static GLOBAL_ALLOCATOR: MiMalloc = MiMalloc;
