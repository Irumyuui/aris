use std::{
    alloc::Layout,
    cell::RefCell,
    sync::atomic::{AtomicUsize, Ordering},
};

pub trait Arena {
    /// Similar to `std::alloc::alloc`
    unsafe fn allocate<T>(&self, layout: Layout) -> *mut T;

    /// Get the memory usage of the arena
    fn memory_usage(&self) -> usize;
}

const BLOCK_SIZE: usize = 4096;

/// `BlockArena` is similar to the implementation in Leveldb.
/// When writing, should need to ensure that only one thread is writing at the same time.
///
/// # NOTICE
///
/// Remember that this allocator does not guarantee that memory allocated in it will be dropped, so remember to use `ptr::drop_in_place`
#[derive(Debug)]
pub struct BlockArena {
    inner: RefCell<InternalBlockArena>,
}

impl BlockArena {
    pub fn new() -> Self {
        Self {
            inner: RefCell::new(InternalBlockArena {
                cur_ptr: 0,
                block_bytes_remaining: 0,
                blocks: vec![],
                memory_usage: AtomicUsize::new(0),
            }),
        }
    }
}

impl Default for BlockArena {
    fn default() -> Self {
        Self::new()
    }
}

impl Arena for BlockArena {
    unsafe fn allocate<T>(&self, layout: Layout) -> *mut T {
        assert_eq!(layout.align() & (layout.align() - 1), 0);

        self.inner.borrow_mut().allocate(layout)
    }

    fn memory_usage(&self) -> usize {
        self.inner.borrow().memory_usage.load(Ordering::Acquire)
    }
}

#[derive(Debug)]
pub struct InternalBlockArena {
    cur_ptr: usize,
    block_bytes_remaining: usize,
    blocks: Vec<Vec<u8>>,
    memory_usage: AtomicUsize,
}

impl InternalBlockArena {
    unsafe fn allocate<T>(&mut self, layout: Layout) -> *mut T {
        let size = layout.size();
        let aligin = layout.align();

        let slop = aligin - (self.cur_ptr & (aligin - 1));
        let needed = slop + size;

        let result = if needed <= self.block_bytes_remaining {
            let res = self.cur_ptr + slop;
            self.cur_ptr += needed;
            self.block_bytes_remaining -= needed;
            res as *mut u8
        } else {
            self.allocate_fallback(needed)
        };

        result as *mut _
    }

    fn allocate_fallback(&mut self, size: usize) -> *mut u8 {
        if size > BLOCK_SIZE / 4 {
            return self.allocate_new_block(size);
        }

        let ptr = self.allocate_new_block(BLOCK_SIZE);
        self.cur_ptr = ptr as usize + size;
        self.block_bytes_remaining = BLOCK_SIZE - size;
        ptr
    }

    fn allocate_new_block(&mut self, block_bytes: usize) -> *mut u8 {
        let mut new_block = vec![0_u8; block_bytes];
        let res = new_block.as_mut_ptr();
        self.blocks.push(new_block);
        self.memory_usage.fetch_add(block_bytes, Ordering::Release);
        res
    }
}

#[cfg(test)]
mod tests {
    use std::alloc::Layout;

    use rand::Rng;

    use crate::utils::arena::{Arena, BLOCK_SIZE};

    use super::BlockArena;

    #[test]
    fn allocate() {
        const TOTAL_ALLOCATE_COUNT: usize = 1000;
        const MIN_BYTES: usize = 10;
        const MAX_BYTES: usize = BLOCK_SIZE * 4;

        let mut rng = rand::thread_rng();
        let arena: BlockArena = BlockArena::new();
        let mut expected_total = 0;

        for _ in 0..TOTAL_ALLOCATE_COUNT {
            let size = rng.gen_range(MIN_BYTES..=MAX_BYTES);
            unsafe {
                arena.allocate::<u8>(Layout::array::<u8>(size).unwrap());
            }
            expected_total += size;
        }

        assert!(expected_total <= arena.memory_usage());
    }
}
