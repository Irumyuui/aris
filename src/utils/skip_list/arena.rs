use std::{
    alloc::Layout,
    ptr::{null_mut, NonNull},
    sync::atomic::{AtomicUsize, Ordering},
};

pub(crate) struct Arena {
    cur_block_ptr: *mut u8,
    remain_bytes: usize,
    blocks: Vec<(*mut u8, Layout)>,
    memory_usage: AtomicUsize,
}

unsafe impl Send for Arena {}

const BLOCK_SIZE: usize = 1 << 12;
const NEW_BLOCK_LAYOUT: Layout = Layout::new::<[u8; BLOCK_SIZE]>();

impl Arena {
    pub(crate) fn new() -> Self {
        Self {
            cur_block_ptr: null_mut(),
            remain_bytes: 0,
            blocks: Vec::new(),
            memory_usage: AtomicUsize::new(0),
        }
    }

    pub(crate) unsafe fn alloc_unaligned(&mut self, bytes: usize) -> *mut u8 {
        if bytes > BLOCK_SIZE / 4 {
            let layout = Layout::array::<u8>(bytes).unwrap();
            let result = self.alloc_new_block(layout);
            return result;
        }

        if self.remain_bytes < bytes {
            self.cur_block_ptr = self.alloc_new_block(NEW_BLOCK_LAYOUT);
            self.remain_bytes = BLOCK_SIZE;
        }
        assert!(self.remain_bytes >= bytes);

        let result = self.cur_block_ptr;
        self.cur_block_ptr = self.cur_block_ptr.wrapping_byte_add(bytes);
        self.remain_bytes -= bytes;
        result
    }

    pub(crate) unsafe fn alloc(&mut self, layout: Layout) -> *mut u8 {
        let size = layout.size();
        let align = layout.align();
        let null_bytes = align - (self.cur_block_ptr as usize % align);
        let total_bytes = null_bytes + size;

        if total_bytes <= self.remain_bytes {
            return self.advance_ptr(null_bytes, total_bytes);
        }

        if total_bytes > BLOCK_SIZE / 4 {
            let result = self.alloc_new_block(layout);
            return result;
        }

        // block not enoght..
        self.cur_block_ptr = self.alloc_new_block(NEW_BLOCK_LAYOUT);
        self.remain_bytes = BLOCK_SIZE;
        self.advance_ptr(null_bytes, total_bytes)
    }

    fn advance_ptr(&mut self, null_bytes: usize, total_bytes: usize) -> *mut u8 {
        assert!(total_bytes <= self.remain_bytes);
        let result = self.cur_block_ptr.wrapping_byte_add(null_bytes);
        self.cur_block_ptr = self.cur_block_ptr.wrapping_byte_add(total_bytes);
        self.remain_bytes -= total_bytes;
        result
    }

    unsafe fn alloc_new_block(&mut self, layout: Layout) -> *mut u8 {
        let new_block = std::alloc::alloc(layout);
        self.blocks.push((new_block, layout));
        self.memory_usage
            .fetch_add(layout.size(), Ordering::Relaxed);
        new_block
    }

    pub(crate) fn memory_usage(&self) -> usize {
        self.memory_usage.load(Ordering::Relaxed)
    }
}

impl Drop for Arena {
    fn drop(&mut self) {
        for &(ptr, layout) in self.blocks.iter() {
            unsafe {
                std::alloc::dealloc(ptr, layout);
            }
        }
        self.blocks.clear();
    }
}

#[cfg(test)]
mod tests {
    use std::alloc::Layout;

    use rand::Rng;

    use crate::utils::skip_list::arena::BLOCK_SIZE;

    use super::Arena;

    #[test]
    fn arena_alloc() {
        const TOTAL_ALLOCATE_COUNT: usize = 10000;
        const MIN_BYTES: usize = 10;
        const MAX_BYTES: usize = BLOCK_SIZE * 4;

        let mut rng = rand::thread_rng();
        let mut arena = Arena::new();
        let mut expected_total = 0;

        for _ in 0..TOTAL_ALLOCATE_COUNT {
            let size = rng.gen_range(MIN_BYTES..=MAX_BYTES);
            let ty = rng.gen_bool(0.4);

            unsafe {
                if ty {
                    arena.alloc(Layout::from_size_align(size, 8).unwrap());
                } else {
                    arena.alloc_unaligned(size);
                }
            }
            expected_total += size;
        }

        assert!(expected_total <= arena.memory_usage());
    }
}
