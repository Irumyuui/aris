use std::{
    alloc::Layout,
    sync::{atomic::AtomicUsize, Arc, Mutex},
};

pub trait MemAllocator {
    unsafe fn allocate(&self, layout: Layout) -> *mut u8;
    fn mem_usage(&self) -> usize;
}

#[derive(Default, Debug, Clone)]
pub struct DefaultAllocator(Arc<DefaultAllocatorInner>);

impl MemAllocator for DefaultAllocator {
    unsafe fn allocate(&self, layout: Layout) -> *mut u8 {
        self.0.allocate(layout)
    }

    fn mem_usage(&self) -> usize {
        self.0.mem_usage()
    }
}

#[derive(Default, Debug)]
pub struct DefaultAllocatorInner {
    mems: Mutex<Vec<(*mut u8, Layout)>>,
    mem_alloc: AtomicUsize,
}

impl MemAllocator for DefaultAllocatorInner {
    unsafe fn allocate(&self, layout: Layout) -> *mut u8 {
        let ptr = std::alloc::alloc(layout);
        self.mems.lock().unwrap().push((ptr, layout));
        self.mem_alloc
            .fetch_add(layout.size(), std::sync::atomic::Ordering::SeqCst);
        ptr
    }

    fn mem_usage(&self) -> usize {
        self.mem_alloc.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl Drop for DefaultAllocatorInner {
    fn drop(&mut self) {
        unsafe {
            for (ptr, layout) in self.mems.get_mut().unwrap().iter() {
                std::alloc::dealloc(*ptr, *layout);
            }
        }
    }
}

// const BLOCK_SIZE: usize = 4096;

// // Single-threaded safety
// pub struct BlockArena {
//     blocks: Vec<Vec<u8>>,
//     alloc_ptr: *mut u8,
//     alloc_bytes_remaining: usize,
//     mem_alloc: AtomicUsize,
// }

// impl Default for BlockArena {
//     fn default() -> Self {
//         Self {
//             blocks: Vec::new(),
//             alloc_ptr: std::ptr::null_mut(),
//             alloc_bytes_remaining: 0,
//             mem_alloc: AtomicUsize::new(0),
//         }
//     }
// }

// impl BlockArena {
//     pub fn new() -> Self {
//         Self::default()
//     }

//     pub fn allocate(&mut self, bytes: usize) -> *mut u8 {
//         if self.alloc_bytes_remaining >= bytes {
//             let result = self.alloc_ptr;
//             self.alloc_ptr = unsafe { self.alloc_ptr.add(bytes) };
//             self.alloc_bytes_remaining -= bytes;
//             return result;
//         }

//         self.allocate_fallback(bytes)
//     }

//     pub fn allocate_aligned(&mut self, bytes: usize, align: usize) -> *mut u8 {
//         assert!(align % 2 == 0);

//         let cmod = (self.alloc_ptr as usize) & (align - 1);
//         let slop = if cmod == 0 { 0 } else { align - cmod };
//         let needed = bytes + slop;

//         let result = if needed <= self.alloc_bytes_remaining {
//             unsafe {
//                 let result = self.alloc_ptr.add(slop);
//                 self.alloc_ptr = self.alloc_ptr.add(needed);
//                 self.alloc_bytes_remaining -= needed;
//                 result
//             }
//         } else {
//             self.allocate_fallback(bytes)
//         };

//         assert!(
//             result as usize & (align - 1) == 0,
//             "result: {:?}, slop: {:?}, bytes: {:?}",
//             result,
//             slop,
//             bytes,
//         );
//         result
//     }

//     fn allocate_fallback(&mut self, bytes: usize) -> *mut u8 {
//         if bytes > BLOCK_SIZE / 4 {
//             return self.allocate_new_block(bytes);
//         }

//         self.alloc_ptr = self.allocate_new_block(BLOCK_SIZE);
//         self.alloc_bytes_remaining = BLOCK_SIZE;

//         unsafe {
//             let result = self.alloc_ptr;
//             self.alloc_ptr = self.alloc_ptr.add(bytes);
//             self.alloc_bytes_remaining -= bytes;
//             result
//         }
//     }

//     fn allocate_new_block(&mut self, bytes: usize) -> *mut u8 {
//         let mem = vec![0_u8; bytes]; // aligned!
//         let ptr = mem.as_ptr() as *mut u8;
//         self.blocks.push(mem);
//         self.mem_alloc
//             .fetch_add(bytes, std::sync::atomic::Ordering::SeqCst);
//         ptr
//     }
// }
