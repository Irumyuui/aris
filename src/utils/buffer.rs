use std::{
    alloc::Layout,
    marker::PhantomData,
    ops::{Deref, DerefMut},
    ptr::NonNull,
};

use crate::error::{Error, Result};

const ALIGED_BASE: usize = 4096;

/// If use O_DIRECT mode, should use the aligned memory block here for access.
///
/// `ALIGED_BASE` may change later.
///
/// TODO: If already use io_uring(rio), would it be better to use this? Wait for the total work to be completed first.
/// formats of wal and vlog may need some changes...
pub struct AlignedBlock {
    ptr: NonNull<u8>,
    len: usize,
    layout: Layout,
    _marker: PhantomData<Box<[u8]>>,
}

impl AlignedBlock {
    pub fn new(len: usize) -> Result<Self> {
        assert_ne!(len, 0, "Size must be greater than 0");

        let size = align_up(len, ALIGED_BASE);

        let layout = Layout::from_size_align(size, ALIGED_BASE)
            .map_err(|e| Error::AlignedBlockNotAligned(e))?;

        let ptr = unsafe {
            let ptr = std::alloc::alloc(layout);
            if ptr.is_null() {
                return Err(Error::MemoryAlloc(
                    "Failed to allocate memory for aligned block".to_string(),
                ));
            }
            NonNull::new_unchecked(ptr)
        };

        Ok(Self {
            ptr,
            len,
            layout,
            _marker: PhantomData,
        })
    }

    fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}

impl AsRef<[u8]> for AlignedBlock {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl AsMut<[u8]> for AlignedBlock {
    fn as_mut(&mut self) -> &mut [u8] {
        self.as_mut_slice()
    }
}

impl Deref for AlignedBlock {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl DerefMut for AlignedBlock {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut_slice()
    }
}

impl Drop for AlignedBlock {
    fn drop(&mut self) {
        unsafe {
            let ptr = self.ptr.as_ptr();
            std::alloc::dealloc(ptr, self.layout);
        }
    }
}

fn align_up(n: usize, align: usize) -> usize {
    (n + align - 1) & !(align - 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn align_up_to_4096() {
        assert_eq!(align_up(0, 4096), 0);
        assert_eq!(align_up(1, 4096), 4096);
        assert_eq!(align_up(4095, 4096), 4096);
        assert_eq!(align_up(4096, 4096), 4096);
        assert_eq!(align_up(4097, 4096), 8192);
        assert_eq!(align_up(8191, 4096), 8192);
        assert_eq!(align_up(8192, 4096), 8192);
    }

    #[test]
    fn block_alignment() {
        let block = AlignedBlock::new(5000).unwrap();
        let addr = block.ptr.as_ptr() as usize;
        assert_eq!(addr % 4096, 0, "Pointer is not aligned to 4096 bytes");
    }

    #[test]
    fn block_size_alignment() {
        let block = AlignedBlock::new(5000).unwrap();
        assert!(
            block.layout.size() % ALIGED_BASE == 0,
            "Size is not aligned to 4096 bytes"
        );
    }

    #[test]
    fn as_ref_and_mut() {
        let mut block = AlignedBlock::new(4096).unwrap();
        assert_eq!(block.as_ref().len(), 4096);
        assert_eq!(block.as_mut().len(), 4096);
    }

    #[test]
    fn deref() {
        let mut block = AlignedBlock::new(4096).unwrap();
        block[0] = 123;
        assert_eq!(block[0], 123);
        block[4095] = 45;
        assert_eq!(block[4095], 45);
    }
}
