use std::{marker::PhantomData, ptr::NonNull, sync::atomic::AtomicU64};

use bytes::Bytes;
use crossbeam::epoch::Guard;

pub(crate) type IntenalPtr = NonNull<IntenalNode>;
pub(crate) type LeafPtr = NonNull<LeafNode>;

#[derive(Debug, Clone, Copy)]
pub(crate) enum NodePtr {
    Intenal { ptr: IntenalPtr },
    Leaf { ptr: IntenalPtr },
    None,
}

pub(crate) struct LeafNode {
    key: Bytes,
    value: Bytes,
}

#[repr(u8)]
enum NodeType {
    Node4,
    Node16,
    Node28,
    Node256,
}

#[repr(C)]
pub(crate) struct IntenalNode {
    // Implement optimistic locking.
    // | version: 62 bits | locked: 1 bit | obsoleted: 1 bit |
    version: AtomicU64,

    prefix_len: usize, // maybe lazy prefix.
    prefix: [u8; Self::PREFIX_SIZE],
    num_children: u16,
    node_type: NodeType,
}

impl IntenalNode {
    pub(crate) const PREFIX_SIZE: usize = 10;
}

// Some action like `&IntenalNode`
impl ReadGuard<'_> {}

// Some action like `&mut IntenalNode`
impl WriteGuard<'_> {}

#[repr(C)]
#[repr(align(64))]
struct Node4 {
    base: IntenalNode,
    keys: [u8; 4],
    children: [NodePtr; 4],
}

#[repr(C)]
struct Node16 {
    base: IntenalNode,
    keys: [u8; 16],
    children: [NodePtr; 16],
}

#[repr(C)]
struct Node28 {
    base: IntenalNode,
    keys: [u8; 28],
    children: [NodePtr; 28],
}

#[repr(C)]
struct Node256 {
    base: IntenalNode,
    children: [NodePtr; 256],
}

/* #region Optimistic lock implementation */

fn mark_lock(version: u64) -> u64 {
    version + 0b10
}

fn is_obsolted(version: u64) -> bool {
    version & 0b01 != 0
}

fn is_locked(version: u64) -> bool {
    version & 0b10 != 0
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ArtOptLockError {
    #[error("Version mismatch")]
    VersionMismatch,

    #[error("Node is write locked")]
    Locked,

    #[error("Node is obsoleted")]
    Obsoleted,
}

use std::sync::atomic::Ordering::*;
use ArtOptLockError::*;

impl IntenalNode {
    pub(crate) fn read<'a>(ptr: IntenalPtr) -> Result<ReadGuard<'a>, ArtOptLockError> {
        Ok(ReadGuard::new(ptr, Self::check_version(ptr)?))
    }

    pub(crate) fn write<'a>(ptr: IntenalPtr) -> Result<WriteGuard<'a>, ArtOptLockError> {
        let version = Self::check_version(ptr)?;
        unsafe {
            match ptr.as_ref().version.compare_exchange(
                version,
                mark_lock(version),
                Acquire,
                Relaxed,
            ) {
                Ok(_) => Ok(WriteGuard::new(ptr)),
                Err(_) => Err(VersionMismatch),
            }
        }
    }

    fn check_version(ptr: IntenalPtr) -> Result<u64, ArtOptLockError> {
        let version = unsafe { ptr.as_ref().version.load(Acquire) };
        if is_locked(version) {
            return Err(Locked);
        }
        if is_obsolted(version) {
            return Err(Obsoleted);
        }
        Ok(version)
    }
}

pub(crate) struct ReadGuard<'a> {
    ptr: IntenalPtr,
    version: u64,
    _marker: PhantomData<&'a IntenalNode>,
}

impl ReadGuard<'_> {
    fn new(ptr: IntenalPtr, version: u64) -> Self {
        Self {
            ptr,
            version,
            _marker: PhantomData,
        }
    }

    fn as_ref(&self) -> &IntenalNode {
        unsafe { self.ptr.as_ref() }
    }

    pub(crate) fn check_version(&self) -> Result<(), ArtOptLockError> {
        if self.version == IntenalNode::check_version(self.ptr)? {
            Ok(())
        } else {
            Err(VersionMismatch)
        }
    }

    pub(crate) fn unlock(self) -> Result<(), ArtOptLockError> {
        self.check_version()
    }

    pub(crate) fn upgrade<'a>(self) -> Result<WriteGuard<'a>, (Self, ArtOptLockError)> {
        IntenalNode::write(self.ptr).map_err(|e| (self, e))
    }
}

pub(crate) struct WriteGuard<'a> {
    ptr: IntenalPtr,
    _marker: PhantomData<&'a mut IntenalNode>,
}

impl WriteGuard<'_> {
    fn new(ptr: IntenalPtr) -> Self {
        Self {
            ptr,
            _marker: PhantomData,
        }
    }

    fn as_ref(&self) -> &IntenalNode {
        unsafe { self.ptr.as_ref() }
    }

    fn as_mut(&mut self) -> &mut IntenalNode {
        unsafe { self.ptr.as_mut() }
    }

    pub(crate) fn mark_obsolte(&mut self) {
        self.as_ref().version.fetch_or(0b01, Release);
    }
}

impl Drop for WriteGuard<'_> {
    fn drop(&mut self) {
        self.as_ref().version.fetch_add(0b10, Release);
    }
}

/* #endregion Optimistic lock implementation */

/* #region Drop node */

impl NodePtr {
    pub(crate) fn drop_node(ptr: NodePtr) {
        todo!()
    }
}

/* #endregion Drop node */
