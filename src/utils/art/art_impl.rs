use std::{ptr::NonNull, sync::Arc};

use bytes::Bytes;
use crossbeam::epoch::Guard;

use super::node::{ArtOptLockError, IntenalPtr, NodePtr};

#[derive(Clone)]
pub struct Art {
    inner: Arc<ArtInner>,
}

struct ArtInner {
    root: IntenalPtr,
}

unsafe impl Send for ArtInner {}
unsafe impl Sync for ArtInner {}

impl ArtInner {
    fn new() -> Self {
        todo!()
    }

    fn get_inner(&self, key: &Bytes, _guard: &Guard) -> Result<Option<&Bytes>, ArtOptLockError> {
        todo!()
    }

    fn get(&self, key: &Bytes, guard: &Guard) -> Option<&Bytes> {
        'retry: loop {
            match self.get_inner(key, guard) {
                Ok(res) => return res,
                Err(_) => continue 'retry,
            }
        }
    }

    fn insert(&self, key: Bytes, value: Bytes, guard: &Guard) {
        todo!()
    }
}

impl Drop for ArtInner {
    fn drop(&mut self) {
        NodePtr::drop_node(NodePtr::Intenal { ptr: self.root });
    }
}
