use std::sync::{atomic::AtomicUsize, Arc};

use bytes::{BufMut, Bytes, BytesMut};
use crossbeam_skiplist::SkipMap;

use crate::{vlog::ValuePointer, write_batch::WriteType};

// TODO: Maybe use arena or not.
#[derive(Debug, Clone)]
pub struct MemTable {
    inner: Arc<MemTableInner>,
}

impl MemTable {
    #[inline]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(MemTableInner::new()),
        }
    }

    #[inline]
    pub fn approximate_mem_usage(&self) -> usize {
        self.inner.approximate_mem_usage()
    }

    #[inline]
    pub fn add(&self, seq: u64, ty: WriteType, key: Bytes, value_ptr: ValuePointer) {
        self.inner.add(seq, ty, key, value_ptr);
    }

    #[inline]
    pub fn get(&self, key: &LookupKey) -> Option<ValuePointer> {
        self.inner.get(key)
    }
}

#[derive(Debug)]
struct MemTableInner {
    table: SkipMap<Bytes, ValuePointer>,
    // table: SkipSet<Bytes>,
    mem_usage: AtomicUsize,
}

impl MemTableInner {
    fn new() -> Self {
        Self {
            table: Default::default(),
            mem_usage: AtomicUsize::new(0),
        }
    }

    fn approximate_mem_usage(&self) -> usize {
        self.mem_usage.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Insert internal key into memtable.
    ///
    /// - If found a same key and seq, just **update the value inplace**.
    /// - No costom comparator, use byte order.
    ///
    /// Key format like this:
    ///
    /// ```text
    ///     +---------------------------------------------+
    ///     | key bytes | seq num: 8 bytes | type: 1 byte |  
    ///     +---------------------------------------------+
    /// ```
    ///
    /// Value format like this:
    ///
    /// ```text
    ///     +--------------------------------------------------------------+
    ///     | value len: 4 bytes | file id: 4 bytes | file offset: 8 bytes |
    ///     +--------------------------------------------------------------+
    ///
    /// ```
    fn add(&self, seq: u64, ty: WriteType, key: Bytes, value_ptr: ValuePointer) {
        let LookupKey { bytes } = LookupKey::new(key, seq);
        let buf = bytes;

        let mem_use = buf.len() + 12;
        self.table.insert(buf, value_ptr);
        self.mem_usage
            .fetch_add(mem_use, std::sync::atomic::Ordering::Release);
    }

    fn get(&self, key: &LookupKey) -> Option<ValuePointer> {
        match self.table.get(&key.bytes) {
            Some(e) => Some(e.value().clone()),
            None => None,
        }
    }
}

pub struct LookupKey {
    bytes: Bytes,
}

impl LookupKey {
    pub fn new(key: Bytes, seq: u64) -> Self {
        let mut buf = BytesMut::with_capacity(key.len() + 7 + 1);
        buf.extend(key);
        buf.put_u64(seq);
        buf.put_u8(WriteType::Value as u8);

        Self {
            bytes: buf.freeze(),
        }
    }
}
