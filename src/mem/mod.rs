use std::sync::Arc;

use bytes::Bytes;
use entry::ParsedMemKeyEntry;
use mem_impl::MemTableInner;

use crate::vlog::{ValuePointer, ValueType};

mod entry;
mod mem_impl;

#[derive(Clone)]
pub struct MemTable {
    table: Arc<MemTableInner>,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            table: Arc::new(MemTableInner::new()),
        }
    }

    pub fn get(&self, key: &Bytes, seq: u64) -> Option<ValuePointer> {
        let lookup = ParsedMemKeyEntry::new(key.clone(), seq, ValueType::Value, None).encode();
        match self.table.get(&lookup) {
            Some(e) => e.value_ptr().map(|buf| ValuePointer::decode(buf)).flatten(),
            None => None,
        }
    }

    /// `MemKeyEntry` format:
    ///
    /// ```text
    ///     +------------------------------------------------------+
    ///     | key_len: 4 bytes | value type: 1 byte | seq: 8 bytes |
    ///     +------------------------------------------------------+
    ///     |        key data        |      value ptr (may null)   |
    ///     +------------------------------------------------------+
    /// ```
    pub fn insert(&self, key: Bytes, seq: u64, value_type: ValueType, value: Option<ValuePointer>) {
        let entry = ParsedMemKeyEntry::new(key, seq, value_type, value).encode();
        self.table.insert(entry);
    }

    pub fn mem_usage(&self) -> usize {
        self.table.mem_usage()
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use itertools::Itertools;

    use crate::vlog::{ValuePointer, ValueType};

    use super::{entry::ParsedMemKeyEntry, MemTable};

    #[test]
    fn zero_memory() {
        let mem = MemTable::new();
        assert_eq!(mem.mem_usage(), 0);
    }

    fn gen_data(count: usize) -> (Vec<Bytes>, Vec<ValuePointer>) {
        let keys = (0..count)
            .map(|i| Bytes::from(format!("key-{:05}", i)))
            .collect_vec();
        let vptrs = (0..count)
            .map(|i| ValuePointer {
                file_id: i as _,
                len: i as _,
                offset: i as _,
            })
            .collect_vec();
        (keys, vptrs)
    }

    #[test]
    fn insert_and_get() {
        let (keys, values) = gen_data(10000);

        let mem = MemTable::new();
        let mut mem_usage = 0;
        for (key, value) in keys.iter().zip(&values) {
            mem.insert(key.clone(), 0, ValueType::Value, Some(value.clone()));
            mem_usage +=
                ParsedMemKeyEntry::new(key.clone(), 0, ValueType::Value, Some(value.clone()))
                    .encode()
                    .bytes();
        }

        for (key, value) in keys.iter().zip(&values) {
            let ptr = mem.get(key, 0).unwrap();
            assert_eq!(ptr, *value);
        }
        assert_eq!(mem_usage, mem.mem_usage());
    }
}
