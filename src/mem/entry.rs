use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::vlog::{ValuePointer, ValueType};

const HEADER: usize = 13;

/// `MemKeyEntry` format:
///
/// ```text
///     +------------------------------------------------------+
///     | key_len: 4 bytes | value type: 1 byte | seq: 8 bytes |
///     +------------------------------------------------------+
///     |        key data        |      value ptr (may null)   |
///     +------------------------------------------------------+
/// ```
pub(crate) struct ParsedMemKeyEntry {
    key: Bytes,
    seq: u64,
    value_type: ValueType,
    value_ptr: Option<ValuePointer>,
}

impl ParsedMemKeyEntry {
    pub fn new(
        key: Bytes,
        seq: u64,
        value_type: ValueType,
        value_ptr: Option<ValuePointer>,
    ) -> Self {
        Self {
            key,
            seq,
            value_type,
            value_ptr,
        }
    }

    pub fn encode(&self) -> MemKeyEntry {
        let mut buf = BytesMut::with_capacity(
            HEADER
                + self.key.len()
                + self
                    .value_ptr
                    .map(|_| ValuePointer::ENCODE_SIZE)
                    .unwrap_or(0),
        );

        buf.put_u32(self.key.len() as _);
        buf.put_u8(self.value_type as _);
        buf.put_u64(self.seq);
        buf.put(self.key.as_ref());
        if let Some(ptr) = self.value_ptr.map(|ptr| ptr.encode()) {
            buf.put(&ptr[..]);
        }

        MemKeyEntry { data: buf.freeze() }
    }
}

#[derive(Clone)]
pub(crate) struct MemKeyEntry {
    data: Bytes,
}

impl MemKeyEntry {
    #[inline]
    pub(crate) fn key_len(&self) -> usize {
        (&self.data[..]).get_u32() as usize
    }

    #[inline]
    pub(crate) fn value_type(&self) -> u8 {
        self.data[4]
    }

    #[inline]
    pub(crate) fn seq(&self) -> u64 {
        (&self.data[5..]).get_u64()
    }

    #[inline]
    pub(crate) fn key(&self) -> &[u8] {
        let key_len = self.key_len();
        &self.data[HEADER..HEADER + key_len]
    }

    #[inline]
    pub(crate) fn intenal_key(&self) -> &[u8] {
        let key_len = self.key_len();
        &self.data[..HEADER + key_len]
    }

    #[inline]
    pub(crate) fn value_ptr(&self) -> Option<&[u8]> {
        let key_len = self.key_len();
        let value_len = self.data.len() - HEADER - key_len;
        if value_len == 0 {
            None
        } else {
            Some(&self.data[HEADER + key_len..])
        }
    }

    #[inline]
    pub(crate) fn compare_inner(&self, other: &Self) -> std::cmp::Ordering {
        let res = self.key_len().cmp(&other.key_len());
        if !res.is_eq() {
            return res;
        }

        let res = self.value_type().cmp(&other.value_type());
        if !res.is_eq() {
            return res;
        }

        let res = self.seq().cmp(&other.seq());
        if !res.is_eq() {
            return res;
        }

        let res = self.key().cmp(other.key());
        res
    }

    #[inline]
    pub(crate) fn bytes(&self) -> usize {
        self.data.len()
    }
}

impl PartialEq for MemKeyEntry {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.compare_inner(other).is_eq()
    }
}

impl PartialOrd for MemKeyEntry {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.compare_inner(other))
    }
}

impl Eq for MemKeyEntry {}

impl Ord for MemKeyEntry {
    #[inline]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.compare_inner(other)
    }
}
