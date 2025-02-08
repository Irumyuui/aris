use bytes::BufMut;

use crate::utils::varint::VarInt;

pub(crate) const MAX_ENCODE_LEN: usize = 10 + 10;

pub(crate) const MAGIT_NUMBER: u64 = 1145141919810;

#[derive(Debug, Clone, Copy)]
pub struct BlockHandle {
    offset: u64,
    size: u64,
}

impl BlockHandle {
    pub fn new(offset: u64, size: u64) -> Self {
        Self { offset, size }
    }

    pub fn encode_to(&self, buf: &mut impl BufMut) {
        VarInt::put_varint(&self.offset, buf);
        VarInt::put_varint(&self.size, buf);
    }

    pub fn set_offset(&mut self, offset: u64) {
        self.offset = offset;
    }

    pub fn set_size(&mut self, size: u64) {
        self.size = size;
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(MAX_ENCODE_LEN);
        self.encode_to(&mut buf);
        buf
    }
}

pub struct Footer {
    meta_index_handle: BlockHandle,
    index_handle: BlockHandle,
}

impl Footer {
    pub fn new(meta_index_handle: BlockHandle, index_handle: BlockHandle) -> Self {
        Self {
            meta_index_handle,
            index_handle,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(MAX_ENCODE_LEN * 2 + std::mem::size_of::<u64>());
        self.meta_index_handle.encode_to(&mut buf);
        self.index_handle.encode_to(&mut buf);
        buf.put_u64_le(MAGIT_NUMBER);
        assert_eq!(buf.len(), 48);
        buf
    }
}
