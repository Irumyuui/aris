use std::{cmp::Ordering, sync::Arc};

use bytes::{Buf, Bytes};

use crate::{
    comparator::Comparator,
    error::{DBError, DBResult},
    iterator::Iterator,
    utils::varint::VarInt,
};

use super::SIZE_U32;

pub struct Block {
    data: Bytes,
    restart_offset: u32,
    restart_count: u32,
}

impl Block {
    pub fn new(data: Bytes) -> DBResult<Self> {
        if data.len() < 4 {
            return Err(DBError::Corruption("block data too short".into()));
        }

        let max_restarts_allowed = (data.len() - SIZE_U32) / SIZE_U32;
        let restart_count = Self::get_restart_count(&data);

        if restart_count as usize <= max_restarts_allowed {
            let restart_offset = data.len() as u32 - (1 + restart_count) * SIZE_U32 as u32;
            return Ok(Self {
                data,
                restart_offset,
                restart_count,
            });
        }

        return Err(DBError::Corruption("block data invalid".into()));
    }

    pub fn iter(&self, comp: Arc<dyn Comparator>) -> BlockIter {
        BlockIter::new(
            self.data.clone(),
            self.restart_offset,
            self.restart_count,
            comp,
        )
    }

    fn get_restart_count(data: &[u8]) -> u32 {
        (&data[data.len() - SIZE_U32..]).get_u32_le()
    }
}

pub struct BlockIter {
    comparator: Arc<dyn Comparator>,
    data: Bytes,

    restart_offset: u32,
    restart_count: u32,

    // current
    current: u32,
    restart_index: u32,

    key: Vec<u8>,
    // value: Bytes,
    shared_len: u32,
    non_shared_len: u32,
    key_offset: u32,
    value_len: u32,

    status: Option<DBError>,
}

impl BlockIter {
    fn new(
        data: Bytes,
        restart_offset: u32,
        restart_count: u32,
        comp: Arc<dyn Comparator>,
    ) -> Self {
        let this = Self {
            comparator: comp,
            data,

            restart_offset,
            restart_count,

            current: restart_count, // invalid status
            restart_index: 0,

            key: Vec::new(),
            shared_len: 0,
            non_shared_len: 0,
            key_offset: 0,
            value_len: 0,

            status: None,
        };
        return this;
    }

    fn next_entry_offset(&self) -> u32 {
        self.key_offset + self.non_shared_len + self.value_len
    }

    fn get_restart_point(&self, index: u32) -> u32 {
        assert!(index < self.restart_count);
        (&self.data[self.restart_offset as usize + index as usize * SIZE_U32..]).get_u32_le()
    }

    fn seek_to_restart_point(&mut self, index: u32) {
        self.key.clear();
        self.restart_index = index;

        let offset = self.get_restart_point(index);
        self.current = offset;
    }

    fn parse_next_entry(&mut self) -> bool {
        // 来到了重启点数据的范围
        if self.current >= self.restart_offset {
            self.current = self.restart_offset;
            self.restart_index = self.restart_count;
            return false;
        }

        // parse entry
        let mut offset = self.current as usize;
        let (shared_len, next): (u32, _) = VarInt::from_varint(&self.data[offset..]).unwrap();
        offset += next;
        let (non_shared_len, next): (u32, _) = VarInt::from_varint(&self.data[offset..]).unwrap();
        offset += next;
        let (value_len, next): (u32, _) = VarInt::from_varint(&self.data[offset..]).unwrap();
        offset += next;
        if offset as u32 + non_shared_len + value_len > self.restart_offset {
            self.corruption();
            return false;
        }

        self.key_offset = offset as u32;
        self.shared_len = shared_len;
        self.non_shared_len = non_shared_len;
        self.value_len = value_len;

        let key_len = shared_len + non_shared_len;
        self.key.resize(key_len as usize, 0);
        let buf = &self.data[self.key_offset as usize..(self.key_offset + non_shared_len) as usize];
        for i in shared_len as usize..key_len as usize {
            self.key[i] = buf[i - shared_len as usize];
        }

        while self.restart_index + 1 < self.restart_count
            && self.get_restart_point(self.restart_index + 1) < self.current
        {
            self.restart_index += 1;
        }
        return true;
    }

    fn corruption(&mut self) {
        self.current = self.restart_offset;
        self.restart_index = self.restart_count;
        self.status = Some(DBError::Corruption("bad entry in block".into()));
    }

    #[inline]
    fn required_valid(&self) {
        assert!(self.is_valid());
    }
}

impl Iterator for BlockIter {
    fn is_valid(&self) -> bool {
        self.status.is_none() && self.current < self.restart_count
    }

    fn next(&mut self) {
        self.required_valid();
        self.current = self.next_entry_offset();
        self.parse_next_entry();
    }

    fn prev(&mut self) {
        self.required_valid();

        let original = self.current;
        while self.get_restart_point(self.restart_index) >= original {
            if self.restart_count == 0 {
                self.current = self.restart_offset;
                self.restart_index = self.restart_count;
                return;
            }
            self.restart_index -= 1;
        }

        self.seek_to_restart_point(self.restart_index);
        while self.parse_next_entry() && self.next_entry_offset() < original {
            self.current = self.next_entry_offset();
        }
    }

    fn key(&self) -> &[u8] {
        self.required_valid();
        &self.key
    }

    fn value(&self) -> &[u8] {
        self.required_valid();
        let start = (self.next_entry_offset() - self.value_len) as usize;
        let end = start + self.value_len as usize;
        &self.data[start..end]
    }

    fn seek_to_first(&mut self) {
        self.seek_to_restart_point(0);
        self.parse_next_entry();
    }

    fn seek_to_last(&mut self) {
        assert!(self.restart_count > 0);
        self.seek_to_restart_point(self.restart_count - 1);
        while self.parse_next_entry() && self.next_entry_offset() < self.restart_offset {
            self.current = self.next_entry_offset();
        }
    }

    fn seek(&mut self, target: &[u8]) {
        let mut l = 0;
        let mut r = self.restart_count - 1;

        while l < r {
            let mid = (l + r + 1) / 2;
            let region_offset = self.get_restart_point(mid);

            let mut offset = region_offset as usize;
            let (shared_len, next): (u32, _) = VarInt::from_varint(&self.data[offset..]).unwrap();
            offset += next;
            let (non_shared_len, next): (u32, _) =
                VarInt::from_varint(&self.data[offset..]).unwrap();
            offset += next;
            let (_value_len, next): (u32, _) = VarInt::from_varint(&self.data[offset..]).unwrap();
            offset += next;

            let key_offset = offset;
            if shared_len != 0 {
                self.corruption();
                return;
            }

            let key_len = (shared_len + non_shared_len) as usize;
            let mid_key = &self.data[key_offset as usize..key_offset + key_len];
            match self.comparator.compare(mid_key, target) {
                Ordering::Less => l = mid,
                _ => r = mid - 1,
            }
        }

        self.seek_to_restart_point(l);
        loop {
            if !self.parse_next_entry() {
                return;
            }
            match self.comparator.compare(&self.key, target) {
                Ordering::Less => {}
                _ => return,
            }
            self.current = self.next_entry_offset();
        }
    }

    fn status(&mut self) -> DBResult<()> {
        match self.status.take() {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }
}
