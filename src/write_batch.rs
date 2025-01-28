use bytes::Bytes;

#[repr(u8)]
#[derive(Debug, Clone, Copy)]
pub enum WriteType {
    Delete = 0,
    Value = 1,
}

pub struct WriteEntry {
    user_key: Bytes,
    user_value: Bytes,
    w_type: WriteType,
}

pub struct WriteBatch {
    seq: Option<u64>,
    entries: Vec<WriteEntry>,
}

impl Default for WriteBatch {
    fn default() -> Self {
        Self {
            seq: None,
            entries: Vec::new(),
        }
    }
}

impl WriteBatch {
    pub(crate) fn set_seq(&mut self, seq: u64) {
        self.seq.replace(seq);
    }

    pub fn put(&mut self, key: Bytes, value: Bytes) {
        self.entries.push(WriteEntry {
            user_key: key,
            user_value: value,
            w_type: WriteType::Value,
        });
    }

    pub fn delete(&mut self, key: Bytes) {
        self.entries.push(WriteEntry {
            user_key: key,
            user_value: Bytes::new(),
            w_type: WriteType::Delete,
        });
    }
}
