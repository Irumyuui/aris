use bytes::Bytes;

/// Only use 7 bytes.
///
/// The format like this:
///
/// | SeqNumber: 7 bytes | value type: 1 bytes |
///
/// But we will use the lower seq number.
pub type SeqNumber = u64;

pub const MAX_SEQ: SeqNumber = 0x00FFFFFFFFFFFFFF;

#[repr(u8)]
#[derive(Debug)]
pub enum ValueType {
    /// Mark the key is deleted. The value is not used (zero length).
    Deleted = 0,

    /// This means that the key is inserted or update with the value.
    Value = 1,

    /// This means that the value is stored in the v-log file,
    /// so the value will contains (file_no, file_offset).
    ValueLog = 2,
}

/// The internal key is used to store the key in the sstable.
///
/// The format like this:
///
/// | user key | seq number | value type |
pub struct ParsedInternalKey {
    user_key: Bytes,
    seq: SeqNumber,
    value_type: ValueType,
}

pub struct InternalKey {
    data: Bytes,
}

/// `LookupKey` is used to search the key.
///
/// The format like this:
///
/// | varlen of internal key | internal key |
pub struct LookupKey {
    data: Bytes,
    intelnal_key_len: usize,
}

/// The memtable entry is used to store the key-value pair in the memtable.
/// User value may is (file_no, file_offset) if the value is stored in the v-log file.
///
/// The format like this:
///
/// | var user key len | user key | seq number | value type | var user value len | user value |
pub struct MemTableEntry {
    user_key: Bytes,
    seq: SeqNumber,
    value_type: ValueType,
    user_value: Bytes,
}
