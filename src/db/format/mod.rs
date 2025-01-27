pub mod internal_key;
pub mod lookup_key;
pub mod memtable_key;

#[cfg(test)]
mod tests;

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
#[derive(Debug, Clone, Copy)]
pub enum ValueType {
    /// Mark the key is deleted. The value is not used (zero length).
    Deleted = 0,

    /// This means that the key is inserted or update with the value.
    Value = 1,

    /// This means that the value is stored in the v-log file,
    /// so the value will contains (file_no, file_offset).
    ValueLog = 2,
}

pub(crate) fn pack_value_type_and_seq(seq: SeqNumber, value_type: ValueType) -> u64 {
    (seq << 8) | value_type as u64
}

pub(crate) fn unpack_value_type_and_seq(data: u64) -> (SeqNumber, ValueType) {
    let seq = data >> 8;
    let value_type = unsafe { std::mem::transmute((data & 0xFF) as u8) };

    (seq, value_type)
}
