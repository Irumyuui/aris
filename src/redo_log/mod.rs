/// Log file format:
///
/// ```text
///     +---------+
///     | Block 1 |
///     +---------+
///     | Block 2 |
///     +---------+
///     |   ...   |
///     +---------+
///     | Block n |
///     +---------+
///
/// ```
///
/// Block format:
///
/// ```text
///     +---------------+
///     | header | data |
///     +---------------+
///     | header | data |
///     +---------------+
///     |      ...      |
///     +---------------+
///     | header | data |
///     +---------------+
/// ```
///
/// Header format:
///
/// ```text
///     +---------------------------------+
///     | crc32 4b | data len 2b | ty: 1b |
///     +---------------------------------+
/// ```
pub mod reader;

pub const HEADER_SIZE: usize = 7;
pub const BLOCK_SIZE: usize = 32 * 1024;

#[derive(Debug, Clone, Copy)]
pub enum RecordType {
    Full = 1,
    First = 2,
    Mid = 3,
    Last = 4,
}

impl From<u8> for RecordType {
    fn from(value: u8) -> Self {
        match value {
            1 => RecordType::Full,
            2 => RecordType::First,
            3 => RecordType::Mid,
            4 => RecordType::Last,
            _ => panic!("Invalid record type, found number: {}", value),
        }
    }
}
