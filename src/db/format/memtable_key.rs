use bytes::{Bytes, BytesMut};

/// The key used to store the key and value type in the memtable.
///
/// The format like this:
///
/// | LookupKey | var value len | value |
#[derive(Clone)]
pub struct MemTableKey {
    bytes: Bytes,
}
