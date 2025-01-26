use std::path::Path;

use bytes::{BufMut, Bytes, BytesMut};

use crate::error::{Error, Result};

/// WAL (Write-Ahead-Log) is a log file that records all changes to the database.
///
/// One recovery log file will store some blocks of data, each of block will contains some records.
/// The block format like this:
///
/// RE-Log file: | Block 1 | Block 2 | ... | Block n |
///
/// Block: | Record 1 | Record 2 | ... | Record n |
///
/// Record: | payload len: 2 bytes |  record type: 1 byte | payload: dyn len | check sum: 4 bytes |
///
/// A recovery file corresponds to a memtable.

const BLOCK_SIZE: usize = 8 * 1024 * 1024;

#[repr(u8)]
#[derive(Debug, Clone, Copy)]
enum RecordType {
    None = 0, // Invalid record type.
    First = 1,
    Middle = 2,
    Last = 3,
    Full = 4,
}

#[derive(Debug)]
struct Record {
    payload: Bytes,
    ty: RecordType,
}

impl Record {
    fn encord(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(self.payload.len() + 7);
        buf.put_u16(self.payload.len() as u16);
        buf.put_u8(self.ty as u8);
        buf.put(self.payload.as_ref());
        let crc = crc32fast::hash(buf.as_ref());
        buf.put_u32(crc);
        buf.freeze()
    }
}

#[cfg(test)]
mod tests {
    use bytes::{Buf, BufMut, Bytes, BytesMut};

    use super::{Record, RecordType};

    #[test]
    fn re_log_encord() {
        let data = Bytes::copy_from_slice(b"key-value");
        let record = Record {
            payload: data.clone(),
            ty: RecordType::Full,
        };

        let encord = record.encord();
        assert_eq!(encord.len(), 7 + data.len());

        let mut buf = &encord[..];
        let len = buf.get_u16();
        assert_eq!(len, data.len() as u16);

        let ty = buf.get_u8();
        assert_eq!(ty, RecordType::Full as u8);

        let payload = &buf[..data.len()];
        assert_eq!(payload, data.as_ref());

        let mut buf = &buf[data.len()..];
        let crc = buf.get_u32();

        let mut buf = BytesMut::with_capacity(data.len() + 7);
        buf.put_u16(data.len() as u16);
        buf.put_u8(RecordType::Full as u8);
        buf.put(data.as_ref());

        let calc_crc32 = crc32fast::hash(buf.as_ref());

        assert_eq!(crc, calc_crc32);
    }
}
