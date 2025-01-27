use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::{Error, Result};

const ENTRY_HEADER_SIZE: usize = 8;

/// The entry for vlog file. It contains the key and value.
///
/// **WARNING**: `key len` and `value len` use 4 bytes to store the length,
/// so the limit of the length is 2^32 - 1, which is 4GB.
///
/// The format of `Entry` in vlog file like this:
///
/// ```text
///   +---------------------------------------+
///   | key len: 4 bytes | value len: 4 bytes |
///   +---------------------------------------+
///   |  key bytes                            |
///   +---------------------------------------+
///   |  value bytes                          |
///   +---------------------------------------+  
///   |  check sum (crc32): 4 bytes           |
///   +---------------------------------------+  
///
/// ```
///
/// Checksum will be calculated for the key, value and len,
/// and stored in the entry.
///
#[derive(Debug, Clone)]
pub struct Entry {
    key: Bytes,
    value: Bytes,
    // meta: u32,  reserve
}

impl Entry {
    pub fn new(key: Bytes, value: Bytes) -> Self {
        Self { key, value }
    }

    pub fn encode(&self) -> Bytes {
        let mut buf =
            BytesMut::with_capacity(ENTRY_HEADER_SIZE + self.key.len() + self.value.len() + 4);
        buf.put_u32(self.key.len() as u32);
        buf.put_u32(self.value.len() as u32);
        buf.put(self.key.as_ref());
        buf.put(self.value.as_ref());
        let crc = crc32fast::hash(buf.as_ref());
        buf.put_u32(crc);
        buf.freeze()
    }

    pub fn decode(bytes: Bytes) -> Result<Self> {
        if bytes.len() < ENTRY_HEADER_SIZE + 4 {
            return Err(Error::InvalidVlogEntry("buf too short".to_string()));
        }

        let mut ptr = &bytes[..];
        let key_len = ptr.get_u32() as usize;
        let value_len = ptr.get_u32() as usize;
        if key_len + value_len + ENTRY_HEADER_SIZE + 4 > bytes.len() {
            return Err(Error::InvalidVlogEntry("buf length not match".to_string()));
        }

        let key = bytes.slice(ENTRY_HEADER_SIZE..ENTRY_HEADER_SIZE + key_len);
        let value =
            bytes.slice(ENTRY_HEADER_SIZE + key_len..ENTRY_HEADER_SIZE + key_len + value_len);
        let crc = (&bytes[ENTRY_HEADER_SIZE + key_len + value_len..]).get_u32();

        let calc_crc = crc32fast::hash(&bytes[..ENTRY_HEADER_SIZE + key_len + value_len]);
        if crc != calc_crc {
            return Err(Error::InvalidVlogEntry("crc not match".to_string()));
        }

        Ok(Self { key, value })
    }
}

// pub struct ValuePointer {
//     file_id: u32,
//     oofset: u64,
// }

#[cfg(test)]
mod tests {
    use bytes::{Buf, BufMut, Bytes, BytesMut};

    use crate::vlog::ENTRY_HEADER_SIZE;

    use super::Entry;

    #[test]
    fn entry_encode_decode() -> anyhow::Result<()> {
        let key = Bytes::copy_from_slice(b"key");
        let value = Bytes::copy_from_slice(b"value");

        let entry = Entry::new(key.clone(), value.clone());

        let encode = entry.encode();
        assert_eq!(
            encode.len(),
            ENTRY_HEADER_SIZE + key.len() + value.len() + 4
        );
        let mut ptr = &encode[..];
        assert_eq!(ptr.get_u32(), key.len() as u32);
        assert_eq!(ptr.get_u32(), value.len() as u32);
        assert_eq!(&ptr[..key.len()], key.as_ref());
        let ptr = &ptr[key.len()..];
        assert_eq!(&ptr[..value.len()], value.as_ref());
        let mut ptr = &ptr[value.len()..];
        let crc = ptr.get_u32();

        let mut buf = BytesMut::new();
        buf.put_u32(key.len() as u32);
        buf.put_u32(value.len() as u32);
        buf.put(key.as_ref());
        buf.put(value.as_ref());
        let calc_crc = crc32fast::hash(buf.as_ref());
        assert_eq!(crc, calc_crc);

        let decode = Entry::decode(encode)?;
        assert_eq!(decode.key, key);
        assert_eq!(decode.value, value);

        Ok(())
    }

    #[test]
    fn entry_decode_failed() -> anyhow::Result<()> {
        let buf = Bytes::copy_from_slice(b"keyvalue");
        let res = Entry::decode(buf);
        assert!(res.is_err());
        Ok(())
    }
}
