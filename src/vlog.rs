use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::{Error, Result};

const ENTRY_HEADER_SIZE: usize = 8 + 1;

#[repr(u8)]
#[derive(Debug, Clone, Copy)]
pub enum ValueType {
    Delete = 0,
    Value = 1,
}

impl TryFrom<u8> for ValueType {
    type Error = Error;

    fn try_from(value: u8) -> std::result::Result<Self, Self::Error> {
        match value {
            0 => Ok(ValueType::Delete),
            1 => Ok(ValueType::Value),
            _ => Err(Error::InvalidVlogEntry(format!(
                "invalid value type: {}",
                value
            ))),
        }
    }
}

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
///   |  entry meta: 1 bytes                  |
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
    meta: ValueType, // reserve
}

impl Entry {
    pub fn new(key: Bytes, value: Bytes, meta: ValueType) -> Self {
        Self { key, value, meta }
    }

    pub fn encode(&self) -> Bytes {
        let mut buf =
            BytesMut::with_capacity(ENTRY_HEADER_SIZE + self.key.len() + self.value.len() + 4);
        buf.put_u32(self.key.len() as u32);
        buf.put_u32(self.value.len() as u32);
        buf.put_u8(self.meta as u8);
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
        let value_type = ValueType::try_from(ptr.get_u8())?;

        let key = bytes.slice(ENTRY_HEADER_SIZE..ENTRY_HEADER_SIZE + key_len);
        let value =
            bytes.slice(ENTRY_HEADER_SIZE + key_len..ENTRY_HEADER_SIZE + key_len + value_len);
        let crc = (&bytes[ENTRY_HEADER_SIZE + key_len + value_len..]).get_u32();

        let calc_crc = crc32fast::hash(&bytes[..ENTRY_HEADER_SIZE + key_len + value_len]);
        if crc != calc_crc {
            return Err(Error::InvalidVlogEntry("crc not match".to_string()));
        }

        Ok(Entry::new(key, value, value_type))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct EntryPointer {
    pub(crate) file_id: u32,
    pub(crate) offset: u64,
    pub(crate) len: u64,
}

pub fn gen_vlog_file_path(file_id: u32) -> String {
    format!("{:09}.vlog", file_id)
}

pub struct VLogWriter {
    file: std::fs::File,
    file_id: u32,
    offset: u64,
    ring: rio::Rio,
}

impl VLogWriter {
    pub fn new(ring: rio::Rio, file_id: u32) -> std::io::Result<Self> {
        let file = std::fs::OpenOptions::new()
            .read(false)
            .write(true)
            .create(true)
            .open(gen_vlog_file_path(file_id))?;

        Ok(Self {
            file,
            file_id,
            offset: 0,
            ring,
        })
    }

    pub async fn write_entry(&mut self, entry: Entry) -> Result<EntryPointer> {
        let buf = entry.encode();
        let offset = self.offset;
        let result = self
            .ring
            .write_at(&self.file, &buf.as_ref(), offset)
            .await?;

        if result != buf.len() {
            return Err(Error::VLogFileCorrupted(format!(
                "write size not match, expect: {}, actual: {}",
                buf.len(),
                result
            )));
        }

        self.offset += buf.len() as u64;
        Ok(EntryPointer {
            file_id: self.file_id,
            offset,
            len: buf.len() as u64,
        })
    }
}

pub struct VLogReader {
    // file: std::fs::File,
    // file_id: u32,
    ring: rio::Rio,
}

impl VLogReader {
    pub fn new(ring: rio::Rio) -> std::io::Result<Self> {
        Ok(Self { ring })
    }

    pub async fn read_entry(&self, pointer: EntryPointer) -> Result<Entry> {
        let file = std::fs::OpenOptions::new()
            .create(false)
            .read(true)
            .write(false)
            .open(gen_vlog_file_path(pointer.file_id))?;

        self.read_entry_from_file(&file, pointer.offset, pointer.len)
            .await
    }

    async fn read_entry_from_file(
        &self,
        file: &std::fs::File,
        offset: u64,
        len: u64,
    ) -> Result<Entry> {
        if len < 4 + ENTRY_HEADER_SIZE as u64 {
            return Err(Error::VLogFileCorrupted("entry ptr too short".to_string()));
        }

        let mut buf = BytesMut::zeroed(len as usize);
        let result = self.ring.read_at(file, &mut buf.as_mut(), offset).await?;

        if result != buf.len() {
            return Err(Error::VLogFileCorrupted(format!(
                "read size not match, expect: {}, actual: {}",
                buf.len(),
                result
            )));
        }

        let entry = Entry::decode(buf.freeze())?;
        Ok(entry)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::{Buf, BufMut, Bytes, BytesMut};
    use tempfile::tempfile;
    use tokio::task::JoinSet;

    use crate::{
        utils::rio_config::RioConfigWrapper,
        vlog::{VLogReader, VLogWriter, ValueType, ENTRY_HEADER_SIZE},
    };

    use super::Entry;

    #[test]
    fn entry_encode_decode() -> anyhow::Result<()> {
        let key = Bytes::copy_from_slice(b"key");
        let value = Bytes::copy_from_slice(b"value");

        let entry = Entry::new(key.clone(), value.clone(), ValueType::Value);

        let encode = entry.encode();
        assert_eq!(
            encode.len(),
            ENTRY_HEADER_SIZE + key.len() + value.len() + 4
        );
        let mut ptr = &encode[..];
        assert_eq!(ptr.get_u32(), key.len() as u32);
        assert_eq!(ptr.get_u32(), value.len() as u32);
        assert_eq!(ptr.get_u8(), ValueType::Value as u8);
        assert_eq!(&ptr[..key.len()], key.as_ref());
        let ptr = &ptr[key.len()..];
        assert_eq!(&ptr[..value.len()], value.as_ref());
        let mut ptr = &ptr[value.len()..];
        let crc = ptr.get_u32();

        let mut buf = BytesMut::new();
        buf.put_u32(key.len() as u32);
        buf.put_u32(value.len() as u32);
        buf.put_u8(ValueType::Value as u8);
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

    fn gen_entries(count: usize) -> Vec<Entry> {
        (0..count)
            .map(|i| {
                Entry::new(
                    Bytes::from(format!("key-{i:05}")),
                    Bytes::from(format!("value-{i:05}")),
                    ValueType::Value,
                )
            })
            .collect()
    }

    #[tokio::test]
    async fn write_entry() -> anyhow::Result<()> {
        let ring = RioConfigWrapper::new().depth(1024).build()?;
        let file = tempfile()?;

        let mut writer = VLogWriter {
            ring: ring.clone(),
            file_id: 0,
            file: file.try_clone()?,
            offset: 0,
        };

        let entries = gen_entries(100);
        for e in &entries {
            writer.write_entry(e.clone()).await?;
        }
        drop(writer);

        let mut buf = BytesMut::new();
        for e in &entries {
            buf.extend(e.encode());
        }
        let buf = buf.freeze();

        let file_len = file.metadata()?.len();
        let read_buf = BytesMut::zeroed(file_len as _);

        let read_len = ring.read_at(&file, &read_buf, 0).await?;
        assert_eq!(read_len, file_len as usize);

        assert_eq!(buf, read_buf.freeze());

        Ok(())
    }

    #[tokio::test]
    async fn read_entry() -> anyhow::Result<()> {
        let ring = RioConfigWrapper::new().depth(1024).build()?;
        let file = tempfile()?;

        let entries = gen_entries(100);
        let mut write_buf = BytesMut::new();
        let mut offsets = vec![];
        for e in &entries {
            let e = e.encode();
            offsets.push(write_buf.len() as u64);
            write_buf.extend(e);
        }

        let write_buf = write_buf.freeze();
        let write_len = ring.write_at(&file, &write_buf, 0).await?;
        assert_eq!(write_len, write_buf.len());

        let mut tasks = JoinSet::new();
        let file = Arc::new(file);
        for (i, offset) in offsets.iter().enumerate() {
            let reader = VLogReader::new(ring.clone())?;
            let target = entries[i].clone();
            let offset = *offset;
            let file = file.clone();

            tasks.spawn(async move {
                let entry = reader
                    .read_entry_from_file(
                        file.as_ref(),
                        offset,
                        (ENTRY_HEADER_SIZE + target.key.len() + target.value.len() + 4) as u64,
                    )
                    .await
                    .unwrap();
                assert_eq!(entry.key, target.key);
                assert_eq!(entry.value, target.value);
            });
        }

        tasks.join_all().await;
        Ok(())
    }
}
