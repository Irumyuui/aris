use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use regex::Regex;

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
pub struct VLogEntry {
    key: Bytes,
    value: Bytes,
    meta: ValueType, // reserve
}

impl VLogEntry {
    pub fn new(key: Bytes, value: Bytes, meta: ValueType) -> Self {
        Self { key, value, meta }
    }

    pub fn encode_for_buf(&self, buf: &mut BytesMut) -> usize {
        // buf.put_u32(self.key.len() as u32);
        // buf.put_u32(self.value.len() as u32);
        // buf.put_u8(self.meta as u8);
        // buf.put(self.key.as_ref());
        // buf.put(self.value.as_ref());
        // let crc = crc32fast::hash(buf.as_ref());
        // buf.put_u32(crc);

        let e = self.encode();
        let res = e.len();
        buf.extend(e);
        res
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

        Ok(VLogEntry::new(key, value, value_type))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ValuePointer {
    pub(crate) file_id: u32,
    pub(crate) offset: u64,
    pub(crate) len: u64,
}

pub fn gen_vlog_file_path(path: &PathBuf, file_id: u32) -> PathBuf {
    path.join(format!("{:09}.vlog", file_id))
}

pub struct VLogSet {
    path: PathBuf,
    vlog_files: BTreeMap<u32, Arc<std::path::PathBuf>>,
    max_fid: u32,
    ring: rio::Rio,
    wirten_offset: u64,
    max_file_size: u64,
    current_file: Arc<std::fs::File>,
    buf: BytesMut,
}

impl VLogSet {
    pub async fn new(
        ring: rio::Rio,
        max_file_size: u64,
        vlog_file_path: impl AsRef<std::path::Path>,
    ) -> Result<Self> {
        let mut vlog_files = read_vlog_dir(vlog_file_path.as_ref())?;
        let path = PathBuf::from(vlog_file_path.as_ref());

        let (file, offset) = match vlog_files.iter().max() {
            Some((_, path)) => {
                let file = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(false)
                    .append(true)
                    .open(path.as_ref())?;
                let offset = file.metadata()?.len();
                (file, offset)
            }
            None => {
                let filepath = gen_vlog_file_path(&path, 0);
                let file = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .append(true)
                    .open(&filepath)?;
                vlog_files.insert(0, Arc::new(filepath));
                (file, 0)
            }
        };

        let max_file_id = vlog_files.keys().max().copied().unwrap() + 1;
        Ok(Self {
            path,
            vlog_files,
            max_fid: max_file_id,
            ring,
            wirten_offset: offset as u64,
            max_file_size,
            current_file: Arc::new(file),
            buf: BytesMut::with_capacity(10000),
        })
    }

    pub async fn append(&mut self, entries: &[VLogEntry]) -> Result<Vec<ValuePointer>> {
        self.buf.clear();

        let mut pointers = Vec::with_capacity(entries.len());
        for e in entries {
            let len = e.encode_for_buf(&mut self.buf);
            let ptr = ValuePointer {
                file_id: self.max_fid,
                len: len as u64,
                offset: self.wirten_offset + self.buf.len() as u64,
            };
            pointers.push(ptr);

            if self.buf.len() as u64 + self.wirten_offset >= self.max_file_size {
                self.write_buf_to_file().await?;
            }
        }

        self.write_buf_to_file().await?;
        Ok(pointers)
    }

    async fn write_buf_to_file(&mut self) -> Result<()> {
        tracing::debug!("write buf to file, len: {}", self.buf.len());

        if self.buf.is_empty() {
            return Ok(());
        }

        let res = self
            .ring
            .write_at(self.current_file.as_ref(), &self.buf, self.wirten_offset)
            .await?;
        assert_eq!(res, self.buf.len());
        self.buf.clear();

        self.wirten_offset += res as u64;
        if self.wirten_offset >= self.max_file_size {
            let filepath = gen_vlog_file_path(&self.path, self.max_fid);
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .append(true)
                .open(&filepath)?;
            self.vlog_files.insert(self.max_fid, Arc::new(filepath));
            self.current_file = Arc::new(file);
            self.max_fid += 1;
            self.wirten_offset = 0;
        }

        Ok(())
    }
}

fn read_vlog_dir(path: &Path) -> Result<BTreeMap<u32, Arc<PathBuf>>> {
    if !path.is_dir() {
        return Err(Error::VLogFileCorrupted(format!(
            "{:?} is not a directory, vlog path must be a directory",
            path
        )));
    }

    let re = Regex::new(r"^(\d+)\.vlog$").unwrap();
    let mut vlog_files = BTreeMap::new();
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let file_path = entry.path();
        if !file_path.is_file() {
            continue;
        }

        if let Some(file_name) = file_path.file_name().and_then(|n| n.to_str()) {
            if let Some(caps) = re.captures(file_name) {
                let num_str = caps.get(1).unwrap().as_str();

                match num_str.parse::<u32>() {
                    Ok(num) => {
                        let res = vlog_files.insert(num, Arc::new(file_path.clone()));
                        assert!(res.is_none());
                    }
                    Err(_) => {
                        continue;
                    }
                }
            }
        }
    }

    Ok(vlog_files)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::VLogEntry;
    use crate::{
        utils::rio_config::RioConfigWrapper,
        vlog::{read_vlog_dir, VLogSet, ValueType, ENTRY_HEADER_SIZE},
    };
    use bytes::{Buf, BufMut, Bytes, BytesMut};

    use scopeguard::defer;

    #[test]
    fn entry_encode_decode() -> anyhow::Result<()> {
        let key = Bytes::copy_from_slice(b"key");
        let value = Bytes::copy_from_slice(b"value");

        let entry = VLogEntry::new(key.clone(), value.clone(), ValueType::Value);

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

        let decode = VLogEntry::decode(encode)?;
        assert_eq!(decode.key, key);
        assert_eq!(decode.value, value);

        Ok(())
    }

    #[test]
    fn entry_decode_failed() -> anyhow::Result<()> {
        let buf = Bytes::copy_from_slice(b"keyvalue");
        let res = VLogEntry::decode(buf);
        assert!(res.is_err());
        Ok(())
    }

    fn gen_entries(count: usize) -> Vec<VLogEntry> {
        (0..count)
            .map(|i| {
                VLogEntry::new(
                    Bytes::from(format!("key-{i:05}")),
                    Bytes::from(format!("value-{i:05}")),
                    ValueType::Value,
                )
            })
            .collect()
    }

    #[tokio::test]
    async fn write_vlog() -> anyhow::Result<()> {
        let ring = RioConfigWrapper::new().depth(1024).build()?;

        let vlog_path = PathBuf::from("temp_vlog");
        std::fs::create_dir(&vlog_path)?;
        defer! {
            std::fs::remove_dir_all(&vlog_path).unwrap();
        }

        let mut vlog = VLogSet::new(ring.clone(), 66, &vlog_path).await?;
        let entries = gen_entries(6);
        vlog.append(&entries).await?;

        let mut buf = BytesMut::new();
        entries.iter().for_each(|e| {
            e.encode_for_buf(&mut buf);
        });
        let expected = buf.freeze();

        let mut buf = BytesMut::new();
        let file_set = read_vlog_dir(&vlog_path)?;
        assert!(file_set.len() == 4);

        for (_, path) in file_set.iter() {
            let content = std::fs::read(path.as_ref())?;
            buf.extend(content);
        }

        assert_eq!(buf, expected);

        Ok(())
    }
}
