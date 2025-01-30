use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::ensure;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use fast_async_mutex::rwlock::RwLock;
use regex::Regex;
use vlog_file::{ReadLogFile, WriteLogFile};

mod vlog_file;

/* #region entry */

#[repr(u8)]
#[derive(Debug, Clone, Copy)]
pub enum ValueType {
    Delete = 0,
    Value = 1,
    BatchBegin = 2,
    BatchMid = 3,
    BatchEnd = 4,
}

impl TryFrom<u8> for ValueType {
    type Error = anyhow::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(ValueType::Delete),
            1 => Ok(ValueType::Value),
            2 => Ok(ValueType::BatchBegin),
            3 => Ok(ValueType::BatchMid),
            4 => Ok(ValueType::BatchEnd),
            _ => Err(anyhow::anyhow!("invalid value type")),
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

const ENTRY_HEADER_SIZE: usize = 8 + 1;

impl VLogEntry {
    pub(crate) fn new(key: Bytes, value: Bytes, meta: ValueType) -> Self {
        Self { key, value, meta }
    }

    pub(crate) fn encode_for_buf(&self, buf: &mut BytesMut) -> usize {
        let e = self.encode();
        let res = e.len();
        buf.extend(e);
        res
    }

    pub(crate) fn encode(&self) -> Bytes {
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

    pub(crate) fn decode(bytes: Bytes) -> anyhow::Result<Self> {
        ensure!(bytes.len() >= ENTRY_HEADER_SIZE + 4);

        let mut ptr = &bytes[..];
        let key_len = ptr.get_u32() as usize;
        let value_len = ptr.get_u32() as usize;

        ensure!(key_len + value_len + ENTRY_HEADER_SIZE + 4 <= bytes.len());
        let value_type = ValueType::try_from(ptr.get_u8())?;
        let key = bytes.slice(ENTRY_HEADER_SIZE..ENTRY_HEADER_SIZE + key_len);
        let value =
            bytes.slice(ENTRY_HEADER_SIZE + key_len..ENTRY_HEADER_SIZE + key_len + value_len);
        let crc = (&bytes[ENTRY_HEADER_SIZE + key_len + value_len..]).get_u32();

        let calc_crc = crc32fast::hash(&bytes[..ENTRY_HEADER_SIZE + key_len + value_len]);
        ensure!(crc == calc_crc);

        Ok(VLogEntry::new(key, value, value_type))
    }
}

/* #endregion entry */

#[derive(Debug, Clone, Copy)]
pub struct ValuePointer {
    pub(crate) file_id: u32,
    pub(crate) len: u32,
    pub(crate) offset: u64,
}

pub struct ValueLogSet {
    path: PathBuf,
    files: Arc<RwLock<LogFiles>>,
}

fn gen_file_name(fid: u32) -> String {
    format!("{:06}.vlog", fid)
}

impl ValueLogSet {
    pub fn new(ring: rio::Rio, path: impl AsRef<Path>, max_file_size: u64) -> anyhow::Result<Self> {
        let files = Arc::new(RwLock::new(LogFiles::new(ring, &path, max_file_size)?));
        let path = PathBuf::from(path.as_ref());
        Ok(Self { path, files })
    }

    pub async fn write_entry(&self, entry: &VLogEntry) -> anyhow::Result<ValuePointer> {
        let buf = entry.encode();
        self.write_inner(&buf).await
    }

    pub async fn read_entry(&self, ptr: &ValuePointer) -> anyhow::Result<VLogEntry> {
        tracing::debug!("Read entry ptr: {:?}", ptr);
        let buf = self.read_inner(ptr).await?;
        tracing::debug!("Read entry buf: {:?}", buf);
        VLogEntry::decode(buf.freeze())
    }

    async fn write_inner(&self, buf: &[u8]) -> anyhow::Result<ValuePointer> {
        self.files.write().await.write(buf).await
    }

    async fn read_inner(&self, ptr: &ValuePointer) -> anyhow::Result<BytesMut> {
        self.files.read().await.read(ptr).await
    }
}

struct LogFiles {
    max_fid: u32,
    max_file_size: u64,

    current_file: WriteLogFile,
    active_files: BTreeMap<u32, ReadLogFile>,
    deleted_files: BTreeSet<u32>,

    ring: rio::Rio,
    path: PathBuf,
}

impl LogFiles {
    pub fn new(ring: rio::Rio, path: impl AsRef<Path>, max_file_size: u64) -> anyhow::Result<Self> {
        let path = path.as_ref();
        if !path.is_dir() {
            std::fs::create_dir(path)?;
        }

        let re = Regex::new(r"^(\d+)\.vlog$").expect("invalid regex");
        let mut files = Vec::new();
        for entry in std::fs::read_dir(path)? {
            let entry = if let Ok(entry) = entry {
                entry
            } else {
                continue;
            };
            let file_path = entry.path();
            if !file_path.is_file() {
                continue;
            }
            if let Some(file_name) = file_path.file_name().and_then(|n| n.to_str()) {
                if let Some(caps) = re.captures(file_name) {
                    let fid = match caps.get(1) {
                        Some(v) => match v.as_str().parse::<u32>() {
                            Ok(fid) => fid,
                            Err(_) => continue,
                        },
                        None => continue,
                    };
                    files.push((fid, file_path));
                }
            }
        }
        files.sort();

        let mut max_fid = files.len() as u32;

        let mut active_files = BTreeMap::new();
        for (fid, file_path) in files.iter().take(files.len().max(1) - 1) {
            let file = ReadLogFile::new(ring.clone(), file_path.clone(), fid.clone())?;
            active_files.insert(fid.clone(), file);
        }

        let current_file;
        if let Some((fid, file_path)) = files.last() {
            let file = WriteLogFile::new(ring.clone(), file_path.clone(), fid.clone())?;
            if file.writen_len() >= max_file_size {
                let file = file.into_read();
                active_files.insert(fid.clone(), file);

                let new_file_path = path.join(gen_file_name(max_fid));
                current_file = WriteLogFile::new(ring.clone(), new_file_path, max_fid)?;
                max_fid += 1;
            } else {
                current_file = file;
            }
        } else {
            let new_file_path = path.join(gen_file_name(max_fid));
            current_file = WriteLogFile::new(ring.clone(), new_file_path, max_fid)?;
            max_fid += 1;
        }

        let deleted_files = BTreeSet::new();

        let this = Self {
            path: PathBuf::from(path),
            max_fid,
            max_file_size,
            current_file,
            active_files,
            deleted_files,
            ring,
        };

        tracing::debug!(
            "Create state, max fid: {}, max file size: {}, current_file: {}",
            this.max_fid,
            this.max_file_size,
            this.current_file.fid()
        );
        Ok(this)
    }

    async fn write(&mut self, buf: &[u8]) -> anyhow::Result<ValuePointer> {
        let ptr = self.current_file.write(buf).await?;
        self.next_write_file().await?;
        Ok(ptr)
    }

    async fn next_write_file(&mut self) -> anyhow::Result<()> {
        if self.current_file.writen_len() < self.max_file_size {
            return Ok(());
        }

        tracing::debug!(
            "Next vlog file, current file size: {}",
            self.current_file.writen_len()
        );

        let new_file = WriteLogFile::new(
            self.ring.clone(),
            self.path.join(gen_file_name(self.max_fid)),
            self.max_fid,
        )?;
        tracing::debug!("New vlog file: {:?}", new_file.fid());

        let old_file = std::mem::replace(&mut self.current_file, new_file).into_read();
        self.active_files.insert(old_file.fid(), old_file);
        self.max_fid += 1;
        tracing::debug!("Max fid: {}", self.max_fid);
        Ok(())
    }

    async fn read(&self, ptr: &ValuePointer) -> anyhow::Result<BytesMut> {
        if let Some(file) = self.active_files.get(&ptr.file_id) {
            tracing::debug!("Read from active file: {:?}", file.fid());
            return file.read(ptr).await;
        }

        tracing::debug!("Read from current file: {:?}", self.current_file.fid());
        self.current_file.read(ptr).await
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use bytes::{Buf, BufMut, Bytes, BytesMut};
    use itertools::Itertools;

    use crate::{
        utils::rio_config::RioConfigWrapper,
        vlog::{gen_file_name, VLogEntry, ValueLogSet, ValuePointer, ValueType, ENTRY_HEADER_SIZE},
    };

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
            .map(|i| VLogEntry {
                key: Bytes::copy_from_slice(format!("key-{:05}", i).as_bytes()),
                value: Bytes::copy_from_slice(format!("value-{:05}", i).as_bytes()),
                meta: ValueType::Value,
            })
            .collect()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn write_entries() -> anyhow::Result<()> {
        const TEST_COUNT: usize = 700;
        const SPLIT_COUNT: usize = 3;

        let ring = RioConfigWrapper::new().depth(4096).build()?;
        let path = PathBuf::from("temp_vlog_write_entries");
        scopeguard::defer! {
            let _ = std::fs::remove_dir_all(&path);
        }

        let entries = gen_entries(TEST_COUNT);
        let encodes = entries.iter().map(|e| e.encode()).collect_vec();
        let total_len = encodes.iter().map(|e| e.len()).sum::<usize>();
        let file_len_limit = total_len / SPLIT_COUNT;

        let vlog = ValueLogSet::new(ring.clone(), &path, file_len_limit as _)?;

        let mut ptrs = Vec::with_capacity(TEST_COUNT);
        for e in entries.iter() {
            let ptr = vlog.write_entry(e).await?;
            ptrs.push(ptr);
        }
        drop(vlog);

        for (i, ptr) in ptrs.iter().enumerate() {
            let file_path = path.join(gen_file_name(ptr.file_id));
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(false)
                .create(false)
                .open(file_path)?;
            let buf = BytesMut::zeroed(ptr.len as usize);
            let read_bytes = ring.read_at(&file, &buf, ptr.offset).await?;
            assert_eq!(read_bytes, ptr.len as usize);

            let buf = buf.freeze();

            assert_eq!(buf, encodes[i], "i: {}", i);
        }

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn read_entries() -> anyhow::Result<()> {
        const TEST_COUNT: usize = 700;
        const SPLIT_COUNT: usize = 3;

        let ring = RioConfigWrapper::new().depth(4096).build()?;
        let path = PathBuf::from("temp_vlog_read_entries");

        std::fs::create_dir(&path)?;
        scopeguard::defer! {
            let _ = std::fs::remove_dir_all(&path);
        }

        let entries = gen_entries(TEST_COUNT);
        let encodes = entries.iter().map(|e| e.encode()).collect_vec();
        let total_len = encodes.iter().map(|e| e.len()).sum::<usize>();
        let file_len_limit = total_len / SPLIT_COUNT;

        let mut offset = 0;
        let mut file_id = 0;
        let mut ptrs = Vec::new();
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path.join(gen_file_name(file_id)))?;
        for (_i, e) in encodes.iter().enumerate() {
            let write_len = ring.write_at(&file, &e, offset).await?;
            assert_eq!(write_len, e.len());
            ptrs.push(ValuePointer {
                file_id,
                len: e.len() as u32,
                offset,
            });
            offset += e.len() as u64;
            if offset >= file_len_limit as u64 {
                file_id += 1;
                offset = 0;
                file = std::fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .read(true)
                    .open(path.join(gen_file_name(file_id)))?;
            }
        }
        drop(file);

        let vlog = ValueLogSet::new(ring.clone(), &path, file_len_limit as u64)?;
        for (i, ptr) in ptrs.iter().enumerate() {
            let entry = vlog.read_entry(ptr).await?;
            let encode = entry.encode();
            assert_eq!(encode, encodes[i]);
        }

        Ok(())
    }
}
