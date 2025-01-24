use std::{os::fd, path::Path};

use anyhow::ensure;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use fast_async_mutex::mutex::Mutex;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
};

// Format:
// | crc32 | key_len | value_len | key | value |
pub struct Entry {
    key: Bytes,
    value: Bytes,
}

impl Entry {
    fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(8 + 8 + self.key.len() + self.value.len());

        buf.put_u64(self.key.len() as u64);
        buf.put_u64(self.value.len() as u64);
        buf.put(self.key.as_ref());
        buf.put(self.value.as_ref());

        buf.freeze()
    }

    fn decode(&self, mut buf: &[u8]) -> anyhow::Result<Entry> {
        ensure!(buf.len() >= 24, "buf is too short, len: {}", buf.len());

        let crc32 = buf.get_u32();
        let key_len = buf.get_u64();
        let value_len = buf.get_u64();

        ensure!(
            buf.len() >= 24 + key_len as usize + value_len as usize,
            "buf is too short, len: {}",
            buf.len()
        );

        let key = Bytes::copy_from_slice(&buf[..key_len as usize]);
        let value =
            Bytes::copy_from_slice(&buf[key_len as usize..key_len as usize + value_len as usize]);

        Ok(Self { key, value })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Pointer {
    offset: u64,
}
pub struct LogWriter {
    fd: Mutex<File>,
}

impl LogWriter {
    pub async fn open(log_file_path: impl AsRef<Path>) -> anyhow::Result<Self> {
        ensure!(
            !tokio::fs::metadata(&log_file_path).await?.is_file(),
            "log file already exists"
        );

        let file = tokio::fs::OpenOptions::new()
            .read(false)
            .write(true)
            .create(true)
            .append(true)
            .open(log_file_path)
            .await?;

        Ok(Self {
            fd: Mutex::new(file),
        })
    }

    pub async fn write(&self, entries: &[Entry]) -> anyhow::Result<Vec<Pointer>> {
        let mut buf = Vec::with_capacity(entries.len() * 24 * 10);
        let mut ptrs = Vec::with_capacity(entries.len());

        for entry in entries {
            let entry_bytes = entry.encode();
            let crc32 = crc32fast::hash(&entry_bytes);

            let ptrs = Pointer {
                offset: buf.len() as _,
            };

            let ptr = buf.put_u32(crc32);
            buf.put(entry_bytes);
        }

        self.fd.lock().await.write_all(&buf).await?;
        Ok(ptrs)
    }
}

pub struct LogReader {
    fd: Mutex<File>,
}

impl LogReader {
    pub async fn open(log_file_path: impl AsRef<Path>) -> anyhow::Result<Self> {
        ensure!(
            tokio::fs::metadata(&log_file_path).await?.is_file(),
            "log file not exists"
        );

        let file = tokio::fs::OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .open(log_file_path)
            .await?;

        Ok(Self {
            fd: Mutex::new(file),
        })
    }

    pub async fn read(&self) -> anyhow::Result<Vec<Entry>> {
        let mut head = [0u8; 24];
        let mut entries = Vec::new();
        let mut entry_buf = Vec::with_capacity(16);

        // Meybe consider use io-uring
        loop {
            let mut fd = self.fd.lock().await;
            let n = fd.read_exact(&mut head).await?;
            if n == 0 {
                break;
            }
            ensure!(n == 24, "read head failed");

            let mut buf = head.as_slice();
            let crc32 = buf.get_u32();
            let key_len = buf.get_u64();
            let value_len = buf.get_u64();

            entry_buf.clear();
            entry_buf.resize(8 + 8 + key_len as usize + value_len as usize, 0);
            let mut buf = &mut entry_buf[..];
            buf.put_u64(key_len);
            buf.put_u64(value_len);

            let n = fd.read_exact(buf).await?;
            ensure!(
                n == key_len as usize + value_len as usize,
                "read entry failed"
            );

            ensure!(crc32 == crc32fast::hash(&entry_buf), "crc32 not match");
            let entry = Entry {
                key: Bytes::copy_from_slice(&entry_buf[16..16 + key_len as usize]),
                value: Bytes::copy_from_slice(&entry_buf[16 + key_len as usize..]),
            };
            entries.push(entry);
        }

        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, str::FromStr};

    use crate::db::vlog::{Entry, LogReader, LogWriter};

    struct Guard(PathBuf);

    impl Drop for Guard {
        fn drop(&mut self) {
            println!("Remove");
            let _ = std::fs::remove_file(&self.0);
        }
    }

    #[tokio::test]
    async fn log_rw() {
        let path = std::env::temp_dir().join("vlog_rw_test");

        let _guard = Guard(path.clone());

        let entries = vec![
            Entry {
                key: "key1".into(),
                value: "value1".into(),
            },
            Entry {
                key: "key2".into(),
                value: "value2".into(),
            },
        ];

        let writer = LogWriter::open(&path).await.unwrap();  // wtf, why it will crash???
        let ptrs = writer.write(&entries).await.unwrap();

        let mut prev_offset = 0;
        for (i, e) in entries.iter().enumerate() {
            let ptr = ptrs[i];
            assert_eq!(ptr.offset, prev_offset);
            prev_offset += 24 + e.key.len() as u64 + e.value.len() as u64;
        }

        let reader = LogReader::open(&path).await.unwrap();
        let read_entries = reader.read().await.unwrap();
        for (a, b) in entries.iter().zip(read_entries.iter()) {
            assert_eq!(a.key, b.key);
            assert_eq!(a.value, b.value);
        }
    }
}
