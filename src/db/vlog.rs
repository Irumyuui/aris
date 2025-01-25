use std::path::Path;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::{
    error::{Error, Result},
    utils::varint::VarUInt,
};

/// Log record is siminal to a WAL record, but it use to store the bigger value.
///
/// The format on value-log file like this:
///
/// | data len: 1 byte | key len | value len | key | value | check sum: 4 bytes |
///
///  - `key len` and `value len` will store like a `VarUInt` format.
///  - `check sum`: a crc32 checksum of the record.
///  - `data len`: the len of `key_len`'s varint format len and `value_len`'s varint format len sum.   
#[derive(Clone)]
pub struct ValueLogRecord {
    key: Bytes,
    value: Bytes,
}

impl ValueLogRecord {
    pub fn new(key: Bytes, value: Bytes) -> Self {
        Self { key, value }
    }

    pub fn encode(&self) -> Bytes {
        let key_len = VarUInt::from(self.key.len() as u64);
        let value_len = VarUInt::from(self.value.len() as u64);

        let mut buf = BytesMut::with_capacity(
            1 + key_len.as_slice().len()
                + value_len.as_slice().len()
                + self.key.len()
                + self.value.len()
                + 4,
        );

        buf.put_u8((key_len.len() + value_len.len()) as u8);
        buf.put(key_len.as_bytes().clone());
        buf.put(value_len.as_bytes().clone());
        buf.put(self.key.clone());
        buf.put(self.value.clone());

        let crc = crc32fast::hash(&buf[..]);
        buf.put_u32(crc);

        tracing::debug!("key_len: {:?}, value_len: {:?}", key_len, value_len);
        tracing::debug!("key: {:?}, value: {:?}", self.key, self.value);
        tracing::debug!("crc32: {}", crc);

        buf.freeze()
    }
}

pub struct ValueLogReader {
    ring: rio::Rio,
    file: std::fs::File,
    offset: u64,
}

impl ValueLogReader {
    pub fn new(ring: rio::Rio, path: impl AsRef<Path>) -> Result<Self> {
        if !std::fs::exists(path.as_ref())? {
            return Err(Error::ValueLogFileNotFound(
                path.as_ref().to_string_lossy().into_owned(),
            ));
        }

        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .open(path)?;

        Ok(Self {
            ring,
            file,
            offset: 0,
        })
    }

    // TODO: Maybe split this impl, `self.offset` is not a good idea.
    pub async fn read_record(&mut self) -> Result<ValueLogRecord> {
        let mut buf = BytesMut::zeroed(1);

        let read_len = self.ring.read_at(&self.file, &mut buf, self.offset).await?;
        if read_len != 1 {
            return Err(Error::ValueLogFileCorrupted("Read data len failed".into()));
        }

        let data_len = buf[0] as usize;
        tracing::debug!("read_len: {} data_len: {}", read_len, data_len);

        buf.resize(buf.len() + data_len, 0);

        let size_buf = &mut buf[1..];
        let key_value_len = self
            .ring
            .read_at(&self.file, &size_buf, 1 + self.offset)
            .await?;
        if key_value_len != data_len {
            return Err(Error::ValueLogFileCorrupted("Read data failed".into()));
        }
        tracing::debug!("key_len + value_len: {}", key_value_len);

        let var_key_len = VarUInt::try_from(&buf[1..])
            .map_err(|e| Error::ValueLogFileCorrupted(format!("Parse key len failed: {}", e)))?;
        let var_value_len = VarUInt::try_from(&buf[1 + var_key_len.as_slice().len()..])
            .map_err(|e| Error::ValueLogFileCorrupted(format!("Parse value len failed: {}", e)))?;

        let key_len = var_key_len.try_to_u64().map_err(|e| {
            Error::ValueLogFileCorrupted(format!("Convert key len to u64 failed: {}", e))
        })?;
        let value_len = var_value_len.try_to_u64().map_err(|e| {
            Error::ValueLogFileCorrupted(format!("Convert value len to u64 failed: {}", e))
        })?;

        buf.resize(1 + data_len + key_len as usize + value_len as usize + 4, 0);

        let (mut key_buf, value_buf) = buf.split_at_mut(1 + data_len + key_len as usize);
        key_buf = &mut key_buf[1 + data_len..];

        let (value_buf, crc_buf) = value_buf.split_at_mut(value_len as usize);

        let read_key_req =
            self.ring
                .read_at(&self.file, &key_buf, 1 + data_len as u64 + self.offset);
        let read_value_req = self.ring.read_at(
            &self.file,
            &value_buf,
            1 + data_len as u64 + key_len as u64 + self.offset,
        );
        let read_crc_req = self.ring.read_at(
            &self.file,
            &crc_buf,
            1 + data_len as u64 + key_len as u64 + value_len as u64 + self.offset,
        );

        if read_key_req.await? != key_len as usize {
            return Err(Error::ValueLogFileCorrupted("Read key failed".into()));
        }
        if read_value_req.await? != value_len as usize {
            return Err(Error::ValueLogFileCorrupted("Read value failed".into()));
        }
        if read_crc_req.await? != 4 {
            return Err(Error::ValueLogFileCorrupted("Read crc failed".into()));
        }

        let buf = buf.freeze();
        let read_crc = (&buf[buf.len() - 4..]).get_u32();
        let crc = crc32fast::hash(&buf[..buf.len() - 4]);

        if read_crc != crc {
            return Err(Error::ValueLogFileCorrupted(format!(
                "CRC32 checksum failed, read: {}, calc: {}",
                read_crc, crc
            )));
        }

        let key = buf.slice((1 + data_len as usize)..(1 + data_len as usize + key_len as usize));
        let value = buf.slice(
            (1 + data_len as usize + key_len as usize)
                ..(1 + data_len as usize + key_len as usize + value_len as usize),
        );

        self.offset += 1 + data_len as u64 + key_len as u64 + value_len as u64 + 4;

        return Ok(ValueLogRecord { key, value });
    }
}

#[cfg(test)]
mod tests {
    use bytes::{Buf, BufMut, Bytes, BytesMut};
    use itertools::Itertools;
    use tempfile::tempfile;

    use crate::db::vlog::ValueLogReader;

    use super::{ValueLogRecord, VarUInt};

    #[test]
    fn record_encode() {
        let key = Bytes::copy_from_slice(b"key");
        let value = Bytes::copy_from_slice(b"value");

        let record = ValueLogRecord::new(key.clone(), value.clone());
        let encord = record.encode();

        let var_key_len = VarUInt::from(key.len() as u64);
        let var_value_len = VarUInt::from(value.len() as u64);

        assert_eq!(var_key_len.len() + var_value_len.len(), encord[0] as usize);
        assert_eq!(var_key_len.as_bytes(), &encord[1..1 + var_key_len.len()]);
        assert_eq!(
            var_value_len.as_bytes(),
            &encord[1 + var_key_len.len()..1 + var_key_len.len() + var_value_len.len()]
        );

        assert_eq!(
            key,
            encord.slice(
                1 + var_key_len.len() + var_value_len.len()
                    ..1 + var_key_len.len() + var_value_len.len() + key.len()
            )
        );
        assert_eq!(
            value,
            encord.slice(1 + var_key_len.len() + var_value_len.len() + key.len()..encord.len() - 4)
        );

        let mut buf = BytesMut::new();
        buf.put_u8(var_key_len.len() as u8 + var_value_len.len() as u8);
        buf.put(var_key_len.as_bytes().clone());
        buf.put(var_value_len.as_bytes().clone());
        buf.put(key);
        buf.put(value);

        let buf = buf.freeze();
        let crc32 = crc32fast::hash(&buf[..]);
        assert_eq!(crc32, (&encord[encord.len() - 4..]).get_u32());
    }

    fn gen_record(id: usize) -> ValueLogRecord {
        let key = Bytes::copy_from_slice(format!("key-{:05}", id).as_bytes());
        let value = Bytes::copy_from_slice(format!("value-{:05}", id).as_bytes());
        ValueLogRecord::new(key, value)
    }

    #[tokio::test]
    async fn read_some_record() -> anyhow::Result<()> {
        let ring = rio::new()?;
        let file = tempfile()?;

        let records = (0..100).map(gen_record).collect_vec();
        let encords = records.iter().map(|r| r.encode()).collect_vec();

        let mut offset = 0;
        let mut reqs = Vec::with_capacity(encords.len());

        for encord in encords.iter() {
            let req = ring.write_at(&file, encord, offset);
            reqs.push(req);
            offset += encord.len() as u64;
        }
        for req in reqs.drain(..) {
            req.await?;
        }

        let mut reader = ValueLogReader {
            ring: ring.clone(),
            file: file.try_clone()?,
            offset: 0,
        };

        for (_, record) in records.iter().enumerate() {
            let read_record = reader.read_record().await?;
            assert_eq!(record.key, read_record.key);
            assert_eq!(record.value, read_record.value);
        }

        Ok(())
    }
}
