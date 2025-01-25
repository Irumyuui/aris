use std::path::Path;

use bytes::{Buf, Bytes, BytesMut};

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

pub struct ValueLogReader {
    ring: rio::Rio,
    file: std::fs::File,
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

        Ok(Self { ring, file })
    }

    pub async fn read_record(&self) -> Result<ValueLogRecord> {
        let mut buf = BytesMut::zeroed(1);

        let read_size = self.ring.read_at(&self.file, &mut buf, 0).await?;
        if read_size != 1 {
            return Err(Error::ValueLogFileCorrupted("Read data len failed".into()));
        }

        let data_len = buf[0] as usize;

        buf.resize(buf.len() + data_len, 0);

        let size_buf = &mut buf[1..];
        let read_size = self.ring.read_at(&self.file, &size_buf, 1).await?;
        if read_size != data_len {
            return Err(Error::ValueLogFileCorrupted("Read data failed".into()));
        }

        let var_key_len = VarUInt::try_from(&buf[..])
            .map_err(|e| Error::ValueLogFileCorrupted(format!("Parse key len failed: {}", e)))?;
        let var_value_len = VarUInt::try_from(&buf[var_key_len.as_slice().len()..])
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

        let read_key_req = self.ring.read_at(&self.file, &key_buf, 1 + data_len as u64);
        let read_value_req =
            self.ring
                .read_at(&self.file, &value_buf, 1 + data_len as u64 + key_len as u64);
        let read_crc_req = self.ring.read_at(
            &self.file,
            &crc_buf,
            1 + data_len as u64 + key_len as u64 + value_len as u64,
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
            return Err(Error::ValueLogFileCorrupted("CRC32 checksum failed".into()));
        }

        let key = buf.slice((1 + data_len as usize)..(1 + data_len as usize + key_len as usize));
        let value = buf.slice(
            (1 + data_len as usize + key_len as usize)
                ..(1 + data_len as usize + key_len as usize + value_len as usize),
        );

        return Ok(ValueLogRecord { key, value });
    }
}
