use std::{path::Path, sync::Arc};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use itertools::Itertools;

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

// const PADDING: &[u8] = &[0; 7];

const PADDING: [&[u8]; 7] = [
    &[0],
    &[0, 0],
    &[0, 0, 0],
    &[0, 0, 0, 0],
    &[0, 0, 0, 0, 0],
    &[0, 0, 0, 0, 0, 0],
    &[0, 0, 0, 0, 0, 0, 0],
];
const BLOCK_SIZE: usize = 8 * 1024 * 1024;

#[repr(u8)]
#[derive(Debug, Clone, Copy)]
enum RecordType {
    // None = 0, // Invalid record type.
    First = 1,
    Middle = 2,
    Last = 3,
    Full = 4,
}

impl TryFrom<u8> for RecordType {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            1 => Ok(RecordType::First),
            2 => Ok(RecordType::Middle),
            3 => Ok(RecordType::Last),
            4 => Ok(RecordType::Full),
            _ => Err(Error::ReLogReadCorrupted(format!(
                "invalid record type: {}",
                value
            ))),
        }
    }
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

#[derive(Debug)]
pub struct ReLogWriter {
    ring: rio::Rio,
    file: std::fs::File,

    // Already written block count.
    written_block_len: usize,

    // The offset within the current block.
    block_offset: u64,
}

impl ReLogWriter {
    pub fn new(ring: rio::Rio, path: impl AsRef<Path>) -> Result<Self> {
        if path.as_ref().exists() {
            return Err(Error::ReLogFileCreatedFailed(format!(
                "re-log file already exists, path: {:?}",
                path.as_ref()
            )));
        }

        let file = std::fs::OpenOptions::new()
            .read(false)
            .write(true)
            .create_new(true)
            .append(true)
            .open(path)?;

        let this = Self {
            ring,
            file,
            written_block_len: 0,
            block_offset: 0,
        };
        Ok(this)
    }

    pub async fn write(&mut self, payload: Bytes) -> Result<()> {
        self.try_pad_block().await?;

        tracing::debug!(
            "write payload: {:?}, less block bytes: {}",
            payload.len(),
            self.block_remain_bytes(),
        );

        if payload.len() + 7 <= self.block_remain_bytes() as usize {
            let record = Record {
                payload,
                ty: RecordType::Full,
            };

            return self.write_records(&[record]).await;
        }

        let mut records = Vec::with_capacity(payload.len() / BLOCK_SIZE + 1);
        let mut payload_remain = payload.len();

        // ready to split payload
        // first block
        records.push(Record {
            payload: payload.slice(0..(self.block_remain_bytes() - 7) as usize),
            ty: RecordType::First,
        });
        payload_remain -= self.block_remain_bytes() as usize - 7;

        // middle block and last block
        while payload_remain > 0 {
            let record = if payload_remain > BLOCK_SIZE - 7 {
                Record {
                    payload: payload.slice(
                        (payload.len() - payload_remain)
                            ..(payload.len() - payload_remain + BLOCK_SIZE - 7),
                    ),
                    ty: RecordType::Middle,
                }
            } else {
                Record {
                    payload: payload.slice((payload.len() - payload_remain)..),
                    ty: RecordType::Last,
                }
            };
            records.push(record);
            payload_remain -= (BLOCK_SIZE - 7).min(payload_remain);
        }

        if records.len() == 1 {
            // is it possible?
            records[0].ty = RecordType::Full;
        }

        return self.write_records(&records).await;
    }

    // WARNING
    // io_uring needs to ensure that the lifetime of buf in the user process
    // is longer than the lifetime of kernel operations
    async fn write_records(&mut self, records: &[Record]) -> Result<()> {
        tracing::debug!("write records: {:?}", records.len());

        let encords = records.into_iter().map(|r| r.encord()).collect_vec();
        let mut completions = Vec::with_capacity(encords.len());

        for i in 0..encords.len() {
            tracing::debug!("write record: {:?}, encode len: {}", i, encords[i].len());

            let comp =
                self.ring
                    .write_at(&self.file, encords.get(i).unwrap(), self.last_file_offset());
            completions.push((comp, encords.get(i).unwrap().len()));
            self.block_offset += encords.get(i).unwrap().len() as u64;

            // ensure next block
            if self.block_remain_bytes() <= 7 {
                if self.block_remain_bytes() > 0 {
                    let comp = self.ring.write_at(
                        &self.file,
                        &PADDING[self.block_remain_bytes() as usize - 1],
                        self.last_file_offset(),
                    );
                    completions.push((comp, self.block_remain_bytes() as usize));
                }

                self.block_offset = 0;
                self.written_block_len += 1;
            }
        }

        for (comp, len) in completions.into_iter() {
            let writted_bytes = comp.await?;
            assert_eq!(writted_bytes, len, "write bytes not equal, is it right?");
        }

        self.try_pad_block().await?;
        Ok(())
    }

    async fn try_pad_block(&mut self) -> Result<()> {
        if self.block_remain_bytes() <= 7 {
            tracing::debug!("try pad block");

            if self.block_remain_bytes() > 0 {
                self.ring
                    .write_at(
                        &self.file,
                        &PADDING[self.block_remain_bytes() as usize - 1],
                        self.last_file_offset(),
                    )
                    .await?;
            }
            self.block_offset = 0;
            self.written_block_len += 1;
        }
        Ok(())
    }

    fn last_file_offset(&self) -> u64 {
        self.written_block_len as u64 * BLOCK_SIZE as u64 + self.block_offset
    }

    fn block_remain_bytes(&self) -> u64 {
        BLOCK_SIZE as u64 - self.block_offset
    }

    pub async fn finish(self) -> Result<(), (Self, Error)> {
        Ok(())
    }
}

/// Re-Log Reader,
pub struct ReLogReader {
    ring: rio::Rio,
}

impl ReLogReader {
    pub fn new(ring: rio::Rio) -> Self {
        Self { ring }
    }

    pub async fn read(&self, path: impl AsRef<Path>) -> Result<Vec<Bytes>, (Vec<Bytes>, Error)> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .open(path)
            .map_err(|e| (vec![], e.into()))?;
        let mut err = None;
        let records = match self.read_file(file).await {
            Ok(v) => v,
            Err((v, e)) => {
                err = Some(e);
                v
            }
        };

        let mut payloads = Vec::with_capacity(records.len());
        let mut buf = BytesMut::new();
        for Record { payload, ty } in records.into_iter() {
            match ty {
                RecordType::First => {
                    buf.clear();
                    buf.extend_from_slice(&payload);
                }
                RecordType::Middle => {
                    buf.extend_from_slice(&payload);
                }
                RecordType::Last => {
                    buf.extend_from_slice(&payload);
                    payloads.push(buf.clone().freeze());
                    buf.clear();
                }
                RecordType::Full => {
                    payloads.push(payload);
                    buf.clear();
                }
            }
        }

        match err.take() {
            None => Ok(payloads),
            Some(e) => Err((payloads, e)),
        }
    }

    async fn read_file(&self, file: std::fs::File) -> Result<Vec<Record>, (Vec<Record>, Error)> {
        let file_len = file.metadata().map_err(|e| (vec![], e.into()))?.len();
        let file = Arc::new(file);

        let blcok_count = (file_len + BLOCK_SIZE as u64) / BLOCK_SIZE as u64;
        let mut tasks = Vec::with_capacity(blcok_count as usize);
        for i in 0..blcok_count {
            let block_remain = if i == blcok_count - 1 {
                file_len % BLOCK_SIZE as u64
            } else {
                BLOCK_SIZE as u64
            };

            let ring = self.ring.clone();
            let file = file.clone();
            let task =
                tokio::spawn(async move { Self::read_block(ring, file, i, block_remain).await });

            tasks.push(task);
        }

        let mut results = Vec::with_capacity(blcok_count as usize);
        for task in tasks.into_iter() {
            let res = task.await.unwrap();
            results.push(res);
        }

        let mut records = Vec::with_capacity(BLOCK_SIZE);
        for res in results.into_iter() {
            match res {
                Ok(res) => records.extend(res.into_iter()),
                Err((v, e)) => {
                    records.extend(v.into_iter());
                    return Err((records, e));
                }
            }
        }

        Ok(records)
    }

    async fn read_block(
        ring: rio::Rio,
        file: Arc<std::fs::File>,
        block_id: u64,
        block_remain: u64,
    ) -> Result<Vec<Record>, (Vec<Record>, Error)> {
        let mut buf = BytesMut::zeroed(block_remain as usize);
        let read_len = ring
            .read_at(&file, &mut buf, block_id * BLOCK_SIZE as u64)
            .await
            .map_err(|e| (vec![], e.into()))?;
        assert_eq!(read_len, block_remain as usize);

        let buf = buf.freeze();
        let records = Self::get_records_from_block(buf)?;

        Ok(records)
    }

    fn get_records_from_block(block: Bytes) -> Result<Vec<Record>, (Vec<Record>, Error)> {
        let mut buf = &block[..];

        let mut records = Vec::with_capacity(10);
        while buf.has_remaining() && buf.len() > 7 {
            let pref = buf;

            let len = buf.get_u16();
            let ty = buf.get_u8();
            let payload = block.slice_ref(&buf[..len as usize]);

            buf = &buf[len as usize..];
            let crc = buf.get_u32();

            let record_slice = &pref[..len as usize + 3];
            let calc_crc = crc32fast::hash(record_slice);
            if crc != calc_crc {
                return Err((
                    records,
                    Error::ReLogReadCorrupted(format!(
                        "crc not equal, expect: {}, calc: {}",
                        crc, calc_crc,
                    )),
                ));
            }

            let ty = match RecordType::try_from(ty) {
                Ok(t) => t,
                Err(e) => return Err((records, e)),
            };

            let record = Record { payload, ty };
            records.push(record);
        }

        Ok(records)
    }
}

#[cfg(test)]
mod tests {
    use bytes::{Buf, BufMut, Bytes, BytesMut};
    use tempfile::tempfile;

    use crate::db::wal::{ReLogWriter, BLOCK_SIZE};

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

    fn gen_payload(len: usize) -> Bytes {
        let mut payload = BytesMut::with_capacity(len);
        let mut b = b'a';
        while payload.len() < len {
            payload.put_u8(b);
            b = (b - b'a' + 1) % 26 + b'a';
        }
        payload.freeze()
    }

    fn gen_writer(ring: rio::Rio, file: std::fs::File) -> ReLogWriter {
        ReLogWriter {
            ring,
            file,
            written_block_len: 0,
            block_offset: 0,
        }
    }

    // expect write only one block.
    #[tokio::test]
    async fn write_in_one_block() -> anyhow::Result<()> {
        let ring = rio::new()?;
        let file = tempfile()?;

        let payload = gen_payload(BLOCK_SIZE - 7);

        let mut writer = gen_writer(ring.clone(), file.try_clone()?);
        writer.write(payload.clone()).await?;
        writer.finish().await.expect("finish failed");

        assert_eq!(file.metadata()?.len(), BLOCK_SIZE as u64);

        let mut buf = BytesMut::zeroed(BLOCK_SIZE);
        let read_len = ring.read_at(&file, &mut buf, 0).await?;
        assert_eq!(read_len, BLOCK_SIZE);

        let buf = buf.freeze();

        let record = Record {
            payload,
            ty: RecordType::Full,
        };
        let encord = record.encord();
        assert_eq!(encord.len(), BLOCK_SIZE);
        assert_eq!(buf, encord);

        Ok(())
    }

    #[tokio::test]
    async fn write_some_block() -> anyhow::Result<()> {
        let ring = rio::new()?;
        let file = tempfile()?;

        // 4 blocks
        let payload = gen_payload((BLOCK_SIZE - 7) * 3 + BLOCK_SIZE / 2);

        let mut writer = gen_writer(ring.clone(), file.try_clone()?);

        writer.write(payload.clone()).await?;
        writer.finish().await.expect("finish failed");

        assert_eq!(
            file.metadata()?.len(),
            BLOCK_SIZE as u64 * 3 + BLOCK_SIZE as u64 / 2 + 7
        );

        let mut buf = BytesMut::zeroed(BLOCK_SIZE * 3 + BLOCK_SIZE / 2 + 7);
        let read_len = ring.read_at(&file, &mut buf, 0).await?;
        assert_eq!(read_len, BLOCK_SIZE * 3 + BLOCK_SIZE / 2 + 7);
        let read_buf = buf.freeze();

        let mut records = Vec::with_capacity(4);
        let mut offset = 0;
        for i in 0..4 {
            let record = if i == 0 {
                Record {
                    payload: payload.slice(0..(BLOCK_SIZE - 7)),
                    ty: RecordType::First,
                }
            } else if i == 3 {
                Record {
                    payload: payload.slice(offset..),
                    ty: RecordType::Last,
                }
            } else {
                Record {
                    payload: payload.slice(offset..(offset + BLOCK_SIZE - 7)),
                    ty: RecordType::Middle,
                }
            };
            offset += BLOCK_SIZE - 7;
            records.push(record);
        }

        let mut buf = BytesMut::zeroed(BLOCK_SIZE * 3 + BLOCK_SIZE / 2 + 7);
        let mut put_buf = &mut buf[..];
        for r in &records {
            put_buf.put(r.encord());
        }

        let target_buf = buf.freeze();
        assert!(read_buf == target_buf);

        Ok(())
    }

    #[tokio::test]
    async fn write_pad_records() -> anyhow::Result<()> {
        let ring = rio::new()?;
        let file = tempfile()?;

        let payloads = vec![gen_payload(BLOCK_SIZE - 14), gen_payload(14)];
        let mut writer = gen_writer(ring.clone(), file.try_clone()?);

        for payload in &payloads {
            writer.write(payload.clone()).await?;
        }
        writer.finish().await.expect("finish failed");

        assert_eq!(file.metadata()?.len(), BLOCK_SIZE as u64 + 21);
        let read_buf = BytesMut::zeroed(BLOCK_SIZE + 21);
        let read_len = ring.read_at(&file, &read_buf, 0).await?;
        assert_eq!(read_len, BLOCK_SIZE + 21);
        let read_buf = read_buf.freeze();

        let first_record = Record {
            payload: payloads[0].clone(),
            ty: RecordType::Full,
        };
        let first_encord = first_record.encord();
        assert_eq!(first_encord, &read_buf[..first_encord.len()]);

        let sec_encode = Record {
            payload: payloads[1].clone(),
            ty: RecordType::Full,
        }
        .encord();

        assert_eq!(
            &sec_encode[..],
            &read_buf[BLOCK_SIZE..BLOCK_SIZE + sec_encode.len()]
        );

        Ok(())
    }

    #[tokio::test]
    async fn write_pad_split_records() -> anyhow::Result<()> {
        let ring = rio::new()?;
        let file = tempfile()?;

        let payloads = vec![gen_payload(BLOCK_SIZE - 15), gen_payload(14)];
        let mut writer = gen_writer(ring.clone(), file.try_clone()?);

        for payload in &payloads {
            writer.write(payload.clone()).await?;
        }
        writer.finish().await.expect("finish failed");

        assert_eq!(file.metadata()?.len(), BLOCK_SIZE as u64 + 20);
        let read_buf = BytesMut::zeroed(BLOCK_SIZE + 20);
        let read_len = ring.read_at(&file, &read_buf, 0).await?;
        assert_eq!(read_len, BLOCK_SIZE + 20);
        let read_buf = read_buf.freeze();

        let first_record = Record {
            payload: payloads[0].clone(),
            ty: RecordType::Full,
        };
        let first_encord = first_record.encord();
        assert_eq!(first_encord, &read_buf[..first_encord.len()]);

        let sec_first_encode = Record {
            payload: payloads[1].slice(0..1),
            ty: RecordType::First,
        }
        .encord();
        let sec_last_encode = Record {
            payload: payloads[1].slice(1..),
            ty: RecordType::Last,
        }
        .encord();

        let mut sec_encode =
            BytesMut::with_capacity(sec_first_encode.len() + sec_last_encode.len());
        sec_encode.put(sec_first_encode);
        sec_encode.put(sec_last_encode);
        let sec_encode = sec_encode.freeze();

        assert_eq!(
            &sec_encode[..],
            &read_buf[first_encord.len()..first_encord.len() + sec_encode.len()]
        );

        Ok(())
    }
}
