use bytes::BufMut;

use crate::{
    error::DBResult,
    redo_log::{RecordType, BLOCK_SIZE, HEADER_SIZE},
};

pub struct LogWriter<'f> {
    fd: &'f std::fs::File,
    file_offset: u64,
    block_offset: u64,
    ring: rio::Rio,
    buf: Vec<u8>,
}

const EMPTY: [&[u8]; 8] = [
    &[],
    &[0; 1],
    &[0; 2],
    &[0; 3],
    &[0; 4],
    &[0; 5],
    &[0; 6],
    &[0; 7],
];

impl<'f> LogWriter<'f> {
    pub fn new(fd: &'f std::fs::File, ring: rio::Rio) -> Self {
        Self {
            fd,
            file_offset: 0,
            block_offset: 0,
            ring,
            buf: vec![],
        }
    }

    pub fn append(&mut self, data: &[u8]) -> DBResult<()> {
        let mut begin = true;
        let mut data_offset = 0;
        loop {
            if data_offset >= data.len() {
                break;
            }

            let remain = BLOCK_SIZE - self.block_offset as usize;
            if remain < HEADER_SIZE {
                if remain > 0 {
                    self.buf.put_slice(EMPTY[remain]);
                    self.write_buf()?;
                }
                self.block_offset = 0;
            }

            let remain_block_size = BLOCK_SIZE - self.block_offset as usize - HEADER_SIZE;
            let l = data_offset;
            let r = data_offset + remain_block_size.min(data.len() - data_offset);
            let end = r == data.len();

            let ty = if begin && end {
                RecordType::Full
            } else if begin {
                RecordType::First
            } else if end {
                RecordType::Last
            } else {
                RecordType::Mid
            };

            let buf = &mut self.buf;
            let data_len = (r - l) as u16;
            buf.put_u16_le(data_len);
            buf.put_u8(ty as u8);
            buf.put_slice(&data[l..r]);
            let crc = crc32fast::hash(&buf[..]);
            buf.put_u32_le(crc);

            self.write_buf()?;

            data_offset = r;
            begin = false;
        }

        Ok(())
    }

    fn write_buf(&mut self) -> DBResult<()> {
        let buf = &self.buf;
        let comp = self.ring.write_at(self.fd, &buf, self.file_offset);
        let count = comp.wait()?;
        assert_eq!(buf.len(), count);
        self.buf.clear();
        Ok(())
    }
}
