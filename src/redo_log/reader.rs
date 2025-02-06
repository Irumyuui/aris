use std::vec;

use bytes::Buf;

use crate::{
    error::DBResult,
    redo_log::{BLOCK_SIZE, HEADER_SIZE},
};

use super::RecordType;

struct Record {
    ty: RecordType,
    data: Vec<u8>,
}

pub trait ErrorReporter {
    fn report(&mut self, read_pos: usize, err: Box<dyn std::error::Error>);
}

pub struct LogReader<'a> {
    fd: &'a std::fs::File,
    ring: rio::Rio,
    reporter: Option<Box<dyn ErrorReporter>>,
    data: Vec<u8>,

    read_offset: usize,
    read_rec_err: bool,
}

impl<'a> LogReader<'a> {
    pub fn new(
        fd: &'a std::fs::File,
        ring: rio::Rio,
        reporter: Option<Box<dyn ErrorReporter>>,
    ) -> DBResult<Self> {
        Ok(Self {
            fd,
            ring,
            reporter,
            data: vec![],
            read_offset: 0,
            read_rec_err: false,
        })
    }

    // if some block error, report it, and skip taill
    fn read_blocks(&mut self) -> DBResult<()> {
        let len = self.fd.metadata()?.len() as usize;
        let mut buf = vec![0_u8; len];

        let block_count = (len + BLOCK_SIZE - 1) / BLOCK_SIZE;
        let mut chunks = Vec::with_capacity(block_count);
        let mut comps = Vec::with_capacity(block_count);

        for i in 0..block_count {
            let l = i * BLOCK_SIZE;
            let r = ((i + 1) * BLOCK_SIZE).min(len);
            // let chunk = &buf[l..r];
            let ptr = buf.as_mut_ptr();
            let chunk = unsafe { std::slice::from_raw_parts_mut(ptr.add(l), r - l) };
            chunks.push((chunk, l, r - l));
        }
        for (chunk, offset, len) in chunks.iter_mut() {
            let comp = self.ring.read_at(self.fd, chunk, *offset as u64);
            comps.push((comp, *len, *offset));
        }

        let mut last = 0;
        let mut have_err = false;
        for (comp, len, offset) in comps.into_iter() {
            let res = comp.wait();
            if have_err {
                continue;
            }

            match res {
                Ok(count) => {
                    if count != len {
                        if let Some(r) = &mut self.reporter {
                            r.report(
                                offset,
                                Box::new(std::io::Error::new(
                                    std::io::ErrorKind::UnexpectedEof,
                                    "read block not enough",
                                )),
                            );
                            have_err = true;
                            continue;
                        }
                    }
                    last = offset + len;
                }
                Err(e) => {
                    if let Some(r) = &mut self.reporter {
                        r.report(offset, e.into());
                    }
                    have_err = true;
                    continue;
                }
            }
        }

        buf.truncate(last);

        self.data = buf;
        Ok(())
    }

    // if some record error, report it, and skip tail
    fn read_raw_record(&mut self) -> Option<Record> {
        macro_rules! report_err {
            ($err:expr) => {
                if let Some(r) = &mut self.reporter {
                    r.report(self.read_offset, $err);
                }
                self.read_rec_err = true;
            };
        }

        if self.read_rec_err || self.read_offset >= self.data.len() {
            return None;
        }

        // align offset ptr;
        let block_remain = BLOCK_SIZE - self.read_offset % BLOCK_SIZE;
        if block_remain < HEADER_SIZE {
            self.read_offset += block_remain;
        }

        let mut buf = &self.data[self.read_offset..];
        if buf.len() < 7 {
            return None;
        }

        let len = buf.get_u16_le() as usize;
        if len > buf.len() + 4 + 1 {
            report_err!(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "record len too long"
            )));
            return None;
        }

        let ty = RecordType::from(buf.get_u8());
        let data = Vec::from(&buf[..len - 4]);
        let mut buf = &buf[len - 4..];
        let crc32 = buf.get_u32_le();

        let crc = crc32fast::hash(&self.data[self.read_offset..self.read_offset + len + 3]);
        if crc != crc32 {
            report_err!(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "crc32 not match"
            )));
            return None;
        }

        self.read_offset += len + HEADER_SIZE;
        Some(Record { ty, data })
    }

    fn read_record(&mut self) -> Option<Record> {
        loop {
            if self.read_offset >= self.data.len() {
                if let Err(e) = self.read_blocks() {
                    if let Some(r) = &mut self.reporter {
                        r.report(self.read_offset, e.into());
                    }
                }
            }

            let rec = self.read_raw_record();
            if rec.is_none() {
                return None;
            }

            if !self.read_rec_err {
                return rec;
            }
        }
    }

    pub fn read_data(&mut self) -> Option<Vec<u8>> {
        let mut data = vec![];
        loop {
            let rec = self.read_record();
            let rec = match rec {
                Some(r) => r,
                None => return None,
            };

            match rec.ty {
                RecordType::Full => {
                    return Some(rec.data);
                }
                RecordType::First => {
                    data.extend_from_slice(&rec.data);
                }
                RecordType::Mid => {
                    data.extend_from_slice(&rec.data);
                }
                RecordType::Last => {
                    data.extend_from_slice(&rec.data);
                    return Some(data);
                }
            }
        }
    }
}
