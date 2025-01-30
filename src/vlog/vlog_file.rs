use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::ensure;
use bytes::BytesMut;

use crate::vlog::ValuePointer;

pub struct VLogFile {
    path: PathBuf,
    file_id: u32,
    fd: Arc<std::fs::File>,
    ring: rio::Rio,
    _read_only: bool,
}

impl VLogFile {
    pub fn new(
        ring: rio::Rio,
        path: impl AsRef<Path>,
        file_id: u32,
        read_only: bool,
    ) -> anyhow::Result<Self> {
        let fd = match read_only {
            true => Self::open_read(path.as_ref()),
            false => Self::open_write(path.as_ref()),
        }?;
        let fd = Arc::new(fd);

        let this = Self {
            path: PathBuf::from(path.as_ref()),
            file_id,
            fd,
            ring,
            _read_only: read_only,
        };
        Ok(this)
    }

    fn open_read(path: impl AsRef<Path>) -> anyhow::Result<std::fs::File> {
        let fd = std::fs::OpenOptions::new()
            .create(false)
            .read(true)
            .open(path)?;

        Ok(fd)
    }

    fn open_write(path: impl AsRef<Path>) -> anyhow::Result<std::fs::File> {
        let fd = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .append(true)
            .open(path)?;
        Ok(fd)
    }

    pub(crate) async fn read(&self, ptr: &ValuePointer) -> anyhow::Result<BytesMut> {
        let mut buf = BytesMut::with_capacity(ptr.len as usize);
        let offset = ptr.offset;
        let read_bytes = self
            .ring
            .read_at(self.fd.as_ref(), &mut buf, offset)
            .await?;
        ensure!(buf.len() == read_bytes);
        Ok(buf)
    }

    pub(crate) async fn write(
        &self,
        buf: &[u8],
        offset: u64,
    ) -> anyhow::Result<(ValuePointer, usize)> {
        ensure!(self._read_only);

        let ptr = ValuePointer {
            file_id: self.file_id,
            len: buf.len() as u32,
            offset,
        };

        self.ring
            .write_at(self.fd.as_ref(), &buf, offset)
            .await
            .map(|writen_len| (ptr, writen_len))
            .map_err(|e| e.into())
    }
}

pub struct WriteLogFile {
    log_file: VLogFile,
    offset: u64,
}

impl WriteLogFile {
    pub(crate) fn new(
        ring: rio::Rio,
        path: impl AsRef<Path>,
        file_id: u32,
    ) -> anyhow::Result<Self> {
        let log_file = VLogFile::new(ring, path, file_id, false)?;
        let offset = log_file.fd.metadata()?.len();
        Ok(Self { log_file, offset })
    }

    pub(crate) async fn write(&mut self, buf: &[u8]) -> anyhow::Result<ValuePointer> {
        let (ptr, written_len) = self.log_file.write(buf, self.offset).await?;
        ensure!(written_len == buf.len());
        self.offset += written_len as u64;
        Ok(ptr)
    }

    pub(crate) fn into_read(mut self) -> ReadLogFile {
        self.log_file._read_only = true;
        ReadLogFile(Arc::new(self.log_file))
    }

    pub(crate) fn writen_len(&self) -> u64 {
        self.offset
    }

    pub(crate) fn fid(&self) -> u32 {
        self.log_file.file_id
    }

    pub(crate) async fn read(&self, ptr: &ValuePointer) -> anyhow::Result<BytesMut> {
        self.log_file.read(ptr).await
    }
}

#[derive(Clone)]
pub struct ReadLogFile(Arc<VLogFile>);

impl ReadLogFile {
    pub(crate) fn new(
        ring: rio::Rio,
        path: impl AsRef<Path>,
        file_id: u32,
    ) -> anyhow::Result<Self> {
        let log_file = VLogFile::new(ring, path, file_id, true)?;
        Ok(Self(Arc::new(log_file)))
    }

    pub(crate) fn fid(&self) -> u32 {
        self.0.file_id
    }

    pub(crate) async fn read(&self, ptr: &ValuePointer) -> anyhow::Result<BytesMut> {
        self.0.read(ptr).await
    }
}
