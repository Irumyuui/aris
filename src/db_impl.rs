use std::{collections::VecDeque, sync::Arc};

use bytes::Bytes;
use fast_async_mutex::rwlock::RwLock;

use crate::{
    error::Result,
    memtable::{LookupKey, MemTable},
    options::{Options, ReadOptions},
    vlog::VLogSet,
    write_batch::WriteBatch,
};

pub struct DBImpl {
    mem: RwLock<MemTable>,
    im_mems: RwLock<VecDeque<MemTable>>,

    vlog: RwLock<VLogSet>,

    options: Arc<Options>,
}

impl DBImpl {
    async fn write(&self, batch: WriteBatch) -> Result<()> {
        todo!()
    }

    async fn get(&self, key: &[u8], read_opts: ReadOptions) -> Result<Option<Vec<u8>>> {
        // TODO: snapshot
        let seq = read_opts.snapshot.unwrap_or(0);

        let lookup_key = LookupKey::new(Bytes::copy_from_slice(key), seq);

        // Read from memtable.
        let mems = self.get_memtables().await;
        let mut ptr = None;
        for mem in &mems {
            if let Some(p) = mem.get(&lookup_key) {
                ptr.replace(p);
                break;
            }
        }

        // TODO!: Read from sstable.

        // TODO!: Read from cache and vlog.
        match ptr {
            Some(_) => todo!(),
            None => return Ok(None),
        }
    }

    async fn get_memtables(&self) -> Vec<MemTable> {
        let mut mems = Vec::new();
        mems.push(self.mem.read().await.clone());
        for im in self.im_mems.read().await.iter() {
            mems.push(im.clone());
        }
        mems
    }
}
