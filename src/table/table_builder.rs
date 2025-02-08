use std::{sync::Arc, vec};

use bytes::BufMut;

use crate::{
    config::{CompressionType, Config},
    error::DBResult,
    table::block_handler::Footer,
};

use super::{
    block_builder::BlockBuilder, block_handler::BlockHandle, filter_block::FilterBlockBuilder,
};

pub struct TableBuilder {
    // From options
    // comp: Arc<okedyn Comparator>,
    // filter_policy: Option<Arc<dyn FilterPolicy>>,
    config: Arc<Config>,

    // write file
    fd: Arc<std::fs::File>,
    ring: rio::Rio,
    append_offset: u64,

    data_block: BlockBuilder,
    index_block: BlockBuilder,
    last_key: Vec<u8>,
    entries_count: u64,

    closed: bool,

    filter_block: Option<FilterBlockBuilder>,

    pending_index_entry: bool,
    pending_handle: BlockHandle,
}

impl TableBuilder {
    pub fn new(config: Arc<Config>, fd: Arc<std::fs::File>, ring: rio::Rio) -> Self {
        let data_block = BlockBuilder::new(config.clone());
        let index_block = BlockBuilder::new(config.clone());

        let filter_policy = config.filter_policy.clone();
        let mut filter_block = None;
        if let Some(ref filter) = filter_policy {
            let mut builder = FilterBlockBuilder::new(filter.clone());
            builder.start_block(0);
            filter_block = Some(builder);
        }

        let this = Self {
            config,

            fd,
            ring,
            append_offset: 0,

            data_block,
            index_block,
            last_key: Vec::new(),
            entries_count: 0,

            closed: false,

            filter_block,

            pending_index_entry: false,
            pending_handle: BlockHandle::new(0, 0),
        };

        todo!()
    }

    pub fn add(&mut self, key: &[u8], value: &[u8]) -> DBResult<()> {
        assert!(!self.closed);

        if self.entries_count > 0 {
            // 需要输入 key 必须大于 last_key，按照给定的比较顺序
            assert!(self.config.comparator.compare(key, &self.last_key).is_ge());
        }

        if self.pending_index_entry {
            assert!(self.data_block.is_empty());
            let sep = self
                .config
                .comparator
                .find_shortest_separator(&self.last_key, key);

            let mut handle_encoding = vec![];
            self.pending_handle.encode_to(&mut handle_encoding);
            self.index_block.add(&sep, &handle_encoding);
            self.pending_index_entry = false;
        }
        if let Some(ref mut filter_block) = self.filter_block {
            filter_block.add_key(key);
        }

        self.last_key.clear();
        self.last_key.extend(key);
        self.entries_count += 1;
        self.data_block.add(key, value);

        if self.data_block.current_size_estimate() >= self.config.block_size {
            self.flush()?;
        }

        return Ok(());
    }

    pub fn flush(&mut self) -> DBResult<()> {
        assert!(!self.closed);
        if self.data_block.is_empty() {
            return Ok(());
        }

        assert!(!self.pending_index_entry);
        let raw_block = self.data_block.finish();
        let compress_block = do_compress(raw_block, self.config.compresstion_type)?;
        write_raw_block(
            &self.ring,
            &self.fd,
            &mut self.append_offset,
            &compress_block,
            self.config.compresstion_type,
            &mut self.pending_handle,
        )?;

        self.data_block.reset();
        self.pending_index_entry = true;
        if let Some(filter_builder) = &mut self.filter_block {
            filter_builder.start_block(self.append_offset);
        }

        return Ok(());
    }

    pub fn finish(&mut self) -> DBResult<()> {
        self.flush()?;
        assert!(!self.closed);
        self.closed = true;

        // build filter
        let mut filter_block_handler = BlockHandle::new(0, 0);
        if let Some(b) = &mut self.filter_block {
            let filter_raw_block = b.finish();
            write_raw_block(
                &self.ring,
                &self.fd,
                &mut self.append_offset,
                filter_raw_block,
                CompressionType::None,
                &mut filter_block_handler,
            )?;
        }

        // builder meta block
        let mut meta_block_builder = BlockBuilder::new(self.config.clone());
        let mut meta_block_handle = BlockHandle::new(0, 0);
        let meta_raw_block = if self.filter_block.is_some() {
            let filter_key = self.config.filter_policy.as_ref().unwrap().name();
            meta_block_builder.add(filter_key.as_bytes(), &filter_block_handler.encode());
            meta_block_builder.finish()
        } else {
            meta_block_builder.finish()
        };
        let meta_block = do_compress(meta_raw_block, self.config.compresstion_type)?;
        write_raw_block(
            &self.ring,
            &self.fd,
            &mut self.append_offset,
            &meta_block,
            self.config.compresstion_type,
            &mut meta_block_handle,
        )?;

        // index
        if self.pending_index_entry {
            let sep = self.config.comparator.find_short_successor(&self.last_key);
            let mut handle_encoding = vec![];
            self.pending_handle.encode_to(&mut handle_encoding);
            self.index_block.add(&sep, &handle_encoding);
            self.pending_index_entry = false;
        }
        let index_raw_block = self.index_block.finish();
        let mut index_block_handle = BlockHandle::new(0, 0);
        let index_block = do_compress(index_raw_block, self.config.compresstion_type)?;
        write_raw_block(
            &self.ring,
            &self.fd,
            &mut self.append_offset,
            &index_block,
            self.config.compresstion_type,
            &mut index_block_handle,
        )?;
        self.index_block.reset();

        // footer
        let footer = Footer::new(meta_block_handle, index_block_handle).encode();

        let comp = self
            .ring
            .write_at(self.fd.as_ref(), &footer, self.append_offset);
        self.append_offset += footer.len() as u64;

        Ok(())
    }

    pub fn abandon(&mut self) {
        assert!(!self.closed);
        self.closed;
    }

    pub fn entries_count(&self) -> u64 {
        self.entries_count
    }

    pub fn file_size(&self) -> u64 {
        self.append_offset
    }
}

fn do_compress(raw_block_content: &[u8], compression_type: CompressionType) -> DBResult<Vec<u8>> {
    let res = match compression_type {
        CompressionType::None => raw_block_content.to_vec(),
        CompressionType::Snappy => {
            todo!()
        }
    };
    return Ok(res);
}

fn write_raw_block(
    ring: &rio::Rio,
    fd: &std::fs::File,
    append_offset: &mut u64,
    content: &[u8],
    compression_type: CompressionType,
    handle: &mut BlockHandle,
) -> DBResult<()> {
    let comp1 = ring.write_at(fd, &content, *append_offset);
    *append_offset += content.len() as u64;
    handle.set_offset(*append_offset);
    handle.set_size(content.len() as u64);

    let mut trailer = vec![compression_type as u8];
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(content);
    hasher.update(&trailer);
    let crc = hasher.finalize();
    trailer.put_u32_le(crc);
    assert_eq!(trailer.len(), 5);

    let comp2 = ring.write_at(fd, &trailer, *append_offset);
    *append_offset += trailer.len() as u64;

    let count = comp1.wait()?;
    assert_eq!(count, content.len());
    let count = comp2.wait()?;
    assert_eq!(count, trailer.len());

    Ok(())
}
