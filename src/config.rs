use std::sync::Arc;

use crate::{comparator::Comparator, filter::FilterPolicy, utils::comparators::BytewiseComparator};

pub struct Config {
    pub(crate) block_restart_interval: u32,

    pub(crate) comparator: Arc<dyn Comparator>,

    pub(crate) filter_policy: Option<Arc<dyn FilterPolicy>>,

    pub(crate) block_size: usize,

    pub(crate) compresstion_type: CompressionType,
}

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum CompressionType {
    None = 0,
    Snappy = 1, // not impl
}

pub struct ConfigBuilder {
    block_restart_interval: u32,
    comparator: Arc<dyn Comparator>,
    filter_policy: Option<Arc<dyn FilterPolicy>>,
    block_size: usize,
    compression: CompressionType,
}

impl Default for ConfigBuilder {
    fn default() -> Self {
        let block_restart_interval = 16;
        let comparator = Arc::new(BytewiseComparator);
        let filter_policy = None;
        let block_size = 4096;
        let compression = CompressionType::None;

        Self {
            block_restart_interval,
            comparator,
            filter_policy,
            block_size,
            compression,
        }
    }
}

impl ConfigBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn block_restart_interval(&mut self, interval: u32) -> &mut Self {
        self.block_restart_interval = interval;
        self
    }

    pub fn comparator(&mut self, comparator: Arc<dyn Comparator>) -> &mut Self {
        self.comparator = comparator;
        self
    }

    pub fn filter_policy(&mut self, filter_policy: Arc<dyn FilterPolicy>) -> &mut Self {
        self.filter_policy = Some(filter_policy);
        self
    }

    pub fn block_size(&mut self, size: usize) -> &mut Self {
        self.block_size = size;
        self
    }

    pub fn compression(&mut self, compression: CompressionType) -> &mut Self {
        self.compression = compression;
        self
    }

    pub fn build(&self) -> Arc<Config> {
        Arc::new(Config {
            block_restart_interval: self.block_restart_interval,
            comparator: self.comparator.clone(),
            filter_policy: self.filter_policy.clone(),
            block_size: self.block_size,
            compresstion_type: self.compression,
        })
    }
}
