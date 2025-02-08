use std::sync::Arc;

use bytes::BufMut;

use crate::{config::Config, utils::varint::VarInt};

use super::SIZE_U32;

pub struct BlockBuilder {
    buf: Vec<u8>,
    restarts: Vec<u32>,

    last_key: Vec<u8>,

    // config
    config: Arc<Config>,
    counter: usize,

    finished: bool,
}

impl BlockBuilder {
    pub fn new(config: Arc<Config>) -> Self {
        assert!(
            config.block_restart_interval > 0,
            "required block_restart_interval > 0"
        );

        let this = Self {
            buf: Vec::new(),
            restarts: vec![0], // First restart point is at offset 0,
            last_key: Vec::new(),
            counter: 0,
            finished: false,
            config,
        };
        return this;
    }

    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        assert!(
            !self.finished,
            "block builder finished, should not add more"
        );
        assert!(
            self.counter <= self.config.block_restart_interval as usize,
            "counter > block_restart_interval, counter: {}, block_restart_interval: {}",
            self.counter,
            self.config.block_restart_interval
        );
        assert!(
            self.buf.is_empty() || self.config.comparator.compare(key, &self.last_key).is_ge(),
            "key is not greater than last key, key: {:?}, last key: {:?}",
            key,
            self.last_key
        );

        let mut shared = 0;
        if self.counter < self.config.block_restart_interval as usize {
            // try match
            let min_len = self.last_key.len().min(key.len());
            while shared < min_len && self.last_key[shared] == key[shared] {
                shared += 1;
            }
        } else {
            // new restart
            self.restarts.push(self.buf.len() as u32);
            self.counter = 0;
        }

        let shared = shared as u32;
        let non_shared = key.len() as u32 - shared;

        // | shared key len | non-shared key len | value len | non-shared key | value |
        shared.put_varint(&mut self.buf);
        non_shared.put_varint(&mut self.buf);
        (value.len() as u32).put_varint(&mut self.buf);

        self.buf.put(&key[shared as usize..]);
        self.buf.put(value);

        self.last_key.clear();
        self.last_key.extend_from_slice(key);

        self.counter += 1;
    }

    pub fn finish(&mut self) -> &[u8] {
        assert!(!self.finished);
        for offset in self.restarts.iter() {
            self.buf.put_u32_le(*offset);
        }
        self.buf.put_u32_le(self.restarts.len() as u32);
        self.finished = true;
        &self.buf
    }

    pub fn current_size_estimate(&self) -> usize {
        self.buf.len() + self.restarts.len() * SIZE_U32 + SIZE_U32
    }

    pub fn reset(&mut self) {
        assert!(self.finished, "should finished");
        self.buf.clear();
        self.finished = false;
        self.counter = 0;
        self.restarts.clear();
        self.restarts.push(0);
        self.last_key.clear();
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, vec};

    use crate::{comparator::Comparator, config::ConfigBuilder};

    use super::BlockBuilder;

    struct TestComparator;

    impl Comparator for TestComparator {
        fn compare(&self, a: &[u8], b: &[u8]) -> std::cmp::Ordering {
            a.cmp(b)
        }

        fn name(&self) -> &str {
            unreachable!();
        }

        fn find_shortest_separator(&self, a: &[u8], b: &[u8]) -> Vec<u8> {
            unimplemented!()
        }

        fn find_short_successor(&self, key: &[u8]) -> Vec<u8> {
            unimplemented!()
        }
    }

    #[test]
    fn build_new_block() {
        let inputs = vec!["a", "ab", "abc", "acd", "adc", "bcd", "bde", "eee"];

        let config = ConfigBuilder::default()
            .block_restart_interval(3)
            .comparator(Arc::new(TestComparator))
            .build();
        let mut builder = BlockBuilder::new(config);

        for input in inputs.iter() {
            builder.add(input.as_bytes(), input.as_bytes());
        }
        builder.finish();

        assert_eq!(builder.restarts, vec![0, 18, 44]);
    }
}
