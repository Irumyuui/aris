use std::sync::Arc;

use bytes::{Buf, BufMut};

use crate::filter::FilterPolicy;

const FILTER_BASE_LG: usize = 11;
const FILTER_BASE: usize = 1 << FILTER_BASE_LG; // 2 kb

pub struct FilterBlockBuilder {
    policy: Arc<dyn FilterPolicy>,
    // starts: Vec<u32>,
    keys: Vec<Vec<u8>>,
    filter_offsets: Vec<u32>,
    buf: Vec<u8>,
}

impl FilterBlockBuilder {
    pub fn new(policy: Arc<dyn FilterPolicy>) -> Self {
        let this = Self {
            policy,
            keys: Vec::new(),
            filter_offsets: Vec::new(),
            buf: Vec::new(),
        };
        this
    }

    pub fn add_key(&mut self, key: &[u8]) {
        let key = Vec::from(key);
        self.keys.push(key);
    }

    pub fn start_block(&mut self, block_offset: u64) {
        // 每一个 filter 分配 2kb
        let filter_index = block_offset / FILTER_BASE as u64;
        assert!(filter_index >= self.filter_offsets.len() as u64);
        while filter_index > self.filter_offsets.len() as u64 {
            self.generate_filter();
        }
    }

    pub fn generate_filter(&mut self) {
        self.filter_offsets.push(self.buf.len() as u32);
        // if not new keys, just update offsets.
        if self.keys.is_empty() {
            return;
        }
        let filter = self.policy.create_filter(&self.keys);
        self.buf.extend(filter);
        self.keys.clear();
    }

    pub fn finish(&mut self) -> &[u8] {
        if !self.keys.is_empty() {
            self.generate_filter();
        }

        let filter_offset_start = self.buf.len() as u32;
        for &off in self.filter_offsets.iter() {
            self.buf.put_u32_le(off);
        }
        self.buf.put_u32_le(filter_offset_start);
        self.buf.put_u8(FILTER_BASE_LG as u8);

        // | filters | filter offsets | filter offset start | base lg |
        &self.buf
    }
}

pub struct FilterBlockReader {
    policy: Arc<dyn FilterPolicy>,

    data: Vec<u8>, // use Byte will better?
    base_lg: usize,
    filter_count: usize,
    filter_offset: usize,
}

impl FilterBlockReader {
    pub fn new(policy: Arc<dyn FilterPolicy>, fiter_block: Vec<u8>) -> Self {
        let mut this = Self {
            policy,
            data: fiter_block,
            base_lg: 0,
            filter_count: 0,
            filter_offset: 0,
        };

        if this.data.len() < 5 {
            return this;
        }
        this.base_lg = this.data.last().copied().unwrap() as usize;
        let start_offset = (&this.data[this.data.len() - 5..]).get_u32_le();
        if start_offset + 5 > this.data.len() as u32 {
            // should panic?
            return this;
        }
        this.filter_offset = start_offset as usize;
        this.filter_count = (this.data.len() - 5 - this.filter_offset) / 4;
        return this;
    }

    pub fn key_may_match(&self, block_offset: u64, key: &[u8]) -> bool {
        let index = block_offset >> self.base_lg;
        if index >= self.filter_count as _ {
            return true;
        }

        let start = (&self.data[self.filter_offset + index as usize * 4..]).get_u32_le() as usize;
        let limit =
            (&self.data[self.filter_offset + (index + 1) as usize * 4..]).get_u32_le() as usize;

        if start <= limit && limit <= self.filter_offset {
            return self.policy.may_contain(&self.data[start..limit], key);
        } else if start == limit {
            return false;
        }

        return true; // ?
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::{Buf, BufMut};

    use crate::{
        filter::FilterPolicy,
        table::filter_block::{FilterBlockReader, FILTER_BASE_LG},
        utils::hash::basic_hash,
    };

    use super::FilterBlockBuilder;

    struct TestHashFilter;

    impl FilterPolicy for TestHashFilter {
        fn name(&self) -> &str {
            todo!()
        }

        fn create_filter(&self, keys: &[Vec<u8>]) -> Vec<u8> {
            let mut res = Vec::new();
            for key in keys {
                let h = basic_hash(&key, 1);
                res.put_u32_le(h);
            }
            res
        }

        fn may_contain(&self, mut filter: &[u8], key: &[u8]) -> bool {
            let h = basic_hash(key, 1);
            while filter.len() >= 4 {
                let res = filter.get_u32_le();
                if h == res {
                    return true;
                }
            }
            false
        }
    }

    #[test]
    fn empty_builder() {
        let mut builder = FilterBlockBuilder::new(Arc::new(TestHashFilter));
        let block = builder.finish();

        assert_eq!(block, &[0, 0, 0, 0, FILTER_BASE_LG as _]);
        let r = FilterBlockReader::new(Arc::new(TestHashFilter), block.to_vec());
        assert!(r.key_may_match(0, b"foo"));
        assert!(r.key_may_match(100000, b"foo"));
    }

    #[test]
    fn single_chunk() {
        let mut builder = FilterBlockBuilder::new(Arc::new(TestHashFilter));
        builder.start_block(100);
        builder.add_key(b"foo");
        builder.add_key(b"bar");
        builder.add_key(b"box");
        builder.start_block(200);
        builder.add_key(b"box");
        builder.start_block(300);
        builder.add_key(b"hello");
        let block = builder.finish();

        let r = FilterBlockReader::new(Arc::new(TestHashFilter), block.to_vec());
        assert!(r.key_may_match(100, b"foo"));
        assert!(r.key_may_match(100, b"bar"));
        assert!(r.key_may_match(100, b"box"));
        assert!(r.key_may_match(100, b"hello"));
        assert!(!r.key_may_match(100, b"missing"));
        assert!(!r.key_may_match(100, b"other"));
    }

    #[test]
    fn milti_chunk() {
        let mut builder = FilterBlockBuilder::new(Arc::new(TestHashFilter));

        // First filter
        builder.start_block(0);
        builder.add_key("foo".as_bytes());
        builder.start_block(2000);
        builder.add_key("bar".as_bytes());

        // Second filter
        builder.start_block(3100);
        builder.add_key("box".as_bytes());

        // Third filter is empty

        // Last filter
        builder.start_block(9000);
        builder.add_key("box".as_bytes());
        builder.add_key("hello".as_bytes());

        let block = builder.finish();
        let reader = FilterBlockReader::new(Arc::new(TestHashFilter), block.to_vec());

        // Check first filter
        assert!(reader.key_may_match(0, "foo".as_bytes()));
        assert!(reader.key_may_match(2000, "bar".as_bytes()));
        assert!(!reader.key_may_match(0, "box".as_bytes()));
        assert!(!reader.key_may_match(0, "hello".as_bytes()));

        // Check second filter
        assert!(reader.key_may_match(3100, "box".as_bytes()));
        assert!(!reader.key_may_match(3100, "foo".as_bytes()));
        assert!(!reader.key_may_match(3100, "bar".as_bytes()));
        assert!(!reader.key_may_match(3100, "hello".as_bytes()));

        // Check third filter (empty)
        assert!(!reader.key_may_match(4100, "box".as_bytes()));
        assert!(!reader.key_may_match(4100, "foo".as_bytes()));
        assert!(!reader.key_may_match(4100, "bar".as_bytes()));
        assert!(!reader.key_may_match(4100, "hello".as_bytes()));

        // Check last filter
        assert!(reader.key_may_match(9000, "box".as_bytes()));
        assert!(!reader.key_may_match(9000, "foo".as_bytes()));
        assert!(!reader.key_may_match(9000, "bar".as_bytes()));
        assert!(reader.key_may_match(9000, "hello".as_bytes()));
    }
}
