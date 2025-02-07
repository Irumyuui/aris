use crate::utils::bloom::BloomBuilder;

pub trait FilterPolicy {
    fn name(&self) -> &str;

    fn create_filter(&self, keys: &[Vec<u8>]) -> Vec<u8>;

    fn may_contain(&self, filter: &[u8], key: &[u8]) -> bool;
}

pub struct BloomFilter {
    builder: BloomBuilder,
}

impl FilterPolicy for BloomFilter {
    fn name(&self) -> &str {
        "bloom"
    }

    fn create_filter(&self, keys: &[Vec<u8>]) -> Vec<u8> {
        self.builder.build(keys)
    }

    fn may_contain(&self, filter: &[u8], key: &[u8]) -> bool {
        BloomBuilder::may_contain(filter, &key)
    }
}
