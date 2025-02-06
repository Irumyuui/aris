use super::hash::BasicHash;

#[derive(Debug, Clone, Copy)]
pub struct BloomBuilder {
    k_num: u8,
    bits_per_key: usize,
}

impl BloomBuilder {
    pub fn new(bits_per_key: usize) -> Self {
        // ln2 * (m / n)
        Self {
            k_num: (((bits_per_key as f64 * 0.69) as usize).max(1).min(30) as u8),
            bits_per_key,
        }
    }

    pub fn build<T>(&self, keys: &[T]) -> Vec<u8>
    where
        T: BasicHash,
    {
        let bits = (keys.len() * self.bits_per_key).max(64);
        let bytes = (bits + 7) / 8;
        let bits = bytes * 8;

        let mut filter = vec![0u8; bytes + 1];
        filter[bytes] = self.k_num;

        for key in keys {
            let mut h = Self::hash(key);
            let delta = (h >> 17) | (h << 15);
            for _ in 0..self.k_num {
                let bit_pos = h % bits as u32;
                filter[bit_pos as usize / 8] |= 1 << (bit_pos % 8);
                h = h.wrapping_add(delta);
            }
        }

        filter
    }

    pub fn may_contain<T>(filter: &[u8], key: &T) -> bool
    where
        T: BasicHash,
    {
        if filter.len() < 1 {
            return false;
        }

        let k = filter.last().unwrap().clone();
        if k > 30 {
            return true;
        }

        let bits = (filter.len() - 1) * 8;
        let mut h = Self::hash(key);
        let delta = (h >> 17) | (h << 15);
        for _ in 0..k {
            let bit_pos = h % (bits as u32);
            if (filter[bit_pos as usize / 8] & (1 << (bit_pos % 8))) == 0 {
                return false;
            }
            h = h.wrapping_add(delta);
        }
        true
    }

    fn hash<T>(data: &T) -> u32
    where
        T: BasicHash,
    {
        data.gen_basic_hash()
    }
}

#[cfg(test)]
mod tests {
    use crate::utils::bloom::BloomBuilder;

    #[test]
    fn empty_should_not_found() {
        let builder = BloomBuilder::new(10);
        let empty_ref: &[&[u8]] = &[];
        let filter = builder.build(empty_ref);

        assert!(!BloomBuilder::may_contain(&filter, &"key1".as_bytes()));
        assert!(!BloomBuilder::may_contain(&filter, &"key2".as_bytes()));
        assert!(!BloomBuilder::may_contain(&filter, &"empty".as_bytes()));
    }

    #[test]
    fn must_contain() {
        let builder = BloomBuilder::new(10);
        let keys = vec!["key1".as_bytes(), "key2".as_bytes()];
        let filter = builder.build(&keys);

        assert!(BloomBuilder::may_contain(&filter, &"key1".as_bytes()));
        assert!(BloomBuilder::may_contain(&filter, &"key2".as_bytes()));
    }

    #[test]
    fn may_contains() {
        let builder = BloomBuilder::new(10);

        let mut mediocre = 0;
        let mut good = 0;
        for len in [1, 10, 100, 1000, 10000] {
            let keys: Vec<Vec<u8>> = (0..len)
                .map(|i| i.to_string().as_bytes().to_vec())
                .collect();
            let filter = builder.build(&keys);

            // must be contains
            for key in keys.iter() {
                assert!(BloomBuilder::may_contain(&filter, key), "key: {:?}", key);
            }

            // false check
            let mut hits = 0;
            for i in 0..10000 {
                let key = (i + 1000000000).to_string().as_bytes().to_vec();
                if BloomBuilder::may_contain(&filter, &key) {
                    hits += 1;
                }
            }
            let rate = f64::from(hits) / 10000.;
            assert!(rate < 0.02, "rate: {}, len: {}", rate, len);

            if rate > 0.125 {
                mediocre += 1;
            } else {
                good += 1;
            }
        }

        assert!(
            mediocre * 5 < good,
            "mediocre: {}, good: {}",
            mediocre,
            good
        );
    }
}
