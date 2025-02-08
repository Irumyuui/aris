use crate::comparator::Comparator;

#[derive(Debug, Clone, Copy)]
pub struct BytewiseComparator;

impl Comparator for BytewiseComparator {
    fn compare(&self, a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        a.cmp(b)
    }

    fn name(&self) -> &str {
        "arisdb.BytewiseComparator"
    }

    // 得到一个 str ，满足 start < str <= limit 的第一个字符串
    fn find_shortest_separator(&self, start: &[u8], limit: &[u8]) -> Vec<u8> {
        let min_len = start.len().min(limit.len());
        let mut diff_index = 0;
        while diff_index < min_len && start[diff_index] == limit[diff_index] {
            diff_index += 1;
        }

        // 找前缀
        if diff_index >= min_len {
        } else {
            let diff = start[diff_index];
            if diff != 0xff && diff + 1 < limit[diff_index] {
                let mut res = (&start[..=diff_index]).to_vec();
                res[diff_index] += 1;
                return res;
            }
        }
        return start.to_vec();
    }

    fn find_short_successor(&self, key: &[u8]) -> Vec<u8> {
        for i in 0..key.len() {
            let b = key[i];
            if b != 0xff {
                let mut res = (&key[0..=i]).to_vec();
                res[i] += 1;
                return res;
            }
        }
        key.to_vec()
    }
}
