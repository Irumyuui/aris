pub trait Comparator {
    fn compare(&self, a: &[u8], b: &[u8]) -> std::cmp::Ordering;

    fn name(&self) -> &str;

    fn find_shortest_separator(&self, a: &[u8], b: &[u8]) -> Vec<u8>;

    fn find_short_successor(&self, key: &[u8]) -> Vec<u8>;
}
