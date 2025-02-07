pub trait Comparator {
    fn compare(&self, a: &[u8], b: &[u8]) -> std::cmp::Ordering;

    fn name(&self) -> &str;
}
