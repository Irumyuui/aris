pub trait Comparator {
    fn compare(&self, left: &[u8], right: &[u8]) -> std::cmp::Ordering;

    fn id(&self) -> &str;
}
