pub trait Comparator {
    fn compare(&self, left: impl AsRef<[u8]>, right: impl AsRef<[u8]>) -> std::cmp::Ordering {
        left.as_ref().cmp(right.as_ref())
    }

    fn id(&self) -> &str;
}
