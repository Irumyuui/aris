pub mod lru;

pub trait Cache<K, V>: Sync + Send
where
    K: Sync + Send,
    V: Sync + Send,
{
    fn insert(&self, key: K, value: V, charge: usize) -> Option<V>;

    fn get(&self, key: &K) -> Option<&V>;

    fn erase(&self, key: &K) -> Option<V>;

    fn total_charge(&self) -> usize;
}
