use std::sync::atomic::AtomicUsize;

use crossbeam_skiplist::SkipSet;

use super::entry::MemKeyEntry;

pub struct MemTableInner {
    skl: SkipSet<MemKeyEntry>,
    mem_usage: AtomicUsize,
}

impl MemTableInner {
    pub(crate) fn new() -> Self {
        Self {
            skl: Default::default(),
            mem_usage: AtomicUsize::new(0),
        }
    }

    pub(crate) fn get(&self, key: &MemKeyEntry) -> Option<MemKeyEntry> {
        self.skl.get(key).map(|e| e.value().clone())
    }

    pub(crate) fn insert(&self, key: MemKeyEntry) {
        let mem_inc = key.bytes();
        self.skl.insert(key);
        self.mem_usage
            .fetch_add(mem_inc, std::sync::atomic::Ordering::Release);
    }

    pub(crate) fn mem_usage(&self) -> usize {
        self.mem_usage.load(std::sync::atomic::Ordering::Acquire)
    }
}
