use std::{
    alloc::Layout,
    ops::{Deref, Index},
    ptr::NonNull,
    sync::atomic::{AtomicPtr, AtomicUsize, Ordering::*},
};

use bytes::Bytes;

use crate::utils::{arena::Arena, comparator::Comparator, iterator::Iterator};

const MAX_HEIGHT: usize = 20;

#[repr(C)]
struct Tower {
    ptrs: [AtomicPtr<Node>; 0],
}

impl Index<usize> for Tower {
    type Output = AtomicPtr<Node>;

    fn index(&self, index: usize) -> &Self::Output {
        unsafe { &*self.ptrs.as_ptr().add(index) }
    }
}

impl Tower {
    fn get_next(&self, level: usize) -> *mut Node {
        self[level].load(Acquire)
    }

    fn set_next(&self, level: usize, node: *mut Node) {
        self[level].store(node, Release);
    }
}

#[repr(C)]
struct Head {
    ptrs: [AtomicPtr<Node>; MAX_HEIGHT],
}

impl Index<usize> for Head {
    type Output = AtomicPtr<Node>;

    fn index(&self, index: usize) -> &Self::Output {
        &self.ptrs[index]
    }
}

impl Deref for Head {
    type Target = Tower;

    fn deref(&self) -> &Self::Target {
        unsafe { &*(self as *const Self as *const _) }
    }
}

impl Default for Head {
    fn default() -> Self {
        Self {
            ptrs: Default::default(), // fxxk
        }
    }
}

impl Head {
    fn get_next(&self, level: usize) -> *mut Node {
        self[level].load(Acquire)
    }

    fn set_next(&self, level: usize, node: *mut Node) {
        self[level].store(node, Release);
    }
}

#[repr(C)]
struct Node {
    key: Bytes,
    tower: Tower,
}

impl Node {
    unsafe fn alloc(key: Bytes, height: usize, arena: &impl Arena) -> *mut Self {
        let layout = Self::get_layout(height);

        let ptr = arena.allocate::<Self>(layout);
        std::ptr::addr_of_mut!((*ptr).key).write(key);
        std::ptr::addr_of_mut!((*ptr).tower)
            .cast::<AtomicPtr<Node>>()
            .write_bytes(0, height * std::mem::size_of::<AtomicPtr<Node>>());

        let key = String::from_utf8((*ptr).key.to_vec()).unwrap();

        ptr
    }

    fn get_layout(height: usize) -> Layout {
        Layout::new::<Node>()
            .extend(Layout::array::<AtomicPtr<Node>>(height).unwrap())
            .unwrap()
            .0
            .pad_to_align()
    }
}

pub struct SkipList<C, A>
where
    C: Comparator,
    A: Arena,
{
    head: Head,
    max_level: AtomicUsize,
    cmp: C,
    arena: A,
}

impl<C, A> Drop for SkipList<C, A>
where
    C: Comparator,
    A: Arena,
{
    fn drop(&mut self) {
        let mut node = self.head.get_next(0);
        while !node.is_null() {
            let next = unsafe { node.as_ref().unwrap().tower.get_next(0) };
            unsafe {
                std::ptr::drop_in_place(node);
            }
            node = next;
        }
    }
}

impl<C, A> SkipList<C, A>
where
    C: Comparator,
    A: Arena,
{
    pub fn new(cmp: C, arena: A) -> Self {
        Self {
            head: Head::default(),
            max_level: AtomicUsize::new(1),
            cmp,
            arena,
        }
    }

    pub fn contains(&self, key: impl AsRef<[u8]>) -> bool {
        unsafe {
            let node = self.search_ge_node(&key, None);
            !node.is_null()
                && self
                    .cmp
                    .compare(key.as_ref(), node.as_ref().unwrap().key.as_ref())
                    .is_eq()
        }
    }

    // TODO: maybe insert need just `&self`?
    pub fn insert(&mut self, key: impl Into<Bytes>) {
        let key = key.into();
        let mut pref = [&*self.head as *const _; MAX_HEIGHT];

        unsafe {
            let node = self.search_ge_node(&key, Some(&mut pref));
            if !node.is_null() {
                assert!(
                    !self.cmp.compare(&node.as_ref().unwrap().key, &key).is_eq(),
                    "Key exists."
                );
            }
        }

        let height = random_height();
        unsafe {
            let new_node = Node::alloc(key, height, &self.arena);
            for i in 0..height {
                (*new_node).tower.set_next(i, (*pref[i]).get_next(i));
                (*pref[i]).set_next(i, new_node);
            }
        }

        if height > self.max_level() {
            self.max_level.store(height, Release);
        }
    }

    pub fn memory_usage(&self) -> usize {
        self.arena.memory_usage()
    }

    fn max_level(&self) -> usize {
        self.max_level.load(Acquire)
    }

    unsafe fn search_ge_node(
        &self,
        key: impl AsRef<[u8]>,
        mut pref: Option<&mut [*const Tower]>,
    ) -> *mut Node {
        let mut level = self.max_level() - 1;
        let mut cur = &*self.head;

        loop {
            let next = cur.get_next(level);
            if self.key_le_node(key.as_ref(), next) {
                if let Some(ref mut pref) = pref {
                    pref[level] = cur;
                }

                if level == 0 {
                    return next;
                }
                level -= 1;
            } else {
                cur = &next.as_ref().unwrap().tower;
            }
        }
    }

    unsafe fn search_lt_node(&self, key: impl AsRef<[u8]>) -> *mut Node {
        let mut level = self.max_level() - 1;
        let mut cur = &*self.head;
        let mut cur_node = std::ptr::null_mut();

        loop {
            let next = cur.get_next(level);

            if next.is_null() || !self.cmp.compare(&(*next).key, key.as_ref()).is_lt() {
                if level == 0 {
                    return cur_node;
                }
                level -= 1;
            } else {
                cur_node = next;
                cur = &(*next).tower;
            }
        }
    }

    unsafe fn search_last(&self) -> *mut Node {
        let mut level = self.max_level() - 1;
        let mut cur = &*self.head;
        let mut cur_node = std::ptr::null_mut();

        loop {
            let next = cur.get_next(level);

            if next.is_null() {
                if level == 0 {
                    return cur_node;
                }
                level -= 1;
            } else {
                cur_node = next;
                cur = &(*next).tower;
            }
        }
    }

    unsafe fn saerch_first(&self) -> *mut Node {
        self.head.get_next(0)
    }

    unsafe fn key_le_node(&self, key: impl AsRef<[u8]>, node: *const Node) -> bool {
        if node.is_null() {
            true
        } else {
            self.cmp.compare(key.as_ref(), (*node).key.as_ref()).is_le()
        }
    }

    pub fn iter(&self) -> Iter<'_, C, A> {
        Iter::new(self)
    }
}

fn random_height() -> usize {
    let mut h = 1;
    while h < MAX_HEIGHT && rand::random::<u32>() % 4 == 0 {
        h += 1;
    }
    h
}

pub struct Iter<'a, C, A>
where
    C: Comparator,
    A: Arena,
{
    list: &'a SkipList<C, A>,
    node: Option<NonNull<Node>>,
}

impl<'a, C, A> Iter<'a, C, A>
where
    C: Comparator,
    A: Arena,
{
    pub fn new(list: &'a SkipList<C, A>) -> Self {
        Self { list, node: None }
    }
}

impl<C, A> Iterator for Iter<'_, C, A>
where
    C: Comparator,
    A: Arena,
{
    fn is_valid(&self) -> bool {
        self.node.is_some()
    }

    fn seek_to_first(&mut self) {
        unsafe {
            let node = self.list.saerch_first();
            self.node = NonNull::new(node);
        }
    }

    fn seek_to_last(&mut self) {
        unsafe {
            let node = self.list.search_last();
            self.node = NonNull::new(node);
        }
    }

    fn seek(&mut self, key: impl AsRef<[u8]>) {
        unsafe {
            let node = self.list.search_ge_node(key, None);
            self.node = NonNull::new(node);
        }
    }

    fn next(&mut self) {
        unsafe {
            self.node = NonNull::new(self.node.expect("Null ptr").as_ref().tower.get_next(0));
        }
    }

    fn prev(&mut self) {
        unsafe {
            self.node = NonNull::new(
                self.list
                    .search_lt_node(&self.node.expect("Null ptr").as_ref().key),
            );
        }
    }

    fn key(&self) -> &[u8] {
        unsafe { self.node.expect("Null ptr").as_ref().key.as_ref() }
    }

    fn value(&self) -> &[u8] {
        unimplemented!("Value store with key.");
    }

    fn status<E>(&self) -> Result<(), E> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use bytes::Bytes;
    use itertools::Itertools;

    use crate::utils::{arena::BlockArena, comparator::Comparator, iterator::Iterator};

    use super::SkipList;

    struct TestComparator;

    impl Comparator for TestComparator {
        fn compare(&self, left: &[u8], right: &[u8]) -> std::cmp::Ordering {
            left.cmp(right)
        }

        fn id(&self) -> &str {
            unimplemented!()
        }
    }

    fn gen_keys(n: usize) -> Vec<Bytes> {
        (0..n)
            .map(|i| format!("key{:09}", i))
            .map(Bytes::from)
            .collect_vec()
    }

    #[test]
    fn insert_and_contains() {
        let keys = gen_keys(10000);

        let mut skip_list = SkipList::new(TestComparator, BlockArena::new());
        for key in keys.iter() {
            skip_list.insert(key.clone());
        }

        for key in keys.iter() {
            assert!(skip_list.contains(key.as_ref()));
        }
        let mut node = skip_list.head.get_next(0);
        unsafe {
            for i in 0..keys.len() {
                assert!(!node.is_null());
                assert_eq!(node.as_ref().unwrap().key, keys[i]);
                node = (*node).tower.get_next(0);
            }
        }
    }

    #[test]
    fn iter_next() {
        let keys = gen_keys(10000);

        let mut skip_list = SkipList::new(TestComparator, BlockArena::new());
        for key in keys.iter() {
            skip_list.insert(key.clone());
        }

        let mut iter = skip_list.iter();
        iter.seek_to_first();

        for key in keys.iter() {
            assert!(iter.is_valid());
            assert_eq!(iter.key(), key.as_ref());
            iter.next();
        }
    }

    #[test]
    fn iter_seek() {
        let keys = gen_keys(10000);

        let mut skip_list = SkipList::new(TestComparator, BlockArena::new());
        for key in keys.iter() {
            skip_list.insert(key.clone());
        }

        let mut iter = skip_list.iter();
        iter.seek_to_first();

        for key in keys.iter() {
            iter.seek(key.as_ref());
            assert!(iter.is_valid());
            assert_eq!(iter.key(), key.as_ref());
        }
    }

    #[test]
    fn iter_from_last() {
        let keys = gen_keys(10000);

        let mut skip_list = SkipList::new(TestComparator, BlockArena::new());
        for key in keys.iter() {
            skip_list.insert(key.clone());
        }

        let mut iter = skip_list.iter();
        iter.seek_to_last();

        for key in keys.iter().rev() {
            assert!(iter.is_valid());
            assert_eq!(iter.key(), key.as_ref());
            iter.prev();
        }
    }

    #[test]
    fn random_seek() {
        let keys = gen_keys(10000);

        let mut skip_list = SkipList::new(TestComparator, BlockArena::new());
        for key in keys.iter() {
            skip_list.insert(key.clone());
        }

        let mut iter = skip_list.iter();
        iter.seek_to_first();

        for key in keys.iter().step_by(10) {
            iter.seek(key.as_ref());
            assert!(iter.is_valid());
            assert_eq!(iter.key(), key.as_ref());
        }
    }

    #[test]
    #[should_panic]
    fn iter_get_key() {
        let keys = gen_keys(10000);

        let mut skip_list = SkipList::new(TestComparator, BlockArena::new());
        for key in keys.iter() {
            skip_list.insert(key.clone());
        }

        let mut iter = skip_list.iter();
        iter.seek_to_first();
        iter.value();
    }

    #[test]
    #[should_panic]
    fn insert_twice() {
        let key = Bytes::from("key");
        let mut skip_list = SkipList::new(TestComparator, BlockArena::new());
        skip_list.insert(key.clone());
        assert!(skip_list.contains(key.clone()));
        skip_list.insert(key.clone());
    }

    // 一个测试，多线程插入数据，但同时只有一个线程可以插入，对于读者不加锁
    #[test]
    fn multi_thread_insert() {
        const KEY_COUNT: usize = 10000;
        const THREAD_COUNT: usize = 10;

        let ks = gen_keys(10000);
        let keys = ks.chunks(KEY_COUNT / THREAD_COUNT).collect_vec();

        let sk = SkipList::new(TestComparator, BlockArena::new());

        let sk_m = Arc::new(Mutex::new(sk));

        let ths = (0..THREAD_COUNT).map(|i| {
            let keys = keys[i].iter().map(|e| e.clone()).collect_vec();
            let sk = sk_m.clone();

            std::thread::spawn(move || {
                for key in keys.iter() {
                    let mut guard = sk.lock().unwrap();
                    guard.insert(key.clone());
                }
            })
        });

        for th in ths {
            th.join().unwrap();
        }

        let sk = Arc::try_unwrap(sk_m)
            .ok()
            .unwrap()
            .into_inner()
            .ok()
            .unwrap();

        let sk = Arc::new(sk);
        let keys = Arc::new(ks);
        let ths = (0..THREAD_COUNT).map(|i| {
            let sk = sk.clone();
            let keys = keys.clone();

            std::thread::spawn(move || {
                for key in keys
                    .iter()
                    .skip(i * (KEY_COUNT / THREAD_COUNT))
                    .take(KEY_COUNT / THREAD_COUNT)
                {
                    assert!(sk.contains(key.as_ref()));
                }
            })
        });

        for th in ths {
            th.join().unwrap();
        }
    }
}
