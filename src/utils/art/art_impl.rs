use std::{ptr::NonNull, sync::Arc};

use bytes::Bytes;
use crossbeam::epoch::Guard;

use crate::utils::art::node::{IntenalNode, LeafNode, ReadGuard};

use super::node::{ArtOptLockError, IntenalPtr, NodePtr};

// #[derive(Clone)]
pub struct Art {
    inner: ArtInner,
}

impl Art {
    pub fn new() -> Self {
        Self {
            inner: ArtInner::new(),
        }
    }

    pub fn insert(&self, key: Bytes, value: Bytes, guard: &Guard) {
        // the key must contains a special key at the end.
        self.inner.insert(key, value, guard);
    }

    pub fn get(&self, key: &Bytes, guard: &Guard) -> Option<&Bytes> {
        self.inner.get(key, guard)
    }
}

struct ArtInner {
    root: IntenalPtr,
}

unsafe impl Send for ArtInner {}
unsafe impl Sync for ArtInner {}

impl ArtInner {
    fn new() -> Self {
        todo!()
    }

    fn get_inner(&self, key: &Bytes, _guard: &Guard) -> Result<Option<&Bytes>, ArtOptLockError> {
        let mut node = IntenalNode::read(self.root.cast())?;
        let mut depth = 0;

        loop {
            // maybe search to leaf
            let prefix_len = node.prefix_matches(&key[depth..])?;
            if prefix_len != node.prefix_len() {
                node.unlock()?;
                return Ok(None);
            }
            depth += prefix_len;

            // find child
            let child: NodePtr = node.get_child(key[depth])?;
            depth += 1;

            match child {
                NodePtr::Intenal { ptr } => {
                    let next_node = IntenalNode::read(ptr)?;
                    node.unlock()?;
                    node = next_node;
                }

                NodePtr::Leaf { ptr } => unsafe {
                    // next try match
                    let leaf = ptr.as_ref();
                    let prefix_len = leaf.prefix_matches(key, depth);

                    let result = if depth + prefix_len == leaf.key().len() {
                        Some(leaf.value())
                    } else {
                        None
                    };
                    node.unlock()?;

                    return Ok(result);
                },

                NodePtr::None => {
                    node.unlock()?;
                    return Ok(None);
                }
            }
        }
    }

    fn get(&self, key: &Bytes, guard: &Guard) -> Option<&Bytes> {
        'retry: loop {
            match self.get_inner(key, guard) {
                Ok(res) => return res,
                Err(_) => continue 'retry,
            }
        }
    }

    fn insert_inner(
        &self,
        key: Bytes,
        value: Bytes,
        guard: &Guard,
    ) -> Result<(), (Bytes, Bytes, ArtOptLockError)> {
        macro_rules! handle_result {
            ($expr: expr) => {
                match $expr {
                    Ok(v) => v,
                    Err(e) => return Err((key, value, e)),
                }
            };
        }

        let mut parent: Option<ReadGuard<'_>> = None;
        let mut cur_pos = None;
        let mut cur = handle_result!(IntenalNode::read(self.root.cast()));
        let mut depth = 0;

        loop {
            let prefix_len = match cur.prefix_matches(&key[depth..]) {
                Ok(v) => v,
                Err(e) => return Err((key, value, e)),
            };

            // match prefix
            if prefix_len == cur.prefix_len() {
                depth += prefix_len;

                let child_pos = key[depth];
                let child = handle_result!(cur.get_child(key[depth]));
                handle_result!(cur.check_version());

                match child {
                    NodePtr::Intenal { ptr } => {
                        depth += 1;
                        let next_node = handle_result!(IntenalNode::read(ptr));
                        if let Some(parent) = parent.take() {
                            handle_result!(parent.unlock());
                        }
                        parent = Some(cur);
                        cur = next_node;
                        cur_pos = Some(child_pos);
                        continue;
                    }

                    NodePtr::Leaf { ptr } => unsafe {
                        depth += 1;

                        if ptr.as_ref().key() == key.as_ref() {
                            panic!("try to insert same key");
                        }

                        let prefix_len = ptr.as_ref().prefix_matches(&key, depth);
                        // TODO: build new prefix

                        let mut node = match cur.upgrade() {
                            Ok(v) => v,
                            Err((_, e)) => return Err((key, value, e)),
                        };

                        let mut new_node: NonNull<IntenalNode> = todo!();

                        let old_pos = ptr.as_ref().key()[depth + prefix_len];
                        let old_leaf = ptr.clone();
                        let new_pos = key[depth + prefix_len];
                        let new_leaf: NonNull<LeafNode> = todo!();

                        IntenalNode::insert_child(
                            new_node,
                            old_pos,
                            NodePtr::Leaf { ptr: old_leaf },
                        );
                        IntenalNode::insert_child(
                            new_node,
                            new_pos,
                            NodePtr::Leaf { ptr: new_leaf },
                        );

                        node.replace_child(child_pos, NodePtr::Intenal { ptr: new_node });

                        // write lock finish

                        return Ok(());
                    },

                    NodePtr::None => {
                        if cur.is_full() {
                            // grow current node
                            let mut parent = match parent.expect("parent must exist.").upgrade() {
                                Ok(v) => v,
                                Err((_, e)) => return Err((key, value, e)),
                            };
                            let mut cur = match cur.upgrade() {
                                Ok(v) => v,
                                Err((_, e)) => return Err((key, value, e)),
                            };

                            let new_pos = key[depth];
                            let new_leaf: NonNull<LeafNode> = todo!();
                            let new_node: NonNull<IntenalNode> = cur.grow();
                            IntenalNode::insert_child(
                                new_node,
                                new_pos,
                                NodePtr::Leaf { ptr: new_leaf },
                            );
                            parent.replace_child(child_pos, NodePtr::Intenal { ptr: new_node });

                            cur.mark_obsolte();
                            unsafe {
                                cur.defer_drop(guard);
                            }
                        } else {
                            let mut node = match cur.upgrade() {
                                Ok(v) => v,
                                Err((_, e)) => return Err((key, value, e)),
                            };

                            let new_leaf: NonNull<LeafNode> = todo!();
                            node.insert_child(child_pos, NodePtr::Leaf { ptr: new_leaf });
                        }

                        return Ok(());
                    }
                }

                unreachable!();
            }

            // split node
            let mut parent = match parent.expect("parent must exist.").upgrade() {
                Ok(v) => v,
                Err((_, e)) => return Err((key, value, e)),
            };
            let mut cur = match cur.upgrade() {
                Ok(v) => v,
                Err((_, e)) => return Err((key, value, e)),
            };

            // create a new node4, it will hold a part of old intenal node prefix (0..prefix_len) ,
            // the old intenal node should adjust it's prefix from the min leaf node's key,
            // and compare with new leaf.
            parent.insert_split_prefix(
                cur_pos.expect("current node pos must be exist."),
                cur,
                key,
                value,
                depth,
                prefix_len,
            );

            return Ok(());
        }
    }

    fn insert(&self, mut key: Bytes, mut value: Bytes, guard: &Guard) {
        'retry: loop {
            match self.insert_inner(key, value, guard) {
                Ok(_) => return,
                Err((k, v, _)) => {
                    key = k;
                    value = v;
                    continue 'retry;
                }
            }
        }
    }
}

impl Drop for ArtInner {
    fn drop(&mut self) {
        NodePtr::drop_node(NodePtr::Intenal { ptr: self.root });
    }
}
