use crate::error::DBResult;

pub trait Iterator {
    // type PeekItem;
    // type KeyItem;
    // type ValueItem;

    fn is_valid(&self) -> bool;

    fn next(&mut self);

    fn prev(&mut self);

    // fn peek(&self) -> Option<&Self::PeekItem>;

    fn key(&self) -> &[u8];

    fn value(&self) -> &[u8];

    fn seek_to_first(&mut self);

    fn seek_to_last(&mut self);

    fn seek<K>(&mut self, target: &K);

    fn status(&mut self) -> DBResult<()>;
}
