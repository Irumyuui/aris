use crate::error::DBResult;

pub trait Iterator {
    type PeekItem;

    fn is_valid(&self) -> bool;

    fn next(&mut self);

    fn prev(&mut self);

    fn peek(&self) -> Option<&Self::PeekItem>;

    fn seek_to_first(&mut self);

    fn seek_to_last(&mut self);

    fn seek<K>(&mut self, target: &K);

    fn status(&mut self) -> DBResult<()>;
}
