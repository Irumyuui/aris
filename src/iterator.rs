use crate::error::DBResult;

pub trait Iterator {
    fn is_valid(&self) -> bool;

    fn next(&mut self);

    fn prev(&mut self);

    fn key(&self) -> &[u8];

    fn value(&self) -> &[u8];

    fn seek_to_first(&mut self);

    fn seek_to_last(&mut self);

    fn seek(&mut self, target: &[u8]);

    fn status(&mut self) -> DBResult<()>;
}
