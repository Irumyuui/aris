pub trait Iterator {
    fn is_valid(&self) -> bool;

    fn seek_to_first(&mut self);

    fn seek_to_last(&mut self);

    fn seek(&mut self, key: impl AsRef<[u8]>);

    fn next(&mut self);

    fn prev(&mut self);

    fn key(&self) -> &[u8];

    fn value(&self) -> &[u8];

    fn status<E>(&self) -> Result<(), E>;
}

// pub trait MakeIter
