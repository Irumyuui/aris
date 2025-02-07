use crate::comparator::Comparator;

pub struct Config {
    pub(crate) block_restart_interval: u32,

    pub(crate) comparator: Box<dyn Comparator>,
}
