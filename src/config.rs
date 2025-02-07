use std::sync::Arc;

use crate::comparator::Comparator;

pub struct Config {
    pub(crate) block_restart_interval: u32,

    pub(crate) comparator: Arc<dyn Comparator>,
}
