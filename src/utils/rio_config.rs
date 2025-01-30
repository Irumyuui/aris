#![allow(unused)]

#[derive(Default, Debug, Clone, Copy)]
pub struct RioConfigWrapper {
    config: rio::Config,
}

impl RioConfigWrapper {
    pub fn new() -> Self {
        Self::default()
    }

    /// The number of entries in the submission queue.
    /// The completion queue size may be specified by
    /// using `raw_params` instead. By default, the
    /// kernel will choose a completion queue that is 2x
    /// the submission queue's size.
    pub fn depth(&mut self, depth: usize) -> &mut Self {
        self.config.depth = depth;
        self
    }

    /// Enable `SQPOLL` mode, which spawns a kernel
    /// thread that polls for submissions without
    /// needing to block as often to submit.
    ///
    /// This is a privileged operation, and
    /// will cause `start` to fail if run
    /// by a non-privileged user.
    pub fn sq_poll(&mut self, sq_poll: bool) -> &mut Self {
        self.config.sq_poll = sq_poll;
        self
    }

    /// Specify a particular CPU to pin the
    /// `SQPOLL` thread onto.
    pub fn sq_poll_affinity(&mut self, sq_poll_affinity: u32) -> &mut Self {
        self.config.sq_poll_affinity = sq_poll_affinity;
        self
    }

    /// Specify that the user will directly
    /// poll the hardware for operation completion
    /// rather than using the completion queue.
    ///
    /// CURRENTLY UNSUPPORTED
    pub fn io_poll(&mut self, io_poll: bool) -> &mut Self {
        self.config.io_poll = io_poll;
        self
    }

    /// Print a profile table on drop, showing where
    /// time was spent.
    pub fn print_profile_on_drop(&mut self, print_profile_on_drop: bool) -> &mut Self {
        self.config.print_profile_on_drop = print_profile_on_drop;
        self
    }

    // pub fn raw_params(&mut self, raw_params: Option<io_uring_params>) -> &mut Self {
    //     self.config.raw_params = raw_params;
    //     self
    // }

    pub fn build(self) -> std::io::Result<rio::Rio> {
        self.config.start()
    }
}
