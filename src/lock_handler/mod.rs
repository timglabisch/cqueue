use time::Timespec;

#[derive(Debug, Clone)]
pub struct AcquiredLock {
    queue: String,
    partition: u32,
    seed: u32,
    valid_until: Timespec
}

impl AcquiredLock {
    pub fn new(queue: String, partition: u32, seed: u32, valid_until: Timespec) -> Self {
        AcquiredLock {
            queue,
            partition,
            seed,
            valid_until
        }
    }

    pub fn get_queue(&self) -> &str {
        &self.queue
    }

    pub fn get_partition(&self) -> u32 {
        self.partition
    }

    pub fn get_seed(&self) -> u32 {
        self.seed
    }

    pub fn get_valid_until(&self) -> &Timespec {
        &self.valid_until
    }

    pub fn update_valid_until(&mut self, valid_until : Timespec) {
        self.valid_until = valid_until
    }
}



