use time::Timespec;

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

pub trait LockHandler {
    fn lock_acquire(&mut self, queue: &str, partition: u32) -> Result<Option<AcquiredLock>, String>;

    fn lock_renew(&mut self, lock: &mut AcquiredLock) -> Result<bool, String>;
}

struct Lock {
    queue: String,
    partition: u32,
    time: Timespec,
    owner: String
}

struct Locks {
    inner: Vec<Lock>
}

impl Locks {

    pub fn get_locks_by_owner(&self, owner : &str) -> Vec<&Lock> {
        self.inner
            .iter()
            .filter(|x|&x.owner == owner)
            .collect()
    }

}


