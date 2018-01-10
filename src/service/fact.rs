extern crate time;

use dto::Queue;
use config::Config;
use dto::Partition;
use std::collections::HashMap;
use time::Timespec;
use std::collections::hash_map::Entry;
use lock_handler::AcquiredLock;
use std::sync::{Arc, RwLock};
use service::global_fact_service::GlobalFact;
use time::Tm;

#[derive(Debug, Clone)]
pub struct PartitionFact {
    partition: Partition,
    partition_type: PartitionType,
    partition_lock: PartitionLockType
}

#[derive(Debug, Clone)]
pub enum PartitionType {
    Read,
    Write,
    ReadWrite
}

#[derive(Debug, Clone)]
pub enum PartitionLockType {
    LockUntil {
        lock: AcquiredLock,
        owner: String
    },
    Unknown,
    NotLocked
}

impl PartitionFact {
    pub fn new(
        partition: Partition,
        partition_type: PartitionType
    ) -> Self {
        PartitionFact {
            partition,
            partition_type,
            partition_lock: PartitionLockType::Unknown
        }
    }

    pub fn get_partition(&self) -> &Partition {
        &self.partition
    }

    pub fn update_partition_lock(&mut self, lock : PartitionLockType)
    {
        self.partition_lock = lock;
    }

    pub fn get_partition_lock(&self) -> &PartitionLockType {
        &self.partition_lock
    }

    pub fn is_readable(&self) -> bool {
        match self.partition_type {
            PartitionType::Read => true,
            PartitionType::ReadWrite => true,
            _ => false
        }
    }

    pub fn is_writeable(&self) -> bool {
        match self.partition_type {
            PartitionType::Write => true,
            PartitionType::ReadWrite => true,
            _ => false
        }
    }
}

#[derive(Debug, Clone)]
pub struct Facts {
    pub partition_facts: HashMap<String, PartitionFact>,
    pub global_partition_facts: HashMap<String, GlobalFact>,
    pub global_partition_facts_updated_at: Option<Tm>
}

impl Facts {

    pub fn new(
        partition_facts: HashMap<String, PartitionFact>,
        global_partition_facts: HashMap<String, GlobalFact>
    ) -> Facts
    {
        Facts {
            partition_facts,
            global_partition_facts,
            global_partition_facts_updated_at: None
        }
    }

    pub fn get_partition_fact(&self, partition: &Partition) -> Option<&PartitionFact> {
        self.partition_facts.get(&partition.to_string())
    }

    pub fn get_global_partition_fact(&self, partition: &Partition) -> Option<&GlobalFact> {
        self.global_partition_facts.get(&partition.to_string())
    }

}

pub type SharedFacts = Arc<RwLock<Facts>>;

pub struct FactService {
    facts: Facts,
    shared_facts: SharedFacts
}

impl FactService {
    pub fn new() -> Self {

        let facts = Facts::new(HashMap::new(), HashMap::new());

        FactService {
            facts: facts.clone(),
            shared_facts: Arc::new(RwLock::new(facts)),
        }
    }


    pub fn update_partition_lock(&mut self, partition : &Partition, partition_lock_type: PartitionLockType)
    {
        let hash = partition.to_string();

        match self.facts.partition_facts.get_mut(&hash) {
            Some(e) => { e.update_partition_lock(partition_lock_type); },
            None => {}
        };
    }

    pub fn get_facts(&self) -> Facts {
        self.facts.clone()
    }

    pub fn get_shared_facts(&self) -> SharedFacts {
        self.shared_facts.clone()
    }

    pub fn commit_facts(&self) {

        let arc: Arc<RwLock<_>> = self.shared_facts.clone();

        let mut lock = arc.write().expect("should be save, lock cant be poisened");

        *lock = self.facts.clone();

    }

    pub fn update_global_facts(&mut self, global_facts: Vec<GlobalFact>) {

        let mut buf = HashMap::new();

        for global_fact in global_facts {
            buf.insert(global_fact.partition.to_string(), global_fact);
        }

        self.facts.global_partition_facts = buf;
        self.facts.global_partition_facts_updated_at = Some(::time::now_utc());
    }

    // todo, doesnt make sense, should be a constructor
    pub fn apply(&mut self, config: &Config) {

        self.facts.partition_facts = HashMap::new();

        for config_queue in config.get_queues() {

            let partition_count = if config_queue.get_partitions_read() > config_queue.get_partitions_write() {
                config_queue.get_partitions_read()
            } else {
                config_queue.get_partitions_write()
            };

            for partition in 1..partition_count + 1 {

                let partition_type = if partition <= config_queue.get_partitions_read() && partition <= config_queue.get_partitions_write() {
                    PartitionType::ReadWrite
                } else if partition <= config_queue.get_partitions_write() {
                    PartitionType::Write
                } else {
                    PartitionType::Read
                };

                let fact = PartitionFact::new(
                    Partition::new(
                        Queue::new(config_queue.get_name().to_string()),
                        partition
                    ),
                    partition_type
                );

                self.facts.partition_facts.insert(
                    partition.to_string(),
                    fact
                );

            }

        }

    }
}

