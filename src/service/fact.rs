use dto::Queue;
use config::Config;
use dto::Partition;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct PartitionFact {
    partition: Partition,
    partition_type: PartitionType
}

#[derive(Debug, Clone)]
pub enum PartitionType {
    Read,
    Write,
    ReadWrite
}

impl PartitionFact {
    pub fn new(
        partition: Partition,
        partition_type: PartitionType
    ) -> Self {
        PartitionFact {
            partition,
            partition_type
        }
    }

    pub fn get_partition(&self) -> &Partition {
        &self.partition
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
    pub partition_facts: HashMap<String, PartitionFact>
}

impl Facts {

    pub fn new(partition_facts: HashMap<String, PartitionFact>) -> Facts
    {
        Facts {
            partition_facts
        }
    }



}

pub struct FactService {
    facts: Facts
}

impl FactService {
    pub fn new() -> Self {
        FactService {
            facts: Facts::new(HashMap::new())
        }
    }

    pub fn create_hash(&self, queue: &str, partition: u32) -> String {
        format!("{}_{}", queue, partition)
    }

    pub fn get_facts(&self) -> Facts {
        self.facts.clone()
    }

    pub fn apply(&mut self, config: &Config) {

        self.facts.partition_facts = HashMap::new();

        for configQueue in config.get_queues() {

            let partition_count = if configQueue.get_partitions_read() > configQueue.get_partitions_write() {
                configQueue.get_partitions_read()
            } else {
                configQueue.get_partitions_write()
            };

            for partition in 1..partition_count + 1 {

                let partition_type = if partition <= configQueue.get_partitions_read() && partition <= configQueue.get_partitions_write() {
                    PartitionType::ReadWrite
                } else if partition <= configQueue.get_partitions_write() {
                    PartitionType::Write
                } else {
                    PartitionType::Read
                };

                let fact = PartitionFact::new(
                    Partition::new(
                        Queue::new(configQueue.get_name().to_string()),
                        partition
                    ),
                    partition_type
                );

                let hash = self.create_hash(&configQueue.get_name(), partition);

                self.facts.partition_facts.insert(
                    hash,
                    fact
                );

            }

        }

    }
}

