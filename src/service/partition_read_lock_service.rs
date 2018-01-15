use std::sync::RwLock;
use service::fact::SharedFacts;
use dto::Queue;
use rand;
use time::Tm;
use std::ops::Deref;
use std::ops::DerefMut;
use std::collections::HashMap;
use service::fact::PartitionLockType;
use service::fact::PartitionFact;
use std::collections::hash_map::Entry;


struct PartitionReadLockService {
    shared_facts: SharedFacts,
    locks: RwLock<HashMap<String, PartitionLockInfo>>
}

struct PartitionReadRequest {
    queues: Vec<Queue>,
    valid_until: Tm
}

impl PartitionReadRequest {
    pub fn get_queues(&self) -> &Vec<Queue> {
        &self.queues
    }
}


#[derive(Clone, Debug)]
struct PartitionLockInfo {
    pub valid_until: Tm,
    pub partition_fact: PartitionFact
}

impl PartitionReadLockService {

    pub fn try_lock(&self, request: PartitionReadRequest) -> Option<PartitionLockInfo> {

        let possibilities = {

            let mut buffer = Vec::new();

            let shared_facts_arc = self.shared_facts.clone();
            let shared_facts = shared_facts_arc.deref();


            let facts = shared_facts.read().expect("lock cant be poisened");

            for (_, partition_fact) in &facts.deref().partition_facts {


                for request_queue in request.get_queues() {

                    if partition_fact.get_partition().get_queue_name() != request_queue.get_name() {
                        continue;
                    }

                    match partition_fact.get_partition_lock() {
                        &PartitionLockType::LockUntil { ref lock, .. } => {
                            //lock.valid_until.
                            if ::time::at_utc(lock.get_valid_until().clone()) < request.valid_until {
                                continue;
                            }
                        },
                        _ => {
                            continue;
                        }
                    };

                    buffer.push(partition_fact.clone());
                }

            }

            buffer
        };

        let mut rng = rand::thread_rng();
        let samples = rand::sample(&mut rng, &possibilities, possibilities.len());

        for possibility in &possibilities {
            let mut map_lock_handle = self.locks.write().expect("should not be poisened");
            let map = map_lock_handle.deref_mut();

            match map.entry(possibility.get_partition().to_string()) {
                Entry::Vacant(mut ent) => {

                    let res = PartitionLockInfo {
                        valid_until: request.valid_until,
                        partition_fact: possibility.clone()
                    };

                    ent.insert(res.clone());

                    return Some(res)
                },
                Entry::Occupied(mut ent) => {

                    if ent.get().valid_until > ::time::now() {
                        continue;
                    }

                    let res = PartitionLockInfo {
                        valid_until: request.valid_until,
                        partition_fact: possibility.clone()
                    };

                    ent.insert(res.clone());

                    return Some(res)
                }
            }

        }

        return None;

    }

}