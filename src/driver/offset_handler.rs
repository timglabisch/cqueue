use cdrs::authenticators::Authenticator;
use cdrs::transport::CDRSTransport;
use cdrs::query::QueryBuilder;
use cdrs::types::IntoRustByName;
use cdrs::client::Session;
use dto::Partition;
use driver::Pool;
use std::sync::{RwLock, Arc};
use driver::Connection;
use dto::Queue;
use std::collections::HashMap;
use service::fact::SharedFacts;
use service::fact::PartitionLockType;
use std::collections::hash_map::Entry;
use rand;
use std::ops::Deref;

pub type SharedOffsetHandler = Arc<RwLock<Box<OffsetHandler>>>;


pub struct OffsetHandler {
    driver_handler: CassandraOffsetHandler,
    partitions: RwLock<HashMap<String, RwLock<i32>>>,
    shared_facts: SharedFacts
}

impl OffsetHandler {

    pub fn new(
        driver_handler: CassandraOffsetHandler,
        shared_facts: SharedFacts
    ) -> Self {
        OffsetHandler {
            driver_handler,
            partitions: RwLock::new(HashMap::new()),
            shared_facts
        }
    }

    pub fn block_queue_and<T>(&self, queue : &Queue, cb : T) -> Result<(), String> where T: Fn(&Partition, i32) -> Result<(), String> {

        let partition = {
            let shared_fact_arc = self.shared_facts.clone();
            let shared_fact = shared_fact_arc.read().expect("should not be poinsened");


            let mut rng = rand::thread_rng();
            let mut samples : &Vec<u32> = shared_fact
                .deref()
                .partitions_by_queue
                .get(queue.get_name())
                .ok_or_else(|| "could not find any partitions for queue".to_string())?
            ;

            let sample = rand::sample(&mut rng, samples, 1);

            let x = sample.first().expect("could not find any partition").clone();
            Partition::new(queue.clone(), *x)
        };

        self.block_partition_and(&partition, cb)
    }

    pub fn block_partition_and<T>(&self, partition : &Partition, cb : T) -> Result<(), String> where T: Fn(&Partition, i32) -> Result<(), String> {

        let partition_lock_type = {
            let shared_fact_arc = self.shared_facts.clone();
            let shared_fact = shared_fact_arc.read().expect("should not be poinsened");
            shared_fact.get_partition_fact(&partition)
                .and_then(|p| Some(p.get_partition_lock().clone()))
        };

        let aquired_lock = match partition_lock_type {
            Some(PartitionLockType::LockUntil { lock, ..}) => {
                lock
            },
            _ => {
                return Err("we dont maintain the lock, so it's not possible to block the partition".to_string());
            }
        };

        // todo, here we need to check if we've enought time.

        let hash = partition.to_string();

        // todo, this could become a memory leak
        {
            let mut partition_map = self.partitions.write().expect("lock could not be poisened");

            match partition_map.entry(hash.clone()) {
                Entry::Vacant(entry) => {
                    entry.insert(RwLock::new(self.driver_handler.get_latest_offset(&partition)?));
                },
                _ => {}
            };
        }

        {
            let partitions = self.partitions.read().expect("lock shoildnt be poisened");

            let mut partition_lock = partitions.get(&hash).expect("must be exists, because we never clean it up");

            let mut partition_write_lock_guard = partition_lock.write().expect("partition_lock should not poisened");

            let next_offset = &*partition_write_lock_guard + 1;

            let locked_function = cb(partition, next_offset)?;

            *partition_write_lock_guard = next_offset;
        }

        Ok(())
    }

}

pub struct CassandraOffsetHandler {
    pool: Box<Pool + Send + Sync>
}

impl CassandraOffsetHandler {

    pub fn new(pool: Box<Pool + Send + Sync>) -> Self {
        CassandraOffsetHandler {
            pool
        }
    }

    fn get_latest_offset(&self, partition : &Partition) -> Result<i32, String>
    {
        let max_offset_query = QueryBuilder::new("select id from queue where queue = ? and part = ? order by id desc limit 1;").values(vec![
            partition.get_queue_name().into(),
            partition.get_id().into()
        ]).finalize();


        let mut pooled_conn = self.pool.get().map_err(|_| "[get_latest_offset] could not get connection from pool".to_string())?;

        let max_offset_query_result: ::cdrs::frame::Frame = pooled_conn.getConnection().query(max_offset_query)
            .or_else(|_| Err("[get_latest_offset]  Update Queue Locks failed"))?;

        let body = max_offset_query_result
            .get_body()
            .or_else(|_| { Err("update get_latest_offset query failed") })?;

        let rows = body
            .into_rows()
            .ok_or_else(|| "[get_latest_offset] could not parse rows [applied] rows".to_string())?;


        if rows.len() == 0 {
            return Ok(0);
        }

        Ok(
            rows
                .get(0)
                .ok_or_else(|| "[get_latest_offset] could not find [applied] row".to_string())?
                .get_by_name("id")
                .or_else(|_| Err("[renew lock] could not find applied field".to_string()))?
                .ok_or_else(|| "[get_latest_offset] could not parse [applied] field".to_string())?
        )
    }

    fn commit(&mut self, queue: &str, partition : &Partition, offset : i32) -> Result<(), String>
    {
        let query = ::cdrs::query::QueryBuilder::new("insert into queue (\"queue\", \"part\", \"id\", \"msg\", \"date\") values (?, ?, ?, ?, ?);").values(vec![
            format!("{}_offset", partition.get_queue_name()).into(),
            partition.get_id().into(),
            offset.into(),
            "".into(),
            ::time::get_time().into()
        ]).finalize();

        let mut pooled_conn = self.pool.get().map_err(|_| "could not get connection from pool".to_string())?;

        pooled_conn.getConnection().query(query).expect("Insert into queue_locks failed");

        Ok(())
    }

    fn get_last_commited_offset(&mut self, partition : &Partition) -> Result<i32, String>
    {

        let offset_partition = Partition::new(Queue::new(format!("{}{}", partition.get_queue_name(), "_format")), partition.get_id());

        self.get_latest_offset(
            &offset_partition
        )
    }

}

