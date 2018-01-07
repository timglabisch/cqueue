use driver::Pool;
use cdrs::query::QueryBuilder;
use rand::random;
use cdrs::frame::Frame;
use lock_handler::AcquiredLock;
use cdrs::types::IntoRustByName;
use std::clone::Clone;
use dto::Partition;
use service::config_service::SharedConfig;
use service::config_service::ConfigHelper;

pub struct LockHandler<'a, P> where P: Pool, P: 'a {
    pool : &'a P,
    config: SharedConfig
}

impl<'a, P> LockHandler<'a, P> where P: Pool, P: 'a {

    pub fn new(pool: &'a P, config: SharedConfig) -> Self {
        LockHandler {
            pool,
            config
        }
    }

    pub fn lock_acquire(&mut self, partition : &Partition) -> Result<Option<AcquiredLock>, String>
    {

        let mut pool = self.pool.get().map_err(|_| "could not get conection from pool".to_string())?;
        let mut connection = pool.getConnection();

        let query = QueryBuilder::new("INSERT INTO queue_locks (queue, part) VALUES (?,?) IF NOT EXISTS USING TTL ?").values(vec![
            partition.get_queue_name().into(),
            partition.get_id().into(),
            5.into()
        ]).finalize();

        connection.query(query).or_else(|_| Err("Insert into queue_locks failed"))?;

        let seed = random::<u32>();

        let time = ::time::get_time();
        let update_lock_query = QueryBuilder::new("UPDATE queue_locks USING TTL ? SET seed = ? WHERE queue = ? AND part = ? IF seed = null").values(vec![
            100.into(),
            seed.into(),
            partition.get_queue_name().into(),
            partition.get_id().into()
        ]).finalize();

        let update_lock_query_result : Frame = connection.query(update_lock_query)
            .or_else(|_| Err("Update Queue Locks failed"))?;

        let body = update_lock_query_result
            .get_body()
            .or_else(|_| Err("Could not parse Update Queue Locks Query"))?;

        let rows = body.into_rows();

        let applied : bool = rows
            .ok_or_else(|| "could not parse rows [applied] rows".to_string())?
            .get(0)
            .ok_or_else(|| "could not find [applied] row".to_string())?
            .get_by_name("[applied]")
            .or_else(|_| Err("could not find applied field".to_string()))?
            .ok_or_else(|| "could not parse [applied] field".to_string())?;

        if !applied {
            return Ok(None);
        }

        let update_owner_query = QueryBuilder::new("UPDATE queue_locks USING TTL ? SET seed = ?, owner = ?, lock = ? WHERE queue = ? AND part = ? IF seed = ?").values(vec![
            100.into(),
            seed.into(),
            self.config.get_endpoint().into(),
            time.into(),
            partition.get_queue_name().into(),
            partition.get_id().into(),
            seed.into(),
        ]).finalize();

        let update_owner_query_result : Frame = connection.query(update_owner_query)
            .or_else(|_| Err("Update owner Query failed"))?;

        Ok(Some(AcquiredLock::new(partition.get_queue_name().to_string(), partition.get_id(), seed, time)))
    }

    pub fn lock_renew(&mut self, lock: &AcquiredLock) -> Result<Option<AcquiredLock>, String> {


        let mut pool = self.pool.get().map_err(|_| "could not get conection from pool".to_string())?;
        let mut connection = pool.getConnection();

        let valid_until = ::time::get_time();

        let update_lock_query = QueryBuilder::new("UPDATE queue_locks USING TTL ? SET seed = ? WHERE queue = ? AND part = ? IF seed = ?").values(vec![
            100.into(),
            lock.get_seed().into(),
            lock.get_queue().into(),
            lock.get_partition().into(),
            lock.get_seed().into(),
        ]).finalize();

        let mut updated_lock = lock.clone();

        updated_lock.update_valid_until(valid_until);

        let update_lock_query_result : Frame = connection.query(update_lock_query)
            .or_else(|_| Err("[renew lock]  Update Queue Locks failed"))?;

        let body = update_lock_query_result
            .get_body()
            .or_else(|_| { Err("update queue_locks query failed") })?;

        let rows = body.into_rows();

        let applied : bool = rows
            .ok_or_else(|| "[renew lock] could not parse rows [applied] rows".to_string())?
            .get(0)
            .ok_or_else(|| "[renew lock] could not find [applied] row".to_string())?
            .get_by_name("[applied]")
            .or_else(|_| Err("[renew lock] could not find applied field".to_string()))?
            .ok_or_else(|| "[renew lock] could not parse [applied] field".to_string())?;

        if !applied {
            return Ok(None);
        }

        Ok(Some(updated_lock.clone()))
    }

}