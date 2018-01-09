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

pub trait OffsetHandler {

    fn get_latest_offset(&mut self, partition : &Partition) -> Result<i32, String>;

    fn commit(&mut self, queue: &str, partition : &Partition, offset: i32) -> Result<(), String>;

    fn get_last_commited_offset(&mut self, partition : &Partition) -> Result<i32, String>;

}

pub type SharedOffsetHandler = Arc<RwLock<Box<OffsetHandler>>>;

pub struct CassandraOffsetHandler<P> where P: Pool {
    pool: P
}


impl<P> CassandraOffsetHandler<P> where P: Pool {

    pub fn new(pool: P) -> Self {
        CassandraOffsetHandler {
            pool
        }
    }

}

impl<P> OffsetHandler for CassandraOffsetHandler<P> where P: Pool {

    fn get_latest_offset(&mut self, partition : &Partition) -> Result<i32, String>
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

