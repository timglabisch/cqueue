use dto::Partition;
use time::Timespec;
use driver::{Pool};
use cdrs::query::QueryBuilder;
use cdrs::types::rows::Row;
use cdrs::types::IntoRustByName;
use dto::Queue;

#[derive(Debug, Clone)]
pub struct GlobalFact {
    pub partition: Partition,
    pub lock_until: Option<Timespec>,
    pub owner: Option<String>,
}

pub struct GlobalFactCassandraService<'a, P> where P: Pool, P: 'a {
    pool: &'a P
}

pub trait GlobalFactService {

    fn get_global_facts(&self) -> Result<Vec<GlobalFact>, String>;

}

impl<'a, P> GlobalFactCassandraService<'a, P> where P: Pool, P: 'a {

    pub fn new(pool : &'a P) -> Self {
        GlobalFactCassandraService {
            pool
        }
    }

}

impl<'a, P> GlobalFactService for GlobalFactCassandraService<'a, P> where P: Pool, P: 'a {

    fn get_global_facts(&self) -> Result<Vec<GlobalFact>, String> {
        let mut pool = self.pool.get().map_err(|_|  "[global facts] could not get connection from pool".to_string())?;
        let conn = pool.getConnection();

        let queue_locks_query = QueryBuilder::new("SELECT * from queue_locks").values(vec![]).finalize();

        let queue_locks_query_result = conn.query(queue_locks_query).map_err(|_| "[global facts] load query failed".to_string())?;

        let body = queue_locks_query_result.get_body().map_err(|_| "C".to_string())?;

        body
            .into_rows()
            .ok_or_else(|| "[global facts] could not parse rows".to_string())?
            .iter()
            .map(| row : &Row| {

                let raw_partition : i32 = row
                    .get_by_name("part")
                    .or_else(|_| Err("[parse_from_cassandra_row] could not find field part".to_string()))?
                    .ok_or_else(|| "[renew lock] could not parse field partition".to_string())?;

                let raw_queue : String = row
                    .get_by_name("queue")
                    .or_else(|_| Err("[parse_from_cassandra_row] could not find field part".to_string()))?
                    .ok_or_else(|| "[renew lock] could not parse field raw_queue".to_string())?;

                Ok(
                    GlobalFact {
                        partition: Partition::new(Queue::new(raw_queue), raw_partition as u32),
                        lock_until: row
                            .get_by_name("lock")
                            .or_else(|_| Err("[parse_from_cassandra_row] could not find field part".to_string()))?,
                        owner: row
                            .get_by_name("owner")
                            .or_else(|_| Err("[parse_from_cassandra_row] could not find field part".to_string()))?,
                    }
                )

            })
            .collect()

    }

}