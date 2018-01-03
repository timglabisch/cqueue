extern crate cdrs;

use cdrs::transport::CDRSTransport;
use cdrs::query::QueryBuilder;
use cdrs::types::IntoRustByName;
use cdrs::client::Session;
use cdrs::types::rows::Row;
use cdrs::authenticators::Authenticator;


pub struct QueueMsg {
    queue: String,
    partition: i32,
    offset: i32,
    pub content: String,
}

impl QueueMsg {
    pub fn parse_from_cassandra_row(row: &Row) -> Result<Self, String> {
        let offset = row
            .get_by_name("id")
            .or_else(|_| Err("[parse_from_cassandra_row] could not find field id".to_string()))?
            .ok_or_else(|| "[renew lock] could not parse field id".to_string())?;

        let partition = row
            .get_by_name("part")
            .or_else(|_| Err("[parse_from_cassandra_row] could not find field part".to_string()))?
            .ok_or_else(|| "[renew lock] could not parse field part".to_string())?;

        let queue = row
            .get_by_name("queue")
            .or_else(|_| Err("[parse_from_cassandra_row] could not find field queue".to_string()))?
            .ok_or_else(|| "[renew lock] could not parse field queue".to_string())?;

        let content = row
            .get_by_name("msg")
            .or_else(|_| Err("[parse_from_cassandra_row] could not find field msg".to_string()))?
            .ok_or_else(|| "[renew lock] could not parse field msg".to_string())?;

        Ok(
            QueueMsg {
                queue,
                offset,
                partition,
                content,
            }
        )
    }
}

pub trait CassandraQueueHandler {
    fn get_queue_msg(&mut self, queue: &str, partition: u32, offset: u32) -> Result<Option<QueueMsg>, String>;
}


impl<A, T> CassandraQueueHandler for Session<A, T> where A: Authenticator, T: CDRSTransport {
    fn get_queue_msg(&mut self, queue: &str, partition: u32, offset: u32) -> Result<Option<QueueMsg>, String> {
        let raw_query = QueryBuilder::new("select * from queue where queue = ? and part = ? and id = ? limit 1;").values(vec![
            queue.into(),
            partition.into(),
            offset.into()
        ]).finalize();


        let raw_query_resulr: ::cdrs::frame::Frame = self.query(raw_query, false, false)
            .or_else(|_| Err("[get_queue_msg] Select failed"))?;

        let body = raw_query_resulr
            .get_body()
            .or_else(|_| { Err("[get_queue_msg] could not get body") })?;

        let rows = body
            .into_rows()
            .ok_or_else(|| "[get_queue_msg] could not parse rows".to_string())?;


        if rows.len() == 0 {
            return Ok(None);
        }

        if rows.len() > 1 {
            return Err("to much rows returned".to_string());
        }

        match rows.first() {
            None => Ok(None),
            Some(ref row) => {
                match QueueMsg::parse_from_cassandra_row(row) {
                    Err(e) => Err(e),
                    Ok(r) => Ok(Some(r))
                }
            }
        }
    }
}