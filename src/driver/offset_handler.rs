extern crate cdrs;

use cdrs::authenticators::Authenticator;
use cdrs::transport::CDRSTransport;
use cdrs::query::QueryBuilder;
use cdrs::types::IntoRustByName;
use cdrs::client::Session;

/*struct OffsetHandler<S> where S: Session<A, T>, A: Authenticator, T: CDRSTransport {
    inner : S
}
*/

pub trait CassandraOffsetHandler {

    fn get_latest_offset(&mut self, queue: &str, partition: u32) -> Result<i32, String>;

    fn commit(&self, queue: &str, partition: u32) -> bool;

}



impl<A, T> CassandraOffsetHandler for Session<A, T> where A: Authenticator, T: CDRSTransport {

    fn get_latest_offset(&mut self, queue: &str, partition: u32) -> Result<i32, String>
    {

        let max_offset_query = QueryBuilder::new("select id from queue where queue = ? and part = ? order by id desc limit 1;").values(vec![
            queue.into(),
            partition.into()
        ]).finalize();


        let max_offset_query_result : ::cdrs::frame::Frame = self.query(max_offset_query, false, false)
            .or_else(|_| Err("[renew lock]  Update Queue Locks failed"))?;

        let body = max_offset_query_result
            .get_body()
            .or_else(|_| { Err("update queue_locks query failed") })?;

        let rows = body
            .into_rows()
            .ok_or_else(|| "[renew lock] could not parse rows [applied] rows".to_string())?;


        if rows.len() == 0 {
            return Ok(0);
        }

        Ok(
            rows
                .get(0)
                .ok_or_else(|| "[renew lock] could not find [applied] row".to_string())?
                .get_by_name("id")
                .or_else(|_| Err("[renew lock] could not find applied field".to_string()))?
                .ok_or_else(|| "[renew lock] could not parse [applied] field".to_string())?
        )

    }

    fn commit(&self, queue: &str, partition: u32) -> bool
    {
        unimplemented!(); // wir brauchen eine 2. queue zum commiteen
    }
}