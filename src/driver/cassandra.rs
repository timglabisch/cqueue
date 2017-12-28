use driver::ConnectionConfig;
use cdrs::client::Session;
use cdrs::authenticators::Authenticator;
use cdrs::transport::CDRSTransport;
use cdrs::query::QueryBuilder;
use lock_handler::LockHandler;
use lock_handler::AcquiredLock;
use time;
use cdrs::types::ByName;
use time::Timespec;
use cdrs::types::IntoRustByName;
use rand;

pub struct ConnectionCassandra {
    //session: Box<Session<::cdrs::authenticators::PasswordAuthenticator, ::cdrs::transport::TransportTcp>>
}

impl<A, T> LockHandler for Session<A, T> where A: Authenticator, T: CDRSTransport {
    
    fn lock_acquire(&mut self, queue: &str, partition: u32) -> Result<Option<AcquiredLock>, String>
    {
        let query = QueryBuilder::new("INSERT INTO queue_locks (queue, part) VALUES (?,?) IF NOT EXISTS USING TTL ?").values(vec![
            queue.into(),
            partition.into(),
            5.into()
        ]).finalize();

        self.query(query, false, false).or_else(|_| Err("Insert into queue_locks failed"))?;

        let seed = rand::random::<u32>();

        let time = ::time::get_time();
        let update_lock_query = QueryBuilder::new("UPDATE queue_locks USING TTL ? SET lock = ?, seed = ? WHERE queue = ? AND part = ? IF seed = null").values(vec![
            5.into(),
            time.into(),
            seed.into(),
            queue.into(),
            partition.into()
        ]).finalize();

        let update_lock_query_result : ::cdrs::frame::Frame = self.query(update_lock_query, false, false)
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

        Ok(Some(AcquiredLock::new(queue.to_string(), partition, seed, time)))
    }

    fn lock_renew(&mut self, lock: &mut AcquiredLock) -> Result<bool, String> {

        let valid_until = ::time::get_time();

        let update_lock_query = QueryBuilder::new("UPDATE queue_locks USING TTL ? SET lock = ?, seed = ? WHERE queue = ? AND part = ? IF seed = ?").values(vec![
            5.into(),
            valid_until.into(),
            lock.get_seed().into(),
            lock.get_queue().into(),
            lock.get_partition().into(),
            //lock.get_valid_until().clone().into(),
            lock.get_seed().into(),
        ]).finalize();

        lock.update_valid_until(valid_until);

        let update_lock_query_result : ::cdrs::frame::Frame = self.query(update_lock_query, false, false)
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

        Ok(applied)
    }
}

impl ConnectionCassandra {
    pub fn new(config: ConnectionConfig) -> Self {
        /*
        let authenticator = PasswordAuthenticator::new(config.get_user(), config.get_pass());
        let tcp_transport = TransportTcp::new(config.get_dsn()).expect("failed to get connection");

        // pass authenticator and transport into CDRS' constructor
        let client = CDRS::new(tcp_transport, authenticator);
        use cdrs::compression;
        
        // start session without compression
        let mut session = client.start(Compression::None).expect("session...");
        
        */
        ConnectionCassandra {
            
        } 
    }
}