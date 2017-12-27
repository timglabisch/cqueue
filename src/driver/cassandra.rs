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
    
    fn lock_acquire(&mut self, queue: &str, partition: u32) -> Option<AcquiredLock>
    {
        let query = QueryBuilder::new("INSERT INTO queue_locks (queue, part) VALUES (?,?) IF NOT EXISTS USING TTL ?").values(vec![
            queue.into(),
            partition.into(),
            5.into()
        ]).finalize();

        self.query(query, false, false).expect("#1 failed");

        let prepare_lock_query = QueryBuilder::new("SELECT * FROM queue_locks WHERE queue = ? AND part = ? LIMIT 1").values(vec![
            queue.into(),
            partition.into(),
        ]).finalize();

        let prepare_lock_query_result : ::cdrs::frame::Frame = self.query(prepare_lock_query, false, false).expect("#2 failed");

        let rows = prepare_lock_query_result.get_body()
            .expect("could not get body #1")
            .into_rows()
            .expect("could not get rows #1");

        let x : Option<Timespec> = rows
            .get(0)
            .expect("could not get first row")
            .get_by_name("lock")
            .expect("could not read timespec");

        let seed = rand::random::<u32>();

        let time = ::time::get_time();
        let update_lock_query = QueryBuilder::new("UPDATE queue_locks USING TTL ? SET lock = ?, seed = ? WHERE queue = ? AND part = ? IF lock = ? AND seed = null").values(vec![
            5.into(),
            time.into(),
            seed.into(),
            queue.into(),
            partition.into(),
            x.into()
        ]).finalize();

        let update_lock_query_result : ::cdrs::frame::Frame = self.query(update_lock_query, false, false).expect("#3 failed");

        let body = update_lock_query_result
            .get_body()
            .expect("could not get body #2");

        let cols : &::cdrs::frame::frame_result::BodyResResultRows = body
            .as_cols()
            .expect("could not get rows #2");

        let col0 : &::cdrs::frame::frame_result::ColSpec = cols.metadata.col_specs.get(0).expect("foo");

        if col0.name.as_str() != "[applied]" {
            return None;
        }        

        Some(AcquiredLock::new(queue.to_string(), partition, seed, time))
    }

    fn lock_renew(&mut self, lock: &mut AcquiredLock) -> bool {

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

        let update_lock_query_result : ::cdrs::frame::Frame = self.query(update_lock_query, false, false).expect("#4 failed");

        let body = update_lock_query_result
            .get_body()
            .expect("could not get body #r1");


        //let cols : &::cdrs::frame::frame_result::BodyResResultRows = body
        //    .as_cols()
        //    .expect("could not get rows #r1");

        //let col0 : &::cdrs::frame::frame_result::ColSpec = cols.metadata.col_specs.get(0).expect("foo");
        //let content0 : ::cdrs::types::rows::Row = cols.rows_content.get(0).expect("could not parse").clone();

        let rows = body.into_rows();

        let applied : bool = rows.expect("could not parse rows").get(0).expect("could not get first").get_by_name("[applied]").expect("could not parse applied").expect("no value given");

        applied
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