extern crate r2d2;
extern crate cdrs;
extern crate rand;

use std::thread;
use rocket;
use cdrs::connection_manager::ConnectionManager;
use cdrs::authenticators::PasswordAuthenticator;
use cdrs::transport::TransportTcp;
use cdrs::compression::Compression;
use rocket::{Request, State, Outcome};
use std::ops::DerefMut;
use r2d2::CustomizeConnection;
use cdrs::transport::CDRSTransport;
use cdrs::authenticators::Authenticator;
use cdrs::query::QueryBuilder;
use driver::offset_handler::CassandraOffsetHandler;
use rand::thread_rng;
use rand::Rng;
use driver::queue_handler::CassandraQueueHandler;
use driver::queue_handler::QueueMsg;
use driver::CPool as Pool;
use service::fact::SharedFacts;
use service::config_service::SharedConfig;
use service::config_service::ConfigHelper;

#[get("/")]
pub fn index(facts: State<SharedFacts>, config: State<SharedConfig>) -> String {


    let f = facts.clone();
    let facts = { f.read().expect("no poison expected").clone() };

    format!("{} => {:#?}", config.get_endpoint(), facts)
}


#[post("/queue/<queue>", data = "<input>")]
pub fn push(queue: String, input: String, csdr: State<Pool>) -> &'static str {
    let pool: &Pool = &*csdr;

    match pool.get() {
        Ok(mut conn) => {
            let mut rng = thread_rng();
            let partition: u32 = rng.gen_range(1, 4);

            let mut session: &mut ::cdrs::client::Session<_, _> = conn.deref_mut();

            let next_id = session.get_latest_offset(&queue, partition).expect("could not get id") + 1;

            let query = ::cdrs::query::QueryBuilder::new("insert into queue (\"queue\", \"part\", \"id\", \"msg\", \"date\") values (?, ?, ?, ?, ?);").values(vec![
                queue.into(),
                partition.into(),
                next_id.into(),
                input.into(),
                ::time::get_time().into()
            ]).finalize();

            session.query(query, false, false).expect("Insert into queue_locks failed");

            "{\"success\": true }"
        }
        Err(_) => {
            "{\"success\": false, \"reason\": \"could not get connection from pool\" }"
        }
    }
}

#[get("/queue/<queue>/<partition>/<offset>")]
pub fn queue_get(queue: String, partition: u32, offset: u32, csdr: State<Pool>) -> String {
    let pool: &Pool = &*csdr;

    match pool.get() {
        Ok(mut conn) => {

            let res : Result<Option<QueueMsg>, String> = conn.get_queue_msg(&queue, partition, offset);

            match res {
                Err(e) => format!("{{\"success\": false, \"reason\": \"{}\" }}", e),
                Ok(None) => "{}".into(),
                Ok(Some(msg)) => msg.content.into()
            }
        }
        Err(_) => {
            "{\"success\": false, \"reason\": \"could not get connection from pool\" }".into()
        }
    }
}


#[derive(Debug)]
struct ConnectionCustomizerKeyspace;

impl ConnectionCustomizerKeyspace {
    pub fn new() -> Self {
        ConnectionCustomizerKeyspace {}
    }
}

impl<E, A, T> CustomizeConnection<::cdrs::client::Session<A, T>, E> for ConnectionCustomizerKeyspace
    where A: Authenticator,
          T: CDRSTransport,
          E: ::std::error::Error + 'static
{
    fn on_acquire(&self, session: &mut ::cdrs::client::Session<A, T>) -> Result<(), E> {
        println!("-----new connection----");
        let query = QueryBuilder::new("use queue;").finalize();

        session.query(query, false, false).expect("Insert into queue_locks failed");
        Ok(())
    }
}

pub struct Api;

impl Api {
    pub fn init_pool() -> Pool {
        let config = r2d2::Config::builder()
            .pool_size(1)
            .connection_customizer(Box::new(ConnectionCustomizerKeyspace::new()))
            .build();

        let transport = TransportTcp::new("127.0.0.1:9042").unwrap();
        let authenticator = PasswordAuthenticator::new("user", "pass");
        let manager = ConnectionManager::new(transport, authenticator, Compression::None);

        r2d2::Pool::new(config, manager).expect("could not get pool")
    }

    pub fn run(
        shared_facts: SharedFacts,
        shared_config: SharedConfig
    ) {
        thread::spawn(|| {
            rocket::ignite()
                .manage(Self::init_pool())
                .manage(shared_facts)
                .manage(shared_config)
                .mount("/", routes![index, queue_get, push]).launch();
        });
    }
}