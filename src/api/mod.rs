extern crate r2d2;
extern crate cdrs;

use std::{thread};
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


#[get("/")]
pub fn index(csdr: State<Pool>) -> &'static str {

    let pool : &Pool = &*csdr;

    match pool.get() {
        Ok(mut conn) => {

            let mut session : &mut ::cdrs::client::Session<_, _> = conn.deref_mut();

            let query = ::cdrs::query::QueryBuilder::new("insert into queue (\"queue\", \"part\", \"id\", \"msg\", \"date\") values (?, ?, ?, ?, ?);").values(vec![
                "foo".into(),
                1.into(),
                5.into(),
                "mgs".into(),
                ::time::get_time().into()
            ]).finalize();

            session.query(query, false, false).expect("Insert into queue_locks failed");

            "got connection from pool"
        },
        Err(_) => {
            "could not get connection"
        } 
    }

}

type Pool = ::r2d2::Pool<::cdrs::connection_manager::ConnectionManager<::cdrs::authenticators::PasswordAuthenticator<'static>, ::cdrs::transport::TransportTcp>>;


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

        let query = QueryBuilder::new("use queue;").finalize();

        session.query(query, false, false).expect("Insert into queue_locks failed");
        Ok(())
    }

}

pub struct Api;

impl Api {

    pub fn init_pool() -> Pool {

        let config = r2d2::Config::builder()
            .pool_size(15)
            .connection_customizer(Box::new(ConnectionCustomizerKeyspace::new()))
            .build();
        
        let transport = TransportTcp::new("127.0.0.1:9042").unwrap();
        let authenticator = PasswordAuthenticator::new("user", "pass");
        let manager = ConnectionManager::new(transport, authenticator, Compression::None);
        
        r2d2::Pool::new(config, manager).expect("could not get pool")
    }

    pub fn run() {

        thread::spawn(||{

            rocket::ignite()
                .manage(Self::init_pool())
                .mount("/", routes![index]).launch();
        });
    }

}