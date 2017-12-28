extern crate cdrs;
extern crate time;
extern crate rand;

mod lock_handler;
mod driver;

use cdrs::client::CDRS;
use cdrs::authenticators::PasswordAuthenticator;
use cdrs::transport::TransportTcp;
use cdrs::compression::Compression;
use cdrs::query::{QueryBuilder, QueryParamsBuilder};
use std::convert::Into;
use cdrs::types::value::{Value, Bytes};
use lock_handler::LockHandler;
use std::{thread};



fn main() {

    let authenticator = PasswordAuthenticator::new("user", "pass");
    let addr = "127.0.0.1:9042";
    let tcp_transport = TransportTcp::new(addr).unwrap();

    // pass authenticator and transport into CDRS' constructor
    let client = CDRS::new(tcp_transport, authenticator);
    use cdrs::compression;
    
    // start session without compression
    let mut session = client.start(Compression::None).expect("session...");

    let q = QueryBuilder::new("use queue").finalize();
    match session.query(q, false, false) {
        Err(ref err) => panic!("create_table map (v3) {:?}", err),
        Ok(_) => true,
    }; 

    loop {

        thread::sleep(::std::time::Duration::from_millis(1000));

        let mut lockOption = session.lock_acquire("foo", 123);

        match lockOption {
            Err(_) => {
                println!("there was an error acquiring the lock");
                continue;
            },
            Ok(None) => {
                println!("could not get lock");
                continue;
            },
            Ok(Some(mut lock)) => {
                loop {

                    thread::sleep(::std::time::Duration::from_millis(1000));

                    match session.lock_renew(&mut lock) {
                        Ok(true) => {println!("renewed lock");},
                        Ok(false) => {
                            println!("could not renew lock");
                            break;
                        },
                        Err(e) => {
                            println!("error occured when trying to renew the lock. -> {}", e);
                            break;
                        }
                    };
                }
            }
        }
    }


    /*
    let query = QueryBuilder::new("INSERT INTO queue_foo_partition1 (id, date, msg) VALUES (?,?,?)").values(vec![
        123.into(),
        time::get_time().into(),
        "fooo".into()
    ]).finalize();

    let inserted = session.query(query, false, false);
    
    match inserted {
        Err(ref err) => panic!("inserted str {:?}", err),
        Ok(_) => true,
    };
    */

    println!("Hello, world!");
}
