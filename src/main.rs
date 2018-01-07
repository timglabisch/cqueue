#![feature(plugin)]
#![plugin(rocket_codegen)]
extern crate cdrs;
extern crate time;
extern crate rand;
extern crate rocket;
extern crate r2d2;
extern crate toml;
extern crate serde;
#[macro_use] extern crate serde_derive;

mod lock_handler;
mod driver;
mod api;
mod config;
mod dto;
mod service;

use cdrs::client::CDRS;
use cdrs::authenticators::PasswordAuthenticator;
use cdrs::transport::TransportTcp;
use cdrs::compression::Compression;
use cdrs::query::{QueryBuilder, QueryParamsBuilder};
use std::convert::Into;
use cdrs::types::value::{Value, Bytes};
use std::{thread};
use config::Config;
use service::fact::FactService;
use service::lock_handler::LockHandler;
use driver::Pool;
use service::fact::PartitionLockType;


fn main() {

    let pool = ::api::Api::init_pool();

    //let mut pool : ::;

    let config = Config::new_from_file("config.toml").expect("could not read config.toml");

    let mut fact_service = FactService::new();
    fact_service.apply(&config);


    ::api::Api::run(fact_service.get_shared_facts());


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

    run_maintain_locks(&pool, &mut fact_service);

}

pub fn run_maintain_locks<P: Pool>(pool : &P, fact_service: &mut FactService) {

    let mut lock_handler = LockHandler::new(pool);

    loop {

        let facts = fact_service.get_facts();


        for (_, parition_fact) in &facts.partition_facts {

            let partition = parition_fact.get_partition();

            match *parition_fact.get_partition_lock() {
                PartitionLockType::LockUntil { ref lock, .. } => {
                    // try to renew the lock

                    thread::sleep(::std::time::Duration::from_millis(400));

                    match lock_handler.lock_renew(&lock) {
                        Ok(Some(lock)) => {
                            println!("renewed lock {}::{}", &partition.get_queue_name(), &partition.get_id());
                            fact_service.update_partition_lock(partition, PartitionLockType::LockUntil {
                                lock: lock.clone(),
                                owner: "unknwon".to_string()
                            })
                        },
                        Ok(None) => {
                            println!("could not renew lock {}::{}", &partition.get_queue_name(), &partition.get_id());
                            fact_service.update_partition_lock(partition, PartitionLockType::Unknown);
                        },
                        Err(e) => {
                            println!("error occured when trying to renew the lock. -> {}", e);
                            fact_service.update_partition_lock(partition, PartitionLockType::Unknown);
                        }
                    };

                },
                _ => {

                    thread::sleep(::std::time::Duration::from_millis(100));

                    let lock_option = lock_handler.lock_acquire(&partition);

                    match lock_option {
                        Err(_) => {
                            println!("there was an error acquiring the lock");
                            fact_service.update_partition_lock(partition, PartitionLockType::Unknown);
                        },
                        Ok(None) => {
                            println!("could not get lock {}::{}", &partition.get_queue_name(), &partition.get_id());
                            fact_service.update_partition_lock(partition, PartitionLockType::Unknown);
                        },
                        Ok(Some(mut lock)) => {
                            println!("ackquired lock");
                            fact_service.update_partition_lock(partition, PartitionLockType::LockUntil {
                                lock: lock.clone(),
                                owner: "unknwon".to_string()
                            })
                        }
                    }

                }
            }


        }


        fact_service.commit_facts();
        println!("--------------");

        thread::sleep(::std::time::Duration::from_millis(500));


    }

}