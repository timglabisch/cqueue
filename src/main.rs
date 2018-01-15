#![feature(plugin)]
#![plugin(rocket_codegen)]
#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(deprecated)]
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
use cdrs::query::{QueryBuilder};
use std::{thread};
use config::Config;
use service::fact::FactService;
use service::lock_handler::LockHandler;
use driver::Pool;
use service::fact::PartitionLockType;
use service::config_service::ConfigService;
use service::global_fact_service::{GlobalFactCassandraService, GlobalFactService};
use driver::offset_handler::CassandraOffsetHandler;
use std::sync::{Arc};
use service::queue_msg_service::CassandraQueueMsgService;
use driver::offset_handler::OffsetHandler;


fn main() {

    let pool = ::api::Api::init_pool();

    //let mut pool : ::;

    let config = Config::new_from_file("config.toml").expect("could not read config.toml");
    let config_service = ConfigService::new(config.clone());

    let mut fact_service = FactService::new();
    fact_service.apply(&config); // todo, should use the config service

    let offset_handler = OffsetHandler::new(CassandraOffsetHandler::new(Box::new(pool.clone())), fact_service.get_shared_facts());

    let queue_msg_service = CassandraQueueMsgService::new(Box::new(pool.clone()));

    ::api::Api::run(
        fact_service.get_shared_facts(),
        config_service.get_shared_config(),
        offset_handler,
        Arc::new(Box::new(queue_msg_service))
    );



    let authenticator = PasswordAuthenticator::new("user", "pass");
    let addr = "127.0.0.1:9042";
    let tcp_transport = TransportTcp::new(addr).unwrap();

    // pass authenticator and transport into CDRS' constructor
    let client = CDRS::new(tcp_transport, authenticator);
    
    // start session without compression
    let mut session = client.start(Compression::None).expect("session...");

    let q = QueryBuilder::new("use queue").finalize();
    match session.query(q, false, false) {
        Err(ref err) => panic!("create_table map (v3) {:?}", err),
        Ok(_) => true,
    };

    run_maintain_locks(&pool, &mut fact_service, &config_service);

}

pub fn run_maintain_locks<P: Pool>(pool : &P, fact_service: &mut FactService, config_service: &ConfigService) {

    let mut lock_handler = LockHandler::new(pool, config_service.get_shared_config());
    let global_fact_service = GlobalFactCassandraService::new(pool);

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
                        Err(e) => {
                            println!("there was an error acquiring the lock {}", &e);
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

        let global_facts = global_fact_service.get_global_facts().expect("could not fetch global facts");
        fact_service.update_global_facts(global_facts);

        fact_service.commit_facts();

        thread::sleep(::std::time::Duration::from_millis(500));


    }

}