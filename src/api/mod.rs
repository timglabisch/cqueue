use std::thread;
use rocket;
use cdrs::connection_manager::ConnectionManager;
use cdrs::authenticators::PasswordAuthenticator;
use cdrs::transport::TransportTcp;
use cdrs::compression::Compression;
use rocket::{State};
use r2d2::CustomizeConnection;
use cdrs::transport::CDRSTransport;
use cdrs::authenticators::Authenticator;
use cdrs::query::QueryBuilder;
use driver::CPool as Pool;
use service::fact::SharedFacts;
use service::config_service::SharedConfig;
use service::config_service::ConfigHelper;
use std::fmt::Write;
use service::fact::PartitionLockType;
use driver::offset_handler::OffsetHandler;
use std::sync::{Arc};
use dto::Partition;
use dto::Queue;
use service::queue_msg_service::QueueMsgService;

#[get("/info")]
pub fn info(facts: State<SharedFacts>, config: State<SharedConfig>) -> Result<String, Box<::std::fmt::Error>> {
    let f = facts.clone();
    let facts = { f.read().expect("no poison expected").clone() };


    let partitions = facts.global_partition_facts
        .iter()
        .map(|(_, global_fact)| {
            let mut buffer = String::new();

            buffer.write_str(&format!("\n\t\t\"{}_{}\": {{\n", global_fact.partition.get_queue_name(), &global_fact.partition.get_id().to_string()))?;

            buffer.write_str("\t\t\t\"partition_id\": ")?;
            buffer.write_str(&global_fact.partition.get_id().to_string())?;
            buffer.write_str(",\n")?;

            buffer.write_str("\t\t\t\"queue_name\": \"")?;
            buffer.write_str(global_fact.partition.get_queue_name())?;
            buffer.write_str("\",\n")?;


            if let Some(ref lock_since) = global_fact.lock_until {
                buffer.write_str("\t\t\t\"lock_since\": \"")?;
                buffer.write_str(&::time::at_utc(lock_since.clone()).rfc3339().to_string())?;
                buffer.write_str("\"")?;
            } else {
                buffer.write_str("\t\t\t\"lock_since\": null")?;
            };

            buffer.write_str(",\n")?;

            if let Some(ref owner) = global_fact.owner {
                buffer.write_str("\t\t\t\"endpoint\": \"")?;
                buffer.write_str(owner)?;
                buffer.write_str("\"")?;
            } else {
                buffer.write_str("\t\t\t\"endpoint\": null")?;
            };

            buffer.write_str("\n\t\t}")?;

            Ok(buffer)
        })
        .collect::<Result<Vec<String>, Box<::std::fmt::Error>>>()?
        .join(",");


    let mut buffer = String::new();
    buffer.write_str("{\n")?;
    buffer.write_str(&format!("\t\"now\": \"{}\",\n", &::time::now_utc().rfc3339().to_string()))?;
    buffer.write_str("\t\"global_partition_facts_updated_at\": ")?;

    if let Some(ref global_partition_facts_updated_at) = facts.global_partition_facts_updated_at {
        buffer.write_str("\"")?;
        buffer.write_str(&global_partition_facts_updated_at.rfc3339().to_string())?;
        buffer.write_str("\"")?;
    } else {
        buffer.write_str("null")?;
    };


    buffer.write_str(",\n")?;
    buffer.write_str("\t\"partitons\":{")?;
    buffer.write_str(&partitions)?;
    buffer.write_str("\n\t},")?;


    let locks = facts.partition_facts.iter().map(|(_, partiton_fact)|{

        let mut buffer = String::new();

        buffer.write_str(&format!("\n\t\t\"{}_{}\":", partiton_fact.get_partition().get_queue_name(), partiton_fact.get_partition().get_id()))?;
        buffer.write_str("{\n")?;
        buffer.write_str(&format!("\t\t\t\"queue\": \"{}\",\n", partiton_fact.get_partition().get_queue_name()))?;
        buffer.write_str(&format!("\t\t\t\"partition\": {},\n", partiton_fact.get_partition().get_id()))?;
        buffer.write_str(&format!("\t\t\t\"readable\": {},\n", if partiton_fact.is_readable() { "true" } else { "false" }))?;
        buffer.write_str(&format!("\t\t\t\"writeable\": {},\n", if partiton_fact.is_writeable() { "true" } else { "false" }))?;

        match *partiton_fact.get_partition_lock() {
            PartitionLockType::LockUntil { ref lock, .. } => {
                buffer.write_str(&format!("\t\t\t\"lock_until\": \"{}\"\n", &::time::at_utc(lock.get_valid_until().clone()).rfc3339().to_string()))?;
            },
            _  => {
                buffer.write_str("\t\t\t\"lock_until\": null\n")?;
            }
        };

        buffer.write_str("\t\t}")?;

        Ok(buffer)

    })
        .collect::<Result<Vec<String>, Box<::std::fmt::Error>>>()?
        .join(",");

    buffer.write_str("\n\t\"locks\":{")?;
    buffer.write_str(&locks)?;
    buffer.write_str("\n\t}")?;

    buffer.write_str("\n}")?;

    Ok(buffer)
}

#[get("/")]
pub fn index(facts: State<SharedFacts>, config: State<SharedConfig>) -> String {
    let f = facts.clone();
    let facts = { f.read().expect("no poison expected").clone() };

    format!("{} => {:#?}", config.get_endpoint(), facts)
}


#[post("/queue/<queue>/<partition_id>", data = "<content>")]
pub fn push_partition(
    queue: String,
    partition_id: u32,
    content: String,
    mut offset_handler: State<OffsetHandler>,
    queue_msg_service_arc: State<Arc<Box<QueueMsgService + Sync + Send>>>
) -> String {


    let queue_msg_service = &*queue_msg_service_arc.clone();

    let partition = Partition::new(Queue::new(queue), partition_id);


    match offset_handler.block_partition_and(&partition, |partition, offset| {
        queue_msg_service.push_queue_msg(&partition, offset as u32, &content)
    }) {
        Err(_) =>  return format!("{{\"success\": false, \"queue\": \"{}\", partition: {}  }}", partition.get_queue_name(), partition.get_id()),
        _ => {},
    };

    format!("{{\"success\": true, \"queue\": \"{}\", partition: {}  }}", partition.get_queue_name(), partition.get_id())
}

#[post("/queue/<queue>", data = "<content>")]
pub fn push_queue(
    queue: String,
    content: String,
    mut offset_handler: State<OffsetHandler>,
    queue_msg_service_arc: State<Arc<Box<QueueMsgService + Sync + Send>>>
) -> String {

    let queue_msg_service = &*queue_msg_service_arc.clone();

    let queue = Queue::new(queue);

    match offset_handler.block_queue_and(&queue, |p, offset| {
        queue_msg_service.push_queue_msg(p, offset as u32, &content)
    }) {
        Err(_) =>  return format!("{{\"success\": false, \"queue\": \"{}\"}}", queue.get_name()),
        _ => {},
    };

    format!("{{\"success\": true, \"queue\": \"{}\"}}", queue.get_name())
}

#[get("/queue/<queue>/<partition>/<offset>")]
pub fn queue_partition_get(queue: String, partition: u32, offset: u32, queue_msg_service: State<Arc<Box<QueueMsgService + Sync + Send>>>) -> String {

    let partiton = Partition::new(Queue::new(queue), partition);

    let msg_service = &*queue_msg_service.clone();
    let msg = msg_service.get_queue_msg(&partiton, offset);

    match msg {
        Ok(Some(ref msg)) => {
            format!("{{\"success\": true, \"content\": \"{}\" }}", msg.content)
        },
        Ok(None) => {
            "{\"success\": false, \"reason\": \"could not find msg\" }".into()
        },
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
        let config = ::r2d2::Config::builder()
            .pool_size(1)
            .connection_customizer(Box::new(ConnectionCustomizerKeyspace::new()))
            .build();

        let transport = TransportTcp::new("127.0.0.1:9042").unwrap();
        let authenticator = PasswordAuthenticator::new("user", "pass");
        let manager = ConnectionManager::new(transport, authenticator, Compression::None);

        ::r2d2::Pool::new(config, manager).expect("could not get pool")
    }

    pub fn run(
        shared_facts: SharedFacts,
        shared_config: SharedConfig,
        shared_offset_handler: OffsetHandler,
        queue_msg_service: Arc<Box<QueueMsgService + Sync + Send>>
    ) {
        thread::spawn(|| {
            rocket::ignite()
                .manage(Self::init_pool())
                .manage(shared_facts)
                .manage(shared_config)
                .manage(shared_offset_handler)
                .manage(queue_msg_service)
                .mount("/", routes![info, index, queue_partition_get, push_queue, push_partition]).launch();
        });
    }
}