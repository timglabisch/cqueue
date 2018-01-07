use std::fs::File;
use toml;
use std::io::Read;

#[derive(Clone, Debug, Deserialize)]
pub struct ConfigQueue {
    name: String,
    partitions_read: u32,
    partitions_write: u32,
}

impl ConfigQueue {
    pub fn get_name(&self) -> &str {
        &self.name
    }

    pub fn get_partitions_read(&self) -> u32 {
        self.partitions_read
    }

    pub fn get_partitions_write(&self) -> u32 {
        self.partitions_write
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    pub endpoint: Option<String>,
    pub queues: Vec<ConfigQueue>
}

impl Config {

    pub fn new_from_file(path : &str) -> Result<Config, String> {

        let mut file_handle = File::open(path).map_err(|_|format!("could not open file {}", path.to_string()))?;


        let mut buf = String::new();
        file_handle.read_to_string(&mut buf).map_err(|_|format!("could not read from file {}", path.to_string()))?;

        println!("{}", &buf);

        let config: Config = toml::from_str(&buf).map_err(|e| format!("{:#?}", e))?;

        if !config.endpoint.is_some() {
            return Err("argument \"endpoint\" is required. example: endpoint = \"[PUBLIC_IP]:[PORT]\"".to_string());
        }

        Ok(config)
    }

    pub fn get_queues(&self) -> &Vec<ConfigQueue>
    {
        &self.queues
    }

}