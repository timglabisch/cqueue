use std::fs::File;
use toml;
use std::io::Read;

#[derive(Debug, Deserialize)]
pub struct ConfigQueue {
    name: String,
    partitions_read: Option<u32>,
    partitions_write: Option<u32>
}

#[derive(Debug, Deserialize)]
pub struct Config {
    queues: Vec<ConfigQueue>
}

impl Config {

    pub fn new_from_file(path : &str) -> Result<Config, String> {

        let mut file_handle = File::open(path).map_err(|_|format!("could not open file {}", path.to_string()))?;


        let mut buf = String::new();
        file_handle.read_to_string(&mut buf).map_err(|_|format!("could not read from file {}", path.to_string()))?;


        toml::from_str(&buf).map_err(|e|e.to_string())?
    }

}