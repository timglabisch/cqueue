use std::sync::{RwLock, Arc};
use config::Config;
use std::clone::Clone;

pub type SharedConfig = Arc<RwLock<Config>>;

pub trait ConfigHelper {
    fn get_endpoint(&self) -> String;
}

impl ConfigHelper for SharedConfig {

    fn get_endpoint(&self) -> String {

        let arc = self.clone();
        let lock = &arc.read().expect("could not get endpoint");

        lock.endpoint.clone().expect("endpoint must exists")
    }

}

pub struct ConfigService {
    config: Arc<RwLock<Config>>
}

impl ConfigService {
    pub fn new(config: Config) -> Self {
        ConfigService {
            config: Arc::new(RwLock::new(config))
        }
    }

    pub fn get_shared_config(&self) -> SharedConfig {
        self.config.clone()
    }
}