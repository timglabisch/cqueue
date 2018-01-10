#[derive(Debug, Clone)]
pub struct Queue {
    name: String,
}

impl Queue {
    pub fn new<T>(name: T) -> Queue where T: Into<String> {
        Queue {
            name: name.into()
        }
    }

    pub fn get_name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug, Clone)]
pub struct Partition {
    queue: Queue,
    id: u32
}

impl Partition {
    pub fn new(queue: Queue, id: u32) -> Partition {
        Partition {
            queue,
            id
        }
    }

    pub fn get_id(&self) -> u32 {
        self.id
    }

    pub fn get_queue_name(&self) -> &str {
        &self.queue.name
    }

    pub fn get_queue(&self) -> &Queue {
        &self.queue
    }

    pub fn to_string(&self) -> String {
        format!("{}_{}", self.get_queue_name(), self.get_id())
    }
}