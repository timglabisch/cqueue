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
}