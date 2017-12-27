mod cassandra;

pub struct ConnectionConfigs {
    inner: Vec<ConnectionConfig>
}

pub struct ConnectionConfig {
    dsn: String,
    user: String,
    pass: String
}

impl ConnectionConfig {

    pub fn new<T>(dsn: T, user: T, pass: T) -> Self where T : Into<String> {
        ConnectionConfig {
            dsn: dsn.into(),
            user: user.into(),
            pass: pass.into()
        }
    }

    pub fn get_dsn(&self) -> &str {
        &self.dsn
    }

    pub fn get_user(&self) -> &str {
        &self.user
    }

    pub fn get_pass(&self) -> &str {
        &self.pass
    }

}