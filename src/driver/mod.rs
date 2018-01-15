use std::error::Error;
use std::ops::DerefMut;
use cdrs::authenticators::Authenticator;
use cdrs::transport::CDRSTransport;
use cdrs::connection_manager::ConnectionManager;
use cdrs::query::Query;

pub mod offset_handler;

pub type CPool = ::r2d2::Pool<::cdrs::connection_manager::ConnectionManager<::cdrs::authenticators::PasswordAuthenticator<'static>, ::cdrs::transport::TransportTcp>>;

pub trait Connection {

    fn query(&mut self, query: Query) -> ::cdrs::error::Result<::cdrs::frame::Frame>;

}

impl<T, X> Connection for ::cdrs::client::Session<T, X> where
T: Authenticator + Send + Sync + 'static,
X: CDRSTransport + Send + Sync + 'static {

    fn query(&mut self, query: Query) -> ::cdrs::error::Result<::cdrs::frame::Frame> {
        ::cdrs::client::Session::query(self, query, false, false)
    }
}


pub trait PooledConnection {

    fn getConnection(&mut self) -> &mut Connection;
}

impl<T, X> PooledConnection for ::r2d2::PooledConnection<ConnectionManager<T, X>> where
    T: Authenticator + Send + Sync + 'static,
    X: CDRSTransport + Send + Sync + 'static
{
    fn getConnection(&mut self) -> &mut Connection {
       self.deref_mut()
    }
}

pub trait Pool {
    fn get(&self) -> Result<Box<PooledConnection>, Box<Error>>;
}

impl<T, X> Pool for ::r2d2::Pool<ConnectionManager<T, X>> where
    T: Authenticator + Send + Sync + 'static,
    X: CDRSTransport + Send + Sync + 'static {

    fn get(&self) -> Result<Box<PooledConnection>, Box<Error>> {

        match ::r2d2::Pool::get(self) {
            Ok(c) => Ok(Box::new(c)),
            Err(e) => Err(Box::new(e))
        }
    }

}

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