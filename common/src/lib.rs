//! Everything more than one DeLawson process needs: configuration, the wire
//! protocol, the daemon lifecycle, and the placement-rule report.

pub mod block;
pub mod config;
pub mod crc;
pub mod framing;
pub mod log;
pub mod logread;
pub mod offset;
pub mod peer;
pub mod placement;
pub mod random;
pub mod registry;
pub mod seal;
pub mod server;
#[cfg(any(test, feature = "testing"))]
pub mod testing;
pub mod wire;

#[derive(Debug)]
pub struct Error(String);

impl Error {
    pub fn new(message: impl Into<String>) -> Self {
        Error(message.into())
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;

pub trait Context<T> {
    fn context(self, what: impl std::fmt::Display) -> Result<T>;
}

impl<T, E: std::fmt::Display> Context<T> for std::result::Result<T, E> {
    fn context(self, what: impl std::fmt::Display) -> Result<T> {
        self.map_err(|e| Error(format!("{what}: {e}")))
    }
}
