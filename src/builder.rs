use crate::{error::Error, Skiff};
use std::net::Ipv4Addr;

#[derive(Debug, Clone)]
pub struct Builder {
    bind_address: Ipv4Addr,
    port: u16,
    data_dir: String,

    // If empty, we are the sole node in a new cluster.
    // Otherwise, we are joining an existing cluster.
    peers: Vec<Ipv4Addr>,
}

impl Default for Builder {
    fn default() -> Self {
        Self::new()
    }
}

impl Builder {
    pub fn new() -> Self {
        Self {
            bind_address: Ipv4Addr::new(127, 0, 0, 1),
            port: 9400,
            data_dir: "/tmp/skiff".to_string(),
            peers: vec![],
        }
    }

    pub fn set_dir(mut self, dir: &str) -> Self {
        self.data_dir = dir.to_string();
        self
    }

    pub fn bind(mut self, address: Ipv4Addr) -> Self {
        self.bind_address = address;
        self
    }

    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    pub fn join_cluster(mut self, peers: Vec<Ipv4Addr>) -> Self {
        self.peers = peers;
        self
    }

    pub fn build(self) -> Result<Skiff, Error> {
        Skiff::new(self.bind_address, self.port, self.data_dir, self.peers)
    }
}
