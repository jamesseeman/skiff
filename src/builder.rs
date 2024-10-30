use crate::{error::Error, Skiff};
use std::net::Ipv4Addr;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct Builder {
    bind_address: Ipv4Addr,
    data_dir: String,

    // If empty, we are the leader of a new cluster
    // Otherwise, we are a follower in an existing cluster
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

    pub fn join_cluster(mut self, peers: Vec<Ipv4Addr>) -> Self {
        self.peers = peers;
        self
    }

    // todo: load id, snapshots, etc from data_dir if present
    pub fn build(self) -> Result<Skiff, Error> {
        Skiff::new(Uuid::new_v4(), self.bind_address, self.data_dir, self.peers)
    }
}
