use crate::{error::Error, Skiff};
use std::net::Ipv4Addr;

/// Builder for constructing a [`Skiff`] node.
///
/// Call [`Builder::new`] to create a builder with sensible defaults, configure
/// it with the chainable setter methods, then call [`Builder::build`] to
/// obtain a [`Skiff`] instance ready to be started with [`Skiff::start`].
///
/// # Defaults
///
/// | Setting      | Default value       |
/// |--------------|---------------------|
/// | Bind address | `127.0.0.1`         |
/// | Port         | `9400`              |
/// | Data dir     | `/tmp/skiff`        |
/// | Peers        | *(none — solo node)*|
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
    /// Create a new builder with default settings.
    pub fn new() -> Self {
        Self {
            bind_address: Ipv4Addr::new(127, 0, 0, 1),
            port: 9400,
            data_dir: "/tmp/skiff".to_string(),
            peers: vec![],
        }
    }

    /// Set the directory where sled will store persistent data.
    ///
    /// The directory is created if it does not already exist.  Reusing the
    /// same directory across restarts lets the node recover its identity and
    /// Raft log from disk.
    pub fn set_dir(mut self, dir: &str) -> Self {
        self.data_dir = dir.to_string();
        self
    }

    /// Set the IPv4 address this node will bind its gRPC listener to.
    pub fn bind(mut self, address: Ipv4Addr) -> Self {
        self.bind_address = address;
        self
    }

    /// Override the TCP port used for inter-node and client gRPC traffic.
    ///
    /// Defaults to `9400`.  All nodes in a cluster must use the same port.
    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Provide the addresses of existing cluster members.
    ///
    /// When peers are supplied the node will contact one of them on startup
    /// and register itself via the `add_server` RPC.  Omit this (or pass an
    /// empty vec) when bootstrapping the very first node of a new cluster.
    pub fn join_cluster(mut self, peers: Vec<Ipv4Addr>) -> Self {
        self.peers = peers;
        self
    }

    /// Build the [`Skiff`] node.
    ///
    /// Opens (or creates) the sled database, loads any previously persisted
    /// Raft state, and returns a node ready to be started.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if the data directory cannot be created or the
    /// sled database cannot be opened.
    pub fn build(self) -> Result<Skiff, Error> {
        Skiff::new(self.bind_address, self.port, self.data_dir, self.peers)
    }
}
