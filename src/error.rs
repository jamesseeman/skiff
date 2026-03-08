use thiserror::Error;

/// All errors that can be returned by skiff.
#[derive(Debug, Error)]
#[error("error")]
pub enum Error {
    /// A node failed to register itself with any peer in the cluster.
    #[error("Failed to join cluster")]
    ClusterJoinFailed,

    /// The cluster could not be initialized (e.g. sled could not be opened).
    #[error("Failed to initialize cluster")]
    ClusterInitFailed,

    /// An address string could not be parsed as a valid IPv4 address.
    #[error("Invalid node address")]
    InvalidAddress,

    /// The gRPC server could not bind to the requested address and port.
    #[error("Failed to start RPC server")]
    RPCBindFailed(tonic::transport::Error),

    /// The data directory could not be created.
    #[error("Failed to create data directory")]
    DataDirectoryCreateFailed(std::io::Error),

    /// A sled database operation failed.
    #[error("Failed to open sled database")]
    SledError(sled::Error),

    /// The client could not connect to any node in the cluster address list.
    #[error("Client failed to connect")]
    ClientConnectFailed,

    /// A gRPC call to a cluster node returned an error status.
    #[error("RPC client call failed")]
    RPCCallFailed,

    /// Stored bytes could not be deserialized into the expected type.
    #[error("Deserialize failed")]
    DeserializeFailed,

    /// A value could not be serialized with bincode.
    #[error("Serialize failed")]
    SerializeFailed(bincode::Error),

    /// The cluster did not acknowledge an insert within the timeout.
    #[error("Insert failed")]
    InsertFailed,

    /// No `Configure` entry was found in the Raft log.
    #[error("Missing cluster configuration")]
    MissingClusterConfig,

    /// The requested peer UUID is not in the current cluster configuration.
    #[error("Peer not found")]
    PeerNotFound,

    /// A gRPC connection to a peer could not be established.
    #[error("Failed to connect to peer")]
    PeerConnectFailed,

    /// The subscription stream was closed by the server.
    #[error("Subscriber stream closed")]
    StreamClosed,
}

impl From<tonic::transport::Error> for Error {
    fn from(err: tonic::transport::Error) -> Self {
        Self::RPCBindFailed(err)
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self::DataDirectoryCreateFailed(err)
    }
}

impl From<sled::Error> for Error {
    fn from(err: sled::Error) -> Self {
        Self::SledError(err)
    }
}

impl From<bincode::Error> for Error {
    fn from(err: bincode::Error) -> Self {
        Self::SerializeFailed(err)
    }
}
