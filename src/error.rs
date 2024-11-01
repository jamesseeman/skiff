use thiserror::Error;

#[derive(Debug, Error)]
#[error("error")]
pub enum Error {
    #[error("Failed to join cluster")]
    ClusterJoinFailed,
    #[error("Failed to initialize cluster")]
    ClusterInitFailed,
    #[error("Invalid node address")]
    InvalidAddress,
    #[error("Failed to start RPC server")]
    RPCBindFailed(tonic::transport::Error),
    #[error("Failed to create data directory")]
    DataDirectoryCreateFailed(std::io::Error),
    #[error("Failed to open sled database")]
    SledError(sled::Error),
    #[error("Client failed to connect")]
    ClientConnectFailed,
    #[error("RPC client call failed")]
    RPCCallFailed,
    #[error("Deserialize failed")]
    DeserializeFailed,
    #[error("Serialize failed")]
    SerializeFailed(bincode::Error),
    #[error("Insert failed")]
    InsertFailed,
    #[error("Missing cluster configuration")]
    MissingClusterConfig,
    #[error("Peer not found")]
    PeerNotFound,
    #[error("Failed to connect to peer")]
    PeerConnectFailed,
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
