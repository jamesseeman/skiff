use thiserror::Error;

#[derive(Debug, Error)]
#[error("error")]
pub enum SkiffError {
    #[error("Failed to join cluster")]
    ClusterJoinFailed,
    #[error("Failed to initialize cluster")]
    ClusterInitFailed,
    #[error("Invalid node address")]
    InvalidAddress,
    #[error("Failed to start RPC server")]
    RPCBindFailed,
    #[error("Failed to create data directory")]
    DataDirectoryCreateFailed,
    #[error("Failed to open sled database")]
    SledError,
    #[error("Client failed to connect")]
    ClientConnectFailed,
    #[error("RPC client call failed")]
    RPCCallFailed,
    #[error("Deserialize failed")]
    DeserializeFailed,
    #[error("Serialize failed")]
    SerializeFailed,
    #[error("Insert failed")]
    InsertFailed,
    #[error("Missing cluster configuration")]
    MissingClusterConfig,
}

impl From<tonic::transport::Error> for SkiffError {
    fn from(err: tonic::transport::Error) -> Self {
        Self::RPCBindFailed
    }
}

impl From<std::io::Error> for SkiffError {
    fn from(err: std::io::Error) -> Self {
        Self::DataDirectoryCreateFailed
    }
}

impl From<sled::Error> for SkiffError {
    fn from(err: sled::Error) -> Self {
        Self::SledError
    }
}

impl From<bincode::Error> for SkiffError {
    fn from(err: bincode::Error) -> Self {
        Self::SerializeFailed
    }
}