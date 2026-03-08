use std::net::{Ipv4Addr, SocketAddrV4};

use serde::{de::DeserializeOwned, Serialize};
use tonic::{transport::Channel, Request};
use uuid::Uuid;

use crate::{
    error::Error,
    skiff::skiff_proto::{
        skiff_client::SkiffClient, DeleteRequest, Empty, GetRequest, InsertRequest,
        ListKeysRequest, ServerRequest, SubscribeRequest,
    },
    Subscriber,
};

/// A client for interacting with a skiff cluster.
///
/// `Client` maintains a single gRPC connection to one cluster node.  All
/// write requests are automatically forwarded to the current leader by the
/// node, so the client does not need to track leadership itself.
///
/// # Example
///
/// ```no_run
/// use skiff_rs::Client;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let mut client = Client::new(vec!["127.0.0.1".parse()?]);
/// client.connect().await?;
///
/// client.insert("key", "value").await?;
/// let val: Option<String> = client.get("key").await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct Client {
    conn: Option<SkiffClient<Channel>>,
    cluster: Vec<Ipv4Addr>,
    port: u16,
}
impl Client {
    /// Create a new client that will try each address in `cluster` in order
    /// when connecting.
    pub fn new(cluster: Vec<Ipv4Addr>) -> Self {
        Self {
            conn: None,
            cluster,
            port: 9400,
        }
    }

    /// Override the port used to connect to cluster nodes. Defaults to `9400`.
    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Establish a gRPC connection to the first reachable node in the cluster.
    ///
    /// This is called automatically by the other methods, but can be called
    /// explicitly to verify connectivity before performing operations.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ClientConnectFailed`] if no node in the cluster list
    /// can be reached.
    pub async fn connect(&mut self) -> Result<(), Error> {
        if self.conn.is_some() {
            return Ok(());
        }

        for peer in self.cluster.iter() {
            match SkiffClient::connect(format!("http://{}", SocketAddrV4::new(*peer, self.port)))
                .await
            {
                Ok(conn) => {
                    self.conn = Some(conn);
                    return Ok(());
                }
                Err(_) => continue,
            }
        }

        Err(Error::ClientConnectFailed)
    }

    /// Retrieve the value stored at `key`, deserializing it as `T`.
    ///
    /// Returns `Ok(None)` if the key does not exist.
    ///
    /// # Errors
    ///
    /// Returns an error if the RPC fails or the stored bytes cannot be
    /// deserialized into `T`.
    pub async fn get<T: DeserializeOwned>(&mut self, key: &str) -> Result<Option<T>, Error> {
        self.connect().await?;
        let response = self
            .conn
            .as_mut()
            .unwrap()
            .get(Request::new(GetRequest {
                key: key.to_string(),
            }))
            .await;

        match response {
            Ok(resp) => match resp.into_inner().value {
                Some(value) => match bincode::deserialize::<T>(value.as_slice()) {
                    Ok(value2) => Ok(Some(value2)),
                    Err(_) => Err(Error::DeserializeFailed),
                },
                None => Ok(None),
            },
            Err(_) => Err(Error::RPCCallFailed),
        }
    }

    /// Insert or overwrite `key` with `value`.
    ///
    /// The call blocks until the entry has been committed to a majority of
    /// cluster nodes.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails, the RPC fails, or the cluster
    /// does not commit the entry within the timeout.
    pub async fn insert<T: Serialize>(&mut self, key: &str, value: T) -> Result<(), Error> {
        self.connect().await?;
        let response = self
            .conn
            .as_mut()
            .unwrap()
            .insert(Request::new(InsertRequest {
                key: key.to_string(),
                value: bincode::serialize(&value)?.to_vec(),
            }))
            .await;

        match response {
            Ok(resp) => match resp.into_inner().success {
                true => Ok(()),
                false => Err(Error::RPCCallFailed),
            },
            Err(_) => Err(Error::RPCCallFailed),
        }
    }

    /// Remove `key` from the store.
    ///
    /// The call blocks until the deletion has been committed to a majority of
    /// cluster nodes.  Returns `Ok(())` even if the key did not exist.
    ///
    /// # Errors
    ///
    /// Returns an error if the RPC fails or the cluster does not commit the
    /// deletion within the timeout.
    pub async fn remove(&mut self, key: &str) -> Result<(), Error> {
        self.connect().await?;
        let response = self
            .conn
            .as_mut()
            .unwrap()
            .delete(Request::new(DeleteRequest {
                key: key.to_string(),
            }))
            .await;

        match response {
            Ok(resp) => match resp.into_inner().success {
                true => Ok(()),
                false => Err(Error::RPCCallFailed),
            },
            Err(_) => Err(Error::RPCCallFailed),
        }
    }

    /// Return all key prefixes (namespace segments) currently in the store.
    ///
    /// Keys are organised hierarchically using `/` as a separator.  This
    /// method returns the distinct prefix segments, e.g. `["users", "posts"]`
    /// for a store containing `"users/alice"` and `"posts/hello"`.
    ///
    /// # Errors
    ///
    /// Returns an error if the RPC fails.
    pub async fn get_prefixes(&mut self) -> Result<Vec<String>, Error> {
        self.connect().await?;
        let response = self
            .conn
            .as_mut()
            .unwrap()
            .get_prefixes(Request::new(Empty {}))
            .await;

        match response {
            Ok(resp) => Ok(resp.into_inner().prefixes),
            Err(_) => Err(Error::RPCCallFailed),
        }
    }

    /// Return all keys whose path starts with `prefix`.
    ///
    /// Pass an empty string or `"/"` to list all keys at the root level.
    ///
    /// # Errors
    ///
    /// Returns an error if the RPC fails.
    pub async fn list_keys(&mut self, prefix: &str) -> Result<Vec<String>, Error> {
        self.connect().await?;
        let response = self
            .conn
            .as_mut()
            .unwrap()
            .list_keys(Request::new(ListKeysRequest {
                prefix: String::from(prefix),
            }))
            .await;

        match response {
            Ok(resp) => Ok(resp.into_inner().keys),
            Err(_) => Err(Error::RPCCallFailed),
        }
    }

    /// Remove a node from the cluster configuration.
    ///
    /// `id` and `address` must match an existing member.  The removal is
    /// propagated through the Raft log so all surviving nodes eventually drop
    /// the node from their cluster view.
    ///
    /// # Errors
    ///
    /// Returns an error if the RPC fails or the node is not found.
    pub async fn remove_node(&mut self, id: Uuid, address: Ipv4Addr) -> Result<(), Error> {
        self.connect().await?;
        let response = self
            .conn
            .as_mut()
            .unwrap()
            .remove_server(Request::new(ServerRequest {
                id: id.to_string(),
                address: address.to_string(),
            }))
            .await;

        match response {
            Ok(resp) => match resp.into_inner().success {
                true => Ok(()),
                false => Err(Error::RPCCallFailed),
            },
            Err(_) => Err(Error::RPCCallFailed),
        }
    }

    /// Subscribe to changes under `prefix`.
    ///
    /// Returns a [`Subscriber`] that yields a `(key, value)` pair each time
    /// an entry whose path starts with `prefix` is inserted.  The stream
    /// remains open until the connection is dropped or the server closes it.
    ///
    /// # Errors
    ///
    /// Returns an error if the RPC fails to establish the stream.
    pub async fn watch(&mut self, prefix: &str) -> Result<Subscriber, Error> {
        self.connect().await?;
        let response = self
            .conn
            .as_mut()
            .unwrap()
            .subscribe(Request::new(SubscribeRequest {
                prefix: String::from(prefix),
            }))
            .await;

        match response {
            Ok(resp) => Ok(Subscriber::new(resp.into_inner())),
            Err(_) => Err(Error::RPCCallFailed),
        }
    }
}
