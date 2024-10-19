use std::net::{Ipv4Addr, SocketAddrV4};

use serde::{de::DeserializeOwned, Serialize};
use tonic::{transport::Channel, Request};

use crate::{error::SkiffError, skiff::skiff_proto::{skiff_client::SkiffClient, GetRequest, InsertRequest}};

pub struct Client {
    conn: Option<SkiffClient<Channel>>,
    cluster: Vec<Ipv4Addr>,
}
 impl Client {
    pub fn new(cluster: Vec<Ipv4Addr>) -> Self {
        Self {
            conn: None,
            cluster,
        }
    }

    async fn connect(&mut self) -> Result<(), SkiffError> {
        if self.conn.is_some() { return Ok(()) }

        for peer in self.cluster.iter() {
            match SkiffClient::connect(format!("http://{}", SocketAddrV4::new(*peer, 9400))).await {
                Ok(conn) => {
                    self.conn = Some(conn);
                    return Ok(())
                },
                 Err(_) => continue
            }
        }

        Err(SkiffError::ClientConnectFailed)
    }

    pub async fn get<T: DeserializeOwned>(&mut self, key: &str, tree: Option<&str>) -> Result<Option<T>, SkiffError> {
        self.connect().await?;        
        let response = self.conn.as_mut().unwrap().get(Request::new(GetRequest {
            tree: tree.map(|s| s.to_string()),
            key: key.to_string(),
        })).await;

        match response {
            Ok(resp) => {
                match resp.into_inner().value {
                    Some(value) => {
                        match bincode::deserialize::<T>(value.as_slice()) {
                            Ok(value2) => Ok(Some(value2)),
                            Err(_) => Err(SkiffError::DeserializeFailed),
                        }
                    },
                    None => Ok(None),
                }
            },
            Err(_) => Err(SkiffError::RPCCallFailed),
        }
    }

    pub async fn insert<T: Serialize>(&mut self, key: &str, value: T, tree: Option<&str>) -> Result<(), SkiffError> {
        self.connect().await?;
        let response = self.conn.as_mut().unwrap().insert(Request::new(InsertRequest {
            tree: tree.map(|s| s.to_string()),
            key: key.to_string(),
            value: bincode::serialize(&value)?.to_vec(),
        })).await;

        match response {
            Ok(resp) => match resp.into_inner().success {
                true => Ok(()),
                false => Err(SkiffError::RPCCallFailed),
            },
            Err(_) => Err(SkiffError::RPCCallFailed),
        }
    }
}