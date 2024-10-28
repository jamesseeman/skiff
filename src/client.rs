use std::net::{Ipv4Addr, SocketAddrV4};

use serde::{de::DeserializeOwned, Serialize};
use tonic::{transport::Channel, Request};

use crate::{
    error::Error,
    skiff::skiff_proto::{skiff_client::SkiffClient, DeleteRequest, GetRequest, InsertRequest},
};

#[derive(Debug)]
pub struct Client {
    conn: Option<SkiffClient<Channel>>,
    cluster: Vec<Ipv4Addr>,
}
impl Client {
    // todo: get cluster config from server
    pub fn new(cluster: Vec<Ipv4Addr>) -> Self {
        Self {
            conn: None,
            cluster,
        }
    }

    pub async fn connect(&mut self) -> Result<(), Error> {
        if self.conn.is_some() {
            return Ok(());
        }

        for peer in self.cluster.iter() {
            match SkiffClient::connect(format!("http://{}", SocketAddrV4::new(*peer, 9400))).await {
                Ok(conn) => {
                    self.conn = Some(conn);
                    return Ok(());
                }
                Err(_) => continue,
            }
        }

        Err(Error::ClientConnectFailed)
    }

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
}
