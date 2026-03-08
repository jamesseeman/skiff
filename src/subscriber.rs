use serde::de::DeserializeOwned;
use tokio_stream::StreamExt;
use tonic::Streaming;

use crate::{skiff::skiff_proto::SubscribeReply, Error};

pub struct Subscriber {
    stream: Streaming<SubscribeReply>,
}

impl Subscriber {
    pub fn new(stream: Streaming<SubscribeReply>) -> Self {
        Self { stream }
    }

    pub async fn recv<T: DeserializeOwned>(&mut self) -> Result<(String, T), Error> {
        let message = self
            .stream
            .next()
            .await
            .ok_or(Error::StreamClosed)?
            .map_err(|_| Error::RPCCallFailed)?;
        let value = message.value.ok_or(Error::DeserializeFailed)?;
        Ok((message.key, bincode::deserialize::<T>(value.as_slice())?))
    }
}
