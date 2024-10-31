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
        // Todo: better error handling
        let message = self.stream.next().await.unwrap().unwrap();
        let value = message.value.unwrap();
        Ok((message.key, bincode::deserialize::<T>(value.as_slice())?))
    }
}
