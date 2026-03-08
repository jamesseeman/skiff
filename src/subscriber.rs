use serde::de::DeserializeOwned;
use tokio_stream::StreamExt;
use tonic::Streaming;

use crate::{skiff::skiff_proto::SubscribeReply, Error};

/// A streaming receiver for key-value change notifications.
///
/// Obtained by calling [`Client::watch`](crate::Client::watch).  Each call to
/// [`recv`](Subscriber::recv) blocks until the next insert is committed for
/// the watched prefix, then returns the key and deserialized value.
///
/// # Example
///
/// ```no_run
/// use skiff_rs::Client;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let mut client = Client::new(vec!["127.0.0.1".parse()?]);
/// let mut sub = client.watch("users/").await?;
///
/// loop {
///     let (key, value): (String, String) = sub.recv().await?;
///     println!("change: {} = {}", key, value);
/// }
/// # }
/// ```
pub struct Subscriber {
    stream: Streaming<SubscribeReply>,
}

impl Subscriber {
    pub(crate) fn new(stream: Streaming<SubscribeReply>) -> Self {
        Self { stream }
    }

    /// Wait for the next change notification and deserialize its value as `T`.
    ///
    /// Returns a `(key, value)` tuple where `key` is the full key path that
    /// was inserted.
    ///
    /// # Errors
    ///
    /// Returns [`Error::StreamClosed`] if the server closes the stream, or
    /// [`Error::DeserializeFailed`] if the value cannot be decoded as `T`.
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
