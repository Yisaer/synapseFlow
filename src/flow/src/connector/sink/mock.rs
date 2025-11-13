//! Mock sink connector useful for tests and demos.

use super::{SinkConnector, SinkConnectorError};
use async_trait::async_trait;
use tokio::sync::mpsc;

/// Connector that pushes every payload into an in-memory channel.
pub struct MockSinkConnector {
    id: String,
    sender: Option<mpsc::Sender<Vec<u8>>>,
}

/// Handle that exposes the receiver side of the mock connector.
pub struct MockSinkHandle {
    receiver: mpsc::Receiver<Vec<u8>>,
}

impl MockSinkConnector {
    /// Create a new mock connector along with its handle.
    pub fn new(id: impl Into<String>) -> (Self, MockSinkHandle) {
        let (sender, receiver) = mpsc::channel(100);
        (
            Self {
                id: id.into(),
                sender: Some(sender),
            },
            MockSinkHandle { receiver },
        )
    }
}

impl MockSinkHandle {
    /// Receive the next payload, awaiting until one is available or the sender closes.
    pub async fn recv(&mut self) -> Option<Vec<u8>> {
        self.receiver.recv().await
    }

    /// Non-blocking check for the next payload.
    pub fn try_recv(&mut self) -> Result<Vec<u8>, tokio::sync::mpsc::error::TryRecvError> {
        self.receiver.try_recv()
    }

    /// Consume the handle and return the underlying receiver for advanced use cases.
    pub fn into_inner(self) -> mpsc::Receiver<Vec<u8>> {
        self.receiver
    }
}

#[async_trait]
impl SinkConnector for MockSinkConnector {
    fn id(&self) -> &str {
        &self.id
    }

    async fn send(&mut self, payload: &[u8]) -> Result<(), SinkConnectorError> {
        match self.sender.as_ref() {
            Some(sender) => sender
                .send(payload.to_vec())
                .await
                .map_err(|_| SinkConnectorError::Unavailable(self.id.clone())),
            None => Err(SinkConnectorError::Unavailable(self.id.clone())),
        }
    }

    async fn close(&mut self) -> Result<(), SinkConnectorError> {
        self.sender.take();
        Ok(())
    }
}
