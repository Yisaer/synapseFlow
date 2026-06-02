//! Mock sink connector useful for tests and demos.

use super::{DeliveryResult, SinkConnector, SinkConnectorError};
use async_trait::async_trait;
use tokio::sync::mpsc;

/// Connector that pushes every payload into an in-memory channel.
pub struct MockSinkConnector {
    id: String,
    sender: Option<mpsc::Sender<Vec<u8>>>,
    buffer: Option<Vec<u8>>,
}

/// Handle that exposes the receiver side of the mock connector.
pub struct MockSinkHandle {
    receiver: mpsc::Receiver<Vec<u8>>,
}

impl MockSinkConnector {
    /// Create a new mock connector along with its handle.
    pub fn new(id: impl Into<String>) -> (Self, MockSinkHandle) {
        Self::new_with_channel_capacity(id, crate::processor::base::DEFAULT_DATA_CHANNEL_CAPACITY)
    }

    pub fn new_with_channel_capacity(
        id: impl Into<String>,
        channel_capacity: usize,
    ) -> (Self, MockSinkHandle) {
        let (sender, receiver) = mpsc::channel(channel_capacity.max(1));
        (
            Self {
                id: id.into(),
                sender: Some(sender),
                buffer: None,
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

    async fn ready(&mut self) -> Result<(), SinkConnectorError> {
        tracing::info!(connector_id = %self.id, "mock sink ready");
        Ok(())
    }

    async fn start_delivery(&mut self) -> Result<(), SinkConnectorError> {
        self.buffer = Some(Vec::new());
        Ok(())
    }

    async fn write_chunk(&mut self, payload: &[u8]) -> Result<(), SinkConnectorError> {
        let Some(buffer) = self.buffer.as_mut() else {
            return Err(SinkConnectorError::Other(format!(
                "mock sink `{}` received chunk without active delivery",
                self.id
            )));
        };
        buffer.extend_from_slice(payload);
        Ok(())
    }

    async fn finish_delivery(&mut self) -> Result<DeliveryResult, SinkConnectorError> {
        let payload = self.buffer.take().ok_or_else(|| {
            SinkConnectorError::Other(format!(
                "mock sink `{}` finished without active delivery",
                self.id
            ))
        })?;
        let bytes_written = payload.len() as u64;
        if let Some(sender) = self.sender.clone() {
            if sender.send(payload).await.is_err() {
                self.sender = None;
            }
        }

        Ok(DeliveryResult { bytes_written })
    }

    async fn abort_delivery(&mut self) {
        self.buffer = None;
    }

    async fn close(&mut self) -> Result<(), SinkConnectorError> {
        self.sender.take();
        tracing::info!(connector_id = %self.id, "mock sink closed");
        Ok(())
    }
}
