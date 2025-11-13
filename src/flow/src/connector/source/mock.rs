//! Mock connector that lets tests or scripts push bytes into the pipeline manually.

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::connector::{ConnectorError, ConnectorEvent, ConnectorStream, SourceConnector};

/// Connector exposing a handle for manually injecting payloads.
pub struct MockSourceConnector {
    id: String,
    payload_rx: Option<mpsc::Receiver<Result<ConnectorEvent, ConnectorError>>>,
}

/// Handle used to push payloads into a [`MockSourceConnector`].
pub struct MockSourceHandle {
    sender: mpsc::Sender<Result<ConnectorEvent, ConnectorError>>,
}

impl MockSourceConnector {
    /// Create a new mock connector along with the handle used for sending data.
    pub fn new(id: impl Into<String>) -> (Self, MockSourceHandle) {
        let (sender, receiver) = mpsc::channel(100);
        (
            Self {
                id: id.into(),
                payload_rx: Some(receiver),
            },
            MockSourceHandle { sender },
        )
    }
}

impl SourceConnector for MockSourceConnector {
    fn id(&self) -> &str {
        &self.id
    }

    fn subscribe(&mut self) -> Result<ConnectorStream, ConnectorError> {
        let receiver = self
            .payload_rx
            .take()
            .ok_or_else(|| ConnectorError::AlreadySubscribed(self.id.clone()))?;

        Ok(Box::pin(ReceiverStream::new(receiver)))
    }
}

impl MockSourceHandle {
    /// Send a payload to every subscriber of the mock connector.
    pub async fn send(&self, payload: impl Into<Vec<u8>>) -> Result<(), MockSourceError> {
        self.sender
            .send(Ok(ConnectorEvent::Payload(payload.into())))
            .await
            .map_err(|_| MockSourceError::Closed)
    }

    /// Signal that no further payloads will be sent.
    pub async fn close(&self) -> Result<(), MockSourceError> {
        self.sender
            .send(Ok(ConnectorEvent::EndOfStream))
            .await
            .map_err(|_| MockSourceError::Closed)
    }

    /// Borrow a clone of the underlying sender for advanced scenarios.
    pub fn sender(&self) -> mpsc::Sender<Result<ConnectorEvent, ConnectorError>> {
        self.sender.clone()
    }
}

/// Errors returned by [`MockSourceHandle`].
#[derive(thiserror::Error, Debug)]
pub enum MockSourceError {
    /// All subscribers dropped, no further payloads can be delivered.
    #[error("mock source closed")]
    Closed,
}
