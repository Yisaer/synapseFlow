//! Sink connector abstractions for delivering results to external systems.

use async_trait::async_trait;

use crate::model::Collection;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DeliveryResult {
    pub bytes_written: u64,
}

/// Trait implemented by all sink connectors.
#[async_trait]
pub trait SinkConnector: Send + Sync + 'static {
    /// Identifier for logging/metrics.
    fn id(&self) -> &str;

    /// Start a new encoded delivery.
    async fn start_delivery(&mut self) -> Result<(), SinkConnectorError> {
        Err(SinkConnectorError::Other(format!(
            "connector `{}` does not support encoded delivery",
            self.id()
        )))
    }

    /// Write a chunk into the active encoded delivery.
    async fn write_chunk(&mut self, _bytes: &[u8]) -> Result<(), SinkConnectorError> {
        Err(SinkConnectorError::Other(format!(
            "connector `{}` does not support encoded delivery",
            self.id()
        )))
    }

    /// Finish the active encoded delivery.
    async fn finish_delivery(&mut self) -> Result<DeliveryResult, SinkConnectorError> {
        Err(SinkConnectorError::Other(format!(
            "connector `{}` does not support encoded delivery",
            self.id()
        )))
    }

    /// Abort the active encoded delivery best-effort.
    async fn abort_delivery(&mut self) {}

    /// Send a `Collection` downstream without going through an encoder.
    ///
    /// By default connectors reject collection payloads. Connectors that operate on decoded
    /// data (e.g. Kuksa sink) should override this method.
    async fn send_collection(
        &mut self,
        _collection: &dyn Collection,
    ) -> Result<(), SinkConnectorError> {
        Err(SinkConnectorError::Other(format!(
            "connector `{}` does not support collection payloads",
            self.id()
        )))
    }

    /// Prepare the connector for sending (e.g. establish network connections).
    async fn ready(&mut self) -> Result<(), SinkConnectorError> {
        Ok(())
    }

    /// Signal that no more payloads will be sent.
    async fn close(&mut self) -> Result<(), SinkConnectorError> {
        Ok(())
    }
}

/// Errors shared by sink connectors.
#[derive(thiserror::Error, Debug)]
pub enum SinkConnectorError {
    /// Connector is not available anymore (e.g. channel closed).
    #[error("connector unavailable: {0}")]
    Unavailable(String),
    /// Any custom error surfaced by the connector.
    #[error("{0}")]
    Other(String),
}

pub mod file;
pub mod kuksa;
pub mod kura;
pub mod memory;
pub mod mock;
pub mod mqtt;
#[cfg(feature = "nng_pubsub")]
pub mod nng_pubsub;
pub mod nop;
pub mod video;
