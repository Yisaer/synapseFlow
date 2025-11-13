//! Sink connector abstractions for delivering results to external systems.

use async_trait::async_trait;

/// Trait implemented by all sink connectors.
#[async_trait]
pub trait SinkConnector: Send + Sync + 'static {
    /// Identifier for logging/metrics.
    fn id(&self) -> &str;

    /// Send a single payload downstream.
    async fn send(&mut self, payload: &[u8]) -> Result<(), SinkConnectorError>;

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

pub mod mock;
