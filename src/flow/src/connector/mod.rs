//! Connector abstractions for interacting with external systems.
//!
//! - Source connectors ingest bytes and expose them as async streams.
//! - Sink connectors consume encoded payloads and push them outward.

use futures::stream::Stream;
use std::pin::Pin;

pub mod mqtt_client;
pub mod registry;
pub mod sink;
pub mod source;

/// Convenience alias for boxed connector streams.
pub type ConnectorStream =
    Pin<Box<dyn Stream<Item = Result<ConnectorEvent, ConnectorError>> + Send>>;

/// Events emitted by an upstream connector.
#[derive(Debug)]
pub enum ConnectorEvent {
    /// Binary payload received from the source.
    Payload(Vec<u8>),
    /// The connector has no more data to produce.
    EndOfStream,
}

/// Trait implemented by every data source connector.
pub trait SourceConnector: Send + Sync + 'static {
    /// Identifier for logging/metrics.
    fn id(&self) -> &str;
    /// Subscribe to the underlying source and obtain an async payload stream.
    fn subscribe(&mut self) -> Result<ConnectorStream, ConnectorError>;

    /// Signal that no further data should be produced (default no-op).
    fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

/// Error type shared by connectors.
#[derive(thiserror::Error, Debug, Clone)]
pub enum ConnectorError {
    /// Connector has already been subscribed.
    #[error("connector already subscribed: {0}")]
    AlreadySubscribed(String),
    /// Connector client already exists in the shared manager.
    #[error("connector client already exists: {0}")]
    AlreadyExists(String),
    /// Connector client not found in the shared manager.
    #[error("connector client not found: {0}")]
    NotFound(String),
    /// Connector client cannot be dropped because it is still in use.
    #[error("connector client in use: {0}")]
    ResourceBusy(String),
    /// Connection-level failure.
    #[error("connection error: {0}")]
    Connection(String),
    /// Miscellaneous error.
    #[error("{0}")]
    Other(String),
}

pub use mqtt_client::{
    MqttClientManager, SharedMqttClient, SharedMqttClientConfig, SharedMqttEvent,
};
pub use registry::ConnectorRegistry;
pub use sink::mock::{MockSinkConnector, MockSinkHandle};
pub use sink::mqtt::{MqttSinkConfig, MqttSinkConnector};
pub use sink::{SinkConnector, SinkConnectorError};
pub use source::mock::{MockSourceConnector, MockSourceError, MockSourceHandle};
/// MQTT-specific helpers and connector implementation.
pub use source::mqtt::{MqttSourceConfig, MqttSourceConnector};
