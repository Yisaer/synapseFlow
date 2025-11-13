//! Connector abstractions for interacting with external systems.
//!
//! - Source connectors ingest bytes and expose them as async streams.
//! - Sink connectors consume encoded payloads and push them outward.

use futures::stream::Stream;
use std::pin::Pin;

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
}

/// Error type shared by connectors.
#[derive(thiserror::Error, Debug)]
pub enum ConnectorError {
    /// Connector has already been subscribed.
    #[error("connector already subscribed: {0}")]
    AlreadySubscribed(String),
    /// Connection-level failure.
    #[error("connection error: {0}")]
    Connection(String),
    /// Miscellaneous error.
    #[error("{0}")]
    Other(String),
}

pub use sink::mock::{MockSinkConnector, MockSinkHandle};
pub use sink::{SinkConnector, SinkConnectorError};
pub use source::mock::{MockSourceConnector, MockSourceError, MockSourceHandle};
/// MQTT-specific helpers and connector implementation.
pub use source::mqtt::{MqttSourceConfig, MqttSourceConnector};
