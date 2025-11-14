//! Processor trait and implementations for stream processing
//!
//! This module defines the core Processor trait and concrete implementations:
//! - ControlSourceProcessor: Starting point for data flow, handles control signals
//! - DataSourceProcessor: Processes data from PhysicalDatasource
//! - ResultCollectProcessor: Final destination, prints received data

use crate::processor::StreamData;
use futures::stream::SelectAll;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;

/// Default buffer size for processor broadcast channels
pub(crate) const DEFAULT_CHANNEL_CAPACITY: usize = 1024;

/// Trait for all stream processors
///
/// Processors are the building blocks of the stream processing pipeline.
/// Each processor can have multiple inputs and multiple outputs, communicating
/// via tokio mpsc channels with StreamData.
pub trait Processor: Send + Sync {
    /// Get the processor identifier
    fn id(&self) -> &str;

    /// Start the processor asynchronously
    /// Returns a handle that can be used to await completion
    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>>;

    /// Get output channel senders (for connecting downstream processors)
    fn subscribe_output(&self) -> Option<broadcast::Receiver<StreamData>>;

    /// Add an input channel (connect upstream processor)
    fn add_input(&mut self, receiver: broadcast::Receiver<StreamData>);
}

/// Error type for processor operations
#[derive(Debug, Clone, PartialEq)]
pub enum ProcessorError {
    /// Channel closed unexpectedly
    ChannelClosed,
    /// Processing error with message
    ProcessingError(String),
    /// Invalid configuration
    InvalidConfiguration(String),
    /// Timeout waiting for data
    Timeout,
}

impl std::fmt::Display for ProcessorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProcessorError::ChannelClosed => write!(f, "Channel closed unexpectedly"),
            ProcessorError::ProcessingError(msg) => write!(f, "Processing error: {}", msg),
            ProcessorError::InvalidConfiguration(msg) => {
                write!(f, "Invalid configuration: {}", msg)
            }
            ProcessorError::Timeout => write!(f, "Timeout waiting for data"),
        }
    }
}

impl std::error::Error for ProcessorError {}

/// Combined input stream built from multiple broadcast receivers
pub(crate) type ProcessorInputStream = SelectAll<BroadcastStream<StreamData>>;

/// Convert a list of broadcast receivers into a single SelectAll stream
pub(crate) fn fan_in_streams(inputs: Vec<broadcast::Receiver<StreamData>>) -> ProcessorInputStream {
    let mut streams = SelectAll::new();
    for receiver in inputs {
        streams.push(BroadcastStream::new(receiver));
    }
    streams
}
