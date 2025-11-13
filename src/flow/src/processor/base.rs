//! Processor trait and implementations for stream processing
//!
//! This module defines the core Processor trait and concrete implementations:
//! - ControlSourceProcessor: Starting point for data flow, handles control signals
//! - DataSourceProcessor: Processes data from PhysicalDatasource
//! - ResultSinkProcessor: Final destination, prints received data

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use futures::stream::SelectAll;
use crate::processor::StreamData;

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
    fn output_senders(&self) -> Vec<mpsc::Sender<StreamData>>;
    
    /// Add an input channel (connect upstream processor)
    fn add_input(&mut self, receiver: mpsc::Receiver<StreamData>);
    
    /// Add an output channel (connect downstream processor)
    fn add_output(&mut self, sender: mpsc::Sender<StreamData>);
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
            ProcessorError::InvalidConfiguration(msg) => write!(f, "Invalid configuration: {}", msg),
            ProcessorError::Timeout => write!(f, "Timeout waiting for data"),
        }
    }
}

impl std::error::Error for ProcessorError {}

/// Combined input stream built from multiple mpsc receivers
pub(crate) type ProcessorInputStream = SelectAll<ReceiverStream<StreamData>>;

/// Convert a list of mpsc receivers into a single SelectAll stream
pub(crate) fn fan_in_streams(
    inputs: Vec<mpsc::Receiver<StreamData>>,
) -> ProcessorInputStream {
    let mut streams = SelectAll::new();
    for receiver in inputs {
        streams.push(ReceiverStream::new(receiver));
    }
    streams
}

/// Broadcast a StreamData payload to every downstream sender
pub(crate) async fn broadcast_all(
    outputs: &[mpsc::Sender<StreamData>],
    data: StreamData,
) -> Result<(), ProcessorError> {
    if outputs.is_empty() {
        return Ok(());
    }

    if let Some((last, rest)) = outputs.split_last() {
        for sender in rest {
            sender
                .send(data.clone())
                .await
                .map_err(|_| ProcessorError::ChannelClosed)?;
        }
        last.send(data).await.map_err(|_| ProcessorError::ChannelClosed)?;
    }

    Ok(())
}
