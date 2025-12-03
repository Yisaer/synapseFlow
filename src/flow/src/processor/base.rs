//! Processor trait and implementations for stream processing
//!
//! This module defines the core Processor trait and concrete implementations:
//! - ControlSourceProcessor: Starting point for data flow, handles control signals
//! - DataSourceProcessor: Processes data from PhysicalDatasource
//! - ResultCollectProcessor: Final destination, prints received data

use crate::processor::{ControlSignal, StreamData, StreamError};
use futures::stream::SelectAll;
use tokio::sync::broadcast;
use tokio::time::{sleep, Duration};
use tokio_stream::wrappers::BroadcastStream;

/// Log received StreamData for debugging
pub fn log_received_data(processor_id: &str, data: &StreamData) {
    match data {
        StreamData::Collection(collection) => {
            println!("[Processor:{}] received Collection: {} rows", processor_id, collection.num_rows());
            // Print first few rows for debugging
            let rows = collection.rows();
            for (i, row) in rows.iter().take(3).enumerate() {
                println!("  Row {}: {:?}", i, row);
            }
            if rows.len() > 3 {
                println!("  ... and {} more rows", rows.len() - 3);
            }
        }
        StreamData::Encoded { payload, collection } => {
            println!("[Processor:{}] received Encoded: {} rows, {} bytes", processor_id, collection.num_rows(), payload.len());
        }
        StreamData::Bytes(payload) => {
            println!("[Processor:{}] received Bytes: {} bytes", processor_id, payload.len());
        }
        StreamData::Control(signal) => {
            println!("[Processor:{}] received Control: {:?}", processor_id, signal);
        }
        StreamData::Error(error) => {
            println!("[Processor:{}] received Error: {}", processor_id, error.message);
        }
    }
}

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

    /// Subscribe to the processor's control signal output (high priority path)
    fn subscribe_control_output(&self) -> Option<broadcast::Receiver<ControlSignal>>;

    /// Add an input channel (connect upstream processor)
    fn add_input(&mut self, receiver: broadcast::Receiver<StreamData>);

    /// Add a control-signal input channel (connect upstream control path)
    fn add_control_input(&mut self, receiver: broadcast::Receiver<ControlSignal>);
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
pub(crate) type ControlInputStream = SelectAll<BroadcastStream<ControlSignal>>;

/// Convert a list of broadcast receivers into a single SelectAll stream
pub(crate) fn fan_in_streams(inputs: Vec<broadcast::Receiver<StreamData>>) -> ProcessorInputStream {
    let mut streams = SelectAll::new();
    for receiver in inputs {
        streams.push(BroadcastStream::new(receiver));
    }
    streams
}

pub(crate) fn fan_in_control_streams(
    inputs: Vec<broadcast::Receiver<ControlSignal>>,
) -> ControlInputStream {
    let mut streams = SelectAll::new();
    for receiver in inputs {
        streams.push(BroadcastStream::new(receiver));
    }
    streams
}

/// Send data to a broadcast channel while applying cooperative backpressure.
///
/// `tokio::broadcast` drops the oldest messages when the channel is full.
/// To avoid that behaviour we proactively wait until space becomes
/// available (or until there are no receivers left) before sending.
pub(crate) async fn send_with_backpressure(
    sender: &broadcast::Sender<StreamData>,
    data: StreamData,
) -> Result<(), ProcessorError> {
    const BACKOFF: Duration = Duration::from_millis(1);
    let mut payload = Some(data);
    loop {
        if sender.receiver_count() == 0 || sender.len() < DEFAULT_CHANNEL_CAPACITY {
            let value = payload
                .take()
                .expect("send_with_backpressure payload already taken");
            sender
                .send(value)
                .map(|_| ())
                .map_err(|_| ProcessorError::ChannelClosed)?;
            return Ok(());
        }
        sleep(BACKOFF).await;
    }
}

pub(crate) async fn send_control_with_backpressure(
    sender: &broadcast::Sender<ControlSignal>,
    signal: ControlSignal,
) -> Result<(), ProcessorError> {
    const BACKOFF: Duration = Duration::from_millis(1);
    let mut payload = Some(signal);
    loop {
        if sender.receiver_count() == 0 || sender.len() < DEFAULT_CHANNEL_CAPACITY {
            let value = payload
                .take()
                .expect("send_control_with_backpressure payload already taken");
            sender
                .send(value)
                .map(|_| ())
                .map_err(|_| ProcessorError::ChannelClosed)?;
            return Ok(());
        }
        sleep(BACKOFF).await;
    }
}

/// Convenience helper to emit a [`StreamData::Error`] for the given processor.
pub(crate) async fn forward_error(
    sender: &broadcast::Sender<StreamData>,
    processor_id: &str,
    message: impl Into<String>,
) -> Result<(), ProcessorError> {
    let error = StreamError::new(message).with_source(processor_id.to_string());
    send_with_backpressure(sender, StreamData::error(error)).await
}
