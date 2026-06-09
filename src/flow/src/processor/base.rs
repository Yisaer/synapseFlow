//! Processor trait and implementations for stream processing
//!
//! This module defines the core Processor trait and concrete implementations:
//! - ControlSourceProcessor: Starting point for data flow, handles control signals
//! - DataSourceProcessor: Processes data from PhysicalDatasource
//! - ResultCollectProcessor: Final destination, prints received data

use crate::processor::{ControlSignal, ProcessorStats, StreamData};
use crate::runtime::TaskSpawner;
use futures::stream::SelectAll;
use tokio::sync::{broadcast, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};
use tokio_stream::wrappers::BroadcastStream;

/// Log received StreamData for debugging
pub fn log_received_data(processor_id: &str, data: &StreamData) {
    if !tracing::enabled!(tracing::Level::DEBUG) {
        return;
    }
    match data {
        StreamData::Collection(collection) => {
            tracing::debug!(
                processor_id = %processor_id,
                rows = collection.num_rows(),
                "received collection"
            );
            // Print first few rows for debugging
            let rows = collection.rows();
            for (i, row) in rows.iter().take(3).enumerate() {
                tracing::debug!(processor_id = %processor_id, row_idx = i, row = ?row, "row");
            }
            if rows.len() > 3 {
                tracing::debug!(
                    processor_id = %processor_id,
                    remaining = rows.len() - 3,
                    "more rows"
                );
            }
        }
        StreamData::EncodedDelivery { flags, bytes } => {
            tracing::debug!(
                processor_id = %processor_id,
                flags = ?flags,
                bytes = bytes.len(),
                "received encoded delivery"
            );
        }
        StreamData::Bytes(payload) => {
            tracing::debug!(
                processor_id = %processor_id,
                bytes = payload.len(),
                "received bytes"
            );
        }
        StreamData::Control(signal) => {
            tracing::debug!(
                processor_id = %processor_id,
                signal = ?signal,
                "received control"
            );
        }
        StreamData::Watermark(ts) => {
            tracing::debug!(processor_id = %processor_id, ts = ?ts, "received watermark");
        }
        StreamData::Error(error) => {
            tracing::debug!(
                processor_id = %processor_id,
                message = %error.message,
                "received error"
            );
        }
    }
}

/// Log a broadcast receiver lag event.
///
/// This should be rare when all sends are routed through cooperative backpressure helpers.
pub fn log_broadcast_lagged(processor_id: &str, skipped: u64, context: &str) {
    tracing::warn!(
        processor_id = %processor_id,
        skipped = skipped,
        context = %context,
        "broadcast receiver lagged"
    );
}

/// Default buffer size for processor data broadcast channels.
pub(crate) const DEFAULT_DATA_CHANNEL_CAPACITY: usize = 16;

/// Default buffer size for processor control broadcast channels.
pub(crate) const DEFAULT_CONTROL_CHANNEL_CAPACITY: usize = 2;

pub(crate) fn normalize_channel_capacity(capacity: usize) -> usize {
    capacity.max(1)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ProcessorChannelCapacities {
    pub data: usize,
    pub control: usize,
}

impl ProcessorChannelCapacities {
    pub(crate) fn new(data: usize, control: usize) -> Self {
        Self {
            data: normalize_channel_capacity(data),
            control: normalize_channel_capacity(control),
        }
    }
}

pub(crate) fn default_channel_capacities() -> ProcessorChannelCapacities {
    ProcessorChannelCapacities::new(
        DEFAULT_DATA_CHANNEL_CAPACITY,
        DEFAULT_CONTROL_CHANNEL_CAPACITY,
    )
}

pub(crate) type ProcessorReadyReceiver = oneshot::Receiver<Result<(), ProcessorError>>;

pub(crate) struct ProcessorStart {
    pub(crate) handle: JoinHandle<Result<(), ProcessorError>>,
    ready: Option<ProcessorReadyReceiver>,
}

impl ProcessorStart {
    pub(crate) fn ready(handle: JoinHandle<Result<(), ProcessorError>>) -> Self {
        Self {
            handle,
            ready: None,
        }
    }

    pub(crate) fn failed(spawner: &TaskSpawner, err: ProcessorError) -> Self {
        let (ready_tx, ready_rx) = oneshot::channel();
        let task_err = err.clone();
        let _ = ready_tx.send(Err(err));
        Self {
            handle: spawner.spawn(async move { Err(task_err) }),
            ready: Some(ready_rx),
        }
    }

    pub(crate) fn with_ready(
        handle: JoinHandle<Result<(), ProcessorError>>,
        ready: ProcessorReadyReceiver,
    ) -> Self {
        Self {
            handle,
            ready: Some(ready),
        }
    }

    pub(crate) fn take_ready(&mut self) -> Option<ProcessorReadyReceiver> {
        self.ready.take()
    }
}

impl std::future::IntoFuture for ProcessorStart {
    type Output = <JoinHandle<Result<(), ProcessorError>> as std::future::Future>::Output;
    type IntoFuture = JoinHandle<Result<(), ProcessorError>>;

    fn into_future(self) -> Self::IntoFuture {
        self.handle
    }
}

/// Trait for all stream processors
///
/// Processors are the building blocks of the stream processing pipeline.
/// Each processor can have multiple inputs and multiple outputs, communicating
/// via tokio mpsc channels with StreamData.
pub(crate) trait Processor: Send + Sync {
    /// Get the processor identifier
    fn id(&self) -> &str;

    /// Start the processor asynchronously.
    ///
    /// Processors with external startup work should resolve the readiness receiver only after
    /// they can safely accept upstream data.
    fn start(&mut self, spawner: &TaskSpawner) -> ProcessorStart;

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
    capacity: usize,
    data: StreamData,
    stats: Option<&ProcessorStats>,
) -> Result<(), ProcessorError> {
    const BACKOFF: Duration = Duration::from_millis(1);
    let capacity = normalize_channel_capacity(capacity);
    let mut payload = Some(data);
    loop {
        if sender.receiver_count() == 0 || sender.len() < capacity {
            let value = payload.take().ok_or_else(|| {
                ProcessorError::ProcessingError(
                    "send_with_backpressure payload state corrupted".to_string(),
                )
            })?;
            sender
                .send(value)
                .map(|_| ())
                .map_err(|_| ProcessorError::ChannelClosed)?;
            return Ok(());
        }
        if let Some(stats) = stats {
            // One tick per cooperative backpressure sleep.
            stats.record_send_backpressure_wait_tick();
        }
        sleep(BACKOFF).await;
    }
}

pub(crate) async fn send_control_with_backpressure(
    sender: &broadcast::Sender<ControlSignal>,
    capacity: usize,
    signal: ControlSignal,
) -> Result<(), ProcessorError> {
    const BACKOFF: Duration = Duration::from_millis(1);
    let capacity = normalize_channel_capacity(capacity);
    let mut payload = Some(signal);
    loop {
        if sender.receiver_count() == 0 || sender.len() < capacity {
            let value = payload.take().ok_or_else(|| {
                ProcessorError::ProcessingError(
                    "send_control_with_backpressure payload state corrupted".to_string(),
                )
            })?;
            sender
                .send(value)
                .map(|_| ())
                .map_err(|_| ProcessorError::ChannelClosed)?;
            return Ok(());
        }
        sleep(BACKOFF).await;
    }
}
