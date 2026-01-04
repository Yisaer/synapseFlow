//! ControlSourceProcessor - starting point for data flow
//!
//! This processor is responsible for receiving and sending control signals
//! that coordinate the entire stream processing pipeline.

use crate::processor::base::{
    fan_in_control_streams, forward_error, send_control_with_backpressure, send_with_backpressure,
    DEFAULT_CHANNEL_CAPACITY,
};
use crate::processor::{BarrierControlSignal, ControlSignal, Processor, ProcessorError, StreamData};
use futures::stream::StreamExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_stream::wrappers::{errors::BroadcastStreamRecvError, BroadcastStream};

/// ControlSourceProcessor - handles control signals for the pipeline
///
/// This processor acts as the starting point of the data flow. It:
/// - Receives StreamData from a single input (single-input)
/// - Forwards StreamData to multiple downstream processors (multi-output)
/// - Coordinates the start/end of stream processing
pub struct ControlSourceProcessor {
    /// Processor identifier
    id: String,
    /// Single input channel for receiving StreamData (single-input)
    input: Option<broadcast::Receiver<StreamData>>,
    /// Broadcast channel for all downstream processors
    output: broadcast::Sender<StreamData>,
    /// Dedicated control channel for urgent control propagation
    control_output: broadcast::Sender<ControlSignal>,
    /// Upstream control-signal inputs
    control_inputs: Vec<broadcast::Receiver<ControlSignal>>,
    /// Monotonically increasing barrier id allocator (shared across control/data channels).
    next_barrier_id: Arc<AtomicU64>,
}

impl ControlSourceProcessor {
    /// Create a new ControlSourceProcessor
    pub fn new(id: impl Into<String>) -> Self {
        let (output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        let (control_output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        Self {
            id: id.into(),
            input: None,
            output,
            control_output,
            control_inputs: Vec::new(),
            next_barrier_id: Arc::new(AtomicU64::new(1)),
        }
    }

    pub fn allocate_barrier_id(&self) -> u64 {
        self.next_barrier_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Send StreamData to all downstream processors
    pub async fn send(&self, data: StreamData) -> Result<(), ProcessorError> {
        send_with_backpressure(&self.output, data).await
    }

    /// Send StreamData to a specific downstream processor by id.
    ///
    /// The broadcast-based fan-out no longer supports targeted sends, so this
    /// method now returns an error to signal that behavior.
    pub async fn send_stream_data(
        &self,
        _processor_id: &str,
        data: StreamData,
    ) -> Result<(), ProcessorError> {
        self.send(data).await
    }
}

impl Processor for ControlSourceProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let input_result = self.input.take().ok_or_else(|| {
            ProcessorError::InvalidConfiguration(
                "ControlSourceProcessor input must be set before starting".to_string(),
            )
        });
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let processor_id = self.id.clone();
        let barrier_id_allocator = Arc::clone(&self.next_barrier_id);
        let control_inputs = std::mem::take(&mut self.control_inputs);
        let mut control_streams = fan_in_control_streams(control_inputs);
        let mut control_active = !control_streams.is_empty();
        tracing::info!(processor_id = %processor_id, "control source processor starting");

        tokio::spawn(async move {
            let input = match input_result {
                Ok(input) => input,
                Err(e) => return Err(e),
            };
            let mut stream = BroadcastStream::new(input);

            loop {
                tokio::select! {
                    biased;
                    control_item = control_streams.next(), if control_active => {
                        if let Some(Ok(control_signal)) = control_item {
                            let is_terminal = control_signal.is_terminal();
                            send_control_with_backpressure(&control_output, control_signal).await?;
                            if is_terminal {
                                tracing::info!(processor_id = %processor_id, "received terminal control signal (control)");
                                return Ok(());
                            }
                            continue;
                        } else {
                            control_active = false;
                        }
                    }
                    item = stream.next() => {
                        let data = match item {
                            Some(Ok(data)) => data,
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                let message =
                                    format!("Control source input lagged by {} messages", skipped);
                                tracing::warn!(processor_id = %processor_id, skipped = skipped, "input lagged");
                                forward_error(&output, &processor_id, message).await?;
                                continue;
                            }
                            None => break,
                        };

                        let is_terminal = data.is_terminal();
                        send_with_backpressure(&output, data).await?;
                        if is_terminal {
                            tracing::info!(processor_id = %processor_id, "received StreamEnd");
                            return Ok(());
                        }
                    }
                }
            }

            // Input closed without explicit StreamEnd, propagate shutdown.
            tracing::info!(processor_id = %processor_id, "stopping (input closed)");
            let barrier_id = barrier_id_allocator.fetch_add(1, Ordering::Relaxed);
            send_with_backpressure(
                &output,
                StreamData::control(ControlSignal::Barrier(
                    BarrierControlSignal::StreamGracefulEnd { barrier_id },
                )),
            )
            .await?;
            Ok(())
        })
    }

    fn subscribe_output(&self) -> Option<broadcast::Receiver<StreamData>> {
        Some(self.output.subscribe())
    }

    fn subscribe_control_output(&self) -> Option<broadcast::Receiver<ControlSignal>> {
        Some(self.control_output.subscribe())
    }

    fn add_input(&mut self, receiver: broadcast::Receiver<StreamData>) {
        self.input = Some(receiver);
    }

    fn add_control_input(&mut self, receiver: broadcast::Receiver<ControlSignal>) {
        self.control_inputs.push(receiver);
    }
}
