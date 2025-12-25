//! ControlSourceProcessor - starting point for data flow
//!
//! This processor is responsible for receiving and sending control signals
//! that coordinate the entire stream processing pipeline.

use crate::processor::base::{
    fan_in_control_streams, forward_error, send_control_with_backpressure, send_with_backpressure,
    DEFAULT_CHANNEL_CAPACITY,
};
use crate::processor::{ControlSignal, Processor, ProcessorError, StreamData};
use futures::stream::StreamExt;
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
        }
    }

    /// Send StreamData to all downstream processors
    pub async fn send(&self, data: StreamData) -> Result<(), ProcessorError> {
        if let StreamData::Control(signal) = &data {
            if signal.routes_via_control() {
                send_control_with_backpressure(&self.control_output, signal.clone()).await?;
            }
            if !signal.routes_via_data() {
                return Ok(());
            }
        }
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
                            handle_control_signal(&output, &control_output, control_signal).await?;
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
                        let mut deliver_to_data = true;

                        if let StreamData::Control(signal) = &data {
                            handle_control_signal(&output, &control_output, signal.clone()).await?;
                            if !signal.routes_via_data() {
                                deliver_to_data = false;
                            }
                        }

                        if deliver_to_data {
                            send_with_backpressure(&output, data).await?;
                        }

                        if is_terminal {
                            tracing::info!(processor_id = %processor_id, "received StreamEnd");
                            return Ok(());
                        }
                    }
                }
            }

            // Input closed without explicit StreamEnd, propagate shutdown.
            tracing::info!(processor_id = %processor_id, "stopping (input closed)");
            send_with_backpressure(&output, StreamData::stream_end()).await?;
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

async fn handle_control_signal(
    data_output: &broadcast::Sender<StreamData>,
    control_output: &broadcast::Sender<ControlSignal>,
    signal: ControlSignal,
) -> Result<(), ProcessorError> {
    if signal.routes_via_control() {
        send_control_with_backpressure(control_output, signal.clone()).await?;
    }
    if signal.routes_via_data() {
        send_with_backpressure(data_output, StreamData::control(signal)).await?;
    }
    Ok(())
}
