//! ResultCollectProcessor - final destination for data flow
//!
//! This processor receives data from upstream processors and forwards it to a single output.

use crate::processor::base::{fan_in_control_streams, fan_in_streams, log_received_data};
use crate::processor::{ControlSignal, Processor, ProcessorError, StreamData, StreamError};
use futures::stream::StreamExt;
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

/// ResultCollectProcessor - forwards received data to a single output
///
/// This processor acts as the final destination in the data flow. It:
/// - Receives StreamData from multiple upstream processors (multi-input)
/// - Forwards all received data to a single output channel (single-output)
/// - Can be used to collect results or forward to external systems
pub struct ResultCollectProcessor {
    /// Processor identifier
    id: String,
    /// Input channels for receiving data (multi-input)
    inputs: Vec<broadcast::Receiver<StreamData>>,
    /// Control input channels for high-priority signals
    control_inputs: Vec<broadcast::Receiver<ControlSignal>>,
    /// Single output channel for forwarding received data (single-output)
    output: Option<mpsc::Sender<StreamData>>,
    /// Broadcast sender for downstream subscriptions
    broadcast_output: broadcast::Sender<StreamData>,
    /// Broadcast sender for control signals
    broadcast_control_output: broadcast::Sender<ControlSignal>,
}

impl ResultCollectProcessor {
    /// Create a new ResultCollectProcessor
    pub fn new(id: impl Into<String>) -> Self {
        let (broadcast_output, _) = broadcast::channel(crate::processor::base::DEFAULT_CHANNEL_CAPACITY);
        let (broadcast_control_output, _) = broadcast::channel(crate::processor::base::DEFAULT_CHANNEL_CAPACITY);
        Self {
            id: id.into(),
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output: None,
            broadcast_output,
            broadcast_control_output,
        }
    }

    /// Get the output receiver (for connecting to external systems)
    /// Returns None if output is not set
    pub fn output_receiver(&self) -> Option<&mpsc::Sender<StreamData>> {
        self.output.as_ref()
    }

    /// Set the downstream output channel (typically the pipeline output)
    pub fn set_output(&mut self, sender: mpsc::Sender<StreamData>) {
        self.output = Some(sender);
    }
}

impl Processor for ResultCollectProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let mut input_streams = fan_in_streams(std::mem::take(&mut self.inputs));
        let control_receivers = std::mem::take(&mut self.control_inputs);
        let mut control_streams = fan_in_control_streams(control_receivers);
        let mut control_active = !control_streams.is_empty();
        let output = self.output.take().ok_or_else(|| {
            ProcessorError::InvalidConfiguration(
                "ResultCollectProcessor output must be set before starting".to_string(),
            )
        });
        let broadcast_output = self.broadcast_output.clone();
        let broadcast_control_output = self.broadcast_control_output.clone();
        let processor_id = self.id.clone();
        println!("[ResultCollectProcessor:{processor_id}] starting");

        tokio::spawn(async move {
            let output = match output {
                Ok(output) => output,
                Err(e) => return Err(e),
            };

            loop {
                tokio::select! {
                    biased;
                    control_item = control_streams.next(), if control_active => {
                        if let Some(Ok(control_signal)) = control_item {
                            // Forward control signal to broadcast
                            let _ = broadcast_control_output.send(control_signal.clone());
                            if control_signal.is_terminal() {
                                println!("[ResultCollectProcessor:{}] received StreamEnd (control)", processor_id);
                                let stream_end = StreamData::stream_end();
                                let _ = broadcast_output.send(stream_end.clone());
                                output
                                    .send(stream_end)
                                    .await
                                    .map_err(|_| ProcessorError::ChannelClosed)?;
                                println!("[ResultCollectProcessor:{}] stopped", processor_id);
                                return Ok(());
                            }
                            continue;
                        } else {
                            control_active = false;
                        }
                    }
                    item = input_streams.next() => {
                        match item {
                            Some(Ok(data)) => {
                                log_received_data(&processor_id, &data);
                                let is_terminal = data.is_terminal();
                                // Forward data to broadcast
                                let _ = broadcast_output.send(data.clone());
                                // Forward data to mpsc output
                                output
                                    .send(data)
                                    .await
                                    .map_err(|_| ProcessorError::ChannelClosed)?;
                                if is_terminal {
                                    println!("[ResultCollectProcessor:{}] received StreamEnd (data)", processor_id);
                                    return Ok(());
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                let message = format!(
                                    "ResultCollectProcessor input lagged by {} messages",
                                    skipped
                                );
                                println!(
                                    "[ResultCollectProcessor:{}] input lagged by {} messages",
                                    processor_id, skipped
                                );
                                let error_data = StreamData::error(
                                    StreamError::new(message)
                                        .with_source(processor_id.clone()),
                                );
                                let _ = broadcast_output.send(error_data.clone());
                                output
                                    .send(error_data)
                                    .await
                                    .map_err(|_| ProcessorError::ChannelClosed)?;
                                continue;
                            }
                            None => {
                                let stream_end = StreamData::stream_end();
                                let _ = broadcast_output.send(stream_end.clone());
                                output
                                    .send(stream_end)
                                    .await
                                    .map_err(|_| ProcessorError::ChannelClosed)?;
                                println!("[ResultCollectProcessor:{}] stopped", processor_id);
                                return Ok(());
                            }
                        }
                    }
                }
            }
        })
    }

    fn subscribe_output(&self) -> Option<broadcast::Receiver<StreamData>> {
        Some(self.broadcast_output.subscribe())
    }

    fn add_input(&mut self, receiver: broadcast::Receiver<StreamData>) {
        self.inputs.push(receiver);
    }

    fn subscribe_control_output(&self) -> Option<broadcast::Receiver<ControlSignal>> {
        Some(self.broadcast_control_output.subscribe())
    }

    fn add_control_input(&mut self, receiver: broadcast::Receiver<ControlSignal>) {
        self.control_inputs.push(receiver);
    }
}
