//! ResultCollectProcessor - final destination for data flow
//!
//! This processor receives data from upstream processors and forwards it to a single output.

use crate::processor::base::fan_in_streams;
use crate::processor::{Processor, ProcessorError, StreamData};
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
    control_inputs: Vec<broadcast::Receiver<StreamData>>,
    /// Single output channel for forwarding received data (single-output)
    output: Option<mpsc::Sender<StreamData>>,
}

impl ResultCollectProcessor {
    /// Create a new ResultCollectProcessor
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output: None,
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
        let mut control_streams = fan_in_streams(control_receivers);
        let mut control_active = !control_streams.is_empty();
        let output = self.output.take().ok_or_else(|| {
            ProcessorError::InvalidConfiguration(
                "ResultCollectProcessor output must be set before starting".to_string(),
            )
        });
        let processor_id = self.id.clone();

        tokio::spawn(async move {
            let output = match output {
                Ok(output) => output,
                Err(e) => return Err(e),
            };

            loop {
                tokio::select! {
                    control_item = control_streams.next(), if control_active => {
                        if let Some(result) = control_item {
                            let control_data = match result {
                                Ok(data) => data,
                                Err(BroadcastStreamRecvError::Lagged(skipped)) => {
                                    return Err(ProcessorError::ProcessingError(format!(
                                        "ResultCollectProcessor control input lagged by {} messages",
                                        skipped
                                    )))
                                }
                            };
                            output
                                .send(control_data.clone())
                                .await
                                .map_err(|_| ProcessorError::ChannelClosed)?;
                            if control_data.is_terminal() {
                                println!("[ResultCollectProcessor:{}] received StreamEnd (control)", processor_id);
                                output
                                    .send(StreamData::stream_end())
                                    .await
                                    .map_err(|_| ProcessorError::ChannelClosed)?;
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
                                output
                                    .send(data.clone())
                                    .await
                                    .map_err(|_| ProcessorError::ChannelClosed)?;
                                if data.is_terminal() {
                                    println!("[ResultCollectProcessor:{}] received StreamEnd (data)", processor_id);
                                    output
                                        .send(StreamData::stream_end())
                                        .await
                                        .map_err(|_| ProcessorError::ChannelClosed)?;
                                    return Ok(());
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                return Err(ProcessorError::ProcessingError(format!(
                                    "ResultCollectProcessor input lagged by {} messages",
                                    skipped
                                )))
                            }
                            None => {
                                output
                                    .send(StreamData::stream_end())
                                    .await
                                    .map_err(|_| ProcessorError::ChannelClosed)?;
                                return Ok(());
                            }
                        }
                    }
                }
            }
        })
    }

    fn subscribe_output(&self) -> Option<broadcast::Receiver<StreamData>> {
        None
    }

    fn add_input(&mut self, receiver: broadcast::Receiver<StreamData>) {
        self.inputs.push(receiver);
    }

    fn subscribe_control_output(&self) -> Option<broadcast::Receiver<StreamData>> {
        None
    }

    fn add_control_input(&mut self, receiver: broadcast::Receiver<StreamData>) {
        self.control_inputs.push(receiver);
    }
}
