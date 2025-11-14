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
    /// Single output channel for forwarding received data (single-output)
    output: Option<mpsc::Sender<StreamData>>,
}

impl ResultCollectProcessor {
    /// Create a new ResultCollectProcessor
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            inputs: Vec::new(),
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
        let output = self.output.take().ok_or_else(|| {
            ProcessorError::InvalidConfiguration(
                "ResultCollectProcessor output must be set before starting".to_string(),
            )
        });

        tokio::spawn(async move {
            let output = match output {
                Ok(output) => output,
                Err(e) => return Err(e),
            };

            while let Some(item) = input_streams.next().await {
                let data = match item {
                    Ok(data) => data,
                    Err(BroadcastStreamRecvError::Lagged(skipped)) => {
                        return Err(ProcessorError::ProcessingError(format!(
                            "ResultCollectProcessor input lagged by {} messages",
                            skipped
                        )))
                    }
                };

                output
                    .send(data.clone())
                    .await
                    .map_err(|_| ProcessorError::ChannelClosed)?;

                if data.is_terminal() {
                    output
                        .send(StreamData::stream_end())
                        .await
                        .map_err(|_| ProcessorError::ChannelClosed)?;
                    return Ok(());
                }
            }

            output
                .send(StreamData::stream_end())
                .await
                .map_err(|_| ProcessorError::ChannelClosed)?;
            Ok(())
        })
    }

    fn subscribe_output(&self) -> Option<broadcast::Receiver<StreamData>> {
        None
    }

    fn add_input(&mut self, receiver: broadcast::Receiver<StreamData>) {
        self.inputs.push(receiver);
    }
}
