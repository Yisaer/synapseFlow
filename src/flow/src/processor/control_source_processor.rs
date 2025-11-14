//! ControlSourceProcessor - starting point for data flow
//!
//! This processor is responsible for receiving and sending control signals
//! that coordinate the entire stream processing pipeline.

use crate::processor::base::DEFAULT_CHANNEL_CAPACITY;
use crate::processor::{Processor, ProcessorError, StreamData};
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
}

impl ControlSourceProcessor {
    /// Create a new ControlSourceProcessor
    pub fn new(id: impl Into<String>) -> Self {
        let (output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        Self {
            id: id.into(),
            input: None,
            output,
        }
    }

    /// Send StreamData to all downstream processors
    pub async fn send(&self, data: StreamData) -> Result<(), ProcessorError> {
        self.output
            .send(data)
            .map(|_| ())
            .map_err(|_| ProcessorError::ChannelClosed)
    }

    /// Send StreamData to a specific downstream processor by id.
    ///
    /// The broadcast-based fan-out no longer supports targeted sends, so this
    /// method now returns an error to signal that behavior.
    pub async fn send_stream_data(
        &self,
        processor_id: &str,
        data: StreamData,
    ) -> Result<(), ProcessorError> {
        let _ = data;
        Err(ProcessorError::InvalidConfiguration(format!(
            "Targeted sends are not supported for control source outputs (requested: {})",
            processor_id
        )))
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

        tokio::spawn(async move {
            let input = match input_result {
                Ok(input) => input,
                Err(e) => return Err(e),
            };
            let mut stream = BroadcastStream::new(input);

            while let Some(item) = stream.next().await {
                let data = match item {
                    Ok(data) => data,
                    Err(BroadcastStreamRecvError::Lagged(skipped)) => {
                        return Err(ProcessorError::ProcessingError(format!(
                            "Control source input lagged by {} messages",
                            skipped
                        )))
                    }
                };
                output
                    .send(data.clone())
                    .map_err(|_| ProcessorError::ChannelClosed)?;
                if data.is_terminal() {
                    return Ok(());
                }
            }

            // Input closed without explicit StreamEnd, propagate shutdown.
            output
                .send(StreamData::stream_end())
                .map_err(|_| ProcessorError::ChannelClosed)?;
            Ok(())
        })
    }

    fn subscribe_output(&self) -> Option<broadcast::Receiver<StreamData>> {
        Some(self.output.subscribe())
    }

    fn add_input(&mut self, receiver: broadcast::Receiver<StreamData>) {
        // ControlSourceProcessor only supports single input
        // If input is already set, replace it
        self.input = Some(receiver);
    }
}
