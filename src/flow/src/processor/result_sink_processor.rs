//! ResultSinkProcessor - final destination for data flow
//!
//! This processor receives data from upstream processors and forwards it to a single output.

use tokio::sync::mpsc;
use futures::stream::StreamExt;
use crate::processor::{Processor, ProcessorError, StreamData};
use crate::processor::base::fan_in_streams;

/// ResultSinkProcessor - forwards received data to a single output
///
/// This processor acts as the final destination in the data flow. It:
/// - Receives StreamData from multiple upstream processors (multi-input)
/// - Forwards all received data to a single output channel (single-output)
/// - Can be used to collect results or forward to external systems
pub struct ResultSinkProcessor {
    /// Processor identifier
    id: String,
    /// Input channels for receiving data (multi-input)
    inputs: Vec<mpsc::Receiver<StreamData>>,
    /// Single output channel for forwarding received data (single-output)
    output: Option<mpsc::Sender<StreamData>>,
}

impl ResultSinkProcessor {
    /// Create a new ResultSinkProcessor
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
}

impl Processor for ResultSinkProcessor {
    fn id(&self) -> &str {
        &self.id
    }
    
    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let mut input_streams = fan_in_streams(std::mem::take(&mut self.inputs));
        let output = self.output.take()
            .ok_or_else(|| ProcessorError::InvalidConfiguration(
                "ResultSinkProcessor output must be set before starting".to_string()
            ));
        
        tokio::spawn(async move {
            let output = match output {
                Ok(output) => output,
                Err(e) => return Err(e),
            };

            while let Some(data) = input_streams.next().await {
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
    
    fn output_senders(&self) -> Vec<mpsc::Sender<StreamData>> {
        // Return single output as a vector (for compatibility with Processor trait)
        self.output.as_ref().map(|s| vec![s.clone()]).unwrap_or_default()
    }
    
    fn add_input(&mut self, receiver: mpsc::Receiver<StreamData>) {
        self.inputs.push(receiver);
    }
    
    fn add_output(&mut self, sender: mpsc::Sender<StreamData>) {
        // ResultSinkProcessor only supports single output
        // If output is already set, replace it
        self.output = Some(sender);
    }
}
