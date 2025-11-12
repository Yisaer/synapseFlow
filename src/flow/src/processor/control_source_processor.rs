//! ControlSourceProcessor - starting point for data flow
//!
//! This processor is responsible for receiving and sending control signals
//! that coordinate the entire stream processing pipeline.

use tokio::sync::mpsc;
use crate::processor::{Processor, ProcessorError, StreamData};

/// ControlSourceProcessor - handles control signals for the pipeline
///
/// This processor acts as the starting point of the data flow. It can:
/// - Receive StreamData (which may contain control signals, data, or errors) from inputs
/// - Forward StreamData to downstream processors
/// - Coordinate the start/end of stream processing
pub struct ControlSourceProcessor {
    /// Processor identifier
    id: String,
    /// Input channels for receiving StreamData
    inputs: Vec<mpsc::Receiver<StreamData>>,
    /// Output channels for sending StreamData downstream
    outputs: Vec<mpsc::Sender<StreamData>>,
}

impl ControlSourceProcessor {
    /// Create a new ControlSourceProcessor
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            inputs: Vec::new(),
            outputs: Vec::new(),
        }
    }
    
    /// Send StreamData to all downstream processors
    pub async fn send(&self, data: StreamData) -> Result<(), ProcessorError> {
        for output in &self.outputs {
            output
                .send(data.clone())
                .await
                .map_err(|_| ProcessorError::ChannelClosed)?;
        }
        Ok(())
    }
}

impl Processor for ControlSourceProcessor {
    fn id(&self) -> &str {
        &self.id
    }
    
    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let _id = self.id.clone();
        let mut inputs = std::mem::take(&mut self.inputs);
        let outputs = self.outputs.clone();
        
        tokio::spawn(async move {
            loop {
                let mut all_closed = true;
                
                // Check all input channels
                for input in &mut inputs {
                    match input.try_recv() {
                        Ok(data) => {
                            all_closed = false;
                            // Forward to all outputs
                            for output in &outputs {
                                if output.send(data.clone()).await.is_err() {
                                    return Err(ProcessorError::ChannelClosed);
                                }
                            }
                            // Check if this is a terminal signal
                            if data.is_terminal() {
                                return Ok(());
                            }
                        }
                        Err(mpsc::error::TryRecvError::Empty) => {
                            // Channel not empty but no data ready
                            all_closed = false;
                        }
                        Err(mpsc::error::TryRecvError::Disconnected) => {
                            // Channel disconnected
                        }
                    }
                }
                
                // If all channels are closed, exit
                if all_closed {
                    // Send StreamEnd to all outputs before exiting
                    for output in &outputs {
                        let _ = output.send(StreamData::stream_end()).await;
                    }
                    return Ok(());
                }
                
                // Yield to allow other tasks to run
                tokio::task::yield_now().await;
            }
        })
    }
    
    fn output_senders(&self) -> Vec<mpsc::Sender<StreamData>> {
        self.outputs.clone()
    }
    
    fn add_input(&mut self, receiver: mpsc::Receiver<StreamData>) {
        self.inputs.push(receiver);
    }
    
    fn add_output(&mut self, sender: mpsc::Sender<StreamData>) {
        self.outputs.push(sender);
    }
}
