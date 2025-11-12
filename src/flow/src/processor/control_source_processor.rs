//! Control source processor - starting point for control signals

use async_trait::async_trait;
use tokio::sync::mpsc;
use crate::processor::{StreamProcessor, StreamData, ControlSignal, ProcessorHandle, stream_processor::utils};

/// ControlSourceProcessor - generates and forwards control signals
pub struct ControlSourceProcessor {
    /// Number of output channels
    output_count: usize,
    /// Processor name for debugging
    processor_name: String,
}

impl ControlSourceProcessor {
    /// Create a new ControlSourceProcessor
    pub fn new(output_count: usize) -> Self {
        Self {
            output_count,
            processor_name: "ControlSourceProcessor".to_string(),
        }
    }
    
    /// Create with custom name
    pub fn with_name(name: String, output_count: usize) -> Self {
        Self {
            output_count,
            processor_name: name,
        }
    }
}

#[async_trait]
impl StreamProcessor for ControlSourceProcessor {
    fn name(&self) -> &str {
        &self.processor_name
    }
    
    fn input_count(&self) -> usize {
        0 // Control source has no inputs
    }
    
    fn output_count(&self) -> usize {
        self.output_count
    }
    
    async fn start(&self, _input_receivers: Vec<mpsc::Receiver<StreamData>>) -> ProcessorHandle {
        let (output_senders, output_receivers) = utils::create_channels(self.output_count);
        
        let processor_name = self.processor_name.clone();
        
        let task_handle = tokio::spawn(async move {
            println!("{}: 🚀 Starting control source processor", processor_name);
            
            // Send initial control signal to start the pipeline
            let init_signal = StreamData::control(ControlSignal::StreamStart);
            
            for (i, sender) in output_senders.iter().enumerate() {
                if let Err(e) = sender.send(init_signal.clone()).await {
                    println!("{}: ❌ Failed to send initial signal to output {}: {}", processor_name, i, e);
                } else {
                    println!("{}: 📡 Sent initial control signal to output {}", processor_name, i);
                }
            }
            
            // Keep the processor running to handle future control signals
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
                println!("{}: 💓 Control source heartbeat", processor_name);
            }
        });
        
        ProcessorHandle {
            task_handle,
            output_receivers,
        }
    }
}