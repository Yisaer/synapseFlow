//! Result sink processor - final destination for data streams

use async_trait::async_trait;
use tokio::sync::mpsc;
use crate::processor::{StreamProcessor, StreamData, ControlSignal, ProcessorHandle, stream_processor::utils};

/// ResultSinkProcessor - receives data and forwards results for verification
/// 
/// Unlike a true sink, this processor has outputs so we can verify
/// that data flowed correctly through the entire chain.
pub struct ResultSinkProcessor {
    /// Number of input channels
    input_count: usize,
    /// Number of output channels (for verification)
    output_count: usize,
    /// Processor name for debugging
    processor_name: String,
}

impl ResultSinkProcessor {
    /// Create a new ResultSinkProcessor with default 1 output for verification
    pub fn new(input_count: usize) -> Self {
        Self {
            input_count,
            output_count: 1, // Default 1 output for verification
            processor_name: "ResultSinkProcessor".to_string(),
        }
    }
    
    /// Create with custom name and specified outputs
    pub fn with_outputs(name: String, input_count: usize, output_count: usize) -> Self {
        Self {
            input_count,
            output_count,
            processor_name: name,
        }
    }
    
    /// Create with custom name and default 1 output
    pub fn with_name(name: String, input_count: usize) -> Self {
        Self::with_outputs(name, input_count, 1)
    }
}

#[async_trait]
impl StreamProcessor for ResultSinkProcessor {
    fn name(&self) -> &str {
        &self.processor_name
    }
    
    fn input_count(&self) -> usize {
        self.input_count
    }
    
    fn output_count(&self) -> usize {
        self.output_count // Now has outputs for verification
    }
    
    async fn start(&self, input_receivers: Vec<mpsc::Receiver<StreamData>>) -> ProcessorHandle {
        let processor_name = self.processor_name.clone();
        let output_count = self.output_count;
        
        // Create output channels for verification
        let (output_senders, output_receivers) = utils::create_channels(output_count);
        
        let task_handle = tokio::spawn(async move {
            println!("{}: 🎯 Starting result sink processor with {} inputs and {} verification outputs", 
                     processor_name, input_receivers.len(), output_count);
            
            let mut total_received = 0;
            let mut completed_inputs = 0;
            let mut verification_data = Vec::new();
            
            // Process all input channels
            for (i, mut receiver) in input_receivers.into_iter().enumerate() {
                println!("{}: 📥 Monitoring input channel {}", processor_name, i);
                
                loop {
                    match receiver.recv().await {
                        Some(stream_data) => {
                            total_received += 1;
                            match stream_data {
                                StreamData::Collection(_collection) => {
                                    println!("{}: 📊 Received collection from input {}", processor_name, i);
                                    // Store for verification
                                    verification_data.push(format!("Collection from input {}", i));
                                }
                                StreamData::Control(control_signal) => {
                                    println!("{}: 📡 Received control signal from input {}", processor_name, i);
                                    
                                    if matches!(control_signal, ControlSignal::StreamEnd) {
                                        println!("{}: ✅ Stream end signal received on input {}", processor_name, i);
                                        completed_inputs += 1;
                                        break;
                                    }
                                }
                                StreamData::Error(error) => {
                                    println!("{}: ⚠️  Received error from input {}: {:?}", processor_name, i, error);
                                    // Store error for verification
                                    verification_data.push(format!("Error: {}", error.message));
                                }
                            }
                        }
                        None => {
                            println!("{}: 🔚 Input channel {} closed", processor_name, i);
                            break;
                        }
                    }
                }
            }
            
            // Send verification results through output channels
            println!("{}: 📤 Sending verification results through {} output channels", processor_name, output_count);
            
            let summary = format!("ResultSink Summary: {} items received, {} inputs completed, verification data: {:?}", 
                                total_received, completed_inputs, verification_data);
            
            for (i, sender) in output_senders.iter().enumerate() {
                let verification_stream = StreamData::error_message(summary.clone());
                if let Err(e) = sender.send(verification_stream).await {
                    println!("{}: ❌ Failed to send verification data to output {}: {}", processor_name, i, e);
                } else {
                    println!("{}: ✅ Sent verification data to output {}", processor_name, i);
                }
            }
            
            println!("{}: 🏁 Processing complete! Total items received: {}, Completed inputs: {}", 
                     processor_name, total_received, completed_inputs);
            println!("{}: ✨ Verification data sent through outputs", processor_name);
        });
        
        ProcessorHandle {
            task_handle,
            output_receivers,
        }
    }
}