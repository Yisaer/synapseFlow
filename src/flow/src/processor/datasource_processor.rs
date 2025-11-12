//! Data source processor - handles data generation and streaming

use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::mpsc;
use crate::processor::{StreamProcessor, StreamData, ControlSignal, ProcessorHandle, stream_processor::utils};
use crate::planner::physical::PhysicalPlan;

/// DataSourceProcessor - generates data streams based on physical plan
pub struct DataSourceProcessor {
    /// The physical plan this processor corresponds to
    physical_plan: Arc<dyn PhysicalPlan>,
    /// Number of input channels (for control signals)
    input_count: usize,
    /// Number of output channels
    output_count: usize,
    /// Processor name for debugging
    processor_name: String,
}

impl DataSourceProcessor {
    /// Create a new DataSourceProcessor
    pub fn new(physical_plan: Arc<dyn PhysicalPlan>, input_count: usize, output_count: usize) -> Self {
        Self {
            physical_plan,
            input_count,
            output_count,
            processor_name: "DataSourceProcessor".to_string(),
        }
    }
    
    /// Create with custom name
    pub fn with_name(name: String, physical_plan: Arc<dyn PhysicalPlan>, input_count: usize, output_count: usize) -> Self {
        Self {
            physical_plan,
            input_count,
            output_count,
            processor_name: name,
        }
    }
}

#[async_trait]
impl StreamProcessor for DataSourceProcessor {
    fn name(&self) -> &str {
        &self.processor_name
    }
    
    fn input_count(&self) -> usize {
        self.input_count
    }
    
    fn output_count(&self) -> usize {
        self.output_count
    }
    
    async fn start(&self, input_receivers: Vec<mpsc::Receiver<StreamData>>) -> ProcessorHandle {
        let (output_senders, output_receivers) = utils::create_channels(self.output_count);
        
        let processor_name = self.processor_name.clone();
        let data_source_name = self.physical_plan.get_plan_index().to_string();
        
        let task_handle = tokio::spawn(async move {
            let mut input_receivers = input_receivers;
            println!("{}: 🚀 Starting data source processor for source {}", processor_name, data_source_name);
            
            // For now, generate some sample data as error messages for simplicity
            let sample_data = vec![
                StreamData::error_message("Sample data 1"),
                StreamData::error_message("Sample data 2"),
                StreamData::error_message("Sample data 3"),
            ];
            
            // Wait for control signal before starting
            if let Some(mut control_receiver) = input_receivers.into_iter().next() {
                println!("{}: ⏳ Waiting for control signal...", processor_name);
                if let Some(_control_signal) = control_receiver.recv().await {
                    println!("{}: 📡 Received control signal", processor_name);
                }
            }
            
            // Send data to all outputs
            for (i, data) in sample_data.iter().enumerate() {
                for (j, sender) in output_senders.iter().enumerate() {
                    if let Err(e) = sender.send(data.clone()).await {
                        println!("{}: ❌ Failed to send data {} to output {}: {}", processor_name, i, j, e);
                    } else {
                        println!("{}: 📤 Sent data {} to output {}", processor_name, i, j);
                    }
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
            
            // Send completion signal
            let completion_signal = StreamData::control(ControlSignal::StreamEnd);
            for (j, sender) in output_senders.iter().enumerate() {
                if let Err(e) = sender.send(completion_signal.clone()).await {
                    println!("{}: ❌ Failed to send completion to output {}: {}", processor_name, j, e);
                } else {
                    println!("{}: ✅ Sent completion signal to output {}", processor_name, j);
                }
            }
            
            println!("{}: 🏁 Data source processing complete", processor_name);
        });
        
        ProcessorHandle {
            task_handle,
            output_receivers,
        }
    }
}