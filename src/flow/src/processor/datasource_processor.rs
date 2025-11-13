//! DataSourceProcessor - processes data from PhysicalDatasource
//!
//! This processor reads data from a PhysicalDatasource and sends it downstream
//! as StreamData::Collection.

use tokio::sync::mpsc;
use futures::stream::StreamExt;
use crate::processor::{Processor, ProcessorError, StreamData};
use crate::processor::base::{fan_in_streams, broadcast_all};

/// DataSourceProcessor - reads data from PhysicalDatasource
///
/// This processor:
/// - Takes a PhysicalDatasource as input
/// - Reads data from the source when triggered by control signals
/// - Sends data downstream as StreamData::Collection
pub struct DataSourceProcessor {
    /// Processor identifier
    source_name: String,
    /// Input channels for receiving control signals
    inputs: Vec<mpsc::Receiver<StreamData>>,
    /// Output channels for sending data downstream
    outputs: Vec<mpsc::Sender<StreamData>>,
}

impl DataSourceProcessor {
    /// Create a new DataSourceProcessor from PhysicalDatasource
    pub fn new(
        source_name: impl Into<String>,
    ) -> Self {
        Self {
            source_name: source_name.into(),
            inputs: Vec::new(),
            outputs: Vec::new(),
        }
    }
}

impl Processor for DataSourceProcessor {
    fn id(&self) -> &str {
        &self.source_name
    }
    
    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let outputs = self.outputs.clone();
        let mut input_streams = fan_in_streams(std::mem::take(&mut self.inputs));
        
        tokio::spawn(async move {
            while let Some(data) = input_streams.next().await {
                match data.as_control() {
                    Some(crate::processor::ControlSignal::StreamEnd) => {
                        broadcast_all(&outputs, data.clone()).await?;
                        return Ok(());
                    }
                    Some(_) => {
                        broadcast_all(&outputs, data.clone()).await?;
                    }
                    None => {
                        broadcast_all(&outputs, data.clone()).await?;
                    }
                }
            }

            Ok(())
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
