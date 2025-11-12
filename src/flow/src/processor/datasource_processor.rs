//! DataSourceProcessor - processes data from PhysicalDatasource
//!
//! This processor reads data from a PhysicalDatasource and sends it downstream
//! as StreamData::Collection.

use tokio::sync::mpsc;
use std::sync::Arc;
use crate::processor::{Processor, ProcessorError, StreamData};
use crate::planner::physical::PhysicalDataSource;

/// DataSourceProcessor - reads data from PhysicalDatasource
///
/// This processor:
/// - Takes a PhysicalDatasource as input
/// - Reads data from the source when triggered by control signals
/// - Sends data downstream as StreamData::Collection
pub struct DataSourceProcessor {
    /// Processor identifier
    id: String,
    /// Physical datasource to read from
    physical_datasource: Arc<PhysicalDataSource>,
    /// Input channels for receiving control signals
    inputs: Vec<mpsc::Receiver<StreamData>>,
    /// Output channels for sending data downstream
    outputs: Vec<mpsc::Sender<StreamData>>,
}

impl DataSourceProcessor {
    /// Create a new DataSourceProcessor from PhysicalDatasource
    pub fn new(
        id: impl Into<String>,
        physical_datasource: Arc<PhysicalDataSource>,
    ) -> Self {
        Self {
            id: id.into(),
            physical_datasource,
            inputs: Vec::new(),
            outputs: Vec::new(),
        }
    }
    
    /// Create a DataSourceProcessor from a PhysicalPlan
    /// Returns None if the plan is not a PhysicalDataSource
    pub fn from_physical_plan(
        id: impl Into<String>,
        plan: Arc<dyn crate::planner::physical::PhysicalPlan>,
    ) -> Option<Self> {
        if let Some(ds) = plan.as_any().downcast_ref::<PhysicalDataSource>() {
            Some(Self::new(id, Arc::new(ds.clone())))
        } else {
            None
        }
    }
}

impl Processor for DataSourceProcessor {
    fn id(&self) -> &str {
        &self.id
    }
    
    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let id = self.id.clone();
        let source_name = self.physical_datasource.source_name.clone();
        let mut inputs = std::mem::take(&mut self.inputs);
        let outputs = self.outputs.clone();
        
        tokio::spawn(async move {
            let mut stream_started = false;
            
            loop {
                let mut all_closed = true;
                let mut received_data = false;
                
                // Check all input channels
                for input in &mut inputs {
                    match input.try_recv() {
                        Ok(data) => {
                            all_closed = false;
                            received_data = true;
                            
                            // Handle control signals
                            if let Some(control) = data.as_control() {
                                match control {
                                    crate::processor::ControlSignal::StreamStart => {
                                        stream_started = true;
                                        // Forward StreamStart to outputs
                                        for output in &outputs {
                                            if output.send(data.clone()).await.is_err() {
                                                return Err(ProcessorError::ChannelClosed);
                                            }
                                        }
                                    }
                                    crate::processor::ControlSignal::StreamEnd => {
                                        // Forward StreamEnd to outputs
                                        for output in &outputs {
                                            let _ = output.send(data.clone()).await;
                                        }
                                        return Ok(());
                                    }
                                    _ => {
                                        // Forward other control signals
                                        for output in &outputs {
                                            if output.send(data.clone()).await.is_err() {
                                                return Err(ProcessorError::ChannelClosed);
                                            }
                                        }
                                    }
                                }
                            } else {
                                // Forward non-control data (Collection or Error)
                                for output in &outputs {
                                    if output.send(data.clone()).await.is_err() {
                                        return Err(ProcessorError::ChannelClosed);
                                    }
                                }
                            }
                        }
                        Err(mpsc::error::TryRecvError::Empty) => {
                            all_closed = false;
                        }
                        Err(mpsc::error::TryRecvError::Disconnected) => {
                            // Channel disconnected
                        }
                    }
                }
                
                // If stream has started, try to read data
                if stream_started {
                    match read_data_internal(&source_name).await {
                        Ok(Some(collection)) => {
                            all_closed = false;
                            // Send data to all outputs
                            let stream_data = StreamData::collection(collection);
                            for output in &outputs {
                                if output.send(stream_data.clone()).await.is_err() {
                                    return Err(ProcessorError::ChannelClosed);
                                }
                            }
                        }
                        Ok(None) => {
                            // No data available, continue
                        }
                        Err(e) => {
                            // Send error downstream
                            let error_data = StreamData::error(
                                crate::processor::StreamError::new(e.to_string())
                                    .with_source(id.clone()),
                            );
                            for output in &outputs {
                                if output.send(error_data.clone()).await.is_err() {
                                    return Err(ProcessorError::ChannelClosed);
                                }
                            }
                        }
                    }
                }
                
                // If all channels are closed and no data received, exit
                if all_closed && !received_data {
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

/// Internal helper to read data from datasource
/// This is a placeholder - should be replaced with actual implementation
async fn read_data_internal(
    _source_name: &str,
) -> Result<Option<Box<dyn crate::model::Collection>>, ProcessorError> {
    // TODO: Implement actual data reading logic based on source_name
    // For now, return None to indicate no data available
    // This should be replaced with actual data source reading logic
    Ok(None)
}
