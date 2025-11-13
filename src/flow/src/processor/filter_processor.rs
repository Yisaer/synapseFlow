//! FilterProcessor - processes filter operations
//!
//! This processor evaluates filter expressions and produces output with filtered records.

use tokio::sync::mpsc;
use std::sync::Arc;
use crate::processor::{Processor, ProcessorError, StreamData};
use crate::planner::physical::PhysicalFilter;
use crate::model::Collection;

/// FilterProcessor - evaluates filter expressions
///
/// This processor:
/// - Takes input data (Collection) and filter expressions
/// - Evaluates the expressions to filter records
/// - Sends the filtered data downstream as StreamData::Collection
pub struct FilterProcessor {
    /// Processor identifier
    id: String,
    /// Physical filter configuration
    physical_filter: Arc<PhysicalFilter>,
    /// Input channels for receiving data
    inputs: Vec<mpsc::Receiver<StreamData>>,
    /// Output channels for sending data downstream
    outputs: Vec<mpsc::Sender<StreamData>>,
}

impl FilterProcessor {
    /// Create a new FilterProcessor from PhysicalFilter
    pub fn new(
        id: impl Into<String>,
        physical_filter: Arc<PhysicalFilter>,
    ) -> Self {
        Self {
            id: id.into(),
            physical_filter,
            inputs: Vec::new(),
            outputs: Vec::new(),
        }
    }
    
    /// Create a FilterProcessor from a PhysicalPlan
    /// Returns None if the plan is not a PhysicalFilter
    pub fn from_physical_plan(
        id: impl Into<String>,
        plan: Arc<dyn crate::planner::physical::PhysicalPlan>,
    ) -> Option<Self> {
        plan.as_any().downcast_ref::<PhysicalFilter>().map(|filter| Self::new(id, Arc::new(filter.clone())))
    }
}

/// Apply filter to a collection
fn apply_filter(input_collection: &dyn Collection, filter_expr: &crate::expr::ScalarExpr) -> Result<Box<dyn Collection>, ProcessorError> {
    // Use the collection's apply_filter method
    input_collection.apply_filter(filter_expr)
        .map_err(|e| ProcessorError::ProcessingError(format!("Failed to apply filter: {}", e)))
}

impl Processor for FilterProcessor {
    fn id(&self) -> &str {
        &self.id
    }
    
    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let id = self.id.clone();
        let mut inputs = std::mem::take(&mut self.inputs);
        let outputs = self.outputs.clone();
        let filter_expr = self.physical_filter.scalar_predicate.clone();
        
        tokio::spawn(async move {
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
                            } else if let Some(collection) = data.as_collection() {
                                // Apply filter to the collection
                                match apply_filter(collection, &filter_expr) {
                                    Ok(filtered_collection) => {
                                        // Send filtered data to all outputs
                                        let filtered_data = StreamData::collection(filtered_collection);
                                        for output in &outputs {
                                            if output.send(filtered_data.clone()).await.is_err() {
                                                return Err(ProcessorError::ChannelClosed);
                                            }
                                        }
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
                            } else {
                                // Forward non-collection data (errors, etc.)
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