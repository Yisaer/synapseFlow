//! FilterProcessor - processes filter operations
//!
//! This processor evaluates filter expressions and produces output with filtered records.

use tokio::sync::mpsc;
use futures::stream::StreamExt;
use std::sync::Arc;
use crate::processor::{Processor, ProcessorError, StreamData, StreamError};
use crate::processor::base::{fan_in_streams, broadcast_all};
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
        let mut input_streams = fan_in_streams(std::mem::take(&mut self.inputs));
        let outputs = self.outputs.clone();
        let filter_expr = self.physical_filter.scalar_predicate.clone();
        
        tokio::spawn(async move {
            while let Some(data) = input_streams.next().await {
                if let Some(control) = data.as_control() {
                    match control {
                        crate::processor::ControlSignal::StreamEnd => {
                            broadcast_all(&outputs, data.clone()).await?;
                            return Ok(());
                        }
                        _ => {
                            broadcast_all(&outputs, data.clone()).await?;
                        }
                    }
                    continue;
                }

                if let Some(collection) = data.as_collection() {
                    match apply_filter(collection, &filter_expr) {
                        Ok(filtered_collection) => {
                            let filtered_data = StreamData::collection(filtered_collection);
                            broadcast_all(&outputs, filtered_data).await?;
                        }
                        Err(e) => {
                            let error_data = StreamData::error(
                                StreamError::new(e.to_string()).with_source(id.clone()),
                            );
                            broadcast_all(&outputs, error_data).await?;
                        }
                    }
                } else {
                    broadcast_all(&outputs, data.clone()).await?;
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
