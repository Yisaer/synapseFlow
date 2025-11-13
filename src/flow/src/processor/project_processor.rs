//! ProjectProcessor - processes projection operations
//!
//! This processor evaluates projection expressions and produces output with projected fields.

use tokio::sync::mpsc;
use futures::stream::StreamExt;
use std::sync::Arc;
use crate::processor::{Processor, ProcessorError, StreamData, StreamError};
use crate::processor::base::{fan_in_streams, broadcast_all};
use crate::planner::physical::{PhysicalProject, PhysicalProjectField};
use crate::model::Collection;

/// ProjectProcessor - evaluates projection expressions
///
/// This processor:
/// - Takes input data (Collection) and projection expressions
/// - Evaluates the expressions to create projected fields
/// - Sends the projected data downstream as StreamData::Collection
pub struct ProjectProcessor {
    /// Processor identifier
    id: String,
    /// Physical projection configuration
    physical_project: Arc<PhysicalProject>,
    /// Input channels for receiving data
    inputs: Vec<mpsc::Receiver<StreamData>>,
    /// Output channels for sending data downstream
    outputs: Vec<mpsc::Sender<StreamData>>,
}

impl ProjectProcessor {
    /// Create a new ProjectProcessor from PhysicalProject
    pub fn new(
        id: impl Into<String>,
        physical_project: Arc<PhysicalProject>,
    ) -> Self {
        Self {
            id: id.into(),
            physical_project,
            inputs: Vec::new(),
            outputs: Vec::new(),
        }
    }
    
    /// Create a ProjectProcessor from a PhysicalPlan
    /// Returns None if the plan is not a PhysicalProject
    pub fn from_physical_plan(
        id: impl Into<String>,
        plan: Arc<dyn crate::planner::physical::PhysicalPlan>,
    ) -> Option<Self> {
        plan.as_any().downcast_ref::<PhysicalProject>().map(|proj| Self::new(id, Arc::new(proj.clone())))
    }
}

/// Apply projection to a collection
fn apply_projection(input_collection: &dyn Collection, fields: &[PhysicalProjectField]) -> Result<Box<dyn Collection>, ProcessorError> {
    // Use the collection's apply_projection method
    input_collection.apply_projection(fields)
        .map_err(|e| ProcessorError::ProcessingError(format!("Failed to apply projection: {}", e)))
}

impl Processor for ProjectProcessor {
    fn id(&self) -> &str {
        &self.id
    }
    
    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let id = self.id.clone();
        let mut input_streams = fan_in_streams(std::mem::take(&mut self.inputs));
        let outputs = self.outputs.clone();
        let fields = self.physical_project.fields.clone();
        
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
                    match apply_projection(collection, &fields) {
                        Ok(projected_collection) => {
                            let projected_data = StreamData::collection(projected_collection);
                            broadcast_all(&outputs, projected_data).await?;
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
