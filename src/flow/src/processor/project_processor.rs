//! ProjectProcessor - processes projection operations
//!
//! This processor evaluates projection expressions and produces output with projected fields.

use crate::model::Collection;
use crate::planner::physical::{PhysicalProject, PhysicalProjectField};
use crate::processor::base::{fan_in_streams, DEFAULT_CHANNEL_CAPACITY};
use crate::processor::{Processor, ProcessorError, StreamData, StreamError};
use futures::stream::StreamExt;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

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
    inputs: Vec<broadcast::Receiver<StreamData>>,
    /// Control input channels
    control_inputs: Vec<broadcast::Receiver<StreamData>>,
    /// Broadcast channel for downstream processors
    output: broadcast::Sender<StreamData>,
    /// Dedicated control output channel
    control_output: broadcast::Sender<StreamData>,
}

impl ProjectProcessor {
    /// Create a new ProjectProcessor from PhysicalProject
    pub fn new(id: impl Into<String>, physical_project: Arc<PhysicalProject>) -> Self {
        let (output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        let (control_output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        Self {
            id: id.into(),
            physical_project,
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
        }
    }

    /// Create a ProjectProcessor from a PhysicalPlan
    /// Returns None if the plan is not a PhysicalProject
    pub fn from_physical_plan(
        id: impl Into<String>,
        plan: Arc<dyn crate::planner::physical::PhysicalPlan>,
    ) -> Option<Self> {
        plan.as_any()
            .downcast_ref::<PhysicalProject>()
            .map(|proj| Self::new(id, Arc::new(proj.clone())))
    }
}

/// Apply projection to a collection
fn apply_projection(
    input_collection: &dyn Collection,
    fields: &[PhysicalProjectField],
) -> Result<Box<dyn Collection>, ProcessorError> {
    // Use the collection's apply_projection method
    input_collection
        .apply_projection(fields)
        .map_err(|e| ProcessorError::ProcessingError(format!("Failed to apply projection: {}", e)))
}

impl Processor for ProjectProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let id = self.id.clone();
        let mut input_streams = fan_in_streams(std::mem::take(&mut self.inputs));
        let control_receivers = std::mem::take(&mut self.control_inputs);
        let mut control_streams = fan_in_streams(control_receivers);
        let mut control_active = !control_streams.is_empty();
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let fields = self.physical_project.fields.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    control_item = control_streams.next(), if control_active => {
                        if let Some(result) = control_item {
                            let control_data = match result {
                                Ok(data) => data,
                                Err(BroadcastStreamRecvError::Lagged(skipped)) => {
                                    return Err(ProcessorError::ProcessingError(format!(
                                        "ProjectProcessor control input lagged by {} messages",
                                        skipped
                                    )))
                                }
                            };
                            let _ = control_output.send(control_data.clone());
                            if control_data.is_terminal() {
                                println!("[ProjectProcessor:{id}] received StreamEnd (control)");
                                return Ok(());
                            }
                            continue;
                        } else {
                            control_active = false;
                        }
                    }
                    item = input_streams.next() => {
                        match item {
                            Some(Ok(data)) => {
                                if let Some(collection) = data.as_collection() {
                                    match apply_projection(collection, &fields) {
                                        Ok(projected_collection) => {
                                            let projected_data = StreamData::collection(projected_collection);
                                            output
                                                .send(projected_data)
                                                .map_err(|_| ProcessorError::ChannelClosed)?;
                                        }
                                        Err(e) => {
                                            let error_data = StreamData::error(
                                                StreamError::new(e.to_string()).with_source(id.clone()),
                                            );
                                            output
                                                .send(error_data)
                                                .map_err(|_| ProcessorError::ChannelClosed)?;
                                        }
                                    }
                                } else {
                                    output
                                        .send(data.clone())
                                        .map_err(|_| ProcessorError::ChannelClosed)?;
                                }
                                if let Some(control) = data.as_control() {
                                    if matches!(control, crate::processor::ControlSignal::StreamEnd)
                                    {
                                        println!("[ProjectProcessor:{id}] received StreamEnd (data)");
                                        return Ok(());
                                    }
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                return Err(ProcessorError::ProcessingError(format!(
                                    "ProjectProcessor input lagged by {} messages",
                                    skipped
                                )))
                            }
                            None => return Ok(()),
                        }
                    }
                }
            }
        })
    }

    fn subscribe_output(&self) -> Option<broadcast::Receiver<StreamData>> {
        Some(self.output.subscribe())
    }

    fn subscribe_control_output(&self) -> Option<broadcast::Receiver<StreamData>> {
        Some(self.control_output.subscribe())
    }

    fn add_input(&mut self, receiver: broadcast::Receiver<StreamData>) {
        self.inputs.push(receiver);
    }

    fn add_control_input(&mut self, receiver: broadcast::Receiver<StreamData>) {
        self.control_inputs.push(receiver);
    }
}
