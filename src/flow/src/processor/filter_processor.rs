//! FilterProcessor - processes filter operations
//!
//! This processor evaluates filter expressions and produces output with filtered records.

use crate::model::Collection;
use crate::planner::physical::PhysicalFilter;
use crate::processor::base::{fan_in_streams, DEFAULT_CHANNEL_CAPACITY};
use crate::processor::{Processor, ProcessorError, StreamData, StreamError};
use futures::stream::StreamExt;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

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
    inputs: Vec<broadcast::Receiver<StreamData>>,
    /// Control input channels
    control_inputs: Vec<broadcast::Receiver<StreamData>>,
    /// Broadcast channel for downstream processors
    output: broadcast::Sender<StreamData>,
    /// Dedicated control output channel
    control_output: broadcast::Sender<StreamData>,
}

impl FilterProcessor {
    /// Create a new FilterProcessor from PhysicalFilter
    pub fn new(id: impl Into<String>, physical_filter: Arc<PhysicalFilter>) -> Self {
        let (output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        let (control_output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        Self {
            id: id.into(),
            physical_filter,
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
        }
    }

    /// Create a FilterProcessor from a PhysicalPlan
    /// Returns None if the plan is not a PhysicalFilter
    pub fn from_physical_plan(
        id: impl Into<String>,
        plan: Arc<dyn crate::planner::physical::PhysicalPlan>,
    ) -> Option<Self> {
        plan.as_any()
            .downcast_ref::<PhysicalFilter>()
            .map(|filter| Self::new(id, Arc::new(filter.clone())))
    }
}

/// Apply filter to a collection
fn apply_filter(
    input_collection: &dyn Collection,
    filter_expr: &crate::expr::ScalarExpr,
) -> Result<Box<dyn Collection>, ProcessorError> {
    // Use the collection's apply_filter method
    input_collection
        .apply_filter(filter_expr)
        .map_err(|e| ProcessorError::ProcessingError(format!("Failed to apply filter: {}", e)))
}

impl Processor for FilterProcessor {
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
        let filter_expr = self.physical_filter.scalar_predicate.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    control_item = control_streams.next(), if control_active => {
                        if let Some(result) = control_item {
                            let control_data = match result {
                                Ok(data) => data,
                                Err(BroadcastStreamRecvError::Lagged(skipped)) => {
                                    return Err(ProcessorError::ProcessingError(format!(
                                        "FilterProcessor control input lagged by {} messages",
                                        skipped
                                    )))
                                }
                            };
                            let _ = control_output.send(control_data.clone());
                            if control_data.is_terminal() {
                                println!("[FilterProcessor:{id}] received StreamEnd (control)");
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
                                    match apply_filter(collection, &filter_expr) {
                                        Ok(filtered_collection) => {
                                            let filtered_data = StreamData::collection(filtered_collection);
                                            output
                                                .send(filtered_data)
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
                                        println!("[FilterProcessor:{id}] received StreamEnd (data)");
                                        return Ok(());
                                    }
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                return Err(ProcessorError::ProcessingError(format!(
                                    "FilterProcessor input lagged by {} messages",
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
