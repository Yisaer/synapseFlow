use crate::aggregation::AggregateFunctionRegistry;
use crate::planner::physical::PhysicalStreamingAggregation;
use crate::processor::{ControlSignal, Processor, ProcessorError, StreamData};
use std::sync::Arc;
use tokio::sync::broadcast;

/// Placeholder implementation for sliding streaming aggregation.
///
/// The physical planner and optimizer can produce a sliding `PhysicalStreamingAggregation`,
/// but runtime execution will be implemented as part of step 3 (processor support).
pub struct StreamingSlidingAggregationProcessor {
    id: String,
    _physical: Arc<PhysicalStreamingAggregation>,
    _aggregate_registry: Arc<AggregateFunctionRegistry>,
}

impl StreamingSlidingAggregationProcessor {
    pub fn new(
        id: impl Into<String>,
        physical: Arc<PhysicalStreamingAggregation>,
        aggregate_registry: Arc<AggregateFunctionRegistry>,
    ) -> Self {
        Self {
            id: id.into(),
            _physical: physical,
            _aggregate_registry: aggregate_registry,
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }
}

impl Processor for StreamingSlidingAggregationProcessor {
    fn id(&self) -> &str {
        self.id()
    }

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let id = self.id.clone();
        tokio::spawn(async move {
            Err(ProcessorError::InvalidConfiguration(format!(
                "Sliding streaming aggregation processor not implemented yet: {id}"
            )))
        })
    }

    fn subscribe_output(&self) -> Option<broadcast::Receiver<StreamData>> {
        None
    }

    fn subscribe_control_output(&self) -> Option<broadcast::Receiver<ControlSignal>> {
        None
    }

    fn add_input(&mut self, _receiver: broadcast::Receiver<StreamData>) {}

    fn add_control_input(&mut self, _receiver: broadcast::Receiver<ControlSignal>) {}
}
