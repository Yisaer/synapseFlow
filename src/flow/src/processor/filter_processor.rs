//! FilterProcessor - processes filter operations
//!
//! This processor evaluates filter expressions and produces output with filtered records.

use crate::model::Collection;
use crate::planner::physical::{PhysicalFilter, PhysicalPlan};
use crate::processor::barrier::{align_control_signal, BarrierAligner};
use crate::processor::base::{
    fan_in_control_streams, fan_in_streams, log_broadcast_lagged, log_received_data,
    send_control_with_backpressure, send_with_backpressure, DEFAULT_CHANNEL_CAPACITY,
};
use crate::processor::{ControlSignal, Processor, ProcessorError, StreamData, StreamError};
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
    control_inputs: Vec<broadcast::Receiver<ControlSignal>>,
    /// Broadcast channel for downstream processors
    output: broadcast::Sender<StreamData>,
    /// Dedicated control output channel
    control_output: broadcast::Sender<ControlSignal>,
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
    pub fn from_physical_plan(id: impl Into<String>, plan: Arc<PhysicalPlan>) -> Option<Self> {
        match plan.as_ref() {
            PhysicalPlan::Filter(filter) => Some(Self::new(id, Arc::new(filter.clone()))),
            _ => None,
        }
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
        let data_receivers = std::mem::take(&mut self.inputs);
        let expected_data_upstreams = data_receivers.len();
        let mut input_streams = fan_in_streams(data_receivers);

        let control_receivers = std::mem::take(&mut self.control_inputs);
        let expected_control_upstreams = control_receivers.len();
        let mut control_streams = fan_in_control_streams(control_receivers);
        let control_active = !control_streams.is_empty();

        let mut data_barrier = BarrierAligner::new("data", expected_data_upstreams);
        let mut control_barrier = BarrierAligner::new("control", expected_control_upstreams);

        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let filter_expr = self.physical_filter.scalar_predicate.clone();
        tracing::info!(processor_id = %id, "filter processor starting");

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    control_item = control_streams.next(), if control_active => {
                        match control_item {
                            Some(Ok(control_signal)) => {
                                if let Some(signal) =
                                    align_control_signal(&mut control_barrier, control_signal)?
                                {
                                    let is_terminal = signal.is_terminal();
                                    send_control_with_backpressure(&control_output, signal).await?;
                                    if is_terminal {
                                        tracing::info!(processor_id = %id, "received StreamEnd (control)");
                                        tracing::info!(processor_id = %id, "stopped");
                                        return Ok(());
                                    }
                                }
                                continue;
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                log_broadcast_lagged(&id, skipped, "filter control input");
                                continue;
                            }
                            None => {
                                return Err(ProcessorError::ChannelClosed);
                            }
                        }
                    }
                    item = input_streams.next() => {
                        match item {
                            Some(Ok(data)) => {
                                log_received_data(&id, &data);
                                match data {
                                    StreamData::Collection(collection) => {
                                        match apply_filter(collection.as_ref(), &filter_expr) {
                                            Ok(filtered_collection) => {
                                                let filtered_data =
                                                    StreamData::collection(filtered_collection);
                                                send_with_backpressure(&output, filtered_data)
                                                    .await?;
                                            }
                                            Err(e) => {
                                                let error = StreamError::new(e.to_string())
                                                    .with_source(id.clone());
                                                send_with_backpressure(
                                                    &output,
                                                    StreamData::error(error),
                                                )
                                                .await?;
                                            }
                                        }
                                    }
                                    StreamData::Control(control_signal) => {
                                        if let Some(signal) =
                                            align_control_signal(&mut data_barrier, control_signal)?
                                        {
                                            let is_terminal = signal.is_terminal();
                                            send_with_backpressure(
                                                &output,
                                                StreamData::control(signal),
                                            )
                                            .await?;
                                            if is_terminal {
                                                tracing::info!(processor_id = %id, "received StreamEnd (data)");
                                                tracing::info!(processor_id = %id, "stopped");
                                                return Ok(());
                                            }
                                        }
                                    }
                                    other => {
                                        let is_terminal = other.is_terminal();
                                        send_with_backpressure(&output, other).await?;
                                        if is_terminal {
                                            tracing::info!(processor_id = %id, "received StreamEnd (data)");
                                            tracing::info!(processor_id = %id, "stopped");
                                            return Ok(());
                                        }
                                    }
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                log_broadcast_lagged(&id, skipped, "filter data input");
                                continue;
                            }
                            None => return Err(ProcessorError::ChannelClosed),
                        }
                    }
                }
            }
        })
    }

    fn subscribe_output(&self) -> Option<broadcast::Receiver<StreamData>> {
        Some(self.output.subscribe())
    }

    fn subscribe_control_output(&self) -> Option<broadcast::Receiver<ControlSignal>> {
        Some(self.control_output.subscribe())
    }

    fn add_input(&mut self, receiver: broadcast::Receiver<StreamData>) {
        self.inputs.push(receiver);
    }

    fn add_control_input(&mut self, receiver: broadcast::Receiver<ControlSignal>) {
        self.control_inputs.push(receiver);
    }
}
