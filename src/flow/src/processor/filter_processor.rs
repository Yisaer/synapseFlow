//! FilterProcessor - processes filter operations
//!
//! This processor evaluates filter expressions and produces output with filtered records.

use crate::expr::ScalarExpr;
use crate::model::Collection;
use crate::model::RecordBatch;
use crate::planner::physical::{PhysicalFilter, PhysicalPlan, PipelineStateUsage};
use crate::processor::base::{
    default_channel_capacities, fan_in_control_streams, fan_in_streams, log_broadcast_lagged,
    log_received_data, send_control_with_backpressure, send_with_backpressure, LinkOutput,
    LinkReceiver, ProcessorChannelCapacities,
};
use crate::processor::pipeline_state_runtime::{update_collection_hit_state, update_row_hit_state};
use crate::processor::processor_state::ProcessorState;
use crate::processor::{
    ControlSignal, Processor, ProcessorError, ProcessorStart, ProcessorStats, StreamData,
};
use crate::runtime::TaskSpawner;
use datatypes::Value;
use futures::stream::StreamExt;
use std::sync::Arc;
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
    /// Processor-local state for pipeline state functions (e.g. last_hit_count).
    pub(crate) processor_state: Option<Arc<ProcessorState>>,
    pub(crate) pipeline_state_usage: PipelineStateUsage,
    /// Input channels for receiving data
    inputs: Vec<LinkReceiver<StreamData>>,
    /// Control input channels
    control_inputs: Vec<LinkReceiver<ControlSignal>>,
    /// Broadcast channel for downstream processors
    output: LinkOutput<StreamData>,
    /// Dedicated control output channel
    control_output: LinkOutput<ControlSignal>,
    channel_capacities: ProcessorChannelCapacities,
    stats: Arc<ProcessorStats>,
}

impl FilterProcessor {
    /// Create a new FilterProcessor from PhysicalFilter
    pub fn new(id: impl Into<String>, physical_filter: Arc<PhysicalFilter>) -> Self {
        Self::new_with_channel_capacities(id, physical_filter, default_channel_capacities())
    }

    pub(crate) fn new_with_channel_capacities(
        id: impl Into<String>,
        physical_filter: Arc<PhysicalFilter>,
        channel_capacities: ProcessorChannelCapacities,
    ) -> Self {
        let output = LinkOutput::new(channel_capacities.data_link_kind, channel_capacities.data);
        let control_output = LinkOutput::new(
            channel_capacities.control_link_kind,
            channel_capacities.control,
        );
        Self {
            id: id.into(),
            processor_state: physical_filter.processor_state.clone(),
            pipeline_state_usage: physical_filter.pipeline_state_usage,
            physical_filter,
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
            channel_capacities,
            stats: Arc::new(ProcessorStats::default()),
        }
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        self.stats = stats;
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

/// Apply filter to a collection, optionally tracking pipeline state updates.
///
/// `last_hit_count()` is row-scoped and increments after each accepted row.
/// `last_hit_time_unix_ms()` is row-scoped and updates after each accepted row.
/// `last_agg_hit_count()` is collection-scoped and increments once after a
/// non-empty filtered collection is produced.
fn apply_filter(
    input_collection: &dyn Collection,
    filter_expr: &ScalarExpr,
    state: Option<&ProcessorState>,
    state_usage: PipelineStateUsage,
) -> Result<Box<dyn Collection>, ProcessorError> {
    match (state, !state_usage.is_empty()) {
        (Some(state), true) => {
            let mut kept = Vec::with_capacity(input_collection.num_rows());
            for tuple in input_collection.rows() {
                let result = filter_expr
                    .eval_with_tuple(tuple)
                    .map_err(|e| ProcessorError::ProcessingError(e.to_string()))?;
                if matches!(result, Value::Bool(true)) {
                    update_row_hit_state(state, state_usage, tuple.timestamp)?;
                    kept.push(tuple.clone());
                }
            }
            if !kept.is_empty() {
                update_collection_hit_state(state, state_usage);
            }
            Ok(Box::new(
                RecordBatch::new_with_metadata_from(kept, input_collection)
                    .map_err(|e| ProcessorError::ProcessingError(e.to_string()))?,
            ))
        }
        _ => input_collection
            .apply_filter(filter_expr)
            .map_err(|e| ProcessorError::ProcessingError(format!("Failed to apply filter: {}", e))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::func::BinaryFunc;
    use crate::expr::ProcStateField;
    use crate::model::Tuple;
    use datatypes::{ConcreteDatatype, Int64Type};
    use std::sync::atomic::Ordering;
    use std::time::{Duration, UNIX_EPOCH};

    fn tuple_at_ms(ms: u64) -> Tuple {
        Tuple::with_timestamp(
            Tuple::empty_messages(),
            UNIX_EPOCH + Duration::from_millis(ms),
        )
    }

    #[test]
    fn filter_updates_last_hit_time_after_accepted_rows() {
        let state = Arc::new(ProcessorState::new());
        let input = RecordBatch::new(vec![
            tuple_at_ms(1000),
            tuple_at_ms(2000),
            tuple_at_ms(3000),
        ])
        .expect("record batch");
        let expr = ScalarExpr::CallBinary {
            func: BinaryFunc::Lt,
            expr1: Box::new(ScalarExpr::ProcessorState {
                state: Arc::clone(&state),
                field: ProcStateField::LastHitTimeUnixMs,
            }),
            expr2: Box::new(ScalarExpr::Literal(
                Value::Int64(1500),
                ConcreteDatatype::Int64(Int64Type),
            )),
        };
        let usage = PipelineStateUsage {
            last_hit_time_unix_ms: true,
            ..PipelineStateUsage::default()
        };

        let output = apply_filter(&input, &expr, Some(state.as_ref()), usage)
            .expect("filter should succeed");

        assert_eq!(output.num_rows(), 2);
        assert_eq!(state.last_hit_time_unix_ms.load(Ordering::Relaxed), 2000);
    }
}

impl Processor for FilterProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    fn start(&mut self, spawner: &TaskSpawner) -> ProcessorStart {
        let id = self.id.clone();
        let data_receivers = std::mem::take(&mut self.inputs);
        let mut input_streams = fan_in_streams(data_receivers);

        let control_receivers = std::mem::take(&mut self.control_inputs);
        let mut control_streams = fan_in_control_streams(control_receivers);
        let control_active = !control_streams.is_empty();

        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let channel_capacities = self.channel_capacities;
        let filter_expr = self.physical_filter.scalar_predicate.clone();
        let state_usage = self.pipeline_state_usage;
        let processor_state = self.processor_state.clone();
        let stats = Arc::clone(&self.stats);
        tracing::info!(processor_id = %id, "filter processor starting");

        ProcessorStart::ready(spawner.spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    control_item = control_streams.next(), if control_active => {
                        match control_item {
                            Some(Ok(control_signal)) => {
                                let is_terminal = control_signal.is_terminal();
                                send_control_with_backpressure(
                                    &control_output,
                                    channel_capacities.control,
                                    control_signal,
                                )
                                .await?;
                                if is_terminal {
                                    tracing::info!(processor_id = %id, "received StreamEnd (control)");
                                    tracing::info!(processor_id = %id, "stopped");
                                    return Ok(());
                                }
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
                                if let Some(rows) = data.num_rows_hint() {
                                    stats.record_in(rows);
                                }
                                match data {
                                    StreamData::Collection(collection) => {
                                        let handle_start = std::time::Instant::now();
                                        let result = apply_filter(
                                            collection.as_ref(),
                                            &filter_expr,
                                            processor_state.as_deref(),
                                            state_usage,
                                        );
                                        match result {
                                            Ok(filtered_collection) => {
                                                let out_rows = filtered_collection.num_rows();
                                                // A filter that matches nothing emits nothing
                                                // (standard WHERE semantics, like eKuiper): skip
                                                // broadcasting the empty collection so downstream
                                                // stages (project, suppress, sink) and their
                                                // broadcast channels don't run for it. For
                                                // event-detection rules the predicate rejects
                                                // almost every row, so forwarding empties was the
                                                // dominant per-row broadcast cost. Watermarks,
                                                // control, and the graceful-end barrier ride other
                                                // StreamData arms and still propagate, so graceful
                                                // close is unaffected.
                                                if out_rows == 0 {
                                                    stats.record_handle_duration(handle_start.elapsed());
                                                } else {
                                                    let filtered_data =
                                                        StreamData::collection(filtered_collection);
                                                    let send_res = send_with_backpressure(
                                                        &output,
                                                        channel_capacities.data,
                                                        filtered_data,
                                                        Some(stats.as_ref()),
                                                    )
                                                    .await;
                                                    // Handle duration includes downstream send/backpressure time.
                                                    stats.record_handle_duration(handle_start.elapsed());
                                                    send_res?;
                                                    stats.record_out(out_rows as u64);
                                                }
                                            }
                                            Err(e) => {
                                                stats.record_handle_duration(handle_start.elapsed());
                                                stats.record_error_logged("filter processor error", e.to_string());
                                            }
                                        }
                                    }
                                    StreamData::Control(control_signal) => {
                                        let is_terminal = control_signal.is_terminal();
                                        let out = StreamData::control(control_signal);
                                        send_with_backpressure(
                                            &output,
                                            channel_capacities.data,
                                            out,
                                            Some(stats.as_ref()),
                                        )
                                        .await?;
                                        if is_terminal {
                                            tracing::info!(processor_id = %id, "received StreamEnd (data)");
                                            tracing::info!(processor_id = %id, "stopped");
                                            return Ok(());
                                        }
                                    }
                                    other => {
                                        let is_terminal = other.is_terminal();
                                        send_with_backpressure(
                                            &output,
                                            channel_capacities.data,
                                            other,
                                            Some(stats.as_ref()),
                                        )
                                        .await?;
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
        }))
    }

    fn subscribe_output(&self) -> Option<LinkReceiver<StreamData>> {
        self.output.subscribe()
    }

    fn subscribe_control_output(&self) -> Option<LinkReceiver<ControlSignal>> {
        self.control_output.subscribe()
    }

    fn add_input<R>(&mut self, receiver: R)
    where
        R: Into<LinkReceiver<StreamData>>,
    {
        self.inputs.push(receiver.into());
    }

    fn add_control_input<R>(&mut self, receiver: R)
    where
        R: Into<LinkReceiver<ControlSignal>>,
    {
        self.control_inputs.push(receiver.into());
    }
}
