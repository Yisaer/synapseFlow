//! AggregationProcessor - processes aggregation operations
//!
//! This processor:
//! - Takes input data (Collection) and aggregation calls
//! - Creates accumulators for each aggregate function
//! - Updates accumulators with each incoming row
//! - Finalizes aggregates and outputs results when stream ends

use crate::aggregation::{AggregateAccumulator, AggregateFunctionRegistry, AggregateUpdate};
use crate::model::Collection;
use crate::planner::physical::{PhysicalAggregation, PhysicalPlan};
use crate::processor::base::{
    default_channel_capacities, fan_in_control_streams, fan_in_streams, log_broadcast_lagged,
    log_received_data, send_control_with_backpressure, send_with_backpressure, LinkOutput,
    LinkReceiver, ProcessorChannelCapacities,
};
use crate::processor::{
    ControlSignal, Processor, ProcessorError, ProcessorStart, ProcessorStats, StreamData,
};
use crate::runtime::TaskSpawner;
use datatypes::Value;
use futures::stream::StreamExt;
use sqlparser::ast::Expr;
use std::sync::Arc;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

/// Group-by metadata bundled for evaluation and output decoration.
struct GroupByMeta {
    is_simple: bool,
    output_name: String,
}

/// AggregationProcessor - evaluates aggregation expressions
///
/// This processor:
/// - Takes input data (Collection) and aggregation configuration
/// - Creates accumulators for each aggregate call
/// - Updates accumulators with each incoming row
/// - Finalizes aggregates and outputs results when stream ends
pub struct AggregationProcessor {
    /// Processor identifier
    id: String,
    /// Physical aggregation configuration
    physical_aggregation: Arc<PhysicalAggregation>,
    /// Aggregate function registry for creating accumulators
    aggregate_registry: Arc<AggregateFunctionRegistry>,
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

impl AggregationProcessor {
    /// Process a collection with optional grouping (group_by_scalars in PhysicalAggregation).
    fn process_batch_with_grouping(
        physical_aggregation: &PhysicalAggregation,
        aggregate_registry: &Arc<AggregateFunctionRegistry>,
        collection: &dyn Collection,
        stats: &ProcessorStats,
    ) -> Result<Option<Box<dyn Collection>>, String> {
        use crate::model::RecordBatch;
        use std::collections::hash_map::Entry;
        use std::collections::HashMap;

        let num_rows = collection.num_rows();
        let group_by_meta = build_group_by_meta(&physical_aggregation.group_by_exprs);

        // Group states keyed by evaluated group-by values.
        struct GroupState {
            accumulators: Vec<Box<dyn AggregateAccumulator>>,
            last_row_idx: usize,
            key_values: Vec<Value>,
        }

        let rows = collection.rows();
        let mut groups: HashMap<String, GroupState> = HashMap::new();

        for (row_idx, row) in rows.iter().enumerate() {
            let key_values = match Self::evaluate_group_by_for_tuple(physical_aggregation, row) {
                Ok(values) => values,
                Err(message) => {
                    stats.record_error_logged("aggregation processor error", message);
                    continue;
                }
            };
            let row_call_args =
                match Self::evaluate_aggregate_args_for_tuple(physical_aggregation, row) {
                    Ok(args) => args,
                    Err(message) => {
                        stats.record_error_logged("aggregation processor error", message);
                        continue;
                    }
                };

            let key_repr = format!("{:?}", key_values);
            match groups.entry(key_repr) {
                Entry::Occupied(mut o) => {
                    let entry = o.get_mut();
                    if let Err(message) =
                        apply_aggregate_updates(&mut entry.accumulators, &row_call_args)
                    {
                        stats.record_error_logged("aggregation processor error", message);
                        continue;
                    }
                    entry.last_row_idx = row_idx;
                    entry.key_values = key_values;
                }
                Entry::Vacant(v) => {
                    let accumulators =
                        Self::create_accumulators_static(physical_aggregation, aggregate_registry)?;
                    let mut state = GroupState {
                        accumulators,
                        last_row_idx: row_idx,
                        key_values: key_values.clone(),
                    };
                    if let Err(message) =
                        apply_aggregate_updates(&mut state.accumulators, &row_call_args)
                    {
                        stats.record_error_logged("aggregation processor error", message);
                        continue;
                    }
                    v.insert(state);
                }
            }
        }

        if num_rows == 0 {
            // No rows: fall back to single-group finalize (existing behavior).
            let mut accumulators =
                Self::create_accumulators_static(physical_aggregation, aggregate_registry)?;
            // No updates needed as there are no rows.
            let tuple = Self::finalize_group(
                physical_aggregation,
                &mut accumulators,
                None,
                rows,
                &[],
                &group_by_meta,
            )?;
            let collection = RecordBatch::new_with_metadata_from(vec![tuple], collection)
                .map_err(|e| format!("Failed to create RecordBatch: {}", e))?;
            return Ok(Some(Box::new(collection)));
        }

        if groups.is_empty() {
            return Ok(None);
        }

        // Finalize each group into output tuples.
        let mut output_tuples = Vec::with_capacity(groups.len());
        for (_key, mut state) in groups.into_iter() {
            let tuple = Self::finalize_group(
                physical_aggregation,
                &mut state.accumulators,
                Some(state.last_row_idx),
                rows,
                &state.key_values,
                &group_by_meta,
            )?;
            output_tuples.push(tuple);
        }

        let collection = RecordBatch::new_with_metadata_from(output_tuples, collection)
            .map_err(|e| format!("Failed to create RecordBatch: {}", e))?;
        Ok(Some(Box::new(collection)))
    }

    /// Create a new AggregationProcessor from PhysicalAggregation
    pub fn new(
        id: impl Into<String>,
        physical_aggregation: Arc<PhysicalAggregation>,
        aggregate_registry: Arc<AggregateFunctionRegistry>,
    ) -> Self {
        Self::new_with_channel_capacities(
            id,
            physical_aggregation,
            aggregate_registry,
            default_channel_capacities(),
        )
    }

    pub(crate) fn new_with_channel_capacities(
        id: impl Into<String>,
        physical_aggregation: Arc<PhysicalAggregation>,
        aggregate_registry: Arc<AggregateFunctionRegistry>,
        channel_capacities: ProcessorChannelCapacities,
    ) -> Self {
        let output = LinkOutput::new(channel_capacities.data_link_kind, channel_capacities.data);
        let control_output = LinkOutput::new(
            channel_capacities.control_link_kind,
            channel_capacities.control,
        );
        Self {
            id: id.into(),
            physical_aggregation,
            aggregate_registry,
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

    /// Create an AggregationProcessor from a PhysicalPlan
    /// Returns None if the plan is not a PhysicalAggregation
    pub fn from_physical_plan(
        id: impl Into<String>,
        plan: Arc<PhysicalPlan>,
        aggregate_registry: Arc<AggregateFunctionRegistry>,
    ) -> Option<Self> {
        match plan.as_ref() {
            PhysicalPlan::Aggregation(aggregation) => Some(Self::new(
                id,
                Arc::new(aggregation.clone()),
                aggregate_registry,
            )),
            _ => None,
        }
    }
}

impl Processor for AggregationProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    fn start(&mut self, spawner: &TaskSpawner) -> ProcessorStart {
        let id = self.id.clone();
        let mut input_streams = fan_in_streams(std::mem::take(&mut self.inputs));
        let control_receivers = std::mem::take(&mut self.control_inputs);
        let control_active = !control_receivers.is_empty();
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let channel_capacities = self.channel_capacities;
        let physical_aggregation = self.physical_aggregation.clone();
        let aggregate_registry = self.aggregate_registry.clone();
        let stats = Arc::clone(&self.stats);
        tracing::info!(processor_id = %id, "aggregation processor starting");

        ProcessorStart::ready(spawner.spawn(async move {
            let mut control_streams = fan_in_control_streams(control_receivers);
            let mut stream_ended = false;

            loop {
                tokio::select! {
                    biased;
                    control_item = control_streams.next(), if control_active => {
                        if let Some(Ok(control_signal)) = control_item {
                            let is_terminal = control_signal.is_terminal();
                            send_control_with_backpressure(
                                &control_output,
                                channel_capacities.control,
                                control_signal,
                            )
                            .await?;
                            if is_terminal {
                                tracing::info!(processor_id = %id, "received StreamEnd (control)");
                                stream_ended = true;
                                break;
                            }
                        }
                    }
                    data_item = input_streams.next() => {
                        match data_item {
                            Some(Ok(data)) => {
                                log_received_data(&id, &data);
                                if let Some(rows) = data.num_rows_hint() {
                                    stats.record_in(rows);
                                }
                                match data {
                                    StreamData::Collection(collection) => {
                                        let handle_start = std::time::Instant::now();
                                        match Self::process_batch_with_grouping(
                                            &physical_aggregation,
                                            &aggregate_registry,
                                            collection.as_ref(),
                                            stats.as_ref(),
                                        ) {
                                            Ok(Some(result_collection)) => {
                                                let result_data = StreamData::Collection(result_collection);
                                                let out_rows = result_data.num_rows_hint();
                                                let send_res = send_with_backpressure(
                                                    &output,
                                                    channel_capacities.data,
                                                    result_data,
                                                    Some(stats.as_ref()),
                                                )
                                                .await;
                                                // For synchronous processors, handle duration includes downstream send/backpressure time.
                                                stats.record_handle_duration(handle_start.elapsed());
                                                if let Err(e) = send_res {
                                                    tracing::error!(processor_id = %id, error = %e, "failed to send result");
                                                    return Err(e);
                                                };
                                                if let Some(rows) = out_rows {
                                                    stats.record_out(rows);
                                                }
                                                tracing::debug!(processor_id = %id, "processed batch and sent grouped results");
                                            }
                                            Ok(None) => {
                                                stats.record_handle_duration(handle_start.elapsed());
                                            }
                                            Err(e) => {
                                                stats.record_handle_duration(handle_start.elapsed());
                                                return Err(ProcessorError::ProcessingError(
                                                    format!(
                                                        "Failed to process aggregation: {}",
                                                        e
                                                    ),
                                                ));
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
                                            stream_ended = true;
                                            break;
                                        }
                                    }
                                    other_data => {
                                        // Forward non-collection data (like Encoded, Bytes) as-is
                                        send_with_backpressure(
                                            &output,
                                            channel_capacities.data,
                                            other_data,
                                            Some(stats.as_ref()),
                                        )
                                        .await?;
                                    }
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(n))) => {
                                log_broadcast_lagged(&id, n, "data input");
                            }
                            None => {
                                tracing::info!(processor_id = %id, "all input streams ended");
                                break;
                            }
                        }
                    }
                }
            }

            if stream_ended {
                tracing::info!(processor_id = %id, "aggregation processing completed");
            }

            tracing::info!(processor_id = %id, "stopped");
            Ok(())
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

impl AggregationProcessor {
    /// Static version of create_accumulators for use in async context
    fn create_accumulators_static(
        physical_aggregation: &PhysicalAggregation,
        aggregate_registry: &Arc<AggregateFunctionRegistry>,
    ) -> Result<Vec<Box<dyn AggregateAccumulator>>, String> {
        let mut accumulators = Vec::new();

        for call in &physical_aggregation.aggregate_calls {
            if call.distinct {
                return Err("DISTINCT aggregates are not supported yet".to_string());
            }

            let function = aggregate_registry.get(&call.func_name).ok_or_else(|| {
                format!(
                    "Aggregate function '{}' not found in registry",
                    call.func_name
                )
            })?;

            let accumulator = function.create_accumulator();
            accumulators.push(accumulator);
        }

        Ok(accumulators)
    }

    fn evaluate_aggregate_args_for_tuple(
        physical_aggregation: &PhysicalAggregation,
        tuple: &crate::model::Tuple,
    ) -> Result<Vec<(usize, Vec<Value>)>, String> {
        let mut all_args = Vec::with_capacity(physical_aggregation.aggregate_calls.len());
        for (idx, call) in physical_aggregation.aggregate_calls.iter().enumerate() {
            let mut args = Vec::with_capacity(call.args.len());
            for arg_expr in &call.args {
                args.push(
                    arg_expr
                        .eval_with_tuple(tuple)
                        .map_err(|e| format!("Failed to evaluate aggregate argument: {}", e))?,
                );
            }
            all_args.push((idx, args));
        }
        Ok(all_args)
    }

    fn finalize_group(
        physical_aggregation: &PhysicalAggregation,
        accumulators: &mut [Box<dyn AggregateAccumulator>],
        last_row_idx: Option<usize>,
        all_rows: &[crate::model::Tuple],
        key_values: &[Value],
        group_by_meta: &[GroupByMeta],
    ) -> Result<crate::model::Tuple, String> {
        use std::sync::Arc;

        // Finalize aggregate values.
        let mut affiliate_entries = Vec::new();
        for (call, accumulator) in physical_aggregation
            .aggregate_calls
            .iter()
            .zip(accumulators.iter_mut())
        {
            affiliate_entries.push((Arc::new(call.output_column.clone()), accumulator.finalize()));
        }

        // Add computed group-by keys (non-simple column refs) to affiliate so downstream can access them.
        for (idx, value) in key_values.iter().enumerate() {
            if let Some(meta) = group_by_meta.get(idx) {
                if !meta.is_simple {
                    affiliate_entries.push((Arc::new(meta.output_name.clone()), value.clone()));
                }
            }
        }

        let mut tuple = match last_row_idx {
            Some(idx) => {
                let anchor = all_rows
                    .get(idx)
                    .ok_or_else(|| format!("row index {} out of bounds", idx))?;
                crate::model::Tuple::with_timestamp(anchor.messages.clone(), anchor.timestamp)
            }
            None => crate::model::Tuple::new(vec![]),
        };
        tuple.add_affiliate_columns(affiliate_entries);
        Ok(tuple)
    }
}

fn apply_aggregate_updates(
    accumulators: &mut [Box<dyn AggregateAccumulator>],
    row_call_args: &[(usize, Vec<Value>)],
) -> Result<(), String> {
    let mut prepared = Vec::<(usize, Box<dyn AggregateUpdate>)>::with_capacity(row_call_args.len());
    for (idx, args) in row_call_args {
        let update = accumulators
            .get(*idx)
            .ok_or_else(|| "accumulator missing".to_string())?
            .prepare_update(args)?;
        prepared.push((*idx, update));
    }

    for (idx, update) in prepared {
        accumulators
            .get_mut(idx)
            .ok_or_else(|| "accumulator missing".to_string())?
            .commit_update(update);
    }

    Ok(())
}

fn is_simple_column_expr(expr: &Expr) -> bool {
    matches!(expr, Expr::Identifier(_))
}

fn build_group_by_meta(exprs: &[Expr]) -> Vec<GroupByMeta> {
    exprs
        .iter()
        .map(|expr| GroupByMeta {
            is_simple: is_simple_column_expr(expr),
            output_name: expr.to_string(),
        })
        .collect()
}

impl AggregationProcessor {
    fn evaluate_group_by_for_tuple(
        physical_aggregation: &PhysicalAggregation,
        tuple: &crate::model::Tuple,
    ) -> Result<Vec<Value>, String> {
        let mut values = Vec::with_capacity(physical_aggregation.group_by_scalars.len());
        for scalar in &physical_aggregation.group_by_scalars {
            values.push(
                scalar
                    .eval_with_tuple(tuple)
                    .map_err(|e| format!("Failed to evaluate group-by expression: {}", e))?,
            );
        }
        Ok(values)
    }
}
