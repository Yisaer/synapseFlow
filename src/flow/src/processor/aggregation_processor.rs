//! AggregationProcessor - processes aggregation operations
//!
//! This processor:
//! - Takes input data (Collection) and aggregation calls
//! - Creates accumulators for each aggregate function
//! - Updates accumulators with each incoming row
//! - Finalizes aggregates and outputs results when stream ends

use crate::aggregation::{AggregateAccumulator, AggregateFunctionRegistry};
use crate::model::Collection;
use crate::planner::physical::{AggregateCall, PhysicalAggregation, PhysicalPlan};
use crate::processor::base::{
    fan_in_control_streams, fan_in_streams, log_received_data, send_control_with_backpressure,
    send_with_backpressure, DEFAULT_CHANNEL_CAPACITY,
};
use crate::processor::{ControlSignal, Processor, ProcessorError, StreamData};
use datatypes::Value;
use futures::stream::StreamExt;
use sqlparser::ast::Expr;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

/// Group-by metadata bundled for evaluation and output decoration.
struct GroupByMeta {
    is_simple: bool,
    output_name: String,
}

/// Pre-evaluated argument columns for a single aggregate call.
///
/// Example: for two aggregates `sum(a)` and `sum(b)` over 3 rows,
/// we will have two `AggregateCallArgs`. For `sum(a)`, `arg_values` is
/// `[ [1, 2, 3] ]`; for `sum(b)`, `arg_values` is `[ [10, 20, 30] ]`.
/// Outer Vec = args per call, inner Vec = values per row for that arg.
struct AggregateCallArgs {
    call_idx: usize,
    arg_values: Vec<Vec<Value>>,
}

/// Pre-evaluated group-by expressions for the batch.
struct PreparedGroupBy {
    values_per_expr: Vec<Vec<Value>>,
    meta: Vec<GroupByMeta>,
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
    inputs: Vec<broadcast::Receiver<StreamData>>,
    /// Control input channels
    control_inputs: Vec<broadcast::Receiver<ControlSignal>>,
    /// Broadcast channel for downstream processors
    output: broadcast::Sender<StreamData>,
    /// Dedicated control output channel
    control_output: broadcast::Sender<ControlSignal>,
}

impl AggregationProcessor {
    /// Process a collection with optional grouping (group_by_scalars in PhysicalAggregation).
    fn process_batch_with_grouping(
        physical_aggregation: &PhysicalAggregation,
        aggregate_registry: &Arc<AggregateFunctionRegistry>,
        collection: &dyn Collection,
    ) -> Result<Box<dyn Collection>, String> {
        use crate::model::RecordBatch;
        use std::collections::hash_map::Entry;
        use std::collections::HashMap;

        let num_rows = collection.num_rows();

        let aggregate_args: Vec<AggregateCallArgs> =
            Self::prepare_aggregate_args(physical_aggregation, collection)?;
        let group_by = Self::prepare_group_by(physical_aggregation, collection)?;

        // Group states keyed by evaluated group-by values.
        struct GroupState {
            accumulators: Vec<Box<dyn AggregateAccumulator>>,
            last_row_idx: usize,
            key_values: Vec<Value>,
        }

        let rows = collection.rows();
        let mut groups: HashMap<String, GroupState> = HashMap::new();

        for row_idx in 0..num_rows {
            // Build group key values for this row.
            let mut key_values = Vec::with_capacity(group_by.values_per_expr.len());
            for values in &group_by.values_per_expr {
                key_values.push(values[row_idx].clone());
            }

            let key_repr = format!("{:?}", key_values);
            let entry = match groups.entry(key_repr) {
                Entry::Occupied(o) => o.into_mut(),
                Entry::Vacant(v) => {
                    let accumulators =
                        Self::create_accumulators_static(physical_aggregation, aggregate_registry)?;
                    v.insert(GroupState {
                        accumulators,
                        last_row_idx: row_idx,
                        key_values: key_values.clone(),
                    })
                }
            };

            // Update last tuple for this group to the current row.
            entry.last_row_idx = row_idx;

            // Update aggregates for this group.
            for call_args in &aggregate_args {
                let call_idx = call_args.call_idx;
                let mut row_args = Vec::new();
                for arg_values in &call_args.arg_values {
                    row_args.push(arg_values[row_idx].clone());
                }
                entry
                    .accumulators
                    .get_mut(call_idx)
                    .ok_or_else(|| "accumulator missing".to_string())?
                    .update(&row_args)?;
            }
        }

        if groups.is_empty() {
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
                &group_by.meta,
            )?;
            let collection = RecordBatch::new(vec![tuple])
                .map_err(|e| format!("Failed to create RecordBatch: {}", e))?;
            return Ok(Box::new(collection));
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
                &group_by.meta,
            )?;
            output_tuples.push(tuple);
        }

        let collection = RecordBatch::new(output_tuples)
            .map_err(|e| format!("Failed to create RecordBatch: {}", e))?;
        Ok(Box::new(collection))
    }

    /// Create a new AggregationProcessor from PhysicalAggregation
    pub fn new(
        id: impl Into<String>,
        physical_aggregation: Arc<PhysicalAggregation>,
        aggregate_registry: Arc<AggregateFunctionRegistry>,
    ) -> Self {
        let (output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        let (control_output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        Self {
            id: id.into(),
            physical_aggregation,
            aggregate_registry,
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
        }
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

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let id = self.id.clone();
        let mut input_streams = fan_in_streams(std::mem::take(&mut self.inputs));
        let control_receivers = std::mem::take(&mut self.control_inputs);
        let control_active = !control_receivers.is_empty();
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let physical_aggregation = self.physical_aggregation.clone();
        let aggregate_registry = self.aggregate_registry.clone();
        tracing::info!(processor_id = %id, "aggregation processor starting");

        tokio::spawn(async move {
            let mut control_streams = fan_in_control_streams(control_receivers);
            let mut stream_ended = false;

            loop {
                tokio::select! {
                    biased;
                    control_item = control_streams.next(), if control_active => {
                        if let Some(Ok(control_signal)) = control_item {
                            let is_terminal = control_signal.is_terminal();
                            send_control_with_backpressure(&control_output, control_signal).await?;
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
                                match data {
                                    StreamData::Collection(collection) => {
                                        match Self::process_batch_with_grouping(
                                            &physical_aggregation,
                                            &aggregate_registry,
                                            collection.as_ref(),
                                        ) {
                                            Ok(result_collection) => {
                                                let result_data =
                                                    StreamData::Collection(result_collection);
                                                if let Err(e) =
                                                    send_with_backpressure(&output, result_data)
                                                        .await
                                                {
                                                    tracing::error!(processor_id = %id, error = %e, "failed to send result");
                                                    return Err(e);
                                                }
                                                tracing::debug!(processor_id = %id, "processed batch and sent grouped results");
                                            }
                                            Err(e) => {
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
                                        send_control_with_backpressure(
                                            &control_output,
                                            control_signal,
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
                                        send_with_backpressure(&output, other_data).await?;
                                    }
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(n))) => {
                                tracing::warn!(processor_id = %id, skipped = n, "input lagged");
                            }
                            None => {
                                tracing::info!(processor_id = %id, "all input streams ended");
                                break;
                            }
                        }
                    }
                }
            }

            // Stream has ended, just forward the end signal as results were already sent for each batch
            if stream_ended {
                tracing::info!(processor_id = %id, "stream ended, forwarding end signal");

                // Send StreamGracefulEnd signal downstream
                send_control_with_backpressure(&control_output, ControlSignal::StreamGracefulEnd)
                    .await?;
                tracing::info!(processor_id = %id, "aggregation processing completed");
            }

            tracing::info!(processor_id = %id, "stopped");
            Ok(())
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

    /// Static version of evaluate_arguments for use in async context
    fn evaluate_arguments_static(
        call: &AggregateCall,
        collection: &dyn Collection,
    ) -> Result<Vec<Vec<Value>>, String> {
        let mut all_args = Vec::new();

        // Evaluate each argument expression for all rows
        for arg_expr in &call.args {
            let arg_values = arg_expr
                .eval_with_collection(collection)
                .map_err(|e| format!("Failed to evaluate argument: {}", e))?;
            all_args.push(arg_values);
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
        use crate::model::AffiliateRow;
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
        let mut affiliate = tuple
            .affiliate
            .take()
            .unwrap_or_else(|| AffiliateRow::new(Vec::new()));
        for (k, v) in affiliate_entries {
            affiliate.insert(k, v);
        }
        tuple.affiliate = Some(affiliate);
        Ok(tuple)
    }
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
    fn prepare_aggregate_args(
        physical_aggregation: &PhysicalAggregation,
        collection: &dyn Collection,
    ) -> Result<Vec<AggregateCallArgs>, String> {
        let mut all_call_args = Vec::new();
        for (idx, call) in physical_aggregation.aggregate_calls.iter().enumerate() {
            let args = Self::evaluate_arguments_static(call, collection)?;
            all_call_args.push(AggregateCallArgs {
                call_idx: idx,
                arg_values: args,
            });
        }
        Ok(all_call_args)
    }

    fn prepare_group_by(
        physical_aggregation: &PhysicalAggregation,
        collection: &dyn Collection,
    ) -> Result<PreparedGroupBy, String> {
        let mut values_per_expr = Vec::new();
        for scalar in &physical_aggregation.group_by_scalars {
            let values = scalar
                .eval_with_collection(collection)
                .map_err(|e| format!("Failed to evaluate group-by expression: {}", e))?;
            values_per_expr.push(values);
        }

        let meta = build_group_by_meta(&physical_aggregation.group_by_exprs);

        Ok(PreparedGroupBy {
            values_per_expr,
            meta,
        })
    }
}
