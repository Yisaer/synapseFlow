//! StreamingAggregationProcessor - incremental aggregation with windowing.

use crate::aggregation::{AggregateAccumulator, AggregateFunctionRegistry, AggregateUpdate};
use crate::expr::ScalarExpr;
use crate::model::{Collection, RecordBatch};
use crate::planner::physical::{
    AggregateCall, PhysicalPlan, PhysicalStreamingAggregation, StreamingWindowSpec,
};
use crate::processor::base::{
    default_channel_capacities, LinkReceiver, ProcessorChannelCapacities,
};
use crate::processor::{Processor, ProcessorError, ProcessorStart, ProcessorStats};
use crate::runtime::TaskSpawner;
use datatypes::Value;
use sqlparser::ast::Expr;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

#[path = "streaming_count_aggregation_processor.rs"]
mod streaming_count_aggregation_processor;
#[path = "streaming_eos_aggregation_processor.rs"]
mod streaming_eos_aggregation_processor;
#[path = "streaming_tumbling_aggregation_processor.rs"]
mod streaming_tumbling_aggregation_processor;

pub use streaming_count_aggregation_processor::StreamingCountAggregationProcessor;
pub use streaming_eos_aggregation_processor::StreamingEosAggregationProcessor;
#[path = "streaming_sliding_aggregation_processor.rs"]
mod streaming_sliding_aggregation_processor;
pub use streaming_sliding_aggregation_processor::StreamingSlidingAggregationProcessor;
pub use streaming_tumbling_aggregation_processor::StreamingTumblingAggregationProcessor;
#[path = "streaming_state_aggregation_processor.rs"]
mod streaming_state_aggregation_processor;
pub use streaming_state_aggregation_processor::StreamingStateAggregationProcessor;

/// StreamingAggregationProcessor - performs incremental aggregation with windowing.
pub enum StreamingAggregationProcessor {
    Count(StreamingCountAggregationProcessor),
    Tumbling(StreamingTumblingAggregationProcessor),
    Sliding(StreamingSlidingAggregationProcessor),
    State(StreamingStateAggregationProcessor),
    Eos(StreamingEosAggregationProcessor),
}

impl StreamingAggregationProcessor {
    pub fn new(
        id: impl Into<String>,
        physical: Arc<PhysicalStreamingAggregation>,
        aggregate_registry: Arc<AggregateFunctionRegistry>,
    ) -> Result<Self, ProcessorError> {
        Self::new_with_channel_capacities(
            id,
            physical,
            aggregate_registry,
            default_channel_capacities(),
        )
    }

    pub(crate) fn new_with_channel_capacities(
        id: impl Into<String>,
        physical: Arc<PhysicalStreamingAggregation>,
        aggregate_registry: Arc<AggregateFunctionRegistry>,
        channel_capacities: ProcessorChannelCapacities,
    ) -> Result<Self, ProcessorError> {
        let window = physical.window.clone();
        match window {
            StreamingWindowSpec::Count { count, .. } => Ok(StreamingAggregationProcessor::Count(
                StreamingCountAggregationProcessor::new_with_channel_capacities(
                    id,
                    Arc::clone(&physical),
                    Arc::clone(&aggregate_registry),
                    count,
                    channel_capacities,
                ),
            )),
            StreamingWindowSpec::Tumbling { .. } => {
                // Currently only seconds are supported at the logical level.
                Ok(StreamingAggregationProcessor::Tumbling(
                    StreamingTumblingAggregationProcessor::new_with_channel_capacities(
                        id,
                        Arc::clone(&physical),
                        aggregate_registry,
                        channel_capacities,
                    )?,
                ))
            }
            StreamingWindowSpec::Sliding { .. } => Ok(StreamingAggregationProcessor::Sliding(
                StreamingSlidingAggregationProcessor::new_with_channel_capacities(
                    id,
                    physical,
                    aggregate_registry,
                    channel_capacities,
                )?,
            )),
            StreamingWindowSpec::State { .. } => Ok(StreamingAggregationProcessor::State(
                StreamingStateAggregationProcessor::new_with_channel_capacities(
                    id,
                    physical,
                    aggregate_registry,
                    channel_capacities,
                )?,
            )),
            StreamingWindowSpec::Eos => Ok(StreamingAggregationProcessor::Eos(
                StreamingEosAggregationProcessor::new_with_channel_capacities(
                    id,
                    physical,
                    aggregate_registry,
                    channel_capacities,
                ),
            )),
        }
    }

    pub fn from_physical_plan(
        id: impl Into<String>,
        plan: Arc<PhysicalPlan>,
        aggregate_registry: Arc<AggregateFunctionRegistry>,
    ) -> Result<Option<Self>, ProcessorError> {
        match plan.as_ref() {
            PhysicalPlan::StreamingAggregation(aggregation) => Ok(Some(Self::new(
                id,
                Arc::new(aggregation.clone()),
                aggregate_registry,
            )?)),
            _ => Ok(None),
        }
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        match self {
            StreamingAggregationProcessor::Count(p) => p.set_stats(stats),
            StreamingAggregationProcessor::Tumbling(p) => p.set_stats(stats),
            StreamingAggregationProcessor::Sliding(p) => p.set_stats(stats),
            StreamingAggregationProcessor::State(p) => p.set_stats(stats),
            StreamingAggregationProcessor::Eos(p) => p.set_stats(stats),
        }
    }
}

impl Processor for StreamingAggregationProcessor {
    fn id(&self) -> &str {
        match self {
            StreamingAggregationProcessor::Count(p) => p.id(),
            StreamingAggregationProcessor::Tumbling(p) => p.id(),
            StreamingAggregationProcessor::Sliding(p) => p.id(),
            StreamingAggregationProcessor::State(p) => p.id(),
            StreamingAggregationProcessor::Eos(p) => p.id(),
        }
    }

    fn start(&mut self, spawner: &TaskSpawner) -> ProcessorStart {
        match self {
            StreamingAggregationProcessor::Count(p) => p.start(spawner),
            StreamingAggregationProcessor::Tumbling(p) => p.start(spawner),
            StreamingAggregationProcessor::Sliding(p) => p.start(spawner),
            StreamingAggregationProcessor::State(p) => p.start(spawner),
            StreamingAggregationProcessor::Eos(p) => p.start(spawner),
        }
    }

    fn subscribe_output(&self) -> Option<LinkReceiver<crate::processor::StreamData>> {
        match self {
            StreamingAggregationProcessor::Count(p) => p.subscribe_output(),
            StreamingAggregationProcessor::Tumbling(p) => p.subscribe_output(),
            StreamingAggregationProcessor::Sliding(p) => p.subscribe_output(),
            StreamingAggregationProcessor::State(p) => p.subscribe_output(),
            StreamingAggregationProcessor::Eos(p) => p.subscribe_output(),
        }
    }

    fn subscribe_control_output(&self) -> Option<LinkReceiver<crate::processor::ControlSignal>> {
        match self {
            StreamingAggregationProcessor::Count(p) => p.subscribe_control_output(),
            StreamingAggregationProcessor::Tumbling(p) => p.subscribe_control_output(),
            StreamingAggregationProcessor::Sliding(p) => p.subscribe_control_output(),
            StreamingAggregationProcessor::State(p) => p.subscribe_control_output(),
            StreamingAggregationProcessor::Eos(p) => p.subscribe_control_output(),
        }
    }

    fn add_input<R>(&mut self, receiver: R)
    where
        R: Into<LinkReceiver<crate::processor::StreamData>>,
    {
        match self {
            StreamingAggregationProcessor::Count(p) => p.add_input(receiver),
            StreamingAggregationProcessor::Tumbling(p) => p.add_input(receiver),
            StreamingAggregationProcessor::Sliding(p) => p.add_input(receiver),
            StreamingAggregationProcessor::State(p) => p.add_input(receiver),
            StreamingAggregationProcessor::Eos(p) => p.add_input(receiver),
        }
    }

    fn add_control_input<R>(&mut self, receiver: R)
    where
        R: Into<LinkReceiver<crate::processor::ControlSignal>>,
    {
        match self {
            StreamingAggregationProcessor::Count(p) => p.add_control_input(receiver),
            StreamingAggregationProcessor::Tumbling(p) => p.add_control_input(receiver),
            StreamingAggregationProcessor::Sliding(p) => p.add_control_input(receiver),
            StreamingAggregationProcessor::State(p) => p.add_control_input(receiver),
            StreamingAggregationProcessor::Eos(p) => p.add_control_input(receiver),
        }
    }
}

/// Group-by metadata bundled for evaluation and output.
#[derive(Clone)]
struct GroupByMeta {
    scalar: ScalarExpr,
    is_simple: bool,
    output_name: String,
}

/// Tracks aggregation state for a single group key.
struct GroupState {
    accumulators: Vec<Box<dyn AggregateAccumulator>>,
    last_tuple: crate::model::Tuple,
    key_values: Vec<Value>,
}

/// Shared aggregation logic reused by count and tumbling windows.
struct AggregationWorker {
    physical: Arc<PhysicalStreamingAggregation>,
    aggregate_registry: Arc<AggregateFunctionRegistry>,
    group_by_meta: Vec<GroupByMeta>,
    groups: HashMap<String, GroupState>,
}

impl AggregationWorker {
    fn new(
        physical: Arc<PhysicalStreamingAggregation>,
        aggregate_registry: Arc<AggregateFunctionRegistry>,
        group_by_meta: Vec<GroupByMeta>,
    ) -> Self {
        Self {
            physical,
            aggregate_registry,
            group_by_meta,
            groups: HashMap::new(),
        }
    }

    fn update_groups(&mut self, tuple: &crate::model::Tuple) -> Result<(), String> {
        let key_values = self.evaluate_group_by(tuple)?;
        let row_call_args = self.evaluate_aggregate_args(tuple)?;
        let key_repr = format!("{:?}", key_values);

        match self.groups.entry(key_repr) {
            Entry::Occupied(mut o) => {
                let entry = o.get_mut();
                apply_aggregate_updates(&mut entry.accumulators, &row_call_args)?;
                entry.last_tuple = tuple.clone();
                entry.key_values = key_values;
            }
            Entry::Vacant(v) => {
                let accumulators = create_accumulators_static(
                    &self.physical.aggregate_calls,
                    self.aggregate_registry.as_ref(),
                )?;
                let mut state = GroupState {
                    accumulators,
                    last_tuple: tuple.clone(),
                    key_values: key_values.clone(),
                };
                apply_aggregate_updates(&mut state.accumulators, &row_call_args)?;
                v.insert(state);
            }
        }

        Ok(())
    }

    fn evaluate_group_by(&self, tuple: &crate::model::Tuple) -> Result<Vec<Value>, String> {
        let mut values = Vec::with_capacity(self.group_by_meta.len());
        for meta in &self.group_by_meta {
            values.push(
                meta.scalar
                    .eval_with_tuple(tuple)
                    .map_err(|e| format!("Failed to evaluate group-by expression: {}", e))?,
            );
        }
        Ok(values)
    }

    fn evaluate_aggregate_args(
        &self,
        tuple: &crate::model::Tuple,
    ) -> Result<Vec<(usize, Vec<Value>)>, String> {
        let mut all_args = Vec::with_capacity(self.physical.aggregate_calls.len());
        for (idx, call) in self.physical.aggregate_calls.iter().enumerate() {
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

    fn finalize_current_window(&mut self) -> Result<Option<Box<dyn Collection>>, String> {
        if self.groups.is_empty() {
            self.groups.clear();
            return Ok(None);
        }

        let mut output_tuples = Vec::with_capacity(self.groups.len());
        for (_key, mut state) in self.groups.drain() {
            let tuple = finalize_group(
                &self.physical.aggregate_calls,
                &self.group_by_meta,
                &mut state.accumulators,
                &state.last_tuple,
                &state.key_values,
            )?;
            output_tuples.push(tuple);
        }

        let collection = RecordBatch::new(output_tuples)
            .map_err(|e| format!("Failed to build RecordBatch: {e}"))?;
        Ok(Some(Box::new(collection)))
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

fn finalize_group(
    aggregate_calls: &[AggregateCall],
    group_by_meta: &[GroupByMeta],
    accumulators: &mut [Box<dyn AggregateAccumulator>],
    last_tuple: &crate::model::Tuple,
    key_values: &[Value],
) -> Result<crate::model::Tuple, String> {
    use std::sync::Arc;

    let mut affiliate_entries = Vec::new();
    for (call, accumulator) in aggregate_calls.iter().zip(accumulators.iter_mut()) {
        affiliate_entries.push((Arc::new(call.output_column.clone()), accumulator.finalize()));
    }

    for (idx, value) in key_values.iter().enumerate() {
        if let Some(meta) = group_by_meta.get(idx) {
            if !meta.is_simple {
                affiliate_entries.push((Arc::new(meta.output_name.clone()), value.clone()));
            }
        }
    }

    let mut tuple =
        crate::model::Tuple::with_timestamp(last_tuple.messages.clone(), last_tuple.timestamp);
    tuple.add_affiliate_columns(affiliate_entries);
    Ok(tuple)
}

fn create_accumulators_static(
    aggregate_calls: &[AggregateCall],
    registry: &AggregateFunctionRegistry,
) -> Result<Vec<Box<dyn AggregateAccumulator>>, String> {
    let mut accumulators = Vec::new();
    for call in aggregate_calls {
        if call.distinct {
            return Err("DISTINCT aggregates are not supported yet".to_string());
        }
        let function = registry
            .get(&call.func_name)
            .ok_or_else(|| format!("Aggregate function '{}' not found", call.func_name))?;
        accumulators.push(function.create_accumulator());
    }
    Ok(accumulators)
}

fn build_group_by_meta(exprs: &[Expr], scalars: &[ScalarExpr]) -> Vec<GroupByMeta> {
    assert_eq!(
        exprs.len(),
        scalars.len(),
        "group-by exprs and scalars length mismatch"
    );
    exprs
        .iter()
        .zip(scalars.iter())
        .map(|(expr, scalar)| GroupByMeta {
            scalar: scalar.clone(),
            is_simple: is_simple_column_expr(expr),
            output_name: expr.to_string(),
        })
        .collect()
}

fn is_simple_column_expr(expr: &Expr) -> bool {
    matches!(expr, Expr::Identifier(_))
}
