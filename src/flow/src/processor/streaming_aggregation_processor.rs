//! StreamingAggregationProcessor - incremental aggregation with windowing.

use crate::aggregation::{AggregateAccumulator, AggregateFunctionRegistry};
use crate::expr::ScalarExpr;
use crate::model::{Collection, RecordBatch};
use crate::planner::physical::{
    AggregateCall, PhysicalPlan, PhysicalStreamingAggregation, StreamingWindowSpec,
};
use crate::processor::{Processor, ProcessorError};
use datatypes::Value;
use sqlparser::ast::Expr;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::broadcast;

#[path = "streaming_count_aggregation_processor.rs"]
mod streaming_count_aggregation_processor;
#[path = "streaming_tumbling_aggregation_processor.rs"]
mod streaming_tumbling_aggregation_processor;

pub use streaming_count_aggregation_processor::StreamingCountAggregationProcessor;
#[path = "streaming_sliding_aggregation_processor.rs"]
mod streaming_sliding_aggregation_processor;
pub use streaming_sliding_aggregation_processor::StreamingSlidingAggregationProcessor;
pub use streaming_tumbling_aggregation_processor::StreamingTumblingAggregationProcessor;

/// StreamingAggregationProcessor - performs incremental aggregation with windowing.
pub enum StreamingAggregationProcessor {
    Count(StreamingCountAggregationProcessor),
    Tumbling(StreamingTumblingAggregationProcessor),
    Sliding(StreamingSlidingAggregationProcessor),
}

impl StreamingAggregationProcessor {
    pub fn new(
        id: impl Into<String>,
        physical: Arc<PhysicalStreamingAggregation>,
        aggregate_registry: Arc<AggregateFunctionRegistry>,
    ) -> Self {
        let window = physical.window.clone();
        match window {
            StreamingWindowSpec::Count { count } => {
                StreamingAggregationProcessor::Count(StreamingCountAggregationProcessor::new(
                    id,
                    Arc::clone(&physical),
                    Arc::clone(&aggregate_registry),
                    count,
                ))
            }
            StreamingWindowSpec::Tumbling {
                time_unit: _,
                length: _,
            } => {
                // Currently only seconds are supported at the logical level.
                StreamingAggregationProcessor::Tumbling(StreamingTumblingAggregationProcessor::new(
                    id,
                    Arc::clone(&physical),
                    aggregate_registry,
                ))
            }
            StreamingWindowSpec::Sliding { .. } => StreamingAggregationProcessor::Sliding(
                StreamingSlidingAggregationProcessor::new(id, physical, aggregate_registry),
            ),
        }
    }

    pub fn from_physical_plan(
        id: impl Into<String>,
        plan: Arc<PhysicalPlan>,
        aggregate_registry: Arc<AggregateFunctionRegistry>,
    ) -> Option<Self> {
        match plan.as_ref() {
            PhysicalPlan::StreamingAggregation(aggregation) => Some(Self::new(
                id,
                Arc::new(aggregation.clone()),
                aggregate_registry,
            )),
            _ => None,
        }
    }
}

impl Processor for StreamingAggregationProcessor {
    fn id(&self) -> &str {
        match self {
            StreamingAggregationProcessor::Count(p) => p.id(),
            StreamingAggregationProcessor::Tumbling(p) => p.id(),
            StreamingAggregationProcessor::Sliding(p) => p.id(),
        }
    }

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        match self {
            StreamingAggregationProcessor::Count(p) => p.start(),
            StreamingAggregationProcessor::Tumbling(p) => p.start(),
            StreamingAggregationProcessor::Sliding(p) => p.start(),
        }
    }

    fn subscribe_output(&self) -> Option<broadcast::Receiver<crate::processor::StreamData>> {
        match self {
            StreamingAggregationProcessor::Count(p) => p.subscribe_output(),
            StreamingAggregationProcessor::Tumbling(p) => p.subscribe_output(),
            StreamingAggregationProcessor::Sliding(p) => p.subscribe_output(),
        }
    }

    fn subscribe_control_output(
        &self,
    ) -> Option<broadcast::Receiver<crate::processor::ControlSignal>> {
        match self {
            StreamingAggregationProcessor::Count(p) => p.subscribe_control_output(),
            StreamingAggregationProcessor::Tumbling(p) => p.subscribe_control_output(),
            StreamingAggregationProcessor::Sliding(p) => p.subscribe_control_output(),
        }
    }

    fn add_input(&mut self, receiver: broadcast::Receiver<crate::processor::StreamData>) {
        match self {
            StreamingAggregationProcessor::Count(p) => p.add_input(receiver),
            StreamingAggregationProcessor::Tumbling(p) => p.add_input(receiver),
            StreamingAggregationProcessor::Sliding(p) => p.add_input(receiver),
        }
    }

    fn add_control_input(
        &mut self,
        receiver: broadcast::Receiver<crate::processor::ControlSignal>,
    ) {
        match self {
            StreamingAggregationProcessor::Count(p) => p.add_control_input(receiver),
            StreamingAggregationProcessor::Tumbling(p) => p.add_control_input(receiver),
            StreamingAggregationProcessor::Sliding(p) => p.add_control_input(receiver),
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
        let key_repr = format!("{:?}", key_values);

        let entry = match self.groups.entry(key_repr) {
            Entry::Occupied(o) => o.into_mut(),
            Entry::Vacant(v) => {
                let accumulators = create_accumulators_static(
                    &self.physical.aggregate_calls,
                    self.aggregate_registry.as_ref(),
                )?;
                v.insert(GroupState {
                    accumulators,
                    last_tuple: tuple.clone(),
                    key_values: key_values.clone(),
                })
            }
        };

        entry.last_tuple = tuple.clone();
        entry.key_values = key_values;

        for (idx, call) in self.physical.aggregate_calls.iter().enumerate() {
            let mut args = Vec::new();
            for arg_expr in &call.args {
                args.push(
                    arg_expr
                        .eval_with_tuple(tuple)
                        .map_err(|e| format!("Failed to evaluate aggregate argument: {}", e))?,
                );
            }
            entry
                .accumulators
                .get_mut(idx)
                .ok_or_else(|| "accumulator missing".to_string())?
                .update(&args)?;
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

fn finalize_group(
    aggregate_calls: &[AggregateCall],
    group_by_meta: &[GroupByMeta],
    accumulators: &mut [Box<dyn AggregateAccumulator>],
    last_tuple: &crate::model::Tuple,
    key_values: &[Value],
) -> Result<crate::model::Tuple, String> {
    use crate::model::AffiliateRow;
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

    let mut tuple = last_tuple.clone();
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
