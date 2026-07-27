use crate::aggregation::AggregateFunctionRegistry;
use crate::codec::EncoderRegistry;
use crate::planner::physical::{
    output_layout::OutputLayout, PhysicalBarrier, PhysicalPlan, PhysicalStreamingAggregation,
    StreamingWindowSpec,
};
use std::sync::Arc;

/// A physical optimization rule.
trait PhysicalOptRule {
    fn name(&self) -> &str;
    fn optimize(
        &self,
        plan: Arc<PhysicalPlan>,
        encoder_registry: &EncoderRegistry,
    ) -> Arc<PhysicalPlan>;
}

/// Apply physical plan optimizations using the provided registries.
pub fn optimize_physical_plan(
    physical_plan: Arc<PhysicalPlan>,
    encoder_registry: &EncoderRegistry,
    aggregate_registry: Arc<AggregateFunctionRegistry>,
) -> Arc<PhysicalPlan> {
    let rules: Vec<Box<dyn PhysicalOptRule>> = vec![
        Box::new(StreamingAggregationRewrite {
            aggregate_registry: Arc::clone(&aggregate_registry),
        }),
        Box::new(StreamingEncoderRewrite),
        Box::new(AttachEncoderOutputLayout),
        Box::new(RemovePlannerColumnFilter),
        Box::new(InsertBarrierForFanIn),
    ];
    let mut current = physical_plan;
    for rule in rules {
        let _ = rule.name();
        current = rule.optimize(current, encoder_registry);
    }
    current
}

/// Rule: fuse Window -> Aggregation into StreamingAggregation when all calls are incremental.
struct StreamingAggregationRewrite {
    aggregate_registry: Arc<AggregateFunctionRegistry>,
}

/// Rule: insert a barrier node between a fan-in plan and its children.
///
/// A node is considered fan-in when it has more than one child. The inserted barrier node keeps
/// the original children, and the parent is rewritten to have a single barrier child.
///
/// This is a topology rewrite rule and is intentionally applied as the last physical optimization.
struct InsertBarrierForFanIn;

/// Rule: fuse `PhysicalBatch -> PhysicalSinkEncoder` into `PhysicalIncSinkEncoder`.
///
/// This eliminates one data-pass between BatchProcessor and SinkEncoderProcessor
/// for registered `SinkEncoder` implementations.
struct StreamingEncoderRewrite;

/// Rule: preserve final encoder output schema before rewrites delay materialization.
struct AttachEncoderOutputLayout;

/// Remove planner-only column filter nodes after all sink consumers captured their layouts.
struct RemovePlannerColumnFilter;

impl PhysicalOptRule for StreamingAggregationRewrite {
    fn name(&self) -> &str {
        "streaming_aggregation_rewrite"
    }

    fn optimize(
        &self,
        plan: Arc<PhysicalPlan>,
        encoder_registry: &EncoderRegistry,
    ) -> Arc<PhysicalPlan> {
        self.optimize_node(plan, encoder_registry)
    }
}

impl StreamingAggregationRewrite {
    fn optimize_node(
        &self,
        plan: Arc<PhysicalPlan>,
        encoder_registry: &EncoderRegistry,
    ) -> Arc<PhysicalPlan> {
        let optimized_children = self.optimize_children(plan.children(), encoder_registry);

        match plan.as_ref() {
            PhysicalPlan::Aggregation(agg) => {
                if let Some(child) = optimized_children.first() {
                    if let Some(streaming) =
                        self.try_fuse_streaming_agg(agg, child, encoder_registry)
                    {
                        return streaming;
                    }
                }
                rebuild_with_children(plan.as_ref(), optimized_children)
            }
            _ => rebuild_with_children(plan.as_ref(), optimized_children),
        }
    }

    fn optimize_children(
        &self,
        children: &[Arc<PhysicalPlan>],
        encoder_registry: &EncoderRegistry,
    ) -> Vec<Arc<PhysicalPlan>> {
        children
            .iter()
            .map(|child| self.optimize_node(Arc::clone(child), encoder_registry))
            .collect()
    }

    fn try_fuse_streaming_agg(
        &self,
        agg: &crate::planner::physical::PhysicalAggregation,
        child: &Arc<PhysicalPlan>,
        _encoder_registry: &EncoderRegistry,
    ) -> Option<Arc<PhysicalPlan>> {
        if !crate::planner::physical::PhysicalAggregation::all_calls_incremental(
            &agg.aggregate_calls,
            &self.aggregate_registry,
        ) {
            return None;
        }

        let (window_spec, upstream_child) = match child.as_ref() {
            PhysicalPlan::TumblingWindow(window) => {
                let spec = StreamingWindowSpec::Tumbling {
                    time_unit: window.time_unit,
                    length: window.length,
                };
                let upstream = window.base.children.first()?.clone();
                (spec, upstream)
            }
            PhysicalPlan::CountWindow(window) => {
                let spec = StreamingWindowSpec::Count {
                    count: window.count,
                };
                let upstream = window.base.children.first()?.clone();
                (spec, upstream)
            }
            PhysicalPlan::SlidingWindow(window) => {
                let spec = StreamingWindowSpec::Sliding {
                    time_unit: window.time_unit,
                    lookback: window.lookback,
                    lookahead: window.lookahead,
                };
                let upstream = window.base.children.first()?.clone();
                (spec, upstream)
            }
            PhysicalPlan::StateWindow(window) => {
                let spec = StreamingWindowSpec::State {
                    open_expr: window.open_expr.clone(),
                    emit_expr: window.emit_expr.clone(),
                    partition_by_exprs: window.partition_by_exprs.clone(),
                    open_scalar: window.open_scalar.clone(),
                    emit_scalar: window.emit_scalar.clone(),
                    partition_by_scalars: window.partition_by_scalars.clone(),
                };
                let upstream = window.base.children.first()?.clone();
                (spec, upstream)
            }
            _ => return None,
        };

        let streaming = PhysicalStreamingAggregation::new(
            window_spec,
            agg.aggregate_mappings.clone(),
            agg.group_by_exprs.clone(),
            agg.aggregate_calls.clone(),
            agg.group_by_scalars.clone(),
            vec![upstream_child],
            agg.base.index(),
        );
        Some(Arc::new(PhysicalPlan::StreamingAggregation(streaming)))
    }
}

impl PhysicalOptRule for StreamingEncoderRewrite {
    fn name(&self) -> &str {
        "streaming_encoder_rewrite"
    }

    fn optimize(
        &self,
        plan: Arc<PhysicalPlan>,
        _encoder_registry: &EncoderRegistry,
    ) -> Arc<PhysicalPlan> {
        fuse_streaming_encoder(plan)
    }
}

impl PhysicalOptRule for AttachEncoderOutputLayout {
    fn name(&self) -> &str {
        "attach_encoder_output_layout"
    }

    fn optimize(
        &self,
        plan: Arc<PhysicalPlan>,
        _encoder_registry: &EncoderRegistry,
    ) -> Arc<PhysicalPlan> {
        attach_encoder_output_layout(plan)
    }
}

impl PhysicalOptRule for RemovePlannerColumnFilter {
    fn name(&self) -> &str {
        "remove_planner_column_filter"
    }

    fn optimize(
        &self,
        plan: Arc<PhysicalPlan>,
        _encoder_registry: &EncoderRegistry,
    ) -> Arc<PhysicalPlan> {
        remove_planner_column_filter(plan)
    }
}

fn remove_planner_column_filter(plan: Arc<PhysicalPlan>) -> Arc<PhysicalPlan> {
    let children = plan
        .children()
        .iter()
        .map(|child| remove_planner_column_filter(Arc::clone(child)))
        .collect::<Vec<_>>();

    if matches!(plan.as_ref(), PhysicalPlan::ColumnFilter(_)) && children.len() == 1 {
        return Arc::clone(&children[0]);
    }
    rebuild_with_children(plan.as_ref(), children)
}

fn fuse_streaming_encoder(plan: Arc<PhysicalPlan>) -> Arc<PhysicalPlan> {
    let children: Vec<Arc<PhysicalPlan>> = plan
        .children()
        .iter()
        .map(|child| fuse_streaming_encoder(Arc::clone(child)))
        .collect();

    match plan.as_ref() {
        PhysicalPlan::SinkEncoder(encoder) => {
            // Check if the unique child is PhysicalBatch
            if children.len() != 1 {
                return rebuild_with_children(plan.as_ref(), children);
            }
            let first_child = &children[0];
            if !matches!(first_child.as_ref(), PhysicalPlan::Batch(_)) {
                return rebuild_with_children(plan.as_ref(), children);
            }

            // Fuse PhysicalBatch into PhysicalIncSinkEncoder:
            // - take PhysicalBatch's children (skip the batch node)
            // - take PhysicalBatch's common (batch params)
            // - create PhysicalIncSinkEncoder with same sink_id, encoder, index
            if let PhysicalPlan::Batch(batch) = first_child.as_ref() {
                let batch_children = first_child.children().to_vec();
                let fused_index = encoder.base.index();
                let mut fused = crate::planner::physical::PhysicalIncSinkEncoder::new(
                    batch_children,
                    fused_index,
                    encoder.sink_id.clone(),
                    encoder.encoder.clone(),
                    batch.common.clone(),
                );
                fused.output_layout = encoder.output_layout.clone();
                Arc::new(PhysicalPlan::IncSinkEncoder(fused))
            } else {
                rebuild_with_children(plan.as_ref(), children)
            }
        }
        _ => rebuild_with_children(plan.as_ref(), children),
    }
}

fn attach_encoder_output_layout(plan: Arc<PhysicalPlan>) -> Arc<PhysicalPlan> {
    let children = plan
        .children()
        .iter()
        .map(|child| attach_encoder_output_layout(Arc::clone(child)))
        .collect::<Vec<_>>();

    match plan.as_ref() {
        PhysicalPlan::SinkEncoder(encoder) => {
            let mut new = encoder.clone();
            new.base.children = children;
            new.output_layout =
                encoder_output_layout_from_single_child("PhysicalSinkEncoder", &new.base.children);
            Arc::new(PhysicalPlan::SinkEncoder(new))
        }
        PhysicalPlan::IncSinkEncoder(encoder) => {
            let mut new = encoder.clone();
            new.base.children = children;
            new.output_layout = encoder_output_layout_from_single_child(
                "PhysicalIncSinkEncoder",
                &new.base.children,
            );
            Arc::new(PhysicalPlan::IncSinkEncoder(new))
        }
        _ => rebuild_with_children(plan.as_ref(), children),
    }
}

fn encoder_output_layout_from_single_child(
    encoder_plan_type: &str,
    children: &[Arc<PhysicalPlan>],
) -> Option<Arc<OutputLayout>> {
    let [child] = children else {
        return None;
    };
    match child.output_layout() {
        Ok(output_layout) => Some(Arc::new(output_layout)),
        Err(err) => {
            tracing::warn!(
                encoder_plan_type,
                child_plan_type = child.get_plan_type(),
                child_plan_index = child.get_plan_index(),
                error = %err,
                "failed to derive encoder output schema from child plan"
            );
            None
        }
    }
}

impl PhysicalOptRule for InsertBarrierForFanIn {
    fn name(&self) -> &str {
        "insert_barrier_for_fan_in"
    }

    fn optimize(
        &self,
        plan: Arc<PhysicalPlan>,
        _encoder_registry: &EncoderRegistry,
    ) -> Arc<PhysicalPlan> {
        let mut next_index = max_physical_index(&plan) + 1;
        insert_barrier_for_fan_in(plan, &mut next_index)
    }
}

fn insert_barrier_for_fan_in(plan: Arc<PhysicalPlan>, next_index: &mut i64) -> Arc<PhysicalPlan> {
    let optimized_children = plan
        .children()
        .iter()
        .map(|child| insert_barrier_for_fan_in(Arc::clone(child), next_index))
        .collect::<Vec<_>>();

    let rebuilt = rebuild_with_children(plan.as_ref(), optimized_children);
    if matches!(rebuilt.as_ref(), PhysicalPlan::Barrier(_)) {
        return rebuilt;
    }

    if rebuilt.children().len() <= 1 {
        return rebuilt;
    }

    let barrier_index = allocate_index(next_index);
    let barrier_children = rebuilt.children().to_vec();
    let barrier = PhysicalBarrier::new(barrier_children, barrier_index);
    let barrier_node = Arc::new(PhysicalPlan::Barrier(barrier));
    rebuild_with_children(rebuilt.as_ref(), vec![barrier_node])
}

fn max_physical_index(plan: &Arc<PhysicalPlan>) -> i64 {
    let mut max_index = plan.get_plan_index();
    for child in plan.children() {
        max_index = max_index.max(max_physical_index(child));
    }
    max_index
}

fn allocate_index(next: &mut i64) -> i64 {
    let index = *next;
    *next += 1;
    index
}

fn rebuild_with_children(
    plan: &PhysicalPlan,
    children: Vec<Arc<PhysicalPlan>>,
) -> Arc<PhysicalPlan> {
    match plan {
        PhysicalPlan::DataSource(ds) => {
            let mut new = ds.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::DataSource(new))
        }
        PhysicalPlan::TableScan(scan) => {
            let mut new = scan.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::TableScan(new))
        }
        PhysicalPlan::Decoder(decoder) => {
            let mut new = decoder.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::Decoder(new))
        }
        PhysicalPlan::CollectionLayoutNormalize(normalize) => {
            let mut new = normalize.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::CollectionLayoutNormalize(new))
        }
        PhysicalPlan::MemoryCollectionMaterialize(materialize) => {
            let mut new = materialize.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::MemoryCollectionMaterialize(new))
        }
        PhysicalPlan::StatefulFunction(stateful) => {
            let mut new = stateful.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::StatefulFunction(new))
        }
        PhysicalPlan::SharedStream(stream) => {
            let mut new = stream.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::SharedStream(new))
        }
        PhysicalPlan::SourceChangeGate(gate) => {
            let mut new = gate.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::SourceChangeGate(new))
        }
        PhysicalPlan::Filter(filter) => {
            let mut new = filter.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::Filter(new))
        }
        PhysicalPlan::Compute(compute) => {
            let mut new = compute.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::Compute(new))
        }
        PhysicalPlan::Order(order) => {
            let mut new = order.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::Order(new))
        }
        PhysicalPlan::Project(project) => {
            let mut new = project.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::Project(new))
        }
        PhysicalPlan::RowDiff(row_diff) => {
            let mut new = row_diff.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::RowDiff(new))
        }
        PhysicalPlan::ColumnFilter(filter) => {
            let mut new = filter.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::ColumnFilter(new))
        }
        PhysicalPlan::EmptySuppress(empty_suppress) => {
            let mut new = empty_suppress.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::EmptySuppress(new))
        }
        PhysicalPlan::Aggregation(agg) => {
            let mut new = agg.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::Aggregation(new))
        }
        PhysicalPlan::Batch(batch) => {
            let mut new = batch.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::Batch(new))
        }
        PhysicalPlan::SinkEncoder(encoder) => {
            let mut new = encoder.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::SinkEncoder(new))
        }

        PhysicalPlan::IncSinkEncoder(encoder) => {
            let mut new = encoder.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::IncSinkEncoder(new))
        }
        PhysicalPlan::SinkCompress(compress) => {
            let mut new = compress.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::SinkCompress(new))
        }
        PhysicalPlan::SinkEncrypt(encrypt) => {
            let mut new = encrypt.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::SinkEncrypt(new))
        }
        PhysicalPlan::StreamingAggregation(agg) => {
            let mut new = agg.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::StreamingAggregation(new))
        }
        PhysicalPlan::ResultCollect(collect) => {
            let mut new = collect.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::ResultCollect(new))
        }
        PhysicalPlan::Barrier(barrier) => {
            let mut new = barrier.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::Barrier(new))
        }
        PhysicalPlan::TumblingWindow(window) => {
            let mut new = window.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::TumblingWindow(new))
        }
        PhysicalPlan::ProcessTimeWatermark(watermark) => {
            let mut new = watermark.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::ProcessTimeWatermark(new))
        }
        PhysicalPlan::EventtimeWatermark(watermark) => {
            let mut new = watermark.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::EventtimeWatermark(new))
        }
        PhysicalPlan::Watermark(watermark) => {
            let mut new = watermark.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::Watermark(new))
        }
        PhysicalPlan::CountWindow(window) => {
            let mut new = window.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::CountWindow(new))
        }
        PhysicalPlan::SlidingWindow(window) => {
            let mut new = window.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::SlidingWindow(new))
        }
        PhysicalPlan::StateWindow(window) => {
            let mut new = window.as_ref().clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::StateWindow(Box::new(new)))
        }
        PhysicalPlan::DataSink(sink) => {
            let mut new = sink.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::DataSink(new))
        }
        PhysicalPlan::SinkConnector(sink) => {
            let mut new = sink.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::SinkConnector(new))
        }
        PhysicalPlan::Sampler(sampler) => {
            let mut new = sampler.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::Sampler(new))
        }
    }
}
