use crate::aggregation::AggregateFunctionRegistry;
use crate::codec::EncoderRegistry;
use crate::expr::scalar::ColumnRef;
use crate::expr::ScalarExpr;
use crate::planner::physical::{
    ByIndexProjection, ByIndexProjectionColumn, PhysicalBarrier, PhysicalDataSink, PhysicalPlan,
    PhysicalProjectField, PhysicalSinkConnector, PhysicalStreamingAggregation,
    PhysicalStreamingEncoder, StreamingWindowSpec,
};
use sqlparser::ast::Expr;
use std::collections::{HashMap, HashSet};
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
        Box::new(ByIndexProjectionIntoEncoderRewrite),
        Box::new(InsertBarrierForFanIn),
    ];
    let mut current = physical_plan;
    for rule in rules {
        let _ = rule.name();
        current = rule.optimize(current, encoder_registry);
    }
    current
}

/// Rule: if a sink has a Batch -> Encoder chain and the encoder supports streaming,
/// rewrite it to StreamingEncoder -> Sink (dropping the batch).
struct StreamingEncoderRewrite;

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

/// Rule: detect shared `Project` nodes that are pure `ColumnRef::ByIndex` projections
/// (with no aliases) directly upstream of encoders, and prepare them for
/// encoder-side delayed materialization.
///
/// Note: The actual rewrite (removing `Project` and attaching projection specs to encoders)
/// is implemented in subsequent steps once encoder-side support is wired up.
struct ByIndexProjectionIntoEncoderRewrite;

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
        encoder_registry: &EncoderRegistry,
    ) -> Arc<PhysicalPlan> {
        self.optimize_node(plan, encoder_registry)
    }
}

impl StreamingEncoderRewrite {
    fn optimize_node(
        &self,
        plan: Arc<PhysicalPlan>,
        encoder_registry: &EncoderRegistry,
    ) -> Arc<PhysicalPlan> {
        let optimized_children = self.optimize_children(plan.children(), encoder_registry);
        match plan.as_ref() {
            PhysicalPlan::DataSink(sink) => {
                self.optimize_data_sink(sink, optimized_children, encoder_registry)
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

    fn optimize_data_sink(
        &self,
        sink: &PhysicalDataSink,
        optimized_children: Vec<Arc<PhysicalPlan>>,
        encoder_registry: &EncoderRegistry,
    ) -> Arc<PhysicalPlan> {
        if let Some(child) = optimized_children.first() {
            if let Some((rewritten_child, connector)) =
                self.rewrite_streaming_encoder_chain(sink, child, encoder_registry)
            {
                let mut new_sink = sink.clone();
                new_sink.base.children = vec![rewritten_child];
                new_sink.connector = connector;
                return Arc::new(PhysicalPlan::DataSink(new_sink));
            }
        }

        let mut new_sink = sink.clone();
        new_sink.base.children = optimized_children;
        Arc::new(PhysicalPlan::DataSink(new_sink))
    }

    fn rewrite_streaming_encoder_chain(
        &self,
        sink: &PhysicalDataSink,
        child: &Arc<PhysicalPlan>,
        encoder_registry: &EncoderRegistry,
    ) -> Option<(Arc<PhysicalPlan>, PhysicalSinkConnector)> {
        let encoder = match child.as_ref() {
            PhysicalPlan::Encoder(encoder) => encoder,
            _ => return None,
        };

        if !encoder_registry.supports_streaming(encoder.encoder.kind_str()) {
            return None;
        }

        let batch = match encoder.base.children.first() {
            Some(plan) => match plan.as_ref() {
                PhysicalPlan::Batch(batch) => batch,
                _ => return None,
            },
            None => return None,
        };

        let upstream = batch.base.children.first().cloned()?;
        let streaming_index = encoder.base.index();
        let streaming_encoder = PhysicalStreamingEncoder::new(
            vec![upstream],
            streaming_index,
            encoder.sink_id.clone(),
            encoder.encoder.clone(),
            batch.common.clone(),
        );

        let mut connector = sink.connector.clone();
        connector.encoder_plan_index = Some(streaming_index);

        Some((
            Arc::new(PhysicalPlan::StreamingEncoder(streaming_encoder)),
            connector,
        ))
    }
}

impl PhysicalOptRule for ByIndexProjectionIntoEncoderRewrite {
    fn name(&self) -> &str {
        "by_index_projection_into_encoder_rewrite"
    }

    fn optimize(
        &self,
        plan: Arc<PhysicalPlan>,
        encoder_registry: &EncoderRegistry,
    ) -> Arc<PhysicalPlan> {
        rewrite_by_index_projection_into_encoder(plan, encoder_registry)
    }
}

#[derive(Clone, Debug)]
enum ProjectConsumer {
    Encoder { encoder_index: i64, kind: String },
    StreamingEncoder { encoder_index: i64, kind: String },
    Other,
}

#[derive(Clone, Debug)]
struct ByIndexRewriteState {
    projects_to_passthrough: HashSet<i64>,
    project_to_remaining_fields: HashMap<i64, Vec<PhysicalProjectField>>,
    encoder_to_projection: HashMap<i64, Arc<ByIndexProjection>>,
}

fn rewrite_by_index_projection_into_encoder(
    plan: Arc<PhysicalPlan>,
    encoder_registry: &EncoderRegistry,
) -> Arc<PhysicalPlan> {
    let (node_map, consumer_map) = build_node_and_consumer_maps(&plan);

    let mut state = ByIndexRewriteState {
        projects_to_passthrough: HashSet::new(),
        project_to_remaining_fields: HashMap::new(),
        encoder_to_projection: HashMap::new(),
    };

    for (node_index, node) in node_map.iter() {
        let PhysicalPlan::Project(project) = node.as_ref() else {
            continue;
        };
        if project.fields.is_empty() {
            continue;
        }
        if project
            .fields
            .iter()
            .any(|field| matches!(&field.compiled_expr, ScalarExpr::Wildcard { .. }))
        {
            continue;
        }

        let mut columns = Vec::new();
        let mut remaining_fields = Vec::new();
        for field in &project.fields {
            if is_unaliased_by_index_field(field) {
                if let Some(column) = by_index_projection_column_from_field(field) {
                    columns.push(column);
                }
            } else {
                remaining_fields.push(field.clone());
            }
        }

        if columns.is_empty() {
            continue;
        }

        let consumers = consumer_map.get(node_index).cloned().unwrap_or_default();
        if consumers.is_empty() {
            continue;
        }

        // Design constraint: when a `Project` is shared (DAG), only apply this rewrite
        // if every consumer is an encoder that can honor delayed materialization.
        if !consumers.iter().all(|consumer| match consumer {
            ProjectConsumer::Encoder { kind, .. } => {
                supports_by_index_projection(kind, encoder_registry)
            }
            ProjectConsumer::StreamingEncoder { kind, .. } => {
                supports_by_index_projection(kind, encoder_registry)
            }
            ProjectConsumer::Other => false,
        }) {
            continue;
        }

        let spec = Arc::new(ByIndexProjection::new(columns));

        state.projects_to_passthrough.insert(*node_index);
        state
            .project_to_remaining_fields
            .insert(*node_index, remaining_fields);
        for consumer in consumers {
            match consumer {
                ProjectConsumer::Encoder { encoder_index, .. }
                | ProjectConsumer::StreamingEncoder { encoder_index, .. } => {
                    state
                        .encoder_to_projection
                        .insert(encoder_index, Arc::clone(&spec));
                }
                ProjectConsumer::Other => {}
            }
        }
    }

    if state.projects_to_passthrough.is_empty() && state.encoder_to_projection.is_empty() {
        return plan;
    }

    let mut memo = HashMap::new();
    rewrite_by_index_nodes(plan, &state, &mut memo)
}

fn supports_by_index_projection(kind: &str, _encoder_registry: &EncoderRegistry) -> bool {
    _encoder_registry.supports_by_index_projection(kind)
}

fn by_index_projection_column_from_field(
    field: &PhysicalProjectField,
) -> Option<ByIndexProjectionColumn> {
    let ScalarExpr::Column(ColumnRef::ByIndex {
        source_name,
        column_index,
    }) = &field.compiled_expr
    else {
        return None;
    };

    Some(ByIndexProjectionColumn::new(
        source_name.clone(),
        *column_index,
        field.field_name.clone(),
    ))
}

fn build_node_and_consumer_maps(
    root: &Arc<PhysicalPlan>,
) -> (
    HashMap<i64, Arc<PhysicalPlan>>,
    HashMap<i64, Vec<ProjectConsumer>>,
) {
    fn helper(
        plan: &Arc<PhysicalPlan>,
        nodes: &mut HashMap<i64, Arc<PhysicalPlan>>,
        consumers: &mut HashMap<i64, Vec<ProjectConsumer>>,
        visited: &mut HashSet<i64>,
    ) {
        let index = plan.get_plan_index();
        let already_visited = !visited.insert(index);
        nodes.entry(index).or_insert_with(|| Arc::clone(plan));

        for child in plan.children() {
            let child_index = child.get_plan_index();
            let consumer = match plan.as_ref() {
                PhysicalPlan::Encoder(encoder) => ProjectConsumer::Encoder {
                    encoder_index: encoder.base.index(),
                    kind: encoder.encoder.kind_str().to_string(),
                },
                PhysicalPlan::StreamingEncoder(encoder) => ProjectConsumer::StreamingEncoder {
                    encoder_index: encoder.base.index(),
                    kind: encoder.encoder.kind_str().to_string(),
                },
                _ => ProjectConsumer::Other,
            };
            consumers.entry(child_index).or_default().push(consumer);

            if !already_visited {
                helper(child, nodes, consumers, visited);
            }
        }
    }

    let mut nodes = HashMap::new();
    let mut consumers = HashMap::new();
    let mut visited = HashSet::new();
    helper(root, &mut nodes, &mut consumers, &mut visited);
    (nodes, consumers)
}

fn rewrite_by_index_nodes(
    plan: Arc<PhysicalPlan>,
    state: &ByIndexRewriteState,
    memo: &mut HashMap<i64, Arc<PhysicalPlan>>,
) -> Arc<PhysicalPlan> {
    let index = plan.get_plan_index();
    if let Some(rewritten) = memo.get(&index) {
        return Arc::clone(rewritten);
    }

    let rewritten_children = plan
        .children()
        .iter()
        .map(|child| rewrite_by_index_nodes(Arc::clone(child), state, memo))
        .collect::<Vec<_>>();

    let rebuilt = match plan.as_ref() {
        PhysicalPlan::Project(project) if state.projects_to_passthrough.contains(&index) => {
            let mut new = project.clone();
            new.base.children = rewritten_children;
            new.fields = state
                .project_to_remaining_fields
                .get(&index)
                .cloned()
                .unwrap_or_default();
            new.passthrough_messages = true;
            Arc::new(PhysicalPlan::Project(new))
        }
        PhysicalPlan::Encoder(encoder) if state.encoder_to_projection.contains_key(&index) => {
            let mut new = encoder.clone();
            new.base.children = rewritten_children;
            new.by_index_projection = state.encoder_to_projection.get(&index).cloned();
            Arc::new(PhysicalPlan::Encoder(new))
        }
        PhysicalPlan::StreamingEncoder(encoder)
            if state.encoder_to_projection.contains_key(&index) =>
        {
            let mut new = encoder.clone();
            new.base.children = rewritten_children;
            new.by_index_projection = state.encoder_to_projection.get(&index).cloned();
            Arc::new(PhysicalPlan::StreamingEncoder(new))
        }
        _ => rebuild_with_children(plan.as_ref(), rewritten_children),
    };

    memo.insert(index, Arc::clone(&rebuilt));
    rebuilt
}

fn is_unaliased_by_index_field(field: &PhysicalProjectField) -> bool {
    let ScalarExpr::Column(ColumnRef::ByIndex { .. }) = &field.compiled_expr else {
        return false;
    };
    original_expr_matches_field_name(&field.original_expr, &field.field_name)
}

fn original_expr_matches_field_name(original_expr: &Expr, field_name: &str) -> bool {
    match original_expr {
        Expr::Identifier(ident) => ident.value == field_name,
        Expr::CompoundIdentifier(parts) => {
            parts.last().is_some_and(|ident| ident.value == field_name)
        }
        _ => false,
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
        PhysicalPlan::Decoder(decoder) => {
            let mut new = decoder.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::Decoder(new))
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
        PhysicalPlan::Filter(filter) => {
            let mut new = filter.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::Filter(new))
        }
        PhysicalPlan::Project(project) => {
            let mut new = project.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::Project(new))
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
        PhysicalPlan::Encoder(encoder) => {
            let mut new = encoder.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::Encoder(new))
        }
        PhysicalPlan::StreamingAggregation(agg) => {
            let mut new = agg.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::StreamingAggregation(new))
        }
        PhysicalPlan::StreamingEncoder(streaming) => {
            let mut new = streaming.clone();
            new.base.children = children;
            Arc::new(PhysicalPlan::StreamingEncoder(new))
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
    }
}
