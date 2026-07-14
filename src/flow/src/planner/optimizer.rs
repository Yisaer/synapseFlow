use crate::aggregation::AggregateFunctionRegistry;
use crate::codec::EncoderRegistry;
use crate::expr::scalar::ColumnRef;
use crate::expr::ScalarExpr;
use crate::planner::physical::{
    output_schema::OutputSchema, ByIndexProjection, ByIndexProjectionColumn, PhysicalBarrier,
    PhysicalPlan, PhysicalProjectField, PhysicalStreamingAggregation, StreamingWindowSpec,
};
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
        Box::new(AttachEncoderOutputSchema),
        Box::new(ByIndexProjectionAcrossMixedConsumersRewrite),
        Box::new(PartialByIndexRowDiffAndEncoderRewrite),
        Box::new(ByIndexProjectionIntoRowDiffRewrite),
        Box::new(ByIndexProjectionIntoEncoderRewrite),
        Box::new(ColumnFilterProjectionIntersection),
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
struct AttachEncoderOutputSchema;

/// Rule: detect shared `Project` nodes that are pure `ColumnRef::ByIndex` projections
/// (with no aliases) directly upstream of encoders, and prepare them for
/// encoder-side delayed materialization.
///
/// Note: The actual rewrite (removing `Project` and attaching projection specs to encoders)
/// is implemented in subsequent steps once encoder-side support is wired up.
struct ByIndexProjectionIntoEncoderRewrite;

/// Rule: detect shared `Project` nodes that are pure `ColumnRef::ByIndex` projections directly
/// upstream of row-diff nodes, and prepare them for row-diff-side delayed materialization.
struct ByIndexProjectionIntoRowDiffRewrite;

/// Rule: detect shared `Project` nodes whose direct consumers are a mix of row-diff and encoder
/// branches, and rewrite the shared `Project` into passthrough while letting each branch delay
/// the same by-index fields to its own first eligible consumer.
struct ByIndexProjectionAcrossMixedConsumersRewrite;

/// Rule: split a pure by-index `Project -> RowDiff -> Encoder` branch so row diff only late-reads
/// tracked columns and the downstream encoder late-reads the remaining pass-through columns.
struct PartialByIndexRowDiffAndEncoderRewrite;

struct ColumnFilterProjectionIntersection;

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

impl PhysicalOptRule for AttachEncoderOutputSchema {
    fn name(&self) -> &str {
        "attach_encoder_output_schema"
    }

    fn optimize(
        &self,
        plan: Arc<PhysicalPlan>,
        _encoder_registry: &EncoderRegistry,
    ) -> Arc<PhysicalPlan> {
        attach_encoder_output_schema(plan)
    }
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
                fused.output_schema = encoder.output_schema.clone();
                Arc::new(PhysicalPlan::IncSinkEncoder(fused))
            } else {
                rebuild_with_children(plan.as_ref(), children)
            }
        }
        _ => rebuild_with_children(plan.as_ref(), children),
    }
}

fn attach_encoder_output_schema(plan: Arc<PhysicalPlan>) -> Arc<PhysicalPlan> {
    let children = plan
        .children()
        .iter()
        .map(|child| attach_encoder_output_schema(Arc::clone(child)))
        .collect::<Vec<_>>();

    match plan.as_ref() {
        PhysicalPlan::SinkEncoder(encoder) => {
            let mut new = encoder.clone();
            new.base.children = children;
            new.output_schema =
                encoder_output_schema_from_single_child("PhysicalSinkEncoder", &new.base.children);
            Arc::new(PhysicalPlan::SinkEncoder(new))
        }
        PhysicalPlan::IncSinkEncoder(encoder) => {
            let mut new = encoder.clone();
            new.base.children = children;
            new.output_schema = encoder_output_schema_from_single_child(
                "PhysicalIncSinkEncoder",
                &new.base.children,
            );
            Arc::new(PhysicalPlan::IncSinkEncoder(new))
        }
        _ => rebuild_with_children(plan.as_ref(), children),
    }
}

fn encoder_output_schema_from_single_child(
    encoder_plan_type: &str,
    children: &[Arc<PhysicalPlan>],
) -> Option<Arc<OutputSchema>> {
    let [child] = children else {
        return None;
    };
    match child.output_schema() {
        Ok(output_schema) => Some(Arc::new(output_schema)),
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

impl PhysicalOptRule for ByIndexProjectionIntoRowDiffRewrite {
    fn name(&self) -> &str {
        "by_index_projection_into_row_diff_rewrite"
    }

    fn optimize(
        &self,
        plan: Arc<PhysicalPlan>,
        _encoder_registry: &EncoderRegistry,
    ) -> Arc<PhysicalPlan> {
        rewrite_by_index_projection_into_row_diff(plan)
    }
}

impl PhysicalOptRule for ByIndexProjectionAcrossMixedConsumersRewrite {
    fn name(&self) -> &str {
        "by_index_projection_across_mixed_consumers_rewrite"
    }

    fn optimize(
        &self,
        plan: Arc<PhysicalPlan>,
        encoder_registry: &EncoderRegistry,
    ) -> Arc<PhysicalPlan> {
        rewrite_by_index_projection_across_mixed_consumers(plan, encoder_registry)
    }
}

impl PhysicalOptRule for PartialByIndexRowDiffAndEncoderRewrite {
    fn name(&self) -> &str {
        "partial_by_index_row_diff_and_encoder_rewrite"
    }

    fn optimize(
        &self,
        plan: Arc<PhysicalPlan>,
        encoder_registry: &EncoderRegistry,
    ) -> Arc<PhysicalPlan> {
        rewrite_partial_by_index_row_diff_and_encoder(plan, encoder_registry)
    }
}

impl PhysicalOptRule for ColumnFilterProjectionIntersection {
    fn name(&self) -> &str {
        "column_filter_projection_intersection"
    }

    fn optimize(
        &self,
        plan: Arc<PhysicalPlan>,
        _encoder_registry: &EncoderRegistry,
    ) -> Arc<PhysicalPlan> {
        intersect_column_filter_projection(plan)
    }
}

#[derive(Clone, Debug)]
enum ProjectConsumer {
    RowDiff {
        row_diff_index: i64,
    },
    SinkEncoder {
        encoder_index: i64,
        kind: String,
        transform_enabled: bool,
    },
    Other,
}

#[derive(Clone, Debug)]
struct ByIndexRewriteState {
    projects_to_passthrough: HashSet<i64>,
    project_to_remaining_fields: HashMap<i64, Vec<PhysicalProjectField>>,
    encoder_to_projection: HashMap<i64, Arc<ByIndexProjection>>,
}

#[derive(Clone, Debug)]
struct ByIndexRowDiffRewriteState {
    projects_to_passthrough: HashSet<i64>,
    project_to_remaining_fields: HashMap<i64, Vec<PhysicalProjectField>>,
    row_diff_to_projection: HashMap<i64, Arc<ByIndexProjection>>,
}

#[derive(Clone, Debug)]
struct ByIndexMixedRewriteState {
    projects_to_passthrough: HashSet<i64>,
    project_to_remaining_fields: HashMap<i64, Vec<PhysicalProjectField>>,
    encoder_to_projection: HashMap<i64, Arc<ByIndexProjection>>,
    row_diff_to_projection: HashMap<i64, Arc<ByIndexProjection>>,
}

#[derive(Clone, Debug)]
struct ByIndexRowDiffEncoderRewriteState {
    projects_to_passthrough: HashSet<i64>,
    project_to_remaining_fields: HashMap<i64, Vec<PhysicalProjectField>>,
    encoder_to_projection: HashMap<i64, Arc<ByIndexProjection>>,
    row_diff_to_projection: HashMap<i64, Arc<ByIndexProjection>>,
}

fn rewrite_by_index_projection_across_mixed_consumers(
    plan: Arc<PhysicalPlan>,
    encoder_registry: &EncoderRegistry,
) -> Arc<PhysicalPlan> {
    let (node_map, consumer_map) = build_node_and_consumer_maps(&plan);

    let mut state = ByIndexMixedRewriteState {
        projects_to_passthrough: HashSet::new(),
        project_to_remaining_fields: HashMap::new(),
        encoder_to_projection: HashMap::new(),
        row_diff_to_projection: HashMap::new(),
    };

    'project_loop: for (node_index, node) in node_map.iter() {
        let PhysicalPlan::Project(project) = node.as_ref() else {
            continue;
        };
        let Some((columns, remaining_fields)) =
            split_by_index_projection_fields(project.fields.as_ref())
        else {
            continue;
        };

        let consumers = consumer_map.get(node_index).cloned().unwrap_or_default();
        if consumers.is_empty() {
            continue;
        }

        let has_row_diff = consumers
            .iter()
            .any(|consumer| matches!(consumer, ProjectConsumer::RowDiff { .. }));
        let has_encoder = consumers
            .iter()
            .any(|consumer| matches!(consumer, ProjectConsumer::SinkEncoder { .. }));
        if !has_row_diff || !has_encoder {
            continue;
        }

        if !consumers.iter().all(|consumer| match consumer {
            ProjectConsumer::RowDiff { .. } => true,
            ProjectConsumer::SinkEncoder {
                kind,
                transform_enabled,
                ..
            } => !transform_enabled && supports_by_index_projection(kind, encoder_registry),
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
                ProjectConsumer::RowDiff { row_diff_index } => {
                    let downstream_consumers = consumer_map
                        .get(&row_diff_index)
                        .cloned()
                        .unwrap_or_default();
                    if downstream_consumers.is_empty()
                        || !downstream_consumers
                            .iter()
                            .all(|downstream| match downstream {
                                ProjectConsumer::SinkEncoder {
                                    kind,
                                    transform_enabled,
                                    ..
                                } => {
                                    !transform_enabled
                                        && supports_by_index_projection(kind, encoder_registry)
                                }
                                ProjectConsumer::RowDiff { .. } | ProjectConsumer::Other => false,
                            })
                    {
                        continue 'project_loop;
                    }
                    state
                        .row_diff_to_projection
                        .insert(row_diff_index, Arc::clone(&spec));
                    for downstream in downstream_consumers {
                        match downstream {
                            ProjectConsumer::SinkEncoder { encoder_index, .. } => {
                                state
                                    .encoder_to_projection
                                    .insert(encoder_index, Arc::clone(&spec));
                            }
                            ProjectConsumer::RowDiff { .. } | ProjectConsumer::Other => {}
                        }
                    }
                }
                ProjectConsumer::SinkEncoder { encoder_index, .. } => {
                    state
                        .encoder_to_projection
                        .insert(encoder_index, Arc::clone(&spec));
                }
                ProjectConsumer::Other => {}
            }
        }
    }

    if state.projects_to_passthrough.is_empty()
        && state.encoder_to_projection.is_empty()
        && state.row_diff_to_projection.is_empty()
    {
        return plan;
    }

    let mut memo = HashMap::new();
    rewrite_by_index_mixed_nodes(plan, &state, &mut memo)
}

fn rewrite_partial_by_index_row_diff_and_encoder(
    plan: Arc<PhysicalPlan>,
    encoder_registry: &EncoderRegistry,
) -> Arc<PhysicalPlan> {
    let (node_map, consumer_map) = build_node_and_consumer_maps(&plan);

    let mut state = ByIndexRowDiffEncoderRewriteState {
        projects_to_passthrough: HashSet::new(),
        project_to_remaining_fields: HashMap::new(),
        encoder_to_projection: HashMap::new(),
        row_diff_to_projection: HashMap::new(),
    };

    'project_loop: for (node_index, node) in node_map.iter() {
        let PhysicalPlan::Project(project) = node.as_ref() else {
            continue;
        };
        let Some((columns, remaining_fields)) =
            split_by_index_projection_fields(project.fields.as_ref())
        else {
            continue;
        };
        if !remaining_fields.is_empty() {
            continue;
        }

        let consumers = consumer_map.get(node_index).cloned().unwrap_or_default();
        if consumers.is_empty()
            || !consumers
                .iter()
                .all(|consumer| matches!(consumer, ProjectConsumer::RowDiff { .. }))
        {
            continue;
        }

        let mut row_diff_specs = Vec::<(i64, Arc<ByIndexProjection>)>::new();
        let mut encoder_specs = Vec::<(i64, Arc<ByIndexProjection>)>::new();

        for consumer in consumers {
            let ProjectConsumer::RowDiff { row_diff_index } = consumer else {
                continue;
            };
            let Some(row_diff_node) = node_map.get(&row_diff_index) else {
                continue 'project_loop;
            };
            let PhysicalPlan::RowDiff(row_diff) = row_diff_node.as_ref() else {
                continue 'project_loop;
            };
            if row_diff.tracked_column_indexes.is_empty()
                || row_diff.tracked_column_indexes.len() >= columns.len()
            {
                continue 'project_loop;
            }

            let downstream_consumers = consumer_map
                .get(&row_diff_index)
                .cloned()
                .unwrap_or_default();
            if downstream_consumers.is_empty() {
                continue 'project_loop;
            }
            if !downstream_consumers
                .iter()
                .all(|downstream| match downstream {
                    ProjectConsumer::SinkEncoder {
                        kind,
                        transform_enabled,
                        ..
                    } => !transform_enabled && supports_by_index_projection(kind, encoder_registry),
                    ProjectConsumer::RowDiff { .. } | ProjectConsumer::Other => false,
                })
            {
                continue 'project_loop;
            }

            let tracked_indexes = row_diff
                .tracked_column_indexes
                .iter()
                .copied()
                .collect::<HashSet<_>>();
            let row_diff_columns = columns
                .iter()
                .filter(|column| tracked_indexes.contains(&column.output_index))
                .cloned()
                .collect::<Vec<_>>();
            let encoder_columns = columns
                .iter()
                .filter(|column| !tracked_indexes.contains(&column.output_index))
                .cloned()
                .collect::<Vec<_>>();
            if row_diff_columns.is_empty() || encoder_columns.is_empty() {
                continue 'project_loop;
            }

            row_diff_specs.push((
                row_diff_index,
                Arc::new(ByIndexProjection::new(row_diff_columns)),
            ));
            let encoder_spec = Arc::new(ByIndexProjection::new(encoder_columns));
            for downstream in downstream_consumers {
                match downstream {
                    ProjectConsumer::SinkEncoder { encoder_index, .. } => {
                        encoder_specs.push((encoder_index, Arc::clone(&encoder_spec)));
                    }
                    ProjectConsumer::RowDiff { .. } | ProjectConsumer::Other => {}
                }
            }
        }

        if row_diff_specs.is_empty() || encoder_specs.is_empty() {
            continue;
        }

        state.projects_to_passthrough.insert(*node_index);
        state
            .project_to_remaining_fields
            .insert(*node_index, remaining_fields);
        for (row_diff_index, spec) in row_diff_specs {
            state.row_diff_to_projection.insert(row_diff_index, spec);
        }
        for (encoder_index, spec) in encoder_specs {
            state.encoder_to_projection.insert(encoder_index, spec);
        }
    }

    if state.projects_to_passthrough.is_empty()
        && state.encoder_to_projection.is_empty()
        && state.row_diff_to_projection.is_empty()
    {
        return plan;
    }

    let mut memo = HashMap::new();
    rewrite_by_index_row_diff_encoder_nodes(plan, &state, &mut memo)
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
        let Some((columns, remaining_fields)) =
            split_by_index_projection_fields(project.fields.as_ref())
        else {
            continue;
        };

        let consumers = consumer_map.get(node_index).cloned().unwrap_or_default();
        if consumers.is_empty() {
            continue;
        }

        // Design constraint: when a `Project` is shared (DAG), only apply this rewrite
        // if every consumer is an encoder that can honor delayed materialization.
        if !consumers.iter().all(|consumer| match consumer {
            ProjectConsumer::RowDiff { .. } => false,
            ProjectConsumer::SinkEncoder {
                kind,
                transform_enabled,
                ..
            } => !transform_enabled && supports_by_index_projection(kind, encoder_registry),
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
                ProjectConsumer::RowDiff { .. } => {}
                ProjectConsumer::SinkEncoder { encoder_index, .. } => {
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

fn rewrite_by_index_projection_into_row_diff(plan: Arc<PhysicalPlan>) -> Arc<PhysicalPlan> {
    let (node_map, consumer_map) = build_node_and_consumer_maps(&plan);

    let mut state = ByIndexRowDiffRewriteState {
        projects_to_passthrough: HashSet::new(),
        project_to_remaining_fields: HashMap::new(),
        row_diff_to_projection: HashMap::new(),
    };

    for (node_index, node) in node_map.iter() {
        let PhysicalPlan::Project(project) = node.as_ref() else {
            continue;
        };
        let Some((columns, remaining_fields)) =
            split_by_index_projection_fields(project.fields.as_ref())
        else {
            continue;
        };

        let consumers = consumer_map.get(node_index).cloned().unwrap_or_default();
        if consumers.is_empty() {
            continue;
        }

        if !consumers
            .iter()
            .all(|consumer| matches!(consumer, ProjectConsumer::RowDiff { .. }))
        {
            continue;
        }

        let spec = Arc::new(ByIndexProjection::new(columns));
        state.projects_to_passthrough.insert(*node_index);
        state
            .project_to_remaining_fields
            .insert(*node_index, remaining_fields);
        for consumer in consumers {
            if let ProjectConsumer::RowDiff { row_diff_index } = consumer {
                state
                    .row_diff_to_projection
                    .insert(row_diff_index, Arc::clone(&spec));
            }
        }
    }

    if state.projects_to_passthrough.is_empty() && state.row_diff_to_projection.is_empty() {
        return plan;
    }

    let mut memo = HashMap::new();
    rewrite_by_index_row_diff_nodes(plan, &state, &mut memo)
}

fn supports_by_index_projection(kind: &str, encoder_registry: &EncoderRegistry) -> bool {
    encoder_registry.supports_by_index_projection(kind)
}

fn by_index_projection_column_from_field(
    field: &PhysicalProjectField,
    output_index: usize,
) -> Option<ByIndexProjectionColumn> {
    let ScalarExpr::Column(ColumnRef::ByIndex {
        source_name,
        column_index,
    }) = &field.compiled_expr
    else {
        return None;
    };

    Some(ByIndexProjectionColumn::new(
        source_name.as_str(),
        *column_index,
        output_index,
        field.original_expr.to_string(),
        Arc::clone(&field.field_name),
    ))
}

fn split_by_index_projection_fields(
    fields: &[PhysicalProjectField],
) -> Option<(Vec<ByIndexProjectionColumn>, Vec<PhysicalProjectField>)> {
    if fields.is_empty() {
        return None;
    }

    if fields
        .iter()
        .any(|field| matches!(&field.compiled_expr, ScalarExpr::Wildcard { .. }))
    {
        return None;
    }

    let mut columns = Vec::new();
    let mut remaining_fields = Vec::new();
    for (output_index, field) in fields.iter().enumerate() {
        if is_by_index_field(field) {
            if let Some(column) = by_index_projection_column_from_field(field, output_index) {
                columns.push(column);
            }
        } else {
            remaining_fields.push(field.clone());
        }
    }

    if columns.is_empty() {
        return None;
    }

    Some((columns, remaining_fields))
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
        let inherited_consumers = consumers.get(&index).cloned().unwrap_or_default();

        for child in plan.children() {
            let child_index = child.get_plan_index();
            let child_consumers = match plan.as_ref() {
                PhysicalPlan::EmptySuppress(_)
                | PhysicalPlan::Batch(_)
                | PhysicalPlan::ColumnFilter(_) => inherited_consumers.clone(),
                PhysicalPlan::RowDiff(row_diff) => vec![ProjectConsumer::RowDiff {
                    row_diff_index: row_diff.base.index(),
                }],
                PhysicalPlan::SinkEncoder(encoder) => vec![ProjectConsumer::SinkEncoder {
                    encoder_index: encoder.base.index(),
                    kind: encoder.encoder.kind_str().to_string(),
                    transform_enabled: encoder.encoder.transform_kind().is_some(),
                }],
                PhysicalPlan::IncSinkEncoder(encoder) => vec![ProjectConsumer::SinkEncoder {
                    encoder_index: encoder.base.index(),
                    kind: encoder.encoder.kind_str().to_string(),
                    transform_enabled: encoder.encoder.transform_kind().is_some(),
                }],
                _ => vec![ProjectConsumer::Other],
            };
            consumers
                .entry(child_index)
                .or_default()
                .extend(child_consumers);

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
                .unwrap_or_default()
                .into();
            new.passthrough_messages = true;
            Arc::new(PhysicalPlan::Project(new))
        }
        PhysicalPlan::SinkEncoder(encoder) if state.encoder_to_projection.contains_key(&index) => {
            let mut new = encoder.clone();
            new.base.children = rewritten_children;
            new.by_index_projection = state.encoder_to_projection.get(&index).cloned();
            Arc::new(PhysicalPlan::SinkEncoder(new))
        }
        PhysicalPlan::IncSinkEncoder(encoder)
            if state.encoder_to_projection.contains_key(&index) =>
        {
            let mut new = encoder.clone();
            new.base.children = rewritten_children;
            new.by_index_projection = state.encoder_to_projection.get(&index).cloned();
            Arc::new(PhysicalPlan::IncSinkEncoder(new))
        }
        _ => rebuild_with_children(plan.as_ref(), rewritten_children),
    };

    memo.insert(index, Arc::clone(&rebuilt));
    rebuilt
}

fn rewrite_by_index_row_diff_nodes(
    plan: Arc<PhysicalPlan>,
    state: &ByIndexRowDiffRewriteState,
    memo: &mut HashMap<i64, Arc<PhysicalPlan>>,
) -> Arc<PhysicalPlan> {
    let index = plan.get_plan_index();
    if let Some(rewritten) = memo.get(&index) {
        return Arc::clone(rewritten);
    }

    let rewritten_children = plan
        .children()
        .iter()
        .map(|child| rewrite_by_index_row_diff_nodes(Arc::clone(child), state, memo))
        .collect::<Vec<_>>();

    let rebuilt = match plan.as_ref() {
        PhysicalPlan::Project(project) if state.projects_to_passthrough.contains(&index) => {
            let mut new = project.clone();
            new.base.children = rewritten_children;
            new.fields = state
                .project_to_remaining_fields
                .get(&index)
                .cloned()
                .unwrap_or_default()
                .into();
            new.passthrough_messages = true;
            Arc::new(PhysicalPlan::Project(new))
        }
        PhysicalPlan::RowDiff(row_diff) if state.row_diff_to_projection.contains_key(&index) => {
            let mut new = row_diff.clone();
            new.base.children = rewritten_children;
            new.late_projection = state.row_diff_to_projection.get(&index).cloned();
            Arc::new(PhysicalPlan::RowDiff(new))
        }
        _ => rebuild_with_children(plan.as_ref(), rewritten_children),
    };

    memo.insert(index, Arc::clone(&rebuilt));
    rebuilt
}

fn rewrite_by_index_mixed_nodes(
    plan: Arc<PhysicalPlan>,
    state: &ByIndexMixedRewriteState,
    memo: &mut HashMap<i64, Arc<PhysicalPlan>>,
) -> Arc<PhysicalPlan> {
    let index = plan.get_plan_index();
    if let Some(rewritten) = memo.get(&index) {
        return Arc::clone(rewritten);
    }

    let rewritten_children = plan
        .children()
        .iter()
        .map(|child| rewrite_by_index_mixed_nodes(Arc::clone(child), state, memo))
        .collect::<Vec<_>>();

    let rebuilt = match plan.as_ref() {
        PhysicalPlan::Project(project) if state.projects_to_passthrough.contains(&index) => {
            let mut new = project.clone();
            new.base.children = rewritten_children;
            new.fields = state
                .project_to_remaining_fields
                .get(&index)
                .cloned()
                .unwrap_or_default()
                .into();
            new.passthrough_messages = true;
            Arc::new(PhysicalPlan::Project(new))
        }
        PhysicalPlan::RowDiff(row_diff) if state.row_diff_to_projection.contains_key(&index) => {
            let mut new = row_diff.clone();
            new.base.children = rewritten_children;
            new.late_projection = state.row_diff_to_projection.get(&index).cloned();
            Arc::new(PhysicalPlan::RowDiff(new))
        }
        PhysicalPlan::SinkEncoder(encoder) if state.encoder_to_projection.contains_key(&index) => {
            let mut new = encoder.clone();
            new.base.children = rewritten_children;
            new.by_index_projection = state.encoder_to_projection.get(&index).cloned();
            Arc::new(PhysicalPlan::SinkEncoder(new))
        }
        PhysicalPlan::IncSinkEncoder(encoder)
            if state.encoder_to_projection.contains_key(&index) =>
        {
            let mut new = encoder.clone();
            new.base.children = rewritten_children;
            new.by_index_projection = state.encoder_to_projection.get(&index).cloned();
            Arc::new(PhysicalPlan::IncSinkEncoder(new))
        }
        _ => rebuild_with_children(plan.as_ref(), rewritten_children),
    };

    memo.insert(index, Arc::clone(&rebuilt));
    rebuilt
}

fn rewrite_by_index_row_diff_encoder_nodes(
    plan: Arc<PhysicalPlan>,
    state: &ByIndexRowDiffEncoderRewriteState,
    memo: &mut HashMap<i64, Arc<PhysicalPlan>>,
) -> Arc<PhysicalPlan> {
    let index = plan.get_plan_index();
    if let Some(rewritten) = memo.get(&index) {
        return Arc::clone(rewritten);
    }

    let rewritten_children = plan
        .children()
        .iter()
        .map(|child| rewrite_by_index_row_diff_encoder_nodes(Arc::clone(child), state, memo))
        .collect::<Vec<_>>();

    let rebuilt = match plan.as_ref() {
        PhysicalPlan::Project(project) if state.projects_to_passthrough.contains(&index) => {
            let mut new = project.clone();
            new.base.children = rewritten_children;
            new.fields = state
                .project_to_remaining_fields
                .get(&index)
                .cloned()
                .unwrap_or_default()
                .into();
            new.passthrough_messages = true;
            Arc::new(PhysicalPlan::Project(new))
        }
        PhysicalPlan::RowDiff(row_diff) if state.row_diff_to_projection.contains_key(&index) => {
            let mut new = row_diff.clone();
            new.base.children = rewritten_children;
            new.late_projection = state.row_diff_to_projection.get(&index).cloned();
            Arc::new(PhysicalPlan::RowDiff(new))
        }
        PhysicalPlan::SinkEncoder(encoder) if state.encoder_to_projection.contains_key(&index) => {
            let mut new = encoder.clone();
            new.base.children = rewritten_children;
            new.by_index_projection = state.encoder_to_projection.get(&index).cloned();
            Arc::new(PhysicalPlan::SinkEncoder(new))
        }
        PhysicalPlan::IncSinkEncoder(encoder)
            if state.encoder_to_projection.contains_key(&index) =>
        {
            let mut new = encoder.clone();
            new.base.children = rewritten_children;
            new.by_index_projection = state.encoder_to_projection.get(&index).cloned();
            Arc::new(PhysicalPlan::IncSinkEncoder(new))
        }
        _ => rebuild_with_children(plan.as_ref(), rewritten_children),
    };

    memo.insert(index, Arc::clone(&rebuilt));
    rebuilt
}

fn is_by_index_field(field: &PhysicalProjectField) -> bool {
    matches!(
        &field.compiled_expr,
        ScalarExpr::Column(ColumnRef::ByIndex { .. })
    )
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::encoder::{EncodeError, SinkEncoder, SinkEncoderFactory};
    use crate::model::Collection;
    use crate::planner::physical::{
        output_schema::OutputSchema, PhysicalBatch, PhysicalDataSource, PhysicalProject,
        PhysicalSinkEncoder,
    };
    use crate::planner::sink::{CommonSinkProps, SinkEncoderConfig};
    use bytes::Bytes;
    use datatypes::{ColumnSchema, ConcreteDatatype, Int64Type, Schema};
    use serde_json::Map as JsonMap;

    struct TestEncoderFactory;

    impl SinkEncoderFactory for TestEncoderFactory {
        fn id(&self) -> &str {
            "test_by_index"
        }

        fn start_encoder(&self) -> Result<Box<dyn SinkEncoder>, EncodeError> {
            Ok(Box::new(TestEncoder))
        }

        fn supports_index_lazy_materialization(&self) -> bool {
            true
        }

        fn with_by_index_projection(
            self: Arc<Self>,
            _spec: Arc<ByIndexProjection>,
        ) -> Result<Arc<dyn SinkEncoderFactory>, EncodeError> {
            Ok(self)
        }

        fn with_output_schema(
            self: Arc<Self>,
            _output_schema: Arc<OutputSchema>,
        ) -> Result<Arc<dyn SinkEncoderFactory>, EncodeError> {
            Ok(self)
        }
    }

    struct TestEncoder;

    impl SinkEncoder for TestEncoder {
        fn begin_delivery(&mut self) -> Result<Option<Bytes>, EncodeError> {
            Ok(None)
        }

        fn append(&mut self, _record: &dyn Collection) -> Result<Option<Bytes>, EncodeError> {
            Ok(None)
        }

        fn finish_delivery(&mut self) -> Result<Option<Bytes>, EncodeError> {
            Ok(None)
        }
    }

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "s".to_string(),
                "a".to_string(),
                ConcreteDatatype::Int64(Int64Type),
            ),
            ColumnSchema::new(
                "s".to_string(),
                "b".to_string(),
                ConcreteDatatype::Int64(Int64Type),
            ),
        ]))
    }

    #[test]
    fn by_index_projection_rewrite_treats_batch_as_transparent() {
        let registry = EncoderRegistry::new();
        registry.register_encoder_with_caps(
            "test_by_index",
            Arc::new(|_config| Ok(Arc::new(TestEncoderFactory) as Arc<dyn SinkEncoderFactory>)),
            true,
        );

        let source = Arc::new(PhysicalPlan::DataSource(PhysicalDataSource::new(
            "s".to_string(),
            None,
            test_schema(),
            None,
            0,
        )));
        let project = Arc::new(PhysicalPlan::Project(PhysicalProject::new(
            vec![PhysicalProjectField::new(
                "a",
                sqlparser::ast::Expr::Identifier(sqlparser::ast::Ident::new("a")),
                ScalarExpr::Column(ColumnRef::ByIndex {
                    source_name: "s".to_string(),
                    column_index: 0,
                }),
            )],
            vec![source],
            1,
        )));
        let batch = Arc::new(PhysicalPlan::Batch(PhysicalBatch::new(
            vec![project],
            2,
            "sink".to_string(),
            CommonSinkProps {
                batch_count: Some(10),
                batch_duration: None,
            },
        )));
        let encoder = Arc::new(PhysicalPlan::SinkEncoder(PhysicalSinkEncoder::new(
            vec![batch],
            3,
            "sink".to_string(),
            SinkEncoderConfig::new("test_by_index", JsonMap::new()),
            CommonSinkProps::default(),
        )));

        let optimized = rewrite_by_index_projection_into_encoder(encoder, &registry);
        let PhysicalPlan::SinkEncoder(encoder) = optimized.as_ref() else {
            panic!("expected sink encoder");
        };
        assert!(
            encoder.by_index_projection.is_some(),
            "encoder should receive by-index projection through Batch"
        );
        let PhysicalPlan::Batch(batch) = encoder.base.children()[0].as_ref() else {
            panic!("expected batch child");
        };
        let PhysicalPlan::Project(project) = batch.base.children()[0].as_ref() else {
            panic!("expected project child");
        };
        assert!(project.passthrough_messages);
        assert!(project.fields.is_empty());
    }

    #[test]
    fn optimized_encoder_preserves_output_schema_after_by_index_rewrite() {
        let registry = EncoderRegistry::new();
        registry.register_encoder_with_caps(
            "test_by_index",
            Arc::new(|_config| Ok(Arc::new(TestEncoderFactory) as Arc<dyn SinkEncoderFactory>)),
            true,
        );

        let source = Arc::new(PhysicalPlan::DataSource(PhysicalDataSource::new(
            "s".to_string(),
            None,
            test_schema(),
            None,
            0,
        )));
        let project = Arc::new(PhysicalPlan::Project(PhysicalProject::new(
            vec![
                PhysicalProjectField::new(
                    "a",
                    sqlparser::ast::Expr::Identifier(sqlparser::ast::Ident::new("a")),
                    ScalarExpr::Column(ColumnRef::ByIndex {
                        source_name: "s".to_string(),
                        column_index: 0,
                    }),
                ),
                PhysicalProjectField::new(
                    "b",
                    sqlparser::ast::Expr::Identifier(sqlparser::ast::Ident::new("b")),
                    ScalarExpr::Column(ColumnRef::ByIndex {
                        source_name: "s".to_string(),
                        column_index: 1,
                    }),
                ),
            ],
            vec![source],
            1,
        )));
        let encoder = Arc::new(PhysicalPlan::SinkEncoder(PhysicalSinkEncoder::new(
            vec![project],
            2,
            "sink".to_string(),
            SinkEncoderConfig::new("test_by_index", JsonMap::new()),
            CommonSinkProps::default(),
        )));

        let optimized = optimize_physical_plan(
            encoder,
            &registry,
            AggregateFunctionRegistry::with_builtins(),
        );
        let PhysicalPlan::SinkEncoder(encoder) = optimized.as_ref() else {
            panic!("expected sink encoder");
        };
        let output_schema = encoder
            .output_schema
            .as_ref()
            .expect("encoder output schema should be attached");
        let names = output_schema
            .columns
            .iter()
            .map(|column| column.name.as_ref())
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["a", "b"]);
        assert!(
            encoder.by_index_projection.is_some(),
            "encoder should still receive by-index projection"
        );
        let PhysicalPlan::Project(project) = encoder.base.children()[0].as_ref() else {
            panic!("expected project child");
        };
        assert!(project.passthrough_messages);
        assert!(project.fields.is_empty());
    }
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

/// Intersect the include/exclude columns of each `PhysicalColumnFilter` with the
/// by-index projection spec on the nearest upstream projection-carrying node.
fn intersect_column_filter_projection(plan: Arc<PhysicalPlan>) -> Arc<PhysicalPlan> {
    let (mut node_map, mut parent_map) = build_node_and_parent_maps(&plan);

    struct PendingChange {
        target_index: i64,
        new_target: Arc<PhysicalPlan>,
        rewire: Option<(i64, i64, Arc<PhysicalPlan>)>,
    }

    let mut changes: Vec<PendingChange> = Vec::new();

    for (filter_index, filter_node) in &node_map {
        let PhysicalPlan::ColumnFilter(filter) = filter_node.as_ref() else {
            continue;
        };

        let carriers = find_all_projection_carriers_above(*filter_index, &node_map, &parent_map);

        if carriers.is_empty() {
            continue;
        }

        let filter_schema = match filter_node.output_schema() {
            Ok(s) => s,
            Err(_) => continue,
        };

        let keep_names = build_keep_names_set(
            &filter_schema,
            filter.include_columns.as_deref(),
            filter.exclude_columns.as_deref(),
        );

        let shared_schema = match filter_node
            .children()
            .first()
            .and_then(|c| c.output_schema().ok())
        {
            Some(s) => s,
            None => continue,
        };

        // Crop and re-index the projection on each carrier.
        let filter_child = filter_node.children().first().cloned();
        let filter_parent_idx = parent_map.get(filter_index).copied();

        for &(target_index, ref target_kind) in &carriers {
            let projection = match target_kind {
                ProjectionCarrierKind::RowDiff => {
                    let Some(target_node) = node_map.get(&target_index) else {
                        continue;
                    };
                    let PhysicalPlan::RowDiff(rd) = target_node.as_ref() else {
                        continue;
                    };
                    rd.late_projection.clone()
                }
                ProjectionCarrierKind::SinkEncoder => {
                    let Some(target_node) = node_map.get(&target_index) else {
                        continue;
                    };
                    match target_node.as_ref() {
                        PhysicalPlan::SinkEncoder(enc) => enc.by_index_projection.clone(),
                        PhysicalPlan::IncSinkEncoder(enc) => enc.by_index_projection.clone(),
                        _ => continue,
                    }
                }
            };

            let Some(projection) = projection else {
                // No projection to crop, but carrier still exists.
                // For SinkEncoder with now-empty projection, clear it.
                if matches!(target_kind, ProjectionCarrierKind::SinkEncoder) {
                    if let Some(cleared) =
                        make_encoder_with_empty_projection(target_index, &node_map)
                    {
                        changes.push(PendingChange {
                            target_index,
                            new_target: cleared,
                            rewire: None,
                        });
                    }
                }
                continue;
            };

            let reindexed = crop_and_reindex_projection(
                projection.as_ref(),
                &keep_names,
                &shared_schema,
                &filter_schema,
            );

            let new_target = match target_kind {
                ProjectionCarrierKind::RowDiff => {
                    let Some(target_node) = node_map.get(&target_index) else {
                        continue;
                    };
                    let PhysicalPlan::RowDiff(rd) = target_node.as_ref() else {
                        continue;
                    };
                    let mut new_rd = rd.clone();
                    new_rd.late_projection = Some(Arc::new(reindexed));
                    Arc::new(PhysicalPlan::RowDiff(new_rd))
                }
                ProjectionCarrierKind::SinkEncoder => {
                    let Some(target_node) = node_map.get(&target_index) else {
                        continue;
                    };
                    match target_node.as_ref() {
                        PhysicalPlan::SinkEncoder(enc) => {
                            let mut new_enc = enc.clone();
                            new_enc.by_index_projection = Some(Arc::new(reindexed));
                            Arc::new(PhysicalPlan::SinkEncoder(new_enc))
                        }
                        PhysicalPlan::IncSinkEncoder(enc) => {
                            let mut new_enc = enc.clone();
                            new_enc.by_index_projection = Some(Arc::new(reindexed));
                            Arc::new(PhysicalPlan::IncSinkEncoder(new_enc))
                        }
                        _ => continue,
                    }
                }
            };

            // Only the nearest carrier (first in the list) needs rewiring.
            let rewire = if target_index == carriers[0].0 {
                match (&filter_child, filter_parent_idx) {
                    (Some(child), Some(p)) => Some((p, *filter_index, Arc::clone(child))),
                    _ => None,
                }
            } else {
                None
            };

            changes.push(PendingChange {
                target_index,
                new_target,
                rewire,
            });
        }
    }

    for change in &changes {
        node_map.insert(change.target_index, Arc::clone(&change.new_target));
        if let Some((parent_idx, old_filter_idx, ref new_child)) = &change.rewire {
            if let Some(parent_node) = node_map.get(parent_idx) {
                let new_parent = replace_child(parent_node, *old_filter_idx, Arc::clone(new_child));
                node_map.insert(*parent_idx, new_parent);
            }
        }
    }

    for (filter_index, filter_node) in &node_map {
        if matches!(filter_node.as_ref(), PhysicalPlan::ColumnFilter(_)) {
            parent_map.remove(filter_index);
        }
    }

    rebuild_from_maps(plan.get_plan_index(), &node_map, &parent_map)
}

#[derive(Clone, Copy, Debug)]
enum ProjectionCarrierKind {
    RowDiff,
    SinkEncoder,
}

fn find_all_projection_carriers_above(
    start_index: i64,
    node_map: &HashMap<i64, Arc<PhysicalPlan>>,
    parent_map: &HashMap<i64, i64>,
) -> Vec<(i64, ProjectionCarrierKind)> {
    let mut carriers = Vec::new();
    let mut current = start_index;
    while let Some(&parent_idx) = parent_map.get(&current) {
        let Some(parent) = node_map.get(&parent_idx) else {
            break;
        };
        match parent.as_ref() {
            PhysicalPlan::RowDiff(rd) if rd.late_projection.is_some() => {
                carriers.push((parent_idx, ProjectionCarrierKind::RowDiff));
                current = parent_idx;
                continue;
            }
            PhysicalPlan::SinkEncoder(enc) if enc.by_index_projection.is_some() => {
                carriers.push((parent_idx, ProjectionCarrierKind::SinkEncoder));
                current = parent_idx;
                continue;
            }
            PhysicalPlan::IncSinkEncoder(enc) if enc.by_index_projection.is_some() => {
                carriers.push((parent_idx, ProjectionCarrierKind::SinkEncoder));
                current = parent_idx;
                continue;
            }
            PhysicalPlan::EmptySuppress(_) | PhysicalPlan::Batch(_) => {
                current = parent_idx;
                continue;
            }
            _ => break,
        }
    }
    carriers
}

fn make_encoder_with_empty_projection(
    target_index: i64,
    node_map: &HashMap<i64, Arc<PhysicalPlan>>,
) -> Option<Arc<PhysicalPlan>> {
    let target_node = node_map.get(&target_index)?;
    match target_node.as_ref() {
        PhysicalPlan::SinkEncoder(enc) => {
            let mut new_enc = enc.clone();
            new_enc.by_index_projection = None;
            Some(Arc::new(PhysicalPlan::SinkEncoder(new_enc)))
        }
        PhysicalPlan::IncSinkEncoder(enc) => {
            let mut new_enc = enc.clone();
            new_enc.by_index_projection = None;
            Some(Arc::new(PhysicalPlan::IncSinkEncoder(new_enc)))
        }
        _ => None,
    }
}

fn build_keep_names_set(
    filter_schema: &OutputSchema,
    include_columns: Option<&[String]>,
    exclude_columns: Option<&[String]>,
) -> HashSet<String> {
    match (include_columns, exclude_columns) {
        (Some(include), None) => include.iter().cloned().collect(),
        (None, Some(exclude)) => {
            let exclude_set: HashSet<&str> = exclude.iter().map(|s| s.as_str()).collect();
            filter_schema
                .columns
                .iter()
                .filter(|c| !exclude_set.contains(c.name.as_ref()))
                .map(|c| c.name.as_ref().to_string())
                .collect()
        }
        (None, None) => filter_schema
            .columns
            .iter()
            .map(|c| c.name.as_ref().to_string())
            .collect(),
        (Some(_), Some(_)) => HashSet::new(),
    }
}

fn crop_and_reindex_projection(
    projection: &ByIndexProjection,
    keep_names: &HashSet<String>,
    shared_schema: &OutputSchema,
    filter_schema: &OutputSchema,
) -> ByIndexProjection {
    let mut reindexed = Vec::new();
    for col in projection.columns() {
        let col_name = match shared_schema.columns.get(col.output_index) {
            Some(c) => c.name.as_ref(),
            None => continue,
        };
        if !keep_names.contains(col_name) {
            continue;
        }
        if let Some(actual_idx) = filter_schema
            .columns
            .iter()
            .position(|c| c.name.as_ref() == col_name)
        {
            reindexed.push(ByIndexProjectionColumn::new(
                col.source_name.as_ref(),
                col.column_index,
                actual_idx,
                col.source_column_display.as_ref(),
                col.output_name.as_ref(),
            ));
        }
    }
    ByIndexProjection::new(reindexed)
}

fn build_node_and_parent_maps(
    root: &Arc<PhysicalPlan>,
) -> (HashMap<i64, Arc<PhysicalPlan>>, HashMap<i64, i64>) {
    let mut nodes = HashMap::new();
    let mut parents = HashMap::new();
    let mut visited = HashSet::new();
    build_maps_helper(root, None, &mut nodes, &mut parents, &mut visited);
    (nodes, parents)
}

fn build_maps_helper(
    plan: &Arc<PhysicalPlan>,
    parent_index: Option<i64>,
    nodes: &mut HashMap<i64, Arc<PhysicalPlan>>,
    parents: &mut HashMap<i64, i64>,
    visited: &mut HashSet<i64>,
) {
    let index = plan.get_plan_index();
    if let Some(p_idx) = parent_index {
        parents.insert(index, p_idx);
    }
    let already_visited = !visited.insert(index);
    nodes.entry(index).or_insert_with(|| Arc::clone(plan));
    if !already_visited {
        for child in plan.children() {
            build_maps_helper(child, Some(index), nodes, parents, visited);
        }
    }
}

fn replace_child(
    parent: &Arc<PhysicalPlan>,
    old_child_index: i64,
    new_child: Arc<PhysicalPlan>,
) -> Arc<PhysicalPlan> {
    let new_children: Vec<_> = parent
        .children()
        .iter()
        .map(|c| {
            if c.get_plan_index() == old_child_index {
                Arc::clone(&new_child)
            } else {
                Arc::clone(c)
            }
        })
        .collect();
    rebuild_with_children(parent, new_children)
}

fn rebuild_from_maps(
    root_index: i64,
    node_map: &HashMap<i64, Arc<PhysicalPlan>>,
    parent_map: &HashMap<i64, i64>,
) -> Arc<PhysicalPlan> {
    let mut visited = HashMap::new();
    rebuild_from_maps_recursive(root_index, node_map, parent_map, &mut visited)
}

fn rebuild_from_maps_recursive(
    index: i64,
    node_map: &HashMap<i64, Arc<PhysicalPlan>>,
    parent_map: &HashMap<i64, i64>,
    visited: &mut HashMap<i64, Arc<PhysicalPlan>>,
) -> Arc<PhysicalPlan> {
    if let Some(cached) = visited.get(&index) {
        return Arc::clone(cached);
    }
    let node = node_map
        .get(&index)
        .cloned()
        .expect("internal error: node not found during plan rebuild");
    let new_children: Vec<_> = node
        .children()
        .iter()
        .map(|c| {
            let c_idx = c.get_plan_index();
            if parent_map.contains_key(&c_idx) {
                rebuild_from_maps_recursive(c_idx, node_map, parent_map, visited)
            } else {
                Arc::clone(c)
            }
        })
        .collect();
    let rebuilt = rebuild_with_children(&node, new_children);
    visited.insert(index, Arc::clone(&rebuilt));
    rebuilt
}
