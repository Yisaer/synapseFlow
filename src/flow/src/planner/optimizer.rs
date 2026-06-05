use crate::aggregation::AggregateFunctionRegistry;
use crate::codec::EncoderRegistry;
use crate::expr::scalar::ColumnRef;
use crate::expr::ScalarExpr;
use crate::planner::physical::{
    ByIndexProjection, ByIndexProjectionColumn, PhysicalBarrier, PhysicalPlan,
    PhysicalProjectField, PhysicalStreamingAggregation, StreamingWindowSpec,
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
        Box::new(ByIndexProjectionAcrossMixedConsumersRewrite),
        Box::new(PartialByIndexRowDiffAndEncoderRewrite),
        Box::new(ByIndexProjectionIntoRowDiffRewrite),
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

/// Rule: fuse `PhysicalBatch -> PhysicalSinkEncoder` into `PhysicalIncSinkEncoder`
/// when the encoder supports streaming delivery.
///
/// This eliminates one data-pass between BatchProcessor and SinkEncoderProcessor
/// for streaming-capable encoders (e.g. json, protobuf). Non-streaming encoders
/// (future Parquet) keep the two-node chain intact.
struct StreamingEncoderRewrite;

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
        fuse_streaming_encoder(plan, encoder_registry)
    }
}

fn fuse_streaming_encoder(
    plan: Arc<PhysicalPlan>,
    encoder_registry: &EncoderRegistry,
) -> Arc<PhysicalPlan> {
    let children: Vec<Arc<PhysicalPlan>> = plan
        .children()
        .iter()
        .map(|child| fuse_streaming_encoder(Arc::clone(child), encoder_registry))
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

            let encoder_kind = encoder.encoder.kind_str();
            if !encoder_registry.supports_streaming(encoder_kind) {
                // Encoder does not support streaming, keep PhysicalBatch -> SinkEncoder intact
                return rebuild_with_children(plan.as_ref(), children);
            }

            // Fuse PhysicalBatch into PhysicalIncSinkEncoder:
            // - take PhysicalBatch's children (skip the batch node)
            // - take PhysicalBatch's common (batch params)
            // - create PhysicalIncSinkEncoder with same sink_id, encoder, index
            if let PhysicalPlan::Batch(batch) = first_child.as_ref() {
                let batch_children = first_child.children().to_vec();
                let fused_index = encoder.base.index();
                let fused = crate::planner::physical::PhysicalIncSinkEncoder::new(
                    batch_children,
                    fused_index,
                    encoder.sink_id.clone(),
                    encoder.encoder.clone(),
                    batch.common.clone(),
                );
                Arc::new(PhysicalPlan::IncSinkEncoder(fused))
            } else {
                rebuild_with_children(plan.as_ref(), children)
            }
        }
        _ => rebuild_with_children(plan.as_ref(), children),
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
                PhysicalPlan::EmptySuppress(_) => inherited_consumers.clone(),
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
