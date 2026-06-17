use crate::catalog::StreamType;
use crate::codec::{
    VIDEO_FORMAT_COLUMN, VIDEO_HEIGHT_COLUMN, VIDEO_PAYLOAD_COLUMN, VIDEO_TIMESTAMP_COLUMN,
    VIDEO_WIDTH_COLUMN,
};
use crate::expr::internal_columns::is_internal_derived;
use crate::expr::sql_conversion::{SchemaBinding, SchemaBindingEntry};
use crate::planner::decode_projection::{DecodeProjection, FieldPath, FieldPathSegment, ListIndex};
use crate::planner::logical::{LogicalPlan, TailPlan};
use crate::planner::sink::SinkConnectorConfig;
use crate::shared_stream::SharedStreamRegistry;
use datatypes::Schema;
use sqlparser::ast::{
    Expr as SqlExpr, FunctionArg, FunctionArgExpr, Ident, ObjectName, WindowType,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// A logical optimization rule.
trait LogicalOptRule {
    fn name(&self) -> &str;
    fn optimize(
        &self,
        plan: Arc<LogicalPlan>,
        bindings: &SchemaBinding,
    ) -> (Arc<LogicalPlan>, SchemaBinding);
}

/// Apply logical plan optimizations and return the optimized plan and bindings.
pub fn optimize_logical_plan(
    logical_plan: Arc<LogicalPlan>,
    bindings: &SchemaBinding,
) -> (Arc<LogicalPlan>, SchemaBinding) {
    optimize_logical_plan_with_options(logical_plan, bindings, &LogicalOptimizerOptions::default())
}

#[derive(Clone, Copy, Default)]
pub struct LogicalOptimizerOptions<'a> {
    pub eventtime_enabled: bool,
    /// Shared-stream registry, used to assign append-only output-slice slots for
    /// shared CAN/GBF sources (VF-56). `None` keeps the legacy full-width schema.
    /// Actual narrowing is additionally gated by [`shared_slice_projection_enabled`].
    pub shared_slice_registry: Option<&'a SharedStreamRegistry>,
    /// Override the VF-56 shared-stream slice projection gate.
    ///
    /// `None` uses the process-level `VF_SHARED_SLICE_PROJECTION` environment gate.
    pub shared_slice_projection_enabled: Option<bool>,
}

/// Feature gate for shared-stream output slice projection (VF-56). Off unless
/// `VF_SHARED_SLICE_PROJECTION` is `1`/`true`, so the planner + decoder change can
/// land incrementally without altering default behavior until validated.
pub(crate) fn shared_slice_projection_enabled() -> bool {
    std::env::var("VF_SHARED_SLICE_PROJECTION")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

/// Apply logical plan optimizations with extra options (e.g. eventtime).
pub fn optimize_logical_plan_with_options<'a>(
    logical_plan: Arc<LogicalPlan>,
    bindings: &SchemaBinding,
    options: &LogicalOptimizerOptions<'a>,
) -> (Arc<LogicalPlan>, SchemaBinding) {
    let slice_projection_enabled = options
        .shared_slice_projection_enabled
        .unwrap_or_else(shared_slice_projection_enabled);
    let slice_registry = if slice_projection_enabled {
        options.shared_slice_registry
    } else {
        None
    };
    let rules: Vec<Box<dyn LogicalOptRule + 'a>> = vec![
        // Must run before pruning rules; CSE can introduce new expressions whose source columns
        // must be accounted for when pruning schemas.
        Box::new(CommonSubexpressionElimination),
        Box::new(TopLevelColumnPruning {
            eventtime_enabled: options.eventtime_enabled,
            slice_registry,
        }),
        Box::new(StructFieldPruning),
        Box::new(ListElementPruning),
        Box::new(EliminateVideoSinkIdentityProject),
    ];

    let mut current_plan = logical_plan;
    let mut current_bindings = bindings.clone();
    for rule in rules {
        let _ = rule.name();
        let (next_plan, next_bindings) = rule.optimize(current_plan, &current_bindings);
        current_plan = next_plan;
        current_bindings = next_bindings;
    }

    (current_plan, current_bindings)
}

/// Rule: eliminate repeated common subexpressions by computing them once into the tuple affiliate.
///
/// This inserts a `LogicalCompute` node that materializes CSE temps (`__vf_cse_N`) and rewrites
/// expressions in `Project` (and its adjacent `Filter`, if needed) to reference those temps.
struct CommonSubexpressionElimination;

/// Rule: remove identity `Project` nodes that only preserve the complete video tuple before a
/// single video sink.
struct EliminateVideoSinkIdentityProject;

impl LogicalOptRule for EliminateVideoSinkIdentityProject {
    fn name(&self) -> &str {
        "eliminate_video_sink_identity_project"
    }

    fn optimize(
        &self,
        plan: Arc<LogicalPlan>,
        bindings: &SchemaBinding,
    ) -> (Arc<LogicalPlan>, SchemaBinding) {
        (
            eliminate_video_sink_identity_project(plan),
            bindings.clone(),
        )
    }
}

fn eliminate_video_sink_identity_project(plan: Arc<LogicalPlan>) -> Arc<LogicalPlan> {
    match plan.as_ref() {
        LogicalPlan::Tail(tail) => {
            let mut new = tail.clone();
            new.base.children = rewrite_logical_children(&tail.base.children);
            Arc::new(LogicalPlan::Tail(new))
        }
        LogicalPlan::DataSink(sink) => {
            let Some(child) = sink.base.children.first() else {
                return plan;
            };
            let rewritten_child = eliminate_video_sink_identity_project(Arc::clone(child));
            let final_child = try_eliminate_video_sink_project(sink, Arc::clone(&rewritten_child))
                .unwrap_or(rewritten_child);
            let mut new = sink.clone();
            new.base.children = vec![final_child];
            Arc::new(LogicalPlan::DataSink(new))
        }
        LogicalPlan::StatefulFunction(stateful) => {
            let mut new = stateful.clone();
            new.base.children = rewrite_logical_children(&stateful.base.children);
            Arc::new(LogicalPlan::StatefulFunction(new))
        }
        LogicalPlan::Filter(filter) => {
            let mut new = filter.clone();
            new.base.children = rewrite_logical_children(&filter.base.children);
            Arc::new(LogicalPlan::Filter(new))
        }
        LogicalPlan::Aggregation(aggregation) => {
            let mut new = aggregation.clone();
            new.base.children = rewrite_logical_children(&aggregation.base.children);
            Arc::new(LogicalPlan::Aggregation(new))
        }
        LogicalPlan::Compute(compute) => {
            let mut new = compute.clone();
            new.base.children = rewrite_logical_children(&compute.base.children);
            Arc::new(LogicalPlan::Compute(new))
        }
        LogicalPlan::Order(order) => {
            let mut new = order.clone();
            new.base.children = rewrite_logical_children(&order.base.children);
            Arc::new(LogicalPlan::Order(new))
        }
        LogicalPlan::Project(project) => {
            let mut new = project.clone();
            new.base.children = rewrite_logical_children(&project.base.children);
            Arc::new(LogicalPlan::Project(new))
        }
        LogicalPlan::Window(window) => {
            let mut new = window.clone();
            new.base.children = rewrite_logical_children(&window.base.children);
            Arc::new(LogicalPlan::Window(new))
        }
        LogicalPlan::DataSource(_) => plan,
    }
}

fn rewrite_logical_children(children: &[Arc<LogicalPlan>]) -> Vec<Arc<LogicalPlan>> {
    children
        .iter()
        .map(|child| eliminate_video_sink_identity_project(Arc::clone(child)))
        .collect()
}

fn try_eliminate_video_sink_project(
    sink: &crate::planner::logical::DataSinkPlan,
    child: Arc<LogicalPlan>,
) -> Option<Arc<LogicalPlan>> {
    if sink.sink.forward_to_result
        || !matches!(sink.sink.connector.connector, SinkConnectorConfig::Video(_))
    {
        return None;
    }

    let LogicalPlan::Project(project) = child.as_ref() else {
        return None;
    };
    if project.base.children.len() != 1 || !is_complete_video_identity_project(project) {
        return None;
    }

    let project_child = Arc::clone(project.base.children.first()?);
    if !has_single_video_source(project_child.as_ref()) {
        return None;
    }

    Some(project_child)
}

fn is_complete_video_identity_project(project: &crate::planner::logical::Project) -> bool {
    if project.fields.len() == 1 && is_unqualified_wildcard(&project.fields[0].expr) {
        return true;
    }

    let required = video_required_columns();
    if project.fields.len() != required.len() {
        return false;
    }

    let mut seen = HashSet::with_capacity(required.len());
    for field in &project.fields {
        let Some(column_name) = direct_column_name(&field.expr) else {
            return false;
        };
        if field.field_name.as_str() != column_name || !required.contains(column_name) {
            return false;
        }
        if !seen.insert(column_name) {
            return false;
        }
    }

    seen.len() == required.len()
}

fn video_required_columns() -> HashSet<&'static str> {
    [
        VIDEO_PAYLOAD_COLUMN,
        VIDEO_WIDTH_COLUMN,
        VIDEO_HEIGHT_COLUMN,
        VIDEO_FORMAT_COLUMN,
        VIDEO_TIMESTAMP_COLUMN,
    ]
    .into_iter()
    .collect()
}

fn is_unqualified_wildcard(expr: &SqlExpr) -> bool {
    matches!(expr, SqlExpr::Identifier(ident) if ident.value == "*")
}

fn direct_column_name(expr: &SqlExpr) -> Option<&str> {
    match expr {
        SqlExpr::Identifier(ident) if ident.value != "*" => Some(ident.value.as_str()),
        SqlExpr::CompoundIdentifier(idents) if idents.len() == 2 && idents[1].value != "*" => {
            Some(idents[1].value.as_str())
        }
        _ => None,
    }
}

fn has_single_video_source(plan: &LogicalPlan) -> bool {
    let mut found = 0usize;
    let mut all_sources_are_video = true;
    collect_source_kinds(plan, &mut found, &mut all_sources_are_video);
    found == 1 && all_sources_are_video
}

fn collect_source_kinds(plan: &LogicalPlan, found: &mut usize, all_sources_are_video: &mut bool) {
    if let LogicalPlan::DataSource(ds) = plan {
        *found += 1;
        if ds.stream_type() != StreamType::Video {
            *all_sources_are_video = false;
        }
    }

    for child in plan.children() {
        collect_source_kinds(child.as_ref(), found, all_sources_are_video);
    }
}

impl LogicalOptRule for CommonSubexpressionElimination {
    fn name(&self) -> &str {
        "common_subexpression_elimination"
    }

    fn optimize(
        &self,
        plan: Arc<LogicalPlan>,
        bindings: &SchemaBinding,
    ) -> (Arc<LogicalPlan>, SchemaBinding) {
        let mut cache = HashMap::<i64, Arc<LogicalPlan>>::new();
        let max_idx = max_logical_plan_index(plan.as_ref());

        // Avoid colliding with any existing column names in sources.
        let mut used_names = HashSet::<String>::new();
        for entry in bindings.entries() {
            for col in entry.schema.column_schemas() {
                used_names.insert(col.name.clone());
            }
        }

        let max_existing_cse = max_existing_cse_temp_id_in_plan(plan.as_ref());
        let mut ctx = CseContext {
            next_plan_index: max_idx + 1,
            next_temp_id: max_existing_cse + 1,
            used_names,
        };

        let updated = apply_cse_with_cache(plan, &mut ctx, &mut cache);
        (updated, bindings.clone())
    }
}

struct CseContext {
    next_plan_index: i64,
    next_temp_id: u64,
    used_names: HashSet<String>,
}

impl CseContext {
    fn alloc_compute_index(&mut self) -> i64 {
        let idx = self.next_plan_index;
        self.next_plan_index += 1;
        idx
    }

    fn alloc_temp_name(&mut self) -> String {
        use crate::expr::internal_columns::CSE_TEMP_PREFIX;

        loop {
            let candidate = format!("{}{}", CSE_TEMP_PREFIX, self.next_temp_id);
            self.next_temp_id += 1;
            if self.used_names.insert(candidate.clone()) {
                return candidate;
            }
        }
    }
}

fn max_logical_plan_index(plan: &LogicalPlan) -> i64 {
    let mut max_idx = plan.get_plan_index();
    for child in plan.children() {
        max_idx = max_idx.max(max_logical_plan_index(child.as_ref()));
    }
    max_idx
}

fn max_existing_cse_temp_id_in_plan(plan: &LogicalPlan) -> u64 {
    use crate::expr::internal_columns::is_cse_temp;

    fn parse_id(name: &str) -> Option<u64> {
        name.strip_prefix(crate::expr::internal_columns::CSE_TEMP_PREFIX)?
            .parse::<u64>()
            .ok()
    }

    fn visit_expr(expr: &SqlExpr, max_id: &mut u64) {
        match expr {
            SqlExpr::Identifier(ident) if is_cse_temp(&ident.value) => {
                if let Some(id) = parse_id(&ident.value) {
                    *max_id = (*max_id).max(id);
                }
            }
            SqlExpr::Identifier(_) => {}
            SqlExpr::BinaryOp { left, right, .. } => {
                visit_expr(left, max_id);
                visit_expr(right, max_id);
            }
            SqlExpr::UnaryOp { expr, .. } => visit_expr(expr, max_id),
            SqlExpr::Nested(expr) => visit_expr(expr, max_id),
            SqlExpr::Cast { expr, .. } => visit_expr(expr, max_id),
            SqlExpr::Between {
                expr, low, high, ..
            } => {
                visit_expr(expr, max_id);
                visit_expr(low, max_id);
                visit_expr(high, max_id);
            }
            SqlExpr::InList { expr, list, .. } => {
                visit_expr(expr, max_id);
                for e in list {
                    visit_expr(e, max_id);
                }
            }
            SqlExpr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                if let Some(op) = operand.as_ref() {
                    visit_expr(op, max_id);
                }
                for e in conditions {
                    visit_expr(e, max_id);
                }
                for e in results {
                    visit_expr(e, max_id);
                }
                if let Some(e) = else_result.as_ref() {
                    visit_expr(e, max_id);
                }
            }
            SqlExpr::Function(func) => {
                for arg in &func.args {
                    match arg {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => visit_expr(e, max_id),
                        FunctionArg::Named {
                            arg: FunctionArgExpr::Expr(e),
                            ..
                        } => visit_expr(e, max_id),
                        _ => {}
                    }
                }
                visit_function_context_exprs(func, |expr| visit_expr(expr, max_id));
            }
            SqlExpr::JsonAccess { left, right, .. } => {
                visit_expr(left, max_id);
                visit_expr(right, max_id);
            }
            SqlExpr::MapAccess { column, keys } => {
                visit_expr(column, max_id);
                for e in keys {
                    visit_expr(e, max_id);
                }
            }
            _ => {}
        }
    }

    fn visit_plan(plan: &LogicalPlan, max_id: &mut u64) {
        match plan {
            LogicalPlan::Compute(compute) => {
                for f in &compute.fields {
                    visit_expr(&f.expr, max_id);
                }
            }
            LogicalPlan::Project(project) => {
                for f in &project.fields {
                    visit_expr(&f.expr, max_id);
                }
            }
            LogicalPlan::Filter(filter) => visit_expr(&filter.predicate, max_id),
            LogicalPlan::StatefulFunction(stateful) => {
                for call in &stateful.calls {
                    visit_expr(&call.spec.original_expr, max_id);
                }
            }
            LogicalPlan::Aggregation(agg) => {
                for expr in agg.aggregate_mappings.values() {
                    visit_expr(expr, max_id);
                }
                for expr in &agg.group_by_exprs {
                    visit_expr(expr, max_id);
                }
            }
            LogicalPlan::Window(window) => {
                if let crate::planner::logical::LogicalWindowSpec::State {
                    open,
                    emit,
                    partition_by,
                } = &window.spec
                {
                    visit_expr(open.as_ref(), max_id);
                    visit_expr(emit.as_ref(), max_id);
                    for e in partition_by {
                        visit_expr(e, max_id);
                    }
                }
            }
            _ => {}
        }
        for child in plan.children() {
            visit_plan(child.as_ref(), max_id);
        }
    }

    let mut max_id = 0u64;
    visit_plan(plan, &mut max_id);
    max_id
}

fn visit_function_context_exprs(func: &sqlparser::ast::Function, mut visit: impl FnMut(&SqlExpr)) {
    if let Some(filter) = func.filter.as_deref() {
        visit(filter);
    }

    if let Some(WindowType::WindowSpec(spec)) = func.over.as_ref() {
        for expr in &spec.partition_by {
            visit(expr);
        }
        for item in &spec.order_by {
            visit(&item.expr);
        }
    }

    for item in &func.order_by {
        visit(&item.expr);
    }
}

fn apply_cse_with_cache(
    plan: Arc<LogicalPlan>,
    ctx: &mut CseContext,
    cache: &mut HashMap<i64, Arc<LogicalPlan>>,
) -> Arc<LogicalPlan> {
    let idx = plan.get_plan_index();
    if let Some(existing) = cache.get(&idx) {
        return existing.clone();
    }

    let updated = match plan.as_ref() {
        LogicalPlan::DataSource(ds) => Arc::new(LogicalPlan::DataSource(ds.clone())),
        LogicalPlan::StatefulFunction(stateful) => {
            let new_children = stateful
                .base
                .children
                .iter()
                .map(|c| apply_cse_with_cache(c.clone(), ctx, cache))
                .collect::<Vec<_>>();
            let mut new = stateful.clone();
            new.base.children = new_children;
            Arc::new(LogicalPlan::StatefulFunction(new))
        }
        LogicalPlan::Filter(filter) => {
            let new_children = filter
                .base
                .children
                .iter()
                .map(|c| apply_cse_with_cache(c.clone(), ctx, cache))
                .collect::<Vec<_>>();
            let mut new = filter.clone();
            new.base.children = new_children;
            Arc::new(LogicalPlan::Filter(new))
        }
        LogicalPlan::Aggregation(agg) => {
            let new_children = agg
                .base
                .children
                .iter()
                .map(|c| apply_cse_with_cache(c.clone(), ctx, cache))
                .collect::<Vec<_>>();
            let mut new = agg.clone();
            new.base.children = new_children;
            Arc::new(LogicalPlan::Aggregation(new))
        }
        LogicalPlan::Compute(compute) => {
            let new_children = compute
                .base
                .children
                .iter()
                .map(|c| apply_cse_with_cache(c.clone(), ctx, cache))
                .collect::<Vec<_>>();
            let mut new = compute.clone();
            new.base.children = new_children;
            Arc::new(LogicalPlan::Compute(new))
        }
        LogicalPlan::Order(order) => {
            let new_children = order
                .base
                .children
                .iter()
                .map(|c| apply_cse_with_cache(c.clone(), ctx, cache))
                .collect::<Vec<_>>();
            let mut new = order.clone();
            new.base.children = new_children;
            Arc::new(LogicalPlan::Order(new))
        }
        LogicalPlan::Project(project) => {
            // Project is the anchor: attempt to CSE within this project and (optionally) its
            // adjacent filter.
            let new_children = project
                .base
                .children
                .iter()
                .map(|c| apply_cse_with_cache(c.clone(), ctx, cache))
                .collect::<Vec<_>>();

            let mut new = project.clone();
            new.base.children = new_children;

            // Only support the common single-child pipeline shape for now.
            if new.base.children.len() != 1 {
                Arc::new(LogicalPlan::Project(new))
            } else {
                let child = new.base.children[0].clone();
                let (project_plan, maybe_filter_plan) = apply_cse_at_project(new, child, ctx);
                if let Some(filter_plan) = maybe_filter_plan {
                    cache.insert(filter_plan.get_plan_index(), filter_plan.clone());
                }
                project_plan
            }
        }
        LogicalPlan::DataSink(sink) => {
            let new_children = sink
                .base
                .children
                .iter()
                .map(|c| apply_cse_with_cache(c.clone(), ctx, cache))
                .collect::<Vec<_>>();
            let mut new = sink.clone();
            new.base.children = new_children;
            Arc::new(LogicalPlan::DataSink(new))
        }
        LogicalPlan::Tail(tail) => {
            let new_children = tail
                .base
                .children
                .iter()
                .map(|c| apply_cse_with_cache(c.clone(), ctx, cache))
                .collect::<Vec<_>>();
            let mut new = tail.clone();
            new.base.children = new_children;
            Arc::new(LogicalPlan::Tail(new))
        }
        LogicalPlan::Window(window) => {
            let new_children = window
                .base
                .children
                .iter()
                .map(|c| apply_cse_with_cache(c.clone(), ctx, cache))
                .collect::<Vec<_>>();
            let mut new = window.clone();
            new.base.children = new_children;
            Arc::new(LogicalPlan::Window(new))
        }
    };

    cache.insert(idx, updated.clone());
    updated
}

fn apply_cse_at_project(
    mut project: crate::planner::logical::Project,
    child: Arc<LogicalPlan>,
    ctx: &mut CseContext,
) -> (Arc<LogicalPlan>, Option<Arc<LogicalPlan>>) {
    use crate::planner::logical::compute::Compute;

    // Recognize the common shape:
    //   <child> -> Filter -> Project
    // and enable rewriting both the Filter predicate and Project expressions.
    let filter_with_child = match child.as_ref() {
        LogicalPlan::Filter(filter) if filter.base.children.len() == 1 => {
            Some((filter.clone(), filter.base.children[0].clone()))
        }
        _ => None,
    };

    let filter_predicate = filter_with_child.as_ref().map(|(f, _)| f.predicate.clone());

    let (new_project_fields, new_filter_predicate, compute_fields) =
        cse_rewrite_expressions(&project.fields, filter_predicate.as_ref(), ctx);

    if compute_fields.is_empty() {
        project.fields = new_project_fields;
        project.base.children = vec![child];
        return (Arc::new(LogicalPlan::Project(project)), None);
    }

    // Determine whether the filter needs the computed temps (if present).
    let temp_names: HashSet<String> = compute_fields
        .iter()
        .map(|f| f.field_name.clone())
        .collect();

    let filter_uses_temps = new_filter_predicate
        .as_ref()
        .is_some_and(|pred| expr_references_any_ident(pred, &temp_names));

    let compute_idx = ctx.alloc_compute_index();

    let (new_child, new_filter_plan) = if let Some((mut filter, filter_child)) = filter_with_child {
        if let Some(pred) = new_filter_predicate {
            filter.predicate = pred;
        }

        if filter_uses_temps {
            // Place Compute below Filter so Filter can reference computed temps.
            let input = filter_child;
            let compute = Arc::new(LogicalPlan::Compute(Compute::new(
                compute_fields,
                vec![input],
                compute_idx,
            )));
            filter.base.children = vec![compute];
            let filter_plan = Arc::new(LogicalPlan::Filter(filter));
            (filter_plan.clone(), Some(filter_plan))
        } else {
            // Filter does not reference temps; keep it selective by placing Compute above it.
            let filter_plan = Arc::new(LogicalPlan::Filter(filter));
            let compute = Arc::new(LogicalPlan::Compute(Compute::new(
                compute_fields,
                vec![filter_plan.clone()],
                compute_idx,
            )));
            (compute, Some(filter_plan))
        }
    } else {
        let compute = Arc::new(LogicalPlan::Compute(Compute::new(
            compute_fields,
            vec![child.clone()],
            compute_idx,
        )));
        (compute, None)
    };

    project.fields = new_project_fields;
    project.base.children = vec![new_child];
    (Arc::new(LogicalPlan::Project(project)), new_filter_plan)
}

fn expr_references_any_ident(expr: &SqlExpr, names: &HashSet<String>) -> bool {
    match expr {
        SqlExpr::Identifier(ident) => names.contains(ident.value.as_str()),
        SqlExpr::BinaryOp { left, right, .. } => {
            expr_references_any_ident(left, names) || expr_references_any_ident(right, names)
        }
        SqlExpr::UnaryOp { expr, .. } => expr_references_any_ident(expr, names),
        SqlExpr::Nested(expr) => expr_references_any_ident(expr, names),
        SqlExpr::Cast { expr, .. } => expr_references_any_ident(expr, names),
        SqlExpr::Between {
            expr, low, high, ..
        } => {
            expr_references_any_ident(expr, names)
                || expr_references_any_ident(low, names)
                || expr_references_any_ident(high, names)
        }
        SqlExpr::InList { expr, list, .. } => {
            expr_references_any_ident(expr, names)
                || list.iter().any(|e| expr_references_any_ident(e, names))
        }
        SqlExpr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            operand
                .as_ref()
                .is_some_and(|e| expr_references_any_ident(e, names))
                || conditions
                    .iter()
                    .any(|e| expr_references_any_ident(e, names))
                || results.iter().any(|e| expr_references_any_ident(e, names))
                || else_result
                    .as_ref()
                    .is_some_and(|e| expr_references_any_ident(e, names))
        }
        SqlExpr::Function(func) => func.args.iter().any(|arg| match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => expr_references_any_ident(e, names),
            FunctionArg::Named {
                arg: FunctionArgExpr::Expr(e),
                ..
            } => expr_references_any_ident(e, names),
            _ => false,
        }),
        SqlExpr::JsonAccess { left, right, .. } => {
            expr_references_any_ident(left, names) || expr_references_any_ident(right, names)
        }
        SqlExpr::MapAccess { column, keys } => {
            expr_references_any_ident(column, names)
                || keys.iter().any(|e| expr_references_any_ident(e, names))
        }
        _ => false,
    }
}

fn cse_rewrite_expressions(
    project_fields: &[crate::planner::logical::project::ProjectField],
    filter_predicate: Option<&SqlExpr>,
    ctx: &mut CseContext,
) -> (
    Vec<crate::planner::logical::project::ProjectField>,
    Option<SqlExpr>,
    Vec<crate::planner::logical::compute::ComputeField>,
) {
    use crate::planner::logical::compute::ComputeField;
    use crate::planner::logical::project::ProjectField;

    let mut project_exprs = project_fields
        .iter()
        .map(|f| f.expr.clone())
        .collect::<Vec<_>>();
    let mut predicate = filter_predicate.cloned();

    let mut compute_fields = Vec::<ComputeField>::new();

    loop {
        let mut stats = HashMap::<String, SubexprStats>::new();
        for expr in &project_exprs {
            collect_subexpr_stats(expr, &mut stats);
        }
        if let Some(pred) = predicate.as_ref() {
            collect_subexpr_stats(pred, &mut stats);
        }

        let Some(best) = choose_best_cse_candidate(&stats) else {
            break;
        };

        let temp_name = ctx.alloc_temp_name();
        let replacement = SqlExpr::Identifier(Ident::new(temp_name.clone()));
        compute_fields.push(ComputeField {
            field_name: temp_name.clone(),
            expr: best.expr.clone(),
        });

        for expr in &mut project_exprs {
            *expr = replace_subexpr(expr, best.key.as_str(), &replacement);
        }
        if let Some(pred) = predicate.as_mut() {
            *pred = replace_subexpr(pred, best.key.as_str(), &replacement);
        }
    }

    let new_project_fields = project_fields
        .iter()
        .zip(project_exprs)
        .map(|(old, expr)| ProjectField {
            field_name: old.field_name.clone(),
            expr,
        })
        .collect::<Vec<_>>();

    (new_project_fields, predicate, compute_fields)
}

#[derive(Debug, Clone)]
struct SubexprStats {
    key: String,
    expr: SqlExpr,
    count: usize,
    cost: usize,
}

fn collect_subexpr_stats(expr: &SqlExpr, stats: &mut HashMap<String, SubexprStats>) {
    // Parentheses are represented as `Nested`; they shouldn't introduce an extra "occurrence"
    // of the same subexpression, otherwise we double-count `(x)` and `x`.
    if let SqlExpr::Nested(inner) = expr {
        collect_subexpr_stats(inner, stats);
        return;
    }

    let key = normalized_expr_key(expr);
    let cost = expr_node_count(expr);
    stats
        .entry(key.clone())
        .and_modify(|s| {
            s.count += 1;
        })
        .or_insert(SubexprStats {
            key,
            expr: expr.clone(),
            count: 1,
            cost,
        });

    match expr {
        SqlExpr::BinaryOp { left, right, .. } => {
            collect_subexpr_stats(left, stats);
            collect_subexpr_stats(right, stats);
        }
        SqlExpr::UnaryOp { expr, .. } => collect_subexpr_stats(expr, stats),
        SqlExpr::Cast { expr, .. } => collect_subexpr_stats(expr, stats),
        SqlExpr::Between {
            expr, low, high, ..
        } => {
            collect_subexpr_stats(expr, stats);
            collect_subexpr_stats(low, stats);
            collect_subexpr_stats(high, stats);
        }
        SqlExpr::InList { expr, list, .. } => {
            collect_subexpr_stats(expr, stats);
            for e in list {
                collect_subexpr_stats(e, stats);
            }
        }
        SqlExpr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(op) = operand.as_ref() {
                collect_subexpr_stats(op, stats);
            }
            for e in conditions {
                collect_subexpr_stats(e, stats);
            }
            for e in results {
                collect_subexpr_stats(e, stats);
            }
            if let Some(e) = else_result.as_ref() {
                collect_subexpr_stats(e, stats);
            }
        }
        SqlExpr::Function(func) => {
            for arg in &func.args {
                match arg {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => {
                        collect_subexpr_stats(e, stats)
                    }
                    FunctionArg::Named {
                        arg: FunctionArgExpr::Expr(e),
                        ..
                    } => collect_subexpr_stats(e, stats),
                    _ => {}
                }
            }
        }
        SqlExpr::JsonAccess { left, right, .. } => {
            collect_subexpr_stats(left, stats);
            collect_subexpr_stats(right, stats);
        }
        SqlExpr::MapAccess { column, keys } => {
            collect_subexpr_stats(column, stats);
            for e in keys {
                collect_subexpr_stats(e, stats);
            }
        }
        _ => {}
    }
}

struct BestCandidate {
    key: String,
    expr: SqlExpr,
}

fn choose_best_cse_candidate(stats: &HashMap<String, SubexprStats>) -> Option<BestCandidate> {
    let mut best: Option<(i64, BestCandidate)> = None;

    for s in stats.values() {
        if s.count < 2 {
            continue;
        }
        if !is_cse_eligible_root(&s.expr) {
            continue;
        }
        if s.cost < 2 {
            continue;
        }

        // Benefit is the amount of repeated work removed.
        let score = ((s.count - 1) as i64) * (s.cost as i64);
        match &best {
            Some((best_score, _)) if *best_score >= score => {}
            _ => {
                best = Some((
                    score,
                    BestCandidate {
                        key: s.key.clone(),
                        expr: s.expr.clone(),
                    },
                ));
            }
        }
    }

    best.map(|(_, c)| c)
}

fn is_cse_eligible_root(expr: &SqlExpr) -> bool {
    match strip_nested(expr) {
        SqlExpr::Identifier(ident) => {
            !is_internal_derived(ident.value.as_str()) && ident.value != "*"
        }
        SqlExpr::CompoundIdentifier(_) => false,
        SqlExpr::Value(_) => false,
        SqlExpr::Function(_) => false, // Avoid changing semantics for non-deterministic functions.
        _ => true,
    }
}

fn strip_nested(expr: &SqlExpr) -> &SqlExpr {
    match expr {
        SqlExpr::Nested(inner) => strip_nested(inner.as_ref()),
        _ => expr,
    }
}

fn normalized_expr_key(expr: &SqlExpr) -> String {
    match strip_nested(expr) {
        SqlExpr::Identifier(ident) => format!("ident:{}", ident.value),
        SqlExpr::CompoundIdentifier(idents) => format!(
            "cident:{}",
            idents
                .iter()
                .map(|i| i.value.as_str())
                .collect::<Vec<_>>()
                .join(".")
        ),
        SqlExpr::Value(v) => format!("val:{v}"),
        SqlExpr::BinaryOp { left, op, right } => format!(
            "bin:{}:{}:{}",
            op,
            normalized_expr_key(left),
            normalized_expr_key(right)
        ),
        SqlExpr::UnaryOp { op, expr } => format!("un:{op}:{}", normalized_expr_key(expr)),
        SqlExpr::Cast {
            expr, data_type, ..
        } => {
            format!("cast:{data_type}:{}", normalized_expr_key(expr))
        }
        SqlExpr::Between {
            expr,
            negated,
            low,
            high,
        } => format!(
            "between:{}:{}:{}:{}",
            negated,
            normalized_expr_key(expr),
            normalized_expr_key(low),
            normalized_expr_key(high)
        ),
        SqlExpr::InList {
            expr,
            list,
            negated,
        } => format!(
            "inlist:{}:{}:[{}]",
            negated,
            normalized_expr_key(expr),
            list.iter()
                .map(normalized_expr_key)
                .collect::<Vec<_>>()
                .join(",")
        ),
        SqlExpr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => format!(
            "case:{}:[{}]:[{}]:{}",
            operand
                .as_ref()
                .map(|e| normalized_expr_key(e))
                .unwrap_or_else(|| "none".to_string()),
            conditions
                .iter()
                .map(normalized_expr_key)
                .collect::<Vec<_>>()
                .join(","),
            results
                .iter()
                .map(normalized_expr_key)
                .collect::<Vec<_>>()
                .join(","),
            else_result
                .as_ref()
                .map(|e| normalized_expr_key(e))
                .unwrap_or_else(|| "none".to_string())
        ),
        SqlExpr::Function(func) => {
            let args = func
                .args
                .iter()
                .map(|arg| match arg {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => normalized_expr_key(e),
                    FunctionArg::Named {
                        name,
                        arg: FunctionArgExpr::Expr(e),
                        ..
                    } => format!("{}={}", name, normalized_expr_key(e)),
                    _ => "unsupported".to_string(),
                })
                .collect::<Vec<_>>()
                .join(",");
            format!("func:{}({})", func.name, args)
        }
        SqlExpr::JsonAccess { left, right, .. } => format!(
            "json_access:{}:{}",
            normalized_expr_key(left),
            normalized_expr_key(right)
        ),
        SqlExpr::MapAccess { column, keys } => format!(
            "map_access:{}:[{}]",
            normalized_expr_key(column),
            keys.iter()
                .map(normalized_expr_key)
                .collect::<Vec<_>>()
                .join(",")
        ),
        other => format!("dbg:{:?}", other),
    }
}

fn expr_node_count(expr: &SqlExpr) -> usize {
    match expr {
        SqlExpr::BinaryOp { left, right, .. } => 1 + expr_node_count(left) + expr_node_count(right),
        SqlExpr::UnaryOp { expr, .. } => 1 + expr_node_count(expr),
        SqlExpr::Nested(expr) => expr_node_count(expr),
        SqlExpr::Cast { expr, .. } => 1 + expr_node_count(expr),
        SqlExpr::Between {
            expr, low, high, ..
        } => 1 + expr_node_count(expr) + expr_node_count(low) + expr_node_count(high),
        SqlExpr::InList { expr, list, .. } => {
            1 + expr_node_count(expr) + list.iter().map(expr_node_count).sum::<usize>()
        }
        SqlExpr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            1 + operand
                .as_ref()
                .map(|e| expr_node_count(e.as_ref()))
                .unwrap_or(0)
                + conditions.iter().map(expr_node_count).sum::<usize>()
                + results.iter().map(expr_node_count).sum::<usize>()
                + else_result
                    .as_ref()
                    .map(|e| expr_node_count(e.as_ref()))
                    .unwrap_or(0)
        }
        SqlExpr::Function(func) => {
            1 + func
                .args
                .iter()
                .map(|arg| match arg {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => expr_node_count(e),
                    FunctionArg::Named {
                        arg: FunctionArgExpr::Expr(e),
                        ..
                    } => expr_node_count(e),
                    _ => 0,
                })
                .sum::<usize>()
        }
        SqlExpr::JsonAccess { left, right, .. } => {
            1 + expr_node_count(left) + expr_node_count(right)
        }
        SqlExpr::MapAccess { column, keys } => {
            1 + expr_node_count(column) + keys.iter().map(expr_node_count).sum::<usize>()
        }
        _ => 1,
    }
}

fn replace_subexpr(expr: &SqlExpr, target_key: &str, replacement: &SqlExpr) -> SqlExpr {
    if normalized_expr_key(expr) == target_key {
        return replacement.clone();
    }

    match expr {
        SqlExpr::BinaryOp { left, op, right } => SqlExpr::BinaryOp {
            left: Box::new(replace_subexpr(left, target_key, replacement)),
            op: op.clone(),
            right: Box::new(replace_subexpr(right, target_key, replacement)),
        },
        SqlExpr::UnaryOp { op, expr } => SqlExpr::UnaryOp {
            op: *op,
            expr: Box::new(replace_subexpr(expr, target_key, replacement)),
        },
        SqlExpr::Nested(expr) => {
            SqlExpr::Nested(Box::new(replace_subexpr(expr, target_key, replacement)))
        }
        SqlExpr::Cast {
            expr,
            data_type,
            format,
        } => SqlExpr::Cast {
            expr: Box::new(replace_subexpr(expr, target_key, replacement)),
            data_type: data_type.clone(),
            format: format.clone(),
        },
        SqlExpr::Between {
            expr,
            negated,
            low,
            high,
        } => SqlExpr::Between {
            expr: Box::new(replace_subexpr(expr, target_key, replacement)),
            negated: *negated,
            low: Box::new(replace_subexpr(low, target_key, replacement)),
            high: Box::new(replace_subexpr(high, target_key, replacement)),
        },
        SqlExpr::InList {
            expr,
            list,
            negated,
        } => SqlExpr::InList {
            expr: Box::new(replace_subexpr(expr, target_key, replacement)),
            list: list
                .iter()
                .map(|e| replace_subexpr(e, target_key, replacement))
                .collect(),
            negated: *negated,
        },
        SqlExpr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => SqlExpr::Case {
            operand: operand
                .as_ref()
                .map(|e| Box::new(replace_subexpr(e, target_key, replacement))),
            conditions: conditions
                .iter()
                .map(|e| replace_subexpr(e, target_key, replacement))
                .collect(),
            results: results
                .iter()
                .map(|e| replace_subexpr(e, target_key, replacement))
                .collect(),
            else_result: else_result
                .as_ref()
                .map(|e| Box::new(replace_subexpr(e, target_key, replacement))),
        },
        SqlExpr::Function(func) => {
            let mut new_func = func.clone();
            new_func.args = func
                .args
                .iter()
                .map(|arg| match arg {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => FunctionArg::Unnamed(
                        FunctionArgExpr::Expr(replace_subexpr(e, target_key, replacement)),
                    ),
                    FunctionArg::Named { name, arg } => {
                        let new_arg = match arg {
                            FunctionArgExpr::Expr(e) => {
                                FunctionArgExpr::Expr(replace_subexpr(e, target_key, replacement))
                            }
                            other => other.clone(),
                        };
                        FunctionArg::Named {
                            name: name.clone(),
                            arg: new_arg,
                        }
                    }
                    _ => arg.clone(),
                })
                .collect();
            SqlExpr::Function(new_func)
        }
        SqlExpr::JsonAccess {
            left,
            operator,
            right,
        } => SqlExpr::JsonAccess {
            left: Box::new(replace_subexpr(left, target_key, replacement)),
            right: Box::new(replace_subexpr(right, target_key, replacement)),
            operator: *operator,
        },
        SqlExpr::MapAccess { column, keys } => SqlExpr::MapAccess {
            column: Box::new(replace_subexpr(column, target_key, replacement)),
            keys: keys
                .iter()
                .map(|e| replace_subexpr(e, target_key, replacement))
                .collect(),
        },
        _ => expr.clone(),
    }
}

/// Rule: prune unused top-level columns from data sources.
struct TopLevelColumnPruning<'a> {
    eventtime_enabled: bool,
    slice_registry: Option<&'a SharedStreamRegistry>,
}

impl<'a> LogicalOptRule for TopLevelColumnPruning<'a> {
    fn name(&self) -> &str {
        "top_level_column_pruning"
    }

    fn optimize(
        &self,
        plan: Arc<LogicalPlan>,
        bindings: &SchemaBinding,
    ) -> (Arc<LogicalPlan>, SchemaBinding) {
        let mut collector = TopLevelColumnUsageCollector::new(bindings, self.eventtime_enabled);
        collector.slice_registry = self.slice_registry;
        collector.collect_from_plan(plan.as_ref());
        let pruned = collector.build_pruned_binding();
        let shared_required_schemas = collector.build_shared_required_schemas();
        let shared_slot_versions = collector.build_shared_slot_versions();
        let updated_plan = apply_pruned_schemas_to_logical(
            plan,
            &pruned,
            &HashMap::new(),
            &shared_required_schemas,
            &shared_slot_versions,
        );
        (updated_plan, pruned)
    }
}

/// Rule: prune unused struct fields (including list element structs) referenced via nested access.
struct StructFieldPruning;

impl LogicalOptRule for StructFieldPruning {
    fn name(&self) -> &str {
        "struct_field_pruning"
    }

    fn optimize(
        &self,
        plan: Arc<LogicalPlan>,
        bindings: &SchemaBinding,
    ) -> (Arc<LogicalPlan>, SchemaBinding) {
        let mut collector = StructFieldUsageCollector::new(bindings);
        collector.collect_from_plan(plan.as_ref());
        let pruned = collector.build_pruned_binding();
        let updated_plan = apply_pruned_schemas_to_logical(
            plan,
            &pruned,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
        );
        (updated_plan, pruned)
    }
}

/// Rule: collect list index requirements for decode projection.
struct ListElementPruning;

impl LogicalOptRule for ListElementPruning {
    fn name(&self) -> &str {
        "list_element_pruning"
    }

    fn optimize(
        &self,
        plan: Arc<LogicalPlan>,
        bindings: &SchemaBinding,
    ) -> (Arc<LogicalPlan>, SchemaBinding) {
        let mut collector = ListElementUsageCollector::new(bindings);
        collector.collect_from_plan(plan.as_ref());
        let decode_projections = collector.build_decode_projections();
        let updated_plan = apply_pruned_schemas_to_logical(
            plan,
            bindings,
            &decode_projections,
            &HashMap::new(),
            &HashMap::new(),
        );
        (updated_plan, bindings.clone())
    }
}

/// Build a shared source's binding schema from its append-only slot registry
/// (VF-56). Registers this consumer's used columns (in full-schema order, for
/// deterministic slot assignment), then materializes a schema whose column order
/// IS the slot order — so `Schema::column_index(name)` returns the stable slot,
/// which becomes the consumer's `ColumnRef::ByIndex`. Width = current union size.
fn build_shared_slice_schema(
    registry: &SharedStreamRegistry,
    entry: &SchemaBindingEntry,
    used: Option<&HashSet<String>>,
) -> Arc<Schema> {
    let cols = entry.schema.column_schemas();
    let reg = registry.slice_registry(&entry.source_name);
    let snapshot = {
        let mut guard = reg.write();
        let names = cols.iter().filter_map(|col| match used {
            Some(used) if !used.contains(col.name.as_str()) => None,
            _ => Some(col.name.as_str()),
        });
        guard.assign_all(names)
    };
    let by_name: HashMap<&str, &datatypes::ColumnSchema> =
        cols.iter().map(|c| (c.name.as_str(), c)).collect();
    // Every slot key came from some consumer's used columns ⊆ this shared schema,
    // so all keys resolve; order is preserved == slot order.
    let slot_cols: Vec<datatypes::ColumnSchema> = snapshot
        .keys
        .iter()
        .filter_map(|k| by_name.get(k.as_ref()).map(|c| (*c).clone()))
        .collect();
    Arc::new(Schema::new(slot_cols))
}

/// Collects top-level column usage for pruning decisions.
struct TopLevelColumnUsageCollector<'a> {
    bindings: &'a SchemaBinding,
    used_columns: HashMap<String, HashSet<String>>,
    /// Sources for which pruning is disabled (e.g., wildcard or ambiguous)
    prune_disabled: HashSet<String>,
    eventtime_enabled: bool,
    /// When set (VF-56, gated), shared sources are narrowed to the append-only
    /// registry's slot-ordered union instead of being kept full-width.
    slice_registry: Option<&'a SharedStreamRegistry>,
}

#[derive(Debug, Clone, Default)]
struct UsedColumnTree {
    columns: HashMap<String, ColumnUse>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ColumnUse {
    All,
    Fields(HashMap<String, ColumnUse>),
}

fn mark_field_path_used_in_tree(
    used_columns: &mut HashMap<String, UsedColumnTree>,
    source_name: &str,
    column_name: &str,
    path: &[String],
) {
    let tree = used_columns.entry(source_name.to_string()).or_default();
    let Some(root_use) = tree.columns.get_mut(column_name) else {
        tree.columns
            .insert(column_name.to_string(), ColumnUse::Fields(HashMap::new()));
        return mark_field_path_used_in_tree(used_columns, source_name, column_name, path);
    };

    if matches!(root_use, ColumnUse::All) {
        return;
    }

    let mut current = root_use;
    for (idx, segment) in path.iter().enumerate() {
        let is_leaf = idx + 1 == path.len();
        match current {
            ColumnUse::All => return,
            ColumnUse::Fields(map) => {
                if is_leaf {
                    match map.get(segment.as_str()) {
                        Some(ColumnUse::All) => {}
                        _ => {
                            map.insert(segment.clone(), ColumnUse::All);
                        }
                    }
                    return;
                }
                current = map
                    .entry(segment.clone())
                    .or_insert_with(|| ColumnUse::Fields(HashMap::new()));
            }
        }
    }
}

impl<'a> TopLevelColumnUsageCollector<'a> {
    fn new(bindings: &'a SchemaBinding, eventtime_enabled: bool) -> Self {
        Self {
            bindings,
            used_columns: HashMap::new(),
            prune_disabled: HashSet::new(),
            eventtime_enabled,
            slice_registry: None,
        }
    }

    fn collect_from_plan(&mut self, plan: &LogicalPlan) {
        match plan {
            LogicalPlan::Compute(compute) => {
                for field in &compute.fields {
                    self.collect_expr_ast(&field.expr);
                }
            }
            LogicalPlan::Order(order) => {
                for item in &order.items {
                    self.collect_expr_ast(&item.expr);
                }
            }
            LogicalPlan::Project(project) => {
                for field in &project.fields {
                    self.collect_expr_ast(&field.expr);
                }
            }
            LogicalPlan::StatefulFunction(stateful) => {
                for call in &stateful.calls {
                    self.collect_expr_ast(&call.spec.original_expr);
                }
            }
            LogicalPlan::Filter(filter) => self.collect_expr_ast(&filter.predicate),
            LogicalPlan::Aggregation(agg) => {
                for expr in agg.aggregate_mappings.values() {
                    self.collect_expr_ast(expr);
                }
                for expr in &agg.group_by_exprs {
                    self.collect_expr_ast(expr);
                }
            }
            LogicalPlan::Window(window) => {
                if let crate::planner::logical::LogicalWindowSpec::State {
                    open,
                    emit,
                    partition_by,
                } = &window.spec
                {
                    self.collect_expr_ast(open.as_ref());
                    self.collect_expr_ast(emit.as_ref());
                    for expr in partition_by {
                        self.collect_expr_ast(expr);
                    }
                }
            }
            LogicalPlan::DataSource(ds) => {
                if self.eventtime_enabled {
                    if let Some(eventtime) = ds.eventtime() {
                        self.mark_column_used(&ds.source_name, eventtime.column());
                    }
                }
                if ds.source_input().is_on_change() {
                    if let Some(columns) = ds.source_input().on_change_columns() {
                        for column in columns {
                            self.mark_column_used(&ds.source_name, column);
                        }
                    } else {
                        self.prune_disabled.insert(ds.source_name.clone());
                    }
                }
            }
            LogicalPlan::DataSink(_) => {}
            LogicalPlan::Tail(TailPlan { .. }) => {}
        }

        for child in plan.children() {
            self.collect_from_plan(child.as_ref());
        }
    }

    fn collect_expr_ast(&mut self, expr: &SqlExpr) {
        match expr {
            SqlExpr::Identifier(ident) => self.record_identifier(None, ident),
            SqlExpr::CompoundIdentifier(idents) => self.record_compound_identifier(idents),
            SqlExpr::Function(func) => {
                for arg in &func.args {
                    self.collect_function_arg(arg);
                }
                visit_function_context_exprs(func, |expr| self.collect_expr_ast(expr));
            }
            SqlExpr::BinaryOp { left, right, .. } => {
                self.collect_expr_ast(left);
                self.collect_expr_ast(right);
            }
            SqlExpr::UnaryOp { expr, .. } => self.collect_expr_ast(expr),
            SqlExpr::Nested(expr) => self.collect_expr_ast(expr),
            SqlExpr::Cast { expr, .. } => self.collect_expr_ast(expr),
            SqlExpr::Between {
                expr, low, high, ..
            } => {
                self.collect_expr_ast(expr);
                self.collect_expr_ast(low);
                self.collect_expr_ast(high);
            }
            SqlExpr::InList { expr, list, .. } => {
                self.collect_expr_ast(expr);
                for item in list {
                    self.collect_expr_ast(item);
                }
            }
            SqlExpr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                if let Some(expr) = operand.as_ref() {
                    self.collect_expr_ast(expr);
                }
                for expr in conditions {
                    self.collect_expr_ast(expr);
                }
                for expr in results {
                    self.collect_expr_ast(expr);
                }
                if let Some(expr) = else_result.as_ref() {
                    self.collect_expr_ast(expr);
                }
            }
            SqlExpr::JsonAccess { left, .. } => {
                // For top-level pruning, only the base column matters; JsonAccess RHS is a field.
                self.collect_expr_ast(left);
            }
            SqlExpr::MapAccess { column, keys } => {
                self.collect_expr_ast(column);
                for key in keys {
                    self.collect_expr_ast(key);
                }
            }
            _ => {}
        }
    }

    fn collect_function_arg(&mut self, arg: &FunctionArg) {
        match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => self.collect_expr_ast(expr),
            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => self.handle_wildcard(None),
            FunctionArg::Unnamed(FunctionArgExpr::QualifiedWildcard(obj_name)) => {
                self.handle_wildcard(Some(obj_name))
            }
            FunctionArg::Named { arg, .. } => match arg {
                FunctionArgExpr::Expr(expr) => self.collect_expr_ast(expr),
                FunctionArgExpr::Wildcard => self.handle_wildcard(None),
                FunctionArgExpr::QualifiedWildcard(obj_name) => {
                    self.handle_wildcard(Some(obj_name))
                }
            },
        }
    }

    fn handle_wildcard(&mut self, qualifier: Option<&ObjectName>) {
        if let Some(obj_name) = qualifier {
            let qualifier = obj_name.to_string();
            if let Some(source) = self.resolve_source(&qualifier) {
                self.prune_disabled.insert(source);
            } else {
                self.disable_pruning_for_all_sources();
            }
        } else {
            self.disable_pruning_for_all_sources();
        }
    }

    fn record_identifier(&mut self, qualifier: Option<&str>, ident: &Ident) {
        let column_name = ident.value.as_str();
        if column_name == "*" {
            self.handle_wildcard(None);
            return;
        }

        if is_internal_derived(column_name) {
            return;
        }

        match qualifier {
            Some(q) => {
                if let Some(entry) = self.bindings.entries().iter().find(|b| b.matches(q)) {
                    if entry.schema.contains_column(column_name) {
                        self.mark_column_used(&entry.source_name, column_name);
                    } else {
                        self.prune_disabled.insert(entry.source_name.clone());
                    }
                } else {
                    self.disable_pruning_for_all_sources();
                }
            }
            None => {
                let mut matches = Vec::new();
                for entry in self.bindings.entries() {
                    if entry.schema.contains_column(column_name) {
                        matches.push(entry.source_name.clone());
                    }
                }
                match matches.len() {
                    0 => self.disable_pruning_for_all_sources(),
                    1 => self.mark_column_used(&matches[0], column_name),
                    _ => {
                        for src in matches {
                            self.prune_disabled.insert(src);
                        }
                    }
                }
            }
        }
    }

    fn record_compound_identifier(&mut self, idents: &[Ident]) {
        if let Some(last) = idents.last() {
            if last.value == "*" {
                let qualifier = idents
                    .iter()
                    .take(idents.len() - 1)
                    .map(|i| i.value.clone())
                    .collect::<Vec<_>>()
                    .join(".");
                if qualifier.is_empty() {
                    self.handle_wildcard(None);
                } else {
                    let obj = ObjectName(vec![Ident::new(qualifier)]);
                    self.handle_wildcard(Some(&obj));
                }
                return;
            }
        }

        let (qualifier, column) = if idents.len() >= 2 {
            let qualifier = idents[..idents.len() - 1]
                .iter()
                .map(|i| i.value.clone())
                .collect::<Vec<_>>()
                .join(".");
            (Some(qualifier), &idents[idents.len() - 1])
        } else {
            (None, &idents[0])
        };

        self.record_identifier(qualifier.as_deref(), column);
    }

    fn mark_column_used(&mut self, source_name: &str, column_name: &str) {
        self.used_columns
            .entry(source_name.to_string())
            .or_default()
            .insert(column_name.to_string());
    }

    fn resolve_source(&self, qualifier: &str) -> Option<String> {
        self.bindings
            .entries()
            .iter()
            .find(|entry| entry.matches(qualifier))
            .map(|entry| entry.source_name.clone())
    }

    fn disable_pruning_for_all_sources(&mut self) {
        for entry in self.bindings.entries() {
            self.prune_disabled.insert(entry.source_name.clone());
        }
    }

    fn build_pruned_binding(&self) -> SchemaBinding {
        let mut entries = Vec::new();
        for entry in self.bindings.entries() {
            let should_keep_full = matches!(
                entry.kind,
                crate::expr::sql_conversion::SourceBindingKind::Shared
                    | crate::expr::sql_conversion::SourceBindingKind::MemoryCollection
            ) || self.prune_disabled.contains(&entry.source_name)
                || !self.used_columns.contains_key(&entry.source_name);

            // VF-56 (gated): for shared sources, narrow to the append-only
            // slot-ordered union instead of keeping the full DBC width. Slots are
            // stable across consumers, so `column_index` (== slot) never moves.
            let slice_schema = if matches!(
                entry.kind,
                crate::expr::sql_conversion::SourceBindingKind::Shared
            ) && self.slice_registry.is_some()
            {
                self.slice_registry.map(|registry| {
                    let used = if self.prune_disabled.contains(&entry.source_name) {
                        None
                    } else {
                        self.used_columns
                            .get(&entry.source_name)
                            .filter(|used| !used.is_empty())
                    };
                    build_shared_slice_schema(registry, entry, used)
                })
            } else {
                None
            };

            let schema = if let Some(slice_schema) = slice_schema {
                slice_schema
            } else if should_keep_full {
                Arc::clone(&entry.schema)
            } else {
                let required = &self.used_columns[&entry.source_name];
                let filtered: Vec<_> = entry
                    .schema
                    .column_schemas()
                    .iter()
                    .filter(|col| required.contains(col.name.as_str()))
                    .cloned()
                    .collect();
                Arc::new(Schema::new(filtered))
            };

            entries.push(SchemaBindingEntry {
                source_name: entry.source_name.clone(),
                alias: entry.alias.clone(),
                schema,
                kind: entry.kind.clone(),
            });
        }

        SchemaBinding::new(entries)
    }

    fn build_shared_required_schemas(&self) -> HashMap<String, Vec<String>> {
        let mut out = HashMap::new();
        for entry in self.bindings.entries() {
            if !matches!(
                entry.kind,
                crate::expr::sql_conversion::SourceBindingKind::Shared
            ) {
                continue;
            }

            let full: Vec<String> = entry
                .schema
                .column_schemas()
                .iter()
                .map(|col| col.name.clone())
                .collect();

            let required = if self.prune_disabled.contains(&entry.source_name)
                || !self.used_columns.contains_key(&entry.source_name)
            {
                full.clone()
            } else {
                let used = &self.used_columns[&entry.source_name];
                let cols: Vec<String> = entry
                    .schema
                    .column_schemas()
                    .iter()
                    .filter(|col| used.contains(col.name.as_str()))
                    .map(|col| col.name.clone())
                    .collect();
                if cols.is_empty() {
                    full.clone()
                } else {
                    cols
                }
            };

            out.insert(entry.source_name.clone(), required);
        }
        out
    }

    fn build_shared_slot_versions(&self) -> HashMap<String, u64> {
        let mut out = HashMap::new();
        let Some(registry) = self.slice_registry else {
            return out;
        };
        for entry in self.bindings.entries() {
            if matches!(
                entry.kind,
                crate::expr::sql_conversion::SourceBindingKind::Shared
            ) {
                let version = registry
                    .slice_registry(&entry.source_name)
                    .read()
                    .snapshot()
                    .version;
                out.insert(entry.source_name.clone(), version);
            }
        }
        out
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StructFieldAccess {
    qualifier: Option<String>,
    column: String,
    field_path: Vec<String>,
}

fn extract_struct_field_access(expr: &SqlExpr) -> Option<StructFieldAccess> {
    let (base, fields) = extract_struct_access_chain(expr)?;
    let (qualifier, column) = split_compound_identifier(&base)?;
    Some(StructFieldAccess {
        qualifier,
        column,
        field_path: fields,
    })
}

fn extract_struct_access_chain(expr: &SqlExpr) -> Option<(Vec<Ident>, Vec<String>)> {
    match expr {
        SqlExpr::Nested(inner) => extract_struct_access_chain(inner.as_ref()),
        SqlExpr::Identifier(ident) => Some((vec![ident.clone()], Vec::new())),
        SqlExpr::CompoundIdentifier(idents) => Some((idents.clone(), Vec::new())),
        SqlExpr::JsonAccess {
            left,
            operator: sqlparser::ast::JsonOperator::Arrow,
            right,
        } => {
            let (base, mut fields) = extract_struct_access_chain(left.as_ref())?;
            match right.as_ref() {
                SqlExpr::Identifier(ident) => {
                    fields.push(ident.value.clone());
                    Some((base, fields))
                }
                SqlExpr::MapAccess { column, .. } => {
                    let column = match column.as_ref() {
                        SqlExpr::Nested(inner) => inner.as_ref(),
                        other => other,
                    };
                    let SqlExpr::Identifier(ident) = column else {
                        return None;
                    };
                    fields.push(ident.value.clone());
                    Some((base, fields))
                }
                SqlExpr::JsonAccess { left, .. } => match left.as_ref() {
                    SqlExpr::MapAccess { column, .. } => {
                        let column = match column.as_ref() {
                            SqlExpr::Nested(inner) => inner.as_ref(),
                            other => other,
                        };
                        let SqlExpr::Identifier(ident) = column else {
                            return None;
                        };
                        fields.push(ident.value.clone());
                        Some((base, fields))
                    }
                    SqlExpr::Identifier(ident) => {
                        fields.push(ident.value.clone());
                        Some((base, fields))
                    }
                    _ => None,
                },
                _ => None,
            }
        }
        _ => None,
    }
}

/// Collects struct field usage for pruning decisions.
struct StructFieldUsageCollector<'a> {
    bindings: &'a SchemaBinding,
    used_columns: HashMap<String, UsedColumnTree>,
    prune_disabled: HashSet<String>,
}

impl<'a> StructFieldUsageCollector<'a> {
    fn new(bindings: &'a SchemaBinding) -> Self {
        Self {
            bindings,
            used_columns: HashMap::new(),
            prune_disabled: HashSet::new(),
        }
    }

    fn collect_from_plan(&mut self, plan: &LogicalPlan) {
        match plan {
            LogicalPlan::Compute(compute) => {
                for field in &compute.fields {
                    self.collect_expr_ast(&field.expr);
                }
            }
            LogicalPlan::Order(order) => {
                for item in &order.items {
                    self.collect_expr_ast(&item.expr);
                }
            }
            LogicalPlan::Project(project) => {
                for field in &project.fields {
                    self.collect_expr_ast(&field.expr);
                }
            }
            LogicalPlan::StatefulFunction(stateful) => {
                for call in &stateful.calls {
                    self.collect_expr_ast(&call.spec.original_expr);
                }
            }
            LogicalPlan::Filter(filter) => self.collect_expr_ast(&filter.predicate),
            LogicalPlan::Aggregation(agg) => {
                for expr in agg.aggregate_mappings.values() {
                    self.collect_expr_ast(expr);
                }
                for expr in &agg.group_by_exprs {
                    self.collect_expr_ast(expr);
                }
            }
            LogicalPlan::Window(window) => {
                if let crate::planner::logical::LogicalWindowSpec::State {
                    open,
                    emit,
                    partition_by,
                } = &window.spec
                {
                    self.collect_expr_ast(open.as_ref());
                    self.collect_expr_ast(emit.as_ref());
                    for expr in partition_by {
                        self.collect_expr_ast(expr);
                    }
                }
            }
            LogicalPlan::DataSource(ds) => {
                if ds.source_input().is_on_change() {
                    if let Some(columns) = ds.source_input().on_change_columns() {
                        for column in columns {
                            self.mark_field_path_used(&ds.source_name, column, &[]);
                        }
                    } else {
                        self.prune_disabled.insert(ds.source_name.clone());
                    }
                }
            }
            LogicalPlan::DataSink(_) => {}
            LogicalPlan::Tail(TailPlan { .. }) => {}
        }

        for child in plan.children() {
            self.collect_from_plan(child.as_ref());
        }
    }

    fn collect_expr_ast(&mut self, expr: &SqlExpr) {
        match expr {
            SqlExpr::Function(func) => {
                for arg in &func.args {
                    self.collect_function_arg(arg);
                }
                visit_function_context_exprs(func, |expr| self.collect_expr_ast(expr));
            }
            SqlExpr::BinaryOp { left, right, .. } => {
                self.collect_expr_ast(left);
                self.collect_expr_ast(right);
            }
            SqlExpr::UnaryOp { expr, .. } => self.collect_expr_ast(expr),
            SqlExpr::Nested(expr) => self.collect_expr_ast(expr),
            SqlExpr::Cast { expr, .. } => self.collect_expr_ast(expr),
            SqlExpr::JsonAccess { left, right, .. } => {
                if let Some(access) = extract_decode_field_path_access(expr) {
                    self.record_decode_field_path_access(access);
                    return;
                }

                if let Some(access) = extract_struct_field_access(expr) {
                    self.record_struct_field_access(access);
                    return;
                }

                self.collect_expr_ast(left);
                self.collect_expr_ast(right);
            }
            SqlExpr::MapAccess { column, keys } => {
                if let Some(access) = extract_decode_field_path_access(expr) {
                    self.record_decode_field_path_access(access);
                    for key in keys {
                        self.collect_expr_ast(key);
                    }
                    return;
                }

                self.collect_expr_ast(column);
                for key in keys {
                    self.collect_expr_ast(key);
                }
            }
            _ => {}
        }
    }

    fn collect_function_arg(&mut self, arg: &FunctionArg) {
        match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => self.collect_expr_ast(expr),
            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => self.handle_wildcard(None),
            FunctionArg::Unnamed(FunctionArgExpr::QualifiedWildcard(obj_name)) => {
                self.handle_wildcard(Some(obj_name))
            }
            FunctionArg::Named { arg, .. } => match arg {
                FunctionArgExpr::Expr(expr) => self.collect_expr_ast(expr),
                FunctionArgExpr::Wildcard => self.handle_wildcard(None),
                FunctionArgExpr::QualifiedWildcard(obj_name) => {
                    self.handle_wildcard(Some(obj_name))
                }
            },
        }
    }

    fn handle_wildcard(&mut self, qualifier: Option<&ObjectName>) {
        if let Some(obj_name) = qualifier {
            let qualifier = obj_name.to_string();
            if let Some(source) = self.resolve_source(&qualifier) {
                self.prune_disabled.insert(source);
            } else {
                self.disable_pruning_for_all_sources();
            }
        } else {
            self.disable_pruning_for_all_sources();
        }
    }

    fn record_struct_field_access(&mut self, access: StructFieldAccess) {
        if access.field_path.is_empty() {
            return;
        }

        let Some(source_name) =
            self.resolve_source_for_column(access.qualifier.as_deref(), access.column.as_str())
        else {
            return;
        };

        self.mark_field_path_used(&source_name, access.column.as_str(), &access.field_path);
    }

    fn record_decode_field_path_access(&mut self, access: DecodeFieldPathAccess) {
        if access.segments.is_empty() {
            return;
        }

        let Some(source_name) =
            self.resolve_source_for_column(access.qualifier.as_deref(), access.column.as_str())
        else {
            return;
        };

        let schema_path = decode_segments_to_schema_path(access.segments.as_slice());
        self.mark_field_path_used(&source_name, access.column.as_str(), &schema_path);
    }

    fn resolve_source_for_column(
        &mut self,
        qualifier: Option<&str>,
        column_name: &str,
    ) -> Option<String> {
        match qualifier {
            Some(q) => {
                if let Some(entry) = self.bindings.entries().iter().find(|b| b.matches(q)) {
                    if entry.schema.contains_column(column_name) {
                        Some(entry.source_name.clone())
                    } else {
                        self.prune_disabled.insert(entry.source_name.clone());
                        None
                    }
                } else {
                    self.disable_pruning_for_all_sources();
                    None
                }
            }
            None => {
                let mut matches = Vec::new();
                for entry in self.bindings.entries() {
                    if entry.schema.contains_column(column_name) {
                        matches.push(entry.source_name.clone());
                    }
                }
                match matches.len() {
                    0 => {
                        self.disable_pruning_for_all_sources();
                        None
                    }
                    1 => Some(matches[0].clone()),
                    _ => {
                        for src in matches {
                            self.prune_disabled.insert(src);
                        }
                        None
                    }
                }
            }
        }
    }

    fn mark_field_path_used(&mut self, source_name: &str, column_name: &str, path: &[String]) {
        mark_field_path_used_in_tree(&mut self.used_columns, source_name, column_name, path);
    }

    fn resolve_source(&self, qualifier: &str) -> Option<String> {
        self.bindings
            .entries()
            .iter()
            .find(|entry| entry.matches(qualifier))
            .map(|entry| entry.source_name.clone())
    }

    fn disable_pruning_for_all_sources(&mut self) {
        for entry in self.bindings.entries() {
            self.prune_disabled.insert(entry.source_name.clone());
        }
    }

    fn build_pruned_binding(&self) -> SchemaBinding {
        let mut entries = Vec::new();
        for entry in self.bindings.entries() {
            let should_keep_full = matches!(
                entry.kind,
                crate::expr::sql_conversion::SourceBindingKind::Shared
                    | crate::expr::sql_conversion::SourceBindingKind::MemoryCollection
            ) || self.prune_disabled.contains(&entry.source_name)
                || !self.used_columns.contains_key(&entry.source_name);

            let schema = if should_keep_full {
                Arc::clone(&entry.schema)
            } else {
                let required = &self.used_columns[&entry.source_name];
                let cols: Vec<_> = entry
                    .schema
                    .column_schemas()
                    .iter()
                    .map(|col| match required.columns.get(col.name.as_str()) {
                        Some(usage) => prune_struct_fields_column_schema(col, usage),
                        None => col.clone(),
                    })
                    .collect();
                Arc::new(Schema::new(cols))
            };

            entries.push(SchemaBindingEntry {
                source_name: entry.source_name.clone(),
                alias: entry.alias.clone(),
                schema,
                kind: entry.kind.clone(),
            });
        }

        SchemaBinding::new(entries)
    }
}

/// Collects list index requirements for decode projection.
struct ListElementUsageCollector<'a> {
    bindings: &'a SchemaBinding,
    decode_projections: HashMap<String, DecodeProjection>,
    prune_disabled: HashSet<String>,
}

impl<'a> ListElementUsageCollector<'a> {
    fn new(bindings: &'a SchemaBinding) -> Self {
        Self {
            bindings,
            decode_projections: HashMap::new(),
            prune_disabled: HashSet::new(),
        }
    }

    fn collect_from_plan(&mut self, plan: &LogicalPlan) {
        match plan {
            LogicalPlan::Compute(compute) => {
                for field in &compute.fields {
                    self.collect_expr_ast(&field.expr);
                }
            }
            LogicalPlan::Order(order) => {
                for item in &order.items {
                    self.collect_expr_ast(&item.expr);
                }
            }
            LogicalPlan::Project(project) => {
                for field in &project.fields {
                    self.collect_expr_ast(&field.expr);
                }
            }
            LogicalPlan::StatefulFunction(stateful) => {
                for call in &stateful.calls {
                    self.collect_expr_ast(&call.spec.original_expr);
                }
            }
            LogicalPlan::Filter(filter) => self.collect_expr_ast(&filter.predicate),
            LogicalPlan::Aggregation(agg) => {
                for expr in agg.aggregate_mappings.values() {
                    self.collect_expr_ast(expr);
                }
                for expr in &agg.group_by_exprs {
                    self.collect_expr_ast(expr);
                }
            }
            LogicalPlan::Window(window) => {
                if let crate::planner::logical::LogicalWindowSpec::State {
                    open,
                    emit,
                    partition_by,
                } = &window.spec
                {
                    self.collect_expr_ast(open.as_ref());
                    self.collect_expr_ast(emit.as_ref());
                    for expr in partition_by {
                        self.collect_expr_ast(expr);
                    }
                }
            }
            LogicalPlan::DataSource(_) => {}
            LogicalPlan::DataSink(_) => {}
            LogicalPlan::Tail(TailPlan { .. }) => {}
        }

        for child in plan.children() {
            self.collect_from_plan(child.as_ref());
        }
    }

    fn collect_expr_ast(&mut self, expr: &SqlExpr) {
        match expr {
            SqlExpr::Function(func) => {
                for arg in &func.args {
                    self.collect_function_arg(arg);
                }
                visit_function_context_exprs(func, |expr| self.collect_expr_ast(expr));
            }
            SqlExpr::BinaryOp { left, right, .. } => {
                self.collect_expr_ast(left);
                self.collect_expr_ast(right);
            }
            SqlExpr::UnaryOp { expr, .. } => self.collect_expr_ast(expr),
            SqlExpr::Nested(expr) => self.collect_expr_ast(expr),
            SqlExpr::Cast { expr, .. } => self.collect_expr_ast(expr),
            SqlExpr::MapAccess { column, keys } => {
                if let Some(access) = extract_decode_field_path_access(expr) {
                    if access
                        .segments
                        .iter()
                        .any(|seg| matches!(seg, FieldPathSegment::ListIndex(_)))
                    {
                        self.record_list_decode_access(access);
                        for key in keys {
                            self.collect_expr_ast(key);
                        }
                        return;
                    }
                }

                self.collect_expr_ast(column);
                for key in keys {
                    self.collect_expr_ast(key);
                }
            }
            SqlExpr::JsonAccess { left, right, .. } => {
                if let Some(access) = extract_decode_field_path_access(expr) {
                    if access
                        .segments
                        .iter()
                        .any(|seg| matches!(seg, FieldPathSegment::ListIndex(_)))
                    {
                        self.record_list_decode_access(access);
                        return;
                    }
                }

                self.collect_expr_ast(left);
                self.collect_expr_ast(right);
            }
            _ => {}
        }
    }

    fn collect_function_arg(&mut self, arg: &FunctionArg) {
        match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => self.collect_expr_ast(expr),
            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => self.handle_wildcard(None),
            FunctionArg::Unnamed(FunctionArgExpr::QualifiedWildcard(obj_name)) => {
                self.handle_wildcard(Some(obj_name))
            }
            FunctionArg::Named { arg, .. } => match arg {
                FunctionArgExpr::Expr(expr) => self.collect_expr_ast(expr),
                FunctionArgExpr::Wildcard => self.handle_wildcard(None),
                FunctionArgExpr::QualifiedWildcard(obj_name) => {
                    self.handle_wildcard(Some(obj_name))
                }
            },
        }
    }

    fn handle_wildcard(&mut self, qualifier: Option<&ObjectName>) {
        if let Some(obj_name) = qualifier {
            let qualifier = obj_name.to_string();
            if let Some(source) = self.resolve_source(&qualifier) {
                self.prune_disabled.insert(source);
            } else {
                self.disable_pruning_for_all_sources();
            }
        } else {
            self.disable_pruning_for_all_sources();
        }
    }

    fn record_list_decode_access(&mut self, access: DecodeFieldPathAccess) {
        let Some(source_name) =
            self.resolve_source_for_column(access.qualifier.as_deref(), access.column.as_str())
        else {
            return;
        };

        let projection = self
            .decode_projections
            .entry(source_name.to_string())
            .or_default();
        projection.mark_field_path_used(&FieldPath {
            column: access.column,
            segments: access.segments,
        });
    }

    fn resolve_source_for_column(
        &mut self,
        qualifier: Option<&str>,
        column_name: &str,
    ) -> Option<String> {
        match qualifier {
            Some(q) => {
                if let Some(entry) = self.bindings.entries().iter().find(|b| b.matches(q)) {
                    if entry.schema.contains_column(column_name) {
                        Some(entry.source_name.clone())
                    } else {
                        self.prune_disabled.insert(entry.source_name.clone());
                        None
                    }
                } else {
                    self.disable_pruning_for_all_sources();
                    None
                }
            }
            None => {
                let mut matches = Vec::new();
                for entry in self.bindings.entries() {
                    if entry.schema.contains_column(column_name) {
                        matches.push(entry.source_name.clone());
                    }
                }
                match matches.len() {
                    0 => {
                        self.disable_pruning_for_all_sources();
                        None
                    }
                    1 => Some(matches[0].clone()),
                    _ => {
                        for src in matches {
                            self.prune_disabled.insert(src);
                        }
                        None
                    }
                }
            }
        }
    }

    fn resolve_source(&self, qualifier: &str) -> Option<String> {
        self.bindings
            .entries()
            .iter()
            .find(|entry| entry.matches(qualifier))
            .map(|entry| entry.source_name.clone())
    }

    fn disable_pruning_for_all_sources(&mut self) {
        for entry in self.bindings.entries() {
            self.prune_disabled.insert(entry.source_name.clone());
        }
    }

    fn build_decode_projections(&self) -> HashMap<String, DecodeProjection> {
        let mut projections = HashMap::new();
        for entry in self.bindings.entries() {
            let should_keep_full = matches!(
                entry.kind,
                crate::expr::sql_conversion::SourceBindingKind::Shared
                    | crate::expr::sql_conversion::SourceBindingKind::MemoryCollection
            ) || self.prune_disabled.contains(&entry.source_name);

            if should_keep_full {
                continue;
            }

            if let Some(projection) = self.decode_projections.get(&entry.source_name) {
                let mut projection = projection.clone();
                for column in entry.schema.column_schemas() {
                    if projection.column(column.name.as_str()).is_none() {
                        projection.mark_column_all(column.name.as_str());
                    }
                }
                projections.insert(entry.source_name.clone(), projection);
            }
        }
        projections
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DecodeFieldPathAccess {
    qualifier: Option<String>,
    column: String,
    segments: Vec<FieldPathSegment>,
}

fn decode_segments_to_schema_path(segments: &[FieldPathSegment]) -> Vec<String> {
    segments
        .iter()
        .map(|segment| match segment {
            FieldPathSegment::StructField(field) => field.clone(),
            FieldPathSegment::ListIndex(_) => "element".to_string(),
        })
        .collect()
}

fn split_compound_identifier(idents: &[Ident]) -> Option<(Option<String>, String)> {
    let last = idents.last()?;
    if idents.len() == 1 {
        return Some((None, last.value.clone()));
    }
    let qualifier = idents[..idents.len() - 1]
        .iter()
        .map(|i| i.value.clone())
        .collect::<Vec<_>>()
        .join(".");
    Some((Some(qualifier), last.value.clone()))
}

fn extract_decode_field_path_access(expr: &SqlExpr) -> Option<DecodeFieldPathAccess> {
    let (base, segments) = extract_decode_access_chain(expr)?;
    let (qualifier, column) = split_compound_identifier(&base)?;
    Some(DecodeFieldPathAccess {
        qualifier,
        column,
        segments,
    })
}

fn extract_decode_access_chain(expr: &SqlExpr) -> Option<(Vec<Ident>, Vec<FieldPathSegment>)> {
    match expr {
        SqlExpr::Nested(inner) => extract_decode_access_chain(inner.as_ref()),
        SqlExpr::Identifier(ident) => Some((vec![ident.clone()], Vec::new())),
        SqlExpr::CompoundIdentifier(idents) => Some((idents.clone(), Vec::new())),
        SqlExpr::JsonAccess {
            left,
            operator: sqlparser::ast::JsonOperator::Arrow,
            right,
        } => {
            let (base, mut segments) = extract_decode_access_chain(left.as_ref())?;
            if !append_relative_access_expr_segments(right.as_ref(), &mut segments) {
                return None;
            }
            Some((base, segments))
        }
        SqlExpr::MapAccess { column, keys } => {
            let (base, mut segments) = extract_decode_access_chain(column.as_ref())?;
            for key in keys {
                match extract_const_non_negative_index(key) {
                    Some(index) => {
                        segments.push(FieldPathSegment::ListIndex(ListIndex::Const(index)))
                    }
                    None => segments.push(FieldPathSegment::ListIndex(ListIndex::Dynamic)),
                }
            }
            Some((base, segments))
        }
        _ => None,
    }
}

fn append_relative_access_expr_segments(expr: &SqlExpr, out: &mut Vec<FieldPathSegment>) -> bool {
    match expr {
        SqlExpr::Nested(inner) => append_relative_access_expr_segments(inner.as_ref(), out),
        SqlExpr::Identifier(ident) => {
            out.push(FieldPathSegment::StructField(ident.value.clone()));
            true
        }
        SqlExpr::MapAccess { column, keys } => {
            let column = match column.as_ref() {
                SqlExpr::Nested(inner) => inner.as_ref(),
                other => other,
            };
            let SqlExpr::Identifier(ident) = column else {
                return false;
            };
            out.push(FieldPathSegment::StructField(ident.value.clone()));
            for key in keys {
                match extract_const_non_negative_index(key) {
                    Some(index) => out.push(FieldPathSegment::ListIndex(ListIndex::Const(index))),
                    None => out.push(FieldPathSegment::ListIndex(ListIndex::Dynamic)),
                }
            }
            true
        }
        SqlExpr::JsonAccess {
            left,
            operator: sqlparser::ast::JsonOperator::Arrow,
            right,
        } => {
            append_relative_access_expr_segments(left.as_ref(), out)
                && append_relative_access_expr_segments(right.as_ref(), out)
        }
        _ => false,
    }
}

fn extract_const_non_negative_index(expr: &SqlExpr) -> Option<usize> {
    match expr {
        SqlExpr::Value(sqlparser::ast::Value::Number(num, _)) => num.parse::<usize>().ok(),
        SqlExpr::UnaryOp {
            op: sqlparser::ast::UnaryOperator::Minus,
            expr,
        } => match expr.as_ref() {
            SqlExpr::Value(sqlparser::ast::Value::Number(_, _)) => None,
            _ => None,
        },
        SqlExpr::Nested(inner) => extract_const_non_negative_index(inner.as_ref()),
        _ => None,
    }
}

fn prune_struct_fields_column_schema(
    column: &datatypes::ColumnSchema,
    usage: &ColumnUse,
) -> datatypes::ColumnSchema {
    let data_type = prune_nested_datatype_for_usage(&column.data_type, usage);
    datatypes::ColumnSchema::new(column.source_name.clone(), column.name.clone(), data_type)
}

fn prune_nested_datatype_for_usage(
    datatype: &datatypes::ConcreteDatatype,
    usage: &ColumnUse,
) -> datatypes::ConcreteDatatype {
    match usage {
        ColumnUse::All => datatype.clone(),
        ColumnUse::Fields(fields) => match datatype {
            datatypes::ConcreteDatatype::Struct(struct_type) => {
                let mut pruned_fields = Vec::new();
                for field in struct_type.fields().iter() {
                    let Some(field_usage) = fields.get(field.name()) else {
                        continue;
                    };
                    let pruned = prune_nested_datatype_for_usage(field.data_type(), field_usage);
                    pruned_fields.push(datatypes::StructField::new(
                        field.name().to_string(),
                        pruned,
                        field.is_nullable(),
                    ));
                }
                if pruned_fields.is_empty() {
                    datatype.clone()
                } else {
                    datatypes::ConcreteDatatype::Struct(datatypes::StructType::new(Arc::new(
                        pruned_fields,
                    )))
                }
            }
            datatypes::ConcreteDatatype::List(list_type) => {
                let Some(element_usage) = fields.get("element") else {
                    return datatype.clone();
                };
                let item_type =
                    prune_nested_datatype_for_usage(list_type.item_type(), element_usage);
                datatypes::ConcreteDatatype::List(datatypes::ListType::new(Arc::new(item_type)))
            }
            _ => datatype.clone(),
        },
    }
}

/// Replace DataSource schemas in the logical plan with pruned schemas from the binding.
fn apply_pruned_schemas_to_logical(
    plan: Arc<LogicalPlan>,
    bindings: &SchemaBinding,
    decode_projections: &HashMap<String, DecodeProjection>,
    shared_required_schemas: &HashMap<String, Vec<String>>,
    shared_slot_versions: &HashMap<String, u64>,
) -> Arc<LogicalPlan> {
    let mut cache = HashMap::new();
    apply_pruned_with_cache(
        plan,
        bindings,
        decode_projections,
        shared_required_schemas,
        shared_slot_versions,
        &mut cache,
    )
}

fn apply_pruned_with_cache(
    plan: Arc<LogicalPlan>,
    bindings: &SchemaBinding,
    decode_projections: &HashMap<String, DecodeProjection>,
    shared_required_schemas: &HashMap<String, Vec<String>>,
    shared_slot_versions: &HashMap<String, u64>,
    cache: &mut HashMap<i64, Arc<LogicalPlan>>,
) -> Arc<LogicalPlan> {
    let idx = plan.get_plan_index();
    if let Some(existing) = cache.get(&idx) {
        return existing.clone();
    }

    let updated = match plan.as_ref() {
        LogicalPlan::DataSource(ds) => {
            let schema = bindings
                .entries()
                .iter()
                .find(|entry| entry.source_name == ds.source_name)
                .map(|entry| Arc::clone(&entry.schema))
                .unwrap_or_else(|| ds.schema());
            // decoder.type="none" means there is no decode stage; any decode projection must be ignored.
            let decode_projection = if ds.decoder().kind() == "none" {
                None
            } else {
                decode_projections.get(&ds.source_name).cloned()
            };
            let shared_required_schema = shared_required_schemas
                .get(&ds.source_name)
                .cloned()
                .or_else(|| ds.shared_required_schema.clone());
            let shared_slot_version = shared_slot_versions
                .get(&ds.source_name)
                .copied()
                .or(ds.shared_slot_version);
            if Arc::ptr_eq(&schema, &ds.schema)
                && ds.decode_projection == decode_projection
                && ds.shared_required_schema == shared_required_schema
                && ds.shared_slot_version == shared_slot_version
            {
                plan
            } else {
                let mut new_ds = ds.clone();
                new_ds.schema = schema;
                new_ds.decode_projection = decode_projection;
                new_ds.shared_required_schema = shared_required_schema;
                new_ds.shared_slot_version = shared_slot_version;
                Arc::new(LogicalPlan::DataSource(new_ds))
            }
        }
        _ => {
            let new_children: Vec<Arc<LogicalPlan>> = plan
                .children()
                .iter()
                .map(|child| {
                    apply_pruned_with_cache(
                        child.clone(),
                        bindings,
                        decode_projections,
                        shared_required_schemas,
                        shared_slot_versions,
                        cache,
                    )
                })
                .collect();

            let children_unchanged = plan
                .children()
                .iter()
                .zip(new_children.iter())
                .all(|(old, new)| Arc::ptr_eq(old, new));
            if children_unchanged {
                plan
            } else {
                clone_with_children(plan.as_ref(), new_children)
            }
        }
    };

    cache.insert(idx, updated.clone());
    updated
}

fn clone_with_children(plan: &LogicalPlan, children: Vec<Arc<LogicalPlan>>) -> Arc<LogicalPlan> {
    match plan {
        LogicalPlan::DataSource(ds) => Arc::new(LogicalPlan::DataSource(ds.clone())),
        LogicalPlan::StatefulFunction(stateful) => {
            let mut new = stateful.clone();
            new.base.children = children;
            Arc::new(LogicalPlan::StatefulFunction(new))
        }
        LogicalPlan::Filter(filter) => {
            let mut new = filter.clone();
            new.base.children = children;
            Arc::new(LogicalPlan::Filter(new))
        }
        LogicalPlan::Aggregation(agg) => {
            let mut new = agg.clone();
            new.base.children = children;
            Arc::new(LogicalPlan::Aggregation(new))
        }
        LogicalPlan::Compute(compute) => {
            let mut new = compute.clone();
            new.base.children = children;
            Arc::new(LogicalPlan::Compute(new))
        }
        LogicalPlan::Order(order) => {
            let mut new = order.clone();
            new.base.children = children;
            Arc::new(LogicalPlan::Order(new))
        }
        LogicalPlan::Project(project) => {
            let mut new = project.clone();
            new.base.children = children;
            Arc::new(LogicalPlan::Project(new))
        }
        LogicalPlan::DataSink(sink) => {
            let mut new = sink.clone();
            new.base.children = children;
            Arc::new(LogicalPlan::DataSink(new))
        }
        LogicalPlan::Tail(tail) => {
            let mut new = tail.clone();
            new.base.children = children;
            Arc::new(LogicalPlan::Tail(new))
        }
        LogicalPlan::Window(window) => {
            let mut new = window.clone();
            new.base.children = children;
            Arc::new(LogicalPlan::Window(new))
        }
    }
}
