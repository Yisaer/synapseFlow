use crate::expr::sql_conversion::{SchemaBinding, SchemaBindingEntry};
use crate::planner::logical::{LogicalPlan, LogicalWindow, TailPlan};
use datatypes::Schema;
use sqlparser::ast::{Expr as SqlExpr, FunctionArg, FunctionArgExpr, Ident, ObjectName};
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
    let rules: Vec<Box<dyn LogicalOptRule>> = vec![Box::new(ColumnPruning)];

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

/// Rule: prune unused columns from data sources.
///
/// Traverses the logical plan expressions to find which source columns are
/// referenced and constructs a pruned SchemaBinding. Logical plan topology is
/// kept as-is; only the bindings (and thus physical DataSource schemas) change.
struct ColumnPruning;

impl LogicalOptRule for ColumnPruning {
    fn name(&self) -> &str {
        "column_pruning"
    }

    fn optimize(
        &self,
        plan: Arc<LogicalPlan>,
        bindings: &SchemaBinding,
    ) -> (Arc<LogicalPlan>, SchemaBinding) {
        let mut collector = ColumnUsageCollector::new(bindings);
        collector.collect_from_plan(plan.as_ref());
        let pruned = collector.build_pruned_binding();
        let updated_plan = apply_pruned_schemas_to_logical(plan, &pruned);
        (updated_plan, pruned)
    }
}

/// Collects column usage for pruning decisions.
struct ColumnUsageCollector<'a> {
    bindings: &'a SchemaBinding,
    /// Map of source_name -> set of referenced column names
    used_columns: HashMap<String, HashSet<String>>,
    /// Sources for which pruning is disabled (e.g., wildcard or ambiguous)
    prune_disabled: HashSet<String>,
}

impl<'a> ColumnUsageCollector<'a> {
    fn new(bindings: &'a SchemaBinding) -> Self {
        Self {
            bindings,
            used_columns: HashMap::new(),
            prune_disabled: HashSet::new(),
        }
    }

    fn collect_from_plan(&mut self, plan: &LogicalPlan) {
        match plan {
            LogicalPlan::Project(project) => {
                for field in &project.fields {
                    self.collect_expr(&field.expr);
                }
            }
            LogicalPlan::Filter(filter) => {
                self.collect_expr(&filter.predicate);
            }
            LogicalPlan::Aggregation(agg) => {
                for expr in agg.aggregate_mappings.values() {
                    self.collect_expr(expr);
                }
                for expr in &agg.group_by_exprs {
                    self.collect_expr(expr);
                }
            }
            LogicalPlan::Window(LogicalWindow { .. }) => {}
            LogicalPlan::DataSource(_) => {}
            LogicalPlan::DataSink(_) => {}
            LogicalPlan::Tail(TailPlan { .. }) => {}
        }

        for child in plan.children() {
            self.collect_from_plan(child.as_ref());
        }
    }

    fn collect_expr(&mut self, expr: &sqlparser::ast::Expr) {
        self.collect_expr_ast(expr);
    }

    fn collect_expr_ast(&mut self, expr: &SqlExpr) {
        match expr {
            SqlExpr::Identifier(ident) => self.record_identifier(None, ident),
            SqlExpr::CompoundIdentifier(idents) => self.record_compound_identifier(idents),
            SqlExpr::Function(func) => {
                for arg in &func.args {
                    self.collect_function_arg(arg);
                }
            }
            SqlExpr::BinaryOp { left, right, .. } => {
                self.collect_expr_ast(left);
                self.collect_expr_ast(right);
            }
            SqlExpr::UnaryOp { expr, .. } => self.collect_expr_ast(expr),
            SqlExpr::Nested(expr) => self.collect_expr_ast(expr),
            SqlExpr::Cast { expr, .. } => self.collect_expr_ast(expr),
            SqlExpr::JsonAccess { left, right, .. } => {
                self.collect_expr_ast(left);
                self.collect_expr_ast(right);
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

        if is_aggregate_placeholder(column_name) {
            return;
        }

        match qualifier {
            Some(q) => {
                if let Some(entry) = self.bindings.entries().iter().find(|b| b.matches(q)) {
                    if entry.schema.contains_column(column_name) {
                        self.used_columns
                            .entry(entry.source_name.clone())
                            .or_default()
                            .insert(column_name.to_string());
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
                    1 => {
                        self.used_columns
                            .entry(matches[0].clone())
                            .or_default()
                            .insert(column_name.to_string());
                    }
                    _ => {
                        // Ambiguous: keep all matching sources unpruned.
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
            let should_keep_full = self.prune_disabled.contains(&entry.source_name)
                || !self.used_columns.contains_key(&entry.source_name);

            let schema = if should_keep_full {
                Arc::clone(&entry.schema)
            } else {
                let required = &self.used_columns[&entry.source_name];
                let filtered: Vec<_> = entry
                    .schema
                    .column_schemas()
                    .iter()
                    .filter(|col| required.contains(&col.name))
                    .cloned()
                    .collect();
                Arc::new(Schema::new(filtered))
            };

            let pruned_entry = SchemaBindingEntry {
                source_name: entry.source_name.clone(),
                alias: entry.alias.clone(),
                schema,
                kind: entry.kind.clone(),
            };
            entries.push(pruned_entry);
        }

        SchemaBinding::new(entries)
    }
}

/// Replace DataSource schemas in the logical plan with pruned schemas from the binding.
fn apply_pruned_schemas_to_logical(
    plan: Arc<LogicalPlan>,
    bindings: &SchemaBinding,
) -> Arc<LogicalPlan> {
    let mut cache = HashMap::new();
    apply_pruned_with_cache(plan, bindings, &mut cache)
}

fn apply_pruned_with_cache(
    plan: Arc<LogicalPlan>,
    bindings: &SchemaBinding,
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
            if Arc::ptr_eq(&schema, &ds.schema) {
                plan
            } else {
                let mut new_ds = ds.clone();
                new_ds.schema = schema;
                Arc::new(LogicalPlan::DataSource(new_ds))
            }
        }
        _ => {
            let new_children: Vec<Arc<LogicalPlan>> = plan
                .children()
                .iter()
                .map(|child| apply_pruned_with_cache(child.clone(), bindings, cache))
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

fn is_aggregate_placeholder(name: &str) -> bool {
    name.starts_with("col_") && name[4..].chars().all(|c| c.is_ascii_digit())
}
