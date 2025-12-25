use crate::expr::sql_conversion::{SchemaBinding, SchemaBindingEntry};
use crate::planner::logical::{LogicalPlan, TailPlan};
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
    optimize_logical_plan_with_options(logical_plan, bindings, &LogicalOptimizerOptions::default())
}

#[derive(Debug, Clone, Copy, Default)]
pub struct LogicalOptimizerOptions {
    pub eventtime_enabled: bool,
}

/// Apply logical plan optimizations with extra options (e.g. eventtime).
pub fn optimize_logical_plan_with_options(
    logical_plan: Arc<LogicalPlan>,
    bindings: &SchemaBinding,
    options: &LogicalOptimizerOptions,
) -> (Arc<LogicalPlan>, SchemaBinding) {
    let rules: Vec<Box<dyn LogicalOptRule>> = vec![Box::new(ColumnPruning {
        eventtime_enabled: options.eventtime_enabled,
    })];

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
struct ColumnPruning {
    eventtime_enabled: bool,
}

impl LogicalOptRule for ColumnPruning {
    fn name(&self) -> &str {
        "column_pruning"
    }

    fn optimize(
        &self,
        plan: Arc<LogicalPlan>,
        bindings: &SchemaBinding,
    ) -> (Arc<LogicalPlan>, SchemaBinding) {
        let mut collector = ColumnUsageCollector::new(bindings, self.eventtime_enabled);
        collector.collect_from_plan(plan.as_ref());
        let pruned = collector.build_pruned_binding();
        let updated_plan = apply_pruned_schemas_to_logical(plan, &pruned);
        (updated_plan, pruned)
    }
}

/// Collects column usage for pruning decisions.
struct ColumnUsageCollector<'a> {
    bindings: &'a SchemaBinding,
    used_columns: HashMap<String, UsedColumnTree>,
    /// Sources for which pruning is disabled (e.g., wildcard or ambiguous)
    prune_disabled: HashSet<String>,
    eventtime_enabled: bool,
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

impl<'a> ColumnUsageCollector<'a> {
    fn new(bindings: &'a SchemaBinding, eventtime_enabled: bool) -> Self {
        Self {
            bindings,
            used_columns: HashMap::new(),
            prune_disabled: HashSet::new(),
            eventtime_enabled,
        }
    }

    fn collect_from_plan(&mut self, plan: &LogicalPlan) {
        match plan {
            LogicalPlan::Project(project) => {
                for field in &project.fields {
                    self.collect_expr(&field.expr);
                }
            }
            LogicalPlan::StatefulFunction(stateful) => {
                for expr in stateful.stateful_mappings.values() {
                    self.collect_expr(expr);
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
            LogicalPlan::Window(window) => {
                if let crate::planner::logical::LogicalWindowSpec::State {
                    open,
                    emit,
                    partition_by,
                } = &window.spec
                {
                    self.collect_expr(open.as_ref());
                    self.collect_expr(emit.as_ref());
                    for expr in partition_by {
                        self.collect_expr(expr);
                    }
                }
            }
            LogicalPlan::DataSource(ds) => {
                if self.eventtime_enabled {
                    if let Some(eventtime) = ds.eventtime() {
                        self.mark_column_used(&ds.source_name, eventtime.column());
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
                if let Some(access) = extract_struct_field_access(expr) {
                    self.record_struct_field_access(access);
                } else if let Some(access) = extract_list_element_struct_field_access(expr) {
                    self.record_struct_field_access(access);
                } else {
                    self.collect_expr_ast(left);
                    self.collect_expr_ast(right);
                }
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
                    1 => {
                        self.mark_column_used(&matches[0], column_name);
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

    fn mark_column_used(&mut self, source_name: &str, column_name: &str) {
        let tree = self.used_columns.entry(source_name.to_string()).or_default();
        tree.columns
            .insert(column_name.to_string(), ColumnUse::All);
    }

    fn record_struct_field_access(&mut self, access: StructFieldAccess) {
        if access.field_path.is_empty() {
            self.record_identifier(access.qualifier.as_deref(), &Ident::new(access.column));
            return;
        }

        let Some(source_name) = self.resolve_source_for_column(
            access.qualifier.as_deref(),
            access.column.as_str(),
        ) else {
            return;
        };
        self.mark_field_path_used(&source_name, access.column.as_str(), &access.field_path);
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
        let tree = self.used_columns.entry(source_name.to_string()).or_default();
        let Some(root_use) = tree.columns.get_mut(column_name) else {
            tree.columns.insert(
                column_name.to_string(),
                ColumnUse::Fields(HashMap::new()),
            );
            return self.mark_field_path_used(source_name, column_name, path);
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
                    if !map.contains_key(segment.as_str()) {
                        map.insert(segment.clone(), ColumnUse::Fields(HashMap::new()));
                    }
                    current = map.get_mut(segment.as_str()).expect("inserted above");
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

    fn build_pruned_binding(&self) -> SchemaBinding {
        let mut entries = Vec::new();
        for entry in self.bindings.entries() {
            let should_keep_full = matches!(
                entry.kind,
                crate::expr::sql_conversion::SourceBindingKind::Shared
            ) || self.prune_disabled.contains(&entry.source_name)
                || !self.used_columns.contains_key(&entry.source_name);

            let schema = if should_keep_full {
                Arc::clone(&entry.schema)
            } else {
                let required = &self.used_columns[&entry.source_name];
                let filtered: Vec<_> = entry
                    .schema
                    .column_schemas()
                    .iter()
                    .filter_map(|col| {
                        let usage = required.columns.get(col.name.as_str())?;
                        Some(prune_column_schema(col, usage))
                    })
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct StructFieldAccess {
    qualifier: Option<String>,
    column: String,
    field_path: Vec<String>,
}

fn extract_struct_field_access(expr: &SqlExpr) -> Option<StructFieldAccess> {
    let (base, fields) = extract_json_access_chain(expr)?;
    let (qualifier, column) = split_compound_identifier(&base)?;
    Some(StructFieldAccess {
        qualifier,
        column,
        field_path: fields,
    })
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

fn extract_json_access_chain(expr: &SqlExpr) -> Option<(Vec<Ident>, Vec<String>)> {
    match expr {
        SqlExpr::JsonAccess {
            left,
            operator,
            right,
        } => match operator {
            sqlparser::ast::JsonOperator::Arrow => {
                let (base, mut fields) = extract_json_access_chain(left.as_ref())
                    .or_else(|| extract_json_access_base(left.as_ref()).map(|b| (b, Vec::new())))?;
                let field_name = match right.as_ref() {
                    SqlExpr::Identifier(ident) => ident.value.clone(),
                    _ => return None,
                };
                fields.push(field_name);
                Some((base, fields))
            }
            _ => None,
        },
        _ => None,
    }
}

fn extract_json_access_base(expr: &SqlExpr) -> Option<Vec<Ident>> {
    match expr {
        SqlExpr::Identifier(ident) => Some(vec![ident.clone()]),
        SqlExpr::CompoundIdentifier(idents) => Some(idents.clone()),
        _ => None,
    }
}

fn extract_list_element_struct_field_access(expr: &SqlExpr) -> Option<StructFieldAccess> {
    let SqlExpr::JsonAccess { left, right, .. } = expr else {
        return None;
    };

    let SqlExpr::MapAccess { column, .. } = left.as_ref() else {
        return None;
    };

    let base_idents = extract_json_access_base(column.as_ref())?;
    let (qualifier, column) = split_compound_identifier(&base_idents)?;

    let field_name = match right.as_ref() {
        SqlExpr::Identifier(ident) => ident.value.clone(),
        _ => return None,
    };

    Some(StructFieldAccess {
        qualifier,
        column,
        field_path: vec!["element".to_string(), field_name],
    })
}

fn prune_column_schema(column: &datatypes::ColumnSchema, usage: &ColumnUse) -> datatypes::ColumnSchema {
    let data_type = prune_datatype(&column.data_type, usage);
    datatypes::ColumnSchema::new(
        column.source_name.clone(),
        column.name.clone(),
        data_type,
    )
}

fn prune_datatype(datatype: &datatypes::ConcreteDatatype, usage: &ColumnUse) -> datatypes::ConcreteDatatype {
    match usage {
        ColumnUse::All => datatype.clone(),
        ColumnUse::Fields(fields) => match datatype {
            datatypes::ConcreteDatatype::Struct(struct_type) => {
                let mut pruned_fields = Vec::new();
                for field in struct_type.fields().iter() {
                    let Some(field_usage) = fields.get(field.name()) else {
                        continue;
                    };
                    let pruned = prune_datatype(field.data_type(), field_usage);
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
                let item_type = prune_datatype(list_type.item_type(), element_usage);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{MqttStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps};
    use crate::planner::explain::ExplainReport;
    use crate::planner::logical::create_logical_plan;
    use datatypes::{
        ColumnSchema, ConcreteDatatype, Int64Type, ListType, Schema, StringType, StructField,
        StructType,
    };
    use parser::parse_sql;
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn logical_optimizer_prunes_datasource_schema() {
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "stream".to_string(),
                "a".to_string(),
                ConcreteDatatype::Int64(Int64Type),
            ),
            ColumnSchema::new(
                "stream".to_string(),
                "b".to_string(),
                ConcreteDatatype::Int64(Int64Type),
            ),
        ]));
        let definition = StreamDefinition::new(
            "stream",
            Arc::clone(&schema),
            StreamProps::Mqtt(MqttStreamProps::default()),
            StreamDecoderConfig::json(),
        );
        let mut stream_defs = HashMap::new();
        stream_defs.insert("stream".to_string(), Arc::new(definition));

        let select_stmt = parse_sql("SELECT a FROM stream").expect("parse sql");
        let logical_plan =
            create_logical_plan(select_stmt, vec![], &stream_defs).expect("logical plan");

        let bindings = crate::expr::sql_conversion::SchemaBinding::new(vec![
            crate::expr::sql_conversion::SchemaBindingEntry {
                source_name: "stream".to_string(),
                alias: None,
                schema: Arc::clone(&schema),
                kind: crate::expr::sql_conversion::SourceBindingKind::Regular,
            },
        ]);

        let pre_json = ExplainReport::from_logical(Arc::clone(&logical_plan))
            .to_json()
            .to_string();
        assert_eq!(
            pre_json,
            r##"{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a, b]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a]"],"operator":"Project"}"##
        );
        let (optimized_logical, _pruned_binding) =
            optimize_logical_plan(Arc::clone(&logical_plan), &bindings);
        let post_json = ExplainReport::from_logical(optimized_logical)
            .to_json()
            .to_string();
        assert_eq!(
            post_json,
            r##"{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a]"],"operator":"Project"}"##
        );
    }

    #[test]
    fn logical_optimizer_keeps_window_partition_columns() {
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "stream".to_string(),
                "a".to_string(),
                ConcreteDatatype::Int64(Int64Type),
            ),
            ColumnSchema::new(
                "stream".to_string(),
                "b".to_string(),
                ConcreteDatatype::Int64(Int64Type),
            ),
            ColumnSchema::new(
                "stream".to_string(),
                "c".to_string(),
                ConcreteDatatype::Int64(Int64Type),
            ),
            ColumnSchema::new(
                "stream".to_string(),
                "d".to_string(),
                ConcreteDatatype::Int64(Int64Type),
            ),
        ]));
        let definition = StreamDefinition::new(
            "stream",
            Arc::clone(&schema),
            StreamProps::Mqtt(MqttStreamProps::default()),
            StreamDecoderConfig::json(),
        );
        let mut stream_defs = HashMap::new();
        stream_defs.insert("stream".to_string(), Arc::new(definition));

        let sql =
            "SELECT a FROM stream GROUP BY statewindow(a = 1, a = 4) OVER (PARTITION BY b, c)";
        let select_stmt = parse_sql(sql).expect("parse sql");
        let logical =
            create_logical_plan(select_stmt, Vec::new(), &stream_defs).expect("logical plan");

        let bindings = SchemaBinding::new(vec![SchemaBindingEntry {
            source_name: "stream".to_string(),
            alias: None,
            schema: Arc::clone(&schema),
            kind: crate::expr::sql_conversion::SourceBindingKind::Regular,
        }]);

        let (optimized, pruned) = optimize_logical_plan(Arc::clone(&logical), &bindings);

        // The pruned schema must keep a (project) plus b/c (window partition).
        let entry = pruned
            .entries()
            .iter()
            .find(|e| e.source_name == "stream")
            .expect("binding entry");
        let cols: Vec<_> = entry
            .schema
            .column_schemas()
            .iter()
            .map(|c| c.name.as_str())
            .collect();

        assert!(cols.contains(&"a"));
        assert!(cols.contains(&"b"));
        assert!(cols.contains(&"c"));

        // Avoid unused warning for optimized plan.
        assert_eq!(optimized.get_plan_type(), "Project");
    }

    #[test]
    fn logical_optimizer_prunes_struct_fields_and_explain_renders_projection() {
        let user_struct = ConcreteDatatype::Struct(StructType::new(Arc::new(vec![
            StructField::new(
                "c".to_string(),
                ConcreteDatatype::Int64(Int64Type),
                false,
            ),
            StructField::new(
                "d".to_string(),
                ConcreteDatatype::String(StringType),
                false,
            ),
        ])));

        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "stream_2".to_string(),
                "a".to_string(),
                ConcreteDatatype::Int64(Int64Type),
            ),
            ColumnSchema::new("stream_2".to_string(), "b".to_string(), user_struct),
        ]));

        let definition = StreamDefinition::new(
            "stream_2",
            Arc::clone(&schema),
            StreamProps::Mqtt(MqttStreamProps::default()),
            StreamDecoderConfig::json(),
        );
        let mut stream_defs = HashMap::new();
        stream_defs.insert("stream_2".to_string(), Arc::new(definition));

        let select_stmt =
            parse_sql("SELECT stream_2.a, stream_2.b->c FROM stream_2").expect("parse sql");
        let logical_plan =
            create_logical_plan(select_stmt, vec![], &stream_defs).expect("logical plan");

        let bindings = SchemaBinding::new(vec![SchemaBindingEntry {
            source_name: "stream_2".to_string(),
            alias: None,
            schema: Arc::clone(&schema),
            kind: crate::expr::sql_conversion::SourceBindingKind::Regular,
        }]);

        let (optimized, _pruned) = optimize_logical_plan(Arc::clone(&logical_plan), &bindings);
        let report = ExplainReport::from_logical(optimized);
        let topology = report.topology_string();
        println!("{topology}");
        assert!(topology.contains("schema=[a, b{c}]"));
    }

    #[test]
    fn logical_explain_reflects_pruned_list_struct_schema() {
        let element_struct = ConcreteDatatype::Struct(StructType::new(Arc::new(vec![
            StructField::new(
                "c".to_string(),
                ConcreteDatatype::Int64(Int64Type),
                false,
            ),
            StructField::new(
                "d".to_string(),
                ConcreteDatatype::String(StringType),
                false,
            ),
        ])));

        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "stream_3".to_string(),
                "a".to_string(),
                ConcreteDatatype::Int64(Int64Type),
            ),
            ColumnSchema::new(
                "stream_3".to_string(),
                "items".to_string(),
                ConcreteDatatype::List(ListType::new(Arc::new(element_struct))),
            ),
        ]));

        let definition = StreamDefinition::new(
            "stream_3",
            Arc::clone(&schema),
            StreamProps::Mqtt(MqttStreamProps::default()),
            StreamDecoderConfig::json(),
        );
        let mut stream_defs = HashMap::new();
        stream_defs.insert("stream_3".to_string(), Arc::new(definition));

        let select_stmt =
            parse_sql("SELECT stream_3.a, stream_3.items[0]->c FROM stream_3").expect("parse sql");
        let logical_plan =
            create_logical_plan(select_stmt, vec![], &stream_defs).expect("logical plan");

        let bindings = SchemaBinding::new(vec![SchemaBindingEntry {
            source_name: "stream_3".to_string(),
            alias: None,
            schema: Arc::clone(&schema),
            kind: crate::expr::sql_conversion::SourceBindingKind::Regular,
        }]);

        let (optimized, _pruned) = optimize_logical_plan(Arc::clone(&logical_plan), &bindings);
        let report = ExplainReport::from_logical(optimized);
        let topology = report.topology_string();
        println!("{topology}");
        assert!(topology.contains("schema=[a, items[struct{c}]]"));
    }
}
