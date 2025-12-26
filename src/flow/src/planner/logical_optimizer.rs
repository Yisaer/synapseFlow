use crate::expr::sql_conversion::{SchemaBinding, SchemaBindingEntry};
use crate::planner::decode_projection::{DecodeProjection, FieldPath, FieldPathSegment, ListIndex};
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
    let rules: Vec<Box<dyn LogicalOptRule>> = vec![
        Box::new(TopLevelColumnPruning {
            eventtime_enabled: options.eventtime_enabled,
        }),
        Box::new(StructFieldPruning),
        Box::new(ListElementPruning),
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

/// Rule: prune unused top-level columns from data sources.
struct TopLevelColumnPruning {
    eventtime_enabled: bool,
}

impl LogicalOptRule for TopLevelColumnPruning {
    fn name(&self) -> &str {
        "top_level_column_pruning"
    }

    fn optimize(
        &self,
        plan: Arc<LogicalPlan>,
        bindings: &SchemaBinding,
    ) -> (Arc<LogicalPlan>, SchemaBinding) {
        let mut collector = TopLevelColumnUsageCollector::new(bindings, self.eventtime_enabled);
        collector.collect_from_plan(plan.as_ref());
        let pruned = collector.build_pruned_binding();
        let shared_required_schemas = collector.build_shared_required_schemas();
        let updated_plan = apply_pruned_schemas_to_logical(
            plan,
            &pruned,
            &HashMap::new(),
            &shared_required_schemas,
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
        let updated_plan =
            apply_pruned_schemas_to_logical(plan, &pruned, &HashMap::new(), &HashMap::new());
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
        let updated_plan =
            apply_pruned_schemas_to_logical(plan, bindings, &decode_projections, &HashMap::new());
        (updated_plan, bindings.clone())
    }
}

/// Collects top-level column usage for pruning decisions.
struct TopLevelColumnUsageCollector<'a> {
    bindings: &'a SchemaBinding,
    used_columns: HashMap<String, HashSet<String>>,
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
                if !map.contains_key(segment.as_str()) {
                    map.insert(segment.clone(), ColumnUse::Fields(HashMap::new()));
                }
                current = map.get_mut(segment.as_str()).expect("inserted above");
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
        }
    }

    fn collect_from_plan(&mut self, plan: &LogicalPlan) {
        match plan {
            LogicalPlan::Project(project) => {
                for field in &project.fields {
                    self.collect_expr_ast(&field.expr);
                }
            }
            LogicalPlan::StatefulFunction(stateful) => {
                for expr in stateful.stateful_mappings.values() {
                    self.collect_expr_ast(expr);
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
            }
            SqlExpr::BinaryOp { left, right, .. } => {
                self.collect_expr_ast(left);
                self.collect_expr_ast(right);
            }
            SqlExpr::UnaryOp { expr, .. } => self.collect_expr_ast(expr),
            SqlExpr::Nested(expr) => self.collect_expr_ast(expr),
            SqlExpr::Cast { expr, .. } => self.collect_expr_ast(expr),
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
            LogicalPlan::Project(project) => {
                for field in &project.fields {
                    self.collect_expr_ast(&field.expr);
                }
            }
            LogicalPlan::StatefulFunction(stateful) => {
                for expr in stateful.stateful_mappings.values() {
                    self.collect_expr_ast(expr);
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
            LogicalPlan::Project(project) => {
                for field in &project.fields {
                    self.collect_expr_ast(&field.expr);
                }
            }
            LogicalPlan::StatefulFunction(stateful) => {
                for expr in stateful.stateful_mappings.values() {
                    self.collect_expr_ast(expr);
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
            ) || self.prune_disabled.contains(&entry.source_name);

            if should_keep_full {
                continue;
            }

            if let Some(projection) = self.decode_projections.get(&entry.source_name) {
                projections.insert(entry.source_name.clone(), projection.clone());
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
) -> Arc<LogicalPlan> {
    let mut cache = HashMap::new();
    apply_pruned_with_cache(
        plan,
        bindings,
        decode_projections,
        shared_required_schemas,
        &mut cache,
    )
}

fn apply_pruned_with_cache(
    plan: Arc<LogicalPlan>,
    bindings: &SchemaBinding,
    decode_projections: &HashMap<String, DecodeProjection>,
    shared_required_schemas: &HashMap<String, Vec<String>>,
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
            let decode_projection = decode_projections.get(&ds.source_name).cloned();
            let shared_required_schema = shared_required_schemas
                .get(&ds.source_name)
                .cloned()
                .or_else(|| ds.shared_required_schema.clone());
            if Arc::ptr_eq(&schema, &ds.schema)
                && ds.decode_projection == decode_projection
                && ds.shared_required_schema == shared_required_schema
            {
                plan
            } else {
                let mut new_ds = ds.clone();
                new_ds.schema = schema;
                new_ds.decode_projection = decode_projection;
                new_ds.shared_required_schema = shared_required_schema;
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
