use crate::catalog::StreamDefinition;
use parser::window as parser_window;
use parser::SelectStmt;
use std::collections::HashMap;
use std::sync::Arc;

use datatypes::{ConcreteDatatype, Schema};

pub mod aggregation;
pub mod datasource;
pub mod filter;
pub mod project;
pub mod sink;
pub mod stateful_function;
pub mod tail;
pub mod window;

use crate::planner::sink::PipelineSink;
pub use aggregation::Aggregation;
pub use datasource::DataSource;
pub use filter::Filter;
pub use project::Project;
pub use sink::DataSinkPlan;
pub use stateful_function::StatefulFunctionPlan;
pub use tail::TailPlan;
pub use window::{LogicalWindow, LogicalWindowSpec, TimeUnit};

#[derive(Debug, Clone)]
pub struct BaseLogicalPlan {
    pub index: i64,
    pub children: Vec<Arc<LogicalPlan>>,
}

impl BaseLogicalPlan {
    pub fn new(children: Vec<Arc<LogicalPlan>>, index: i64) -> Self {
        Self { children, index }
    }

    pub fn children(&self) -> &[Arc<LogicalPlan>] {
        &self.children
    }

    pub fn index(&self) -> i64 {
        self.index
    }
}

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    DataSource(DataSource),
    StatefulFunction(StatefulFunctionPlan),
    Filter(Filter),
    Aggregation(Aggregation),
    Project(Project),
    DataSink(DataSinkPlan),
    Tail(TailPlan),
    Window(LogicalWindow),
}

impl LogicalPlan {
    pub fn children(&self) -> &[Arc<LogicalPlan>] {
        match self {
            LogicalPlan::DataSource(plan) => plan.base.children(),
            LogicalPlan::StatefulFunction(plan) => plan.base.children(),
            LogicalPlan::Filter(plan) => plan.base.children(),
            LogicalPlan::Aggregation(plan) => plan.base.children(),
            LogicalPlan::Project(plan) => plan.base.children(),
            LogicalPlan::DataSink(plan) => plan.base.children(),
            LogicalPlan::Tail(plan) => plan.base.children(),
            LogicalPlan::Window(plan) => plan.base.children(),
        }
    }

    pub fn get_plan_type(&self) -> &str {
        match self {
            LogicalPlan::DataSource(_) => "DataSource",
            LogicalPlan::StatefulFunction(_) => "StatefulFunction",
            LogicalPlan::Filter(_) => "Filter",
            LogicalPlan::Aggregation(_) => "Aggregation",
            LogicalPlan::Project(_) => "Project",
            LogicalPlan::DataSink(_) => "DataSink",
            LogicalPlan::Tail(_) => "Tail",
            LogicalPlan::Window(_) => "Window",
        }
    }

    pub fn get_plan_index(&self) -> i64 {
        match self {
            LogicalPlan::DataSource(plan) => plan.base.index(),
            LogicalPlan::StatefulFunction(plan) => plan.base.index(),
            LogicalPlan::Filter(plan) => plan.base.index(),
            LogicalPlan::Aggregation(plan) => plan.base.index(),
            LogicalPlan::Project(plan) => plan.base.index(),
            LogicalPlan::DataSink(plan) => plan.base.index(),
            LogicalPlan::Tail(plan) => plan.base.index(),
            LogicalPlan::Window(plan) => plan.base.index(),
        }
    }

    /// Get the plan name in format: {{plan_type}}_{{plan_index}}
    pub fn get_plan_name(&self) -> String {
        format!("{}_{}", self.get_plan_type(), self.get_plan_index())
    }

    /// Print logical topology (similar to PhysicalPlan::print_topology)
    pub fn print_topology(&self, indent: usize) {
        let spacing = "  ".repeat(indent);
        println!(
            "{}{} (index: {})",
            spacing,
            self.get_plan_type(),
            self.get_plan_index()
        );

        for child in self.children() {
            child.print_topology(indent + 1);
        }
    }
}

/// Create a LogicalPlan from a SelectStmt
///
/// The plan structure will be:
/// - DataSource(s) (from SelectStmt::source_infos, one per source)
/// - StatefulFunction (from SelectStmt::stateful_mappings, if present) - takes DataSources as children
/// - Window (from SelectStmt::window, if present) - takes DataSources as children
/// - Aggregation (from SelectStmt::aggregate_mappings, if present) - takes Window or DataSources as children
/// - Filter (from SelectStmt::where_condition, if present) - takes Aggregation, Window, or DataSources as children
/// - Project (from SelectStmt::select_fields) - takes Filter, Aggregation, Window, or DataSources as children
///
/// # Arguments
///
/// * `select_stmt` - The parsed SELECT statement
///
/// # Returns
///
/// Returns the root LogicalPlan node
pub fn create_logical_plan(
    select_stmt: SelectStmt,
    sinks: Vec<PipelineSink>,
    stream_defs: &HashMap<String, Arc<StreamDefinition>>,
) -> Result<Arc<LogicalPlan>, String> {
    validate_group_by_requires_aggregates(&select_stmt)?;
    validate_aggregation_projection(&select_stmt)?;
    validate_expression_types(&select_stmt, stream_defs)?;

    let start_index = 0i64;
    let mut current_index = start_index;

    // 1. Create DataSource(s) from source_infos
    if select_stmt.source_infos.is_empty() {
        return Err("No data source found in SELECT statement".to_string());
    }

    let mut current_plans: Vec<Arc<LogicalPlan>> = Vec::new();
    for source_info in &select_stmt.source_infos {
        let definition = stream_defs.get(&source_info.name).ok_or_else(|| {
            format!(
                "stream {} missing catalog definition for logical planning",
                source_info.name
            )
        })?;
        let schema = definition.schema();
        let datasource = DataSource::new(
            source_info.name.clone(),
            source_info.alias.clone(),
            definition.decoder().clone(),
            current_index,
            schema,
            definition.eventtime().cloned(),
        );
        current_plans.push(Arc::new(LogicalPlan::DataSource(datasource)));
        current_index += 1;
    }

    // 2. Create StatefulFunctionPlan if stateful mappings exist
    if !select_stmt.stateful_mappings.is_empty() {
        let stateful = stateful_function::StatefulFunctionPlan::new(
            select_stmt.stateful_mappings.clone(),
            current_plans,
            current_index,
        );
        current_plans = vec![Arc::new(LogicalPlan::StatefulFunction(stateful))];
        current_index += 1;
    }

    // 3. Create Window from window if present
    if let Some(window) = select_stmt.window {
        let spec = convert_window_spec(window)?;
        let window_plan = LogicalWindow::new(spec, current_plans, current_index);
        current_plans = vec![Arc::new(LogicalPlan::Window(window_plan))];
        current_index += 1;
    }

    // 4. Create Aggregation if aggregate mappings exist
    if !select_stmt.aggregate_mappings.is_empty() {
        let aggregation = aggregation::Aggregation::new(
            select_stmt.aggregate_mappings.clone(),
            select_stmt.group_by_exprs.clone(),
            current_plans,
            current_index,
        );
        current_plans = vec![Arc::new(LogicalPlan::Aggregation(aggregation))];
        current_index += 1;
    }

    // 5. Create Filter from where_condition if present
    if let Some(where_expr) = select_stmt.where_condition {
        // Convert sqlparser Expr to ScalarExpr for the filter predicate
        // For now, we'll keep the original expression in the Filter node
        // In a full implementation, we'd convert this to a ScalarExpr
        let filter = Filter::new(where_expr, current_plans, current_index);
        current_plans = vec![Arc::new(LogicalPlan::Filter(filter))];
        current_index += 1;
    }

    // 6. Create Project from select_fields
    let mut project_fields = Vec::new();
    for select_field in select_stmt.select_fields.iter() {
        let field_name = select_field
            .alias
            .clone()
            .unwrap_or_else(|| select_field.field_name.clone());
        project_fields.push(project::ProjectField {
            field_name,
            expr: select_field.expr.clone(), // Keep original sqlparser expression
        });
    }

    let project = Project::new(project_fields, current_plans, current_index);
    let base = Arc::new(LogicalPlan::Project(project));

    if sinks.is_empty() {
        Ok(base)
    } else {
        // Always create TailPlan for both single and multiple sinks
        // This ensures consistent PhysicalResultCollect creation in physical plan
        let next_index = max_plan_index(&base) + 1;
        let mut sink_children = Vec::new();
        let sink_count = sinks.len();

        for (idx, sink) in sinks.into_iter().enumerate() {
            let sink_index = next_index + idx as i64;
            let sink_plan = DataSinkPlan::new(Arc::clone(&base), sink_index, sink);
            sink_children.push(Arc::new(LogicalPlan::DataSink(sink_plan)));
        }

        // Create TailPlan to hold sink children (1 or more)
        let tail_index = next_index + sink_count as i64;
        let tail_plan = TailPlan::new(sink_children, tail_index);
        Ok(Arc::new(LogicalPlan::Tail(tail_plan)))
    }
}

#[derive(Debug, Clone)]
struct SourceSchemaEntry {
    source_name: String,
    alias: Option<String>,
    schema: Arc<Schema>,
}

fn validate_expression_types(
    select_stmt: &SelectStmt,
    stream_defs: &HashMap<String, Arc<StreamDefinition>>,
) -> Result<(), String> {
    let mut sources = Vec::new();
    for source_info in &select_stmt.source_infos {
        let Some(definition) = stream_defs.get(&source_info.name) else {
            continue;
        };
        sources.push(SourceSchemaEntry {
            source_name: source_info.name.clone(),
            alias: source_info.alias.clone(),
            schema: definition.schema(),
        });
    }

    for field in &select_stmt.select_fields {
        validate_expr_against_sources(&field.expr, &sources)?;
    }
    if let Some(expr) = &select_stmt.where_condition {
        validate_expr_against_sources(expr, &sources)?;
    }
    if let Some(expr) = &select_stmt.having {
        validate_expr_against_sources(expr, &sources)?;
    }
    for expr in &select_stmt.group_by_exprs {
        validate_expr_against_sources(expr, &sources)?;
    }

    Ok(())
}

fn validate_expr_against_sources(
    expr: &sqlparser::ast::Expr,
    sources: &[SourceSchemaEntry],
) -> Result<(), String> {
    use sqlparser::ast::{Expr, JsonOperator};

    fn extract_const_int(expr: &Expr) -> Option<i64> {
        match expr {
            Expr::Value(sqlparser::ast::Value::Number(num, _)) => num.parse::<i64>().ok(),
            Expr::UnaryOp {
                op: sqlparser::ast::UnaryOperator::Minus,
                expr,
            } => extract_const_int(expr.as_ref()).map(|v| -v),
            Expr::Nested(inner) => extract_const_int(inner.as_ref()),
            _ => None,
        }
    }

    match expr {
        Expr::JsonAccess {
            left,
            operator: JsonOperator::Arrow,
            right,
        } => {
            let left_type = infer_expr_datatype(left.as_ref(), sources)?;
            let Some(ConcreteDatatype::Struct(struct_type)) = left_type else {
                return Ok(());
            };
            let field_name = match right.as_ref() {
                Expr::Identifier(ident) => ident.value.as_str(),
                _ => return Ok(()),
            };
            if !struct_type
                .fields()
                .iter()
                .any(|field| field.name() == field_name)
            {
                let available = struct_type
                    .fields()
                    .iter()
                    .map(|f| f.name().to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                return Err(format!(
                    "field `{}` not found in struct (available: {})",
                    field_name, available
                ));
            }

            validate_expr_against_sources(left.as_ref(), sources)?;
            Ok(())
        }
        Expr::MapAccess { column, keys } => {
            let column_type = infer_expr_datatype(column.as_ref(), sources)?;
            if let Some(dtype) = column_type {
                if !matches!(dtype, ConcreteDatatype::List(_)) {
                    return Err(format!("list index requires List type, got {:?}", dtype));
                }
            }
            validate_expr_against_sources(column.as_ref(), sources)?;
            for key in keys {
                if let Some(value) = extract_const_int(key) {
                    if value < 0 {
                        return Err(format!("list index must be non-negative, got {}", value));
                    }
                }
                validate_expr_against_sources(key, sources)?;
            }
            Ok(())
        }
        Expr::BinaryOp { left, right, .. } => {
            validate_expr_against_sources(left.as_ref(), sources)?;
            validate_expr_against_sources(right.as_ref(), sources)?;
            Ok(())
        }
        Expr::UnaryOp { expr, .. } => validate_expr_against_sources(expr.as_ref(), sources),
        Expr::Nested(expr) => validate_expr_against_sources(expr.as_ref(), sources),
        Expr::Cast { expr, .. } => validate_expr_against_sources(expr.as_ref(), sources),
        Expr::Function(func) => {
            for arg in &func.args {
                match arg {
                    sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(expr),
                    ) => validate_expr_against_sources(expr, sources)?,
                    sqlparser::ast::FunctionArg::Named {
                        arg: sqlparser::ast::FunctionArgExpr::Expr(expr),
                        ..
                    } => validate_expr_against_sources(expr, sources)?,
                    _ => {}
                }
            }
            Ok(())
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(expr) = operand.as_ref() {
                validate_expr_against_sources(expr, sources)?;
            }
            for expr in conditions {
                validate_expr_against_sources(expr, sources)?;
            }
            for expr in results {
                validate_expr_against_sources(expr, sources)?;
            }
            if let Some(expr) = else_result.as_ref() {
                validate_expr_against_sources(expr, sources)?;
            }
            Ok(())
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            validate_expr_against_sources(expr.as_ref(), sources)?;
            validate_expr_against_sources(low.as_ref(), sources)?;
            validate_expr_against_sources(high.as_ref(), sources)?;
            Ok(())
        }
        Expr::InList { expr, list, .. } => {
            validate_expr_against_sources(expr.as_ref(), sources)?;
            for item in list {
                validate_expr_against_sources(item, sources)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn infer_expr_datatype(
    expr: &sqlparser::ast::Expr,
    sources: &[SourceSchemaEntry],
) -> Result<Option<ConcreteDatatype>, String> {
    use sqlparser::ast::Expr;

    match expr {
        Expr::Identifier(ident) => Ok(resolve_column_datatype(None, &ident.value, sources)),
        Expr::CompoundIdentifier(idents) => {
            if idents.len() != 2 {
                return Ok(None);
            }
            Ok(resolve_column_datatype(
                Some(&idents[0].value),
                &idents[1].value,
                sources,
            ))
        }
        Expr::Nested(expr) => infer_expr_datatype(expr.as_ref(), sources),
        Expr::Cast { expr, .. } => infer_expr_datatype(expr.as_ref(), sources),
        Expr::MapAccess { column, .. } => {
            let Some(dtype) = infer_expr_datatype(column.as_ref(), sources)? else {
                return Ok(None);
            };
            let ConcreteDatatype::List(list_type) = dtype else {
                return Ok(None);
            };
            Ok(Some(list_type.item_type().clone()))
        }
        Expr::JsonAccess { left, right, .. } => {
            let Some(dtype) = infer_expr_datatype(left.as_ref(), sources)? else {
                return Ok(None);
            };
            let ConcreteDatatype::Struct(struct_type) = dtype else {
                return Ok(None);
            };
            let Expr::Identifier(ident) = right.as_ref() else {
                return Ok(None);
            };
            Ok(struct_type
                .fields()
                .iter()
                .find(|f| f.name() == ident.value)
                .map(|f| f.data_type().clone()))
        }
        _ => Ok(None),
    }
}

fn resolve_column_datatype(
    qualifier: Option<&str>,
    column_name: &str,
    sources: &[SourceSchemaEntry],
) -> Option<ConcreteDatatype> {
    if sources.is_empty() {
        return None;
    }

    let matches: Vec<_> = sources
        .iter()
        .filter(|src| match qualifier {
            Some(q) => src.source_name == q || src.alias.as_deref() == Some(q),
            None => true,
        })
        .filter_map(|src| {
            src.schema
                .column_schemas()
                .iter()
                .find(|c| c.name == column_name)
                .map(|c| c.data_type.clone())
        })
        .collect();

    match matches.len() {
        1 => Some(matches[0].clone()),
        _ => None,
    }
}

fn validate_group_by_requires_aggregates(select_stmt: &SelectStmt) -> Result<(), String> {
    if select_stmt.aggregate_mappings.is_empty() && !select_stmt.group_by_exprs.is_empty() {
        return Err(
            "GROUP BY without aggregate functions is not supported; use aggregates or remove GROUP BY"
                .to_string(),
        );
    }
    Ok(())
}

fn validate_aggregation_projection(select_stmt: &SelectStmt) -> Result<(), String> {
    if select_stmt.aggregate_mappings.is_empty() {
        return Ok(());
    }

    let group_by_exprs: std::collections::HashSet<String> = select_stmt
        .group_by_exprs
        .iter()
        .map(|expr| expr.to_string())
        .collect();

    for field in &select_stmt.select_fields {
        if group_by_exprs.contains(&field.expr.to_string()) {
            continue;
        }

        if !expr_contains_aggregate_placeholder(&field.expr) {
            return Err(format!(
                "SELECT expression '{}' must be an aggregate or appear in GROUP BY",
                field.expr
            ));
        }

        let referenced_columns = collect_non_placeholder_column_refs(&field.expr);
        for column in referenced_columns {
            if !group_by_exprs.contains(&column) {
                return Err(format!(
                    "SELECT expression '{}' references column '{}' which must appear in GROUP BY",
                    field.expr, column
                ));
            }
        }
    }

    if let Some(having) = &select_stmt.having {
        if group_by_exprs.contains(&having.to_string()) {
            return Ok(());
        }

        if !expr_contains_aggregate_placeholder(having) {
            return Err(format!(
                "HAVING expression '{}' must be an aggregate or appear in GROUP BY",
                having
            ));
        }

        let referenced_columns = collect_non_placeholder_column_refs(having);
        for column in referenced_columns {
            if !group_by_exprs.contains(&column) {
                return Err(format!(
                    "HAVING expression '{}' references column '{}' which must appear in GROUP BY",
                    having, column
                ));
            }
        }
    }

    Ok(())
}

fn collect_non_placeholder_column_refs(expr: &sqlparser::ast::Expr) -> Vec<String> {
    use sqlparser::ast::Expr;

    fn is_placeholder(ident: &sqlparser::ast::Ident) -> bool {
        let value = ident.value.as_str();
        let Some(rest) = value.strip_prefix("col_") else {
            return false;
        };
        !rest.is_empty() && rest.chars().all(|c| c.is_ascii_digit())
    }

    fn collect(expr: &Expr, out: &mut std::collections::HashSet<String>) {
        match expr {
            Expr::Identifier(ident) => {
                if !is_placeholder(ident) {
                    out.insert(ident.value.clone());
                }
            }
            Expr::CompoundIdentifier(idents) => {
                out.insert(Expr::CompoundIdentifier(idents.clone()).to_string());
            }
            Expr::BinaryOp { left, right, .. } => {
                collect(left, out);
                collect(right, out);
            }
            Expr::UnaryOp { expr, .. } => collect(expr, out),
            Expr::Nested(expr) => collect(expr, out),
            Expr::Cast { expr, .. } => collect(expr, out),
            Expr::Between {
                expr, low, high, ..
            } => {
                collect(expr, out);
                collect(low, out);
                collect(high, out);
            }
            Expr::InList { expr, list, .. } => {
                collect(expr, out);
                for item in list {
                    collect(item, out);
                }
            }
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                if let Some(expr) = operand.as_ref() {
                    collect(expr, out);
                }
                for expr in conditions {
                    collect(expr, out);
                }
                for expr in results {
                    collect(expr, out);
                }
                if let Some(expr) = else_result.as_ref() {
                    collect(expr, out);
                }
            }
            Expr::Function(func) => {
                for arg in &func.args {
                    match arg {
                        sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(expr),
                        ) => collect(expr, out),
                        sqlparser::ast::FunctionArg::Named {
                            arg: sqlparser::ast::FunctionArgExpr::Expr(expr),
                            ..
                        } => collect(expr, out),
                        _ => {}
                    }
                }
            }
            Expr::JsonAccess { left, right, .. } => {
                collect(left, out);
                collect(right, out);
            }
            Expr::MapAccess { column, keys } => {
                collect(column, out);
                for expr in keys {
                    collect(expr, out);
                }
            }
            _ => {}
        }
    }

    let mut out = std::collections::HashSet::new();
    collect(expr, &mut out);
    let mut out: Vec<_> = out.into_iter().collect();
    out.sort();
    out
}

fn expr_contains_aggregate_placeholder(expr: &sqlparser::ast::Expr) -> bool {
    use sqlparser::ast::Expr;

    fn is_placeholder(ident: &sqlparser::ast::Ident) -> bool {
        let value = ident.value.as_str();
        let Some(rest) = value.strip_prefix("col_") else {
            return false;
        };
        !rest.is_empty() && rest.chars().all(|c| c.is_ascii_digit())
    }

    match expr {
        Expr::Identifier(ident) => is_placeholder(ident),
        Expr::CompoundIdentifier(_) => false,
        Expr::BinaryOp { left, right, .. } => {
            expr_contains_aggregate_placeholder(left) || expr_contains_aggregate_placeholder(right)
        }
        Expr::UnaryOp { expr, .. } => expr_contains_aggregate_placeholder(expr),
        Expr::Nested(expr) => expr_contains_aggregate_placeholder(expr),
        Expr::Cast { expr, .. } => expr_contains_aggregate_placeholder(expr),
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_contains_aggregate_placeholder(expr)
                || expr_contains_aggregate_placeholder(low)
                || expr_contains_aggregate_placeholder(high)
        }
        Expr::InList { expr, list, .. } => {
            expr_contains_aggregate_placeholder(expr)
                || list.iter().any(expr_contains_aggregate_placeholder)
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            operand
                .as_ref()
                .is_some_and(|expr| expr_contains_aggregate_placeholder(expr))
                || conditions.iter().any(expr_contains_aggregate_placeholder)
                || results.iter().any(expr_contains_aggregate_placeholder)
                || else_result
                    .as_ref()
                    .is_some_and(|expr| expr_contains_aggregate_placeholder(expr))
        }
        Expr::Function(func) => func.args.iter().any(|arg| match arg {
            sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(expr)) => {
                expr_contains_aggregate_placeholder(expr)
            }
            sqlparser::ast::FunctionArg::Named { arg, .. } => {
                if let sqlparser::ast::FunctionArgExpr::Expr(expr) = arg {
                    return expr_contains_aggregate_placeholder(expr);
                }
                false
            }
            _ => false,
        }),
        Expr::JsonAccess { left, right, .. } => {
            expr_contains_aggregate_placeholder(left) || expr_contains_aggregate_placeholder(right)
        }
        Expr::MapAccess { column, keys } => {
            expr_contains_aggregate_placeholder(column)
                || keys.iter().any(expr_contains_aggregate_placeholder)
        }
        _ => false,
    }
}

fn convert_window_spec(window: parser_window::Window) -> Result<LogicalWindowSpec, String> {
    match window {
        parser_window::Window::Tumbling { time_unit, length } => {
            let unit = match time_unit {
                parser_window::TimeUnit::Seconds => TimeUnit::Seconds,
            };
            Ok(LogicalWindowSpec::Tumbling {
                time_unit: unit,
                length,
            })
        }
        parser_window::Window::Count { count } => Ok(LogicalWindowSpec::Count { count }),
        parser_window::Window::Sliding {
            time_unit,
            lookback,
            lookahead,
        } => {
            let unit = match time_unit {
                parser_window::TimeUnit::Seconds => TimeUnit::Seconds,
            };
            Ok(LogicalWindowSpec::Sliding {
                time_unit: unit,
                lookback,
                lookahead,
            })
        }
        parser_window::Window::State {
            open,
            emit,
            partition_by,
        } => Ok(LogicalWindowSpec::State {
            open,
            emit,
            partition_by,
        }),
    }
}

/// Helper function to print logical plan structure for debugging
pub fn print_logical_plan(plan: &Arc<LogicalPlan>, indent: usize) {
    plan.print_topology(indent);
}

fn max_plan_index(plan: &Arc<LogicalPlan>) -> i64 {
    let mut max_index = plan.get_plan_index();
    for child in plan.children() {
        let child_max = max_plan_index(child);
        if child_max > max_index {
            max_index = child_max;
        }
    }
    max_index
}
