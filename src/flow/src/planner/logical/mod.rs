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

#[cfg(test)]
mod logical_plan_tests {
    use super::*;
    use crate::catalog::{MqttStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps};
    use datatypes::{
        ColumnSchema, ConcreteDatatype, Int64Type, ListType, Schema, StringType, StructField,
        StructType,
    };
    use parser::parse_sql;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn stream_def(name: &str) -> Arc<StreamDefinition> {
        Arc::new(StreamDefinition::new(
            name.to_string(),
            Arc::new(Schema::new(Vec::new())),
            StreamProps::Mqtt(MqttStreamProps::new("mqtt://localhost:1883", name, 0)),
            StreamDecoderConfig::json(),
        ))
    }

    fn make_stream_defs(names: &[&str]) -> HashMap<String, Arc<StreamDefinition>> {
        let mut map = HashMap::new();
        for name in names {
            map.insert((*name).to_string(), stream_def(name));
        }
        map
    }

    #[test]
    fn test_create_logical_plan_simple() {
        let sql = "SELECT a, b FROM users";
        let select_stmt = parse_sql(sql).unwrap();
        let stream_defs = make_stream_defs(&["users"]);

        let plan = create_logical_plan(select_stmt, Vec::new(), &stream_defs).unwrap();

        // Should be a Project node
        assert_eq!(plan.get_plan_type(), "Project");
        assert_eq!(plan.get_plan_index(), 1); // DataSource(0) -> Project(1)

        // Check that it has one child (DataSource)
        let children = plan.children();
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].get_plan_type(), "DataSource");
        assert_eq!(children[0].get_plan_index(), 0);
    }

    #[test]
    fn test_create_logical_plan_with_filter() {
        let sql = "SELECT a, b FROM users WHERE a > 10";
        let select_stmt = parse_sql(sql).unwrap();
        let stream_defs = make_stream_defs(&["users"]);

        let plan = create_logical_plan(select_stmt, Vec::new(), &stream_defs).unwrap();

        // Should be a Project node
        assert_eq!(plan.get_plan_type(), "Project");
        assert_eq!(plan.get_plan_index(), 2); // DataSource(0) -> Filter(1) -> Project(2)

        // Check the filter in the middle
        let children = plan.children();
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].get_plan_type(), "Filter");
        assert_eq!(children[0].get_plan_index(), 1);

        // Check the datasource at the bottom
        let filter_children = children[0].children();
        assert_eq!(filter_children.len(), 1);
        assert_eq!(filter_children[0].get_plan_type(), "DataSource");
        assert_eq!(filter_children[0].get_plan_index(), 0);
    }

    #[test]
    fn test_create_logical_plan_with_sliding_window() {
        let sql = "SELECT * FROM users GROUP BY slidingwindow('ss', 10)";
        let select_stmt = parse_sql(sql).unwrap();
        let stream_defs = make_stream_defs(&["users"]);

        let plan = create_logical_plan(select_stmt, Vec::new(), &stream_defs).unwrap();

        assert_eq!(plan.get_plan_type(), "Project");
        let project_children = plan.children();
        assert_eq!(project_children.len(), 1);

        assert_eq!(project_children[0].get_plan_type(), "Window");
        let window = match project_children[0].as_ref() {
            LogicalPlan::Window(window) => window,
            other => panic!("Expected Window, found {}", other.get_plan_type()),
        };
        assert!(matches!(
            window.spec,
            LogicalWindowSpec::Sliding {
                time_unit: TimeUnit::Seconds,
                lookback: 10,
                lookahead: None
            }
        ));

        let window_children = project_children[0].children();
        assert_eq!(window_children.len(), 1);
        assert_eq!(window_children[0].get_plan_type(), "DataSource");
    }

    #[test]
    fn test_create_logical_plan_with_state_window() {
        let sql = "SELECT * FROM users GROUP BY statewindow(a > 0, b = 1)";
        let select_stmt = parse_sql(sql).unwrap();
        let stream_defs = make_stream_defs(&["users"]);

        let plan = create_logical_plan(select_stmt, Vec::new(), &stream_defs).unwrap();

        assert_eq!(plan.get_plan_type(), "Project");
        let project_children = plan.children();
        assert_eq!(project_children.len(), 1);

        assert_eq!(project_children[0].get_plan_type(), "Window");
        let window = match project_children[0].as_ref() {
            LogicalPlan::Window(window) => window,
            other => panic!("Expected Window, found {}", other.get_plan_type()),
        };

        match &window.spec {
            LogicalWindowSpec::State {
                open,
                emit,
                partition_by,
            } => {
                assert_eq!(open.as_ref().to_string(), "a > 0");
                assert_eq!(emit.as_ref().to_string(), "b = 1");
                assert!(partition_by.is_empty());
            }
            other => panic!("Expected State window, got {:?}", other),
        }
    }

    #[test]
    fn test_create_logical_plan_with_state_window_partition_by() {
        let sql =
            "SELECT * FROM users GROUP BY statewindow(a > 0, b = 1) OVER (PARTITION BY k1, k2)";
        let select_stmt = parse_sql(sql).unwrap();
        let stream_defs = make_stream_defs(&["users"]);

        let plan = create_logical_plan(select_stmt, Vec::new(), &stream_defs).unwrap();

        assert_eq!(plan.get_plan_type(), "Project");
        let project_children = plan.children();
        assert_eq!(project_children.len(), 1);

        assert_eq!(project_children[0].get_plan_type(), "Window");
        let window = match project_children[0].as_ref() {
            LogicalPlan::Window(window) => window,
            other => panic!("Expected Window, found {}", other.get_plan_type()),
        };

        match &window.spec {
            LogicalWindowSpec::State {
                open,
                emit,
                partition_by,
            } => {
                assert_eq!(open.as_ref().to_string(), "a > 0");
                assert_eq!(emit.as_ref().to_string(), "b = 1");
                assert_eq!(partition_by.len(), 2);
                assert_eq!(partition_by[0].to_string(), "k1");
                assert_eq!(partition_by[1].to_string(), "k2");
            }
            other => panic!("Expected State window, got {:?}", other),
        }
    }

    #[test]
    fn test_reject_group_by_without_aggregates() {
        let sql = "SELECT * FROM users GROUP BY b, c";
        let select_stmt = parse_sql(sql).unwrap();
        let stream_defs = make_stream_defs(&["users"]);

        let err = create_logical_plan(select_stmt, Vec::new(), &stream_defs).unwrap_err();
        assert!(
            err.contains("GROUP BY without aggregate functions is not supported"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn test_create_logical_plan_with_alias() {
        let sql = "SELECT a, b FROM users AS u WHERE a > 10";
        let select_stmt = parse_sql(sql).unwrap();
        let stream_defs = make_stream_defs(&["users"]);

        let plan = create_logical_plan(select_stmt, Vec::new(), &stream_defs).unwrap();

        // Verify the structure is correct
        assert_eq!(plan.get_plan_type(), "Project");

        let children = plan.children();
        assert_eq!(children[0].get_plan_type(), "Filter");

        let filter_children = children[0].children();
        assert_eq!(filter_children[0].get_plan_type(), "DataSource");
    }

    #[test]
    fn test_create_logical_plan_with_func_field() {
        let sql = "SELECT a, concat(b), c AS custom_name FROM users";
        let select_stmt = parse_sql(sql).unwrap();
        let stream_defs = make_stream_defs(&["users"]);

        let plan = create_logical_plan(select_stmt, Vec::new(), &stream_defs).unwrap();

        // Verify the structure is correct
        assert_eq!(plan.get_plan_type(), "Project");

        // Extract project fields
        let project = match plan.as_ref() {
            LogicalPlan::Project(project) => project,
            other => panic!("Expected Project, found {}", other.get_plan_type()),
        };

        // Verify we have 3 fields
        assert_eq!(project.fields.len(), 3);

        // Verify field names
        assert_eq!(project.fields[0].field_name, "a"); // No alias, uses expr string
        assert_eq!(project.fields[1].field_name, "concat(b)"); // No alias, uses expr string
        assert_eq!(project.fields[2].field_name, "custom_name"); // Has alias

        // Verify the plan structure
        let children = plan.children();
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].get_plan_type(), "DataSource");
    }

    #[test]
    fn test_create_logical_plan_with_single_sink() {
        let sql = "SELECT a, b FROM users";
        let select_stmt = parse_sql(sql).unwrap();

        let sink = PipelineSink::new(
            "test_sink",
            crate::planner::sink::PipelineSinkConnector::new(
                "test_conn",
                crate::planner::sink::SinkConnectorConfig::Nop(Default::default()),
                crate::planner::sink::SinkEncoderConfig::json(),
            ),
        );

        let stream_defs = make_stream_defs(&["users"]);
        let plan = create_logical_plan(select_stmt, vec![sink], &stream_defs).unwrap();

        // Debug output
        println!("=== Single Sink Logical Plan ===");
        crate::planner::logical::print_logical_plan(&plan, 0);
        println!("================================");

        // Should be a Tail node (single sink now creates TailPlan for consistency)
        assert_eq!(plan.get_plan_type(), "Tail");

        // Check that it has one child (DataSink)
        let children = plan.children();
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].get_plan_type(), "DataSink");

        // Check that DataSink has Project as child
        let sink_children = children[0].children();
        assert_eq!(sink_children.len(), 1);
        assert_eq!(sink_children[0].get_plan_type(), "Project");
    }

    #[test]
    fn test_create_logical_plan_with_multiple_sinks() {
        let sql = "SELECT a, b FROM users";
        let select_stmt = parse_sql(sql).unwrap();

        let sink1 = PipelineSink::new(
            "sink1",
            crate::planner::sink::PipelineSinkConnector::new(
                "conn1",
                crate::planner::sink::SinkConnectorConfig::Nop(Default::default()),
                crate::planner::sink::SinkEncoderConfig::json(),
            ),
        );

        let sink2 = PipelineSink::new(
            "sink2",
            crate::planner::sink::PipelineSinkConnector::new(
                "conn2",
                crate::planner::sink::SinkConnectorConfig::Nop(Default::default()),
                crate::planner::sink::SinkEncoderConfig::json(),
            ),
        );

        let stream_defs = make_stream_defs(&["users"]);
        let plan = create_logical_plan(select_stmt, vec![sink1, sink2], &stream_defs).unwrap();

        // Should be a Tail node (multiple sinks create TailPlan)
        assert_eq!(plan.get_plan_type(), "Tail");

        // Check that it has two children (DataSinks)
        let children = plan.children();
        assert_eq!(children.len(), 2);
        assert_eq!(children[0].get_plan_type(), "DataSink");
        assert_eq!(children[1].get_plan_type(), "DataSink");

        // Verify each DataSink has Project as child
        for child in children {
            let sink_children = child.children();
            assert_eq!(sink_children.len(), 1);
            assert_eq!(sink_children[0].get_plan_type(), "Project");
        }

        // Verify that all DataSinks share the same Project instance
        if let LogicalPlan::Tail(tail_plan) = plan.as_ref() {
            let data_sink_children: Vec<_> = tail_plan
                .base
                .children()
                .iter()
                .filter(|child| matches!(child.as_ref(), LogicalPlan::DataSink(_)))
                .collect();

            assert_eq!(data_sink_children.len(), 2);

            // Get the Project node from each DataSink
            let mut project_indices = Vec::new();
            for data_sink_node in &data_sink_children {
                if let LogicalPlan::DataSink(data_sink) = data_sink_node.as_ref() {
                    let project_child = &data_sink.base.children()[0];
                    project_indices.push(project_child.get_plan_index());
                }
            }

            // All DataSinks should point to the same Project (same index)
            assert_eq!(project_indices.len(), 2);
            assert_eq!(
                project_indices[0], project_indices[1],
                "DataSinks should share the same Project node"
            );

            println!(
                "✅ Multiple sinks correctly share the same Project node (index: {})",
                project_indices[0]
            );
        }
    }

    #[test]
    fn test_rejects_unknown_list_struct_field_during_logical_planning() {
        let items_struct = ConcreteDatatype::Struct(StructType::new(Arc::new(vec![
            StructField::new("c".to_string(), ConcreteDatatype::Int64(Int64Type), false),
            StructField::new("d".to_string(), ConcreteDatatype::String(StringType), false),
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
                ConcreteDatatype::List(ListType::new(Arc::new(items_struct))),
            ),
        ]));

        let definition = Arc::new(StreamDefinition::new(
            "stream_3".to_string(),
            schema,
            StreamProps::Mqtt(MqttStreamProps::new("mqtt://localhost:1883", "stream_3", 0)),
            StreamDecoderConfig::json(),
        ));

        let mut stream_defs = HashMap::new();
        stream_defs.insert("stream_3".to_string(), definition);

        let select_stmt = parse_sql("SELECT a, items[0]->b FROM stream_3").unwrap();
        let err = create_logical_plan(select_stmt, Vec::new(), &stream_defs).unwrap_err();
        assert!(
            err.contains("field `b` not found"),
            "unexpected error: {err}"
        );
    }
}
