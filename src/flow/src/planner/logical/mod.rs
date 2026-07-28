use crate::catalog::{StreamDefinition, TableDefinition, TableProps};
use crate::pipeline::SourceInputConfig;
use parser::window as parser_window;
use parser::SelectStmt;
use sqlparser::ast::Expr;
use std::collections::HashMap;
use std::sync::Arc;

use datatypes::ConcreteDatatype;

pub mod aggregation;
pub mod compute;
pub mod datasource;
pub mod filter;
pub mod order;
pub mod project;
pub mod sink;
pub mod stateful_function;
pub mod table_scan;
pub mod tail;
pub mod window;

use crate::planner::sink::PipelineSink;
pub use aggregation::Aggregation;
pub use compute::Compute;
pub use datasource::DataSource;
pub use filter::Filter;
pub use order::{Order, OrderItem};
pub use project::Project;
pub use sink::DataSinkPlan;
pub use stateful_function::{LogicalStatefulCall, StatefulFunctionPlan};
pub use table_scan::{TableScan, TableScanRequest, TableScanSpec};
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
    TableScan(TableScan),
    StatefulFunction(StatefulFunctionPlan),
    Filter(Filter),
    Aggregation(Aggregation),
    Compute(Compute),
    Order(Order),
    Project(Project),
    DataSink(DataSinkPlan),
    Tail(TailPlan),
    Window(LogicalWindow),
}

impl LogicalPlan {
    pub fn children(&self) -> &[Arc<LogicalPlan>] {
        match self {
            LogicalPlan::DataSource(plan) => plan.base.children(),
            LogicalPlan::TableScan(plan) => plan.base.children(),
            LogicalPlan::StatefulFunction(plan) => plan.base.children(),
            LogicalPlan::Filter(plan) => plan.base.children(),
            LogicalPlan::Aggregation(plan) => plan.base.children(),
            LogicalPlan::Compute(plan) => plan.base.children(),
            LogicalPlan::Order(plan) => plan.base.children(),
            LogicalPlan::Project(plan) => plan.base.children(),
            LogicalPlan::DataSink(plan) => plan.base.children(),
            LogicalPlan::Tail(plan) => plan.base.children(),
            LogicalPlan::Window(plan) => plan.base.children(),
        }
    }

    pub fn get_plan_type(&self) -> &str {
        match self {
            LogicalPlan::DataSource(_) => "DataSource",
            LogicalPlan::TableScan(_) => "TableScan",
            LogicalPlan::StatefulFunction(_) => "StatefulFunction",
            LogicalPlan::Filter(_) => "Filter",
            LogicalPlan::Aggregation(_) => "Aggregation",
            LogicalPlan::Compute(_) => "Compute",
            LogicalPlan::Order(_) => "Order",
            LogicalPlan::Project(_) => "Project",
            LogicalPlan::DataSink(_) => "DataSink",
            LogicalPlan::Tail(_) => "Tail",
            LogicalPlan::Window(_) => "Window",
        }
    }

    pub fn get_plan_index(&self) -> i64 {
        match self {
            LogicalPlan::DataSource(plan) => plan.base.index(),
            LogicalPlan::TableScan(plan) => plan.base.index(),
            LogicalPlan::StatefulFunction(plan) => plan.base.index(),
            LogicalPlan::Filter(plan) => plan.base.index(),
            LogicalPlan::Aggregation(plan) => plan.base.index(),
            LogicalPlan::Compute(plan) => plan.base.index(),
            LogicalPlan::Order(plan) => plan.base.index(),
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
/// - Filter (from SelectStmt::having, if present) - takes Aggregation as children
/// - Filter (from SelectStmt::where_condition, if present) - takes HAVING/Aggregation/Window/DataSources as children
/// - Order (from SelectStmt::order_by, if present) - takes Filter/HAVING/Aggregation/Window/DataSources as children
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
    create_logical_plan_with_table_defs_and_source_inputs(
        select_stmt,
        sinks,
        stream_defs,
        &HashMap::new(),
        &HashMap::new(),
    )
}

pub fn create_logical_plan_with_source_inputs(
    select_stmt: SelectStmt,
    sinks: Vec<PipelineSink>,
    stream_defs: &HashMap<String, Arc<StreamDefinition>>,
    source_inputs: &HashMap<String, SourceInputConfig>,
) -> Result<Arc<LogicalPlan>, String> {
    create_logical_plan_with_table_defs_and_source_inputs(
        select_stmt,
        sinks,
        stream_defs,
        &HashMap::new(),
        source_inputs,
    )
}

pub fn create_logical_plan_with_table_defs_and_source_inputs(
    select_stmt: SelectStmt,
    sinks: Vec<PipelineSink>,
    stream_defs: &HashMap<String, Arc<StreamDefinition>>,
    table_defs: &HashMap<String, Arc<TableDefinition>>,
    source_inputs: &HashMap<String, SourceInputConfig>,
) -> Result<Arc<LogicalPlan>, String> {
    let mut select_stmt = select_stmt;

    validate_group_by_requires_aggregates(&select_stmt)?;
    validate_having_constraints(&select_stmt)?;
    validate_relation_aliases_unsupported(&select_stmt)?;
    validate_select_alias_names(&select_stmt, stream_defs, table_defs)?;
    resolve_select_aliases(&mut select_stmt)?;
    validate_aggregation_projection(&select_stmt)?;
    validate_order_by_constraints(&select_stmt)?;
    validate_expression_types(&select_stmt, stream_defs, table_defs)?;
    validate_window_filter_constraints(&select_stmt)?;

    let start_index = 0i64;
    let mut current_index = start_index;

    // 1. Create DataSource(s) from source_infos
    if select_stmt.source_infos.is_empty() {
        return Err("No data source found in SELECT statement".to_string());
    }

    let mut current_plans: Vec<Arc<LogicalPlan>> = Vec::new();
    let table_sources = select_stmt
        .source_infos
        .iter()
        .filter(|source| table_defs.contains_key(&source.name))
        .count();
    if table_sources > 0 {
        if select_stmt.source_infos.len() != 1 {
            return Err("table scan currently supports exactly one relation".to_string());
        }
        if select_stmt.window.is_some() {
            return Err("windowed table scans are not supported yet".to_string());
        }
        if !select_stmt.aggregate_mappings.is_empty() {
            return Err("aggregate functions over table scans are not supported yet".to_string());
        }
        let source_info = &select_stmt.source_infos[0];
        let definition = table_defs.get(&source_info.name).ok_or_else(|| {
            format!(
                "table {} missing catalog definition for logical planning",
                source_info.name
            )
        })?;
        let request = TableScanRequest {
            batch_size: match definition.props() {
                TableProps::History(props) => props.batch_size,
            },
        };
        let table_scan = TableScan::new(
            TableScanSpec {
                table_name: source_info.name.clone(),
                table_type: definition.table_type(),
                alias: None,
                decoder: definition.decoder().clone(),
                schema: definition.schema(),
                props: definition.props().clone(),
                request,
            },
            current_index,
        );
        current_plans.push(Arc::new(LogicalPlan::TableScan(table_scan)));
        current_index += 1;
    } else {
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
                None,
                definition.decoder().clone(),
                current_index,
                schema,
                definition.eventtime().cloned(),
                definition.sampler().cloned(),
            )
            .with_stream_type(definition.stream_type());
            let mut datasource = datasource;
            datasource.source_input = source_inputs
                .get(&source_info.name)
                .cloned()
                .unwrap_or_default();
            current_plans.push(Arc::new(LogicalPlan::DataSource(datasource)));
            current_index += 1;
        }
    }

    // 2. Create StatefulFunctionPlan if stateful mappings exist.
    if !select_stmt.stateful_mappings.is_empty() {
        let calls = select_stmt
            .stateful_mappings
            .iter()
            .map(|entry| stateful_function::LogicalStatefulCall {
                output_column: entry.output_column.clone(),
                spec: entry.spec.clone(),
            })
            .collect::<Vec<_>>();
        let stateful =
            stateful_function::StatefulFunctionPlan::new(calls, current_plans, current_index);
        current_plans = vec![Arc::new(LogicalPlan::StatefulFunction(stateful))];
        current_index += 1;
    }

    // 3. Create Window from window if present
    if let Some(window) = select_stmt.window.clone() {
        let (window, window_filter) = split_window_filter(window);
        if let Some(filter_expr) = window_filter {
            let filter = Filter::new(filter_expr, current_plans, current_index);
            current_plans = vec![Arc::new(LogicalPlan::Filter(filter))];
            current_index += 1;
        }
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

    // 5. Create Filter from HAVING if present (post-aggregation)
    if let Some(having_expr) = select_stmt.having.clone() {
        let filter = Filter::new(having_expr, current_plans, current_index);
        current_plans = vec![Arc::new(LogicalPlan::Filter(filter))];
        current_index += 1;
    }

    // 6. Create Filter from where_condition if present
    if let Some(where_expr) = select_stmt.where_condition.clone() {
        // Convert sqlparser Expr to ScalarExpr for the filter predicate
        // For now, we'll keep the original expression in the Filter node
        // In a full implementation, we'd convert this to a ScalarExpr
        let filter = Filter::new(where_expr, current_plans, current_index);
        current_plans = vec![Arc::new(LogicalPlan::Filter(filter))];
        current_index += 1;
    }

    // 7. Create Order from order_by (if present)
    if !select_stmt.order_by.is_empty() {
        let items = select_stmt
            .order_by
            .iter()
            .map(|item| OrderItem {
                expr: item.expr.clone(),
                asc: item.asc,
            })
            .collect();
        let order = Order::new(items, current_plans, current_index);
        current_plans = vec![Arc::new(LogicalPlan::Order(order))];
        current_index += 1;
    }

    // 8. Create Project from select_fields
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

    let root = if sinks.is_empty() {
        base
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
        Arc::new(LogicalPlan::Tail(tail_plan))
    };

    verify_logical_plan(root.as_ref())?;
    Ok(root)
}

pub fn verify_logical_plan(plan: &LogicalPlan) -> Result<(), String> {
    fn verify_node(plan: &LogicalPlan) -> Result<(), String> {
        match plan {
            LogicalPlan::Project(project) => {
                let mut seen =
                    std::collections::HashSet::<&str>::with_capacity(project.fields.len());
                for field in &project.fields {
                    if !seen.insert(field.field_name.as_str()) {
                        return Err(format!(
                            "duplicate project field name `{}` in {}",
                            field.field_name,
                            plan.get_plan_name()
                        ));
                    }
                }
            }
            LogicalPlan::Compute(compute) => {
                let mut seen =
                    std::collections::HashSet::<&str>::with_capacity(compute.fields.len());
                for field in &compute.fields {
                    if !seen.insert(field.field_name.as_str()) {
                        return Err(format!(
                            "duplicate compute field name `{}` in {}",
                            field.field_name,
                            plan.get_plan_name()
                        ));
                    }
                }
            }
            _ => {}
        }

        for child in plan.children() {
            verify_node(child.as_ref())?;
        }
        Ok(())
    }

    verify_node(plan)
}

#[derive(Debug, Clone)]
struct SourceSchemaEntry {
    source_name: String,
    column_lookup: HashMap<String, ConcreteDatatype>,
}

fn validate_expression_types(
    select_stmt: &SelectStmt,
    stream_defs: &HashMap<String, Arc<StreamDefinition>>,
    table_defs: &HashMap<String, Arc<TableDefinition>>,
) -> Result<(), String> {
    let mut sources = Vec::new();
    for source_info in &select_stmt.source_infos {
        let Some(schema) = relation_schema(&source_info.name, stream_defs, table_defs) else {
            continue;
        };
        let mut column_lookup = HashMap::with_capacity(schema.column_schemas().len());
        for col in schema.column_schemas() {
            column_lookup.insert(col.name.clone(), col.data_type.clone());
        }

        sources.push(SourceSchemaEntry {
            source_name: source_info.name.clone(),
            column_lookup,
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
    if let Some(window) = select_stmt.window.as_ref() {
        if let Some(filter) = window.filter() {
            validate_expr_against_sources(filter, &sources)?;
        }
    }
    for item in &select_stmt.order_by {
        validate_expr_against_sources(&item.expr, &sources)?;
    }

    Ok(())
}

fn validate_select_alias_names(
    select_stmt: &SelectStmt,
    stream_defs: &HashMap<String, Arc<StreamDefinition>>,
    table_defs: &HashMap<String, Arc<TableDefinition>>,
) -> Result<(), String> {
    use crate::expr::internal_columns::is_reserved_user_name;
    use std::collections::HashSet;

    // Collect all source schemas to keep them alive.
    let mut schemas = Vec::with_capacity(select_stmt.source_infos.len());
    for source_info in &select_stmt.source_infos {
        if let Some(schema) = relation_schema(&source_info.name, stream_defs, table_defs) {
            schemas.push(schema);
        }
    }

    // Collect all source column names; alias must not collide with any of them.
    let mut source_column_names = HashSet::<&str>::new();
    for schema in &schemas {
        for col in schema.column_schemas() {
            source_column_names.insert(col.name.as_str());
        }
    }

    let mut seen_aliases = HashSet::<String>::new();
    for field in &select_stmt.select_fields {
        let Some(alias) = field.alias.as_ref() else {
            continue;
        };

        if is_reserved_user_name(alias.as_str()) {
            return Err(format!("SELECT alias `{}` is reserved", alias));
        }

        if source_column_names.contains(alias.as_str()) {
            return Err(format!(
                "SELECT alias `{}` collides with an input column name",
                alias
            ));
        }

        if !seen_aliases.insert(alias.clone()) {
            return Err(format!("duplicate SELECT alias `{}`", alias));
        }
    }

    Ok(())
}

fn validate_relation_aliases_unsupported(select_stmt: &SelectStmt) -> Result<(), String> {
    for source_info in &select_stmt.source_infos {
        if let Some(alias) = source_info.alias.as_ref() {
            return Err(format!(
                "relation alias `{alias}` for `{}` is not supported yet",
                source_info.name
            ));
        }
    }
    Ok(())
}

fn relation_schema(
    name: &str,
    stream_defs: &HashMap<String, Arc<StreamDefinition>>,
    table_defs: &HashMap<String, Arc<TableDefinition>>,
) -> Option<Arc<datatypes::Schema>> {
    stream_defs
        .get(name)
        .map(|definition| definition.schema())
        .or_else(|| table_defs.get(name).map(|definition| definition.schema()))
}

fn resolve_select_aliases(select_stmt: &mut SelectStmt) -> Result<(), String> {
    use parser::window::Window;
    use sqlparser::ast::Expr;
    use std::collections::{HashMap, HashSet};

    let all_aliases: HashSet<String> = select_stmt
        .select_fields
        .iter()
        .filter_map(|f| f.alias.clone())
        .collect();

    if all_aliases.is_empty() {
        return Ok(());
    }

    // For now, aliases are only supported in SELECT, WHERE, and ORDER BY.
    //
    // We intentionally fail fast for other clauses so users get a clear error rather
    // than a later "column not found" during physical compilation.
    if let Some(expr) = select_stmt.having.as_ref() {
        if expr_references_any_alias(expr, &all_aliases) {
            return Err("aliases in HAVING are not supported yet".to_string());
        }
    }
    for expr in &select_stmt.group_by_exprs {
        if expr_references_any_alias(expr, &all_aliases) {
            return Err("aliases in GROUP BY are not supported yet".to_string());
        }
    }
    if let Some(window) = select_stmt.window.as_ref() {
        if let Some(filter) = window.filter() {
            if expr_references_any_alias(filter, &all_aliases) {
                return Err("aliases in window FILTER are not supported yet".to_string());
            }
        }
        match window {
            Window::State {
                open,
                emit,
                partition_by,
                ..
            } => {
                if expr_references_any_alias(open.as_ref(), &all_aliases)
                    || expr_references_any_alias(emit.as_ref(), &all_aliases)
                    || partition_by
                        .iter()
                        .any(|expr| expr_references_any_alias(expr, &all_aliases))
                {
                    return Err("aliases in window definitions are not supported yet".to_string());
                }
            }
            Window::Tumbling { .. } | Window::Count { .. } | Window::Sliding { .. } => {}
        }
    }

    // Resolve aliases left-to-right.
    //
    // Example:
    //   SELECT a + 1 AS b, b + 1 AS c
    // becomes:
    //   b := a + 1
    //   c := (a + 1) + 1
    let mut env: HashMap<String, Expr> = HashMap::new();
    for field in &mut select_stmt.select_fields {
        let rewritten = rewrite_expr_with_aliases_select_item(&field.expr, &env, &all_aliases)?;
        field.expr = rewritten;
        if let Some(alias) = field.alias.as_ref() {
            env.insert(alias.clone(), field.expr.clone());
        }
    }

    // Resolve aliases in WHERE using the final environment.
    if let Some(expr) = select_stmt.where_condition.as_mut() {
        let rewritten = rewrite_expr_with_aliases(expr, &env);
        *expr = rewritten;
    }

    // Resolve aliases in ORDER BY using the final environment.
    for item in &mut select_stmt.order_by {
        item.expr = rewrite_expr_with_aliases(&item.expr, &env);
    }

    Ok(())
}

fn expr_references_any_alias(
    expr: &sqlparser::ast::Expr,
    aliases: &std::collections::HashSet<String>,
) -> bool {
    use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr};

    match expr {
        Expr::Identifier(ident) => aliases.contains(ident.value.as_str()),
        Expr::CompoundIdentifier(_) => false,
        Expr::BinaryOp { left, right, .. } => {
            expr_references_any_alias(left, aliases) || expr_references_any_alias(right, aliases)
        }
        Expr::UnaryOp { expr, .. } => expr_references_any_alias(expr, aliases),
        Expr::Nested(expr) => expr_references_any_alias(expr, aliases),
        Expr::Cast { expr, .. } => expr_references_any_alias(expr, aliases),
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_references_any_alias(expr, aliases)
                || expr_references_any_alias(low, aliases)
                || expr_references_any_alias(high, aliases)
        }
        Expr::InList { expr, list, .. } => {
            expr_references_any_alias(expr, aliases)
                || list
                    .iter()
                    .any(|item| expr_references_any_alias(item, aliases))
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            operand
                .as_ref()
                .is_some_and(|expr| expr_references_any_alias(expr, aliases))
                || conditions
                    .iter()
                    .any(|expr| expr_references_any_alias(expr, aliases))
                || results
                    .iter()
                    .any(|expr| expr_references_any_alias(expr, aliases))
                || else_result
                    .as_ref()
                    .is_some_and(|expr| expr_references_any_alias(expr, aliases))
        }
        Expr::Function(func) => func.args.iter().any(|arg| match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                expr_references_any_alias(expr, aliases)
            }
            FunctionArg::Named {
                arg: FunctionArgExpr::Expr(expr),
                ..
            } => expr_references_any_alias(expr, aliases),
            _ => false,
        }),
        Expr::JsonAccess { left, right, .. } => {
            expr_references_any_alias(left, aliases) || expr_references_any_alias(right, aliases)
        }
        Expr::MapAccess { column, keys } => {
            expr_references_any_alias(column, aliases)
                || keys
                    .iter()
                    .any(|expr| expr_references_any_alias(expr, aliases))
        }
        _ => false,
    }
}

fn rewrite_expr_with_aliases_select_item(
    expr: &sqlparser::ast::Expr,
    env: &std::collections::HashMap<String, sqlparser::ast::Expr>,
    all_aliases: &std::collections::HashSet<String>,
) -> Result<sqlparser::ast::Expr, String> {
    use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr};

    Ok(match expr {
        Expr::Identifier(ident) => {
            let name = ident.value.as_str();
            if let Some(replacement) = env.get(name) {
                Expr::Nested(Box::new(replacement.clone()))
            } else if all_aliases.contains(name) {
                return Err(format!(
                    "forward reference to SELECT alias `{}` is not allowed",
                    name
                ));
            } else {
                Expr::Identifier(ident.clone())
            }
        }
        Expr::CompoundIdentifier(idents) => Expr::CompoundIdentifier(idents.clone()),
        Expr::Value(v) => Expr::Value(v.clone()),
        Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
            left: Box::new(rewrite_expr_with_aliases_select_item(
                left,
                env,
                all_aliases,
            )?),
            op: op.clone(),
            right: Box::new(rewrite_expr_with_aliases_select_item(
                right,
                env,
                all_aliases,
            )?),
        },
        Expr::UnaryOp { op, expr } => Expr::UnaryOp {
            op: *op,
            expr: Box::new(rewrite_expr_with_aliases_select_item(
                expr,
                env,
                all_aliases,
            )?),
        },
        Expr::Nested(expr) => Expr::Nested(Box::new(rewrite_expr_with_aliases_select_item(
            expr,
            env,
            all_aliases,
        )?)),
        Expr::Cast {
            expr,
            data_type,
            format,
        } => Expr::Cast {
            expr: Box::new(rewrite_expr_with_aliases_select_item(
                expr,
                env,
                all_aliases,
            )?),
            data_type: data_type.clone(),
            format: format.clone(),
        },
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => Expr::Between {
            expr: Box::new(rewrite_expr_with_aliases_select_item(
                expr,
                env,
                all_aliases,
            )?),
            negated: *negated,
            low: Box::new(rewrite_expr_with_aliases_select_item(
                low,
                env,
                all_aliases,
            )?),
            high: Box::new(rewrite_expr_with_aliases_select_item(
                high,
                env,
                all_aliases,
            )?),
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(rewrite_expr_with_aliases_select_item(
                expr,
                env,
                all_aliases,
            )?),
            list: list
                .iter()
                .map(|item| rewrite_expr_with_aliases_select_item(item, env, all_aliases))
                .collect::<Result<Vec<_>, _>>()?,
            negated: *negated,
        },
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => Expr::Case {
            operand: operand
                .as_ref()
                .map(|expr| rewrite_expr_with_aliases_select_item(expr, env, all_aliases))
                .transpose()?
                .map(Box::new),
            conditions: conditions
                .iter()
                .map(|expr| rewrite_expr_with_aliases_select_item(expr, env, all_aliases))
                .collect::<Result<Vec<_>, _>>()?,
            results: results
                .iter()
                .map(|expr| rewrite_expr_with_aliases_select_item(expr, env, all_aliases))
                .collect::<Result<Vec<_>, _>>()?,
            else_result: else_result
                .as_ref()
                .map(|expr| rewrite_expr_with_aliases_select_item(expr, env, all_aliases))
                .transpose()?
                .map(Box::new),
        },
        Expr::Function(func) => {
            let mut new = func.clone();
            new.args = new
                .args
                .into_iter()
                .map(|arg| match arg {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                        Ok(FunctionArg::Unnamed(FunctionArgExpr::Expr(
                            rewrite_expr_with_aliases_select_item(&expr, env, all_aliases)?,
                        )))
                    }
                    FunctionArg::Named { name, arg } => match arg {
                        FunctionArgExpr::Expr(expr) => Ok(FunctionArg::Named {
                            name,
                            arg: FunctionArgExpr::Expr(rewrite_expr_with_aliases_select_item(
                                &expr,
                                env,
                                all_aliases,
                            )?),
                        }),
                        other => Ok(FunctionArg::Named { name, arg: other }),
                    },
                    other => Ok(other),
                })
                .collect::<Result<Vec<_>, String>>()?;
            Expr::Function(new)
        }
        Expr::JsonAccess {
            left,
            operator,
            right,
        } => Expr::JsonAccess {
            left: Box::new(rewrite_expr_with_aliases_select_item(
                left,
                env,
                all_aliases,
            )?),
            operator: *operator,
            right: Box::new(rewrite_expr_with_aliases_select_item(
                right,
                env,
                all_aliases,
            )?),
        },
        Expr::MapAccess { column, keys } => Expr::MapAccess {
            column: Box::new(rewrite_expr_with_aliases_select_item(
                column,
                env,
                all_aliases,
            )?),
            keys: keys
                .iter()
                .map(|expr| rewrite_expr_with_aliases_select_item(expr, env, all_aliases))
                .collect::<Result<Vec<_>, _>>()?,
        },
        other => other.clone(),
    })
}

fn rewrite_expr_with_aliases(
    expr: &sqlparser::ast::Expr,
    env: &std::collections::HashMap<String, sqlparser::ast::Expr>,
) -> sqlparser::ast::Expr {
    use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr};

    match expr {
        Expr::Identifier(ident) => {
            let name = ident.value.as_str();
            if let Some(replacement) = env.get(name) {
                Expr::Nested(Box::new(replacement.clone()))
            } else {
                Expr::Identifier(ident.clone())
            }
        }
        Expr::CompoundIdentifier(idents) => Expr::CompoundIdentifier(idents.clone()),
        Expr::Value(v) => Expr::Value(v.clone()),
        Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
            left: Box::new(rewrite_expr_with_aliases(left, env)),
            op: op.clone(),
            right: Box::new(rewrite_expr_with_aliases(right, env)),
        },
        Expr::UnaryOp { op, expr } => Expr::UnaryOp {
            op: *op,
            expr: Box::new(rewrite_expr_with_aliases(expr, env)),
        },
        Expr::Nested(expr) => Expr::Nested(Box::new(rewrite_expr_with_aliases(expr, env))),
        Expr::Cast {
            expr,
            data_type,
            format,
        } => Expr::Cast {
            expr: Box::new(rewrite_expr_with_aliases(expr, env)),
            data_type: data_type.clone(),
            format: format.clone(),
        },
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => Expr::Between {
            expr: Box::new(rewrite_expr_with_aliases(expr, env)),
            negated: *negated,
            low: Box::new(rewrite_expr_with_aliases(low, env)),
            high: Box::new(rewrite_expr_with_aliases(high, env)),
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(rewrite_expr_with_aliases(expr, env)),
            list: list
                .iter()
                .map(|item| rewrite_expr_with_aliases(item, env))
                .collect(),
            negated: *negated,
        },
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => Expr::Case {
            operand: operand
                .as_ref()
                .map(|expr| Box::new(rewrite_expr_with_aliases(expr, env))),
            conditions: conditions
                .iter()
                .map(|expr| rewrite_expr_with_aliases(expr, env))
                .collect(),
            results: results
                .iter()
                .map(|expr| rewrite_expr_with_aliases(expr, env))
                .collect(),
            else_result: else_result
                .as_ref()
                .map(|expr| Box::new(rewrite_expr_with_aliases(expr, env))),
        },
        Expr::Function(func) => {
            let mut new = func.clone();
            new.args = new
                .args
                .into_iter()
                .map(|arg| match arg {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => FunctionArg::Unnamed(
                        FunctionArgExpr::Expr(rewrite_expr_with_aliases(&expr, env)),
                    ),
                    FunctionArg::Named { name, arg } => match arg {
                        FunctionArgExpr::Expr(expr) => FunctionArg::Named {
                            name,
                            arg: FunctionArgExpr::Expr(rewrite_expr_with_aliases(&expr, env)),
                        },
                        other => FunctionArg::Named { name, arg: other },
                    },
                    other => other,
                })
                .collect();
            Expr::Function(new)
        }
        Expr::JsonAccess {
            left,
            operator,
            right,
        } => Expr::JsonAccess {
            left: Box::new(rewrite_expr_with_aliases(left, env)),
            operator: *operator,
            right: Box::new(rewrite_expr_with_aliases(right, env)),
        },
        Expr::MapAccess { column, keys } => Expr::MapAccess {
            column: Box::new(rewrite_expr_with_aliases(column, env)),
            keys: keys
                .iter()
                .map(|expr| rewrite_expr_with_aliases(expr, env))
                .collect(),
        },
        other => other.clone(),
    }
}

fn expr_without_outer_nested(expr: &sqlparser::ast::Expr) -> &sqlparser::ast::Expr {
    use sqlparser::ast::Expr;

    let mut current = expr;
    while let Expr::Nested(inner) = current {
        current = inner.as_ref();
    }
    current
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
            Some(q) => src.source_name == q,
            None => true,
        })
        .filter_map(|src| src.column_lookup.get(column_name).cloned())
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

fn validate_having_constraints(select_stmt: &SelectStmt) -> Result<(), String> {
    let Some(having) = &select_stmt.having else {
        return Ok(());
    };

    if select_stmt.window.is_none() {
        return Err("HAVING requires GROUP BY window".to_string());
    }

    if !expr_contains_aggregate_placeholder(having) {
        return Err(format!(
            "HAVING expression '{}' must reference aggregate functions",
            having
        ));
    }

    let referenced_columns = collect_non_placeholder_column_refs(having);
    if !referenced_columns.is_empty() {
        return Err(format!(
            "HAVING expression '{}' must not reference non-aggregate columns: {}",
            having,
            referenced_columns.join(", ")
        ));
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

    Ok(())
}

fn validate_order_by_constraints(select_stmt: &SelectStmt) -> Result<(), String> {
    if select_stmt.order_by.is_empty() {
        return Ok(());
    }

    // For non-aggregation queries, ORDER BY can reference any scalar expressions.
    // Unsupported expressions will be rejected later during physical compilation.
    if select_stmt.aggregate_mappings.is_empty() {
        return Ok(());
    }

    let group_by_exprs: std::collections::HashSet<String> = select_stmt
        .group_by_exprs
        .iter()
        .map(|expr| expr_without_outer_nested(expr).to_string())
        .collect();

    for item in &select_stmt.order_by {
        if group_by_exprs.contains(&expr_without_outer_nested(&item.expr).to_string()) {
            continue;
        }

        if !expr_contains_aggregate_placeholder(&item.expr) {
            return Err(format!(
                "ORDER BY expression '{}' must be an aggregate or appear in GROUP BY",
                item.expr
            ));
        }

        let referenced_columns = collect_non_placeholder_column_refs(&item.expr);
        for column in referenced_columns {
            if !group_by_exprs.contains(&column) {
                return Err(format!(
                    "ORDER BY expression '{}' references column '{}' which must appear in GROUP BY",
                    item.expr, column
                ));
            }
        }

        if expr_references_any_stateful_placeholder(&item.expr, &select_stmt.stateful_mappings) {
            return Err(
                "ORDER BY must not reference stateful functions when aggregation is present"
                    .to_string(),
            );
        }
    }

    Ok(())
}

fn validate_window_filter_constraints(select_stmt: &SelectStmt) -> Result<(), String> {
    let Some(window) = select_stmt.window.as_ref() else {
        return Ok(());
    };
    let Some(filter) = window.filter() else {
        return Ok(());
    };

    if let Some(reason) = find_disallowed_window_filter_expression(filter) {
        return Err(format!(
            "window FILTER expression '{}' is not supported: {}",
            filter, reason
        ));
    }

    Ok(())
}

fn expr_references_any_stateful_placeholder(
    expr: &sqlparser::ast::Expr,
    stateful_mappings: &[parser::StatefulMappingEntry],
) -> bool {
    use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr};

    match expr {
        Expr::Identifier(ident) => stateful_mappings
            .iter()
            .any(|entry| entry.output_column == ident.value),
        Expr::CompoundIdentifier(_) => false,
        Expr::BinaryOp { left, right, .. } => {
            expr_references_any_stateful_placeholder(left, stateful_mappings)
                || expr_references_any_stateful_placeholder(right, stateful_mappings)
        }
        Expr::UnaryOp { expr, .. } => {
            expr_references_any_stateful_placeholder(expr, stateful_mappings)
        }
        Expr::Nested(expr) => expr_references_any_stateful_placeholder(expr, stateful_mappings),
        Expr::Cast { expr, .. } => {
            expr_references_any_stateful_placeholder(expr, stateful_mappings)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_references_any_stateful_placeholder(expr, stateful_mappings)
                || expr_references_any_stateful_placeholder(low, stateful_mappings)
                || expr_references_any_stateful_placeholder(high, stateful_mappings)
        }
        Expr::InList { expr, list, .. } => {
            expr_references_any_stateful_placeholder(expr, stateful_mappings)
                || list
                    .iter()
                    .any(|item| expr_references_any_stateful_placeholder(item, stateful_mappings))
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            operand.as_ref().is_some_and(|expr| {
                expr_references_any_stateful_placeholder(expr, stateful_mappings)
            }) || conditions
                .iter()
                .any(|expr| expr_references_any_stateful_placeholder(expr, stateful_mappings))
                || results
                    .iter()
                    .any(|expr| expr_references_any_stateful_placeholder(expr, stateful_mappings))
                || else_result.as_ref().is_some_and(|expr| {
                    expr_references_any_stateful_placeholder(expr, stateful_mappings)
                })
        }
        Expr::Function(func) => func.args.iter().any(|arg| match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                expr_references_any_stateful_placeholder(expr, stateful_mappings)
            }
            FunctionArg::Named { arg, .. } => {
                if let FunctionArgExpr::Expr(expr) = arg {
                    return expr_references_any_stateful_placeholder(expr, stateful_mappings);
                }
                false
            }
            _ => false,
        }),
        Expr::JsonAccess { left, right, .. } => {
            expr_references_any_stateful_placeholder(left, stateful_mappings)
                || expr_references_any_stateful_placeholder(right, stateful_mappings)
        }
        Expr::MapAccess { column, keys } => {
            expr_references_any_stateful_placeholder(column, stateful_mappings)
                || keys
                    .iter()
                    .any(|expr| expr_references_any_stateful_placeholder(expr, stateful_mappings))
        }
        _ => false,
    }
}

fn collect_non_placeholder_column_refs(expr: &sqlparser::ast::Expr) -> Vec<String> {
    use sqlparser::ast::Expr;

    fn is_placeholder(ident: &sqlparser::ast::Ident) -> bool {
        crate::expr::internal_columns::is_parser_placeholder(ident.value.as_str())
    }

    fn collect(expr: &Expr, out: &mut std::collections::HashSet<String>) {
        match expr {
            Expr::Identifier(ident) if !is_placeholder(ident) => {
                out.insert(ident.value.clone());
            }
            Expr::Identifier(_) => {}
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
        crate::expr::internal_columns::is_parser_placeholder(ident.value.as_str())
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

fn find_disallowed_window_filter_expression(expr: &Expr) -> Option<String> {
    fn visit(expr: &Expr) -> Option<String> {
        match expr {
            Expr::Function(func) => {
                let func_name = func.name.to_string().to_lowercase();
                if is_window_function_name(&func_name) {
                    return Some(format!("window function `{}`", func.name));
                }
                if func_name == "last_hit_count" {
                    return Some("pipeline state function `last_hit_count`".to_string());
                }
                if parser::aggregate_registry::default_aggregate_registry()
                    .is_aggregate_function(&func_name)
                {
                    return Some(format!("aggregate function `{}`", func.name));
                }
                if parser::stateful_registry::default_stateful_registry()
                    .is_stateful_function(&func_name)
                {
                    return Some(format!("stateful function `{}`", func.name));
                }
                if func.filter.is_some() {
                    return Some(format!("function FILTER clause on `{}`", func.name));
                }
                if func.over.is_some() {
                    return Some(format!("function OVER clause on `{}`", func.name));
                }
                if !func.order_by.is_empty() {
                    return Some(format!("function ORDER BY clause on `{}`", func.name));
                }

                for arg in &func.args {
                    match arg {
                        sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(expr),
                        ) => {
                            if let Some(reason) = visit(expr) {
                                return Some(reason);
                            }
                        }
                        sqlparser::ast::FunctionArg::Named {
                            arg: sqlparser::ast::FunctionArgExpr::Expr(expr),
                            ..
                        } => {
                            if let Some(reason) = visit(expr) {
                                return Some(reason);
                            }
                        }
                        _ => {}
                    }
                }
                None
            }
            Expr::BinaryOp { left, right, .. } => visit(left).or_else(|| visit(right)),
            Expr::UnaryOp { expr, .. } => visit(expr),
            Expr::Nested(expr) => visit(expr),
            Expr::Cast { expr, .. } => visit(expr),
            Expr::Between {
                expr, low, high, ..
            } => visit(expr).or_else(|| visit(low)).or_else(|| visit(high)),
            Expr::InList { expr, list, .. } => visit(expr).or_else(|| list.iter().find_map(visit)),
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => operand
                .as_deref()
                .and_then(visit)
                .or_else(|| conditions.iter().find_map(visit))
                .or_else(|| results.iter().find_map(visit))
                .or_else(|| else_result.as_deref().and_then(visit)),
            Expr::JsonAccess { left, right, .. } => visit(left).or_else(|| visit(right)),
            Expr::MapAccess { column, keys } => {
                visit(column).or_else(|| keys.iter().find_map(visit))
            }
            _ => None,
        }
    }

    visit(expr)
}

fn is_window_function_name(name: &str) -> bool {
    matches!(
        name,
        "tumblingwindow" | "countwindow" | "slidingwindow" | "statewindow"
    )
}

fn split_window_filter(window: parser_window::Window) -> (parser_window::Window, Option<Expr>) {
    let filter = window.filter().cloned();
    (window.with_filter(None), filter)
}

fn convert_window_spec(window: parser_window::Window) -> Result<LogicalWindowSpec, String> {
    match window {
        parser_window::Window::Tumbling {
            time_unit, length, ..
        } => {
            let unit = match time_unit {
                parser_window::TimeUnit::Seconds => TimeUnit::Seconds,
            };
            Ok(LogicalWindowSpec::Tumbling {
                time_unit: unit,
                length,
            })
        }
        parser_window::Window::Count { count, .. } => Ok(LogicalWindowSpec::Count { count }),
        parser_window::Window::Sliding {
            time_unit,
            lookback,
            lookahead,
            ..
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
            ..
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
