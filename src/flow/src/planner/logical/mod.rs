use parser::SelectStmt;
use std::sync::Arc;

pub mod sink;

use crate::planner::sink::PipelineSink;
pub use sink::DataSinkPlan;

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
    Filter(Filter),
    Project(Project),
    DataSink(DataSinkPlan),
}

impl LogicalPlan {
    pub fn children(&self) -> &[Arc<LogicalPlan>] {
        match self {
            LogicalPlan::DataSource(plan) => plan.base.children(),
            LogicalPlan::Filter(plan) => plan.base.children(),
            LogicalPlan::Project(plan) => plan.base.children(),
            LogicalPlan::DataSink(plan) => plan.base.children(),
        }
    }

    pub fn get_plan_type(&self) -> &str {
        match self {
            LogicalPlan::DataSource(_) => "DataSource",
            LogicalPlan::Filter(_) => "Filter",
            LogicalPlan::Project(_) => "Project",
            LogicalPlan::DataSink(_) => "DataSink",
        }
    }

    pub fn get_plan_index(&self) -> i64 {
        match self {
            LogicalPlan::DataSource(plan) => plan.base.index(),
            LogicalPlan::Filter(plan) => plan.base.index(),
            LogicalPlan::Project(plan) => plan.base.index(),
            LogicalPlan::DataSink(plan) => plan.base.index(),
        }
    }
}

/// Create a LogicalPlan from a SelectStmt
///
/// The plan structure will be:
/// - DataSource(s) (from SelectStmt::source_infos, one per source)
/// - Filter (from SelectStmt::where_condition, if present) - takes all DataSources as children
/// - Project (from SelectStmt::select_fields) - takes Filter or DataSources as children
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
) -> Result<Arc<LogicalPlan>, String> {
    let start_index = 0i64;
    let mut current_index = start_index;

    // 1. Create DataSource(s) from source_infos
    if select_stmt.source_infos.is_empty() {
        return Err("No data source found in SELECT statement".to_string());
    }

    let mut current_plans: Vec<Arc<LogicalPlan>> = Vec::new();
    for source_info in &select_stmt.source_infos {
        let datasource = DataSource::new(
            source_info.name.clone(),
            source_info.alias.clone(),
            current_index,
        );
        current_plans.push(Arc::new(LogicalPlan::DataSource(datasource)));
        current_index += 1;
    }

    // 2. Create Filter from where_condition if present
    if let Some(where_expr) = select_stmt.where_condition {
        // Convert sqlparser Expr to ScalarExpr for the filter predicate
        // For now, we'll keep the original expression in the Filter node
        // In a full implementation, we'd convert this to a ScalarExpr
        let filter = Filter::new(where_expr, current_plans, current_index);
        current_plans = vec![Arc::new(LogicalPlan::Filter(filter))];
        current_index += 1;
    }

    // 3. Create Project from select_fields
    let mut project_fields = Vec::new();
    for select_field in select_stmt.select_fields.iter() {
        let field_name = select_field
            .alias
            .clone()
            .unwrap_or_else(|| select_field.expr.to_string());

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
        let next_index = max_plan_index(&base) + 1;
        let sink_plan = DataSinkPlan::new(vec![base], next_index, sinks);
        Ok(Arc::new(LogicalPlan::DataSink(sink_plan)))
    }
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

pub mod datasource;
pub mod filter;
pub mod project;

pub use datasource::DataSource;
pub use filter::Filter;
pub use project::Project;

#[cfg(test)]
mod logical_plan_tests {
    use super::*;
    use parser::parse_sql;

    #[test]
    fn test_create_logical_plan_simple() {
        let sql = "SELECT a, b FROM users";
        let select_stmt = parse_sql(sql).unwrap();

        let plan = create_logical_plan(select_stmt, Vec::new()).unwrap();

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

        let plan = create_logical_plan(select_stmt, Vec::new()).unwrap();

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
    fn test_create_logical_plan_with_alias() {
        let sql = "SELECT a, b FROM users AS u WHERE a > 10";
        let select_stmt = parse_sql(sql).unwrap();

        let plan = create_logical_plan(select_stmt, Vec::new()).unwrap();

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

        let plan = create_logical_plan(select_stmt, Vec::new()).unwrap();

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
}
