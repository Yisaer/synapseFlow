use std::sync::Arc;
use std::fmt::Debug;
use std::any::Any;
use parser::SelectStmt;
use crate::expr::sql_conversion::convert_expr_to_scalar;

pub trait LogicalPlan: Send + Sync + Debug {
    fn children(&self) -> &[Arc<dyn LogicalPlan>];
    fn get_plan_type(&self) -> &str;
    fn get_plan_index(&self) -> &i64;
    fn as_any(&self) -> &dyn Any;
}

#[derive(Debug, Clone)]
pub struct BaseLogicalPlan {
    pub index :i64,
    pub children: Vec<Arc<dyn LogicalPlan>>,
}

impl BaseLogicalPlan {
    pub fn new(children: Vec<Arc<dyn LogicalPlan>>,index: i64) -> Self {
        Self { children ,index}
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
pub fn create_logical_plan(select_stmt: SelectStmt) -> Result<Arc<dyn LogicalPlan>, String> {
    let start_index = 0i64;
    let mut current_index = start_index;
    
    // 1. Create DataSource(s) from source_infos
    if select_stmt.source_infos.is_empty() {
        return Err("No data source found in SELECT statement".to_string());
    }
    
    let mut current_plans: Vec<Arc<dyn LogicalPlan>> = Vec::new();
    for source_info in &select_stmt.source_infos {
        let datasource = DataSource::new(
            source_info.name.clone(),
            current_index,
        );
        current_plans.push(Arc::new(datasource));
        current_index += 1;
    }
    
    // 2. Create Filter from where_condition if present
    if let Some(where_expr) = select_stmt.where_condition {
        // Convert sqlparser Expr to ScalarExpr for the filter predicate
        // For now, we'll keep the original expression in the Filter node
        // In a full implementation, we'd convert this to a ScalarExpr
        let filter = Filter::new(
            where_expr,
            current_plans,
            current_index,
        );
        current_plans = vec![Arc::new(filter)];
        current_index += 1;
    }
    
    // 3. Create Project from select_fields
    let mut project_fields = Vec::new();
    for (i, select_field) in select_stmt.select_fields.iter().enumerate() {
        // Convert sqlparser Expr to ScalarExpr
        let scalar_expr = convert_expr_to_scalar(&select_field.expr)
            .map_err(|e| format!("Failed to convert select field {}: {}", i, e))?;
        
        let field_name = select_field.alias.clone()
            .unwrap_or_else(|| select_field.expr.to_string());
        
        project_fields.push(project::ProjectField {
            field_name,
            expr: scalar_expr,
        });
    }
    
    let project = Project::new(
        project_fields,
        current_plans,
        current_index,
    );
    
    Ok(Arc::new(project) as Arc<dyn LogicalPlan>)
}

pub mod datasource;
pub mod project;
pub mod filter;

pub use datasource::DataSource;
pub use project::Project;
pub use filter::Filter;

#[cfg(test)]
mod logical_plan_tests {
    use super::*;
    use parser::parse_sql;

    #[test]
    fn test_create_logical_plan_simple() {
        let sql = "SELECT a, b FROM users";
        let select_stmt = parse_sql(sql).unwrap();
        
        let plan = create_logical_plan(select_stmt).unwrap();
        
        // Should be a Project node
        assert_eq!(plan.get_plan_type(), "Project");
        assert_eq!(plan.get_plan_index(), &1); // DataSource(0) -> Project(1)
        
        // Check that it has one child (DataSource)
        let children = plan.children();
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].get_plan_type(), "DataSource");
        assert_eq!(children[0].get_plan_index(), &0);
    }
    
    #[test]
    fn test_create_logical_plan_with_filter() {
        let sql = "SELECT a, b FROM users WHERE a > 10";
        let select_stmt = parse_sql(sql).unwrap();
        
        let plan = create_logical_plan(select_stmt).unwrap();
        
        // Should be a Project node
        assert_eq!(plan.get_plan_type(), "Project");
        assert_eq!(plan.get_plan_index(), &2); // DataSource(0) -> Filter(1) -> Project(2)
        
        // Check the filter in the middle
        let children = plan.children();
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].get_plan_type(), "Filter");
        assert_eq!(children[0].get_plan_index(), &1);
        
        // Check the datasource at the bottom
        let filter_children = children[0].children();
        assert_eq!(filter_children.len(), 1);
        assert_eq!(filter_children[0].get_plan_type(), "DataSource");
        assert_eq!(filter_children[0].get_plan_index(), &0);
    }
    
    #[test]
    fn test_create_logical_plan_with_alias() {
        let sql = "SELECT a, b FROM users AS u WHERE a > 10";
        let select_stmt = parse_sql(sql).unwrap();
        
        let plan = create_logical_plan(select_stmt).unwrap();
        
        // Verify the structure is correct
        assert_eq!(plan.get_plan_type(), "Project");
        
        let children = plan.children();
        assert_eq!(children[0].get_plan_type(), "Filter");
        
        let filter_children = children[0].children();
        assert_eq!(filter_children[0].get_plan_type(), "DataSource");
    }

    #[test]
    fn test_create_logical_plan_with_func_field() {
        let sql =  "SELECT a, concat(b), c AS custom_name FROM users";
        let select_stmt = parse_sql(sql).unwrap();

        let plan = create_logical_plan(select_stmt).unwrap();

        // Verify the structure is correct
        assert_eq!(plan.get_plan_type(), "Project");

        // Downcast to Project to access fields
        let project = plan.as_any().downcast_ref::<Project>().expect("Should be a Project");

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
