use crate::planner::logical::{BaseLogicalPlan, LogicalPlan};
use sqlparser::ast::Expr;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ProjectField {
    pub field_name: String,
    pub expr: Expr,
}

#[derive(Debug, Clone)]
pub struct Project {
    pub base: BaseLogicalPlan,
    pub fields: Vec<ProjectField>,
}

impl Project {
    pub fn new(fields: Vec<ProjectField>, children: Vec<Arc<LogicalPlan>>, index: i64) -> Self {
        let base = BaseLogicalPlan::new(children, index);
        Self { base, fields }
    }
}
