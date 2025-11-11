use std::sync::Arc;
use crate::planner::logical::{LogicalPlan, BaseLogicalPlan};
use crate::expr::ScalarExpr;

#[derive(Debug, Clone)]
pub struct ProjectField {
    pub field_name: String,
    pub expr: ScalarExpr,
}

#[derive(Debug, Clone)]
pub struct Project {
    pub base: BaseLogicalPlan,
    pub fields: Vec<ProjectField>,
}

impl Project {
    pub fn new(fields: Vec<ProjectField>, children: Vec<Arc<dyn LogicalPlan>>, index: i64) -> Self {
        let base = BaseLogicalPlan::new(children, index);
        Self { base, fields }
    }
}

impl LogicalPlan for Project {
    fn children(&self) -> &[Arc<dyn LogicalPlan>] {
        &self.base.children
    }

    fn get_plan_type(&self) -> &str {
        "Project"
    }

    fn get_plan_index(&self) -> &i64 {
        &self.base.index
    }
}