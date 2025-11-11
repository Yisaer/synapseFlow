use std::any::Any;
use std::sync::Arc;
use crate::planner::logical::{LogicalPlan, BaseLogicalPlan};
use sqlparser::ast::Expr;

#[derive(Debug, Clone)]
pub struct Filter {
    pub base: BaseLogicalPlan,
    pub predicate: Expr,
}

impl Filter {
    pub fn new(predicate: Expr, children: Vec<Arc<dyn LogicalPlan>>,index:i64) -> Self {
        let base = BaseLogicalPlan::new(children,index);
        Self { base, predicate }
    }
}

impl LogicalPlan for Filter {
    fn children(&self) -> &[Arc<dyn LogicalPlan>] {
        &self.base.children
    }

    fn get_plan_type(&self) -> &str {
        "Filter"
    }

    fn get_plan_index(&self) -> &i64 {
        &self.base.index
    }
    
    fn as_any(&self) -> &dyn Any {
        self
    }
}