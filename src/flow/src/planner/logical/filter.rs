use crate::planner::logical::{BaseLogicalPlan, LogicalPlan};
use sqlparser::ast::Expr;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Filter {
    pub base: BaseLogicalPlan,
    pub predicate: Expr,
}

impl Filter {
    pub fn new(predicate: Expr, children: Vec<Arc<LogicalPlan>>, index: i64) -> Self {
        let base = BaseLogicalPlan::new(children, index);
        Self { base, predicate }
    }
}
