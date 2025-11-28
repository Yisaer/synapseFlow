use crate::expr::ScalarExpr;
use crate::planner::physical::{BasePhysicalPlan, PhysicalPlan};
use sqlparser::ast::Expr;
use std::sync::Arc;

/// Physical operator for filter operations
///
/// This operator represents the physical execution of filter operations,
/// applying predicate expressions to filter records from input data.
#[derive(Debug, Clone)]
pub struct PhysicalFilter {
    pub base: BasePhysicalPlan,
    pub predicate: Expr,
    pub scalar_predicate: ScalarExpr,
}

impl PhysicalFilter {
    /// Create a new PhysicalFilter
    pub fn new(
        predicate: Expr,
        scalar_predicate: ScalarExpr,
        children: Vec<Arc<PhysicalPlan>>,
        index: i64,
    ) -> Self {
        let base = BasePhysicalPlan::new(children, index);
        Self {
            base,
            predicate,
            scalar_predicate,
        }
    }
}
