use crate::expr::ScalarExpr;
use crate::planner::logical::TimeUnit;
use crate::planner::physical::{BasePhysicalPlan, PhysicalPlan};
use sqlparser::ast::Expr;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct PhysicalTumblingWindow {
    pub base: BasePhysicalPlan,
    pub time_unit: TimeUnit,
    pub length: u64,
}

impl PhysicalTumblingWindow {
    pub fn new(
        time_unit: TimeUnit,
        length: u64,
        children: Vec<Arc<PhysicalPlan>>,
        index: i64,
    ) -> Self {
        let base = BasePhysicalPlan::new(children, index);
        Self {
            base,
            time_unit,
            length,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PhysicalCountWindow {
    pub base: BasePhysicalPlan,
    pub count: u64,
}

impl PhysicalCountWindow {
    pub fn new(count: u64, children: Vec<Arc<PhysicalPlan>>, index: i64) -> Self {
        let base = BasePhysicalPlan::new(children, index);
        Self { base, count }
    }
}

#[derive(Debug, Clone)]
pub struct PhysicalSlidingWindow {
    pub base: BasePhysicalPlan,
    pub time_unit: TimeUnit,
    pub lookback: u64,
    pub lookahead: Option<u64>,
}

impl PhysicalSlidingWindow {
    pub fn new(
        time_unit: TimeUnit,
        lookback: u64,
        lookahead: Option<u64>,
        children: Vec<Arc<PhysicalPlan>>,
        index: i64,
    ) -> Self {
        let base = BasePhysicalPlan::new(children, index);
        Self {
            base,
            time_unit,
            lookback,
            lookahead,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PhysicalStateWindow {
    pub base: BasePhysicalPlan,
    pub open_expr: Expr,
    pub emit_expr: Expr,
    pub partition_by_exprs: Vec<Expr>,
    pub open_scalar: ScalarExpr,
    pub emit_scalar: ScalarExpr,
    pub partition_by_scalars: Vec<ScalarExpr>,
}

impl PhysicalStateWindow {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        open_expr: Expr,
        emit_expr: Expr,
        partition_by_exprs: Vec<Expr>,
        open_scalar: ScalarExpr,
        emit_scalar: ScalarExpr,
        partition_by_scalars: Vec<ScalarExpr>,
        children: Vec<Arc<PhysicalPlan>>,
        index: i64,
    ) -> Self {
        let base = BasePhysicalPlan::new(children, index);
        Self {
            base,
            open_expr,
            emit_expr,
            partition_by_exprs,
            open_scalar,
            emit_scalar,
            partition_by_scalars,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PhysicalEosWindow {
    pub base: BasePhysicalPlan,
}

impl PhysicalEosWindow {
    pub fn new(children: Vec<Arc<PhysicalPlan>>, index: i64) -> Self {
        let base = BasePhysicalPlan::new(children, index);
        Self { base }
    }
}
