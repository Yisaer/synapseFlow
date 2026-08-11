use crate::expr::ScalarExpr;
use crate::planner::logical::TimeUnit;
use crate::planner::physical::{BasePhysicalPlan, PhysicalPlan, PipelineStateUsage};
use crate::processor::processor_state::ProcessorState;
use sqlparser::ast::Expr;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct PhysicalTumblingWindow {
    pub base: BasePhysicalPlan,
    pub time_unit: TimeUnit,
    pub length: u64,
    pub partition_by_exprs: Vec<Expr>,
    pub partition_by_scalars: Vec<ScalarExpr>,
}

impl PhysicalTumblingWindow {
    pub fn new(
        time_unit: TimeUnit,
        length: u64,
        children: Vec<Arc<PhysicalPlan>>,
        index: i64,
    ) -> Self {
        Self::new_partitioned(time_unit, length, Vec::new(), Vec::new(), children, index)
    }

    pub fn new_partitioned(
        time_unit: TimeUnit,
        length: u64,
        partition_by_exprs: Vec<Expr>,
        partition_by_scalars: Vec<ScalarExpr>,
        children: Vec<Arc<PhysicalPlan>>,
        index: i64,
    ) -> Self {
        let base = BasePhysicalPlan::new(children, index);
        Self {
            base,
            time_unit,
            length,
            partition_by_exprs,
            partition_by_scalars,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PhysicalCountWindow {
    pub base: BasePhysicalPlan,
    pub count: u64,
    pub partition_by_exprs: Vec<Expr>,
    pub partition_by_scalars: Vec<ScalarExpr>,
}

impl PhysicalCountWindow {
    pub fn new(count: u64, children: Vec<Arc<PhysicalPlan>>, index: i64) -> Self {
        Self::new_partitioned(count, Vec::new(), Vec::new(), children, index)
    }

    pub fn new_partitioned(
        count: u64,
        partition_by_exprs: Vec<Expr>,
        partition_by_scalars: Vec<ScalarExpr>,
        children: Vec<Arc<PhysicalPlan>>,
        index: i64,
    ) -> Self {
        let base = BasePhysicalPlan::new(children, index);
        Self {
            base,
            count,
            partition_by_exprs,
            partition_by_scalars,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PhysicalSlidingWindow {
    pub base: BasePhysicalPlan,
    pub time_unit: TimeUnit,
    pub lookback: u64,
    pub lookahead: Option<u64>,
    pub partition_by_exprs: Vec<Expr>,
    pub partition_by_scalars: Vec<ScalarExpr>,
    pub trigger_condition_expr: Option<Expr>,
    pub trigger_condition_scalar: Option<ScalarExpr>,
    pub trigger_processor_state: Option<Arc<ProcessorState>>,
    pub trigger_state_usage: PipelineStateUsage,
}

impl PhysicalSlidingWindow {
    pub fn new(
        time_unit: TimeUnit,
        lookback: u64,
        lookahead: Option<u64>,
        children: Vec<Arc<PhysicalPlan>>,
        index: i64,
    ) -> Self {
        Self::new_partitioned(
            time_unit,
            lookback,
            lookahead,
            Vec::new(),
            Vec::new(),
            children,
            index,
        )
    }

    pub fn new_partitioned(
        time_unit: TimeUnit,
        lookback: u64,
        lookahead: Option<u64>,
        partition_by_exprs: Vec<Expr>,
        partition_by_scalars: Vec<ScalarExpr>,
        children: Vec<Arc<PhysicalPlan>>,
        index: i64,
    ) -> Self {
        Self::new_with_trigger(
            time_unit,
            lookback,
            lookahead,
            partition_by_exprs,
            partition_by_scalars,
            None,
            None,
            children,
            index,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_trigger(
        time_unit: TimeUnit,
        lookback: u64,
        lookahead: Option<u64>,
        partition_by_exprs: Vec<Expr>,
        partition_by_scalars: Vec<ScalarExpr>,
        trigger_condition_expr: Option<Expr>,
        trigger_condition_scalar: Option<ScalarExpr>,
        children: Vec<Arc<PhysicalPlan>>,
        index: i64,
    ) -> Self {
        let base = BasePhysicalPlan::new(children, index);
        Self {
            base,
            time_unit,
            lookback,
            lookahead,
            partition_by_exprs,
            partition_by_scalars,
            trigger_condition_expr,
            trigger_condition_scalar,
            trigger_processor_state: None,
            trigger_state_usage: PipelineStateUsage::default(),
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
