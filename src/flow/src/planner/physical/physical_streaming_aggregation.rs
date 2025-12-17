use crate::aggregation::AggregateFunctionRegistry;
use crate::expr::ScalarExpr;
use crate::planner::logical::TimeUnit;
use crate::planner::physical::{AggregateCall, BasePhysicalPlan, PhysicalPlan};
use sqlparser::ast::Expr;
use std::collections::HashMap;
use std::sync::Arc;

/// Window spec captured for streaming aggregation rewrite.
#[derive(Debug, Clone)]
pub enum StreamingWindowSpec {
    Tumbling {
        time_unit: TimeUnit,
        length: u64,
    },
    Count {
        count: u64,
    },
    Sliding {
        time_unit: TimeUnit,
        lookback: u64,
        lookahead: Option<u64>,
    },
}

/// Physical node that fuses window + aggregation for incremental processing.
#[derive(Debug, Clone)]
pub struct PhysicalStreamingAggregation {
    pub base: BasePhysicalPlan,
    pub window: StreamingWindowSpec,
    pub aggregate_mappings: HashMap<String, Expr>,
    pub aggregate_calls: Vec<AggregateCall>,
    pub group_by_exprs: Vec<Expr>,
    pub group_by_scalars: Vec<ScalarExpr>,
}

impl PhysicalStreamingAggregation {
    pub fn new(
        window: StreamingWindowSpec,
        aggregate_mappings: HashMap<String, Expr>,
        group_by_exprs: Vec<Expr>,
        aggregate_calls: Vec<AggregateCall>,
        group_by_scalars: Vec<ScalarExpr>,
        children: Vec<Arc<PhysicalPlan>>,
        index: i64,
    ) -> Self {
        Self {
            base: BasePhysicalPlan::new(children, index),
            window,
            aggregate_mappings,
            aggregate_calls,
            group_by_exprs,
            group_by_scalars,
        }
    }

    pub fn all_calls_incremental(
        aggregate_calls: &[AggregateCall],
        registry: &AggregateFunctionRegistry,
    ) -> bool {
        aggregate_calls
            .iter()
            .all(|call| registry.supports_incremental(&call.func_name))
    }
}
