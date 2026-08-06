use crate::planner::logical::BaseLogicalPlan;
use sqlparser::ast::Expr;
use std::sync::Arc;

/// Supported time units for window definitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeUnit {
    Seconds,
}

/// Logical window specification.
#[derive(Debug, Clone, PartialEq)]
pub enum LogicalWindowSpec {
    Tumbling {
        time_unit: TimeUnit,
        length: u64,
        /// Optional partition keys extracted from `OVER (PARTITION BY ...)`.
        /// When empty, the window is global (single partition).
        partition_by: Vec<Expr>,
    },
    Count {
        count: u64,
        /// Optional partition keys extracted from `OVER (PARTITION BY ...)`.
        /// When empty, the window is global (single partition).
        partition_by: Vec<Expr>,
    },
    Sliding {
        time_unit: TimeUnit,
        lookback: u64,
        lookahead: Option<u64>,
        /// Optional partition keys extracted from `OVER (PARTITION BY ...)`.
        /// When empty, the window is global (single partition).
        partition_by: Vec<Expr>,
    },
    State {
        open: Box<Expr>,
        emit: Box<Expr>,
        /// Optional partition keys extracted from `OVER (PARTITION BY ...)`.
        /// When empty, the window is global (single partition).
        partition_by: Vec<Expr>,
    },
    Eos,
}

impl LogicalWindowSpec {
    pub fn partition_by(&self) -> &[Expr] {
        match self {
            LogicalWindowSpec::Tumbling { partition_by, .. }
            | LogicalWindowSpec::Count { partition_by, .. }
            | LogicalWindowSpec::Sliding { partition_by, .. }
            | LogicalWindowSpec::State { partition_by, .. } => partition_by,
            LogicalWindowSpec::Eos => &[],
        }
    }

    pub fn expression_inputs(&self) -> Vec<&Expr> {
        let mut exprs = Vec::new();
        match self {
            LogicalWindowSpec::State { open, emit, .. } => {
                exprs.push(open.as_ref());
                exprs.push(emit.as_ref());
            }
            LogicalWindowSpec::Tumbling { .. }
            | LogicalWindowSpec::Count { .. }
            | LogicalWindowSpec::Sliding { .. }
            | LogicalWindowSpec::Eos => {}
        }
        exprs.extend(self.partition_by());
        exprs
    }
}

/// Logical plan node for windowing.
#[derive(Debug, Clone)]
pub struct LogicalWindow {
    pub base: BaseLogicalPlan,
    pub spec: LogicalWindowSpec,
}

impl LogicalWindow {
    pub fn new(
        spec: LogicalWindowSpec,
        children: Vec<Arc<super::LogicalPlan>>,
        index: i64,
    ) -> Self {
        let base = BaseLogicalPlan::new(children, index);
        Self { base, spec }
    }
}
