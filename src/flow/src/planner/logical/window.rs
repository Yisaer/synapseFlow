use crate::planner::logical::BaseLogicalPlan;
use sqlparser::ast::Expr;
use std::sync::Arc;
use std::time::Duration;

/// Supported time units for window definitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeUnit {
    Milliseconds,
    Seconds,
}

impl TimeUnit {
    pub fn duration(self, value: u64) -> Duration {
        match self {
            TimeUnit::Milliseconds => Duration::from_millis(value),
            TimeUnit::Seconds => Duration::from_secs(value),
        }
    }
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
        /// Optional trigger condition extracted from `OVER (WHEN ...)`.
        /// When absent, every input row triggers a sliding window emission.
        trigger_condition: Option<Box<Expr>>,
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
            LogicalWindowSpec::Sliding {
                trigger_condition: Some(cond),
                ..
            } => {
                exprs.push(cond.as_ref());
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
