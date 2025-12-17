use crate::planner::logical::BaseLogicalPlan;
use std::sync::Arc;

/// Supported time units for window definitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeUnit {
    Seconds,
}

/// Logical window specification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogicalWindowSpec {
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
