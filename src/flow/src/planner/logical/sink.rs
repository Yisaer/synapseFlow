use super::BaseLogicalPlan;
use super::LogicalPlan;
use crate::planner::sink::PipelineSink;
use std::fmt;
use std::sync::Arc;

/// Logical plan node that represents the sink stage for a pipeline.
#[derive(Clone)]
pub struct DataSinkPlan {
    pub base: BaseLogicalPlan,
    pub sinks: Vec<PipelineSink>,
}

impl DataSinkPlan {
    pub fn new(children: Vec<Arc<LogicalPlan>>, index: i64, sinks: Vec<PipelineSink>) -> Self {
        Self {
            base: BaseLogicalPlan::new(children, index),
            sinks,
        }
    }
}

impl fmt::Debug for DataSinkPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataSinkPlan")
            .field("index", &self.base.index())
            .field("sink_count", &self.sinks.len())
            .finish()
    }
}
