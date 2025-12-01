use crate::planner::physical::BasePhysicalPlan;
use crate::planner::sink::PipelineSink;
use std::fmt;
use std::sync::Arc;

use super::PhysicalPlan;

/// Physical plan node for sink stage.
#[derive(Clone)]
pub struct PhysicalDataSink {
    pub base: BasePhysicalPlan,
    pub sinks: Vec<PipelineSink>,
}

impl PhysicalDataSink {
    pub fn new(children: Vec<Arc<PhysicalPlan>>, index: i64, sinks: Vec<PipelineSink>) -> Self {
        Self {
            base: BasePhysicalPlan::new(children, index),
            sinks,
        }
    }
}

impl fmt::Debug for PhysicalDataSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PhysicalDataSink")
            .field("index", &self.base.index())
            .field("sink_count", &self.sinks.len())
            .finish()
    }
}
