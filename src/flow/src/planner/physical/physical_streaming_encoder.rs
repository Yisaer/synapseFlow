use crate::planner::physical::BasePhysicalPlan;
use crate::planner::sink::{CommonSinkProps, SinkEncoderConfig};
use std::fmt;
use std::sync::Arc;

use super::PhysicalPlan;

/// Physical node representing a streaming encoder stage that also handles batching.
/// 
/// Note: connector_id has been removed as it's not needed for processor identification.
/// Processor IDs are now generated using only sink_id and physical_plan_name.
#[derive(Clone)]
pub struct PhysicalStreamingEncoder {
    pub base: BasePhysicalPlan,
    pub sink_id: String,
    pub encoder: SinkEncoderConfig,
    pub common: CommonSinkProps,
}

impl PhysicalStreamingEncoder {
    pub fn new(
        children: Vec<Arc<PhysicalPlan>>,
        index: i64,
        sink_id: String,
        encoder: SinkEncoderConfig,
        common: CommonSinkProps,
    ) -> Self {
        Self {
            base: BasePhysicalPlan::new(children, index),
            sink_id,
            encoder,
            common,
        }
    }
}

impl fmt::Debug for PhysicalStreamingEncoder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PhysicalStreamingEncoder")
            .field("index", &self.base.index())
            .field("sink_id", &self.sink_id)
            .finish()
    }
}
