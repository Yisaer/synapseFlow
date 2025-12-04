use crate::planner::physical::BasePhysicalPlan;
use crate::planner::sink::SinkEncoderConfig;
use std::fmt;
use std::sync::Arc;

use super::PhysicalPlan;

/// Physical plan node describing encoder stage before a sink connector.
/// 
/// Note: connector_id has been removed as it's not needed for processor identification.
/// Processor IDs are now generated using only sink_id and physical_plan_name.
#[derive(Clone)]
pub struct PhysicalEncoder {
    pub base: BasePhysicalPlan,
    pub sink_id: String,
    pub encoder: SinkEncoderConfig,
}

impl PhysicalEncoder {
    pub fn new(
        children: Vec<Arc<PhysicalPlan>>,
        index: i64,
        sink_id: String,
        encoder: SinkEncoderConfig,
    ) -> Self {
        Self {
            base: BasePhysicalPlan::new(children, index),
            sink_id,
            encoder,
        }
    }
}

impl fmt::Debug for PhysicalEncoder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PhysicalEncoder")
            .field("index", &self.base.index())
            .field("sink_id", &self.sink_id)
            .finish()
    }
}
