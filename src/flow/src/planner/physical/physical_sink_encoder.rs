use crate::planner::physical::BasePhysicalPlan;
use crate::planner::physical::ByIndexProjection;
use crate::planner::sink::{CommonSinkProps, SinkEncoderConfig};
use std::fmt;
use std::sync::Arc;

use super::PhysicalPlan;

/// Physical node representing the sink-side encoding and delivery boundary.
#[derive(Clone)]
pub struct PhysicalSinkEncoder {
    pub base: BasePhysicalPlan,
    pub sink_id: String,
    pub encoder: SinkEncoderConfig,
    pub common: CommonSinkProps,
    pub by_index_projection: Option<Arc<ByIndexProjection>>,
}

impl PhysicalSinkEncoder {
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
            by_index_projection: None,
        }
    }
}

impl fmt::Debug for PhysicalSinkEncoder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PhysicalSinkEncoder")
            .field("index", &self.base.index())
            .field("sink_id", &self.sink_id)
            .finish()
    }
}
