use crate::planner::physical::output_layout::OutputLayout;
use crate::planner::physical::BasePhysicalPlan;
use crate::planner::sink::{CommonSinkProps, SinkEncoderConfig};
use std::fmt;
use std::sync::Arc;

use super::PhysicalPlan;

/// Physical node representing a fused sink encoder that combines batching and encoding.
///
/// This node is produced by the `StreamingEncoderRewrite` optimizer rule when
/// a `PhysicalBatch` → `PhysicalSinkEncoder` chain is detected and the encoder
/// supports streaming delivery. The batch parameters are moved from the
/// `PhysicalBatch` node into this fused node.
#[derive(Clone)]
pub struct PhysicalIncSinkEncoder {
    pub base: BasePhysicalPlan,
    pub sink_id: String,
    pub encoder: SinkEncoderConfig,
    pub common: CommonSinkProps,
    pub output_layout: Option<Arc<OutputLayout>>,
}

impl PhysicalIncSinkEncoder {
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
            output_layout: None,
        }
    }
}

impl fmt::Debug for PhysicalIncSinkEncoder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PhysicalIncSinkEncoder")
            .field("index", &self.base.index())
            .field("sink_id", &self.sink_id)
            .field("batch_count", &self.common.batch_count)
            .field(
                "batch_duration_ms",
                &self.common.batch_duration.map(|dur| dur.as_millis() as u64),
            )
            .finish()
    }
}
