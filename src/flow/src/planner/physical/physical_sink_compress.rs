use crate::codec::CompressionCodec;
use crate::planner::physical::BasePhysicalPlan;
use std::fmt;
use std::sync::Arc;

use super::PhysicalPlan;

/// Physical node for the sink delivery compression transform.
///
/// Sits between `PhysicalSinkEncoder` (or connector input) and
/// `PhysicalSinkConnector`, compressing each encoded delivery stream.
#[derive(Clone)]
pub struct PhysicalSinkCompress {
    pub base: BasePhysicalPlan,
    pub codec: CompressionCodec,
}

impl PhysicalSinkCompress {
    pub fn new(child: Arc<PhysicalPlan>, index: i64, codec: CompressionCodec) -> Self {
        Self {
            base: BasePhysicalPlan::new(vec![child], index),
            codec,
        }
    }
}

impl fmt::Debug for PhysicalSinkCompress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PhysicalSinkCompress")
            .field("index", &self.base.index())
            .field("codec", &self.codec.kind_str())
            .finish()
    }
}
