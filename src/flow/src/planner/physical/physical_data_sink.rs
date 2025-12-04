use crate::planner::physical::BasePhysicalPlan;
use crate::planner::sink::SinkConnectorConfig;
use std::fmt;
use std::sync::Arc;

use super::PhysicalPlan;

/// Physical plan node for sink stage.
#[derive(Clone)]
pub struct PhysicalDataSink {
    pub base: BasePhysicalPlan,
    pub connector: PhysicalSinkConnector,
}

impl PhysicalDataSink {
    pub fn new(child: Arc<PhysicalPlan>, index: i64, connector: PhysicalSinkConnector) -> Self {
        Self {
            base: BasePhysicalPlan::new(vec![child], index),
            connector,
        }
    }
}

impl fmt::Debug for PhysicalDataSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PhysicalDataSink")
            .field("index", &self.base.index())
            .field("sink_id", &self.connector.sink_id)
            .finish()
    }
}

/// Declarative description of a sink connector bound to an encoder node.
#[derive(Clone)]
pub struct PhysicalSinkConnector {
    pub sink_id: String,
    pub forward_to_result: bool,
    pub connector: SinkConnectorConfig,
    pub encoder_plan_index: i64,
}

impl PhysicalSinkConnector {
    pub fn new(
        sink_id: String,
        forward_to_result: bool,
        connector: SinkConnectorConfig,
        encoder_plan_index: i64,
    ) -> Self {
        Self {
            sink_id,
            forward_to_result,
            connector,
            encoder_plan_index,
        }
    }
}
