use super::base_physical::BasePhysicalPlan;
use crate::planner::physical::PhysicalPlan;
use crate::processor::SamplingStrategy;
use std::any::Any;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

/// Physical plan node for throttling (rate limiting) a stream.
#[derive(Clone)]
pub struct PhysicalSampler {
    pub base: BasePhysicalPlan,
    pub interval: Duration,
    pub strategy: SamplingStrategy,
    schema_artifact: Option<Arc<dyn Any + Send + Sync>>,
}

impl fmt::Debug for PhysicalSampler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PhysicalSampler")
            .field("base", &self.base)
            .field("interval", &self.interval)
            .field("strategy", &self.strategy)
            .field(
                "schema_artifact",
                &self.schema_artifact.as_ref().map(|_| "<artifact>"),
            )
            .finish()
    }
}

impl PhysicalSampler {
    pub fn new(
        interval: Duration,
        strategy: SamplingStrategy,
        schema_artifact: Option<Arc<dyn Any + Send + Sync>>,
        children: Vec<Arc<PhysicalPlan>>,
        index: i64,
    ) -> Self {
        Self {
            base: BasePhysicalPlan::new(children, index),
            interval,
            strategy,
            schema_artifact,
        }
    }

    pub fn schema_artifact(&self) -> Option<Arc<dyn Any + Send + Sync>> {
        self.schema_artifact.clone()
    }
}
