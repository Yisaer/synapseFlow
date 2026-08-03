use crate::planner::logical::TimeUnit;
use crate::planner::physical::{BasePhysicalPlan, PhysicalPlan};
use std::sync::Arc;
use std::time::Duration;

/// Watermark configuration per downstream window/operator kind.
#[derive(Debug, Clone)]
pub enum WatermarkConfig {
    Tumbling {
        time_unit: TimeUnit,
        length: u64,
    },
    Sliding {
        time_unit: TimeUnit,
        lookback: u64,
        lookahead: Option<u64>,
    },
}

impl WatermarkConfig {
    pub fn interval_duration(&self) -> Duration {
        match self {
            WatermarkConfig::Tumbling { time_unit, length } => match time_unit {
                TimeUnit::Seconds => Duration::from_secs((*length).max(1)),
            },
            WatermarkConfig::Sliding { time_unit, .. } => match time_unit {
                TimeUnit::Seconds => Duration::from_secs(1),
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct PhysicalWatermark {
    pub base: BasePhysicalPlan,
    pub config: WatermarkConfig,
}

impl PhysicalWatermark {
    pub fn new(config: WatermarkConfig, children: Vec<Arc<PhysicalPlan>>, index: i64) -> Self {
        let base = BasePhysicalPlan::new(children, index);
        Self { base, config }
    }
}
