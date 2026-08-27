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
            WatermarkConfig::Tumbling { time_unit, length } => time_unit.duration((*length).max(1)),
            // Sliding-window deadline watermarks are emitted per tuple via a heap of sleeps; the
            // periodic ticker only advances wall-clock time to drive downstream GC. Keep it fixed
            // at one second regardless of the window unit.
            WatermarkConfig::Sliding { .. } => Duration::from_secs(1),
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
