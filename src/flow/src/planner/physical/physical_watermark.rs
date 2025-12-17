use crate::planner::logical::TimeUnit;
use crate::planner::physical::{BasePhysicalPlan, PhysicalPlan};
use std::sync::Arc;
use std::time::Duration;

/// Watermark emission strategy.
#[derive(Debug, Clone)]
pub enum WatermarkStrategy {
    /// Emit watermarks based on wall clock/ticker.
    ProcessingTime { time_unit: TimeUnit, interval: u64 },
    /// Forward upstream watermarks (event time).
    External,
}

impl WatermarkStrategy {
    pub fn interval_duration(&self) -> Option<Duration> {
        match self {
            WatermarkStrategy::ProcessingTime {
                time_unit,
                interval,
            } => match time_unit {
                TimeUnit::Seconds => Some(Duration::from_secs(*interval)),
            },
            WatermarkStrategy::External => None,
        }
    }
}

/// Watermark configuration per downstream window/operator kind.
#[derive(Debug, Clone)]
pub enum WatermarkConfig {
    Tumbling {
        time_unit: TimeUnit,
        length: u64,
        strategy: WatermarkStrategy,
    },
    Sliding {
        time_unit: TimeUnit,
        lookback: u64,
        lookahead: Option<u64>,
        strategy: WatermarkStrategy,
    },
}

impl WatermarkConfig {
    pub fn strategy(&self) -> &WatermarkStrategy {
        match self {
            WatermarkConfig::Tumbling { strategy, .. } => strategy,
            WatermarkConfig::Sliding { strategy, .. } => strategy,
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
