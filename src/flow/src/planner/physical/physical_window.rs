use crate::planner::logical::TimeUnit;
use crate::planner::physical::{BasePhysicalPlan, PhysicalPlan};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct PhysicalTumblingWindow {
    pub base: BasePhysicalPlan,
    pub time_unit: TimeUnit,
    pub length: u64,
}

impl PhysicalTumblingWindow {
    pub fn new(
        time_unit: TimeUnit,
        length: u64,
        children: Vec<Arc<PhysicalPlan>>,
        index: i64,
    ) -> Self {
        let base = BasePhysicalPlan::new(children, index);
        Self {
            base,
            time_unit,
            length,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PhysicalCountWindow {
    pub base: BasePhysicalPlan,
    pub count: u64,
}

impl PhysicalCountWindow {
    pub fn new(count: u64, children: Vec<Arc<PhysicalPlan>>, index: i64) -> Self {
        let base = BasePhysicalPlan::new(children, index);
        Self { base, count }
    }
}

#[derive(Debug, Clone)]
pub struct PhysicalSlidingWindow {
    pub base: BasePhysicalPlan,
    pub time_unit: TimeUnit,
    pub lookback: u64,
    pub lookahead: Option<u64>,
}

impl PhysicalSlidingWindow {
    pub fn new(
        time_unit: TimeUnit,
        lookback: u64,
        lookahead: Option<u64>,
        children: Vec<Arc<PhysicalPlan>>,
        index: i64,
    ) -> Self {
        let base = BasePhysicalPlan::new(children, index);
        Self {
            base,
            time_unit,
            lookback,
            lookahead,
        }
    }
}
