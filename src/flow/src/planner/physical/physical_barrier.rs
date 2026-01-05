use crate::planner::physical::BasePhysicalPlan;
use std::sync::Arc;

use super::PhysicalPlan;

/// Physical plan node that aligns barrier control signals across multiple upstream children.
///
/// This node is inserted during physical plan optimization for fan-in nodes (`children.len() > 1`)
/// and will later be translated into a dedicated processor responsible for barrier alignment.
#[derive(Debug, Clone)]
pub struct PhysicalBarrier {
    pub base: BasePhysicalPlan,
}

impl PhysicalBarrier {
    pub fn new(children: Vec<Arc<PhysicalPlan>>, index: i64) -> Self {
        Self {
            base: BasePhysicalPlan::new(children, index),
        }
    }
}
