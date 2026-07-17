use crate::planner::physical::{BasePhysicalPlan, PhysicalPlan};
use std::fmt;
use std::sync::Arc;

/// Planner-only node that narrows the visible output layout for one sink branch.
#[derive(Clone)]
pub struct PhysicalColumnFilter {
    pub base: BasePhysicalPlan,
    pub sink_id: String,
    /// If set, only these columns are emitted.
    pub include_columns: Option<Vec<String>>,
    /// If set, all columns except these are emitted.
    pub exclude_columns: Option<Vec<String>>,
}

impl PhysicalColumnFilter {
    pub fn new(
        children: Vec<Arc<PhysicalPlan>>,
        index: i64,
        sink_id: String,
        include_columns: Option<Vec<String>>,
        exclude_columns: Option<Vec<String>>,
    ) -> Self {
        Self {
            base: BasePhysicalPlan::new(children, index),
            sink_id,
            include_columns,
            exclude_columns,
        }
    }
}

impl fmt::Debug for PhysicalColumnFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("PhysicalColumnFilter");
        debug.field("index", &self.base.index());
        debug.field("sink_id", &self.sink_id);
        if let Some(include) = &self.include_columns {
            debug.field("include_columns", include);
        }
        if let Some(exclude) = &self.exclude_columns {
            debug.field("exclude_columns", exclude);
        }
        debug.finish()
    }
}
