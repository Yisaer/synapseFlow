use crate::planner::physical::output_layout::OutputLayout;
use crate::planner::physical::{BasePhysicalPlan, PhysicalPlan};
use crate::planner::sink::SinkOutputConfig;
use std::fmt;
use std::sync::Arc;

/// Physical plan node describing sink-side row diff output preparation.
#[derive(Clone)]
pub struct PhysicalRowDiff {
    pub base: BasePhysicalPlan,
    pub sink_id: String,
    pub output: SinkOutputConfig,
    pub input_layout: Arc<OutputLayout>,
    pub output_layout: Arc<OutputLayout>,
    pub tracked_columns: Arc<[Arc<str>]>,
    pub tracked_column_indexes: Arc<[usize]>,
}

impl PhysicalRowDiff {
    pub fn new(
        children: Vec<Arc<PhysicalPlan>>,
        index: i64,
        sink_id: String,
        output: SinkOutputConfig,
        input_layout: OutputLayout,
        tracked_columns: Vec<Arc<str>>,
        tracked_column_indexes: Vec<usize>,
    ) -> Self {
        let output_layout = input_layout.materialized();
        Self {
            base: BasePhysicalPlan::new(children, index),
            sink_id,
            output,
            input_layout: Arc::new(input_layout),
            output_layout: Arc::new(output_layout),
            tracked_columns: Arc::from(tracked_columns),
            tracked_column_indexes: Arc::from(tracked_column_indexes),
        }
    }
}

impl fmt::Debug for PhysicalRowDiff {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PhysicalRowDiff")
            .field("index", &self.base.index())
            .field("sink_id", &self.sink_id)
            .field("output", &self.output)
            .field("tracked_columns", &self.tracked_columns)
            .finish()
    }
}
