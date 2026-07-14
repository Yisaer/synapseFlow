use crate::planner::physical::{BasePhysicalPlan, PhysicalPlan};
use std::fmt;
use std::sync::Arc;

/// Per-column metadata for column filtering.
/// Resolved to (msg_index, key_index) at runtime on first tuple.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnFilterKeepSpec {
    pub source_name: Arc<str>,
    pub column_name: Arc<str>,
    pub output_name: Arc<str>,
}

/// Physical plan node that filters columns per sink branch.
///
/// Transparent in consumer map (passes through upstream consumers) so that
/// existing by-index projection rewrite rules continue to fire on the shared
/// upstream Project.
#[derive(Clone)]
pub struct PhysicalColumnFilter {
    pub base: BasePhysicalPlan,
    pub sink_id: String,
    /// If set, only these columns are emitted.
    pub include_columns: Option<Vec<String>>,
    /// If set, all columns except these are emitted.
    pub exclude_columns: Option<Vec<String>>,
    /// Per-column resolution metadata, computed at plan-build time from the
    /// child's output schema. Runtime uses this to build index caches on
    /// first tuple, then performs zero-name-match column reads thereafter.
    pub keep_specs: Vec<ColumnFilterKeepSpec>,
}

impl PhysicalColumnFilter {
    pub fn new(
        children: Vec<Arc<PhysicalPlan>>,
        index: i64,
        sink_id: String,
        include_columns: Option<Vec<String>>,
        exclude_columns: Option<Vec<String>>,
        keep_specs: Vec<ColumnFilterKeepSpec>,
    ) -> Self {
        Self {
            base: BasePhysicalPlan::new(children, index),
            sink_id,
            include_columns,
            exclude_columns,
            keep_specs,
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
