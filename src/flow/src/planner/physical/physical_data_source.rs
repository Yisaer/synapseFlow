use crate::planner::physical::BasePhysicalPlan;
use datatypes::Schema;
use std::sync::Arc;

/// Physical operator for reading data from a data source
///
/// This is typically a leaf node in the physical plan tree that represents
/// the source of data for stream processing (e.g., a Kafka topic, file, etc.)
#[derive(Debug, Clone)]
pub struct PhysicalDataSource {
    pub base: BasePhysicalPlan,
    pub source_name: String,
    pub alias: Option<String>,
    pub schema: Arc<Schema>,
}

impl PhysicalDataSource {
    /// Create a new PhysicalDataSource
    pub fn new(
        source_name: String,
        alias: Option<String>,
        schema: Arc<Schema>,
        index: i64,
    ) -> Self {
        let base = BasePhysicalPlan::new_leaf(index);
        Self {
            base,
            source_name,
            alias,
            schema,
        }
    }

    pub fn source_name(&self) -> &str {
        &self.source_name
    }

    pub fn alias(&self) -> Option<&str> {
        self.alias.as_deref()
    }

    pub fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }
}
