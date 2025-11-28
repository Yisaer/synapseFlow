use crate::planner::physical::BasePhysicalPlan;
use datatypes::Schema;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct PhysicalSharedStream {
    pub base: BasePhysicalPlan,
    stream_name: String,
    alias: Option<String>,
    schema: Arc<Schema>,
}

impl PhysicalSharedStream {
    pub fn new(
        stream_name: String,
        alias: Option<String>,
        schema: Arc<Schema>,
        index: i64,
    ) -> Self {
        let base = BasePhysicalPlan::new_leaf(index);
        Self {
            base,
            stream_name,
            alias,
            schema,
        }
    }

    pub fn stream_name(&self) -> &str {
        &self.stream_name
    }

    pub fn alias(&self) -> Option<&str> {
        self.alias.as_deref()
    }

    pub fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }
}
