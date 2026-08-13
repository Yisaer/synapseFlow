use crate::planner::decode_projection::DecodeProjection;
use crate::planner::physical::{BasePhysicalPlan, DataDomain};
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
    pub schema: Arc<Schema>,
    decode_projection: Option<DecodeProjection>,
    output_domain: DataDomain,
}

impl PhysicalDataSource {
    /// Create a new PhysicalDataSource
    pub fn new(
        source_name: String,
        schema: Arc<Schema>,
        decode_projection: Option<DecodeProjection>,
        index: i64,
    ) -> Self {
        let base = BasePhysicalPlan::new_leaf(index);
        Self {
            base,
            source_name,
            schema,
            decode_projection,
            output_domain: DataDomain::Message,
        }
    }

    pub(crate) fn with_output_domain(mut self, output_domain: DataDomain) -> Self {
        self.output_domain = output_domain;
        self
    }

    pub(crate) fn output_domain(&self) -> DataDomain {
        self.output_domain
    }

    pub fn source_name(&self) -> &str {
        &self.source_name
    }

    pub fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    pub fn decode_projection(&self) -> Option<&DecodeProjection> {
        self.decode_projection.as_ref()
    }
}
