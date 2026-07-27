use crate::catalog::{StreamDecoderConfig, TableProps, TableType};
use crate::planner::logical::table_scan::TableScanRequest;
use crate::planner::physical::BasePhysicalPlan;
use datatypes::Schema;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct PhysicalTableScanSpec {
    pub table_name: String,
    pub table_type: TableType,
    pub decoder: StreamDecoderConfig,
    pub schema: Arc<Schema>,
    pub props: TableProps,
    pub request: TableScanRequest,
}

#[derive(Debug, Clone)]
pub struct PhysicalTableScan {
    pub base: BasePhysicalPlan,
    table_name: String,
    table_type: TableType,
    decoder: StreamDecoderConfig,
    schema: Arc<Schema>,
    props: TableProps,
    request: TableScanRequest,
}

impl PhysicalTableScan {
    pub fn new(spec: PhysicalTableScanSpec, index: i64) -> Self {
        Self {
            base: BasePhysicalPlan::new_leaf(index),
            table_name: spec.table_name,
            table_type: spec.table_type,
            decoder: spec.decoder,
            schema: spec.schema,
            props: spec.props,
            request: spec.request,
        }
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn table_type(&self) -> TableType {
        self.table_type
    }

    pub fn decoder(&self) -> &StreamDecoderConfig {
        &self.decoder
    }

    pub fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    pub fn props(&self) -> &TableProps {
        &self.props
    }

    pub fn request(&self) -> &TableScanRequest {
        &self.request
    }
}
