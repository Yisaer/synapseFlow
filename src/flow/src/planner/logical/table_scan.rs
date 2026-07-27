use crate::catalog::{StreamDecoderConfig, TableProps, TableType};
use crate::planner::logical::BaseLogicalPlan;
use datatypes::Schema;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct TableScanRequest {
    pub batch_size: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct TableScanSpec {
    pub table_name: String,
    pub table_type: TableType,
    pub alias: Option<String>,
    pub decoder: StreamDecoderConfig,
    pub schema: Arc<Schema>,
    pub props: TableProps,
    pub request: TableScanRequest,
}

#[derive(Debug, Clone)]
pub struct TableScan {
    pub base: BaseLogicalPlan,
    pub table_name: String,
    pub table_type: TableType,
    pub alias: Option<String>,
    pub decoder: StreamDecoderConfig,
    pub schema: Arc<Schema>,
    pub props: TableProps,
    pub request: TableScanRequest,
}

impl TableScan {
    pub fn new(spec: TableScanSpec, index: i64) -> Self {
        Self {
            base: BaseLogicalPlan::new(vec![], index),
            table_name: spec.table_name,
            table_type: spec.table_type,
            alias: spec.alias,
            decoder: spec.decoder,
            schema: spec.schema,
            props: spec.props,
            request: spec.request,
        }
    }

    pub fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }
}
