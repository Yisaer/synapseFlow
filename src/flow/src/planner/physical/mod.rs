use std::sync::Arc;

pub mod base_physical;
pub mod physical_data_sink;
pub mod physical_data_source;
pub mod physical_filter;
pub mod physical_project;
pub mod physical_shared_stream;

pub use base_physical::BasePhysicalPlan;
pub use physical_data_sink::PhysicalDataSink;
pub use physical_data_source::PhysicalDataSource;
pub use physical_filter::PhysicalFilter;
pub use physical_project::{PhysicalProject, PhysicalProjectField};
pub use physical_shared_stream::PhysicalSharedStream;

/// Enum describing all supported physical execution nodes
#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    DataSource(PhysicalDataSource),
    Filter(PhysicalFilter),
    Project(PhysicalProject),
    SharedStream(PhysicalSharedStream),
    DataSink(PhysicalDataSink),
}

impl PhysicalPlan {
    /// Get the children of this physical plan
    pub fn children(&self) -> &[Arc<PhysicalPlan>] {
        match self {
            PhysicalPlan::DataSource(plan) => plan.base.children(),
            PhysicalPlan::Filter(plan) => plan.base.children(),
            PhysicalPlan::Project(plan) => plan.base.children(),
            PhysicalPlan::SharedStream(plan) => plan.base.children(),
            PhysicalPlan::DataSink(plan) => plan.base.children(),
        }
    }

    /// Get the type name of this physical plan
    pub fn get_plan_type(&self) -> &str {
        match self {
            PhysicalPlan::DataSource(_) => "PhysicalDataSource",
            PhysicalPlan::Filter(_) => "PhysicalFilter",
            PhysicalPlan::Project(_) => "PhysicalProject",
            PhysicalPlan::SharedStream(_) => "PhysicalSharedStream",
            PhysicalPlan::DataSink(_) => "PhysicalDataSink",
        }
    }

    /// Get the unique index of this physical plan
    pub fn get_plan_index(&self) -> i64 {
        match self {
            PhysicalPlan::DataSource(plan) => plan.base.index(),
            PhysicalPlan::Filter(plan) => plan.base.index(),
            PhysicalPlan::Project(plan) => plan.base.index(),
            PhysicalPlan::SharedStream(plan) => plan.base.index(),
            PhysicalPlan::DataSink(plan) => plan.base.index(),
        }
    }
}
