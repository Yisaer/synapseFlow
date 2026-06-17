pub mod decode_projection;
pub mod explain;
pub mod logical;
pub mod logical_optimizer;
pub mod logical_plan_ir;
pub mod optimizer;
pub mod physical;
pub mod physical_plan_builder;
pub(crate) mod shared_stream_plan;
pub mod sink;

pub use logical_optimizer::{
    optimize_logical_plan, optimize_logical_plan_with_options, LogicalOptimizerOptions,
};
pub use optimizer::optimize_physical_plan;
pub use physical_plan_builder::{
    apply_shared_stream_slot_schemas, create_physical_plan,
    create_physical_plan_with_build_options, read_shared_stream_slot_schemas,
    PhysicalPlanBuildOptions,
};
