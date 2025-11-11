use std::sync::Arc;
use std::fmt::Debug;

pub trait LogicalPlan: Send + Sync + Debug {
    fn children(&self) -> &[Arc<dyn LogicalPlan>];
    fn get_plan_type(&self) -> &str;
    fn get_plan_index(&self) -> &i64;
}

#[derive(Debug, Clone)]
pub struct BaseLogicalPlan {
    pub index :i64,
    pub children: Vec<Arc<dyn LogicalPlan>>,
}

impl BaseLogicalPlan {
    pub fn new(children: Vec<Arc<dyn LogicalPlan>>,index: i64) -> Self {
        Self { children ,index}
    }
}

pub mod datasource;
pub mod project;
pub mod filter;

pub use datasource::DataSource;
pub use project::Project;
pub use filter::Filter;
