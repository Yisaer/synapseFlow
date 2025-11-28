use crate::planner::logical::BaseLogicalPlan;

#[derive(Debug, Clone)]
pub struct DataSource {
    pub base: BaseLogicalPlan,
    pub source_name: String,
    pub alias: Option<String>,
}

impl DataSource {
    pub fn new(source_name: String, alias: Option<String>, index: i64) -> Self {
        let base = BaseLogicalPlan::new(vec![], index);
        Self {
            base,
            source_name,
            alias,
        }
    }
}
