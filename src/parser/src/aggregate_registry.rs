use std::collections::HashSet;
use std::sync::Arc;

pub trait AggregateRegistry: Send + Sync {
    fn is_aggregate_function(&self, name: &str) -> bool;
}

#[derive(Default, Clone)]
pub struct StaticAggregateRegistry {
    functions: HashSet<String>,
}

impl StaticAggregateRegistry {
    pub fn new(functions: impl IntoIterator<Item = impl Into<String>>) -> Self {
        let lowered = functions
            .into_iter()
            .map(|f| f.into().to_lowercase())
            .collect::<HashSet<_>>();
        Self { functions: lowered }
    }
}

impl AggregateRegistry for StaticAggregateRegistry {
    fn is_aggregate_function(&self, name: &str) -> bool {
        self.functions.contains(&name.to_lowercase())
    }
}

pub fn default_aggregate_registry() -> Arc<dyn AggregateRegistry> {
    Arc::new(StaticAggregateRegistry::new(["sum"]))
}
