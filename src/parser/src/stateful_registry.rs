use std::sync::Arc;

pub trait StatefulRegistry: Send + Sync {
    fn is_stateful_function(&self, name: &str) -> bool;
}

const BUILTIN_STATEFUL_FUNCTIONS: [&str; 1] = ["lag"];

#[derive(Default)]
pub struct StaticStatefulRegistry {
    names: std::collections::HashSet<String>,
}

impl StaticStatefulRegistry {
    pub fn new(names: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            names: names.into_iter().map(|s| s.into().to_lowercase()).collect(),
        }
    }
}

impl StatefulRegistry for StaticStatefulRegistry {
    fn is_stateful_function(&self, name: &str) -> bool {
        self.names.contains(&name.to_lowercase())
    }
}

pub fn default_stateful_registry() -> Arc<dyn StatefulRegistry> {
    Arc::new(StaticStatefulRegistry::new(BUILTIN_STATEFUL_FUNCTIONS))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_registry_includes_builtin_stateful_functions() {
        let registry = default_stateful_registry();
        assert!(registry.is_stateful_function("lag"));
        assert!(registry.is_stateful_function("LAG"));
        assert!(!registry.is_stateful_function("missing"));
    }
}
