use std::sync::Arc;

pub trait StatefulRegistry: Send + Sync {
    fn is_stateful_function(&self, name: &str) -> bool;
}

pub const BUILTIN_STATEFUL_FUNCTIONS: [&str; 13] = [
    "acc_avg",
    "acc_count",
    "acc_max",
    "acc_min",
    "acc_sum",
    "change_capture",
    "change_to",
    "changed_col",
    "consecutive_count",
    "consecutive_start",
    "had_changed",
    "lag",
    "latest",
];

pub fn builtin_stateful_function_names() -> &'static [&'static str] {
    &BUILTIN_STATEFUL_FUNCTIONS
}

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
    Arc::new(StaticStatefulRegistry::new(
        builtin_stateful_function_names().iter().copied(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_registry_includes_builtin_stateful_functions() {
        let registry = default_stateful_registry();
        assert!(registry.is_stateful_function("change_capture"));
        assert!(registry.is_stateful_function("change_to"));
        assert!(registry.is_stateful_function("changed_col"));
        assert!(registry.is_stateful_function("consecutive_count"));
        assert!(registry.is_stateful_function("consecutive_start"));
        assert!(registry.is_stateful_function("had_changed"));
        assert!(registry.is_stateful_function("lag"));
        assert!(registry.is_stateful_function("latest"));
        assert!(registry.is_stateful_function("acc_sum"));
        assert!(registry.is_stateful_function("LAG"));
        assert!(!registry.is_stateful_function("missing"));
    }
}
