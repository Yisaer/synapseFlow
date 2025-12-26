use crate::aggregation::{LastRowFunction, SumFunction};
use datatypes::{ConcreteDatatype, Value};
use parser::aggregate_registry::AggregateRegistry;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub trait AggregateAccumulator: Send + Sync {
    fn update(&mut self, args: &[Value]) -> Result<(), String>;
    fn finalize(&self) -> Value;
}

pub trait AggregateFunction: Send + Sync {
    fn name(&self) -> &str;
    fn return_type(&self, input_types: &[ConcreteDatatype]) -> Result<ConcreteDatatype, String>;
    fn create_accumulator(&self) -> Box<dyn AggregateAccumulator>;
    /// Whether this aggregate supports incremental (streaming) updates.
    fn supports_incremental(&self) -> bool {
        false
    }
}

pub struct AggregateFunctionRegistry {
    functions: RwLock<HashMap<String, Arc<dyn AggregateFunction>>>,
}

impl AggregateFunctionRegistry {
    pub fn new() -> Self {
        Self {
            functions: RwLock::new(HashMap::new()),
        }
    }

    pub fn with_builtins() -> Arc<Self> {
        let registry = Arc::new(Self::new());
        registry.register_builtin_functions();
        registry
    }

    pub fn register_function(&self, function: Arc<dyn AggregateFunction>) {
        self.functions
            .write()
            .expect("aggregate function registry poisoned")
            .insert(function.name().to_lowercase(), function);
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn AggregateFunction>> {
        self.functions
            .read()
            .expect("aggregate function registry poisoned")
            .get(&name.to_lowercase())
            .cloned()
    }

    pub fn is_registered(&self, name: &str) -> bool {
        self.functions
            .read()
            .expect("aggregate function registry poisoned")
            .contains_key(&name.to_lowercase())
    }

    /// Check if the given aggregate function supports incremental updates.
    pub fn supports_incremental(&self, name: &str) -> bool {
        self.functions
            .read()
            .expect("aggregate function registry poisoned")
            .get(&name.to_lowercase())
            .map(|f| f.supports_incremental())
            .unwrap_or(false)
    }

    fn register_builtin_functions(&self) {
        self.register_function(Arc::new(SumFunction::new()));
        self.register_function(Arc::new(LastRowFunction::new()));
    }
}

impl Default for AggregateFunctionRegistry {
    fn default() -> Self {
        let registry = Self::new();
        registry.register_builtin_functions();
        registry
    }
}

impl AggregateRegistry for AggregateFunctionRegistry {
    fn is_aggregate_function(&self, name: &str) -> bool {
        self.is_registered(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datatypes::types;

    struct DummyAgg;
    impl AggregateFunction for DummyAgg {
        fn name(&self) -> &str {
            "dummy"
        }
        fn return_type(
            &self,
            _input_types: &[ConcreteDatatype],
        ) -> Result<ConcreteDatatype, String> {
            Ok(ConcreteDatatype::Int64(types::Int64Type))
        }
        fn create_accumulator(&self) -> Box<dyn AggregateAccumulator> {
            struct Acc;
            impl AggregateAccumulator for Acc {
                fn update(&mut self, _args: &[Value]) -> Result<(), String> {
                    Ok(())
                }
                fn finalize(&self) -> Value {
                    Value::Null
                }
            }
            Box::new(Acc)
        }
        fn supports_incremental(&self) -> bool {
            false
        }
    }

    #[test]
    fn test_supports_incremental_lookup() {
        let registry = AggregateFunctionRegistry::new();
        registry.register_function(Arc::new(SumFunction::new()));
        registry.register_function(Arc::new(DummyAgg));

        assert!(registry.supports_incremental("sum"));
        assert!(!registry.supports_incremental("dummy"));
        assert!(!registry.supports_incremental("nonexistent"));
    }
}
