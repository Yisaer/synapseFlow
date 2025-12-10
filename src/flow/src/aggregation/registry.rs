use crate::aggregation::SumFunction;
use datatypes::{ConcreteDatatype, Value};
use parser::aggregate_registry::AggregateRegistry;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub trait AggregateAccumulator: Send {
    fn update(&mut self, args: &[Value]) -> Result<(), String>;
    fn finalize(&self) -> Value;
}

pub trait AggregateFunction: Send + Sync {
    fn name(&self) -> &str;
    fn return_type(&self, input_types: &[ConcreteDatatype]) -> Result<ConcreteDatatype, String>;
    fn create_accumulator(&self) -> Box<dyn AggregateAccumulator>;
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

    fn register_builtin_functions(&self) {
        self.register_function(Arc::new(SumFunction::new()));
    }
}

impl Default for AggregateFunctionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateRegistry for AggregateFunctionRegistry {
    fn is_aggregate_function(&self, name: &str) -> bool {
        self.is_registered(name)
    }
}
