use datatypes::{ConcreteDatatype, Value};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use super::lag::LagFunction;

pub trait StatefulFunctionInstance: Send + Sync {
    fn eval(&mut self, args: &[Value]) -> Result<Value, String>;
}

pub trait StatefulFunction: Send + Sync {
    fn name(&self) -> &str;
    fn return_type(&self, input_types: &[ConcreteDatatype]) -> Result<ConcreteDatatype, String>;
    fn create_instance(&self) -> Box<dyn StatefulFunctionInstance>;
}

#[derive(Debug, Clone, PartialEq)]
pub enum StatefulRegistryError {
    AlreadyRegistered(String),
}

pub struct StatefulFunctionRegistry {
    functions: RwLock<HashMap<String, Arc<dyn StatefulFunction>>>,
}

impl StatefulFunctionRegistry {
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

    pub fn register_function(
        &self,
        function: Arc<dyn StatefulFunction>,
    ) -> Result<(), StatefulRegistryError> {
        let mut write = self
            .functions
            .write()
            .expect("stateful function registry poisoned");
        let key = function.name().to_lowercase();
        if write.contains_key(&key) {
            return Err(StatefulRegistryError::AlreadyRegistered(key));
        }
        write.insert(key, function);
        Ok(())
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn StatefulFunction>> {
        self.functions
            .read()
            .expect("stateful function registry poisoned")
            .get(&name.to_lowercase())
            .cloned()
    }

    pub fn is_registered(&self, name: &str) -> bool {
        self.functions
            .read()
            .expect("stateful function registry poisoned")
            .contains_key(&name.to_lowercase())
    }

    fn register_builtin_functions(&self) {
        let _ = self.register_function(Arc::new(LagFunction::new()));
    }
}

impl Default for StatefulFunctionRegistry {
    fn default() -> Self {
        let registry = Self::new();
        registry.register_builtin_functions();
        registry
    }
}

impl parser::StatefulRegistry for StatefulFunctionRegistry {
    fn is_stateful_function(&self, name: &str) -> bool {
        self.is_registered(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datatypes::types;

    struct DummyFn;

    impl StatefulFunction for DummyFn {
        fn name(&self) -> &str {
            "dummy"
        }

        fn return_type(
            &self,
            _input_types: &[ConcreteDatatype],
        ) -> Result<ConcreteDatatype, String> {
            Ok(ConcreteDatatype::Int64(types::Int64Type))
        }

        fn create_instance(&self) -> Box<dyn StatefulFunctionInstance> {
            struct Inst;
            impl StatefulFunctionInstance for Inst {
                fn eval(&mut self, _args: &[Value]) -> Result<Value, String> {
                    Ok(Value::Null)
                }
            }
            Box::new(Inst)
        }
    }

    #[test]
    fn register_and_resolve_stateful_function() {
        let registry = StatefulFunctionRegistry::new();
        assert!(!registry.is_registered("dummy"));

        registry
            .register_function(Arc::new(DummyFn))
            .expect("register");
        assert!(registry.is_registered("dummy"));
        assert!(registry.get("dummy").is_some());
        assert!(registry.get("DuMmY").is_some());
        assert!(registry.get("missing").is_none());
    }

    #[test]
    fn reject_duplicate_registration() {
        let registry = StatefulFunctionRegistry::new();
        registry
            .register_function(Arc::new(DummyFn))
            .expect("register");
        let err = registry
            .register_function(Arc::new(DummyFn))
            .expect_err("duplicate register should fail");
        assert_eq!(
            err,
            StatefulRegistryError::AlreadyRegistered("dummy".to_string())
        );
    }
}
