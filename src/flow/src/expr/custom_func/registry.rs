use super::{ConcatFunc, CustomFunc};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(Debug, Clone, PartialEq)]
pub enum CustomFuncRegistryError {
    AlreadyRegistered(String),
}

/// Registry for scalar custom functions referenced in SQL (e.g. `concat(a, b)`).
pub struct CustomFuncRegistry {
    functions: RwLock<HashMap<String, Arc<dyn CustomFunc>>>,
}

impl CustomFuncRegistry {
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
        function: Arc<dyn CustomFunc>,
    ) -> Result<(), CustomFuncRegistryError> {
        let mut write = self
            .functions
            .write()
            .expect("custom func registry poisoned");
        let key = function.name().to_lowercase();
        if write.contains_key(&key) {
            return Err(CustomFuncRegistryError::AlreadyRegistered(key));
        }
        write.insert(key, function);
        Ok(())
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn CustomFunc>> {
        self.functions
            .read()
            .expect("custom func registry poisoned")
            .get(&name.to_lowercase())
            .cloned()
    }

    pub fn is_registered(&self, name: &str) -> bool {
        self.functions
            .read()
            .expect("custom func registry poisoned")
            .contains_key(&name.to_lowercase())
    }

    pub fn list_names(&self) -> Vec<String> {
        let mut names: Vec<_> = self
            .functions
            .read()
            .expect("custom func registry poisoned")
            .keys()
            .cloned()
            .collect();
        names.sort();
        names
    }

    fn register_builtin_functions(&self) {
        let _ = self.register_function(Arc::new(ConcatFunc));
    }
}

impl Default for CustomFuncRegistry {
    fn default() -> Self {
        let registry = Self::new();
        registry.register_builtin_functions();
        registry
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::func::EvalError;
    use datatypes::Value;

    #[derive(Debug)]
    struct DummyFn;

    impl CustomFunc for DummyFn {
        fn validate_row(&self, _args: &[Value]) -> Result<(), EvalError> {
            Ok(())
        }

        fn eval_row(&self, _args: &[Value]) -> Result<Value, EvalError> {
            Ok(Value::Null)
        }

        fn name(&self) -> &str {
            "dummy"
        }
    }

    #[test]
    fn register_and_resolve_custom_function() {
        let registry = CustomFuncRegistry::new();
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
        let registry = CustomFuncRegistry::new();
        registry
            .register_function(Arc::new(DummyFn))
            .expect("register");
        let err = registry
            .register_function(Arc::new(DummyFn))
            .expect_err("duplicate register should fail");
        assert_eq!(
            err,
            CustomFuncRegistryError::AlreadyRegistered("dummy".to_string())
        );
    }
}
