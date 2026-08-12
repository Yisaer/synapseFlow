use datatypes::{ConcreteDatatype, Value};
use parking_lot::RwLock;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use super::{
    AccAvgFunction, AccCountFunction, AccMaxFunction, AccMinFunction, AccSumFunction,
    ChangeCaptureFunction, ChangeToFunction, ChangedColFunction, ConsecutiveCountFunction,
    ConsecutiveStartFunction, HadChangedFunction, LagFunction, LatestFunction,
};

pub struct StatefulEvalInput<'a> {
    pub args: &'a [Value],
    pub should_apply: bool,
}

pub struct PreparedStatefulEval {
    pub output: Value,
    pub update: Box<dyn StatefulEvalUpdate>,
}

impl PreparedStatefulEval {
    pub fn new(output: Value, update: Box<dyn StatefulEvalUpdate>) -> Self {
        Self { output, update }
    }
}

pub trait StatefulEvalUpdate: Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

pub trait StatefulFunctionInstance: Send + Sync {
    fn prepare_eval(&self, input: StatefulEvalInput<'_>) -> Result<PreparedStatefulEval, String>;
    fn commit_eval(&mut self, update: Box<dyn StatefulEvalUpdate>);

    fn eval(&mut self, input: StatefulEvalInput<'_>) -> Result<Value, String> {
        let prepared = self.prepare_eval(input)?;
        let output = prepared.output.clone();
        self.commit_eval(prepared.update);
        Ok(output)
    }
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
        let mut write = self.functions.write();
        let key = function.name().to_lowercase();
        if write.contains_key(&key) {
            return Err(StatefulRegistryError::AlreadyRegistered(key));
        }
        write.insert(key, function);
        Ok(())
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn StatefulFunction>> {
        self.functions.read().get(&name.to_lowercase()).cloned()
    }

    pub fn is_registered(&self, name: &str) -> bool {
        self.functions.read().contains_key(&name.to_lowercase())
    }

    fn register_builtin_functions(&self) {
        let _ = self.register_function(Arc::new(AccAvgFunction::new()));
        let _ = self.register_function(Arc::new(AccCountFunction::new()));
        let _ = self.register_function(Arc::new(AccMaxFunction::new()));
        let _ = self.register_function(Arc::new(AccMinFunction::new()));
        let _ = self.register_function(Arc::new(AccSumFunction::new()));
        let _ = self.register_function(Arc::new(ChangeCaptureFunction::new()));
        let _ = self.register_function(Arc::new(ChangeToFunction::new()));
        let _ = self.register_function(Arc::new(ChangedColFunction::new()));
        let _ = self.register_function(Arc::new(ConsecutiveCountFunction::new()));
        let _ = self.register_function(Arc::new(ConsecutiveStartFunction::new()));
        let _ = self.register_function(Arc::new(HadChangedFunction::new()));
        let _ = self.register_function(Arc::new(LagFunction::new()));
        let _ = self.register_function(Arc::new(LatestFunction::new()));
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
            struct Update;
            impl StatefulEvalUpdate for Update {
                fn as_any(&self) -> &dyn Any {
                    self
                }
            }
            struct Inst;
            impl StatefulFunctionInstance for Inst {
                fn prepare_eval(
                    &self,
                    _input: StatefulEvalInput<'_>,
                ) -> Result<PreparedStatefulEval, String> {
                    Ok(PreparedStatefulEval::new(Value::Null, Box::new(Update)))
                }

                fn commit_eval(&mut self, _update: Box<dyn StatefulEvalUpdate>) {}
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

    #[test]
    fn runtime_builtins_cover_parser_builtin_stateful_names() {
        let registry = StatefulFunctionRegistry::default();
        for name in parser::builtin_stateful_function_names() {
            assert!(
                registry.is_registered(name),
                "runtime registry missing parser builtin stateful function '{name}'"
            );
        }
        for name in parser::builtin_acc_function_names() {
            assert!(
                registry.is_registered(name),
                "runtime registry missing parser builtin acc function '{name}'"
            );
        }
    }
}
