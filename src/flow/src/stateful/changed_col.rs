use super::util::{bool_arg, normalize_state_value};
use super::{
    PreparedStatefulEval, StatefulEvalInput, StatefulEvalUpdate, StatefulFunction,
    StatefulFunctionInstance,
};
use crate::catalog::{
    FunctionArgSpec, FunctionContext, FunctionDef, FunctionKind, FunctionRequirement,
    FunctionSignatureSpec, StatefulFunctionSpec, TypeSpec,
};
use datatypes::{ConcreteDatatype, Value};
use std::any::Any;

pub struct ChangedColFunction;

pub fn changed_col_function_def() -> FunctionDef {
    FunctionDef {
        kind: FunctionKind::Stateful,
        name: "changed_col".to_string(),
        aliases: vec![],
        signature: FunctionSignatureSpec {
            args: vec![
                FunctionArgSpec {
                    name: "ignore_null".to_string(),
                    r#type: TypeSpec::Named {
                        name: "bool".to_string(),
                    },
                    optional: false,
                    variadic: false,
                },
                FunctionArgSpec {
                    name: "x".to_string(),
                    r#type: TypeSpec::Any,
                    optional: false,
                    variadic: false,
                },
            ],
            return_type: TypeSpec::Any,
        },
        description:
            "Return the current value only when it differs from the previous accepted value."
                .to_string(),
        allowed_contexts: vec![FunctionContext::Select, FunctionContext::Where],
        requirements: vec![FunctionRequirement::DeterministicOrder],
        constraints: vec![
            "Requires exactly 2 arguments.".to_string(),
            "The first argument must be a boolean ignore_null flag.".to_string(),
            "The first accepted row is treated as changed and returns the current value."
                .to_string(),
            "Returns NULL when the value does not change or when the row is filtered out."
                .to_string(),
        ],
        examples: vec!["SELECT changed_col(true, status) AS status_change FROM stream".to_string()],
        aggregate: None,
        stateful: Some(StatefulFunctionSpec {
            state_semantics: "Maintains the last accepted value and emits only on changes."
                .to_string(),
        }),
    }
}

impl ChangedColFunction {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ChangedColFunction {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Default)]
struct ChangedColInstance {
    config: Option<ChangedColConfig>,
    previous: Option<Value>,
}

#[derive(Clone, Copy)]
struct ChangedColConfig {
    ignore_null: bool,
}

impl StatefulFunctionInstance for ChangedColInstance {
    fn prepare_eval(&self, input: StatefulEvalInput<'_>) -> Result<PreparedStatefulEval, String> {
        if input.args.len() != 2 {
            return Err(format!(
                "changed_col() expects exactly 2 arguments, got {}",
                input.args.len()
            ));
        }

        let config = match self.config {
            Some(config) => config,
            None => ChangedColConfig {
                ignore_null: bool_arg("changed_col() first argument", &input.args[0])?,
            },
        };
        let current = &input.args[1];
        let config_update = self.config.is_none().then_some(config);

        if config.ignore_null && current.is_null() {
            return Ok(PreparedStatefulEval::new(
                Value::Null,
                Box::new(ChangedColUpdate {
                    config: config_update,
                    previous: None,
                }),
            ));
        }
        if !input.should_apply {
            return Ok(PreparedStatefulEval::new(
                Value::Null,
                Box::new(ChangedColUpdate {
                    config: config_update,
                    previous: None,
                }),
            ));
        }

        let normalized = normalize_state_value(current);
        if self.previous != normalized {
            return Ok(PreparedStatefulEval::new(
                current.clone(),
                Box::new(ChangedColUpdate {
                    config: config_update,
                    previous: Some(normalized),
                }),
            ));
        }

        Ok(PreparedStatefulEval::new(
            Value::Null,
            Box::new(ChangedColUpdate {
                config: config_update,
                previous: None,
            }),
        ))
    }

    fn commit_eval(&mut self, update: Box<dyn StatefulEvalUpdate>) {
        let update = update
            .as_any()
            .downcast_ref::<ChangedColUpdate>()
            .expect("changed_col received incompatible update");
        if let Some(config) = update.config {
            self.config = Some(config);
        }
        if let Some(previous) = &update.previous {
            self.previous = previous.clone();
        }
    }
}

struct ChangedColUpdate {
    config: Option<ChangedColConfig>,
    previous: Option<Option<Value>>,
}

impl StatefulEvalUpdate for ChangedColUpdate {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl StatefulFunction for ChangedColFunction {
    fn name(&self) -> &str {
        "changed_col"
    }

    fn return_type(&self, input_types: &[ConcreteDatatype]) -> Result<ConcreteDatatype, String> {
        if input_types.len() != 2 {
            return Err(format!(
                "changed_col() expects exactly 2 argument types, got {}",
                input_types.len()
            ));
        }
        Ok(input_types[1].clone())
    }

    fn create_instance(&self) -> Box<dyn StatefulFunctionInstance> {
        Box::new(ChangedColInstance::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn changed_col_emits_only_when_value_changes() {
        let function = ChangedColFunction::new();
        let mut instance = function.create_instance();

        assert_eq!(
            instance
                .eval(StatefulEvalInput {
                    args: &[Value::Bool(true), Value::Int64(1)],
                    should_apply: true,
                })
                .unwrap(),
            Value::Int64(1)
        );
        assert_eq!(
            instance
                .eval(StatefulEvalInput {
                    args: &[Value::Bool(true), Value::Int64(1)],
                    should_apply: true,
                })
                .unwrap(),
            Value::Null
        );
        assert_eq!(
            instance
                .eval(StatefulEvalInput {
                    args: &[Value::Bool(true), Value::Int64(2)],
                    should_apply: true,
                })
                .unwrap(),
            Value::Int64(2)
        );
    }

    #[test]
    fn changed_col_caches_ignore_null_configuration() {
        let function = ChangedColFunction::new();
        let mut instance = function.create_instance();

        assert_eq!(
            instance
                .eval(StatefulEvalInput {
                    args: &[Value::Bool(true), Value::Null],
                    should_apply: true,
                })
                .unwrap(),
            Value::Null
        );
        assert_eq!(
            instance
                .eval(StatefulEvalInput {
                    args: &[Value::Bool(false), Value::Null],
                    should_apply: true,
                })
                .unwrap(),
            Value::Null
        );
        assert_eq!(
            instance
                .eval(StatefulEvalInput {
                    args: &[Value::Bool(false), Value::Int64(7)],
                    should_apply: true,
                })
                .unwrap(),
            Value::Int64(7)
        );
    }
}
