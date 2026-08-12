use super::util::{bool_arg, lag_offset};
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
use std::collections::VecDeque;

pub struct LagFunction;

pub fn lag_function_def() -> FunctionDef {
    FunctionDef {
        kind: FunctionKind::Stateful,
        name: "lag".to_string(),
        aliases: vec![],
        signature: FunctionSignatureSpec {
            args: vec![
                FunctionArgSpec {
                    name: "x".to_string(),
                    r#type: TypeSpec::Any,
                    optional: false,
                    variadic: false,
                },
                FunctionArgSpec {
                    name: "offset".to_string(),
                    r#type: TypeSpec::Named {
                        name: "int64".to_string(),
                    },
                    optional: true,
                    variadic: false,
                },
                FunctionArgSpec {
                    name: "ignore_null".to_string(),
                    r#type: TypeSpec::Named {
                        name: "bool".to_string(),
                    },
                    optional: true,
                    variadic: false,
                },
            ],
            return_type: TypeSpec::Any,
        },
        description: "Return the value from offset rows back, optionally skipping NULL inputs."
            .to_string(),
        allowed_contexts: vec![FunctionContext::Select, FunctionContext::Where],
        requirements: vec![FunctionRequirement::DeterministicOrder],
        constraints: vec![
            "Requires 1 to 3 arguments.".to_string(),
            "Return type matches the argument type.".to_string(),
            "First rows return NULL until enough history exists.".to_string(),
            "The optional offset must be a positive integer.".to_string(),
            "When ignore_null is true, NULL input values do not advance the lag state."
                .to_string(),
            "Row order is the pipeline's processing order (no explicit ORDER BY support yet)."
                .to_string(),
        ],
        examples: vec![
            "SELECT lag(x) AS prev_x, x".to_string(),
            "SELECT lag(x, 2, true) AS prev_x2 FROM stream".to_string(),
        ],
        aggregate: None,
        stateful: Some(StatefulFunctionSpec {
            state_semantics:
                "Maintains a bounded lag buffer per partition and returns the current visible lag value."
                    .to_string(),
        }),
    }
}

impl LagFunction {
    pub fn new() -> Self {
        Self
    }
}

impl Default for LagFunction {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Default)]
struct LagInstance {
    config: Option<LagConfig>,
    queue: Option<VecDeque<Value>>,
}

#[derive(Clone, Copy)]
struct LagConfig {
    offset: usize,
    ignore_null: bool,
}

impl StatefulFunctionInstance for LagInstance {
    fn prepare_eval(&self, input: StatefulEvalInput<'_>) -> Result<PreparedStatefulEval, String> {
        if input.args.is_empty() || input.args.len() > 3 {
            return Err(format!(
                "lag() expects 1 to 3 arguments, got {}",
                input.args.len()
            ));
        }

        let config = match self.config {
            Some(config) => config,
            None => {
                let config = LagConfig {
                    offset: match input.args.get(1) {
                        Some(value) => lag_offset(value)?,
                        None => 1,
                    },
                    ignore_null: match input.args.get(2) {
                        Some(value) => bool_arg("lag() third argument", value)?,
                        None => true,
                    },
                };
                config
            }
        };
        let config_update = self.config.is_none().then_some(config);

        let visible = self
            .queue
            .as_ref()
            .and_then(|queue| queue.front().cloned())
            .unwrap_or(Value::Null);
        if !input.should_apply {
            return Ok(PreparedStatefulEval::new(
                visible,
                Box::new(LagUpdate {
                    config: config_update,
                    push: None,
                }),
            ));
        }

        let current = &input.args[0];
        if config.ignore_null && current.is_null() {
            return Ok(PreparedStatefulEval::new(
                visible,
                Box::new(LagUpdate {
                    config: config_update,
                    push: None,
                }),
            ));
        }

        Ok(PreparedStatefulEval::new(
            visible,
            Box::new(LagUpdate {
                config: config_update,
                push: Some(current.clone()),
            }),
        ))
    }

    fn commit_eval(&mut self, update: Box<dyn StatefulEvalUpdate>) {
        let update = update
            .as_any()
            .downcast_ref::<LagUpdate>()
            .expect("lag received incompatible update");
        if let Some(config) = update.config {
            self.config = Some(config);
        }
        let Some(current) = &update.push else {
            return;
        };
        let config = update
            .config
            .or(self.config)
            .expect("lag update missing configuration");
        let queue = self.queue.get_or_insert_with(|| {
            let mut values = VecDeque::with_capacity(config.offset);
            for _ in 0..config.offset {
                values.push_back(Value::Null);
            }
            values
        });
        let _ = queue.pop_front();
        queue.push_back(current.clone());
    }
}

struct LagUpdate {
    config: Option<LagConfig>,
    push: Option<Value>,
}

impl StatefulEvalUpdate for LagUpdate {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl StatefulFunction for LagFunction {
    fn name(&self) -> &str {
        "lag"
    }

    fn return_type(&self, input_types: &[ConcreteDatatype]) -> Result<ConcreteDatatype, String> {
        if input_types.is_empty() || input_types.len() > 3 {
            return Err(format!(
                "lag() expects 1 to 3 argument types, got {}",
                input_types.len()
            ));
        }
        Ok(input_types[0].clone())
    }

    fn create_instance(&self) -> Box<dyn StatefulFunctionInstance> {
        Box::new(LagInstance::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datatypes::types;

    #[test]
    fn lag_emits_previous_value() {
        let function = LagFunction::new();
        let mut instance = function.create_instance();

        assert_eq!(
            instance
                .eval(StatefulEvalInput {
                    args: &[Value::Int64(1)],
                    should_apply: true,
                })
                .unwrap(),
            Value::Null
        );
        assert_eq!(
            instance
                .eval(StatefulEvalInput {
                    args: &[Value::Int64(2)],
                    should_apply: true,
                })
                .unwrap(),
            Value::Int64(1)
        );
        assert_eq!(
            instance
                .eval(StatefulEvalInput {
                    args: &[Value::Int64(3)],
                    should_apply: true,
                })
                .unwrap(),
            Value::Int64(2)
        );
    }

    #[test]
    fn lag_skip_returns_current_visible_lag_value() {
        let function = LagFunction::new();
        let mut instance = function.create_instance();

        assert_eq!(
            instance
                .eval(StatefulEvalInput {
                    args: &[Value::Int64(1)],
                    should_apply: true,
                })
                .unwrap(),
            Value::Null
        );
        assert_eq!(
            instance
                .eval(StatefulEvalInput {
                    args: &[Value::Int64(999)],
                    should_apply: false,
                })
                .unwrap(),
            Value::Int64(1)
        );
        assert_eq!(
            instance
                .eval(StatefulEvalInput {
                    args: &[Value::Int64(2)],
                    should_apply: true,
                })
                .unwrap(),
            Value::Int64(1)
        );
    }

    #[test]
    fn lag_supports_offset_and_ignore_null() {
        let function = LagFunction::new();
        let mut instance = function.create_instance();

        assert_eq!(
            instance
                .eval(StatefulEvalInput {
                    args: &[Value::Int64(10), Value::Int64(2), Value::Bool(true)],
                    should_apply: true,
                })
                .unwrap(),
            Value::Null
        );
        assert_eq!(
            instance
                .eval(StatefulEvalInput {
                    args: &[Value::Null, Value::Int64(2), Value::Bool(true)],
                    should_apply: true,
                })
                .unwrap(),
            Value::Null
        );
        assert_eq!(
            instance
                .eval(StatefulEvalInput {
                    args: &[Value::Int64(30), Value::Int64(2), Value::Bool(true)],
                    should_apply: true,
                })
                .unwrap(),
            Value::Null
        );
        assert_eq!(
            instance
                .eval(StatefulEvalInput {
                    args: &[Value::Int64(40), Value::Int64(2), Value::Bool(true)],
                    should_apply: true,
                })
                .unwrap(),
            Value::Int64(10)
        );
    }

    #[test]
    fn lag_does_not_initialize_queue_for_filtered_out_first_row() {
        let mut instance = LagInstance::default();

        assert_eq!(
            instance
                .eval(StatefulEvalInput {
                    args: &[Value::Int64(1), Value::Int64(64), Value::Bool(true)],
                    should_apply: false,
                })
                .unwrap(),
            Value::Null
        );
        assert!(instance.queue.is_none());
    }

    #[test]
    fn lag_does_not_initialize_queue_for_ignored_null_input() {
        let mut instance = LagInstance::default();

        assert_eq!(
            instance
                .eval(StatefulEvalInput {
                    args: &[Value::Null, Value::Int64(64), Value::Bool(true)],
                    should_apply: true,
                })
                .unwrap(),
            Value::Null
        );
        assert!(instance.queue.is_none());
    }

    #[test]
    fn lag_type_matches_input() {
        let function = LagFunction::new();
        let ty = function
            .return_type(&[
                ConcreteDatatype::Int64(types::Int64Type),
                ConcreteDatatype::Int64(types::Int64Type),
                ConcreteDatatype::Bool(types::BooleanType),
            ])
            .unwrap();
        assert!(matches!(ty, ConcreteDatatype::Int64(_)));
    }
}
