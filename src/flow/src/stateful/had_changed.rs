use super::util::{bool_arg, normalize_state_value};
use super::{
    PreparedStatefulEval, StatefulEvalInput, StatefulEvalUpdate, StatefulFunction,
    StatefulFunctionInstance,
};
use crate::catalog::{
    FunctionArgSpec, FunctionContext, FunctionDef, FunctionKind, FunctionRequirement,
    FunctionSignatureSpec, StatefulFunctionSpec, TypeSpec,
};
use datatypes::{BooleanType, ConcreteDatatype, Value};
use std::any::Any;

pub struct HadChangedFunction;

pub fn had_changed_function_def() -> FunctionDef {
    FunctionDef {
        kind: FunctionKind::Stateful,
        name: "had_changed".to_string(),
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
                    variadic: true,
                },
            ],
            return_type: TypeSpec::Named {
                name: "bool".to_string(),
            },
        },
        description:
            "Return true when any tracked argument changes compared with its previous accepted value."
                .to_string(),
        allowed_contexts: vec![FunctionContext::Select, FunctionContext::Where],
        requirements: vec![FunctionRequirement::DeterministicOrder],
        constraints: vec![
            "Requires at least 2 arguments.".to_string(),
            "The first argument must be a boolean ignore_null flag.".to_string(),
            "The first accepted row is treated as changed and returns true.".to_string(),
            "Returns false when the row is filtered out.".to_string(),
        ],
        examples: vec![
            "SELECT had_changed(true, status, code) AS changed FROM stream".to_string(),
        ],
        aggregate: None,
        stateful: Some(StatefulFunctionSpec {
            state_semantics: "Maintains one previous value per tracked argument position."
                .to_string(),
        }),
    }
}

impl HadChangedFunction {
    pub fn new() -> Self {
        Self
    }
}

impl Default for HadChangedFunction {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Default)]
struct HadChangedInstance {
    config: Option<HadChangedConfig>,
    previous: Vec<Option<Value>>,
}

#[derive(Clone, Copy)]
struct HadChangedConfig {
    ignore_null: bool,
}

impl StatefulFunctionInstance for HadChangedInstance {
    fn prepare_eval(&self, input: StatefulEvalInput<'_>) -> Result<PreparedStatefulEval, String> {
        if input.args.len() < 2 {
            return Err(format!(
                "had_changed() expects at least 2 arguments, got {}",
                input.args.len()
            ));
        }

        if !input.should_apply {
            return Ok(PreparedStatefulEval::new(
                Value::Bool(false),
                Box::new(HadChangedUpdate {
                    config: None,
                    tracked_len: None,
                    entries: Vec::new(),
                }),
            ));
        }

        let config = match self.config {
            Some(config) => config,
            None => HadChangedConfig {
                ignore_null: bool_arg("had_changed() first argument", &input.args[0])?,
            },
        };
        let config_update = self.config.is_none().then_some(config);
        let tracked_len = input.args.len() - 1;

        let mut changed = false;
        let mut entries = Vec::new();
        for (index, value) in input.args[1..].iter().enumerate() {
            if config.ignore_null && value.is_null() {
                continue;
            }

            let normalized = normalize_state_value(value);
            let previous = self.previous.get(index).unwrap_or(&None);
            if previous != &normalized {
                entries.push((index, normalized));
                changed = true;
            }
        }

        Ok(PreparedStatefulEval::new(
            Value::Bool(changed),
            Box::new(HadChangedUpdate {
                config: config_update,
                tracked_len: Some(tracked_len),
                entries,
            }),
        ))
    }

    fn commit_eval(&mut self, update: Box<dyn StatefulEvalUpdate>) {
        let update = update
            .as_any()
            .downcast_ref::<HadChangedUpdate>()
            .expect("had_changed received incompatible update");
        if let Some(config) = update.config {
            self.config = Some(config);
        }
        if let Some(tracked_len) = update.tracked_len {
            if self.previous.len() < tracked_len {
                self.previous.resize(tracked_len, None);
            }
        }
        for (index, value) in &update.entries {
            self.previous[*index] = value.clone();
        }
    }
}

struct HadChangedUpdate {
    config: Option<HadChangedConfig>,
    tracked_len: Option<usize>,
    entries: Vec<(usize, Option<Value>)>,
}

impl StatefulEvalUpdate for HadChangedUpdate {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl StatefulFunction for HadChangedFunction {
    fn name(&self) -> &str {
        "had_changed"
    }

    fn return_type(&self, input_types: &[ConcreteDatatype]) -> Result<ConcreteDatatype, String> {
        if input_types.len() < 2 {
            return Err(format!(
                "had_changed() expects at least 2 argument types, got {}",
                input_types.len()
            ));
        }
        Ok(ConcreteDatatype::Bool(BooleanType))
    }

    fn create_instance(&self) -> Box<dyn StatefulFunctionInstance> {
        Box::new(HadChangedInstance::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn had_changed_detects_changes_across_multiple_columns() {
        let function = HadChangedFunction::new();
        let mut instance = function.create_instance();

        assert_eq!(
            instance
                .eval(StatefulEvalInput {
                    args: &[
                        Value::Bool(true),
                        Value::Int64(1),
                        Value::String("a".to_string())
                    ],
                    should_apply: true,
                })
                .unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            instance
                .eval(StatefulEvalInput {
                    args: &[
                        Value::Bool(true),
                        Value::Int64(1),
                        Value::String("a".to_string())
                    ],
                    should_apply: true,
                })
                .unwrap(),
            Value::Bool(false)
        );
        assert_eq!(
            instance
                .eval(StatefulEvalInput {
                    args: &[
                        Value::Bool(true),
                        Value::Int64(2),
                        Value::String("a".to_string())
                    ],
                    should_apply: true,
                })
                .unwrap(),
            Value::Bool(true)
        );
    }

    #[test]
    fn had_changed_caches_ignore_null_configuration() {
        let function = HadChangedFunction::new();
        let mut instance = function.create_instance();

        assert_eq!(
            instance
                .eval(StatefulEvalInput {
                    args: &[Value::Bool(true), Value::Null],
                    should_apply: true,
                })
                .unwrap(),
            Value::Bool(false)
        );
        assert_eq!(
            instance
                .eval(StatefulEvalInput {
                    args: &[Value::Bool(false), Value::Null],
                    should_apply: true,
                })
                .unwrap(),
            Value::Bool(false)
        );
        assert_eq!(
            instance
                .eval(StatefulEvalInput {
                    args: &[Value::Bool(false), Value::Int64(9)],
                    should_apply: true,
                })
                .unwrap(),
            Value::Bool(true)
        );
    }
}
