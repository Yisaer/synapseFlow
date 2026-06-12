use super::util::{bool_arg, normalize_state_value};
use super::{StatefulEvalInput, StatefulFunction, StatefulFunctionInstance};
use crate::catalog::{
    FunctionArgSpec, FunctionContext, FunctionDef, FunctionKind, FunctionRequirement,
    FunctionSignatureSpec, StatefulFunctionSpec, TypeSpec,
};
use crate::expr::value_compare;
use datatypes::{BooleanType, ConcreteDatatype, Value};

pub struct ChangeToFunction;

pub fn change_to_function_def() -> FunctionDef {
    FunctionDef {
        kind: FunctionKind::Stateful,
        name: "change_to".to_string(),
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
                    name: "value".to_string(),
                    r#type: TypeSpec::Any,
                    optional: false,
                    variadic: false,
                },
                FunctionArgSpec {
                    name: "target".to_string(),
                    r#type: TypeSpec::Any,
                    optional: false,
                    variadic: false,
                },
            ],
            return_type: TypeSpec::Named {
                name: "bool".to_string(),
            },
        },
        description:
            "Return true only on the row where value transitions to target (equals target and differs from the previous accepted value)."
                .to_string(),
        allowed_contexts: vec![FunctionContext::Select, FunctionContext::Where],
        requirements: vec![FunctionRequirement::DeterministicOrder],
        constraints: vec![
            "Requires exactly 3 arguments.".to_string(),
            "The first argument must be a boolean ignore_null flag.".to_string(),
            "The first accepted row is treated as changed and returns true when value equals target."
                .to_string(),
            "Target equality is type-coerced; a NULL target never matches.".to_string(),
            "Returns false while value stays at target, when the row is filtered out, or (under ignore_null) on NULL value."
                .to_string(),
        ],
        examples: vec![
            "SELECT change_to(true, status, 1) AS became_active FROM stream".to_string(),
        ],
        aggregate: None,
        stateful: Some(StatefulFunctionSpec {
            state_semantics: "Maintains the last accepted value and emits true only on transition to the target."
                .to_string(),
        }),
    }
}

impl ChangeToFunction {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ChangeToFunction {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Default)]
struct ChangeToInstance {
    config: Option<ChangeToConfig>,
    previous: Option<Value>,
}

#[derive(Clone, Copy)]
struct ChangeToConfig {
    ignore_null: bool,
}

impl StatefulFunctionInstance for ChangeToInstance {
    fn eval(&mut self, input: StatefulEvalInput<'_>) -> Result<Value, String> {
        if input.args.len() != 3 {
            return Err(format!(
                "change_to() expects exactly 3 arguments, got {}",
                input.args.len()
            ));
        }

        let config = match self.config {
            Some(config) => config,
            None => {
                let config = ChangeToConfig {
                    ignore_null: bool_arg("change_to() first argument", &input.args[0])?,
                };
                self.config = Some(config);
                config
            }
        };
        let value = &input.args[1];
        let target = &input.args[2];

        // Freeze state and emit false on a null value (when ignoring nulls) or a
        // filtered-out row, so a later real value still registers as a transition.
        if config.ignore_null && value.is_null() {
            return Ok(Value::Bool(false));
        }
        if !input.should_apply {
            return Ok(Value::Bool(false));
        }

        let normalized = normalize_state_value(value);
        let changed = self.previous != normalized;
        self.previous = normalized;

        let hit = changed && value_compare::values_equal(value, target);
        Ok(Value::Bool(hit))
    }
}

impl StatefulFunction for ChangeToFunction {
    fn name(&self) -> &str {
        "change_to"
    }

    fn return_type(&self, input_types: &[ConcreteDatatype]) -> Result<ConcreteDatatype, String> {
        if input_types.len() != 3 {
            return Err(format!(
                "change_to() expects exactly 3 argument types, got {}",
                input_types.len()
            ));
        }
        Ok(ConcreteDatatype::Bool(BooleanType))
    }

    fn create_instance(&self) -> Box<dyn StatefulFunctionInstance> {
        Box::new(ChangeToInstance::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn eval(
        instance: &mut Box<dyn StatefulFunctionInstance>,
        args: &[Value],
        apply: bool,
    ) -> Value {
        instance
            .eval(StatefulEvalInput {
                args,
                should_apply: apply,
            })
            .unwrap()
    }

    #[test]
    fn change_to_emits_only_on_transition_to_target() {
        let function = ChangeToFunction::new();
        let mut instance = function.create_instance();

        // First row equals target -> changed (no prior state) -> true.
        assert_eq!(
            eval(
                &mut instance,
                &[Value::Bool(true), Value::Int64(1), Value::Int64(1)],
                true
            ),
            Value::Bool(true)
        );
        // Stays at target -> not changed -> false.
        assert_eq!(
            eval(
                &mut instance,
                &[Value::Bool(true), Value::Int64(1), Value::Int64(1)],
                true
            ),
            Value::Bool(false)
        );
        // Moves away from target -> changed but != target -> false.
        assert_eq!(
            eval(
                &mut instance,
                &[Value::Bool(true), Value::Int64(2), Value::Int64(1)],
                true
            ),
            Value::Bool(false)
        );
        // Returns to target -> changed and == target -> true.
        assert_eq!(
            eval(
                &mut instance,
                &[Value::Bool(true), Value::Int64(1), Value::Int64(1)],
                true
            ),
            Value::Bool(true)
        );
    }

    #[test]
    fn change_to_first_row_not_equal_target_is_false() {
        let function = ChangeToFunction::new();
        let mut instance = function.create_instance();

        assert_eq!(
            eval(
                &mut instance,
                &[Value::Bool(true), Value::Int64(2), Value::Int64(1)],
                true
            ),
            Value::Bool(false)
        );
        assert_eq!(
            eval(
                &mut instance,
                &[Value::Bool(true), Value::Int64(1), Value::Int64(1)],
                true
            ),
            Value::Bool(true)
        );
    }

    #[test]
    fn change_to_ignore_null_freezes_state() {
        let function = ChangeToFunction::new();
        let mut instance = function.create_instance();

        // value 2 (not target) accepted.
        assert_eq!(
            eval(
                &mut instance,
                &[Value::Bool(true), Value::Int64(2), Value::Int64(1)],
                true
            ),
            Value::Bool(false)
        );
        // NULL value ignored, state frozen at 2, returns false.
        assert_eq!(
            eval(
                &mut instance,
                &[Value::Bool(true), Value::Null, Value::Int64(1)],
                true
            ),
            Value::Bool(false)
        );
        // Real transition to target still fires (state was frozen at 2).
        assert_eq!(
            eval(
                &mut instance,
                &[Value::Bool(true), Value::Int64(1), Value::Int64(1)],
                true
            ),
            Value::Bool(true)
        );
    }

    #[test]
    fn change_to_ignore_null_false_treats_null_as_value() {
        let function = ChangeToFunction::new();
        let mut instance = function.create_instance();

        // ignore_null=false: NULL participates as a value, target is NULL ->
        // NULL never matches via values_equal, but state advances to NULL.
        assert_eq!(
            eval(
                &mut instance,
                &[Value::Bool(false), Value::Null, Value::Null],
                true
            ),
            Value::Bool(false)
        );
        // value 1 -> changed from NULL, equals target 1 -> true.
        assert_eq!(
            eval(
                &mut instance,
                &[Value::Bool(false), Value::Int64(1), Value::Int64(1)],
                true
            ),
            Value::Bool(true)
        );
    }

    #[test]
    fn change_to_filtered_row_freezes_state() {
        let function = ChangeToFunction::new();
        let mut instance = function.create_instance();

        assert_eq!(
            eval(
                &mut instance,
                &[Value::Bool(true), Value::Int64(2), Value::Int64(1)],
                true
            ),
            Value::Bool(false)
        );
        // Filtered out: returns false, state frozen at 2.
        assert_eq!(
            eval(
                &mut instance,
                &[Value::Bool(true), Value::Int64(1), Value::Int64(1)],
                false
            ),
            Value::Bool(false)
        );
        // Next accepted transition to target still fires.
        assert_eq!(
            eval(
                &mut instance,
                &[Value::Bool(true), Value::Int64(1), Value::Int64(1)],
                true
            ),
            Value::Bool(true)
        );
    }

    #[test]
    fn change_to_null_target_never_matches() {
        let function = ChangeToFunction::new();
        let mut instance = function.create_instance();

        assert_eq!(
            eval(
                &mut instance,
                &[Value::Bool(true), Value::Int64(1), Value::Null],
                true
            ),
            Value::Bool(false)
        );
    }

    #[test]
    fn change_to_target_equality_is_type_coerced() {
        let function = ChangeToFunction::new();
        let mut instance = function.create_instance();

        // Int32 value vs Int64 target literal should match via coercion.
        assert_eq!(
            eval(
                &mut instance,
                &[Value::Bool(true), Value::Int32(1), Value::Int64(1)],
                true
            ),
            Value::Bool(true)
        );
    }
}
