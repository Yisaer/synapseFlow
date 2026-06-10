use super::util::bool_condition;
use super::{StatefulEvalInput, StatefulFunction, StatefulFunctionInstance};
use crate::catalog::{
    FunctionArgSpec, FunctionContext, FunctionDef, FunctionKind, FunctionRequirement,
    FunctionSignatureSpec, StatefulFunctionSpec, TypeSpec,
};
use datatypes::{ConcreteDatatype, Value};

pub struct ConsecutiveStartFunction;

pub fn consecutive_start_function_def() -> FunctionDef {
    FunctionDef {
        kind: FunctionKind::Stateful,
        name: "consecutive_start".to_string(),
        aliases: vec![],
        signature: FunctionSignatureSpec {
            args: vec![
                FunctionArgSpec {
                    name: "condition".to_string(),
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
            ],
            return_type: TypeSpec::Any,
        },
        description:
            "Capture `value` from the row where `condition` starts being continuously true and return it while the condition remains true."
                .to_string(),
        allowed_contexts: vec![FunctionContext::Select, FunctionContext::Where],
        requirements: vec![FunctionRequirement::DeterministicOrder],
        constraints: vec![
            "Requires exactly 2 arguments.".to_string(),
            "The first argument must evaluate to a boolean condition; a NULL condition is treated as false and a non-boolean condition is an error."
                .to_string(),
            "Captures `value` only on the rising edge (false -> true); the captured value is held unchanged while the condition stays true, reporting the start rather than the latest value."
                .to_string(),
            "Returns NULL while the condition is false and clears the captured start so the next true run recaptures.".to_string(),
            "The captured value may itself be NULL; the return type follows the type of `value`.".to_string(),
            "When the row is filtered out, returns the currently held value and does not advance state.".to_string(),
        ],
        examples: vec![
            "SELECT consecutive_start(spi > 5, ts) AS run_start FROM stream".to_string(),
        ],
        aggregate: None,
        stateful: Some(StatefulFunctionSpec {
            state_semantics: "Maintains the previous condition and the value captured when the condition started being continuously true."
                .to_string(),
        }),
    }
}

impl ConsecutiveStartFunction {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ConsecutiveStartFunction {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Default)]
struct ConsecutiveStartInstance {
    prev_condition: bool,
    // Invariant: `start.is_some()` iff `prev_condition == true`.
    start: Option<Value>,
}

impl ConsecutiveStartInstance {
    fn held(&self) -> Value {
        self.start.clone().unwrap_or(Value::Null)
    }
}

impl StatefulFunctionInstance for ConsecutiveStartInstance {
    fn eval(&mut self, input: StatefulEvalInput<'_>) -> Result<Value, String> {
        if input.args.len() != 2 {
            return Err(format!(
                "consecutive_start() expects exactly 2 arguments, got {}",
                input.args.len()
            ));
        }

        // Filtered-out rows return the held value without advancing state, so a
        // later real row still observes the captured start (or recaptures).
        if !input.should_apply {
            return Ok(self.held());
        }

        let condition = bool_condition("consecutive_start() condition", &input.args[0])?;
        if condition {
            if !self.prev_condition {
                // Rising edge: capture the start value once.
                self.start = Some(input.args[1].clone());
            }
        } else {
            // Condition no longer holds: end the run.
            self.start = None;
        }
        self.prev_condition = condition;

        if condition {
            Ok(self.held())
        } else {
            Ok(Value::Null)
        }
    }
}

impl StatefulFunction for ConsecutiveStartFunction {
    fn name(&self) -> &str {
        "consecutive_start"
    }

    fn return_type(&self, input_types: &[ConcreteDatatype]) -> Result<ConcreteDatatype, String> {
        if input_types.len() != 2 {
            return Err(format!(
                "consecutive_start() expects exactly 2 argument types, got {}",
                input_types.len()
            ));
        }
        // Returns the captured value, whose type is that of the `value` argument.
        Ok(input_types[1].clone())
    }

    fn create_instance(&self) -> Box<dyn StatefulFunctionInstance> {
        Box::new(ConsecutiveStartInstance::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn eval(
        instance: &mut Box<dyn StatefulFunctionInstance>,
        condition: Value,
        value: Value,
        apply: bool,
    ) -> Value {
        instance
            .eval(StatefulEvalInput {
                args: &[condition, value],
                should_apply: apply,
            })
            .unwrap()
    }

    #[test]
    fn holds_rising_edge_value_across_true_run() {
        let function = ConsecutiveStartFunction::new();
        let mut instance = function.create_instance();

        // Rising edge captures the start value.
        assert_eq!(
            eval(&mut instance, Value::Bool(true), Value::Int64(100), true),
            Value::Int64(100)
        );
        // Held unchanged across the run even as `value` changes (P4: start, not latest).
        assert_eq!(
            eval(&mut instance, Value::Bool(true), Value::Int64(200), true),
            Value::Int64(100)
        );
        assert_eq!(
            eval(&mut instance, Value::Bool(true), Value::Int64(300), true),
            Value::Int64(100)
        );
        // Condition false: returns NULL and clears.
        assert_eq!(
            eval(&mut instance, Value::Bool(false), Value::Int64(400), true),
            Value::Null
        );
        // New rising edge recaptures.
        assert_eq!(
            eval(&mut instance, Value::Bool(true), Value::Int64(500), true),
            Value::Int64(500)
        );
    }

    #[test]
    fn first_row_true_captures() {
        let function = ConsecutiveStartFunction::new();
        let mut instance = function.create_instance();
        assert_eq!(
            eval(&mut instance, Value::Bool(true), Value::Int64(7), true),
            Value::Int64(7)
        );
    }

    #[test]
    fn null_condition_ends_run() {
        let function = ConsecutiveStartFunction::new();
        let mut instance = function.create_instance();

        assert_eq!(
            eval(&mut instance, Value::Bool(true), Value::Int64(10), true),
            Value::Int64(10)
        );
        // NULL condition is treated as false: returns NULL and clears the run.
        assert_eq!(
            eval(&mut instance, Value::Null, Value::Int64(20), true),
            Value::Null
        );
        // Next true recaptures fresh.
        assert_eq!(
            eval(&mut instance, Value::Bool(true), Value::Int64(30), true),
            Value::Int64(30)
        );
    }

    #[test]
    fn non_boolean_condition_errors() {
        let function = ConsecutiveStartFunction::new();
        let mut instance = function.create_instance();
        let err = instance
            .eval(StatefulEvalInput {
                args: &[Value::Int64(1), Value::Int64(2)],
                should_apply: true,
            })
            .expect_err("non-boolean condition should error");
        assert!(err.contains("must be bool"), "unexpected error: {err}");
    }

    #[test]
    fn null_capture_value_is_captured() {
        let function = ConsecutiveStartFunction::new();
        let mut instance = function.create_instance();

        // A NULL capture value is captured and returned (NULL) while the run is active.
        assert_eq!(
            eval(&mut instance, Value::Bool(true), Value::Null, true),
            Value::Null
        );
        // Still active, still the captured (NULL) start.
        assert_eq!(
            eval(&mut instance, Value::Bool(true), Value::Int64(99), true),
            Value::Null
        );
    }

    #[test]
    fn filtered_row_returns_held_value_and_freezes_state() {
        let function = ConsecutiveStartFunction::new();
        let mut instance = function.create_instance();

        assert_eq!(
            eval(&mut instance, Value::Bool(true), Value::Int64(10), true),
            Value::Int64(10)
        );
        // filtered: returns the held start, no advance.
        assert_eq!(
            eval(&mut instance, Value::Bool(false), Value::Int64(20), false),
            Value::Int64(10)
        );
        // applied true: run continues, still the original start.
        assert_eq!(
            eval(&mut instance, Value::Bool(true), Value::Int64(30), true),
            Value::Int64(10)
        );
    }

    // Mirrors the eKuiper consecutive.go TestConsecutiveStart_Exec table (the
    // behavioral oracle), adapted to VeloFlux's per-instance / should_apply model.
    #[test]
    fn ekuiper_oracle_table() {
        let function = ConsecutiveStartFunction::new();
        let mut instance = function.create_instance();

        // (condition, value, apply, expected)
        let steps: &[(bool, i64, bool, Value)] = &[
            (true, 1, true, Value::Int64(1)),   // capture start1
            (false, 2, false, Value::Int64(1)), // filtered: hold start1
            (true, 3, true, Value::Int64(1)),   // still in run: hold start1
            (false, 4, false, Value::Int64(1)), // filtered: hold start1
            (true, 3, true, Value::Int64(1)),   // still in run: hold start1
            (false, 5, true, Value::Null),      // applied false: end run -> NULL
            (true, 6, false, Value::Null),      // filtered after end: held NULL
            (false, 5, true, Value::Null),      // applied false: NULL
            (true, 6, true, Value::Int64(6)),   // rising edge: capture start6
        ];

        for (i, (cond, val, apply, expected)) in steps.iter().enumerate() {
            let got = eval(
                &mut instance,
                Value::Bool(*cond),
                Value::Int64(*val),
                *apply,
            );
            assert_eq!(&got, expected, "step {i} mismatch");
        }
    }
}
