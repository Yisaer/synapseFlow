use super::util::bool_condition;
use super::{
    PreparedStatefulEval, StatefulEvalInput, StatefulEvalUpdate, StatefulFunction,
    StatefulFunctionInstance,
};
use crate::catalog::{
    FunctionArgSpec, FunctionContext, FunctionDef, FunctionKind, FunctionRequirement,
    FunctionSignatureSpec, StatefulFunctionSpec, TypeSpec,
};
use datatypes::{ConcreteDatatype, Int64Type, Value};
use std::any::Any;

pub struct ConsecutiveCountFunction;

pub fn consecutive_count_function_def() -> FunctionDef {
    FunctionDef {
        kind: FunctionKind::Stateful,
        name: "consecutive_count".to_string(),
        aliases: vec![],
        signature: FunctionSignatureSpec {
            args: vec![FunctionArgSpec {
                name: "condition".to_string(),
                r#type: TypeSpec::Named {
                    name: "bool".to_string(),
                },
                optional: false,
                variadic: false,
            }],
            return_type: TypeSpec::Named {
                name: "int64".to_string(),
            },
        },
        description:
            "Return the number of consecutive rows, ending at the current row, for which the boolean condition has been true."
                .to_string(),
        allowed_contexts: vec![FunctionContext::Select, FunctionContext::Where],
        requirements: vec![FunctionRequirement::DeterministicOrder],
        constraints: vec![
            "Requires exactly 1 argument.".to_string(),
            "The argument must evaluate to a boolean condition; a NULL condition is treated as false and a non-boolean condition is an error."
                .to_string(),
            "Resets to 0 on any row where the condition is false; the first matching row returns 1."
                .to_string(),
            "When the row is filtered out, returns the current count and does not advance state."
                .to_string(),
        ],
        examples: vec!["SELECT consecutive_count(spi > 5) AS streak FROM stream".to_string()],
        aggregate: None,
        stateful: Some(StatefulFunctionSpec {
            state_semantics: "Maintains a running count of consecutive true rows, reset on the first false row."
                .to_string(),
        }),
    }
}

impl ConsecutiveCountFunction {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ConsecutiveCountFunction {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Default)]
struct ConsecutiveCountInstance {
    count: i64,
}

impl StatefulFunctionInstance for ConsecutiveCountInstance {
    fn prepare_eval(&self, input: StatefulEvalInput<'_>) -> Result<PreparedStatefulEval, String> {
        if input.args.len() != 1 {
            return Err(format!(
                "consecutive_count() expects exactly 1 argument, got {}",
                input.args.len()
            ));
        }

        // Filtered-out rows return the current count without advancing state, so a
        // later real row continues the streak from where it left off.
        if !input.should_apply {
            return Ok(PreparedStatefulEval::new(
                Value::Int64(self.count),
                Box::new(ConsecutiveCountUpdate { count: None }),
            ));
        }

        let condition = bool_condition("consecutive_count() condition", &input.args[0])?;
        let count = if condition {
            self.count.saturating_add(1)
        } else {
            0
        };
        Ok(PreparedStatefulEval::new(
            Value::Int64(count),
            Box::new(ConsecutiveCountUpdate { count: Some(count) }),
        ))
    }

    fn commit_eval(&mut self, update: Box<dyn StatefulEvalUpdate>) {
        let update = update
            .as_any()
            .downcast_ref::<ConsecutiveCountUpdate>()
            .expect("consecutive_count received incompatible update");
        if let Some(count) = update.count {
            self.count = count;
        }
    }
}

struct ConsecutiveCountUpdate {
    count: Option<i64>,
}

impl StatefulEvalUpdate for ConsecutiveCountUpdate {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl StatefulFunction for ConsecutiveCountFunction {
    fn name(&self) -> &str {
        "consecutive_count"
    }

    fn return_type(&self, input_types: &[ConcreteDatatype]) -> Result<ConcreteDatatype, String> {
        if input_types.len() != 1 {
            return Err(format!(
                "consecutive_count() expects exactly 1 argument type, got {}",
                input_types.len()
            ));
        }
        Ok(ConcreteDatatype::Int64(Int64Type))
    }

    fn create_instance(&self) -> Box<dyn StatefulFunctionInstance> {
        Box::new(ConsecutiveCountInstance::default())
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
    fn counts_consecutive_true_rows_and_resets_on_false() {
        let function = ConsecutiveCountFunction::new();
        let mut instance = function.create_instance();

        assert_eq!(
            eval(&mut instance, &[Value::Bool(true)], true),
            Value::Int64(1)
        );
        assert_eq!(
            eval(&mut instance, &[Value::Bool(true)], true),
            Value::Int64(2)
        );
        assert_eq!(
            eval(&mut instance, &[Value::Bool(true)], true),
            Value::Int64(3)
        );
        // false resets to 0.
        assert_eq!(
            eval(&mut instance, &[Value::Bool(false)], true),
            Value::Int64(0)
        );
        // next true restarts at 1.
        assert_eq!(
            eval(&mut instance, &[Value::Bool(true)], true),
            Value::Int64(1)
        );
    }

    #[test]
    fn first_false_row_is_zero() {
        let function = ConsecutiveCountFunction::new();
        let mut instance = function.create_instance();
        assert_eq!(
            eval(&mut instance, &[Value::Bool(false)], true),
            Value::Int64(0)
        );
    }

    #[test]
    fn null_condition_is_treated_as_false() {
        let function = ConsecutiveCountFunction::new();
        let mut instance = function.create_instance();

        assert_eq!(
            eval(&mut instance, &[Value::Bool(true)], true),
            Value::Int64(1)
        );
        // NULL condition resets the run.
        assert_eq!(eval(&mut instance, &[Value::Null], true), Value::Int64(0));
        assert_eq!(
            eval(&mut instance, &[Value::Bool(true)], true),
            Value::Int64(1)
        );
    }

    #[test]
    fn non_boolean_condition_errors() {
        let function = ConsecutiveCountFunction::new();
        let mut instance = function.create_instance();
        let err = instance
            .eval(StatefulEvalInput {
                args: &[Value::Int64(1)],
                should_apply: true,
            })
            .expect_err("non-boolean condition should error");
        assert!(err.contains("must be bool"), "unexpected error: {err}");
    }

    #[test]
    fn filtered_row_returns_count_and_freezes_state() {
        let function = ConsecutiveCountFunction::new();
        let mut instance = function.create_instance();

        assert_eq!(
            eval(&mut instance, &[Value::Bool(true)], true),
            Value::Int64(1)
        );
        assert_eq!(
            eval(&mut instance, &[Value::Bool(true)], true),
            Value::Int64(2)
        );
        // filtered: returns current count, no advance.
        assert_eq!(
            eval(&mut instance, &[Value::Bool(true)], false),
            Value::Int64(2)
        );
        // next applied true continues from the frozen state.
        assert_eq!(
            eval(&mut instance, &[Value::Bool(true)], true),
            Value::Int64(3)
        );
    }
}
