use super::util::{bool_arg, normalize_state_value};
use super::{StatefulEvalInput, StatefulFunction, StatefulFunctionInstance};
use crate::catalog::{
    FunctionArgSpec, FunctionContext, FunctionDef, FunctionKind, FunctionRequirement,
    FunctionSignatureSpec, StatefulFunctionSpec, TypeSpec,
};
use crate::expr::value_compare;
use datatypes::{ConcreteDatatype, Value};

pub struct ChangeCaptureFunction;

pub fn change_capture_function_def() -> FunctionDef {
    FunctionDef {
        kind: FunctionKind::Stateful,
        name: "change_capture".to_string(),
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
                    name: "capture".to_string(),
                    r#type: TypeSpec::Any,
                    optional: false,
                    variadic: false,
                },
                FunctionArgSpec {
                    name: "monitor".to_string(),
                    r#type: TypeSpec::Any,
                    optional: false,
                    variadic: false,
                },
                FunctionArgSpec {
                    name: "target".to_string(),
                    r#type: TypeSpec::Any,
                    optional: true,
                    variadic: false,
                },
            ],
            return_type: TypeSpec::Any,
        },
        description:
            "Capture and hold the value of `capture` taken at the moment `monitor` changes (optionally only when it changes to `target`)."
                .to_string(),
        allowed_contexts: vec![FunctionContext::Select, FunctionContext::Where],
        requirements: vec![FunctionRequirement::DeterministicOrder],
        constraints: vec![
            "Requires 3 or 4 arguments.".to_string(),
            "The first argument must be a boolean ignore_null flag.".to_string(),
            "When `target` is omitted, captures on any change of `monitor`.".to_string(),
            "When `target` is provided, captures only when `monitor` changes to `target` (type-coerced; a NULL target never matches)."
                .to_string(),
            "Holds and returns the last captured value between captures; returns NULL until the first capture."
                .to_string(),
            "When `ignore_null = true`, a NULL `monitor` is ignored (no capture, state frozen). The captured value may itself be NULL."
                .to_string(),
            "When the row is filtered out, returns the held value and does not update state.".to_string(),
        ],
        examples: vec![
            "SELECT change_capture(true, ts, status, 1) AS activated_at FROM stream".to_string(),
        ],
        aggregate: None,
        stateful: Some(StatefulFunctionSpec {
            state_semantics: "Maintains the previous monitor value and the last captured value held between captures."
                .to_string(),
        }),
    }
}

impl ChangeCaptureFunction {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ChangeCaptureFunction {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Default)]
struct ChangeCaptureInstance {
    config: Option<ChangeCaptureConfig>,
    previous_monitor: Option<Value>,
    captured: Option<Value>,
}

#[derive(Clone, Copy)]
struct ChangeCaptureConfig {
    ignore_null: bool,
}

impl ChangeCaptureInstance {
    fn held(&self) -> Value {
        self.captured.clone().unwrap_or(Value::Null)
    }
}

impl StatefulFunctionInstance for ChangeCaptureInstance {
    fn eval(&mut self, input: StatefulEvalInput<'_>) -> Result<Value, String> {
        if input.args.len() != 3 && input.args.len() != 4 {
            return Err(format!(
                "change_capture() expects 3 or 4 arguments, got {}",
                input.args.len()
            ));
        }

        let config = match self.config {
            Some(config) => config,
            None => {
                let config = ChangeCaptureConfig {
                    ignore_null: bool_arg("change_capture() first argument", &input.args[0])?,
                };
                self.config = Some(config);
                config
            }
        };
        let capture = &input.args[1];
        let monitor = &input.args[2];

        // Ignore a null monitor (when configured) and filtered-out rows: keep
        // holding the last captured value and freeze the monitor state, so a
        // later real change still triggers a capture.
        if config.ignore_null && monitor.is_null() {
            return Ok(self.held());
        }
        if !input.should_apply {
            return Ok(self.held());
        }

        let normalized = normalize_state_value(monitor);
        let changed = self.previous_monitor != normalized;
        self.previous_monitor = normalized;

        if changed {
            let to_target = match input.args.get(3) {
                Some(target) => value_compare::values_equal(monitor, target),
                None => true,
            };
            if to_target {
                self.captured = Some(capture.clone());
            }
        }

        Ok(self.held())
    }
}

impl StatefulFunction for ChangeCaptureFunction {
    fn name(&self) -> &str {
        "change_capture"
    }

    fn return_type(&self, input_types: &[ConcreteDatatype]) -> Result<ConcreteDatatype, String> {
        if input_types.len() != 3 && input_types.len() != 4 {
            return Err(format!(
                "change_capture() expects 3 or 4 argument types, got {}",
                input_types.len()
            ));
        }
        // Returns the captured value, whose type is that of the `capture` argument.
        Ok(input_types[1].clone())
    }

    fn create_instance(&self) -> Box<dyn StatefulFunctionInstance> {
        Box::new(ChangeCaptureInstance::default())
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
    fn change_capture_captures_on_any_change_and_holds() {
        let function = ChangeCaptureFunction::new();
        let mut instance = function.create_instance();

        // monitor 1 (first row, changed) -> capture 10, hold.
        assert_eq!(
            eval(
                &mut instance,
                &[Value::Bool(true), Value::Int64(10), Value::Int64(1)],
                true
            ),
            Value::Int64(10)
        );
        // monitor 1 (unchanged) -> hold 10.
        assert_eq!(
            eval(
                &mut instance,
                &[Value::Bool(true), Value::Int64(20), Value::Int64(1)],
                true
            ),
            Value::Int64(10)
        );
        // monitor 2 (changed) -> capture 30.
        assert_eq!(
            eval(
                &mut instance,
                &[Value::Bool(true), Value::Int64(30), Value::Int64(2)],
                true
            ),
            Value::Int64(30)
        );
        // monitor 2 (unchanged) -> hold 30.
        assert_eq!(
            eval(
                &mut instance,
                &[Value::Bool(true), Value::Int64(40), Value::Int64(2)],
                true
            ),
            Value::Int64(30)
        );
    }

    #[test]
    fn change_capture_captures_only_on_change_to_target() {
        let function = ChangeCaptureFunction::new();
        let mut instance = function.create_instance();

        // monitor 1, target 2 -> not to target -> no capture, hold NULL.
        assert_eq!(
            eval(
                &mut instance,
                &[
                    Value::Bool(true),
                    Value::Int64(10),
                    Value::Int64(1),
                    Value::Int64(2)
                ],
                true
            ),
            Value::Null
        );
        // monitor 2 (changed to target) -> capture 20.
        assert_eq!(
            eval(
                &mut instance,
                &[
                    Value::Bool(true),
                    Value::Int64(20),
                    Value::Int64(2),
                    Value::Int64(2)
                ],
                true
            ),
            Value::Int64(20)
        );
        // monitor 3 (changed, not target) -> hold 20.
        assert_eq!(
            eval(
                &mut instance,
                &[
                    Value::Bool(true),
                    Value::Int64(30),
                    Value::Int64(3),
                    Value::Int64(2)
                ],
                true
            ),
            Value::Int64(20)
        );
        // monitor 2 (changed to target) -> capture 40.
        assert_eq!(
            eval(
                &mut instance,
                &[
                    Value::Bool(true),
                    Value::Int64(40),
                    Value::Int64(2),
                    Value::Int64(2)
                ],
                true
            ),
            Value::Int64(40)
        );
    }

    #[test]
    fn change_capture_ignore_null_monitor_freezes_state() {
        let function = ChangeCaptureFunction::new();
        let mut instance = function.create_instance();

        // monitor 1 -> capture 10.
        assert_eq!(
            eval(
                &mut instance,
                &[Value::Bool(true), Value::Int64(10), Value::Int64(1)],
                true
            ),
            Value::Int64(10)
        );
        // null monitor ignored -> hold 10, monitor state frozen at 1.
        assert_eq!(
            eval(
                &mut instance,
                &[Value::Bool(true), Value::Int64(20), Value::Null],
                true
            ),
            Value::Int64(10)
        );
        // monitor 2 (changed from frozen 1) -> capture 30.
        assert_eq!(
            eval(
                &mut instance,
                &[Value::Bool(true), Value::Int64(30), Value::Int64(2)],
                true
            ),
            Value::Int64(30)
        );
    }

    #[test]
    fn change_capture_filtered_row_holds_and_freezes() {
        let function = ChangeCaptureFunction::new();
        let mut instance = function.create_instance();

        // monitor 1 -> capture 10.
        assert_eq!(
            eval(
                &mut instance,
                &[Value::Bool(true), Value::Int64(10), Value::Int64(1)],
                true
            ),
            Value::Int64(10)
        );
        // filtered: monitor 2 not observed -> hold 10, frozen at 1.
        assert_eq!(
            eval(
                &mut instance,
                &[Value::Bool(true), Value::Int64(20), Value::Int64(2)],
                false
            ),
            Value::Int64(10)
        );
        // monitor 3 (changed from frozen 1) -> capture 40.
        assert_eq!(
            eval(
                &mut instance,
                &[Value::Bool(true), Value::Int64(40), Value::Int64(3)],
                true
            ),
            Value::Int64(40)
        );
    }

    #[test]
    fn change_capture_captures_null_capture_value() {
        let function = ChangeCaptureFunction::new();
        let mut instance = function.create_instance();

        // ignore_null only applies to the monitor; a NULL capture value is captured.
        assert_eq!(
            eval(
                &mut instance,
                &[Value::Bool(true), Value::Null, Value::Int64(1)],
                true
            ),
            Value::Null
        );
        // unchanged monitor -> hold (still NULL captured).
        assert_eq!(
            eval(
                &mut instance,
                &[Value::Bool(true), Value::Int64(99), Value::Int64(1)],
                true
            ),
            Value::Null
        );
    }

    #[test]
    fn change_capture_target_equality_is_type_coerced() {
        let function = ChangeCaptureFunction::new();
        let mut instance = function.create_instance();

        // Int32 monitor vs Int64 target literal should match via coercion.
        assert_eq!(
            eval(
                &mut instance,
                &[
                    Value::Bool(true),
                    Value::Int64(7),
                    Value::Int32(1),
                    Value::Int64(1)
                ],
                true
            ),
            Value::Int64(7)
        );
    }
}
