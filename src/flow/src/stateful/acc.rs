use super::{
    PreparedStatefulEval, StatefulEvalInput, StatefulEvalUpdate, StatefulFunction,
    StatefulFunctionInstance,
};
use crate::catalog::{
    FunctionArgSpec, FunctionContext, FunctionDef, FunctionKind, FunctionRequirement,
    FunctionSignatureSpec, StatefulFunctionSpec, TypeSpec,
};
use datatypes::{ConcreteDatatype, DataType, Float64Type, Int64Type, Value};
use std::any::Any;

pub struct AccSumFunction;
pub struct AccMaxFunction;
pub struct AccMinFunction;
pub struct AccCountFunction;
pub struct AccAvgFunction;

const ACC_PARTITION_STATE_SEMANTICS: &str =
    "Maintains cumulative state globally when unpartitioned, or independently per partition when OVER (PARTITION BY ...) is used.";

pub fn acc_sum_function_def() -> FunctionDef {
    acc_numeric_function_def(
        "acc_sum",
        "Return the cumulative sum of non-NULL numeric input values.",
    )
}

pub fn acc_max_function_def() -> FunctionDef {
    acc_numeric_function_def(
        "acc_max",
        "Return the cumulative maximum of non-NULL numeric input values.",
    )
}

pub fn acc_min_function_def() -> FunctionDef {
    acc_numeric_function_def(
        "acc_min",
        "Return the cumulative minimum of non-NULL numeric input values.",
    )
}

pub fn acc_avg_function_def() -> FunctionDef {
    acc_numeric_function_def(
        "acc_avg",
        "Return the cumulative average of non-NULL numeric input values.",
    )
}

pub fn acc_count_function_def() -> FunctionDef {
    FunctionDef {
        kind: FunctionKind::Stateful,
        name: "acc_count".to_string(),
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
                    name: "begin_cond".to_string(),
                    r#type: TypeSpec::Named {
                        name: "bool".to_string(),
                    },
                    optional: true,
                    variadic: false,
                },
                FunctionArgSpec {
                    name: "reset_cond".to_string(),
                    r#type: TypeSpec::Named {
                        name: "bool".to_string(),
                    },
                    optional: true,
                    variadic: false,
                },
            ],
            return_type: TypeSpec::Named {
                name: "int64".to_string(),
            },
        },
        description: "Return the cumulative count of non-NULL input values.".to_string(),
        allowed_contexts: vec![FunctionContext::Select, FunctionContext::Where],
        requirements: vec![FunctionRequirement::DeterministicOrder],
        constraints: vec![
            "Requires either 1 argument or 3 arguments.".to_string(),
            "The 3-argument form is acc_count(x, begin_cond, reset_cond).".to_string(),
            "begin_cond and reset_cond must evaluate to boolean values; NULL is treated as false."
                .to_string(),
            "Ignores NULL input values.".to_string(),
            "Returns 0 before any non-NULL value has been accepted.".to_string(),
        ],
        examples: vec![
            "SELECT acc_count(a) AS n FROM stream".to_string(),
            "SELECT acc_count(a, status = 'running', status = 'stopped') AS n FROM stream"
                .to_string(),
            "SELECT acc_count(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k) AS n FROM stream"
                .to_string(),
        ],
        aggregate: None,
        stateful: Some(StatefulFunctionSpec {
            state_semantics: ACC_PARTITION_STATE_SEMANTICS.to_string(),
        }),
    }
}

fn acc_numeric_function_def(name: &str, description: &str) -> FunctionDef {
    FunctionDef {
        kind: FunctionKind::Stateful,
        name: name.to_string(),
        aliases: vec![],
        signature: FunctionSignatureSpec {
            args: vec![
                FunctionArgSpec {
                    name: "x".to_string(),
                    r#type: TypeSpec::Category {
                        name: "numeric".to_string(),
                    },
                    optional: false,
                    variadic: false,
                },
                FunctionArgSpec {
                    name: "begin_cond".to_string(),
                    r#type: TypeSpec::Named {
                        name: "bool".to_string(),
                    },
                    optional: true,
                    variadic: false,
                },
                FunctionArgSpec {
                    name: "reset_cond".to_string(),
                    r#type: TypeSpec::Named {
                        name: "bool".to_string(),
                    },
                    optional: true,
                    variadic: false,
                },
            ],
            return_type: TypeSpec::Named {
                name: "float64".to_string(),
            },
        },
        description: description.to_string(),
        allowed_contexts: vec![FunctionContext::Select, FunctionContext::Where],
        requirements: vec![FunctionRequirement::DeterministicOrder],
        constraints: vec![
            "Requires either 1 argument or 3 arguments.".to_string(),
            format!("The 3-argument form is {name}(x, begin_cond, reset_cond)."),
            "begin_cond and reset_cond must evaluate to boolean values; NULL is treated as false."
                .to_string(),
            "Argument type must be numeric.".to_string(),
            "Ignores NULL input values.".to_string(),
            "Returns 0.0 before any non-NULL value has been accepted.".to_string(),
        ],
        examples: vec![
            format!("SELECT {name}(a) FROM stream"),
            format!("SELECT {name}(a, status = 'running', status = 'stopped') FROM stream"),
            format!("SELECT {name}(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k) FROM stream"),
        ],
        aggregate: None,
        stateful: Some(StatefulFunctionSpec {
            state_semantics: ACC_PARTITION_STATE_SEMANTICS.to_string(),
        }),
    }
}

impl AccSumFunction {
    pub fn new() -> Self {
        Self
    }
}

impl Default for AccSumFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl AccMaxFunction {
    pub fn new() -> Self {
        Self
    }
}

impl Default for AccMaxFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl AccMinFunction {
    pub fn new() -> Self {
        Self
    }
}

impl Default for AccMinFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl AccCountFunction {
    pub fn new() -> Self {
        Self
    }
}

impl Default for AccCountFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl AccAvgFunction {
    pub fn new() -> Self {
        Self
    }
}

impl Default for AccAvgFunction {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Default)]
struct AccSumInstance {
    sum: f64,
    has_begin: bool,
}

#[derive(Default)]
struct AccMaxInstance {
    value: Option<f64>,
    has_begin: bool,
}

#[derive(Default)]
struct AccMinInstance {
    value: Option<f64>,
    has_begin: bool,
}

#[derive(Default)]
struct AccCountInstance {
    count: i64,
    has_begin: bool,
}

#[derive(Default)]
struct AccAvgInstance {
    sum: f64,
    count: u64,
    has_begin: bool,
}

fn validate_arg_count(name: &str, args: &[Value]) -> Result<(), String> {
    if args.len() != 1 && args.len() != 3 {
        return Err(format!(
            "{name} expects either 1 argument or 3 arguments: {name}(value[, begin_cond, reset_cond]), got {}",
            args.len()
        ));
    }
    Ok(())
}

struct AccLifecycle {
    should_update: bool,
    reset_before_eval: bool,
    reset_after_eval: bool,
    next_has_begin: bool,
}

fn condition_arg(name: &str, arg_name: &str, value: &Value) -> Result<bool, String> {
    match value {
        Value::Bool(v) => Ok(*v),
        Value::Null => Ok(false),
        other => Err(format!("{name} {arg_name} must be bool, got {other:?}")),
    }
}

fn prepare_lifecycle(
    name: &str,
    args: &[Value],
    should_apply: bool,
    has_begin: bool,
) -> Result<AccLifecycle, String> {
    validate_arg_count(name, args)?;
    if args.len() == 1 {
        return Ok(AccLifecycle {
            should_update: should_apply,
            reset_before_eval: false,
            reset_after_eval: false,
            next_has_begin: has_begin,
        });
    }

    let reset_before_eval = !has_begin;
    let begin_cond = condition_arg(name, "begin_cond", &args[1])?;
    let reset_cond = condition_arg(name, "reset_cond", &args[2])?;
    let active = has_begin || begin_cond;

    Ok(AccLifecycle {
        should_update: active && should_apply,
        reset_before_eval,
        reset_after_eval: reset_cond,
        next_has_begin: if reset_cond { false } else { active },
    })
}

fn numeric_arg(name: &str, value: &Value) -> Result<Option<f64>, String> {
    if value.is_null() {
        return Ok(None);
    }

    let float64 = Float64Type;
    match float64.try_cast(value.clone()) {
        Some(Value::Float64(v)) => Ok(Some(v)),
        _ => Err(format!("{name} expects a numeric value, got {value:?}")),
    }
}

fn validate_numeric_return_type(
    name: &str,
    input_types: &[ConcreteDatatype],
) -> Result<ConcreteDatatype, String> {
    validate_return_type_arity(name, input_types)?;
    validate_lifecycle_return_types(name, input_types)?;

    match &input_types[0] {
        ConcreteDatatype::Int8(_)
        | ConcreteDatatype::Int16(_)
        | ConcreteDatatype::Int32(_)
        | ConcreteDatatype::Int64(_)
        | ConcreteDatatype::Uint8(_)
        | ConcreteDatatype::Uint16(_)
        | ConcreteDatatype::Uint32(_)
        | ConcreteDatatype::Uint64(_)
        | ConcreteDatatype::Float32(_)
        | ConcreteDatatype::Float64(_) => Ok(ConcreteDatatype::Float64(Float64Type)),
        other => Err(format!("{name} does not support type {other:?}")),
    }
}

fn validate_return_type_arity(name: &str, input_types: &[ConcreteDatatype]) -> Result<(), String> {
    if input_types.len() != 1 && input_types.len() != 3 {
        return Err(format!(
            "{name} expects either 1 argument type or 3 argument types, got {}",
            input_types.len()
        ));
    }
    Ok(())
}

fn validate_lifecycle_return_types(
    name: &str,
    input_types: &[ConcreteDatatype],
) -> Result<(), String> {
    if input_types.len() == 3 {
        for (arg_name, ty) in [
            ("begin_cond", &input_types[1]),
            ("reset_cond", &input_types[2]),
        ] {
            match ty {
                ConcreteDatatype::Bool(_) | ConcreteDatatype::Null => {}
                other => {
                    return Err(format!("{name} {arg_name} must be bool, got {other:?}"));
                }
            }
        }
    }
    Ok(())
}

impl StatefulFunctionInstance for AccSumInstance {
    fn prepare_eval(&self, input: StatefulEvalInput<'_>) -> Result<PreparedStatefulEval, String> {
        let lifecycle =
            prepare_lifecycle("acc_sum", input.args, input.should_apply, self.has_begin)?;
        let mut sum = if lifecycle.reset_before_eval {
            0.0
        } else {
            self.sum
        };
        if lifecycle.should_update {
            if let Some(value) = numeric_arg("acc_sum", &input.args[0])? {
                sum += value;
            }
        }
        let out = Value::Float64(sum);
        let next_sum = if lifecycle.reset_after_eval { 0.0 } else { sum };
        Ok(PreparedStatefulEval::new(
            out,
            Box::new(AccSumUpdate {
                sum: next_sum,
                has_begin: lifecycle.next_has_begin,
            }),
        ))
    }

    fn commit_eval(&mut self, update: Box<dyn StatefulEvalUpdate>) {
        let update = update
            .as_any()
            .downcast_ref::<AccSumUpdate>()
            .expect("acc_sum received incompatible update");
        self.sum = update.sum;
        self.has_begin = update.has_begin;
    }
}

impl StatefulFunctionInstance for AccMaxInstance {
    fn prepare_eval(&self, input: StatefulEvalInput<'_>) -> Result<PreparedStatefulEval, String> {
        let lifecycle =
            prepare_lifecycle("acc_max", input.args, input.should_apply, self.has_begin)?;
        let mut value = if lifecycle.reset_before_eval {
            None
        } else {
            self.value
        };
        if lifecycle.should_update {
            if let Some(next) = numeric_arg("acc_max", &input.args[0])? {
                value = Some(match value {
                    Some(current) => current.max(next),
                    None => next,
                });
            }
        }
        let out = Value::Float64(value.unwrap_or(0.0));
        let next_value = if lifecycle.reset_after_eval {
            None
        } else {
            value
        };
        Ok(PreparedStatefulEval::new(
            out,
            Box::new(AccExtremaUpdate {
                value: next_value,
                has_begin: lifecycle.next_has_begin,
            }),
        ))
    }

    fn commit_eval(&mut self, update: Box<dyn StatefulEvalUpdate>) {
        let update = update
            .as_any()
            .downcast_ref::<AccExtremaUpdate>()
            .expect("acc_max received incompatible update");
        self.value = update.value;
        self.has_begin = update.has_begin;
    }
}

impl StatefulFunctionInstance for AccMinInstance {
    fn prepare_eval(&self, input: StatefulEvalInput<'_>) -> Result<PreparedStatefulEval, String> {
        let lifecycle =
            prepare_lifecycle("acc_min", input.args, input.should_apply, self.has_begin)?;
        let mut value = if lifecycle.reset_before_eval {
            None
        } else {
            self.value
        };
        if lifecycle.should_update {
            if let Some(next) = numeric_arg("acc_min", &input.args[0])? {
                value = Some(match value {
                    Some(current) => current.min(next),
                    None => next,
                });
            }
        }
        let out = Value::Float64(value.unwrap_or(0.0));
        let next_value = if lifecycle.reset_after_eval {
            None
        } else {
            value
        };
        Ok(PreparedStatefulEval::new(
            out,
            Box::new(AccExtremaUpdate {
                value: next_value,
                has_begin: lifecycle.next_has_begin,
            }),
        ))
    }

    fn commit_eval(&mut self, update: Box<dyn StatefulEvalUpdate>) {
        let update = update
            .as_any()
            .downcast_ref::<AccExtremaUpdate>()
            .expect("acc_min received incompatible update");
        self.value = update.value;
        self.has_begin = update.has_begin;
    }
}

impl StatefulFunctionInstance for AccCountInstance {
    fn prepare_eval(&self, input: StatefulEvalInput<'_>) -> Result<PreparedStatefulEval, String> {
        let lifecycle =
            prepare_lifecycle("acc_count", input.args, input.should_apply, self.has_begin)?;
        let mut count = if lifecycle.reset_before_eval {
            0
        } else {
            self.count
        };
        if lifecycle.should_update && !input.args[0].is_null() {
            count = count.saturating_add(1);
        }
        let out = Value::Int64(count);
        let next_count = if lifecycle.reset_after_eval { 0 } else { count };
        Ok(PreparedStatefulEval::new(
            out,
            Box::new(AccCountUpdate {
                count: next_count,
                has_begin: lifecycle.next_has_begin,
            }),
        ))
    }

    fn commit_eval(&mut self, update: Box<dyn StatefulEvalUpdate>) {
        let update = update
            .as_any()
            .downcast_ref::<AccCountUpdate>()
            .expect("acc_count received incompatible update");
        self.count = update.count;
        self.has_begin = update.has_begin;
    }
}

impl StatefulFunctionInstance for AccAvgInstance {
    fn prepare_eval(&self, input: StatefulEvalInput<'_>) -> Result<PreparedStatefulEval, String> {
        let lifecycle =
            prepare_lifecycle("acc_avg", input.args, input.should_apply, self.has_begin)?;
        let (mut sum, mut count) = if lifecycle.reset_before_eval {
            (0.0, 0)
        } else {
            (self.sum, self.count)
        };
        if lifecycle.should_update {
            if let Some(value) = numeric_arg("acc_avg", &input.args[0])? {
                sum += value;
                count = count.saturating_add(1);
            }
        }
        let out = if count == 0 {
            Value::Float64(0.0)
        } else {
            Value::Float64(sum / count as f64)
        };
        let (next_sum, next_count) = if lifecycle.reset_after_eval {
            (0.0, 0)
        } else {
            (sum, count)
        };
        Ok(PreparedStatefulEval::new(
            out,
            Box::new(AccAvgUpdate {
                sum: next_sum,
                count: next_count,
                has_begin: lifecycle.next_has_begin,
            }),
        ))
    }

    fn commit_eval(&mut self, update: Box<dyn StatefulEvalUpdate>) {
        let update = update
            .as_any()
            .downcast_ref::<AccAvgUpdate>()
            .expect("acc_avg received incompatible update");
        self.sum = update.sum;
        self.count = update.count;
        self.has_begin = update.has_begin;
    }
}

struct AccSumUpdate {
    sum: f64,
    has_begin: bool,
}

struct AccExtremaUpdate {
    value: Option<f64>,
    has_begin: bool,
}

struct AccCountUpdate {
    count: i64,
    has_begin: bool,
}

struct AccAvgUpdate {
    sum: f64,
    count: u64,
    has_begin: bool,
}

impl StatefulEvalUpdate for AccSumUpdate {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl StatefulEvalUpdate for AccExtremaUpdate {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl StatefulEvalUpdate for AccCountUpdate {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl StatefulEvalUpdate for AccAvgUpdate {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl StatefulFunction for AccSumFunction {
    fn name(&self) -> &str {
        "acc_sum"
    }

    fn return_type(&self, input_types: &[ConcreteDatatype]) -> Result<ConcreteDatatype, String> {
        validate_numeric_return_type("acc_sum", input_types)
    }

    fn create_instance(&self) -> Box<dyn StatefulFunctionInstance> {
        Box::new(AccSumInstance::default())
    }
}

impl StatefulFunction for AccMaxFunction {
    fn name(&self) -> &str {
        "acc_max"
    }

    fn return_type(&self, input_types: &[ConcreteDatatype]) -> Result<ConcreteDatatype, String> {
        validate_numeric_return_type("acc_max", input_types)
    }

    fn create_instance(&self) -> Box<dyn StatefulFunctionInstance> {
        Box::new(AccMaxInstance::default())
    }
}

impl StatefulFunction for AccMinFunction {
    fn name(&self) -> &str {
        "acc_min"
    }

    fn return_type(&self, input_types: &[ConcreteDatatype]) -> Result<ConcreteDatatype, String> {
        validate_numeric_return_type("acc_min", input_types)
    }

    fn create_instance(&self) -> Box<dyn StatefulFunctionInstance> {
        Box::new(AccMinInstance::default())
    }
}

impl StatefulFunction for AccCountFunction {
    fn name(&self) -> &str {
        "acc_count"
    }

    fn return_type(&self, input_types: &[ConcreteDatatype]) -> Result<ConcreteDatatype, String> {
        validate_return_type_arity("acc_count", input_types)?;
        validate_lifecycle_return_types("acc_count", input_types)?;
        Ok(ConcreteDatatype::Int64(Int64Type))
    }

    fn create_instance(&self) -> Box<dyn StatefulFunctionInstance> {
        Box::new(AccCountInstance::default())
    }
}

impl StatefulFunction for AccAvgFunction {
    fn name(&self) -> &str {
        "acc_avg"
    }

    fn return_type(&self, input_types: &[ConcreteDatatype]) -> Result<ConcreteDatatype, String> {
        validate_numeric_return_type("acc_avg", input_types)
    }

    fn create_instance(&self) -> Box<dyn StatefulFunctionInstance> {
        Box::new(AccAvgInstance::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn eval_all(function: &dyn StatefulFunction, values: &[Value]) -> Vec<Value> {
        let mut instance = function.create_instance();
        values
            .iter()
            .map(|value| {
                instance
                    .eval(StatefulEvalInput {
                        args: std::slice::from_ref(value),
                        should_apply: true,
                    })
                    .expect("acc eval")
            })
            .collect()
    }

    #[test]
    fn acc_sum_returns_running_sum() {
        let function = AccSumFunction::new();
        assert_eq!(
            eval_all(
                &function,
                &[
                    Value::Float64(1.0),
                    Value::Float64(2.5),
                    Value::Null,
                    Value::Int64(3),
                ],
            ),
            vec![
                Value::Float64(1.0),
                Value::Float64(3.5),
                Value::Float64(3.5),
                Value::Float64(6.5),
            ]
        );
    }

    #[test]
    fn acc_max_returns_running_max() {
        let function = AccMaxFunction::new();
        assert_eq!(
            eval_all(
                &function,
                &[
                    Value::Float64(2.0),
                    Value::Float64(1.0),
                    Value::Null,
                    Value::Float64(3.0),
                ],
            ),
            vec![
                Value::Float64(2.0),
                Value::Float64(2.0),
                Value::Float64(2.0),
                Value::Float64(3.0),
            ]
        );
    }

    #[test]
    fn acc_min_returns_running_min() {
        let function = AccMinFunction::new();
        assert_eq!(
            eval_all(
                &function,
                &[
                    Value::Float64(2.0),
                    Value::Float64(1.0),
                    Value::Null,
                    Value::Float64(3.0),
                ],
            ),
            vec![
                Value::Float64(2.0),
                Value::Float64(1.0),
                Value::Float64(1.0),
                Value::Float64(1.0),
            ]
        );
    }

    #[test]
    fn acc_count_returns_running_non_null_count() {
        let function = AccCountFunction::new();
        assert_eq!(
            eval_all(
                &function,
                &[
                    Value::String("a".to_string()),
                    Value::Null,
                    Value::Bool(false),
                    Value::Int64(3),
                ],
            ),
            vec![
                Value::Int64(1),
                Value::Int64(1),
                Value::Int64(2),
                Value::Int64(3),
            ]
        );
    }

    #[test]
    fn acc_avg_returns_running_average() {
        let function = AccAvgFunction::new();
        assert_eq!(
            eval_all(
                &function,
                &[
                    Value::Float64(1.0),
                    Value::Float64(2.0),
                    Value::Null,
                    Value::Float64(3.0),
                ],
            ),
            vec![
                Value::Float64(1.0),
                Value::Float64(1.5),
                Value::Float64(1.5),
                Value::Float64(2.0),
            ]
        );
    }

    #[test]
    fn acc_numeric_functions_return_zero_before_valid_input() {
        for function in [
            Box::new(AccSumFunction::new()) as Box<dyn StatefulFunction>,
            Box::new(AccMaxFunction::new()) as Box<dyn StatefulFunction>,
            Box::new(AccMinFunction::new()) as Box<dyn StatefulFunction>,
            Box::new(AccAvgFunction::new()) as Box<dyn StatefulFunction>,
        ] {
            let mut instance = function.create_instance();
            let out = instance
                .eval(StatefulEvalInput {
                    args: &[Value::Null],
                    should_apply: true,
                })
                .expect("acc eval");
            assert_eq!(out, Value::Float64(0.0));
        }
    }

    #[test]
    fn acc_numeric_functions_reject_non_numeric_values() {
        let function = AccSumFunction::new();
        let mut instance = function.create_instance();
        let err = instance
            .eval(StatefulEvalInput {
                args: &[Value::String("x".to_string())],
                should_apply: true,
            })
            .expect_err("non-numeric value should fail");
        assert!(err.contains("numeric"));
    }
}
